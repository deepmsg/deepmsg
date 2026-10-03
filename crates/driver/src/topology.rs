//! What a cpuset looks like to the CPU it will run on (G4-2).
//!
//! A driver pinned to a cpuset that cuts a physical core in half, or that spans
//! two dies or two L3 caches, will run — and run worse than its operator
//! expects. The reference's answer is to **say so**: three checks that read
//! `/sys/devices/system/cpu` and print a warning per problem
//! (`aeron-driver/src/main/c/aeron_topology.c:467-620`), whose count
//! `aeron.driver.cpuset.warnings.as.errors` can turn into a refusal to start.
//!
//! The three, and what each one is looking at:
//!
//! * **alignment** — a core whose sibling threads are not all in the cpuset.
//!   `thread_siblings_list` gives a CPU's siblings, so a group with some
//!   present and some missing is a core that has been split
//!   (`check_alignment`, `:467`).
//! * **L3 locality** — the cpuset reaching outside the L3 cache that its first
//!   CPU shares (`cache/index3/shared_cpu_list`, `:512`).
//! * **die locality** — the cpuset spanning more than one die
//!   (`topology/die_id`, `:558`).
//!
//! Everything here is a **warning**: none of them fails on its own, and a check
//! that cannot read what it needs is not a warning either — it is silence
//! (`aeron_err_clear()` and carry on, `:539-545`). The only hard limit is the
//! one the reference puts on the array sizes: more CPUs than
//! [`MAX_CPU_ID`] is an error rather than a warning.

use std::io;
use std::path::Path;

use crate::cpuset::{CpusetError, parse_cpulist};

/// `AERON_TOPOLOGY_MAX_CPU_ID` (`aeron_topology.c:30`): how many CPUs the
/// reference's lookup tables hold.
///
/// A count above it is refused rather than truncated, which is the reference's
/// behaviour — and it compares the **count**, not the ids, which is its own
/// quirk and is kept as it is.
pub const MAX_CPU_ID: usize = 8192;

/// Where the kernel describes its CPUs.
pub const SYS_CPU_PATH: &str = "/sys/devices/system/cpu";

/// Why a topology check could not run.
#[derive(Debug)]
pub enum TopologyError {
    /// More CPUs than the reference's tables hold.
    TooManyCpus(usize),
    /// A file that could not be read — a CPU with no `topology/` directory, or
    /// a system that does not describe its CPUs the way Linux does.
    Io(io::Error),
    /// A sibling, peer or `die_id` file whose contents are not a list or a
    /// number.
    Parse(CpusetError),
}

impl std::fmt::Display for TopologyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TooManyCpus(count) => {
                write!(
                    f,
                    "WARNING: cpu count {count} is greater than max cpu count"
                )
            }
            Self::Io(error) => write!(f, "{error}"),
            Self::Parse(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for TopologyError {}

/// One physical core's worth of siblings, as seen from inside a cpuset.
///
/// The reference keeps the two halves separately because it is the pair that
/// matters: a group with both is a split core, and one with only `present` is a
/// whole core that happens to be in the cpuset.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CoreGroup {
    /// The siblings this cpuset contains, ascending.
    pub present: Vec<i32>,
    /// The siblings it does not, ascending.
    pub missing: Vec<i32>,
}

/// Every core that has at least one CPU in `cpus`, in the order the CPUs are
/// visited, each split into what the cpuset has and what it has not
/// (`aeron_topology_read_core_groups`, `:152-290`).
///
/// # Errors
///
/// [`TopologyError`] if a sibling list cannot be read, or the count is over
/// [`MAX_CPU_ID`].
pub fn read_core_groups(root: &Path, cpus: &[i32]) -> Result<Vec<CoreGroup>, TopologyError> {
    check_count(cpus)?;

    let mut seen = vec![false; MAX_CPU_ID];
    let mut groups = Vec::new();

    for cpu in cpus {
        let index = usize::try_from(*cpu).unwrap_or(usize::MAX);
        if index < MAX_CPU_ID && seen[index] {
            continue;
        }

        let siblings = read_siblings(root, *cpu)?;

        for sibling in &siblings {
            let sibling = usize::try_from(*sibling).unwrap_or(usize::MAX);
            if sibling < MAX_CPU_ID {
                seen[sibling] = true;
            }
        }

        let mut present = Vec::new();
        let mut missing = Vec::new();

        for sibling in siblings {
            if cpus.contains(&sibling) {
                present.push(sibling);
            } else {
                missing.push(sibling);
            }
        }

        groups.push(CoreGroup { present, missing });
    }

    Ok(groups)
}

/// A core split between the cpuset and the rest of the machine, one warning
/// each (`:467-509`).
///
/// A cpuset with fewer than two CPUs is never misaligned — there is no core to
/// split — and answers no warnings.
///
/// # Errors
///
/// [`TopologyError`] if a sibling list cannot be read, or the count is over
/// [`MAX_CPU_ID`].
pub fn check_alignment(root: &Path, cpus: &[i32]) -> Result<Vec<String>, TopologyError> {
    check_count(cpus)?;

    if cpus.len() < 2 {
        return Ok(Vec::new());
    }

    let mut warnings = Vec::new();

    for group in read_core_groups(root, cpus)? {
        if group.missing.is_empty() || group.present.is_empty() {
            continue;
        }

        warnings.push(format!(
            "WARNING: cpuset is missing sibling CPU(s) {} of the core containing CPU {} \
             (partial core in cpuset)",
            format_cpulist(&group.missing),
            group.present[0]
        ));
    }

    Ok(warnings)
}

/// The cpuset reaching outside the L3 cache its first CPU is in, one warning
/// (`:512-556`).
///
/// # Errors
///
/// [`TopologyError`] if the count is over [`MAX_CPU_ID`].
pub fn check_l3_locality(root: &Path, cpus: &[i32]) -> Result<Vec<String>, TopologyError> {
    check_count(cpus)?;

    if cpus.len() < 2 {
        return Ok(Vec::new());
    }

    // A CPU with no L3 description — a VM, or a kernel that does not publish
    // one — is not a warning: the reference clears the error and says nothing.
    let Ok(peers) = read_l3_peers(root, cpus[0]) else {
        return Ok(Vec::new());
    };

    if cpus[1..].iter().any(|cpu| !peers.contains(cpu)) {
        return Ok(vec![
            "WARNING: cpuset spans multiple L3 cache domains".to_owned(),
        ]);
    }

    Ok(Vec::new())
}

/// The cpuset spanning more than one die, one warning (`:558-620`).
///
/// The message carries the die ids it found — sorted, comma separated — which
/// is the reference's own wording, and it is the one warning without the
/// `WARNING:` prefix, because the reference prints it without one.
///
/// # Errors
///
/// [`TopologyError`] if the count is over [`MAX_CPU_ID`].
pub fn check_die_locality(root: &Path, cpus: &[i32]) -> Result<Vec<String>, TopologyError> {
    check_count(cpus)?;

    if cpus.len() < 2 {
        return Ok(Vec::new());
    }

    let mut ids: Vec<i32> = Vec::new();

    for cpu in cpus {
        // A CPU whose die id cannot be read makes the whole check silent,
        // which is the reference's `cluster_ok = 0` arms.
        let Ok(id) = read_die_id(root, *cpu) else {
            return Ok(Vec::new());
        };

        if !ids.contains(&id) {
            ids.push(id);
        }
    }

    if ids.len() < 2 {
        return Ok(Vec::new());
    }

    ids.sort_unstable();
    let listed: Vec<String> = ids.iter().map(i32::to_string).collect();

    Ok(vec![format!(
        "cpuset spans {} CPU clusters (cluster IDs: {})",
        ids.len(),
        listed.join(", ")
    )])
}

/// The CPUs in `cpus` as a `cpulist`, with runs collapsed into ranges
/// (`aeron_topology_format_cpulist`, `:294-330`).
pub fn format_cpulist(cpus: &[i32]) -> String {
    let mut text = String::new();
    let mut index = 0;

    while index < cpus.len() {
        let mut end = index;
        while end + 1 < cpus.len() && cpus[end + 1] == cpus[end] + 1 {
            end += 1;
        }

        if !text.is_empty() {
            text.push(',');
        }

        if index == end {
            text.push_str(&cpus[index].to_string());
        } else {
            text.push_str(&format!("{}-{}", cpus[index], cpus[end]));
        }

        index = end + 1;
    }

    text
}

/// `AERON_TOPOLOGY_MAX_CPU_ID < cpu_count`, which is the reference's own
/// comparison: the **count** against the table size, not the ids.
fn check_count(cpus: &[i32]) -> Result<(), TopologyError> {
    if MAX_CPU_ID < cpus.len() {
        return Err(TopologyError::TooManyCpus(cpus.len()));
    }

    Ok(())
}

/// A CPU's sibling threads, from `thread_siblings_list`.
fn read_siblings(root: &Path, cpu: i32) -> Result<Vec<i32>, TopologyError> {
    let text = read_cpu_file(root, cpu, "topology/thread_siblings_list")?;

    parse_cpulist(&text).map_err(TopologyError::Parse)
}

/// The CPUs sharing an L3 cache with `cpu`, from `cache/index3/shared_cpu_list`.
fn read_l3_peers(root: &Path, cpu: i32) -> Result<Vec<i32>, TopologyError> {
    let text = read_cpu_file(root, cpu, "cache/index3/shared_cpu_list")?;

    parse_cpulist(&text).map_err(TopologyError::Parse)
}

/// The die `cpu` is on, from `topology/die_id`.
fn read_die_id(root: &Path, cpu: i32) -> Result<i32, TopologyError> {
    let text = read_cpu_file(root, cpu, "topology/die_id")?;

    text.trim()
        .parse()
        .map_err(|_| TopologyError::Parse(CpusetError::NonNumeric))
}

/// Read one of a CPU's `sysfs` files.
fn read_cpu_file(root: &Path, cpu: i32, suffix: &str) -> Result<String, TopologyError> {
    std::fs::read_to_string(root.join(format!("cpu{cpu}")).join(suffix)).map_err(TopologyError::Io)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    /// A directory of its own in the system temp directory, removed on drop.
    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
            let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let path =
                std::env::temp_dir().join(format!("deepmsg-topology-{}-{n}", std::process::id()));
            let _ = std::fs::remove_dir_all(&path);
            std::fs::create_dir_all(&path).expect("a temp directory");

            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }

        /// Write `contents` at `relative`, making the directories it needs.
        fn write(&self, relative: &str, contents: &str) {
            let path = self.0.join(relative);
            std::fs::create_dir_all(path.parent().expect("a parent")).expect("a directory");
            std::fs::write(&path, contents).expect("the file");
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// The sibling list of each of the sixteen CPUs of the reference's fixture,
    /// which is its own vector entry for entry
    /// (`TopologyTest::setupSiblings`, `aeron_topology_test.cpp:89-101` and the
    /// sixteen pairs at `:139-142`): every CPU names the core it is on, so
    /// `cpu4` and `cpu5` both say `4-5`.
    fn write_siblings(dir: &TempDir) {
        for cpu in 0..16 {
            let core = cpu / 2;
            dir.write(
                &format!("sysfs/cpu{cpu}/topology/thread_siblings_list"),
                &format!("{}-{}\n", core * 2, core * 2 + 1),
            );
        }
    }

    /// `shouldCheckAlignment` (`:124-159`): the cpuset runs from CPU 5 to 10 and
    /// the machine's cores are pairs, so the core holding 5 is split (4 is
    /// outside) and the one holding 10 is too (11 is outside).
    #[test]
    fn a_cpuset_that_splits_a_core_is_reported_once_per_core() {
        let dir = TempDir::new();
        write_siblings(&dir);

        let warnings =
            check_alignment(&dir.path().join("sysfs"), &[5, 6, 7, 8, 9, 10]).expect("the check");

        assert_eq!(2, warnings.len(), "{warnings:?}");
        assert!(
            warnings[0].contains("cpuset is missing sibling CPU(s) 4"),
            "{}",
            warnings[0]
        );
        assert!(
            warnings[1].contains("cpuset is missing sibling CPU(s) 11"),
            "{}",
            warnings[1]
        );
    }

    /// `shouldCheckL3Locality` (`:161-194`): the cpuset's first CPU is in an L3
    /// shared by 0-7, and CPUs 8 to 10 are outside it.
    #[test]
    fn a_cpuset_that_spans_two_l3_caches_is_reported_once() {
        let dir = TempDir::new();
        for cpu in 0..16 {
            let (first, second) = if cpu < 8 { (0, 7) } else { (8, 15) };
            dir.write(
                &format!("sysfs/cpu{cpu}/cache/index3/shared_cpu_list"),
                &format!("{first}-{second}\n"),
            );
        }

        let warnings =
            check_l3_locality(&dir.path().join("sysfs"), &[5, 6, 7, 8, 9, 10]).expect("the check");

        assert_eq!(1, warnings.len(), "{warnings:?}");
        assert!(warnings[0].contains("cpuset spans multiple L3 cache domains"));
    }

    /// `shouldCheckClusterLocality` (`:196-227`): the CPUs are on two dies, and
    /// the message carries the ids.
    #[test]
    fn a_cpuset_that_spans_two_dies_is_reported_once() {
        let dir = TempDir::new();
        for cpu in 0..16 {
            let die = if cpu < 8 { 65535 } else { 0 };
            dir.write(
                &format!("sysfs/cpu{cpu}/topology/die_id"),
                &format!("{die}\n"),
            );
        }

        let warnings =
            check_die_locality(&dir.path().join("sysfs"), &[5, 6, 7, 8, 9, 10]).expect("the check");

        assert_eq!(1, warnings.len(), "{warnings:?}");
        assert!(
            warnings[0].contains("cpuset spans 2 CPU clusters"),
            "{}",
            warnings[0]
        );
        assert!(warnings[0].contains("0, 65535"), "{}", warnings[0]);
    }

    /// A cpuset with one CPU has no core to split and no locality to span, so
    /// every check is silent — the reference's own `cpu_count < 2` arm.
    #[test]
    fn one_cpu_is_never_a_warning() {
        let dir = TempDir::new();
        write_siblings(&dir);

        let root = dir.path().join("sysfs");
        assert!(check_alignment(&root, &[7]).expect("the check").is_empty());
        assert!(
            check_l3_locality(&root, &[7])
                .expect("the check")
                .is_empty()
        );
        assert!(
            check_die_locality(&root, &[7])
                .expect("the check")
                .is_empty()
        );
    }

    /// A file the check needs but cannot read is silence, not a warning: a
    /// machine that does not describe its CPUs is not a misconfigured cpuset.
    #[test]
    fn a_cpu_the_kernel_does_not_describe_is_not_a_warning() {
        let dir = TempDir::new();

        let root = dir.path().join("sysfs");
        assert!(
            check_l3_locality(&root, &[5, 6])
                .expect("the check")
                .is_empty(),
            "no cache/index3 is not a warning"
        );
        assert!(
            check_die_locality(&root, &[5, 6])
                .expect("the check")
                .is_empty(),
            "no die_id is not a warning"
        );
    }

    /// The formatter collapses runs, which is what the alignment message
    /// carries (`aeron_topology_format_cpulist`, `:294`).
    #[test]
    fn a_cpu_list_collapses_runs_into_ranges() {
        assert_eq!("4", format_cpulist(&[4]));
        assert_eq!("4,11", format_cpulist(&[4, 11]));
        assert_eq!("4-6", format_cpulist(&[4, 5, 6]));
        assert_eq!("0-2,6,8-9", format_cpulist(&[0, 1, 2, 6, 8, 9]));
    }
}
