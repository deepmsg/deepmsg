//! The CPUs a driver may use, read from its cgroup (G4-2).
//!
//! `aeron.driver.cpuset.affinity` asks the driver to pin its agents to the CPUs
//! its cgroup allows, and this is where that list comes from: cgroup **v2**
//! keeps it in `<cgroup>/cpuset.cpus.effective` as a `cpulist` — `0-3,7`, the
//! same spelling the kernel uses — and the reference reads it with
//! `aeron_cpuset_cgroup_read_v2` (`aeron-driver/src/main/c/aeron_cpuset.c:353-420`).
//!
//! Two things are worth knowing about the shape of it, because both are the
//! reference's rather than this build's:
//!
//! * the cgroup a process is in comes from `/proc/self/cgroup`, whose v2 line
//!   is the one with an empty controller list (`0::/user.slice/…`);
//! * the file may live in a **parent** cgroup rather than the process's own, so
//!   the search walks up the path until it finds one
//!   (`shouldReadV2CgroupsInParent`, `aeron_cpuset_test.cpp:113`).
//!
//! The file itself is checked before it is read: a path that resolves outside
//! `/sys/` and outside the temporary directory is refused
//! (`aeron_cpuset_validate_path_root`, `:158-175`). The temporary directory is
//! in that list for the reference's own tests, which build a cgroup tree under
//! `/tmp`, and it stays in this build's for the same reason.

use std::io;
use std::path::{Path, PathBuf};

/// Where the unified (v2) hierarchy is mounted (`AERON_CPUSET_CGROUP_MOUNT_V2`).
pub const CGROUP_MOUNT_V2: &str = "/sys/fs/cgroup";

/// The file that says which cgroup this process is in
/// (`AERON_CPUSET_PROC_SELF_CGROUP`).
pub const PROC_SELF_CGROUP: &str = "/proc/self/cgroup";

/// The name the effective CPU list has inside a cgroup.
const CPUSET_FILE: &str = "cpuset.cpus.effective";

/// Why a cpuset could not be read or parsed.
///
/// The wordings are the reference's own, and they are load-bearing: its tests
/// match the message rather than the call
/// (`aeron_cpuset_test.cpp:187-217`), so "empty string" and "trailing comma"
/// are the names of those arms here too.
#[derive(Debug)]
pub enum CpusetError {
    /// The list was empty.
    Empty,
    /// A comma with nothing before it.
    LeadingComma,
    /// A comma with nothing after it.
    TrailingComma,
    /// A range end with no CPU before it, or one right after a comma.
    NegativeCpu,
    /// A range whose end comes before its start.
    RangeEndBeforeStart,
    /// A character that is not part of a `cpulist`.
    NonNumeric,
    /// A file could not be read.
    Io(io::Error),
    /// A path that is not a cgroup file this build will read.
    InvalidLocation(PathBuf),
    /// No `cpuset.cpus.effective` anywhere up the cgroup path.
    NotFound(PathBuf),
    /// A slot asked for a position in the cpuset that it does not have
    /// (`aeron_driver_context.c:3545-3550`).
    AffinityOutOfRange {
        /// The role whose setting it was.
        role: &'static str,
        /// The position it named.
        affinity: i32,
        /// How many CPUs the cpuset has.
        count: usize,
    },
    /// The topology checks complained and `cpuset.warnings.as.errors` is on
    /// (`aeron_driver.c:1190-1194`).
    WarningsAsErrors(usize),
}

impl std::fmt::Display for CpusetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => write!(f, "empty string"),
            Self::LeadingComma => write!(f, "leading comma"),
            Self::TrailingComma => write!(f, "trailing comma"),
            Self::NegativeCpu => write!(f, "negative CPU"),
            Self::RangeEndBeforeStart => write!(f, "range end less than start"),
            Self::NonNumeric => write!(f, "non-numeric CPU"),
            Self::Io(error) => write!(f, "{error}"),
            Self::InvalidLocation(path) => {
                write!(f, "file {} is from an invalid location", path.display())
            }
            Self::AffinityOutOfRange {
                role,
                affinity,
                count,
            } => write!(
                f,
                "{role} affinity {affinity} must be less than cpuset count {count}"
            ),
            Self::WarningsAsErrors(count) => {
                write!(f, "cpuset warnings as errors, {count} warnings")
            }
            Self::NotFound(mount) => write!(
                f,
                "unable to find '{CPUSET_FILE}' in path '{}'",
                mount.display()
            ),
        }
    }
}

impl std::error::Error for CpusetError {}

/// Two errors are the same when they are the same *arm*: the payloads are a
/// path or an `io::Error`, and what a caller — or a test — acts on is which
/// refusal it got.
impl PartialEq for CpusetError {
    fn eq(&self, other: &Self) -> bool {
        std::mem::discriminant(self) == std::mem::discriminant(other)
    }
}

impl Eq for CpusetError {}

/// The CPUs a `cpulist` names, sorted and without duplicates.
///
/// The spelling is the kernel's: a number is a CPU, `a-b` is a range, and
/// commas separate. **A negative number is not a CPU** — it is how a range ends,
/// because that is what the reference's `strtol` walk leaves to the sign
/// (`aeron_cpuset.c:200-330`), and the arms below are that walk's: a comma with
/// no CPU before it is a leading comma, one with nothing after it is a trailing
/// comma, a range end before its start is refused, and anything that is not a
/// number, a comma or a newline is a non-numeric CPU.
///
/// # Errors
///
/// A [`CpusetError`] for each of those arms.
pub fn parse_cpulist(text: &str) -> Result<Vec<i32>, CpusetError> {
    let mut cpus: Vec<i32> = Vec::new();
    let mut rest = text;
    let mut ended_with_comma = false;

    loop {
        // `strtol` skips leading whitespace before the number it reads, which
        // is what makes a `cpulist` read from a file (newline and all) work.
        let trimmed = rest.trim_start_matches(|c: char| c.is_ascii_whitespace());

        let Some(first) = trimmed.chars().next() else {
            break;
        };

        let (negative, after_sign) = match first {
            '-' => (true, &trimmed[1..]),
            _ => (false, trimmed),
        };

        let digits = after_sign.chars().take_while(char::is_ascii_digit).count();

        if 0 == digits {
            // Nothing was parsed. A comma is a separator — and the one thing
            // that survives to the end of the walk, which is how a trailing
            // comma is caught — a newline is ignored, and anything else is not
            // part of a list.
            if ',' == first {
                if cpus.is_empty() {
                    return Err(CpusetError::LeadingComma);
                }

                ended_with_comma = true;
                rest = &trimmed[1..];
                continue;
            }

            if '\n' == first {
                rest = &trimmed[1..];
                continue;
            }

            return Err(CpusetError::NonNumeric);
        }

        let value: i64 = after_sign[..digits]
            .parse()
            .map_err(|_| CpusetError::NonNumeric)?;

        if !negative {
            cpus.push(i32::try_from(value).map_err(|_| CpusetError::NonNumeric)?);
        } else {
            // The end of a range. Its start is the CPU before it — and a
            // negative right after a comma, or before any CPU at all, has no
            // start to speak of.
            if cpus.is_empty() || ended_with_comma {
                return Err(CpusetError::NegativeCpu);
            }

            let end = i32::try_from(value).map_err(|_| CpusetError::NonNumeric)?;
            let start = *cpus.last().expect("a CPU is before a range end");

            if end < start {
                return Err(CpusetError::RangeEndBeforeStart);
            }

            for cpu in start + 1..=end {
                cpus.push(cpu);
            }
        }

        ended_with_comma = false;
        rest = &after_sign[digits..];
    }

    if cpus.is_empty() {
        return Err(CpusetError::Empty);
    }

    if ended_with_comma {
        return Err(CpusetError::TrailingComma);
    }

    cpus.sort_unstable();
    cpus.dedup();

    Ok(cpus)
}

/// The CPUs a driver's cgroup allows: the `cpulist` in the first
/// `cpuset.cpus.effective` found from the process's cgroup upwards.
///
/// `proc_cgroup_file` and `mount_root` are parameters rather than the constants
/// above so that a test can point them at a directory of its own, which is how
/// the reference tests this (`aeron_cpuset_test.cpp:80-165`).
///
/// # Errors
///
/// [`CpusetError::NotFound`] when no cgroup from the process's own to the root
/// has the file, [`CpusetError::InvalidLocation`] for a file that resolves
/// outside `/sys/` and outside the temporary directory, and the read and parse
/// errors beside those.
pub fn cgroup_read_v2(proc_cgroup_file: &Path, mount_root: &Path) -> Result<Vec<i32>, CpusetError> {
    let mut cgroup_path = read_cgroup_path(proc_cgroup_file)?;

    loop {
        let candidate = mount_root
            .join(cgroup_path.trim_start_matches('/'))
            .join(CPUSET_FILE);

        if candidate.is_file() {
            let validated = validate_path_root(&candidate)?;

            return parse_cpulist(&read_file(&validated)?);
        }

        if cgroup_path.is_empty() {
            return Err(CpusetError::NotFound(mount_root.to_owned()));
        }

        // Up one cgroup: the file may live in a parent, and the reference looks
        // there rather than giving up (`:390-410`).
        cgroup_path = match cgroup_path.rfind('/') {
            Some(index) => cgroup_path[..index].to_owned(),
            None => String::new(),
        };
    }
}

/// Where each of the driver's agents should run, as **CPU ids**, or `None` for
/// one that is left where the scheduler put it.
///
/// The reference keeps the same four numbers in its context — an index into the
/// cpuset that [`apply`] turns into a CPU id (`aeron_driver.c:1196-1207`) — and
/// every agent thread reads its own on start.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CpuAssignment {
    /// The conductor's CPU, which is the process's own thread under
    /// `manual_main_loop`.
    pub conductor: Option<i32>,
    /// The receiver's.
    pub receiver: Option<i32>,
    /// The sender's.
    pub sender: Option<i32>,
    /// The native resource agent's.
    pub native_resource_agent: Option<i32>,
}

/// What `aeron_driver_apply_cpuset_affinity` decides
/// (`aeron-driver/src/main/c/aeron_driver.c:1138-1209`).
///
/// With `aeron.driver.cpuset.affinity` off — which is the default — this reads
/// nothing and assigns nothing: the whole path is an operator's choice, and a
/// driver that never asked for it does not look at the machine's topology at
/// all.
///
/// With it on, in the reference's order: read the cpuset, run the three topology
/// checks (their warnings go to **stderr**, which is where the reference's
/// `fprintf(output, ...)` points when `aeronmd` passes `stderr`, `:1160-1178`),
/// refuse if `cpuset.warnings.as.errors` is set and there were any, and finally
/// turn each slot's position into a CPU id.
///
/// # Errors
///
/// [`CpusetError`] for a cgroup that cannot be read, an affinity position the
/// cpuset does not have, and the warnings-as-errors refusal.
pub fn apply(config: &crate::config::DriverConfig) -> Result<CpuAssignment, CpusetError> {
    if !config.cpuset_affinity {
        return Ok(CpuAssignment::default());
    }

    let cpus = cgroup_read_v2(Path::new(PROC_SELF_CGROUP), Path::new(CGROUP_MOUNT_V2))?;

    let root = Path::new(crate::topology::SYS_CPU_PATH);
    let mut warnings = crate::topology::check_alignment(root, &cpus)
        .map_err(|error| CpusetError::Io(io::Error::other(error.to_string())))?;

    for check in [
        crate::topology::check_die_locality
            as fn(&Path, &[i32]) -> Result<Vec<String>, crate::topology::TopologyError>,
        crate::topology::check_l3_locality,
    ] {
        let found = check(root, &cpus)
            .map_err(|error| CpusetError::Io(io::Error::other(error.to_string())))?;
        warnings.extend(found);
    }

    for warning in &warnings {
        // stderr, and on the clean path too: this is a diagnostic about the
        // machine the driver was given, and the reference writes it where an
        // operator will see it without asking.
        eprintln!("{warning}");
    }

    if config.cpuset_warnings_as_errors && !warnings.is_empty() {
        return Err(CpusetError::WarningsAsErrors(warnings.len()));
    }

    Ok(CpuAssignment {
        conductor: slot("conductor", config.conductor_cpu_affinity, &cpus)?,
        receiver: slot("receiver", config.receiver_cpu_affinity, &cpus)?,
        sender: slot("sender", config.sender_cpu_affinity, &cpus)?,
        native_resource_agent: slot(
            "aeron-md-nra",
            config.native_resource_agent_cpu_affinity,
            &cpus,
        )?,
    })
}

/// One slot's position in the cpuset as a CPU id, or `None` for `-1` — the
/// reference's own `-1 < affinity` test (`aeron_driver_context.c:3553`).
fn slot(role: &'static str, affinity: i32, cpus: &[i32]) -> Result<Option<i32>, CpusetError> {
    if affinity < 0 {
        return Ok(None);
    }

    let index = usize::try_from(affinity).unwrap_or(usize::MAX);
    let Some(cpu) = cpus.get(index) else {
        return Err(CpusetError::AffinityOutOfRange {
            role,
            affinity,
            count: cpus.len(),
        });
    };

    Ok(Some(*cpu))
}

/// The cgroup path this process is in, from `/proc/self/cgroup`'s v2 line.
///
/// The line is `id:controllers:path` and the v2 one is the line with an empty
/// controller list whose id is `0` (`aeron_cpuset.c:54-140`). The reference
/// reads only the **first** line; so does this, and a system whose first line
/// is a v1 controller line has no v2 cgroup to report.
fn read_cgroup_path(proc_cgroup_file: &Path) -> Result<String, CpusetError> {
    let data = read_file(proc_cgroup_file)?;
    let line = data.lines().next().unwrap_or_default();

    let mut fields = line.splitn(3, ':');
    let id = fields.next().unwrap_or_default();
    let controllers = fields.next().unwrap_or_default();
    let path = fields.next().unwrap_or_default();

    if "0" == id && controllers.is_empty() {
        return Ok(path.to_owned());
    }

    Err(CpusetError::NotFound(proc_cgroup_file.to_owned()))
}

/// The path a cgroup file must resolve to: under `/sys/`, or under the
/// temporary directory the reference's own tests build their fixtures in
/// (`aeron_cpuset.c:158-175`).
fn validate_path_root(path: &Path) -> Result<PathBuf, CpusetError> {
    let resolved = std::fs::canonicalize(path).map_err(CpusetError::Io)?;

    let temp = std::env::temp_dir();
    let temp = std::fs::canonicalize(&temp).unwrap_or(temp);

    if resolved.starts_with("/sys/") || resolved.starts_with(&temp) {
        return Ok(resolved);
    }

    Err(CpusetError::InvalidLocation(path.to_owned()))
}

/// Read a file to a string.
fn read_file(path: &Path) -> Result<String, CpusetError> {
    std::fs::read_to_string(path).map_err(CpusetError::Io)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A directory of its own in the system temp directory, removed on drop —
    /// under the temp directory because that is one of the two places
    /// [`validate_path_root`] accepts, and the reference's tests are built the
    /// same way.
    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
            let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let path =
                std::env::temp_dir().join(format!("deepmsg-cpuset-{}-{n}", std::process::id()));
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

    /// `shouldParseCpulist` (`aeron_cpuset_test.cpp:170-185`), case for case.
    #[test]
    fn a_cpulist_is_parsed_the_way_the_kernel_writes_it() {
        for (text, expected) in [
            ("0", vec![0]),
            ("7", vec![7]),
            ("3,1,2", vec![1, 2, 3]),
            ("0,0,1", vec![0, 1]),
            ("0-3", vec![0, 1, 2, 3]),
            ("4", vec![4]),
            ("0,2,4-6", vec![0, 2, 4, 5, 6]),
            ("0-2,6", vec![0, 1, 2, 6]),
        ] {
            assert_eq!(Ok(expected), parse_cpulist(text), "{text}");
        }

        assert_eq!(2048, parse_cpulist("0-2047").expect("a range").len());
    }

    /// `shouldNotParseCpulist` (`:187-217`), case for case — the arms are the
    /// reference's, word for word.
    #[test]
    fn a_cpulist_that_is_wrong_is_refused_with_the_references_word() {
        for (text, expected) in [
            ("", CpusetError::Empty),
            ("0,", CpusetError::TrailingComma),
            (",0", CpusetError::LeadingComma),
            ("-1", CpusetError::NegativeCpu),
            ("0,-1", CpusetError::NegativeCpu),
            ("a", CpusetError::NonNumeric),
            ("0-a", CpusetError::NonNumeric),
            ("4-2", CpusetError::RangeEndBeforeStart),
        ] {
            assert_eq!(Err(expected), parse_cpulist(text), "{text}");
        }
    }

    /// `shouldReadV2Cgroups` (`:80-111`): the process's own cgroup has the
    /// file, and the list comes back sorted and expanded.
    #[test]
    fn the_effective_cpus_come_from_the_processes_own_cgroup() {
        let dir = TempDir::new();
        dir.write("proc-cgroup", "0::/user.slice/user-1000.slice\n");
        dir.write(
            "cgroup/user.slice/user-1000.slice/cpuset.cpus.effective",
            "5-10\n",
        );

        let cpus = cgroup_read_v2(&dir.path().join("proc-cgroup"), &dir.path().join("cgroup"))
            .expect("the cgroup is read");

        assert_eq!(vec![5, 6, 7, 8, 9, 10], cpus);
    }

    /// `shouldReadV2CgroupsInParent` (`:113-146`): the file is in a parent
    /// cgroup, and the search walks up to it.
    #[test]
    fn the_search_walks_up_to_a_parent_cgroup() {
        let dir = TempDir::new();
        dir.write("proc-cgroup", "0::/user.slice/user-1000.slice\n");
        dir.write("cgroup/user.slice/cpuset.cpus.effective", "5-10\n");

        let cpus = cgroup_read_v2(&dir.path().join("proc-cgroup"), &dir.path().join("cgroup"))
            .expect("the parent's cgroup is read");

        assert_eq!(vec![5, 6, 7, 8, 9, 10], cpus);
    }

    /// `shouldErrorIfNotFoundReadV2Cgroups` (`:148-165`): no cgroup up the path
    /// has the file, and that is an error rather than an empty list.
    #[test]
    fn a_cgroup_tree_with_no_effective_cpus_is_an_error() {
        let dir = TempDir::new();
        dir.write("proc-cgroup", "0::/user.slice/user-1000.slice\n");

        let error = cgroup_read_v2(&dir.path().join("proc-cgroup"), &dir.path().join("cgroup"))
            .expect_err("there is no such file");

        assert!(matches!(error, CpusetError::NotFound(_)), "{error}");
    }

    /// With `aeron.driver.cpuset.affinity` off — which is the default — the
    /// whole path is skipped: no cgroup is read, no topology is looked at, and
    /// no agent is pinned (`aeron_driver.c:1140-1143`).
    #[test]
    fn a_driver_that_did_not_ask_for_affinity_reads_nothing() {
        let config = crate::config::DriverConfig::default();

        assert!(!config.cpuset_affinity, "the default");
        assert_eq!(
            Ok(CpuAssignment::default()),
            apply(&config),
            "and nothing is assigned"
        );
    }

    /// The path check is the reference's: a file that resolves outside `/sys/`
    /// and outside the temporary directory is refused even when it exists.
    #[test]
    fn a_cgroup_file_that_is_not_where_it_should_be_is_refused() {
        let validated = validate_path_root(Path::new("/etc/hostname"));

        assert!(
            matches!(validated, Err(CpusetError::InvalidLocation(_))),
            "{validated:?}"
        );
    }
}
