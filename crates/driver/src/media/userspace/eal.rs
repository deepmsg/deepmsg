//! What the DPDK media starts its EAL with, and the CPUs it may use.
//!
//! Three of the arguments are the driver's rather than the operator's, and
//! each one is here because leaving it out has a measured consequence
//! (§4.7 of the design; the D0 probe's `run3-lcores.log`, eight cases):
//!
//! * **`--lcores=0@(<the process's own cpuset>)`, always.** With no lcore
//!   argument at all the EAL starts a `dpdk-worker` pinned to *every* CPU the
//!   process may use — seven of them on an eight-CPU machine — and pins the
//!   main thread to the first. Writing the set down instead (`--lcores=0@(0-7)`)
//!   widens the main thread and `dpdk-intr` past an outer `taskset`. Only the
//!   process's *own* affinity at init time leaves both where they were, which
//!   is why the list is read from the kernel here and not from a setting.
//! * **`--no-telemetry`, always.** EAL's telemetry endpoint is a thread and a
//!   socket this driver has no use for.
//! * **`--log-level=1`, always.** The EAL and the PMDs write their own
//!   diagnostics to **stderr**, which the reference's test harness asserts is
//!   empty after every test method — so a driver process that lets them
//!   through fails those tests for a reason that is not theirs. [`LOG_LEVEL`]
//!   has the measurement and what it costs.
//! * **One of `--in-memory` / `--no-shconf`.** Both keep EAL from leaving
//!   runtime state on the machine, and they cannot both be given: `--no-huge`
//!   implies the legacy memory layout, which `--in-memory` refuses
//!   (`EAL: Option --legacy-mem is not compatible with --in-memory`). So the
//!   pair is chosen by whether the operator asked for huge pages — `--no-huge`
//!   in the arguments means `--no-shconf`, and its absence means `--in-memory`.
//!
//! The operator's arguments are carried through untouched and in order, after
//! `argv[0]`. A setting that names code the driver must set itself — an lcore
//! argument, or one of the four above — is refused where the configuration is
//! read rather than merged here ([`crate::config::ConfigError::DpdkEalArgument`]),
//! because a silent override of the cpuset arithmetic is exactly the failure
//! the first bullet is about, and a silent override of the log level is a
//! driver that fails the reference's harness with no way to tell why.
//!
//! Nothing here needs DPDK to compile, and that is deliberate: it is the half
//! of the media's startup that can be judged without an EAL, a port or a
//! privilege, so it is judged in the default gate rather than in the `dpdk`
//! job.

use crate::config::DpdkSettings;

/// The name the EAL is given as `argv[0]`, which it uses for its own
/// diagnostics and for the default `file-prefix`.
pub const PROGRAM: &str = "deepmsg-driver";

/// `-a`: the devices the EAL is allowed to probe. The driver adds it when it
/// picked a port out of a list (`deepmsg.dpdk.ports`).
const ALLOW: &str = "-a";

/// Keep EAL's telemetry thread and endpoint out of the process.
const NO_TELEMETRY: &str = "--no-telemetry";

/// Reserve memory with `mmap` instead of huge pages, for a machine without
/// them or a device that does no DMA.
pub const NO_HUGE: &str = "--no-huge";

/// Do not use a shared-memory configuration file. What pairs with
/// [`NO_HUGE`], and what [`IN_MEMORY`] cannot be given with.
pub const NO_SHCONF: &str = "--no-shconf";

/// Keep EAL's runtime state in memory. What pairs with huge pages.
pub const IN_MEMORY: &str = "--in-memory";

/// Quiet the EAL and the PMDs. What makes a driver's **stderr** empty, which
/// is a contract rather than a preference: the reference's own test harness
/// asserts the driver process's stderr is zero bytes after **every** test
/// method (`aeron-test-support/src/main/java/io/aeron/test/MediaDriverTestUtil.java:128-131`,
/// reached from `:95-99`), and the C harness this driver is launched by is the
/// reference's own. A DPDK driver that printed EAL's five startup lines would
/// fail every one of those tests for a reason that has nothing to do with them.
///
/// **Level 1, measured**: a driver started on `--no-huge --vdev=net_null0`
/// writes **257 bytes** of `EAL: Detected CPU lcores: …` and five lines like
/// it at the default level, and none at this one. `lib.eal:1` also writes
/// none, and is not enough — a PMD reports at INFO and ERR too, and a PMD's
/// stderr is really what this is about. **0 is not a level**: the EAL answers
/// `Invalid log level: 0` and that line goes to stderr as well.
///
/// What it costs is the PMD's own error lines, and nothing this build needed:
/// the shim never reports through logs — it answers a return code and
/// `rte_errno`, which `deepmsg_dpdk::Error` carries
/// (`crates/dpdk/src/lib.rs`) — so a line lost here is a line that was never
/// read.
pub const LOG_LEVEL: &str = "--log-level=1";

/// Why the EAL's arguments could not be built.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EalArgumentError {
    /// The process may run on no CPU, so there is no lcore set to give.
    NoCpus,
}

impl std::fmt::Display for EalArgumentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoCpus => f.write_str(
                "the process may run on no CPU, so there is no lcore set to start the EAL with",
            ),
        }
    }
}

impl std::error::Error for EalArgumentError {}

/// The arguments `rte_eal_init` is called with, `argv[0]` first.
///
/// `cpus` is the calling thread's affinity **before** init
/// ([`crate::sys::current_thread_affinity`]), which is the only set that leaves
/// the process where its cgroup and its `taskset` put it.
///
/// # Errors
///
/// [`EalArgumentError::NoCpus`] when the process may run nowhere, which would
/// otherwise be an EAL started with an empty lcore set.
pub fn arguments(
    settings: &DpdkSettings,
    cpus: &[i32],
    picked: Option<&str>,
) -> Result<Vec<String>, EalArgumentError> {
    if cpus.is_empty() {
        return Err(EalArgumentError::NoCpus);
    }

    let operator: Vec<&str> = settings.eal_args.split_whitespace().collect();

    let mut arguments = Vec::with_capacity(operator.len() + 7);
    arguments.push(PROGRAM.to_owned());

    // **The port the driver picked, when it picked one.** The EAL probes every
    // device it is allowed to and holds each of them until the process ends, so
    // a driver that allow-listed its whole list of candidates would take all of
    // them — and the next driver would find none. The allow-list therefore
    // names the one this process claimed, and it is the driver's own argument:
    // the configuration refuses an operator's `-a` when a list is given, for
    // the same reason it refuses their `--lcores`.
    if let Some(port) = picked {
        arguments.push(ALLOW.to_owned());
        arguments.push(port.to_owned());
    }

    arguments.extend(operator.iter().map(|argument| (*argument).to_owned()));
    arguments.push(NO_TELEMETRY.to_owned());
    arguments.push(LOG_LEVEL.to_owned());
    arguments.push(format!("--lcores=0@({})", cpu_list(cpus)));
    arguments.push(
        if operator.contains(&NO_HUGE) {
            NO_SHCONF
        } else {
            IN_MEMORY
        }
        .to_owned(),
    );

    Ok(arguments)
}

/// A set of CPUs in the kernel's own `cpulist` spelling — `0-3,7`, which is
/// what `--lcores`, `taskset -c` and `cpuset.cpus` all take.
///
/// Sorted, deduplicated, and consecutive runs folded into ranges: the list
/// travels into the EAL's argument parser and into anything a reader compares
/// it against, and two spellings of one set are two things to keep in step.
pub fn cpu_list(cpus: &[i32]) -> String {
    let mut sorted = cpus.to_vec();
    sorted.sort_unstable();
    sorted.dedup();

    let mut parts: Vec<String> = Vec::new();
    let mut index = 0;

    while index < sorted.len() {
        let first = sorted[index];
        let mut last = first;

        while index + 1 < sorted.len() && sorted[index + 1] == last + 1 {
            index += 1;
            last = sorted[index];
        }

        parts.push(if first == last {
            first.to_string()
        } else {
            format!("{first}-{last}")
        });

        index += 1;
    }

    parts.join(",")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings(eal_args: &str) -> DpdkSettings {
        DpdkSettings {
            eal_args: eal_args.to_owned(),
            ..DpdkSettings::default()
        }
    }

    /// The one argument whose absence the D0 probe measured: without a lcore
    /// set the EAL pins a worker to every CPU the process may use.
    #[test]
    fn the_eal_is_always_told_which_lcores_it_may_use() {
        for args in ["", "--no-huge -m 512", "--vdev=net_pcap0,rx_pcap=/tmp/x"] {
            let built = arguments(&settings(args), &[2, 3, 4, 5], None).expect("a cpuset");
            let lcores: Vec<&String> = built
                .iter()
                .filter(|argument| argument.starts_with("--lcores="))
                .collect();

            assert_eq!(
                1,
                lcores.len(),
                "exactly one lcore set, whatever the operator asked for: {built:?}"
            );
            assert_eq!("--lcores=0@(2-5)", lcores[0], "and it is the process's own");
        }
    }

    /// The driver says so itself, so an operator cannot have said otherwise —
    /// and the configuration refuses the attempt before it gets here.
    #[test]
    fn telemetry_is_never_started() {
        let built = arguments(&settings(""), &[0], None).expect("a cpuset");

        assert!(built.contains(&NO_TELEMETRY.to_owned()));
    }

    /// And the log level is always level 1, because what it buys is an empty
    /// **stderr** — which the reference's own harness asserts after every test
    /// method, so a driver that lets EAL's five startup lines through fails
    /// tests that have nothing to do with it.
    ///
    /// Whatever the operator asked for, which is the half that matters: the
    /// configuration refuses a `--log-level` in `deepmsg.dpdk.eal.args`, and
    /// this is the other door — the level the driver appends is not decided by
    /// anything the operator wrote.
    #[test]
    fn the_eal_is_always_told_to_keep_quiet() {
        for args in ["", "--no-huge -m 512", "-a 0000:03:00.0 --in-memory"] {
            let built = arguments(&settings(args), &[0], None).expect("a cpuset");

            assert!(
                built.contains(&LOG_LEVEL.to_owned()),
                "level 1, whatever the operator asked for: {built:?}"
            );
            assert_eq!(
                1,
                built
                    .iter()
                    .filter(|argument| argument.starts_with("--log-level"))
                    .count(),
                "and exactly one of them: {built:?}"
            );
        }
    }

    /// Three settings, two arguments: which memory flag is added is decided by
    /// whether the operator asked for huge pages, and both are never there.
    #[test]
    fn the_two_memory_flags_are_never_given_together() {
        let with_huge_pages =
            arguments(&settings("-a 0000:03:00.0"), &[0], None).expect("a cpuset");
        assert!(with_huge_pages.contains(&IN_MEMORY.to_owned()));
        assert!(!with_huge_pages.contains(&NO_SHCONF.to_owned()));

        let without = arguments(&settings("--no-huge -m 512"), &[0], None).expect("a cpuset");
        assert!(without.contains(&NO_SHCONF.to_owned()));
        assert!(
            !without.contains(&IN_MEMORY.to_owned()),
            "`--no-huge` implies legacy memory, which `--in-memory` refuses: {without:?}"
        );
    }

    /// **The port the driver picked is the only one the EAL is allowed to
    /// probe**, and the rule is one `-a` rather than one per candidate.
    ///
    /// It has to be exactly one: the EAL takes every device it probes and holds
    /// it until the process ends, so a driver that allowed its whole list would
    /// take every port on it and the next driver would find none — which is the
    /// failure this whole slice is about. `deepmsg.dpdk.ports` is a list of
    /// candidates to *choose* from, and the choosing is done before this.
    #[test]
    fn the_port_the_driver_picked_is_the_only_one_it_allows() {
        let many = DpdkSettings {
            ports: vec![
                "0000:03:00.0".to_owned(),
                "0000:03:00.1".to_owned(),
                "0000:01:00.0".to_owned(),
            ],
            ..settings("")
        };

        let built = arguments(&many, &[0], Some("0000:03:00.1")).expect("a cpuset");

        assert_eq!(
            1,
            built.iter().filter(|argument| *argument == "-a").count(),
            "one allow, whatever the list held: {built:?}"
        );
        assert!(
            built.windows(2).any(|pair| pair == ["-a", "0000:03:00.1"]),
            "and it names the port that was picked, not the first candidate: {built:?}"
        );
        for candidate in ["0000:03:00.0", "0000:01:00.0"] {
            assert!(
                !built.contains(&candidate.to_owned()),
                "{candidate} was not picked and must not be allowed: {built:?}"
            );
        }
    }

    /// The operator's arguments arrive as they were written, in order, after
    /// the program name — a `--vdev` line is theirs and this is not the place
    /// to guess at it.
    #[test]
    fn the_operators_own_arguments_are_carried_through() {
        let built = arguments(
            &settings("--vdev=net_pcap0,rx_pcap=/tmp/cap -a 0000:03:00.0 --file-prefix=dp"),
            &[0],
            None,
        )
        .expect("a cpuset");

        assert_eq!(PROGRAM, built[0], "argv[0] is the program's own name");
        assert_eq!(
            &[
                "--vdev=net_pcap0,rx_pcap=/tmp/cap",
                "-a",
                "0000:03:00.0",
                "--file-prefix=dp"
            ],
            &built[1..5]
        );
    }

    /// A process with no CPU to run on is refused rather than started with an
    /// empty set, which is the one thing the EAL would have to invent an
    /// answer for.
    #[test]
    fn a_process_with_no_cpus_has_no_lcore_set() {
        assert_eq!(
            Err(EalArgumentError::NoCpus),
            arguments(&settings(""), &[], None)
        );
    }

    /// The list a `--lcores` argument carries is the kernel's own spelling.
    #[test]
    fn a_cpu_list_folds_consecutive_cpus_into_ranges() {
        assert_eq!("0-3,7", cpu_list(&[0, 1, 2, 3, 7]));
        assert_eq!("5", cpu_list(&[5]));
        assert_eq!("0,2-4,9-10", cpu_list(&[9, 10, 4, 2, 3, 0]));
        assert_eq!(
            "0-2",
            cpu_list(&[2, 1, 0, 1]),
            "unsorted, and with a repeat"
        );
        assert_eq!("", cpu_list(&[]));
    }

    /// The set the driver passes is the process's, read from the kernel — and
    /// this test reads it from the kernel too rather than trusting the shape.
    #[test]
    fn the_lcore_set_is_the_one_the_process_already_has() {
        let mine = crate::sys::current_thread_affinity().expect("this thread's mask");
        let built = arguments(&settings(""), &mine, None).expect("a cpuset");

        assert!(
            built.contains(&format!("--lcores=0@({})", cpu_list(&mine))),
            "this process runs on {mine:?}, so that is what the EAL is told: {built:?}"
        );
    }
}
