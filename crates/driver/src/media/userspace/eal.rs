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
//! * **One of `--in-memory` / `--no-shconf`.** Both keep EAL from leaving
//!   runtime state on the machine, and they cannot both be given: `--no-huge`
//!   implies the legacy memory layout, which `--in-memory` refuses
//!   (`EAL: Option --legacy-mem is not compatible with --in-memory`). So the
//!   pair is chosen by whether the operator asked for huge pages — `--no-huge`
//!   in the arguments means `--no-shconf`, and its absence means `--in-memory`.
//!
//! The operator's arguments are carried through untouched and in order, after
//! `argv[0]`. A setting that names code the driver must set itself — an lcore
//! argument, or one of the three above — is refused where the configuration is
//! read rather than merged here ([`crate::config::ConfigError::DpdkEalArgument`]),
//! because a silent override of the cpuset arithmetic is exactly the failure
//! the first bullet is about.
//!
//! Nothing here needs DPDK to compile, and that is deliberate: it is the half
//! of the media's startup that can be judged without an EAL, a port or a
//! privilege, so it is judged in the default gate rather than in the `dpdk`
//! job.

use crate::config::DpdkSettings;

/// The name the EAL is given as `argv[0]`, which it uses for its own
/// diagnostics and for the default `file-prefix`.
pub const PROGRAM: &str = "deepmsg-driver";

/// Keep EAL's telemetry thread and endpoint out of the process.
pub const NO_TELEMETRY: &str = "--no-telemetry";

/// Reserve memory with `mmap` instead of huge pages, for a machine without
/// them or a device that does no DMA.
pub const NO_HUGE: &str = "--no-huge";

/// Do not use a shared-memory configuration file. What pairs with
/// [`NO_HUGE`], and what [`IN_MEMORY`] cannot be given with.
pub const NO_SHCONF: &str = "--no-shconf";

/// Keep EAL's runtime state in memory. What pairs with huge pages.
pub const IN_MEMORY: &str = "--in-memory";

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
pub fn arguments(settings: &DpdkSettings, cpus: &[i32]) -> Result<Vec<String>, EalArgumentError> {
    if cpus.is_empty() {
        return Err(EalArgumentError::NoCpus);
    }

    let operator: Vec<&str> = settings.eal_args.split_whitespace().collect();

    let mut arguments = Vec::with_capacity(operator.len() + 4);
    arguments.push(PROGRAM.to_owned());
    arguments.extend(operator.iter().map(|argument| (*argument).to_owned()));
    arguments.push(NO_TELEMETRY.to_owned());
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
            let built = arguments(&settings(args), &[2, 3, 4, 5]).expect("a cpuset");
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
        let built = arguments(&settings(""), &[0]).expect("a cpuset");

        assert!(built.contains(&NO_TELEMETRY.to_owned()));
    }

    /// Three settings, two arguments: which memory flag is added is decided by
    /// whether the operator asked for huge pages, and both are never there.
    #[test]
    fn the_two_memory_flags_are_never_given_together() {
        let with_huge_pages = arguments(&settings("-a 0000:03:00.0"), &[0]).expect("a cpuset");
        assert!(with_huge_pages.contains(&IN_MEMORY.to_owned()));
        assert!(!with_huge_pages.contains(&NO_SHCONF.to_owned()));

        let without = arguments(&settings("--no-huge -m 512"), &[0]).expect("a cpuset");
        assert!(without.contains(&NO_SHCONF.to_owned()));
        assert!(
            !without.contains(&IN_MEMORY.to_owned()),
            "`--no-huge` implies legacy memory, which `--in-memory` refuses: {without:?}"
        );
    }

    /// The operator's arguments arrive as they were written, in order, after
    /// the program name — a `--vdev` line is theirs and this is not the place
    /// to guess at it.
    #[test]
    fn the_operators_own_arguments_are_carried_through() {
        let built = arguments(
            &settings("--vdev=net_pcap0,rx_pcap=/tmp/cap -a 0000:03:00.0 --file-prefix=dp"),
            &[0],
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
        assert_eq!(Err(EalArgumentError::NoCpus), arguments(&settings(""), &[]));
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
        let built = arguments(&settings(""), &mine).expect("a cpuset");

        assert!(
            built.contains(&format!("--lcores=0@({})", cpu_list(&mine))),
            "this process runs on {mine:?}, so that is what the EAL is told: {built:?}"
        );
    }
}
