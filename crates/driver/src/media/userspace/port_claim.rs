//! **One port out of several, claimed before the EAL is told about any of
//! them.**
//!
//! A kernel-bypass port is exclusive: one process drives it and the next one to
//! ask is refused — by the IOMMU group, which belongs to one process at a time.
//! That is the whole reason this module exists, and the shape it has to have
//! follows from *when* the answer is needed: **before `rte_eal_init`**, because
//! the EAL takes every device it probes and holds it until the process ends. A
//! driver that allow-lists four candidates and then picks one has already taken
//! all four, and the next driver finds none of them.
//!
//! So the choice is made here, in the kernel's terms: each candidate is a lock
//! file, and the first one this process can take is the port it will drive. The
//! lock is `flock`, which the kernel releases when the file is closed **or the
//! process dies** — a driver killed mid-run therefore gives its port back
//! rather than wedging it (measured: a `SIGKILL`ed holder's lock is gone a
//! moment later, and two descriptors opened separately on one file are
//! mutually exclusive even inside a single process, which is what makes the
//! criterion below a real test rather than a formality).
//!
//! # What a candidate is, and what it is claimed *by*
//!
//! A PCI address (`0000:03:00.0`) is claimed by its **IOMMU group**, not by the
//! address: two functions of one card can share a group, and a group is given
//! to one process whoever asks — a claim on `01:00.0` has to refuse a claim on
//! `01:00.1` when the two are in group 2, or both drivers start and both fail
//! inside the EAL (which is the shape this was written after: `VFIO group is
//! not viable! Not all devices in IOMMU group bound to VFIO or unbound`, met
//! while only one of a pair was bound). What shares a group therefore counts as
//! **one** candidate however many addresses it has.
//!
//! A device with no IOMMU group — every virtual device, and anything that is
//! not a PCI address at all — is claimed by its name, which is the only
//! identity it has.

use std::path::{Path, PathBuf};

use crate::sys::port_lock::LockFile;

/// Where the lock files live.
///
/// In memory, beside this build's aeron directories, and **never removed**: a
/// file that is unlinked while somebody holds its lock leaves the holder with a
/// lock on an inode nobody else can open, so the next claimant would create a
/// second file with the same name and take a lock that excludes nobody.
const LOCK_DIR: &str = "/dev/shm";

/// The root a PCI address's IOMMU group is read from.
const PCI_DEVICES: &str = "/sys/bus/pci/devices";

/// A candidate that nothing else on this machine is driving.
///
/// Holding one of these is what says "this port is mine"; dropping it gives the
/// port back, and so does dying.
#[derive(Debug)]
pub struct Claim {
    port: String,
    /// The lock itself, held for as long as this claim lives.
    _lock: LockFile,
}

impl Claim {
    /// The candidate this claim took, spelled as the configuration spelled it.
    pub fn port(&self) -> &str {
        &self.port
    }

    /// What the port is claimed *by*, which is what tells two candidates apart
    /// in a log: an IOMMU group for a PCI address, the name for anything else.
    pub fn claimed_by(&self) -> String {
        claimed_by(&self.port)
    }
}

/// Why no candidate could be taken.
///
/// **Two reasons, and they are not the same one.** A port somebody else is
/// driving is the ordinary answer and the reason the list exists; a port whose
/// lock file this process cannot open is a fact about *this* process and the
/// machine, and a driver that reported it as "already being driven" would be
/// naming a holder that does not exist — which is what the first round of this
/// slice's own suite run did, from a lock file an earlier run had left owned by
/// another user, and it read exactly like a machine with four busy ports.
#[derive(Debug)]
pub enum NoPort {
    /// Every candidate is being driven by another process.
    AllDriven {
        /// Every candidate that was tried, in the order they were given.
        candidates: Vec<String>,
    },
    /// A candidate's lock file could not be opened or locked at all.
    Unusable {
        /// The candidate it was.
        candidate: String,
        /// What the system call said, which is the only thing that can say
        /// whether this is a permission, a filesystem or something else again.
        reason: String,
    },
}

impl std::fmt::Display for NoPort {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AllDriven { candidates } => write!(
                f,
                "every port this driver was offered is already being driven by \
                 another process: {}. A kernel-bypass port carries one process at \
                 a time (`deepmsg.dpdk.ports`)",
                candidates.join(", ")
            ),
            Self::Unusable { candidate, reason } => write!(
                f,
                "`{candidate}` cannot be claimed at all: {reason}. This is not \
                 another driver holding it — a lock that cannot be opened is a \
                 port this process cannot have (`deepmsg.dpdk.ports`)"
            ),
        }
    }
}

impl std::error::Error for NoPort {}

/// Take the first candidate nothing else is driving.
///
/// The order is the configuration's: a driver that names its ports in a
/// particular order gets the first of them that is free, which is what makes
/// "give every driver the same list" a way to spread them out.
///
/// # Errors
///
/// [`NoPort`] when no candidate can be taken, saying which of the two reasons
/// it was — held, or unusable — rather than running them together.
pub fn claim(candidates: &[String]) -> Result<Claim, NoPort> {
    let mut unusable = None;

    for candidate in candidates {
        match claim_one(candidate) {
            Ok(Some(claim)) => return Ok(claim),
            Ok(None) => {}
            // Kept, not raised: the next candidate may be perfectly good, and
            // this is only worth saying when none of them works.
            Err(reason) => unusable = unusable.or(Some((candidate.clone(), reason))),
        }
    }

    Err(match unusable {
        Some((candidate, reason)) => NoPort::Unusable { candidate, reason },
        None => NoPort::AllDriven {
            candidates: candidates.to_vec(),
        },
    })
}

/// One candidate, or `None` when somebody else has it.
///
/// A lock that could not be taken at all — a directory this process cannot
/// write, a path that is not there — is treated as "not this one": the next
/// candidate is tried, and a driver that can claim none of them says so
/// ([`NothingFree`]) rather than starting on a port it does not have.
fn claim_one(candidate: &str) -> Result<Option<Claim>, String> {
    let lock = crate::sys::port_lock::try_lock(&lock_path(candidate))
        .map_err(|error| error.to_string())?;

    Ok(lock.map(|lock| Claim {
        port: candidate.to_owned(),
        _lock: lock,
    }))
}

/// The file a candidate is claimed under.
fn lock_path(candidate: &str) -> PathBuf {
    Path::new(LOCK_DIR).join(format!("deepmsg-dpdk-{}.lock", claimed_by(candidate)))
}

/// What a candidate is claimed by: its IOMMU group, or its own name.
fn claimed_by(candidate: &str) -> String {
    match iommu_group(candidate) {
        Some(group) => format!("group-{group}"),
        // The name is the only identity a virtual device has, and the only one
        // a device the kernel does not describe by group has either. Reduced to
        // characters a file name can hold: what is inside the file means
        // nothing, but its *name* has to be one this side can open twice.
        None => format!(
            "port-{}",
            candidate
                .chars()
                .map(|character| {
                    if character.is_ascii_alphanumeric() || character == '.' || character == '-' {
                        character
                    } else {
                        '-'
                    }
                })
                .collect::<String>()
        ),
    }
}

/// The IOMMU group a PCI address belongs to, or `None` when the machine does
/// not describe it by one.
///
/// `/sys/bus/pci/devices/<address>/iommu_group` is a symlink whose target's
/// last component is the group's number. A device with no IOMMU — every virtual
/// device, and a machine without one — has no such link, and that is not an
/// error: it is a candidate claimed by name instead.
fn iommu_group(candidate: &str) -> Option<u32> {
    let link = Path::new(PCI_DEVICES).join(candidate).join("iommu_group");
    let target = std::fs::read_link(link).ok()?;

    target.file_name()?.to_str()?.parse().ok()
}

/// Whether a path can be opened for writing, which is what a lock file needs.
///
/// Not used by the claim itself — it is here so a test can ask the same
/// question the claim asks without taking a lock to answer it.
#[cfg(test)]
pub(crate) fn lock_directory_is_writable() -> std::io::Result<()> {
    crate::sys::port_lock::try_lock(&Path::new(LOCK_DIR).join("deepmsg-dpdk-writable-probe.lock"))
        .map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A candidate no other test and no machine state can hold: the process id
    /// is this test binary's own.
    fn ours(name: &str) -> String {
        format!("test-{name}-{}", std::process::id())
    }

    /// **Two candidates that share nothing are both takeable**, which is what
    /// makes a list of ports a way to run two drivers on one machine.
    #[test]
    fn two_candidates_that_are_not_the_same_port_are_both_claimed() {
        let first = claim(&[ours("a")]).expect("the first is free");
        let second = claim(&[ours("b")]).expect("the second is free");

        assert_eq!(ours("a"), first.port());
        assert_eq!(ours("b"), second.port());
    }

    /// **And one of them is not takeable twice** — the whole point. Two
    /// descriptors opened separately on one file are two open file
    /// descriptions, which `flock` treats as two claimants even inside one
    /// process (measured before this was written, because a lock that a
    /// process can take twice would be no lock at all).
    #[test]
    fn a_port_that_is_taken_is_not_taken_again() {
        let held = claim(&[ours("c")]).expect("the first claim takes it");

        let refused = claim(&[ours("c")]).expect_err("the second is refused");
        assert!(
            matches!(&refused, NoPort::AllDriven { candidates } if *candidates == vec![ours("c")]),
            "and it says which: {refused}"
        );
        assert!(
            refused.to_string().contains("already being driven"),
            "and why: {refused}"
        );

        drop(held);
        claim(&[ours("c")]).expect("and the port comes back when the claim goes");
    }

    /// The list is tried **in order**: a driver that names four ports takes the
    /// first free one, so giving two drivers the same list gives them different
    /// ports.
    #[test]
    fn the_first_free_candidate_is_the_one_taken() {
        let _held = claim(&[ours("first")]).expect("the first candidate");

        let next = claim(&[ours("first"), ours("second"), ours("third")]).expect("the second");

        assert_eq!(
            ours("second"),
            next.port(),
            "the first is being driven, so the second is the one this driver gets"
        );
    }

    /// Every candidate held is an error that names them, rather than a driver
    /// that starts on a port it does not have.
    #[test]
    fn nothing_free_is_said_rather_than_guessed() {
        let _one = claim(&[ours("only")]).expect("the only candidate");

        let refused = claim(&[ours("only")]).expect_err("nothing is free");
        assert!(refused.to_string().contains(&ours("only")), "{refused}");
        assert!(
            matches!(refused, NoPort::AllDriven { .. }),
            "a held port is the ordinary answer"
        );
    }

    /// **A lock file that cannot be opened is not a port somebody else has**,
    /// and the two answers are told apart.
    ///
    /// This is the defect the first run of this slice's own suite found, in
    /// this build: an earlier run under another user had left the lock files
    /// owned by root and mode 644, so every driver in the class was refused —
    /// and the message named a holder that did not exist, on a machine with
    /// four idle ports. A lock file this process cannot open is a fact about
    /// this process; a port another process is driving is a fact about that
    /// one, and a driver has to be able to tell which it is looking at.
    #[test]
    fn a_lock_file_that_cannot_be_opened_is_not_a_port_somebody_has() {
        let candidate = ours("unusable");
        let path = lock_path(&candidate);
        std::fs::create_dir_all(&path).expect("a directory where the lock file should be");

        let refused =
            claim(std::slice::from_ref(&candidate)).expect_err("a directory is not a lock file");

        assert!(
            matches!(&refused, NoPort::Unusable { candidate: named, .. } if *named == candidate),
            "unusable, and it says which candidate: {refused}"
        );
        assert!(
            !refused.to_string().contains("already being driven"),
            "and not the holder that does not exist: {refused}"
        );

        let _ = std::fs::remove_dir_all(&path);
    }

    /// A candidate with no IOMMU group is claimed **by its name** — which is
    /// every virtual device, and anything that is not a PCI address.
    #[test]
    fn a_candidate_with_no_group_is_claimed_by_name() {
        let candidate = ours("vdev");

        assert_eq!(
            format!("port-{candidate}"),
            claimed_by(&candidate),
            "no sysfs entry, so the name is the identity"
        );
        assert_eq!(
            Path::new(LOCK_DIR).join(format!("deepmsg-dpdk-port-{candidate}.lock")),
            lock_path(&candidate)
        );
    }

    /// A **real** PCI address is claimed by its group, and the group is read
    /// from the machine rather than assumed. Skipped where there is no such
    /// device, which is every machine this runs on in CI.
    #[test]
    fn a_pci_address_is_claimed_by_its_iommu_group() {
        let Some(candidate) = std::fs::read_dir(PCI_DEVICES)
            .ok()
            .into_iter()
            .flatten()
            .flatten()
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .find(|name| iommu_group(name).is_some())
        else {
            return;
        };

        let group = iommu_group(&candidate).expect("the group it was chosen for");
        assert_eq!(format!("group-{group}"), claimed_by(&candidate));
        assert!(
            iommu_group(&candidate).is_some(),
            "and the group is the machine's own answer, not a default"
        );
    }

    /// The lock directory is writable, which is the one thing this needs from
    /// the machine and the one way it can fail silently otherwise.
    #[test]
    fn the_lock_directory_can_be_written() {
        lock_directory_is_writable().expect("a lock file can be made in /dev/shm");
    }
}
