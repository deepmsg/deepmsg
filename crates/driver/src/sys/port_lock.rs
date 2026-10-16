//! An exclusive claim on a file — which is how a port is claimed *before* the
//! EAL is told about it.
//!
//! `flock` and nothing else, for two properties that were measured before this
//! was written:
//!
//! * the kernel releases the lock when the file is closed **or the process
//!   dies**, so a driver that is killed gives its port back instead of wedging
//!   it;
//! * two descriptors opened separately on one file exclude each other, **even
//!   inside a single process** — a lock a process could take twice would be no
//!   lock at all, and it is what lets the rule be tested without a second
//!   process.
//!
//! The rule itself — which port, and why by group rather than by address —
//! lives in `media/userspace/port_claim.rs`; this is only the system call.

use std::fs::{File, OpenOptions};
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::Path;

/// A file this process holds an exclusive lock on, until this is dropped.
#[derive(Debug)]
pub struct LockFile {
    file: File,
}

impl LockFile {
    /// The file the lock is on.
    pub fn path_hint(&self) -> Option<&Path> {
        // `File` does not remember the path it was opened with, and this type
        // deliberately does not either: nothing needs it, and keeping it would
        // be a second copy of a name that can move under a rename.
        let _ = &self.file;
        None
    }
}

/// Take an exclusive, non-blocking lock on `path`, or answer `None` when
/// somebody else holds one.
///
/// The file is created when it is not there and **never removed**: a lock on an
/// unlinked file excludes nobody, so removing one is a way to break this rather
/// than to clean up after it.
///
/// # Errors
///
/// Whatever the open or the lock answered, other than the one answer that is
/// not a failure — a lock somebody else holds, which is `None`. **The two are
/// not the same thing and a caller must not run them together**: a lock file
/// this process cannot open is a port it cannot claim for a reason that has
/// nothing to do with another driver, and a driver that reported "already being
/// driven" for it would be naming a holder that does not exist.
pub fn try_lock(path: &Path) -> io::Result<Option<LockFile>> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        // **A lock file anybody can open**, because what it guards is a *port*
        // rather than a file: two drivers may well be running as different
        // users — a batch under `sudo` and a test under its owner is this
        // machine's own ordinary case — and a lock the second of them cannot
        // open is a port neither of them can have. What is inside means
        // nothing; a hostile hand can remove the file, which is the one way to
        // get past this, and it is not a threat model this driver has.
        .mode(0o666)
        .open(path)?;

    // …and made that mode **whatever the creating process's umask says**, which
    // is what `mode` alone cannot promise: with the usual `022` a lock file
    // comes out `644`, and a driver of another user cannot open it. Only the
    // owner can change this, so it is done once, by whoever created the file,
    // and a failure is left alone — the open above already answered whether
    // *this* process can use it, and the error a caller sees says more.
    let _ = file.set_permissions(std::fs::Permissions::from_mode(0o666));

    // SAFETY: `flock` takes a file descriptor and a flag word. The descriptor
    // is one this call owns, it is live, and it stays live in the value that
    // goes back — which is the whole of what the lock needs. Nothing is read
    // from or written through it.
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
        return Ok(Some(LockFile { file }));
    }

    let error = io::Error::last_os_error();
    match error.raw_os_error() {
        // A held lock is the answer the caller asked for, not a failure.
        Some(libc::EWOULDBLOCK) => Ok(None),
        _ => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn a_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "deepmsg-port-lock-{}-{name}.lock",
            std::process::id()
        ))
    }

    /// A free file is locked, and the lock answers for as long as it is held.
    #[test]
    fn a_free_file_is_locked_and_a_held_one_is_not() {
        let path = a_path("held");

        let held = try_lock(&path)
            .expect("the call answers")
            .expect("it is free");
        assert!(
            try_lock(&path).expect("the call answers").is_none(),
            "the second descriptor is refused while the first holds it — even \
             though both are this process"
        );

        drop(held);
        try_lock(&path)
            .expect("the call answers")
            .expect("and it is free again when the lock goes");

        let _ = std::fs::remove_file(&path);
    }

    /// A lock file that cannot be made is an error and not a silent "free".
    #[test]
    fn a_path_that_cannot_be_opened_is_an_error() {
        let path = std::path::Path::new("/proc/deepmsg-port-lock/should/not/be/creatable");

        assert!(
            try_lock(path).is_err(),
            "a lock that could not be taken must not answer `None`, which is \
             what a held one answers"
        );
    }
}
