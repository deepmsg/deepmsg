//! The archive's agents, their threads, and what those threads are called
//! (P2-9a).
//!
//! The reference runs an archive's agents three ways (`ArchiveThreadingMode.java:28-38`),
//! and the way decides both how many threads there are and what they are named:
//!
//! * `DEDICATED` — the conductor, the recorder and the replayer are three
//!   agents (`Archive.java:146-147`), and `DedicatedModeArchiveConductor.onStart`
//!   starts the last two on threads of their own
//!   (`DedicatedModeArchiveConductor.java:54-55`).
//! * `SHARED` — one agent for all three, driven in one turn
//!   (`SharedModeArchiveConductor.java:57-64`).
//! * `INVOKER` — the same single agent, driven by the *caller*
//!   (`Archive.java:149-153`: an `AgentInvoker` instead of an `AgentRunner`).
//!
//! This module is the part of that which is not a thread yet: **which name each
//! role's thread gets**. [`Role::name`] is the table, and [`spawn`] is the one
//! place the rule is applied — the archive's agents are started through it, and
//! a test can read the name back out of the thread it made.
//!
//! # Two namings, and one collision
//!
//! `aeron.thread.naming` (`CommonContext.java:534`) picks between two spellings
//! of the same four roles, and the default is `classic` (`:547`):
//!
//! | role | `classic` | `new` |
//! |---|---|---|
//! | conductor (`Archive.java:131`, `:126`) | `archive-conductor` | `aeron-ar-cnd` |
//! | recorder (`:129`, `:124`) | `archive-recorder` | `aeron-ar-rec` |
//! | replayer (`:130`, `:125`) | `archive-replayer` | `aeron-ar-rep` |
//! | shared (`:132`, `:127`) | `archive-conductor` | `aeron-ar-shd` |
//!
//! ★ **`Shared` and `Conductor` are the same name under `classic`** and differ
//! under `new` (`AERON_ARCHIVE_SHARED_THREAD_NAME_CLASSIC` is literally
//! `AERON_ARCHIVE_CONDUCTOR_THREAD_NAME_CLASSIC`, `Archive.java:132`), which is
//! why they are two roles here and not one: a table that folded them would be
//! right in one naming and wrong in the other.
//!
//! The names are what `AgentRunner.startOnThread(runner, …ThreadFactory())`
//! calls `Thread.setName` with, from the runner's `roleName()`
//! (`org.agrona.concurrent.AgentRunner`; the role names themselves are set in
//! the agents' own constructors — `DedicatedModeArchiveConductor.java:39`,
//! `SharedModeArchiveConductor.java:33`, `ArchiveConductor.java:2711`, `:2758`).
//!
//! # Why these are not cut to fifteen bytes
//!
//! The driver's threads are: `deepmsg_driver::driver` cuts every name with
//! `thread_name` because *its* reference is the C media driver, whose
//! `aeron_thread_set_name` truncates before the call
//! (`concurrent/aeron_thread.c:141-159`) — and `archive-conductor` is longer
//! than the fifteen the kernel keeps anyway.
//!
//! The archive's reference is the **Java** one (the C tree ships no archive
//! server), and Java's `Thread.setName` keeps the whole name; only the kernel's
//! copy is cut, by the JVM. So the whole name is what [`spawn`] passes to
//! [`std::thread::Builder::name`] — the Rust spelling of `Thread.setName`, and
//! the name [`std::thread::current`] hands back — rather than a truncated one
//! that would read as "the driver's rule, applied to the archive".

use std::io;
use std::thread::JoinHandle;

use crate::server::config::ThreadNaming;

/// One of the agents an archive runs, and the thread it runs on.
///
/// The conductor and the recorder/replayer are agents in every mode; `Shared`
/// is the one agent the other two collapse into (`SharedModeArchiveConductor`,
/// which has no runner of its own for either). `Invoker` adds no role: there
/// the caller drives that same agent, on the caller's own thread, which is the
/// caller's to name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// The archive's control plane — `archive-conductor` / `aeron-ar-cnd`.
    Conductor,
    /// Every recording in flight — `archive-recorder` / `aeron-ar-rec`.
    Recorder,
    /// Every replay in flight — `archive-replayer` / `aeron-ar-rep`.
    Replayer,
    /// All three in one agent — `archive-conductor` / `aeron-ar-shd`.
    Shared,
}

impl Role {
    /// What `aeron.thread.naming` names this role's thread.
    #[must_use]
    pub const fn name(self, naming: ThreadNaming) -> &'static str {
        match (self, naming) {
            (Self::Conductor, ThreadNaming::Classic) => "archive-conductor",
            (Self::Conductor, ThreadNaming::New) => "aeron-ar-cnd",
            (Self::Recorder, ThreadNaming::Classic) => "archive-recorder",
            (Self::Recorder, ThreadNaming::New) => "aeron-ar-rec",
            (Self::Replayer, ThreadNaming::Classic) => "archive-replayer",
            (Self::Replayer, ThreadNaming::New) => "aeron-ar-rep",
            // The collision: under `classic` this is the conductor's name too.
            (Self::Shared, ThreadNaming::Classic) => "archive-conductor",
            (Self::Shared, ThreadNaming::New) => "aeron-ar-shd",
        }
    }
}

/// Start `body` on a thread named for `role` under `naming`.
///
/// **The seam, and today nothing else uses it.** An archive that runs more than
/// one thread — `DEDICATED`, which this slice is building towards — starts its
/// agents here, so that "which name does this thread get" has one answer in the
/// code and one place a test can ask; under `SHARED`, where there is one thread
/// and the caller owns it, there is nothing to start. It is deliberately thin:
/// what an agent's loop *does* with its turn — the idle strategy it idles with,
/// and what tells it to stop — belongs to the agent, which is why this is not
/// `deepmsg_driver::driver`'s `run_agent` (that one carries the loop because its
/// own reference does, `aeron_agent.c:395-412`).
///
/// # Errors
///
/// [`io::Error`] if the thread cannot be started.
pub fn spawn<F>(role: Role, naming: ThreadNaming, body: F) -> io::Result<JoinHandle<()>>
where
    F: FnOnce() + Send + 'static,
{
    std::thread::Builder::new()
        .name(role.name(naming).to_owned())
        .spawn(body)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The table, cell by cell — including the one cell that is deliberately
    /// the conductor's name and the four that are not.
    #[test]
    fn every_role_has_the_references_name_under_both_namings() {
        for (role, classic, new) in [
            (Role::Conductor, "archive-conductor", "aeron-ar-cnd"),
            (Role::Recorder, "archive-recorder", "aeron-ar-rec"),
            (Role::Replayer, "archive-replayer", "aeron-ar-rep"),
            (Role::Shared, "archive-conductor", "aeron-ar-shd"),
        ] {
            assert_eq!(classic, role.name(ThreadNaming::Classic), "{role:?}");
            assert_eq!(new, role.name(ThreadNaming::New), "{role:?}");
        }

        // The collision is the point of `Shared` being a role at all: under
        // `classic` the two are one name, and under `new` they are not.
        assert_eq!(
            Role::Conductor.name(ThreadNaming::Classic),
            Role::Shared.name(ThreadNaming::Classic)
        );
        assert_ne!(
            Role::Conductor.name(ThreadNaming::New),
            Role::Shared.name(ThreadNaming::New)
        );
    }

    /// The rule, applied: a thread started through [`spawn`] says the name the
    /// table gives it.
    ///
    /// This is the shape the naming half of P2-9a is accepted in — the
    /// reference's own `ArchiveThreadNamingTest` cannot be used here (it runs
    /// in the reference's JVM, against the reference's archive), so what stands
    /// in for it is this: read the name back out of a thread this build made.
    #[test]
    fn a_spawned_thread_carries_the_name_the_table_gives() {
        for role in [
            Role::Conductor,
            Role::Recorder,
            Role::Replayer,
            Role::Shared,
        ] {
            for naming in [ThreadNaming::Classic, ThreadNaming::New] {
                // The body reports the name of the thread it is already on,
                // which is the whole question being asked: what [`spawn`] named
                // it, as the thread itself sees it.
                let (sender, receiver) = std::sync::mpsc::channel();
                spawn(role, naming, move || {
                    let _ = sender.send(std::thread::current().name().map(str::to_owned));
                })
                .expect("a thread")
                .join()
                .expect("the body does not panic");

                assert_eq!(
                    Some(role.name(naming).to_owned()),
                    receiver.recv().expect("the thread reported its name"),
                    "{role:?} under {naming:?}"
                );
            }
        }
    }
}
