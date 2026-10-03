//! The driver's agents and the threads they run on (G4-1).
//!
//! The reference's driver is four pieces of work — conductor, native resource
//! agent, receiver, sender — run by an `aeron_agent_runner_t` set whose shape
//! `aeron.threading.mode` decides (`aeron_driver.c:1003-1122`): one thread
//! each, three with the network pair sharing one, one thread for all of them,
//! or none at all and the caller drives them.
//!
//! This module is that, in this build's terms. [`Agent`] is the one thing every
//! piece of work has in common — a pass that says whether to carry on — and
//! [`run_agent`] is the loop the reference wraps around it
//! (`aeron_agent.c:395-412`), with the idle strategy the runner owns. The
//! threads are built from it, so the loop exists once rather than once per
//! agent.
//!
//! The modes themselves are the next step: what is here is the shape they will
//! be told to take, and the three threads a `DEDICATED` driver starts.

use std::io;
use std::thread::JoinHandle;

use crate::idle::Backoff;
use crate::native_resource_agent::AgentLoop;
use crate::receiver::ReceiverThread;
use crate::sender::SenderThread;

/// One piece of the driver's work, driven a pass at a time.
///
/// `None` means the agent has been asked to stop — the reference's `running`
/// flag, cleared by a command and checked after the pass — and `Some(work)` is
/// what the idle strategy is given.
pub(crate) trait Agent: Send + 'static {
    /// One pass.
    fn do_work(&mut self) -> Option<usize>;

    /// What the agent does with what it holds across passes when its loop is
    /// over.
    ///
    /// Nothing, for the two that hold nothing: the sender's publications and
    /// the receiver's images are dropped, and the conductor's close has already
    /// taken the log buffers back. The native resource agent overrides it to
    /// close the resolver it was carrying (`aeron_driver_name_resolver_close`,
    /// and `on_close` on the agent).
    fn close(&mut self) {}
}

/// Start a thread that drives `agent` until it is asked to stop, and give back
/// its handle.
///
/// The loop is the reference's (`aeron_agent.c:395-412`): one pass, then the
/// idle strategy with what the pass did. Nothing else lives here, which is why
/// the same five lines serve an agent with a thread of its own and, in
/// `SHARED`, four of them on one.
///
/// # Errors
///
/// [`io::Error`] if the thread cannot be spawned.
pub(crate) fn run_agent(name: &str, mut agent: impl Agent) -> io::Result<JoinHandle<()>> {
    std::thread::Builder::new()
        .name(name.to_owned())
        .spawn(move || {
            let mut idle = Backoff::new();

            while let Some(work) = agent.do_work() {
                idle.idle(work);
            }

            agent.close();
        })
}

/// The three agents the conductor does not run itself, before a runner takes
/// them.
///
/// The conductor builds them — they need the settings it was made from and the
/// CnC file it owns — and then hands them over, because who runs them is not
/// the conductor's to decide: it is `aeron.threading.mode`'s.
pub(crate) struct AgentStates {
    /// The sender, whose end the conductor keeps.
    pub sender: SenderThread,
    /// The receiver.
    pub receiver: ReceiverThread,
    /// The native resource agent, with the resolver on it.
    pub native_resource_agent: AgentLoop,
}

/// The threads a `DEDICATED` driver runs those agents on.
///
/// The names are the reference's classic ones
/// (`aeron_driver_context.h:37-42`), which is also the default
/// `aeron.thread.naming`; a driver that asked for `new` gets the short ones in
/// the commit that reads the setting.
pub(crate) struct AgentThreads {
    sender: JoinHandle<()>,
    receiver: JoinHandle<()>,
    native_resource_agent: JoinHandle<()>,
}

impl AgentThreads {
    /// One thread per agent.
    ///
    /// # Errors
    ///
    /// [`io::Error`] if a thread cannot be spawned. The ones already started
    /// are waited for rather than left behind, so a failure here does not
    /// outlive the driver that asked for it.
    pub(crate) fn spawn(states: AgentStates) -> io::Result<Self> {
        let sender = run_agent(Role::Sender.classic_name(), states.sender)?;
        let receiver = match run_agent(Role::Receiver.classic_name(), states.receiver) {
            Ok(handle) => handle,
            Err(error) => {
                let _ = sender.join();
                return Err(error);
            }
        };
        let native_resource_agent = match run_agent(
            Role::NativeResourceAgent.classic_name(),
            states.native_resource_agent,
        ) {
            Ok(handle) => handle,
            Err(error) => {
                let _ = sender.join();
                let _ = receiver.join();
                return Err(error);
            }
        };

        Ok(Self {
            sender,
            receiver,
            native_resource_agent,
        })
    }

    /// Wait for all three to finish what they were doing.
    ///
    /// This is the join half of a close, and it comes **after** the conductor
    /// has asked them to stop: a log buffer handed back to an agent that has
    /// already stopped is a file nobody removes.
    pub(crate) fn join(self) {
        let _ = self.sender.join();
        let _ = self.receiver.join();
        let _ = self.native_resource_agent.join();
    }
}

/// The roles a thread can be running, named the way the reference's classic
/// `aeron.thread.naming` names them (`aeron_driver_context.h:37-42`).
///
/// The conductor's own role, the two shared ones and the `new` naming are the
/// runner commit's: what is here is the naming of the threads this build
/// actually starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Role {
    /// `receiver`.
    Receiver,
    /// `sender`.
    Sender,
    /// `aeron-md-nra` — the native resource agent, whose name is the same in
    /// both namings.
    NativeResourceAgent,
}

impl Role {
    /// The name the reference's classic naming gives this role.
    pub(crate) const fn classic_name(self) -> &'static str {
        match self {
            Self::Receiver => "receiver",
            Self::Sender => "sender",
            Self::NativeResourceAgent => "aeron-md-nra",
        }
    }
}
