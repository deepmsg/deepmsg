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
//! # Runner 0 is the calling thread
//!
//! The slot numbered 0 (`AERON_AGENT_RUNNER_CONDUCTOR`, `aeron_driver.h:26`) is
//! the conductor under `DEDICATED` and `SHARED_NETWORK` and the four-piece
//! composite under `SHARED` and `INVOKER` (`aeron_driver.c:1003-1064`). It is
//! **not** given a thread of its own: the process's own thread drives it, which
//! is what `aeronmd` does — it starts the driver with `manual_main_loop` true
//! (`aeronmd.c:153`) and then runs `aeron_driver_main_do_work` in a loop of its
//! own (`:165-168`). The runners above it are started here, in the reference's
//! order, highest slot first (`aeron_driver.c:1214-1218`).
//!
//! So a `DEDICATED` driver process has three agent threads plus the thread that
//! called [`Driver::run`], and a `SHARED` one has none: one thread does
//! everything. [`Driver::do_work`] is one pass of slot 0, whatever this driver's
//! mode put there (`aeron_driver_main_do_work`, `:1252-1261`).

use std::io;
use std::thread::JoinHandle;

use deepmsg_cnc::CncFile;

use crate::conductor::{Conductor, ConductorError};
use crate::config::{DriverConfig, IdleStrategySetting, ThreadNaming, ThreadingMode};
use crate::idle::Strategy;
use crate::native_resource_agent::AgentLoop;
use crate::receiver::ReceiverThread;
use crate::sender::SenderThread;
use crate::sys;

/// What the kernel keeps of a thread's name: fifteen bytes
/// (`AERON_THREAD_NAME_MAX_LENGTH`, `concurrent/aeron_thread.h:25`).
///
/// The reference cuts a name to this before asking for it
/// (`aeron_thread_set_name`, `concurrent/aeron_thread.c:141-159`), and it has
/// to: `pthread_setname_np` refuses a longer name outright rather than
/// truncating it, and the two shared roles' classic names are longer than this.
pub(crate) const THREAD_NAME_MAX: usize = 15;

/// `name`, cut to what a thread name can hold.
///
/// Bytes, like the reference's `memcpy` of `strlen`, with one guard it does not
/// need: every name here is ASCII, and this refuses to cut one that is not.
pub(crate) fn thread_name(name: &str) -> String {
    let mut end = name.len().min(THREAD_NAME_MAX);
    while !name.is_char_boundary(end) {
        end -= 1;
    }

    name[..end].to_owned()
}

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
/// `SHARED_NETWORK`, two of them on one.
///
/// The name is cut to what the kernel keeps; see [`thread_name`].
///
/// # Errors
///
/// [`io::Error`] if the thread cannot be spawned.
pub(crate) fn run_agent(
    name: &str,
    mut agent: impl Agent,
    mut idle: Strategy,
) -> io::Result<JoinHandle<()>> {
    std::thread::Builder::new()
        .name(thread_name(name))
        .spawn(move || {
            while let Some(work) = agent.do_work() {
                idle.idle(work);
            }

            agent.close();
        })
}

/// The roles a thread can be running, named the way `aeron.thread.naming` names
/// them (`aeron_driver_context.h:37-48`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Role {
    /// `conductor` / `aeron-md-cnd` — slot 0 under the two conductor modes.
    Conductor,
    /// `aeron-md-nra`, the same in both namings — slot 1.
    NativeResourceAgent,
    /// `receiver` / `aeron-md-rcv` — slot 2 under `DEDICATED`.
    Receiver,
    /// `sender` / `aeron-md-snd` — slot 3.
    Sender,
    /// `[sender, receiver]` / `aeron-md-net` — slot 2 under `SHARED_NETWORK`,
    /// where the pair shares one thread.
    SharedNetwork,
    /// `[conductor, sender, receiver]` / `aeron-md-shd` — slot 0 under `SHARED`
    /// and `INVOKER`, where all four share one thread.
    Shared,
}

impl Role {
    /// The name `aeron.thread.naming` gives this role.
    pub(crate) const fn name(self, naming: ThreadNaming) -> &'static str {
        match (self, naming) {
            (Self::Conductor, ThreadNaming::Classic) => "conductor",
            (Self::Conductor, ThreadNaming::New) => "aeron-md-cnd",
            // The one role whose name the two namings agree on.
            (Self::NativeResourceAgent, _) => "aeron-md-nra",
            (Self::Receiver, ThreadNaming::Classic) => "receiver",
            (Self::Receiver, ThreadNaming::New) => "aeron-md-rcv",
            (Self::Sender, ThreadNaming::Classic) => "sender",
            (Self::Sender, ThreadNaming::New) => "aeron-md-snd",
            (Self::SharedNetwork, ThreadNaming::Classic) => "[sender, receiver]",
            (Self::SharedNetwork, ThreadNaming::New) => "aeron-md-net",
            (Self::Shared, ThreadNaming::Classic) => "[conductor, sender, receiver]",
            (Self::Shared, ThreadNaming::New) => "aeron-md-shd",
        }
    }

    /// The name under the default naming, for the paths that start one agent on
    /// a thread of its own and have no driver's settings to read.
    pub(crate) const fn classic_name(self) -> &'static str {
        self.name(ThreadNaming::Classic)
    }
}

/// The strategy a runner gets when nothing named one: the reference's `backoff`,
/// which four of its six slots default to (`aeron_driver_context.c:1143-1147`).
///
/// Only the agents' own thread-owning constructors ask for this — a driver
/// reads its slot's setting — and they do it so that a test can start one agent
/// without a configuration.
pub(crate) fn default_strategy() -> Strategy {
    Strategy::Backoff(crate::idle::Backoff::new())
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

/// The sender and the receiver on one thread, which is `SHARED_NETWORK`'s slot
/// 2 (`aeron_driver.c:746-755`, `aeron_driver_shared_network_do_work`).
///
/// Both are run every pass, in the reference's order — the sender first — and a
/// `Stop` for either ends the pair: the reference stops the runner these two
/// share, so neither outlives it.
struct SharedNetworkAgent {
    sender: SenderThread,
    receiver: ReceiverThread,
}

impl Agent for SharedNetworkAgent {
    fn do_work(&mut self) -> Option<usize> {
        let sent = self.sender.do_work();
        let received = self.receiver.do_work();

        if sent.is_none() || received.is_none() {
            return None;
        }

        Some(sent.unwrap_or(0) + received.unwrap_or(0))
    }
}

/// The six per-slot idle strategies, as the configuration resolved them
/// (`aeron_driver_context.c:1143-1148`).
///
/// One per runner slot, which is what the reference keeps; a runner is built
/// from its slot's setting when it starts, because a strategy carries the state
/// of the loop it is idling.
struct IdleStrategies {
    conductor: IdleStrategySetting,
    sender: IdleStrategySetting,
    receiver: IdleStrategySetting,
    shared: IdleStrategySetting,
    shared_network: IdleStrategySetting,
    native_resource_agent: IdleStrategySetting,
}

impl IdleStrategies {
    fn new(config: &DriverConfig) -> Self {
        Self {
            conductor: config.conductor_idle.clone(),
            sender: config.sender_idle.clone(),
            receiver: config.receiver_idle.clone(),
            shared: config.shared_idle.clone(),
            shared_network: config.shared_network_idle.clone(),
            native_resource_agent: config.native_resource_agent_idle.clone(),
        }
    }

    /// The setting slot 0's runner asks for: the conductor's where the conductor
    /// has that slot to itself, the shared one where all four pieces do
    /// (`aeron_driver.c:1003-1122`).
    fn runner_zero(&self, mode: ThreadingMode) -> &IdleStrategySetting {
        match mode {
            ThreadingMode::Dedicated | ThreadingMode::SharedNetwork => &self.conductor,
            ThreadingMode::Shared | ThreadingMode::Invoker => &self.shared,
        }
    }
}

/// The strategy a setting names, as a runner can hold it.
///
/// The name was checked when the configuration resolved (`config.rs`), which is
/// where the reference checks it too; a failure here is a `DriverConfig` built
/// by hand with a name that table has not got.
fn strategy(setting: &IdleStrategySetting) -> io::Result<Strategy> {
    setting.strategy().ok_or_else(|| {
        io::Error::other(format!(
            "the idle strategy `{}` is not one the reference's table holds",
            setting.name
        ))
    })
}

/// The reference's `aeron_driver_t`: the conductor, the three agents beside it,
/// and the shape `aeron.threading.mode` gives them.
///
/// The driver owns the whole start-up and shutdown order — which runners exist,
/// in what order they start (`aeron_driver.c:1214-1218`) and in what order they
/// are stopped and closed (`:1274-1296`). What it does **not** own is a thread
/// for itself: [`Driver::run`] blocks the calling thread, which is the slot the
/// conductor or the composite runs on.
pub struct Driver {
    conductor: Conductor,
    /// The three agents, until a mode that runs them on threads takes them.
    /// `SHARED` and `INVOKER` start no threads, so they keep theirs here and
    /// [`Driver::do_work`] drives them.
    agents: Option<AgentStates>,
    mode: ThreadingMode,
    naming: ThreadNaming,
    idle: IdleStrategies,
    /// The runners this driver started, which its close stops and joins. Kept
    /// as they start, so a failure part-way through still leaves them to the
    /// close that follows.
    threads: Vec<JoinHandle<()>>,
}

impl Driver {
    /// Build a driver from a freshly created CnC file: the reference's
    /// `aeron_driver_init`, which builds the conductor and the agents and
    /// starts nothing.
    ///
    /// # Errors
    ///
    /// [`ConductorError`] if the file cannot hold the counters, the rings are
    /// not rings, the resolver will not start, or the file cannot be published.
    pub fn new(cnc: CncFile, config: &DriverConfig) -> Result<Self, ConductorError> {
        let mut conductor = Conductor::without_agents(cnc, config)?;
        let agents = conductor.take_agents();

        Ok(Self {
            conductor,
            agents,
            mode: config.threading_mode,
            naming: config.thread_naming,
            idle: IdleStrategies::new(config),
            threads: Vec::new(),
        })
    }

    /// The mode this driver was configured with.
    pub const fn mode(&self) -> ThreadingMode {
        self.mode
    }

    /// One pass of slot 0, whatever this driver's mode put there.
    ///
    /// Under `DEDICATED` and `SHARED_NETWORK` that is the conductor alone, which
    /// is the reference's `aeron_driver_main_do_work` over a conductor runner.
    /// Under `SHARED` and `INVOKER` it is all four pieces in the reference's
    /// fixed order — sender, receiver, native resource agent, conductor
    /// (`aeron_driver_shared_do_work`, `aeron_driver.c:723-734`).
    ///
    /// This is also the entry point for a caller that embeds the driver and
    /// drives it itself (`INVOKER`): it is what the reference's manual main loop
    /// calls.
    pub fn do_work(&mut self) -> usize {
        match self.mode {
            ThreadingMode::Dedicated | ThreadingMode::SharedNetwork => self.conductor.do_work(),
            ThreadingMode::Shared | ThreadingMode::Invoker => {
                let Self {
                    conductor, agents, ..
                } = self;

                let Some(agents) = agents.as_mut() else {
                    return conductor.do_work();
                };

                let work = agents.sender.do_work().unwrap_or(0)
                    + agents.receiver.do_work().unwrap_or(0)
                    + agents.native_resource_agent.do_work().unwrap_or(0);

                work + conductor.do_work()
            }
        }
    }

    /// Start the runners this mode has and drive slot 0 on this thread until the
    /// driver is asked to stop.
    ///
    /// The runners above slot 0 start in the reference's order, highest slot
    /// first (`aeron_driver.c:1214-1218`): the sender, the receiver, the native
    /// resource agent — or the sender and receiver together, under
    /// `SHARED_NETWORK`. Slot 0 itself is this thread's, which is
    /// `aeronmd.c:165-168`.
    ///
    /// It returns when a termination command has stopped the conductor or a
    /// signal has arrived; stopping and joining the runners is
    /// [`Driver::close`]'s, and the reference keeps them apart the same way.
    ///
    /// # Errors
    ///
    /// [`io::Error`] if a runner thread cannot be started, or if a slot's idle
    /// strategy is not one the reference's table holds.
    pub fn run(&mut self) -> io::Result<()> {
        if ThreadNaming::New == self.naming {
            // `aeronmd` renames its own thread to slot 0's role name, and only
            // under the `new` naming (`aeronmd.c:160-163`). The classic names
            // are the ones `ps` shows a process by, and the platform's own
            // thread keeps the process's name there.
            let _ = sys::set_current_thread_name(self.runner_zero_role().name(self.naming));
        }

        self.start_runners()?;

        let mut idle = strategy(self.idle.runner_zero(self.mode))?;

        while self.conductor.is_running() && sys::stop_signal().is_none() {
            idle.idle(self.do_work());
        }

        Ok(())
    }

    /// Stop and join everything this driver started, and close the conductor.
    ///
    /// The order is the reference's `aeron_driver_close`
    /// (`aeron_driver.c:1274-1296`): every runner is stopped, then closed, and
    /// the conductor's own close is the last thing that happens. Here the
    /// conductor's close is what sends the `Stop` each agent's pass answers, so
    /// it comes **before** the join — a log buffer handed back to an agent that
    /// has already stopped is a file nobody removes — and the agents that were
    /// never given a thread are drained the way their thread would have drained
    /// them.
    ///
    /// Calling it twice is not a close, and not supported: the conductor
    /// releases its counters once.
    ///
    /// # Errors
    ///
    /// The error from the conductor's close, if any — a failure there leaves the
    /// agents running rather than draining a `Stop` that was never sent.
    pub fn close(&mut self) -> io::Result<()> {
        let closed = self.conductor.close();

        for thread in self.threads.drain(..) {
            let _ = thread.join();
        }

        if closed.is_ok() {
            if let Some(agents) = self.agents.as_mut() {
                // No thread ran these, so the `Stop` the close above queued is
                // still in front of a queue that has to be drained — and behind
                // it is the work the conductor handed them on its way out.
                while agents.sender.do_work().is_some() {}
                while agents.receiver.do_work().is_some() {}
                while agents.native_resource_agent.do_work().is_some() {}
            }
        }

        closed
    }

    /// Whether the conductor is still running — a termination command clears it.
    pub const fn is_running(&self) -> bool {
        self.conductor.is_running()
    }

    /// Commands that are in the protocol and not implemented here yet.
    pub const fn unhandled_commands(&self) -> u64 {
        self.conductor.unhandled_commands()
    }

    /// Commands whose type id the protocol does not define.
    pub const fn unknown_commands(&self) -> u64 {
        self.conductor.unknown_commands()
    }

    /// The last command in either of those two positions.
    pub const fn last_unhandled(&self) -> Option<crate::conductor::Command> {
        self.conductor.last_unhandled()
    }

    /// The role slot 0 is playing under this driver's mode.
    fn runner_zero_role(&self) -> Role {
        match self.mode {
            ThreadingMode::Dedicated | ThreadingMode::SharedNetwork => Role::Conductor,
            ThreadingMode::Shared | ThreadingMode::Invoker => Role::Shared,
        }
    }

    /// Put this mode's runners above slot 0 on threads of their own.
    fn start_runners(&mut self) -> io::Result<()> {
        match self.mode {
            ThreadingMode::Dedicated => {
                let Some(states) = self.agents.take() else {
                    return Ok(());
                };

                let sender = strategy(&self.idle.sender)?;
                self.spawn(Role::Sender, states.sender, sender)?;

                let receiver = strategy(&self.idle.receiver)?;
                self.spawn(Role::Receiver, states.receiver, receiver)?;

                let native_resource_agent = strategy(&self.idle.native_resource_agent)?;
                self.spawn(
                    Role::NativeResourceAgent,
                    states.native_resource_agent,
                    native_resource_agent,
                )?;
            }
            ThreadingMode::SharedNetwork => {
                let Some(states) = self.agents.take() else {
                    return Ok(());
                };

                let shared = strategy(&self.idle.shared_network)?;
                self.spawn(
                    Role::SharedNetwork,
                    SharedNetworkAgent {
                        sender: states.sender,
                        receiver: states.receiver,
                    },
                    shared,
                )?;

                let native_resource_agent = strategy(&self.idle.native_resource_agent)?;
                self.spawn(
                    Role::NativeResourceAgent,
                    states.native_resource_agent,
                    native_resource_agent,
                )?;
            }
            // `INVOKER` and `SHARED` build the same single runner
            // (`aeron_driver.c:1003-1022`, one `case` arm for both), and under a
            // manual main loop they are the same driver: the pieces are the
            // calling thread's to run, so nothing is started here.
            ThreadingMode::Shared | ThreadingMode::Invoker => {}
        }

        Ok(())
    }

    /// Put one agent on a thread, and keep its handle for the close to join.
    ///
    /// Handles are kept as they arrive, so a failure after some have started
    /// leaves them to [`Driver::close`] rather than detaching a thread that
    /// would spin for ever on a queue nothing else will stop.
    fn spawn(&mut self, role: Role, agent: impl Agent, idle: Strategy) -> io::Result<()> {
        let thread = run_agent(role.name(self.naming), agent, idle)?;
        self.threads.push(thread);

        Ok(())
    }
}

/// Put the three agents of a `DEDICATED` driver on threads, in the reference's
/// order (`aeron_driver.c:1214-1218`), with the idle strategies and the names
/// the configuration asked for.
///
/// This is what [`Conductor::start_agents`] uses; a driver that places its own
/// runners ([`Driver::start_runners`]) does the same thing one slot at a time,
/// because its modes place different sets.
///
/// # Errors
///
/// [`io::Error`] if a thread cannot be started or a slot's idle strategy is not
/// one the reference's table holds. Handles already pushed stay in `threads`,
/// so the caller can stop and join what did start.
pub(crate) fn spawn_dedicated(
    states: AgentStates,
    config: &DriverConfig,
    threads: &mut Vec<JoinHandle<()>>,
) -> io::Result<()> {
    let naming = config.thread_naming;

    threads.push(run_agent(
        Role::Sender.name(naming),
        states.sender,
        strategy(&config.sender_idle)?,
    )?);
    threads.push(run_agent(
        Role::Receiver.name(naming),
        states.receiver,
        strategy(&config.receiver_idle)?,
    )?);
    threads.push(run_agent(
        Role::NativeResourceAgent.name(naming),
        states.native_resource_agent,
        strategy(&config.native_resource_agent_idle)?,
    )?);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The names are the reference's two tables, and the one role they share
    /// (`aeron_driver_context.h:37-48`).
    #[test]
    fn every_role_has_the_name_each_naming_gives_it() {
        let classic = [
            (Role::Conductor, "conductor"),
            (Role::NativeResourceAgent, "aeron-md-nra"),
            (Role::Receiver, "receiver"),
            (Role::Sender, "sender"),
            (Role::SharedNetwork, "[sender, receiver]"),
            (Role::Shared, "[conductor, sender, receiver]"),
        ];

        for (role, expected) in classic {
            assert_eq!(expected, role.name(ThreadNaming::Classic), "{role:?}");
        }

        let new = [
            (Role::Conductor, "aeron-md-cnd"),
            (Role::NativeResourceAgent, "aeron-md-nra"),
            (Role::Receiver, "aeron-md-rcv"),
            (Role::Sender, "aeron-md-snd"),
            (Role::SharedNetwork, "aeron-md-net"),
            (Role::Shared, "aeron-md-shd"),
        ];

        for (role, expected) in new {
            assert_eq!(expected, role.name(ThreadNaming::New), "{role:?}");
        }
    }

    /// Fifteen bytes, which is `AERON_THREAD_NAME_MAX_LENGTH`
    /// (`concurrent/aeron_thread.h:25`) and what `/proc/<pid>/task/*/comm`
    /// shows.
    #[test]
    fn a_thread_name_is_cut_to_fifteen_bytes() {
        assert_eq!("sender", thread_name("sender"));
        assert_eq!("aeron-md-nra", thread_name("aeron-md-nra"));
        assert_eq!("[sender, receiv", thread_name("[sender, receiver]"));
        assert_eq!(
            "[conductor, sen",
            thread_name("[conductor, sender, receiver]")
        );
    }

    /// Slot 0 is the conductor's where it is alone, and the shared slot's where
    /// all four pieces are on it (`aeron_driver.c:1003-1122`).
    #[test]
    fn slot_zero_takes_its_strategy_from_the_slot_it_is() {
        let idle = IdleStrategies::new(&DriverConfig {
            conductor_idle: IdleStrategySetting {
                name: "noop".to_owned(),
                init_args: None,
            },
            shared_idle: IdleStrategySetting {
                name: "spin".to_owned(),
                init_args: None,
            },
            ..DriverConfig::default()
        });

        for mode in [ThreadingMode::Dedicated, ThreadingMode::SharedNetwork] {
            assert_eq!("noop", idle.runner_zero(mode).name, "{mode:?}");
        }

        for mode in [ThreadingMode::Shared, ThreadingMode::Invoker] {
            assert_eq!("spin", idle.runner_zero(mode).name, "{mode:?}");
        }
    }
}
