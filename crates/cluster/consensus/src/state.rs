//! The two state axes the election writes to. The third — `ElectionState` —
//! is in [`crate::election`].
//!
//! All three are written to CnC counters and are the semaphores other
//! processes (the container, `ClusterTool`) read, so the numbers are a
//! contract: they are used as-is, not as an internal enum.

/// The node's role. Counterpart of `Cluster.Role`
/// (`aeron-cluster/src/main/java/io/aeron/cluster/service/Cluster.java:56-66`),
/// written to counter 201 by `ConsensusModuleAgent.role(..)`
/// (`ConsensusModuleAgent.java:1337`).
///
/// **Only the election drives this** (plan §2: `Election.state(..)` at
/// `Election.java:1385-1438` is the one writer). Nothing else may call
/// [`crate::election::ElectionHost::set_role`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(i32)]
pub enum Role {
    Follower = 0,
    Candidate = 1,
    Leader = 2,
}

impl Role {
    /// The code the counter carries (`Cluster.java:56-66`).
    pub const fn code(self) -> i32 {
        self as i32
    }
}

/// The consensus module's lifecycle state. Counterpart of
/// `ConsensusModule.State` (`ConsensusModule.java:148-178`), written to
/// counter 200 by `ConsensusModuleAgent.state(..)`
/// (`ConsensusModuleAgent.java:1310`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(i32)]
pub enum ModuleState {
    Init = 0,
    Active = 1,
    Suspended = 2,
    Snapshot = 3,
    Quitting = 4,
    Terminating = 5,
    Closed = 6,
}

impl ModuleState {
    /// The code the counter carries (`ConsensusModule.java:148-178`).
    pub const fn code(self) -> i32 {
        self as i32
    }

    /// The name the reference's `State.toString()` prints — what `ClusterTool`
    /// puts in "the state of the consensus module is SNAPSHOT".
    pub const fn name(self) -> &'static str {
        match self {
            Self::Init => "INIT",
            Self::Active => "ACTIVE",
            Self::Suspended => "SUSPENDED",
            Self::Snapshot => "SNAPSHOT",
            Self::Quitting => "QUITTING",
            Self::Terminating => "TERMINATING",
            Self::Closed => "CLOSED",
        }
    }

    /// `ConsensusModule.State.get(code)` (`ConsensusModule.java:148-178`):
    /// the state a code names, or `None` for one outside the table.
    pub fn from_code(code: i32) -> Option<Self> {
        match code {
            0 => Some(Self::Init),
            1 => Some(Self::Active),
            2 => Some(Self::Suspended),
            3 => Some(Self::Snapshot),
            4 => Some(Self::Quitting),
            5 => Some(Self::Terminating),
            6 => Some(Self::Closed),
            _ => None,
        }
    }
}
