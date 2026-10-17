//! The 18 states of the election, and their counter codes.
//!
//! Counterpart of `ElectionState.java:30-115`, written to counter 207 by
//! `Election.state(..)` (`Election.java:1385-1438`). The codes are a contract:
//! `ClusterTool` reads them by number.

/// The election state machine's states (`ElectionState.java:30-115`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(i32)]
pub enum ElectionState {
    Init = 0,
    Canvass = 1,
    Nominate = 2,
    CandidateBallot = 3,
    FollowerBallot = 4,
    LeaderLogReplication = 5,
    LeaderReplay = 6,
    LeaderInit = 7,
    LeaderReady = 8,
    FollowerLogReplication = 9,
    FollowerReplay = 10,
    FollowerCatchupInit = 11,
    FollowerCatchupAwait = 12,
    FollowerCatchup = 13,
    FollowerLogInit = 14,
    FollowerLogAwait = 15,
    FollowerReady = 16,
    Closed = 17,
}

impl ElectionState {
    /// The code the counter carries (`ElectionState.java:30-115`).
    pub const fn code(self) -> i32 {
        self as i32
    }

    /// Every state in counter order — the list the test pins.
    pub const ALL: [Self; 18] = [
        Self::Init,
        Self::Canvass,
        Self::Nominate,
        Self::CandidateBallot,
        Self::FollowerBallot,
        Self::LeaderLogReplication,
        Self::LeaderReplay,
        Self::LeaderInit,
        Self::LeaderReady,
        Self::FollowerLogReplication,
        Self::FollowerReplay,
        Self::FollowerCatchupInit,
        Self::FollowerCatchupAwait,
        Self::FollowerCatchup,
        Self::FollowerLogInit,
        Self::FollowerLogAwait,
        Self::FollowerReady,
        Self::Closed,
    ];
}
