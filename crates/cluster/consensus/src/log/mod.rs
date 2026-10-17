//! Reading the cluster log back: the adapter that applies records
//! ([`adapter`]) and the replay that delivers them ([`replay`]) (P3-4d).

pub mod adapter;
pub mod replay;

pub use adapter::{LogAdapter, LogReplayHandler};
pub use replay::AgentLogReplay;
