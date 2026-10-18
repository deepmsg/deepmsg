//! Reading the cluster log back: the adapter that applies records
//! ([`adapter`]), the replay that delivers them ([`replay`]) (P3-4d), and the
//! archive-to-archive replication that fills a follower's gap ([`replication`])
//! (P3-6b).

pub mod adapter;
pub mod replay;
pub mod replication;

pub use adapter::{LogAdapter, LogReplayHandler};
pub use replay::AgentLogReplay;
pub use replication::AgentLogReplication;
