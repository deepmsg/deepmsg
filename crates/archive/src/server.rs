//! Archive server (P2): conductor, control sessions, recording scheduler,
//! replay sessions, replication sessions (M13 / M16).
//!
//! Deployment constraint to preserve: the server records from shared memory
//! it can reach — it snoops local publications (`aeron-spy:` prefixed UDP
//! sources, LOCAL source location) and therefore must live on the same host
//! as the streams it records; cross-site movement goes through
//! archive-to-archive replication.

pub mod auth;
pub mod conductor;
pub mod config;
pub mod control_adapter;
pub mod control_session;
pub mod counters;
pub mod response_proxy;
