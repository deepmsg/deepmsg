//! The consensus module's side of the control channel (P3-4c).
//!
//! | Reference | Here |
//! |---|---|
//! | `ServiceProxy` (CM → service) | [`proxy`] |
//! | `ConsensusModuleAdapter` (service → CM) | [`adapter`] |
//! | `ServiceAck` and its queues | [`ack`] |
//! | `PendingServiceMessageTracker` | [`tracker`] |

pub mod ack;
pub mod adapter;
pub mod proxy;
pub mod tracker;

pub use ack::{ServiceAck, ServiceAckQueues, new_array_of_queues};
pub use adapter::{FRAGMENT_LIMIT, ServiceControlAdapter, ServiceControlHandler};
pub use proxy::{ServiceProxy, ServiceSend};
pub use tracker::{
    LogAppender, PendingServiceMessageTracker, service_id_from_log_message,
    service_id_from_service_message, service_session_id,
};
