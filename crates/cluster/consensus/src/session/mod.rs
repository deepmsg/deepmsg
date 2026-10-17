//! Sessions and the two edges clients see: ingress and egress (P3-4b).
//!
//! The module split follows the reference's classes:
//!
//! | Reference | Here |
//! |---|---|
//! | `ClusterSession`, `ClusterSessionProxy` | [`cluster_session`] |
//! | `SessionManager` | [`manager`] |
//! | `IngressAdapter` | [`ingress`] |
//! | `EgressPublisher` | [`egress`] |
//! | `io.aeron.security.*` | [`authenticator`] |

pub mod authenticator;
pub mod cluster_session;
pub mod egress;
pub mod ingress;
pub mod manager;

pub use authenticator::{Authenticator, AuthorisationService, SessionProxy};
pub use cluster_session::{ClusterSession, ClusterSessionAction, ClusterSessionState};
pub use egress::{EgressPublisher, EgressSession};
pub use ingress::{IngressAdapter, IngressHandler};
pub use manager::{SessionConnectRequest, SessionContext, SessionManager};
