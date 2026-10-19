//! A connected cluster session — the handle, not the machinery.
//!
//! `P3-3a` builds exactly what a *successful connect* has to hand back
//! (`AeronCluster.java:112-118`, `:2572-2598`): where to publish, where the
//! answers arrive, which session this is, which term and member lead it, and a
//! `close` that gives the resources back. Sending, polling egress and following
//! a leader change are `P3-3b`'s, and this type's fields are shaped so that
//! card adds methods rather than changing the seam.
//!
//! # What a `close` does here that the reference's does not
//!
//! The reference's `close` first tells the cluster the session is over
//! (`SessionCloseRequest`, `AC:290-311`, `:1129-1160`) — a log message, so the
//! service sees `on_session_close` at once instead of waiting out the session
//! timeout. That send is the session's own write path and arrives with `P3-3b`;
//! here `close` gives the subscription and the publication back and nothing
//! else. The consequence is recorded in the delivery record: a Java cluster
//! keeps the session until its timeout.
//!
//! # It holds no `Client`
//!
//! The spec's rule (§3): one caller-owned client can carry several sessions.
//! Every method that has to touch the driver takes `&mut Client` — the same
//! shape as the archive's client.

use std::time::Duration;

use deepmsg_client::client::Client;

use crate::client_ops::{ClientOps, PublicationKind};
use crate::context::AeronClusterContext;

/// The ingress publication a session publishes through.
///
/// The reference has one `Publication` type with two implementations behind it
/// (`ExclusivePublication` and `ConcurrentPublication`); the choice is made by
/// `Context.isIngressExclusive`, which defaults to `true` (`AC:1436`,
/// `:795-813`). This build's client keeps the two in separate lists, so the
/// kind travels with the id.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IngressPublication {
    /// `Client::publication` — `isIngressExclusive == false`.
    Concurrent(i64),
    /// `Client::exclusive_publication` — the reference's default.
    Exclusive(i64),
}

impl IngressPublication {
    /// The publication's registration id.
    #[must_use]
    pub const fn registration_id(self) -> i64 {
        match self {
            Self::Concurrent(id) | Self::Exclusive(id) => id,
        }
    }

    /// The list it is in, for this crate's client seam.
    pub(crate) const fn kind(self) -> PublicationKind {
        match self {
            Self::Concurrent(_) => PublicationKind::Concurrent,
            Self::Exclusive(_) => PublicationKind::Exclusive,
        }
    }
}

/// Where a session is. `P3-3b` adds the two `AWAIT_NEW_LEADER` states and
/// `PENDING_CLOSE` (`AC:1170-1193`); a card that does not reconnect needs the
/// two ends only.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionState {
    /// Connected to a leader.
    Connected,
    /// Closed; terminal.
    Closed,
}

/// A session a completed connect established.
#[derive(Debug)]
pub struct AeronCluster {
    context: AeronClusterContext,
    ingress: IngressPublication,
    egress_subscription: i64,
    egress_image: Option<i64>,
    cluster_session_id: i64,
    leadership_term_id: i64,
    leader_member_id: i32,
    state: SessionState,
}

impl AeronCluster {
    /// The synchronous connect (`AC:141-200`): an [`AsyncConnect`] in a loop,
    /// with the caller's client driven.
    ///
    /// **The client is driven here, and nowhere else.** `AsyncConnect::poll`
    /// asks the client questions; the `client.poll()` in this loop is what lets
    /// the driver answer them. A connect written without that call is a
    /// connect that waits for answers nobody reads.
    ///
    /// # Errors
    ///
    /// Whatever the connect fails with.
    pub fn connect(
        context: AeronClusterContext,
        client: &mut Client,
    ) -> Result<Self, crate::error::ClusterClientError> {
        crate::async_connect::connect(context, client)
    }

    /// Assemble a session from what a connect established.
    pub(crate) fn new(
        context: AeronClusterContext,
        ingress: IngressPublication,
        egress_subscription: i64,
        egress_image: Option<i64>,
        cluster_session_id: i64,
        leadership_term_id: i64,
        leader_member_id: i32,
    ) -> Self {
        Self {
            context,
            ingress,
            egress_subscription,
            egress_image,
            cluster_session_id,
            leadership_term_id,
            leader_member_id,
            state: SessionState::Connected,
        }
    }

    /// The context the session was opened with.
    ///
    /// The values a completed connect settled on — `new_leader_timeout_ns`,
    /// which is otherwise `None` — are read here. The reference keeps the
    /// caller's `Context` object alive and updates it in place; this context
    /// moved, so the session is where its settled values live.
    #[must_use]
    pub fn context(&self) -> &AeronClusterContext {
        &self.context
    }

    /// The ingress publication, with its kind.
    #[must_use]
    pub const fn ingress_publication(&self) -> IngressPublication {
        self.ingress
    }

    /// The subscription the cluster's answers arrive on.
    #[must_use]
    pub const fn egress_subscription(&self) -> i64 {
        self.egress_subscription
    }

    /// The image the connect's answer arrived on.
    #[must_use]
    pub const fn egress_image(&self) -> Option<i64> {
        self.egress_image
    }

    /// The session the cluster assigned.
    #[must_use]
    pub const fn cluster_session_id(&self) -> i64 {
        self.cluster_session_id
    }

    /// The leadership term the session was opened in.
    #[must_use]
    pub const fn leadership_term_id(&self) -> i64 {
        self.leadership_term_id
    }

    /// The member that led when the session was opened.
    #[must_use]
    pub const fn leader_member_id(&self) -> i32 {
        self.leader_member_id
    }

    /// How long the session waits for a new leader, now that a connect has
    /// settled it (`AC:2574-2578`, `:1604-1616`).
    #[must_use]
    pub const fn new_leader_timeout_ns(&self) -> Option<i64> {
        self.context.new_leader_timeout_ns
    }

    /// Whether the session is closed (`AC:319-322`).
    #[must_use]
    pub const fn is_closed(&self) -> bool {
        matches!(self.state, SessionState::Closed)
    }

    /// Give the subscription and the publication back (`AC:290-311`, less the
    /// `SessionCloseRequest` — see the module note).
    ///
    /// Failing to give something back is not reported: the reference's
    /// `CloseHelper` hands the failure to its error handler and this has no
    /// handler to hand it to. What a caller can observe is the same either
    /// way — the resources leave this client's lists, and the driver reclaims
    /// them with it.
    pub fn close(&mut self, client: &mut Client) {
        self.close_with(client);
    }

    /// The same, over the crate's client seam.
    pub(crate) fn close_with(&mut self, client: &mut impl ClientOps) {
        if self.is_closed() {
            return;
        }

        let timeout = self.timeout();
        let _ = client.async_remove_subscription(self.egress_subscription, timeout);
        let _ = client.async_remove_publication(self.ingress.registration_id(), timeout);

        self.state = SessionState::Closed;
    }

    fn timeout(&self) -> Duration {
        Duration::from_nanos(u64::try_from(self.context.message_timeout_ns).unwrap_or(0))
    }
}
