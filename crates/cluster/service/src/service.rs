//! The business API: `ClusteredService` and the views handed to it.
//!
//! The reference's business implements `ClusteredService`
//! (`ClusteredService.java:36-167`) and is passed live objects: the container's
//! `Cluster`, a `ClientSession`, a snapshot `Image`. This build passes the same
//! information as short-lived views and values — a `Cluster<'_>` borrowed for
//! the call, a `SessionId` that indexes the container's table, a slice that
//! points into the adapter's buffer — because the container owns all of it and
//! a business that kept a hold would be a business holding a producer's memory.
//!
//! # Callbacks return `Result`
//!
//! The reference's callbacks return `void` and signal failure by throwing,
//! which the container catches and turns into its own termination. A Rust
//! callback that cannot return an error would have to panic, so every method
//! here returns [`ContainerError`] and the container propagates it with `?` —
//! the same control flow, in the language's own spelling (plan rule 5).

use deepmsg_client::client::Client;
use deepmsg_client::image::Image;
use deepmsg_client::publication::{Claim, ExclusivePublication};
use deepmsg_codec_cluster::close_reason::CloseReason;

use crate::cluster::Cluster;
use crate::error::ContainerError;
use crate::role::Role;

/// A cluster session's id (`ClientSession.id()`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SessionId(pub i64);

impl From<i64> for SessionId {
    fn from(id: i64) -> Self {
        Self(id)
    }
}

impl From<SessionId> for i64 {
    fn from(id: SessionId) -> Self {
        id.0
    }
}

impl core::fmt::Display for SessionId {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The Aeron header a session message arrived with (`Header.java`), reduced to
/// what a business can act on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LogHeader {
    /// Where the message ends in the log (`Header.position()`).
    pub position: i64,
    /// The publication's session.
    pub session_id: i32,
    /// The stream.
    pub stream_id: i32,
}

/// A `NewLeadershipTermEvent(24)`, as the business sees it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NewLeadershipTermEvent {
    /// The new leadership term.
    pub leadership_term_id: i64,
    /// The log position the term began at.
    pub log_position: i64,
    /// The log timestamp.
    pub timestamp: i64,
    /// The term's base log position.
    pub term_base_log_position: i64,
    /// The leader.
    pub leader_member_id: i32,
    /// The log publication's session.
    pub log_session_id: i32,
    /// The unit of the term's timestamps.
    pub time_unit: crate::role::TimeUnit,
    /// The application version.
    pub app_version: i32,
}

/// The driver's position-limit counter for a publication
/// (`Publication.positionLimitCounterId`): the limit every claim and offer is
/// clamped to, read where it lives rather than carried by a caller.
pub(crate) fn position_limit(client: &Client, publication: &ExclusivePublication) -> i64 {
    client.counters_reader().map_or(0, |counters| {
        counters
            .value(publication.position_limit_counter_id())
            .unwrap_or(0)
    })
}

/// What the business writes its state into (`ServiceSnapshotTaker`'s
/// publication, `onTakeSnapshot(ExclusivePublication)`).
///
/// The reference hands the business the publication itself. This is a view over
/// it plus the client, because the position limit `try_claim`/`offer` need lives
/// in a driver counter rather than in the publication
/// (`ExclusivePublicationSink`'s reason for carrying both).
///
/// Both methods answer the reference's negative `Publication` codes, so a
/// business that back-pressures writes the same retry loop in either language.
pub struct SnapshotWriter<'a> {
    client: &'a mut Client,
    publication: &'a ExclusivePublication,
}

impl<'a> SnapshotWriter<'a> {
    /// A writer over one snapshot recording's publication.
    pub const fn new(client: &'a mut Client, publication: &'a ExclusivePublication) -> Self {
        Self {
            client,
            publication,
        }
    }

    /// `IdleStrategy.idle` plus the agent invoker's poll (`SnapshotTaker`'s
    /// retry loop): a back-pressured offer is cleared by the driver and the
    /// archive, and this is the turn in which the client sees that happen.
    pub fn idle(&mut self) {
        let _ = self.client.poll();
    }

    /// The snapshot publication.
    pub const fn publication(&self) -> &'a ExclusivePublication {
        self.publication
    }

    /// The client the container runs on, for the business to publish on a
    /// publication of **its own**.
    ///
    /// The counterpart of the `Aeron` a reference business takes out of
    /// `Cluster.aeron()` in `onStart` and keeps (`Cluster.java:151`). This
    /// build's [`Cluster::aeron`] is a borrow that ends with its callback, and
    /// `on_take_snapshot` is handed no `Cluster` — so without this the snapshot
    /// is the one callback in which a business cannot publish.
    ///
    /// It is the **same** client [`Cluster::aeron`] answers: the container's one
    /// connection, not a second one. A publication is addressed by its
    /// **registration id**; keep the id rather than a handle, and look it up
    /// again on the next callback.
    ///
    /// # What this is not for
    ///
    /// * Cluster ingress and egress: those go through [`Cluster`] and
    ///   [`crate::cluster::ClientSession`], whose `check_for_valid_invocation`
    ///   is not on this path.
    /// * Adding or removing a publication. The `&Client` is deliberate:
    ///   `add_publication` is a blocking command with a timeout, and this
    ///   callback runs in the middle of writing the snapshot's recording.
    ///   Create the publication elsewhere and only offer here.
    pub fn aeron(&self) -> &Client {
        &*self.client
    }

    /// The limit a claim or an offer is clamped to.
    fn position_limit(&self) -> i64 {
        position_limit(self.client, self.publication)
    }

    /// How far the recording has been written.
    pub fn position(&self) -> i64 {
        self.publication.position().unwrap_or(0)
    }

    /// `Publication.maxPayloadLength()`.
    pub fn max_payload_length(&self) -> usize {
        self.publication.max_payload_length().unwrap_or(0)
    }

    /// Offer one record.
    pub fn offer(&self, payload: &[u8]) -> i64 {
        match self.publication.offer(self.position_limit(), payload) {
            deepmsg_core::logbuffer::append::Appended::Ok { position, .. } => position.raw(),
            deepmsg_core::logbuffer::append::Appended::BackPressured => {
                crate::proxy::OFFER_BACK_PRESSURED
            }
            deepmsg_core::logbuffer::append::Appended::NotConnected => {
                crate::proxy::OFFER_NOT_CONNECTED
            }
            deepmsg_core::logbuffer::append::Appended::MaxPositionExceeded
            | deepmsg_core::logbuffer::append::Appended::MessageTooLarge
            | deepmsg_core::logbuffer::append::Appended::Malformed => {
                crate::proxy::OFFER_MAX_POSITION_EXCEEDED
            }
            deepmsg_core::logbuffer::append::Appended::EndOfLog
            | deepmsg_core::logbuffer::append::Appended::MidRotation => {
                crate::proxy::OFFER_ADMIN_ACTION
            }
        }
    }

    /// Claim a record of `length` bytes (`ExclusivePublication.tryClaim`).
    ///
    /// The business writes the payload into [`Claim::frame`] and publishes it;
    /// the negative answers are the same ones [`Self::offer`] gives.
    ///
    /// # Errors
    ///
    /// A negative `Publication` code when the claim could not be made.
    pub fn try_claim(&self, length: usize) -> Result<Claim<'_>, i64> {
        match self.publication.try_claim(self.position_limit(), length) {
            Ok(claim) => Ok(claim),
            Err(
                deepmsg_core::logbuffer::append::Appended::BackPressured
                | deepmsg_core::logbuffer::append::Appended::EndOfLog
                | deepmsg_core::logbuffer::append::Appended::MidRotation,
            ) => Err(crate::proxy::OFFER_BACK_PRESSURED),
            Err(deepmsg_core::logbuffer::append::Appended::NotConnected) => {
                Err(crate::proxy::OFFER_NOT_CONNECTED)
            }
            Err(
                deepmsg_core::logbuffer::append::Appended::MaxPositionExceeded
                | deepmsg_core::logbuffer::append::Appended::MessageTooLarge
                | deepmsg_core::logbuffer::append::Appended::Malformed,
            ) => Err(crate::proxy::OFFER_MAX_POSITION_EXCEEDED),
            Err(_) => Err(crate::proxy::OFFER_ADMIN_ACTION),
        }
    }
}

/// What the business reads its state out of (`onStart`'s snapshot image).
///
/// `None` on a start with nothing to recover; otherwise the image positioned
/// just after the container's `SnapshotMarker(END)`, which is the first byte of
/// whatever the business wrote in `on_take_snapshot`.
pub struct SnapshotReader<'a> {
    image: &'a mut Image,
}

impl<'a> SnapshotReader<'a> {
    /// A reader over a replay image.
    pub const fn new(image: &'a mut Image) -> Self {
        Self { image }
    }

    /// Where the reader is.
    pub fn position(&self) -> i64 {
        self.image.position()
    }

    /// Whether the replay has ended.
    pub fn is_end_of_stream(&self) -> bool {
        self.image.is_end_of_stream().unwrap_or(false)
    }

    /// Read up to `fragment_limit` fragments.
    pub fn poll<F>(&mut self, fragment_limit: usize, handler: F) -> usize
    where
        F: FnMut(&deepmsg_client::image::Fragment<'_>),
    {
        self.image.poll(fragment_limit, handler)
    }
}

/// `ClusteredService` (`ClusteredService.java:36-167`).
///
/// Every method has the reference's name and arguments, with two deliberate
/// differences recorded in the plan: sessions arrive as [`SessionId`] rather
/// than as objects (the container's table owns them), and a callback that fails
/// returns an error instead of throwing.
pub trait ClusteredService {
    /// Called once, after recovery has read the snapshot (if any).
    ///
    /// # Errors
    ///
    /// Any error ends the container (`CSA:765-815`).
    fn on_start(
        &mut self,
        cluster: &mut Cluster<'_>,
        snapshot: Option<&mut SnapshotReader<'_>>,
    ) -> Result<(), ContainerError>;

    /// A session has opened (`:61-65`).
    ///
    /// # Errors
    ///
    /// Any error ends the container.
    fn on_session_open(
        &mut self,
        cluster: &mut Cluster<'_>,
        session: SessionId,
        timestamp: i64,
    ) -> Result<(), ContainerError>;

    /// A session has closed (`:70-75`).
    ///
    /// # Errors
    ///
    /// Any error ends the container.
    fn on_session_close(
        &mut self,
        cluster: &mut Cluster<'_>,
        session: SessionId,
        timestamp: i64,
        reason: CloseReason,
    ) -> Result<(), ContainerError>;

    /// A message arrived. `session` is `None` for a service message — one the
    /// service itself offered (`CSA:484-498`).
    ///
    /// # Errors
    ///
    /// Any error ends the container.
    fn on_session_message(
        &mut self,
        cluster: &mut Cluster<'_>,
        session: Option<SessionId>,
        timestamp: i64,
        payload: &[u8],
        header: &LogHeader,
    ) -> Result<(), ContainerError>;

    /// A scheduled timer expired (`:96-99`).
    ///
    /// # Errors
    ///
    /// Any error ends the container.
    fn on_timer_event(
        &mut self,
        cluster: &mut Cluster<'_>,
        correlation_id: i64,
        timestamp: i64,
    ) -> Result<(), ContainerError>;

    /// Take a snapshot (`:108-113`): write the business's state into the
    /// recording, after the container's own records.
    ///
    /// # Errors
    ///
    /// Any error ends the container.
    fn on_take_snapshot(&mut self, writer: &mut SnapshotWriter<'_>) -> Result<(), ContainerError>;

    /// The node's role changed (`:115-119`).
    ///
    /// # Errors
    ///
    /// Any error ends the container.
    fn on_role_change(&mut self, new_role: Role) -> Result<(), ContainerError>;

    /// The container is terminating (`:122-125`).
    ///
    /// # Errors
    ///
    /// The reference reports rather than rethrows here; the container does the
    /// same and keeps going.
    fn on_terminate(&mut self, cluster: &mut Cluster<'_>) -> Result<(), ContainerError>;

    /// A new leadership term began (`:136-150`); the default does nothing.
    ///
    /// # Errors
    ///
    /// Any error ends the container.
    fn on_new_leadership_term_event(
        &mut self,
        _cluster: &mut Cluster<'_>,
        _event: &NewLeadershipTermEvent,
    ) -> Result<(), ContainerError> {
        Ok(())
    }

    /// Short, constant-time work that must not touch the log (`:163-166`).
    fn do_background_work(&mut self, _now_ns: i64) -> usize {
        0
    }
}

/// A business chosen at run time, behind the trait object the registry hands
/// out (`services::by_name`).
///
/// The container's agent is generic over its business; a binary that reads the
/// name from a properties file has no static type to give it. This is the
/// forwarding implementation that makes `ClusteredServiceAgent<Box<dyn
/// ClusteredService>>` the binary's instantiation.
impl ClusteredService for Box<dyn ClusteredService + Send> {
    fn on_start(
        &mut self,
        cluster: &mut Cluster<'_>,
        snapshot: Option<&mut SnapshotReader<'_>>,
    ) -> Result<(), ContainerError> {
        (**self).on_start(cluster, snapshot)
    }

    fn on_session_open(
        &mut self,
        cluster: &mut Cluster<'_>,
        session: SessionId,
        timestamp: i64,
    ) -> Result<(), ContainerError> {
        (**self).on_session_open(cluster, session, timestamp)
    }

    fn on_session_close(
        &mut self,
        cluster: &mut Cluster<'_>,
        session: SessionId,
        timestamp: i64,
        reason: CloseReason,
    ) -> Result<(), ContainerError> {
        (**self).on_session_close(cluster, session, timestamp, reason)
    }

    fn on_session_message(
        &mut self,
        cluster: &mut Cluster<'_>,
        session: Option<SessionId>,
        timestamp: i64,
        payload: &[u8],
        header: &LogHeader,
    ) -> Result<(), ContainerError> {
        (**self).on_session_message(cluster, session, timestamp, payload, header)
    }

    fn on_timer_event(
        &mut self,
        cluster: &mut Cluster<'_>,
        correlation_id: i64,
        timestamp: i64,
    ) -> Result<(), ContainerError> {
        (**self).on_timer_event(cluster, correlation_id, timestamp)
    }

    fn on_take_snapshot(&mut self, writer: &mut SnapshotWriter<'_>) -> Result<(), ContainerError> {
        (**self).on_take_snapshot(writer)
    }

    fn on_role_change(&mut self, new_role: Role) -> Result<(), ContainerError> {
        (**self).on_role_change(new_role)
    }

    fn on_terminate(&mut self, cluster: &mut Cluster<'_>) -> Result<(), ContainerError> {
        (**self).on_terminate(cluster)
    }

    fn on_new_leadership_term_event(
        &mut self,
        cluster: &mut Cluster<'_>,
        event: &NewLeadershipTermEvent,
    ) -> Result<(), ContainerError> {
        (**self).on_new_leadership_term_event(cluster, event)
    }

    fn do_background_work(&mut self, now_ns: i64) -> usize {
        (**self).do_background_work(now_ns)
    }
}
