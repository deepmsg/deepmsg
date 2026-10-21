//! Writing to the cluster log (`LogPublisher.java`).
//!
//! Seven writes, all aimed at one exclusive publication created by the
//! election (`addLogPublication`, `ConsensusModuleAgent.java:1639-1686`):
//!
//! | Here | Reference |
//! |---|---|
//! | [`LogPublisher::append_message`] | `appendMessage` (`LogPublisher.java:115-144`) |
//! | [`LogPublisher::append_session_open`] | `appendSessionOpen` (`:146-178`) |
//! | [`LogPublisher::append_session_close`] | `appendSessionClose` (`:180-211`) |
//! | [`LogPublisher::append_timer`] | `appendTimer` (`:213-239`) |
//! | [`LogPublisher::append_cluster_action`] | `appendClusterAction` (`:241-276`) |
//! | [`LogPublisher::append_new_leadership_term_event`] | `appendNewLeadershipTermEvent` (`:278-319`) |
//! | [`LogPublisher::add_destination`] | `addDestination` (`:107-113`) |
//!
//! # Where the publication lives
//!
//! The log publication is an **exclusive** publication and is taken out of the
//! [`Client`] at creation (`Client::take_exclusive_publication`). That is
//! load-bearing rather than stylistic: the ingress handler borrows `&mut Client`
//! for the poll, and if the publication were still in the client's table the
//! same turn could not also write to it (plan §4.5).
//!
//! # Encoding and framing
//!
//! The reference writes the SBE message **through the claim**, and shares one
//! 32-byte buffer for the `SessionMessageHeader` (which is prefixed to every
//! application message). This build encodes into a reusable buffer and offers
//! it, because the generated encoder writes through a `&mut [u8]` and the term
//! is an `AtomicBuffer`, which exposes no mutable slice. `offer` writes the
//! same frame — one header plus the SBE message, fragmented only if it exceeds
//! the publication's maximum payload — so the bytes on the log are identical;
//! what differs is that the reference's claim of a fixed-size message and this
//! build's offer of it both copy the same bytes once.
//!
//! [`append_new_leadership_term_event`](LogPublisher::append_new_leadership_term_event)
//! keeps its claim because it must know the position the message will occupy
//! **before** writing it (the message carries it).

use std::time::Duration;

use deepmsg_client::client::Client;
use deepmsg_client::publication::ExclusivePublication;
use deepmsg_codec_cluster::WriteBuf;
use deepmsg_codec_cluster::cluster_action::ClusterAction;
use deepmsg_codec_cluster::cluster_action_request_codec::{
    ClusterActionRequestEncoder, SBE_BLOCK_LENGTH as CLUSTER_ACTION_BLOCK_LENGTH,
};
use deepmsg_codec_cluster::cluster_time_unit::ClusterTimeUnit;
use deepmsg_codec_cluster::message_header_codec::ENCODED_LENGTH as MESSAGE_HEADER_LENGTH;
use deepmsg_codec_cluster::new_leadership_term_event_codec::NewLeadershipTermEventEncoder;
use deepmsg_codec_cluster::session_close_event_codec::{
    SBE_BLOCK_LENGTH as SESSION_CLOSE_BLOCK_LENGTH, SessionCloseEventEncoder,
};
use deepmsg_codec_cluster::session_message_header_codec::{
    SBE_BLOCK_LENGTH as SESSION_MESSAGE_HEADER_BLOCK_LENGTH, SessionMessageHeaderEncoder,
};
use deepmsg_codec_cluster::session_open_event_codec::{
    SBE_BLOCK_LENGTH as SESSION_OPEN_BLOCK_LENGTH, SessionOpenEventEncoder,
};
use deepmsg_codec_cluster::timer_event_codec::{
    SBE_BLOCK_LENGTH as TIMER_BLOCK_LENGTH, TimerEventEncoder,
};
use deepmsg_core::logbuffer::append::Appended;
use deepmsg_core::logbuffer::descriptor::FRAME_ALIGNMENT;
use deepmsg_core::logbuffer::frame::DATA_HEADER_LENGTH;
use deepmsg_core::uri::ChannelUri;

use crate::error::ClusterError;
use crate::session::cluster_session::ClusterSession;

/// `LogPublisher.SEND_ATTEMPTS`.
const SEND_ATTEMPTS: usize = 3;

/// The largest SBE message the leadership-term path writes (an encoder buffer
/// bound, not a protocol constant).
const MAX_ENCODED_LENGTH: usize = 256;

/// `AeronCluster.SESSION_HEADER_LENGTH`: the SBE header plus
/// `SessionMessageHeader`'s block (`AeronCluster.java:99-100`).
pub const SESSION_HEADER_LENGTH: usize =
    MESSAGE_HEADER_LENGTH + SESSION_MESSAGE_HEADER_BLOCK_LENGTH as usize;

/// Every variable-length field in schema 111 is a 4-byte length and then bytes.
const VAR_DATA_HEADER_LENGTH: usize = 4;

/// The log publication, and the writes the consensus module needs from it.
pub struct LogPublisher {
    publication: Option<ExclusivePublication>,
    /// The reference's shared `UnsafeBuffer`/`ExpandableArrayBuffer` pair,
    /// collapsed into one reusable buffer (see the module docs).
    buffer: Vec<u8>,
}

impl Default for LogPublisher {
    fn default() -> Self {
        Self::new()
    }
}

impl LogPublisher {
    /// An empty publisher; [`Self::publication`] is set by the agent in
    /// `addLogPublication` (`ConsensusModuleAgent.java:1639-1686`).
    pub fn new() -> Self {
        Self {
            publication: None,
            buffer: vec![0_u8; MAX_ENCODED_LENGTH],
        }
    }

    /// Take the log publication, when the election creates it.
    ///
    /// `LogPublisher.publication(ExclusivePublication)` (`LogPublisher.java:69-76`):
    /// a previous publication is **closed**, not dropped. Dropping one tells
    /// the driver nothing — a `Publication` is `REMOVE_PUBLICATION`'s
    /// registration id and nothing else — so the old log publication would go
    /// on existing for the life of the client, with every reader it has
    /// (the member's service, a follower's log subscription) holding an image
    /// that can never advance and is never told it is over. A re-elected
    /// cluster then cannot join the new term at all: the container waits in
    /// `joinActiveLog` for an image it will not let go of
    /// (`BoundedLogAdapter.isDone`, `BoundedLogAdapter.java:115-118`), and the
    /// consensus module waits for the ack that join never sends.
    ///
    /// # Errors
    ///
    /// None returned: the close is best effort, as the reference's
    /// `CloseHelper`-style call is. A refusal leaves the old publication
    /// behind, which is the state this existed in before — not a new failure.
    pub fn set_publication(&mut self, client: &mut Client, publication: ExclusivePublication) {
        self.disconnect(client);
        self.publication = Some(publication);
    }

    /// Give the log publication back (`LogPublisher.disconnect`,
    /// `LogPublisher.java:83-89`).
    ///
    /// Called from `prepareForNewLeadership` (`ConsensusModuleAgent.java:1381`)
    /// and from `close` (`:300`), in that order of importance: the first is
    /// what makes the *next* term joinable at once rather than after a liveness
    /// timeout, and it is the one this build used to leave out.
    pub fn disconnect(&mut self, client: &mut Client) {
        if let Some(publication) = self.publication.take() {
            let _ = client.remove_publication(publication.registration_id(), COMMAND_TIMEOUT);
        }
    }

    /// The publication, for an extension or a snapshot taker that needs the
    /// log's own handle (`ConsensusModuleAgent.java:1941`).
    pub fn publication(&self) -> Option<&ExclusivePublication> {
        self.publication.as_ref()
    }

    /// Whether a publication has been created.
    pub fn has_publication(&self) -> bool {
        self.publication.is_some()
    }

    /// `LogPublisher.position()`: how far this member has appended.
    pub fn position(&self) -> i64 {
        self.publication
            .as_ref()
            .and_then(ExclusivePublication::position)
            .unwrap_or(0)
    }

    /// `LogPublisher.sessionId()`.
    pub fn session_id(&self) -> i32 {
        self.publication
            .as_ref()
            .map_or(-1, ExclusivePublication::session_id)
    }

    /// The window limit the publication's counter carries, read through the
    /// client (`Publication.positionLimit()` in the reference).
    pub fn position_limit(&self, client: &Client) -> i64 {
        let Some(publication) = self.publication.as_ref() else {
            return 0;
        };
        let Some(counters) = client.counters_reader() else {
            return 0;
        };
        counters
            .value(publication.position_limit_counter_id())
            .unwrap_or(0)
    }

    /// `LogPublisher.addDestination` (`:107-113`): a follower's log endpoint.
    ///
    /// # Errors
    ///
    /// [`ClusterError`] when the destination URI cannot be built or the driver
    /// refuses the command.
    pub fn add_destination(
        &mut self,
        client: &mut Client,
        destination_channel: &str,
        follower_log_endpoint: &str,
    ) -> Result<i64, ClusterError> {
        let Some(publication) = self.publication.as_ref() else {
            return Ok(0);
        };
        let uri =
            ChannelUri::create_destination_uri(destination_channel, Some(follower_log_endpoint))
                .map_err(|error| ClusterError::Cluster(error.to_string()))?;
        let registration_id =
            client.add_destination(publication.registration_id(), &uri, COMMAND_TIMEOUT)?;
        Ok(registration_id)
    }

    /// `LogPublisher.appendMessage` (`:115-144`).
    ///
    /// Returns the position the message was written at, or `0` when the three
    /// attempts were all refused — the caller treats that as `ABORT`.
    ///
    /// # Errors
    ///
    /// [`ClusterError::Cluster`] with the reference's words when the publication
    /// is at its maximum position or does not exist.
    pub fn append_message(
        &mut self,
        position_limit: i64,
        leadership_term_id: i64,
        cluster_session_id: i64,
        timestamp: i64,
        payload: &[u8],
    ) -> Result<i64, ClusterError> {
        if self.publication.is_none() {
            return Err(ClusterError::Cluster(
                "log publication has not been created".to_owned(),
            ));
        }

        let length = SESSION_HEADER_LENGTH + payload.len();
        if self.buffer.len() < length {
            self.buffer.resize(length, 0);
        }
        {
            let encoder = SessionMessageHeaderEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer[..SESSION_HEADER_LENGTH]),
                MESSAGE_HEADER_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut message = header.parent().expect("the encoder just wrapped");
            message
                .leadership_term_id(leadership_term_id)
                .cluster_session_id(cluster_session_id)
                .timestamp(timestamp);
        }
        self.buffer[SESSION_HEADER_LENGTH..length].copy_from_slice(payload);
        self.offer(position_limit, length)
    }

    /// `LogPublisher.appendSessionOpen` (`:146-178`).
    ///
    /// # Errors
    ///
    /// As [`Self::append_message`].
    pub fn append_session_open(
        &mut self,
        client: &Client,
        session: &ClusterSession,
        leadership_term_id: i64,
        timestamp: i64,
    ) -> Result<i64, ClusterError> {
        let position_limit = self.position_limit(client);
        let length = MESSAGE_HEADER_LENGTH
            + usize::from(SESSION_OPEN_BLOCK_LENGTH)
            + VAR_DATA_HEADER_LENGTH
            + session.response_channel().len()
            + VAR_DATA_HEADER_LENGTH
            + session.encoded_principal().len();
        self.size_buffer(length);

        {
            let encoder = SessionOpenEventEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer[..length]),
                MESSAGE_HEADER_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut message = header.parent().expect("the encoder just wrapped");
            message
                .leadership_term_id(leadership_term_id)
                .correlation_id(session.correlation_id())
                .cluster_session_id(session.id())
                .timestamp(timestamp)
                .response_stream_id(session.response_stream_id())
                .response_channel(session.response_channel().as_bytes())
                .encoded_principal(session.encoded_principal());
        }

        self.offer(position_limit, length)
    }

    /// `LogPublisher.appendSessionClose` (`:180-211`).
    ///
    /// # Errors
    ///
    /// As [`Self::append_message`].
    pub fn append_session_close(
        &mut self,
        client: &Client,
        _member_id: i32,
        session: &ClusterSession,
        leadership_term_id: i64,
        timestamp: i64,
    ) -> Result<bool, ClusterError> {
        let position_limit = self.position_limit(client);
        let length = MESSAGE_HEADER_LENGTH + usize::from(SESSION_CLOSE_BLOCK_LENGTH);
        self.size_buffer(length);

        {
            let encoder = SessionCloseEventEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer[..length]),
                MESSAGE_HEADER_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut message = header.parent().expect("the encoder just wrapped");
            message
                .leadership_term_id(leadership_term_id)
                .cluster_session_id(session.id())
                .timestamp(timestamp)
                .close_reason(session.close_reason());
        }

        Ok(self.offer(position_limit, length)? > 0)
    }

    /// `LogPublisher.appendTimer` (`:213-239`).
    ///
    /// # Errors
    ///
    /// As [`Self::append_message`].
    pub fn append_timer(
        &mut self,
        position_limit: i64,
        correlation_id: i64,
        leadership_term_id: i64,
        timestamp: i64,
    ) -> Result<i64, ClusterError> {
        let length = MESSAGE_HEADER_LENGTH + usize::from(TIMER_BLOCK_LENGTH);
        self.size_buffer(length);

        {
            let encoder = TimerEventEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer[..length]),
                MESSAGE_HEADER_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut message = header.parent().expect("the encoder just wrapped");
            message
                .leadership_term_id(leadership_term_id)
                .correlation_id(correlation_id)
                .timestamp(timestamp);
        }

        self.offer(position_limit, length)
    }

    /// `LogPublisher.appendClusterAction` (`:241-276`).
    ///
    /// # Errors
    ///
    /// As [`Self::append_message`].
    pub fn append_cluster_action(
        &mut self,
        client: &Client,
        leadership_term_id: i64,
        timestamp: i64,
        action: ClusterAction,
        flags: i32,
    ) -> Result<bool, ClusterError> {
        let position_limit = self.position_limit(client);
        let length = MESSAGE_HEADER_LENGTH + usize::from(CLUSTER_ACTION_BLOCK_LENGTH);
        let frame_length = align_up(DATA_HEADER_LENGTH + length, FRAME_ALIGNMENT as usize);
        let log_position = self.position() + frame_length as i64;
        self.size_buffer(length);

        {
            let encoder = ClusterActionRequestEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer[..length]),
                MESSAGE_HEADER_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut message = header.parent().expect("the encoder just wrapped");
            message
                .leadership_term_id(leadership_term_id)
                .log_position(log_position)
                .timestamp(timestamp)
                .action(action)
                .flags(flags);
        }

        Ok(self.offer(position_limit, length)? > 0)
    }

    /// `LogPublisher.appendNewLeadershipTermEvent` (`:278-310`).
    ///
    /// `timestamp` is in `time_unit` (the cluster clock's own unit), and
    /// `app_version` is the reference's `ctx.appVersion()` semantic version.
    /// The message's `logPosition` is where the **publication will be after this
    /// frame**, which is what the reference computes before the claim
    /// (`:287`).
    ///
    /// `position_limit` is the driver's window limit, read by the caller from
    /// the publication's counter — the same per-call read every producer here
    /// makes.
    ///
    /// # Errors
    ///
    /// [`ClusterError::Cluster`] when no publication exists, and
    /// [`ClusterError::Cluster`] with the reference's words when the claim or the
    /// frame write fails — the reference throws `ClusterException` from
    /// `checkResult`/`checkEncodedLength`.
    #[allow(clippy::too_many_arguments)]
    pub fn append_new_leadership_term_event(
        &mut self,
        position_limit: i64,
        leadership_term_id: i64,
        timestamp: i64,
        term_base_log_position: i64,
        leader_member_id: i32,
        log_session_id: i32,
        time_unit: ClusterTimeUnit,
        app_version: i32,
    ) -> Result<bool, ClusterError> {
        let Some(publication) = self.publication.as_ref() else {
            return Err(ClusterError::Cluster(
                "log publication has not been created".to_owned(),
            ));
        };

        let mut encoded = [0_u8; MAX_ENCODED_LENGTH];
        let encoder = NewLeadershipTermEventEncoder::default()
            .wrap(WriteBuf::new(&mut encoded), MESSAGE_HEADER_LENGTH);
        let mut header = encoder.header(0);
        let mut encoder = header.parent().expect("the encoder just wrapped");

        // The frame's own length decides `logPosition`, and the aligned length
        // is what `publish` writes: `align(header + body, 32)`.
        let message_length = MESSAGE_HEADER_LENGTH + encoder.encoded_length();
        let frame_length = align_up(
            DATA_HEADER_LENGTH + message_length,
            FRAME_ALIGNMENT as usize,
        );
        let log_position = publication.position().unwrap_or(0) + frame_length as i64;

        encoder
            .leadership_term_id(leadership_term_id)
            .log_position(log_position)
            .timestamp(timestamp)
            .term_base_log_position(term_base_log_position)
            .leader_member_id(leader_member_id)
            .log_session_id(log_session_id)
            .time_unit(time_unit)
            .app_version(app_version);

        let length = MESSAGE_HEADER_LENGTH + encoder.encoded_length();

        let mut attempts = SEND_ATTEMPTS;
        while attempts > 0 {
            attempts -= 1;

            match publication.try_claim(position_limit, length) {
                Ok(claim) => {
                    let Some(()) = claim.frame().write_payload(&encoded[..length]) else {
                        return Err(ClusterError::Cluster(
                            "failed to write NewLeadershipTermEvent into the log".to_owned(),
                        ));
                    };
                    // `publish` takes the unaligned frame length — the one
                    // `Frame::begin` wrote — so the padding is not payload.
                    #[allow(clippy::cast_possible_truncation)] // bounded by the term length
                    let unaligned = (DATA_HEADER_LENGTH + message_length) as i32;
                    if claim.frame().publish(unaligned).is_none() {
                        return Err(ClusterError::Cluster(
                            "failed to publish NewLeadershipTermEvent".to_owned(),
                        ));
                    }
                    return Ok(true);
                }
                Err(Appended::EndOfLog) => {
                    // The term rotated under the claim; the next attempt reads
                    // the new one, as `LogPublisher`'s retry does.
                }
                Err(_) => return Ok(false),
            }
        }

        Ok(false)
    }

    /// Three offers of the encoded message at `self.buffer[..length]`
    /// (`LogPublisher.java:124-142`).
    fn offer(&self, position_limit: i64, length: usize) -> Result<i64, ClusterError> {
        let Some(publication) = self.publication.as_ref() else {
            return Err(ClusterError::Cluster(
                "log publication has not been created".to_owned(),
            ));
        };

        for _ in 0..SEND_ATTEMPTS {
            match publication.offer(position_limit, &self.buffer[..length]) {
                Appended::Ok { position, .. } => return Ok(position.raw()),
                Appended::MaxPositionExceeded => {
                    return Err(ClusterError::Cluster(format!(
                        "log publication at max position: term-length={}",
                        publication.term_buffer_length()
                    )));
                }
                Appended::EndOfLog => {}
                _ => {}
            }
        }
        Ok(0)
    }

    fn size_buffer(&mut self, length: usize) {
        if self.buffer.len() < length {
            self.buffer.resize(length, 0);
        }
    }
}

fn align_up(value: usize, alignment: usize) -> usize {
    (value + alignment - 1) & !(alignment - 1)
}

/// How long a synchronous add waits for the driver, as the other startup calls
/// do. Only re-exported so the agent and this module cannot disagree.
pub const COMMAND_TIMEOUT: Duration = Duration::from_secs(10);
