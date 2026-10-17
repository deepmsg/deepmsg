//! Writing to the cluster log (`LogPublisher.java`).
//!
//! P3-4a needs exactly one of the five messages this class writes — the
//! `NewLeadershipTermEvent` that closes an election (`LogPublisher.java:278-310`)
//! — because a single-member election cannot reach steady state without it.
//! `P3-4b` adds the session/destination half (`appendMessage`,
//! `appendSessionOpen`, `appendSessionClose`, `appendTimer`,
//! `appendClusterAction`, `addDestination`), which is why the type exists now
//! with one method rather than being inlined into the agent.
//!
//! # Where the publication lives
//!
//! The log publication is an **exclusive** publication and is taken out of the
//! [`Client`] at creation (`Client::take_exclusive_publication`). That is
//! load-bearing rather than stylistic: the ingress handler at `P3-4b` will
//! borrow `&mut Client` for the poll, and if the publication were still in the
//! client's table the same turn could not also write to it (plan §4.5).
//!
//! # Encoding
//!
//! The reference writes the SBE message **through the claim**
//! (`LogPublisher.java:288-299`). This build encodes into a stack buffer and
//! copies it into the claim, because the generated encoder writes through a
//! `&mut [u8]` and the term is an `AtomicBuffer`, which exposes no mutable
//! slice. The framing is the same — `try_claim`, then `publish` of the aligned
//! length — so the bytes on the log are identical; only the one copy differs,
//! and it is on an election path, not a message path.

use std::time::Duration;

use deepmsg_client::publication::ExclusivePublication;
use deepmsg_codec_cluster::WriteBuf;
use deepmsg_codec_cluster::cluster_time_unit::ClusterTimeUnit;
use deepmsg_codec_cluster::message_header_codec::ENCODED_LENGTH as MESSAGE_HEADER_LENGTH;
use deepmsg_codec_cluster::new_leadership_term_event_codec::NewLeadershipTermEventEncoder;
use deepmsg_core::logbuffer::descriptor::FRAME_ALIGNMENT;
use deepmsg_core::logbuffer::frame::DATA_HEADER_LENGTH;

use crate::error::ClusterError;

/// `LogPublisher.SEND_ATTEMPTS`.
const SEND_ATTEMPTS: usize = 3;

/// The largest SBE message this publisher writes. A `NewLeadershipTermEvent`
/// has a 48-byte block, plus the 8-byte message header; the bound is the
/// reference's own generous one for the other four messages, none of which is
/// longer than this.
const MAX_ENCODED_LENGTH: usize = 256;

/// The log publication, and the writes this slice needs from it.
pub struct LogPublisher {
    publication: Option<ExclusivePublication>,
}

impl Default for LogPublisher {
    fn default() -> Self {
        Self::new()
    }
}

impl LogPublisher {
    /// An empty publisher; [`Self::publication`] is set by the agent in
    /// `addLogPublication` (`ConsensusModuleAgent.java:1639-1686`).
    pub const fn new() -> Self {
        Self { publication: None }
    }

    /// Take the log publication, once, when the election creates it.
    ///
    /// `LogPublisher.publication(ExclusivePublication)` (`LogPublisher.java:59`).
    pub fn set_publication(&mut self, publication: ExclusivePublication) {
        self.publication = Some(publication);
    }

    /// The publication, for an extension or a snapshot taker that needs the
    /// log's own handle (`ConsensusModuleAgent.java:1941`).
    pub fn publication(&self) -> Option<&ExclusivePublication> {
        self.publication.as_ref()
    }

    /// Whether a publication has been created (the election's LEADER_LOG_REPLICATION
    /// transition is what creates it).
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
                    #[allow(clippy::cast_possible_truncation)] // bounded by the term length
                    let frame_length = frame_length as i32;
                    if claim.frame().publish(frame_length).is_none() {
                        return Err(ClusterError::Cluster(
                            "failed to publish NewLeadershipTermEvent".to_owned(),
                        ));
                    }
                    return Ok(true);
                }
                Err(deepmsg_core::logbuffer::append::Appended::EndOfLog) => {
                    // The term rotated under the claim; the next attempt reads
                    // the new one, as `LogPublisher`'s retry does.
                }
                Err(_) => return Ok(false),
            }
        }

        Ok(false)
    }
}

fn align_up(value: usize, alignment: usize) -> usize {
    (value + alignment - 1) & !(alignment - 1)
}

/// How long a synchronous add waits for the driver, as the other startup calls
/// do. Only re-exported so the agent and this module cannot disagree.
pub const COMMAND_TIMEOUT: Duration = Duration::from_secs(10);
