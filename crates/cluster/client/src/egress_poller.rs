//! The egress poller: what a cluster said, read one message at a time
//! (`EgressPoller.java`, 333 lines).
//!
//! # One slot, and a flag for what is in it
//!
//! A poll reads up to `fragment_limit` messages and **stops at the first one it
//! understands** — a `SessionMessageHeader` (1), a `SessionEvent` (2), a
//! `NewLeaderEvent` (6) or a `Challenge` (7). That is the whole shape: one slot,
//! filled once, with the fields of whichever of the four it holds. Anything
//! else on the subscription — another schema, or a template this build does not
//! read — is *stepped over* and the poll carries on, which is what makes an
//! egress subscription that also carries somebody else's traffic usable at all.
//!
//! `is_poll_complete` is how a caller knows the slot is full, and it is also
//! what starts the next poll: a poll whose slot is full **resets first**, so a
//! caller reads one message per poll without ever calling `reset` itself
//! (`EP:212-230`).
//!
//! # What the poller does not own
//!
//! The reference's poller creates and owns an
//! `aeron_controlled_fragment_assembler_t` and the `Subscription` it polls
//! (`EP:39-40`, `:61-65`), because in Java those are objects somebody has to
//! own. Here the assembler is the subscription's — one per subscription,
//! living as long as it does — and the poller holds only the subscription's id,
//! because the `Client` is the caller's. That is the same deviation
//! `crates/archive/src/client/poller.rs` records for the archive's poller, and
//! for the same reason.
//!
//! # Hostile input is stepped over, not thrown
//!
//! The reference lets its decoders throw on a short message, which ends the
//! poll. This build's cluster decoders **panic** on one instead
//! (`deepmsg_cluster_common::decode`'s module note), and a panic is not a
//! deviation a network-facing reader gets to have. A message that cannot hold
//! its own fixed part, or whose variable field claims to be longer than the
//! message, is skipped — the poll carries on, and a connect that was waiting
//! for an answer waits for the next one.

use deepmsg_client::client::Client;
use deepmsg_client::fragment_assembler::{Action, Message};
use deepmsg_codec::cluster::challenge_codec::{
    ChallengeDecoder, SBE_BLOCK_LENGTH as CHALLENGE_BLOCK_LENGTH,
};
use deepmsg_codec::cluster::event_code::EventCode;
use deepmsg_codec::cluster::message_header_codec::{self, MessageHeaderDecoder};
use deepmsg_codec::cluster::new_leader_event_codec::{
    NewLeaderEventDecoder, SBE_BLOCK_LENGTH as NEW_LEADER_EVENT_BLOCK_LENGTH,
};
use deepmsg_codec::cluster::session_event_codec::{
    SBE_BLOCK_LENGTH as SESSION_EVENT_BLOCK_LENGTH, SessionEventDecoder,
};
use deepmsg_codec::cluster::session_message_header_codec::{
    SBE_BLOCK_LENGTH as SESSION_MESSAGE_HEADER_BLOCK_LENGTH, SessionMessageHeaderDecoder,
};
use deepmsg_codec::cluster::{ReadBuf, SBE_SCHEMA_ID};

use crate::client_ops::ClientOps;

/// The four templates this poller reads, as the reference spells them
/// (`EP:253-317`).
const SESSION_MESSAGE_HEADER_TEMPLATE_ID: u16 = 1;
const SESSION_EVENT_TEMPLATE_ID: u16 = 2;
const NEW_LEADER_EVENT_TEMPLATE_ID: u16 = 6;
const CHALLENGE_TEMPLATE_ID: u16 = 7;

/// What the cluster said, as far as the last poll read.
///
/// Every field the reference leaves at `Aeron.NULL_VALUE` (`Aeron.java:82`) is
/// an `Option` here: the accessors say `None` rather than -1, and
/// `crate::async_connect` is the one place that compares them.
#[derive(Debug)]
pub struct EgressPoller {
    /// The subscription the cluster's answers arrive on.
    subscription: i64,
    /// How many messages one poll may read (`AC:103`).
    fragment_limit: usize,
    /// The image a `SessionEvent` or `NewLeaderEvent` arrived on — the
    /// connection the answer came from (`EP:283`, `:298`).
    egress_image: Option<i64>,
    /// The session the message names.
    cluster_session_id: Option<i64>,
    /// The request the answer is to.
    correlation_id: Option<i64>,
    /// The leadership term the message was written in.
    leadership_term_id: Option<i64>,
    /// The member that leads.
    leader_member_id: Option<i32>,
    /// The template of the last message read, known or not (`EP:252`).
    template_id: Option<u16>,
    /// The protocol version a session event carried.
    version: Option<i32>,
    /// How long a `SessionEvent` says the leader's heartbeat may take.
    leader_heartbeat_timeout_ns: Option<i64>,
    /// What a `SessionEvent` said happened (`OK`, `ERROR`, …).
    event_code: Option<EventCode>,
    /// A `SessionEvent`'s text, or a `NewLeaderEvent`'s ingress endpoints.
    detail: String,
    /// A `Challenge`'s encoded challenge.
    encoded_challenge: Vec<u8>,
    /// The slot is full, and the next poll will reset it (`EP:50`).
    is_poll_complete: bool,
}

impl EgressPoller {
    /// A poller over `subscription`, reading at most `fragment_limit` messages
    /// per poll (`EP:61-65`).
    #[must_use]
    pub fn new(subscription: i64, fragment_limit: usize) -> Self {
        let mut poller = Self {
            subscription,
            fragment_limit,
            egress_image: None,
            cluster_session_id: None,
            correlation_id: None,
            leadership_term_id: None,
            leader_member_id: None,
            template_id: None,
            version: None,
            leader_heartbeat_timeout_ns: None,
            event_code: None,
            detail: String::new(),
            encoded_challenge: Vec::new(),
            is_poll_complete: false,
        };

        poller.reset();
        poller
    }

    /// Forget what the last poll read (`EP:212-227`).
    ///
    /// Called by [`Self::poll`] when the slot is full, so a caller that reads
    /// one message per poll never needs it.
    pub fn reset(&mut self) {
        self.egress_image = None;
        self.cluster_session_id = None;
        self.correlation_id = None;
        self.leadership_term_id = None;
        self.leader_member_id = None;
        self.template_id = None;
        self.version = None;
        self.leader_heartbeat_timeout_ns = None;
        self.event_code = None;
        self.detail.clear();
        self.encoded_challenge.clear();
        self.is_poll_complete = false;
    }

    /// Read up to `fragment_limit` messages, stopping at the first one this
    /// poller understands (`EP:212-230`).
    ///
    /// Answers how many messages were read, which is what a connect uses to
    /// decide whether to poll again or let the client work.
    pub fn poll(&mut self, client: &mut Client) -> usize {
        self.poll_with(client)
    }

    /// The same, over the crate's client seam (see [`ClientOps`]).
    pub(crate) fn poll_with(&mut self, client: &mut impl ClientOps) -> usize {
        if self.is_poll_complete {
            self.reset();
        }

        // Borrowed apart: the scan is handed this poller as its handler, so the
        // two fields it needs to know about are read before the borrow starts.
        let subscription = self.subscription;
        let fragment_limit = self.fragment_limit;

        client.poll_egress(subscription, fragment_limit, &mut |message| {
            self.on_message(message)
        })
    }

    /// The subscription the answers arrive on.
    #[must_use]
    pub const fn subscription(&self) -> i64 {
        self.subscription
    }

    /// The image the last `SessionEvent`/`NewLeaderEvent` arrived on.
    #[must_use]
    pub const fn egress_image(&self) -> Option<i64> {
        self.egress_image
    }

    /// The template of the last message read.
    #[must_use]
    pub const fn template_id(&self) -> Option<u16> {
        self.template_id
    }

    /// The session the message names.
    #[must_use]
    pub const fn cluster_session_id(&self) -> Option<i64> {
        self.cluster_session_id
    }

    /// The request the answer is to.
    #[must_use]
    pub const fn correlation_id(&self) -> Option<i64> {
        self.correlation_id
    }

    /// The leadership term the message was written in.
    #[must_use]
    pub const fn leadership_term_id(&self) -> Option<i64> {
        self.leadership_term_id
    }

    /// The member that leads.
    #[must_use]
    pub const fn leader_member_id(&self) -> Option<i32> {
        self.leader_member_id
    }

    /// The event code the last session event carried.
    #[must_use]
    pub const fn event_code(&self) -> Option<EventCode> {
        self.event_code
    }

    /// The protocol version the cluster answered with.
    #[must_use]
    pub const fn version(&self) -> Option<i32> {
        self.version
    }

    /// The leader's heartbeat timeout, when the answer carried one.
    #[must_use]
    pub const fn leader_heartbeat_timeout_ns(&self) -> Option<i64> {
        self.leader_heartbeat_timeout_ns
    }

    /// A session event's text, or a new leader's ingress endpoints.
    #[must_use]
    pub fn detail(&self) -> &str {
        &self.detail
    }

    /// The last challenge's encoded challenge.
    #[must_use]
    pub fn encoded_challenge(&self) -> &[u8] {
        &self.encoded_challenge
    }

    /// Whether the slot holds something (`EP:192-195`).
    #[must_use]
    pub const fn is_poll_complete(&self) -> bool {
        self.is_poll_complete
    }

    /// Whether the last message was a challenge (`EP:204`).
    #[must_use]
    pub fn is_challenged(&self) -> bool {
        self.template_id == Some(CHALLENGE_TEMPLATE_ID)
    }

    /// Look at one message and say what the scan should do with it
    /// (`EP:236-320`).
    ///
    /// What stops the scan is `Break`: the message that filled the slot is
    /// **consumed**, and the scan stops there, so the next message is still on
    /// the subscription for the next poll to find. A full slot answers `Abort`,
    /// which leaves the message unconsumed — the reference's early-out for a
    /// poller that should not have been polled again without a reset.
    fn on_message(&mut self, message: Message<'_>) -> Action {
        if self.is_poll_complete {
            return Action::Abort;
        }

        let payload = message.payload;

        let Some(header) = sbe_header(payload) else {
            return Action::Continue;
        };

        // Unknown schemas are skipped (`EP:246-250`) — the subscription may
        // carry another service's traffic.
        if header.schema_id() != SBE_SCHEMA_ID {
            return Action::Continue;
        }

        // Set before the dispatch, as the reference does: `is_challenged()`
        // reads this, and so does a caller that wants to know what the poll
        // last saw even when it was not a template this build reads.
        self.template_id = Some(header.template_id());

        match header.template_id() {
            SESSION_MESSAGE_HEADER_TEMPLATE_ID => {
                if !holds(payload, SESSION_MESSAGE_HEADER_BLOCK_LENGTH) {
                    return Action::Continue;
                }

                let decoder = SessionMessageHeaderDecoder::default().header(header, 0);

                self.leadership_term_id = Some(decoder.leadership_term_id());
                self.cluster_session_id = Some(decoder.cluster_session_id());
                self.is_poll_complete = true;

                Action::Break
            }

            SESSION_EVENT_TEMPLATE_ID => {
                if !holds_var(payload, SESSION_EVENT_BLOCK_LENGTH) {
                    return Action::Continue;
                }

                let mut decoder = SessionEventDecoder::default().header(header, 0);

                let coordinates = decoder.detail_decoder();
                let Some(detail) = var_slice(payload, coordinates) else {
                    return Action::Continue;
                };

                self.cluster_session_id = Some(decoder.cluster_session_id());
                self.correlation_id = Some(decoder.correlation_id());
                self.leadership_term_id = Some(decoder.leadership_term_id());
                self.leader_member_id = Some(decoder.leader_member_id());
                self.event_code = Some(decoder.code());
                self.version = decoder.version();
                // The reference turns the schema's null into `Aeron.NULL_VALUE`
                // (`EP:322-332`); here `None` is that value said once.
                self.leader_heartbeat_timeout_ns = decoder.leader_heartbeat_timeout_ns();
                self.detail = String::from_utf8_lossy(detail).into_owned();
                self.egress_image = Some(message.header.image_registration_id);
                self.is_poll_complete = true;

                Action::Break
            }

            NEW_LEADER_EVENT_TEMPLATE_ID => {
                if !holds_var(payload, NEW_LEADER_EVENT_BLOCK_LENGTH) {
                    return Action::Continue;
                }

                let mut decoder = NewLeaderEventDecoder::default().header(header, 0);

                let coordinates = decoder.ingress_endpoints_decoder();
                let Some(endpoints) = var_slice(payload, coordinates) else {
                    return Action::Continue;
                };

                self.cluster_session_id = Some(decoder.cluster_session_id());
                self.leadership_term_id = Some(decoder.leadership_term_id());
                self.leader_member_id = Some(decoder.leader_member_id());
                self.detail = String::from_utf8_lossy(endpoints).into_owned();
                self.egress_image = Some(message.header.image_registration_id);
                self.is_poll_complete = true;

                Action::Break
            }

            CHALLENGE_TEMPLATE_ID => {
                if !holds_var(payload, CHALLENGE_BLOCK_LENGTH) {
                    return Action::Continue;
                }

                let mut decoder = ChallengeDecoder::default().header(header, 0);

                let coordinates = decoder.encoded_challenge_decoder();
                let Some(challenge) = var_slice(payload, coordinates) else {
                    return Action::Continue;
                };

                self.encoded_challenge.clear();
                self.encoded_challenge.extend_from_slice(challenge);
                self.cluster_session_id = Some(decoder.cluster_session_id());
                self.correlation_id = Some(decoder.correlation_id());
                self.is_poll_complete = true;

                Action::Break
            }

            // A template this poller does not read is consumed and the poll
            // carries on (`EP:319`).
            _ => Action::Continue,
        }
    }
}

/// The reference's `EgressPoller implements ControlledFragmentHandler`
/// (`EP:31`, `:236`): the scan hands the poller each message and takes its
/// answer. Implemented on the type rather than behind a closure so that the
/// dispatch and the scan are the same two objects the reference has.
impl deepmsg_client::fragment_assembler::ControlledHandler for EgressPoller {
    fn on_message(&mut self, message: Message<'_>) -> Action {
        EgressPoller::on_message(self, message)
    }
}

/// The SBE header of a message, or `None` when the payload cannot hold one.
///
/// The reference wraps without a length check and lets a short buffer throw.
/// This build's decoders panic instead (see the module note), so the bound is
/// checked here.
fn sbe_header(payload: &[u8]) -> Option<MessageHeaderDecoder<ReadBuf<'_>>> {
    if payload.len() < message_header_codec::ENCODED_LENGTH {
        return None;
    }

    Some(MessageHeaderDecoder::default().wrap(ReadBuf::new(payload), 0))
}

/// Whether a payload is long enough for a template's fixed part.
fn holds(payload: &[u8], block_length: u16) -> bool {
    payload.len() >= message_header_codec::ENCODED_LENGTH + usize::from(block_length)
}

/// The same, plus the four-byte length prefix a variable field starts with.
fn holds_var(payload: &[u8], block_length: u16) -> bool {
    payload.len() >= message_header_codec::ENCODED_LENGTH + usize::from(block_length) + 4
}

/// A variable field's bytes, when the coordinates are inside the payload.
///
/// The generated `*_slice` would panic on a length that runs past the end; a
/// message that says so is not one this poller reads.
fn var_slice(payload: &[u8], (offset, length): (usize, usize)) -> Option<&[u8]> {
    offset
        .checked_add(length)
        .filter(|end| *end <= payload.len())
        .map(|_| &payload[offset..offset + length])
}

#[cfg(test)]
mod tests {
    use deepmsg_codec::archive::WriteBuf as ArchiveWriteBuf;
    use deepmsg_codec::archive::control_response_code as archive_codes;
    use deepmsg_codec::archive::control_response_codec::ControlResponseEncoder;
    use deepmsg_codec::archive::message_header_codec as archive_message_header_codec;
    use deepmsg_codec::cluster::WriteBuf;
    use deepmsg_codec::cluster::session_message_header_codec::SessionMessageHeaderEncoder;

    use super::*;

    /// `shouldIgnoreUnknownMessageSchema`: an archive message on the egress
    /// stream — schema 101, not the cluster's 111 — is stepped over.
    ///
    /// The session message that follows is what proves it was *stepped over*
    /// rather than read: the poller keeps going and fills its slot with the
    /// next message.
    #[test]
    fn an_unknown_schema_is_stepped_over() {
        let mut buffer = vec![0u8; 1024];
        let length = {
            let encoder = ControlResponseEncoder::default().wrap(
                ArchiveWriteBuf::new(&mut buffer),
                archive_message_header_codec::ENCODED_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut encoder = header.parent().expect("the encoder the header wrapped");

            encoder
                .correlation_id(42)
                .code(archive_codes::ControlResponseCode::ERROR)
                .error_message(b"test");

            archive_message_header_codec::ENCODED_LENGTH + encoder.encoded_length()
        };

        let mut poller = EgressPoller::new(7, 10);
        let message = message(&buffer[..length]);

        assert_eq!(Action::Continue, poller.on_message(message));
        assert!(!poller.is_poll_complete());
        assert_eq!(
            None,
            poller.template_id(),
            "another schema's template is not even recorded: the schema check comes first"
        );
    }

    /// `shouldHandleSessionMessage`: the first session message fills the slot
    /// and is consumed; a second look at a full slot is refused unconsumed.
    #[test]
    fn a_session_message_fills_the_slot_and_a_full_slot_aborts() {
        let mut buffer = vec![0u8; 256];
        let length = {
            let encoder = SessionMessageHeaderEncoder::default().wrap(
                WriteBuf::new(&mut buffer),
                message_header_codec::ENCODED_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut encoder = header.parent().expect("the encoder the header wrapped");

            encoder.cluster_session_id(7777).leadership_term_id(5);

            message_header_codec::ENCODED_LENGTH + encoder.encoded_length()
        };

        let mut poller = EgressPoller::new(7, 10);

        assert_eq!(Action::Break, poller.on_message(message(&buffer[..length])));
        assert!(poller.is_poll_complete());
        assert_eq!(Some(7777), poller.cluster_session_id());
        assert_eq!(Some(5), poller.leadership_term_id());

        // A full slot refuses the next message without consuming it, which is
        // what makes it arrive again on the poll that reset the slot.
        assert_eq!(Action::Abort, poller.on_message(message(&buffer[..length])));
    }

    /// A message the poller's view cannot hold is skipped, not a panic.
    #[test]
    fn a_short_message_is_stepped_over() {
        let mut poller = EgressPoller::new(7, 10);
        let short = [0u8; 4];

        assert_eq!(Action::Continue, poller.on_message(message(&short)));
        assert!(!poller.is_poll_complete());
    }

    fn message(payload: &[u8]) -> Message<'_> {
        Message {
            header: deepmsg_client::fragment_assembler::MessageHeader {
                session_id: 0,
                stream_id: 102,
                term_offset: 0,
                flags: 0,
                position: 0,
                frame_length: 0,
                fragmented_frame_length: 0,
                image_registration_id: 11,
            },
            payload,
        }
    }
}
