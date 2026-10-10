//! The archive client's control-response poller: what the archive said, read
//! one message at a time (`aeron_archive_control_response_poller.c`, 340 lines).
//!
//! # One slot, and a flag for what is in it
//!
//! A poll reads up to `fragment_limit` messages and **stops at the first one it
//! understands** — a `ControlResponse` (template 1), a `Challenge` (59), or a
//! `RecordingSignalEvent` (24). That is the whole shape: one slot, filled once,
//! with flags saying which of the three it holds. Anything else on the
//! subscription — a template this poller does not read — is *stepped over* and
//! the poll carries on, which is what makes a control subscription that also
//! carries somebody else's traffic usable at all.
//!
//! `is_poll_complete` is how a caller knows the slot is full, and it is also
//! what starts the next poll: a poll whose slot is full **resets first**, so a
//! caller reads one message per poll without ever calling `reset` itself
//! (`:100-104`).
//!
//! # What the poller does not own
//!
//! The reference's poller creates and owns an
//! `aeron_controlled_fragment_assembler_t` (`:50-57`), because in C the
//! assembler is an object somebody has to make. Here it is the
//! [`Subscription`](deepmsg_client::subscription::Subscription)'s — one per
//! subscription, living as long as it does — and
//! [`Client::poll_subscription_controlled`](deepmsg_client::client::Client::poll_subscription_controlled)
//! hands this poller **whole messages**. So there is no assembler field, and the
//! message this decodes is not a fragment: a challenge too long for one frame
//! has already been put back together by the time it arrives.
//!
//! # A signal is not an answer
//!
//! The third branch is the one that catches callers out. A
//! `RecordingSignalEvent` fills the slot and completes the poll exactly like an
//! answer does — but it is not the answer to anything. The reference's wait loop
//! recognizes it, **dispatches it to the recording-signal handler, and goes back
//! to polling** rather than returning it (`aeron_archive_client.c:109-115`).
//! That is why the poller reports it as its own flag rather than folding it into
//! "a response arrived": the layer above has to be able to tell.
//!
//! # One place this build does not follow
//!
//! The reference's malformed-message flag is **sticky** — cleared only when the
//! slot completes, so a poll that read garbage makes the *next* poll report an
//! error even when it read a good message, which is then reset away and lost.
//! Here it is scoped to the poll. See [`ControlResponsePoller::poll`] for the
//! reading of the reference that says so, and why its own wait loop never
//! notices.

use deepmsg_client::client::Client;
use deepmsg_client::fragment_assembler::{Action, ControlledHandler, Message};
use deepmsg_codec::archive::challenge_codec::{
    ChallengeDecoder, SBE_BLOCK_LENGTH as CHALLENGE_BLOCK_LENGTH,
};
use deepmsg_codec::archive::control_response_code::ControlResponseCode;
use deepmsg_codec::archive::control_response_codec::{
    ControlResponseDecoder, SBE_BLOCK_LENGTH as CONTROL_RESPONSE_BLOCK_LENGTH,
};
use deepmsg_codec::archive::message_header_codec::{self, MessageHeaderDecoder};
use deepmsg_codec::archive::recording_signal::RecordingSignal;
use deepmsg_codec::archive::recording_signal_event_codec::{
    RecordingSignalEventDecoder, SBE_BLOCK_LENGTH as RECORDING_SIGNAL_EVENT_BLOCK_LENGTH,
};
use deepmsg_codec::archive::{ReadBuf, SBE_SCHEMA_ID};

/// `AERON_ARCHIVE_CONTROL_RESPONSE_POLLER_FRAGMENT_LIMIT_DEFAULT` (`:22`): how
/// many messages one poll may step over before it gives up and reports none.
pub const FRAGMENT_LIMIT_DEFAULT: usize = 10;

/// `AERON_NULL_VALUE` (`aeronc.h:30`), which is what every field of the slot
/// holds until something fills it.
pub const NULL_VALUE: i64 = -1;

/// `AERON_ARCHIVE_CLIENT_CONTROL_RESPONSE_SBE_TEMPLATE_ID` (`control_response_codec.rs`).
const CONTROL_RESPONSE_TEMPLATE_ID: u16 = 1;
/// `…_CHALLENGE_SBE_TEMPLATE_ID`.
const CHALLENGE_TEMPLATE_ID: u16 = 59;
/// `…_RECORDING_SIGNAL_EVENT_SBE_TEMPLATE_ID`.
const RECORDING_SIGNAL_EVENT_TEMPLATE_ID: u16 = 24;

/// Reads what an archive says, one message per poll.
///
/// Mirrors `aeron_archive_control_response_poller_stct` (`:26-60`), with one
/// difference worth naming: the reference keeps its code as an `int32_t` and
/// distinguishes "nothing has been read" (`-1`) from "a challenge, which has no
/// code" (`AERON_…_NULL_VALUE`). Here the field is an
/// [`Option`] — there is no reader that wants those two told apart, and the
/// numbers were the C's way of saying `None` twice.
pub struct ControlResponsePoller {
    /// The subscription the archive's answers arrive on.
    subscription: i64,
    /// How many messages one poll may read (`:19`).
    fragment_limit: usize,
    /// The archive's control session, as the message carried it.
    control_session_id: i64,
    /// Which request this answers.
    correlation_id: i64,
    /// What the answer is about — a recording, usually.
    relevant_id: i64,
    /// A recording signal's recording.
    recording_id: i64,
    /// A recording signal's subscription.
    subscription_id: i64,
    /// A recording signal's position.
    position: i64,
    /// A recording signal's kind.
    recording_signal: Option<RecordingSignal>,
    /// The protocol version the archive answered with.
    version: Option<i32>,
    /// A control response's error text, empty when there is none.
    error_message: Vec<u8>,
    /// A challenge's encoded credentials.
    encoded_challenge: Vec<u8>,
    /// `OK` or `ERROR` — `None` before anything is read, and for a challenge.
    code: Option<ControlResponseCode>,
    /// The slot is full, and the next poll will reset it (`:59`).
    is_poll_complete: bool,
    /// The slot holds a `ControlResponse` (`:60`).
    ///
    /// One of three flags rather than a "kind" enum, because that is what the
    /// reference has and what its wait loop reads. The three are mutually
    /// exclusive by construction — exactly one branch of the dispatch sets one —
    /// and `is_poll_complete` is false for all of them until it does.
    is_control_response: bool,
    /// The slot holds a challenge (`:62`). See [`Self::was_challenged`].
    was_challenged: bool,
    /// The slot holds a recording signal (`:63`). See [`Self::is_recording_signal`].
    is_recording_signal: bool,
    /// Set **by a message**, not by the caller: the header could not be read, or
    /// the schema id was not this build's (`:180-190`). A handler cannot return
    /// an error — the scan asks it what to *do* — so the reason travels in a
    /// field and the poll turns it into one.
    error_on_message: bool,
}

impl ControlResponsePoller {
    /// A poller over `subscription`, reading at most `fragment_limit` messages
    /// per poll (`:32-72`).
    #[must_use]
    pub fn new(subscription: i64, fragment_limit: usize) -> Self {
        let mut poller = Self {
            subscription,
            fragment_limit,
            control_session_id: NULL_VALUE,
            correlation_id: NULL_VALUE,
            relevant_id: NULL_VALUE,
            recording_id: NULL_VALUE,
            subscription_id: NULL_VALUE,
            position: NULL_VALUE,
            recording_signal: None,
            version: None,
            error_message: Vec::new(),
            encoded_challenge: Vec::new(),
            code: None,
            is_poll_complete: false,
            is_control_response: false,
            was_challenged: false,
            is_recording_signal: false,
            error_on_message: false,
        };

        poller.reset();
        poller
    }

    /// Forget what the last poll read (`:124-152`).
    ///
    /// Called by [`Self::poll`] when the slot is full, so a caller that reads
    /// one message per poll never needs it.
    pub fn reset(&mut self) {
        self.error_on_message = false;

        self.control_session_id = NULL_VALUE;
        self.correlation_id = NULL_VALUE;
        self.relevant_id = NULL_VALUE;
        self.recording_id = NULL_VALUE;
        self.subscription_id = NULL_VALUE;
        self.position = NULL_VALUE;

        self.recording_signal = None;
        self.version = None;

        self.error_message.clear();
        self.encoded_challenge.clear();

        self.code = None;

        self.is_poll_complete = false;
        self.is_control_response = false;
        self.was_challenged = false;
        self.is_recording_signal = false;
    }

    /// Read up to `fragment_limit` messages, stopping at the first one this
    /// poller understands (`:100-121`).
    ///
    /// Answers how many messages were read, which is what the wait loop uses to
    /// decide whether to poll again or go idle.
    ///
    /// # Errors
    ///
    /// [`ControlResponseError`] when a message on the subscription was
    /// malformed. The reference reports this through a flag the poll turns into
    /// `-1` (`:112-116`), because the fragment handler it is called from has no
    /// way to return one.
    ///
    /// # A recorded deviation
    ///
    /// **The malformed-message flag is cleared here, at the start of every
    /// poll.** The reference clears it only in its reset (`:133`), which runs
    /// only when the slot completed — so it is *sticky*: after one malformed
    /// message, the next poll reads a good one, fills the slot with it, and
    /// still reports `-1`, because the check at `:120` asks whether the flag is
    /// set and not whether *this* poll set it. The poll after that resets, and
    /// the good message goes with it. Nothing reads the flag's age, so the
    /// message is lost rather than misreported — and a caller that retries a
    /// failed poll, which is exactly what a re-readable poll invites, is the
    /// caller that loses one.
    ///
    /// The reference's own wait loop never retries: it returns `-1` and the
    /// operation is over (`aeron_archive_client.c:99-103`), which is why the
    /// stickiness costs it nothing and would cost this build's callers
    /// something. Scoped to the poll, the flag means what its name says.
    pub fn poll(&mut self, client: &mut Client) -> Result<usize, ControlResponseError> {
        if self.is_poll_complete {
            self.reset();
        }

        self.error_on_message = false;

        // Borrowed apart: the scan is handed this poller as its handler, so the
        // two fields it needs to know about are read before the borrow starts.
        let subscription = self.subscription;
        let fragment_limit = self.fragment_limit;

        let fragments = client.poll_subscription_controlled(subscription, fragment_limit, self);

        if self.error_on_message {
            return Err(ControlResponseError::MalformedMessage);
        }

        Ok(fragments)
    }

    /// The subscription the answers arrive on.
    #[must_use]
    pub const fn subscription(&self) -> i64 {
        self.subscription
    }

    /// Whether the slot holds something.
    #[must_use]
    pub const fn is_poll_complete(&self) -> bool {
        self.is_poll_complete
    }

    /// Whether the slot holds a `ControlResponse`.
    #[must_use]
    pub const fn is_control_response(&self) -> bool {
        self.is_control_response
    }

    /// Whether the archive asked this client to prove itself (`:263`).
    ///
    /// The answer this client must give is [`Self::encoded_challenge`], and it
    /// goes back through [`crate::client::proxy::ArchiveProxy::challenge_response`].
    #[must_use]
    pub const fn was_challenged(&self) -> bool {
        self.was_challenged
    }

    /// Whether the slot holds a recording lifecycle signal, which is **not** an
    /// answer — see the module note.
    #[must_use]
    pub const fn is_recording_signal(&self) -> bool {
        self.is_recording_signal
    }

    /// The archive's control session.
    #[must_use]
    pub const fn control_session_id(&self) -> i64 {
        self.control_session_id
    }

    /// Which request the answer belongs to.
    #[must_use]
    pub const fn correlation_id(&self) -> i64 {
        self.correlation_id
    }

    /// What the answer is about.
    #[must_use]
    pub const fn relevant_id(&self) -> i64 {
        self.relevant_id
    }

    /// The recording a signal is about.
    #[must_use]
    pub const fn recording_id(&self) -> i64 {
        self.recording_id
    }

    /// The subscription a signal is about.
    #[must_use]
    pub const fn subscription_id(&self) -> i64 {
        self.subscription_id
    }

    /// The position a signal named.
    #[must_use]
    pub const fn position(&self) -> i64 {
        self.position
    }

    /// Which signal it was.
    #[must_use]
    pub const fn recording_signal(&self) -> Option<RecordingSignal> {
        self.recording_signal
    }

    /// The protocol version the archive answered with.
    #[must_use]
    pub const fn version(&self) -> Option<i32> {
        self.version
    }

    /// A control response's code, or `None` when there was none to read.
    #[must_use]
    pub const fn code(&self) -> Option<ControlResponseCode> {
        self.code
    }

    /// Whether the code was `OK`.
    #[must_use]
    pub fn is_code_ok(&self) -> bool {
        Some(ControlResponseCode::OK) == self.code
    }

    /// Whether the code was `ERROR`.
    #[must_use]
    pub fn is_code_error(&self) -> bool {
        Some(ControlResponseCode::ERROR) == self.code
    }

    /// The archive's reason, when it gave one.
    #[must_use]
    pub fn error_message(&self) -> &[u8] {
        &self.error_message
    }

    /// What the archive wants this client to answer a challenge with.
    #[must_use]
    pub fn encoded_challenge(&self) -> &[u8] {
        &self.encoded_challenge
    }
}

impl ControlledHandler for ControlResponsePoller {
    /// Look at one whole message and say what the scan should do with it
    /// (`:155-334`).
    ///
    /// What stops the scan is `Break`: the message that filled the slot is
    /// **consumed**, and the scan stops there — so the next message is still on
    /// the subscription for the next poll to find. `Continue` would consume it
    /// too and overwrite the slot, which is the whole of the "single slot"
    /// property.
    ///
    /// The `Abort` arm above is the reference's own early-out for a full slot,
    /// and it says something different: leave the message **unconsumed**, so it
    /// arrives again. Given [`Self::poll`] resets a full slot before it scans,
    /// the arm is unreachable from here — it is kept because it is the
    /// reference's, and because "nothing reaches it" is a claim about this
    /// caller rather than about the handler.
    fn on_message(&mut self, message: Message<'_>) -> Action {
        if self.is_poll_complete {
            return Action::Abort;
        }

        let payload = message.payload;

        // The reference's `messageHeader_wrap` and the schema-id check
        // (`:167-190`). Both fail the *poll*, not the message: the scan carries
        // on and `poll` reports it (`:112-116`).
        let Some(header) = sbe_header(payload) else {
            self.error_on_message = true;
            return Action::Break;
        };

        if header.schema_id() != SBE_SCHEMA_ID {
            self.error_on_message = true;
            return Action::Break;
        }

        match header.template_id() {
            CONTROL_RESPONSE_TEMPLATE_ID => {
                if !holds(payload, CONTROL_RESPONSE_BLOCK_LENGTH) {
                    self.error_on_message = true;
                    return Action::Break;
                }

                let mut response = ControlResponseDecoder::default().header(header, 0);

                self.control_session_id = response.control_session_id();
                self.correlation_id = response.correlation_id();
                self.relevant_id = response.relevant_id();
                self.version = response.version();
                self.code = Some(response.code());

                // The only variable field, and it is read **there and then**:
                // the coordinates come out of a cursor the decoder advances, so
                // a later read would be looking at whatever followed it.
                let coordinates = response.error_message_decoder();
                self.error_message
                    .extend_from_slice(response.error_message_slice(coordinates));

                self.is_control_response = true;
                self.is_poll_complete = true;

                Action::Break
            }
            CHALLENGE_TEMPLATE_ID => {
                if !holds(payload, CHALLENGE_BLOCK_LENGTH) {
                    self.error_on_message = true;
                    return Action::Break;
                }

                let mut challenge = ChallengeDecoder::default().header(header, 0);

                self.control_session_id = challenge.control_session_id();
                self.correlation_id = challenge.correlation_id();
                // The reference sets this one field to `AERON_NULL_VALUE` here
                // and leaves the rest at whatever the reset put there (`:245`):
                // a challenge is about no particular thing.
                self.relevant_id = NULL_VALUE;
                self.version = challenge.version();

                // No code, and *neither* of the two code flags set — which is
                // how a caller tells a challenge from an `ERROR` response whose
                // code happens to be `ERROR`.
                self.code = None;

                let coordinates = challenge.encoded_challenge_decoder();
                self.encoded_challenge
                    .extend_from_slice(challenge.encoded_challenge_slice(coordinates));

                self.was_challenged = true;
                self.is_poll_complete = true;

                Action::Break
            }
            RECORDING_SIGNAL_EVENT_TEMPLATE_ID => {
                if !holds(payload, RECORDING_SIGNAL_EVENT_BLOCK_LENGTH) {
                    self.error_on_message = true;
                    return Action::Break;
                }

                let signal = RecordingSignalEventDecoder::default().header(header, 0);

                self.control_session_id = signal.control_session_id();
                self.correlation_id = signal.correlation_id();
                self.recording_id = signal.recording_id();
                self.subscription_id = signal.subscription_id();
                self.position = signal.position();
                self.recording_signal = Some(signal.signal());

                self.is_recording_signal = true;
                self.is_poll_complete = true;

                Action::Break
            }
            // "do nothing" (`:330-332`): a template this poller does not read
            // is consumed and the poll carries on, up to the fragment limit.
            _ => Action::Continue,
        }
    }
}

/// The SBE header of a message, or `None` when the payload cannot hold one.
///
/// The reference's `messageHeader_wrap` with a length (`:167-176`), which
/// answers `NULL` rather than reading past the end. The generated decoder would
/// **panic** on a short payload — every field read is an indexed read — so the
/// check is the port of the reference's bound, not an addition to it.
fn sbe_header(payload: &[u8]) -> Option<MessageHeaderDecoder<ReadBuf<'_>>> {
    if payload.len() < message_header_codec::ENCODED_LENGTH {
        return None;
    }

    Some(MessageHeaderDecoder::default().wrap(ReadBuf::new(payload), 0))
}

/// Whether a payload is long enough for a template's fixed part.
///
/// The same bound as [`sbe_header`], one level down: the reference passes a
/// length to every `…_wrap_for_decode` too (`:207-213`), and the generated Rust
/// decoder has no such argument.
///
/// **What is not checked** is the variable-length fields. A `ControlResponse`
/// whose `errorMessage` claims a length past the end of the message still
/// panics in the generated slice read — the same class of hazard
/// [`crate::client::proxy`]'s module note records for the encoders. Guarding it
/// would mean re-deriving the var-data offsets by hand, which is exactly the
/// arithmetic the generated code exists to do; a message like that has to come
/// from a source that is already lying, and the alternative is a client that
/// reports rather than a client that misreads.
fn holds(payload: &[u8], block_length: u16) -> bool {
    payload.len() >= message_header_codec::ENCODED_LENGTH + usize::from(block_length)
}

/// Why a poll did not read what it was looking at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlResponseError {
    /// A message on the control subscription was not a well-formed archive
    /// message: too short for its own header, or carrying another schema's id.
    MalformedMessage,
}

impl core::fmt::Display for ControlResponseError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::MalformedMessage => write!(f, "malformed message on the control subscription"),
        }
    }
}

impl std::error::Error for ControlResponseError {}
