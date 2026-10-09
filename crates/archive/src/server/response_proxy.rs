//! A session's answers, on the wire.
//!
//! `ControlResponseProxy` is the one place an archive writes to a client
//! (`ControlResponseProxy.java:34-273`): it encodes a message into a buffer it
//! keeps and offers it on the session's own exclusive publication, up to three
//! times, with no queue of its own in between. What goes out is one of four
//! messages in this slice — a `ControlResponse`, a `Challenge`, a `Ping` and a
//! `RecordingDescriptor` — and the signal sends arrive with the sessions that
//! send them.
//!
//! # The descriptor is not encoded, it is moved
//!
//! Every other message here is written field by field. A descriptor is the
//! catalog's own bytes: the reference offers the mapped descriptor straight
//! onto the publication, prefixing only the message header and the two ids the
//! listing session fills in (`:54-89`), and this does the same through
//! [`Response::Descriptor`](crate::server::control_session::Response) — the
//! body is copied once, and the session's two ids are written over the two the
//! catalog leaves zero. That is why the catalog can hand out a
//! `RecordingDescriptor` and mean a **message** by it.
//!
//! # The three attempts, and what ends a session
//!
//! `SEND_ATTEMPTS` is three (`:36`), and a failed offer is not retried blindly:
//! `checkResult` reads the failure and, for three of them, **ends the session**
//! and raises an `ArchiveEvent` (`:242-262`). The three are the publication
//! being closed, not connected, or at its maximum position — the states a
//! session cannot come back from. Everything else (a full window, a log turning
//! over) is what the next turn is for.
//!
//! That is the one shape here that is not the reference's: `checkResult` aborts
//! the session and throws, and this proxy owns neither — the session is the
//! conductor's. So the outcome goes back as a value
//! ([`Offered`](crate::server::control_session::Offered)) and the session, which
//! has `abort`, is what acts on it.
//!
//! # The publication is not held
//!
//! The reference holds the `ExclusivePublication` the driver handed it
//! (`ControlSession.java:869-887`). A publication lives inside this build's
//! client, which the archive is a *user* of, so what can be held across turns is
//! the handle the `ADD_*` drew — see
//! [`Publications`](crate::server::control_session::Publications). The three
//! states the reference keeps in two fields (`controlPublication` and
//! `controlPublicationRegistrationId`) are one enum here, because the reference
//! only ever uses them in the combinations it has.
//!
//! # One field the reference does not write
//!
//! `Challenge` has an optional `version` field inside its block, and the
//! reference never writes it (`:146-159`) — so what a client receives there is
//! whatever the previous message left in the reused buffer, which makes the
//! reference's challenge bytes depend on the response before them. This writes
//! the field's null value, because a buffer this build controls may as well be
//! determinate; nothing reads it.

use std::time::Duration;

use deepmsg_client::client::{AsyncAdd, AsyncAddPoll, Client, CommandError};
use deepmsg_codec::archive::WriteBuf;
use deepmsg_codec::archive::challenge_codec::{self, ChallengeEncoder};
use deepmsg_codec::archive::control_response_codec::{self, ControlResponseEncoder};
use deepmsg_codec::archive::message_header_codec::ENCODED_LENGTH as MESSAGE_HEADER_LENGTH;
use deepmsg_codec::archive::ping_codec::{self, PingEncoder};
use deepmsg_codec::archive::recording_descriptor_codec::RecordingDescriptorEncoder;
use deepmsg_codec::archive::recording_signal_event_codec::{self, RecordingSignalEventEncoder};
use deepmsg_codec::archive::recording_subscription_descriptor_codec::{
    self, RecordingSubscriptionDescriptorEncoder,
};
use deepmsg_core::logbuffer::append::Appended;
use deepmsg_core::version::semantic_version_compose;

use crate::server::control_session::{
    Egress, Offered, Publications, RESPONSE_NOT_CONNECTED_MSG, RESPONSE_PUBLICATION_CLOSED_MSG,
    RESPONSE_PUBLICATION_MAX_POSITION_MSG, Response, ResponseError,
};
use crate::server::replay_session::{PublicationFacts, ReplayPublications};

/// How many times one response is offered before the turn gives up on it
/// (`ControlResponseProxy.java:36`).
pub const SEND_ATTEMPTS: usize = 3;

/// `AeronArchive.Configuration.PROTOCOL_SEMANTIC_VERSION`
/// (`client/AeronArchive.java:2667-2668`), which every `ControlResponse` carries
/// and the challenge does not.
pub const PROTOCOL_SEMANTIC_VERSION: i32 = semantic_version_compose(1, 12, 0);

/// The size the reference's buffer starts at
/// (`ControlResponseProxy.java:42`); it grows for a longer message.
const INITIAL_BUFFER_LENGTH: usize = 1024;

/// What a variable-length field costs before its bytes: the length itself.
const VAR_DATA_LENGTH_PREFIX: usize = 4;

/// Where the publication is (`ControlSession.java:861-887`).
#[derive(Debug, Clone, PartialEq, Eq)]
enum Publication {
    /// Nothing has been asked for yet: the reference's `NULL_VALUE`
    /// registration id with no publication.
    NotAskedFor,
    /// Asked for, and the driver has not answered: the reference's
    /// registration id with no publication yet. It is **not** asked for again
    /// while it is here (`:861-867`).
    Pending(i64),
    /// In hand. The reference's `NULL_VALUE` registration id *with* a
    /// publication — it clears the id when the publication arrives, because
    /// the object is what it uses from then on (`:883-887`).
    InHand(i64),
    /// The driver refused it, or the registration is no longer this client's.
    ///
    /// The reference's `aeron.getExclusivePublication` **throws** here
    /// (`ControlSession.java:874-882` rethrows anything that is not
    /// `RESOURCE_TEMPORARILY_UNAVAILABLE`), and the throw climbs out of
    /// `doWork` and ends the session. This build cannot throw out of a turn, so
    /// the reason is kept until the session asks for the publication again —
    /// which it does every turn it is not ready — and
    /// [`Egress::add_publication`] answers with it, which is the same ending by
    /// the path the session already has for a channel it cannot use.
    ///
    /// Without it the session would poll a registration that will never answer,
    /// for ever, offering the client nothing and saying nothing.
    Failed(String),
}

/// One session's response publication, and the buffer its answers are built in.
#[derive(Debug)]
pub struct ControlResponseProxy {
    publication: Publication,
    /// How long a command may take to reach the driver.
    ///
    /// The reference has no equivalent: its asynchronous add does not wait for
    /// anything, and this client's waits for the command to be taken. A
    /// conductor that blocks is a conductor not serving any other client, so
    /// this wants to be short — the client's own [`DEFAULT_TIMEOUT`] is what
    /// the rest of the stack uses.
    ///
    /// [`DEFAULT_TIMEOUT`]: deepmsg_client::client::DEFAULT_TIMEOUT
    command_timeout: Duration,
    /// Reused across sends, as the reference's is.
    buffer: Vec<u8>,
}

impl ControlResponseProxy {
    /// A proxy that has not asked for its publication yet.
    pub fn new(command_timeout: Duration) -> Self {
        Self {
            publication: Publication::NotAskedFor,
            command_timeout,
            buffer: vec![0u8; INITIAL_BUFFER_LENGTH],
        }
    }

    /// The registration id the response publication is known by, once there is
    /// one.
    pub fn publication(&self) -> Option<i64> {
        match self.publication {
            Publication::InHand(registration_id) => Some(registration_id),
            _ => None,
        }
    }
}

impl Default for ControlResponseProxy {
    fn default() -> Self {
        Self::new(deepmsg_client::client::DEFAULT_TIMEOUT)
    }
}

impl Egress for ControlResponseProxy {
    fn add_publication<P: Publications>(
        &mut self,
        publications: &mut P,
        channel: &str,
        stream_id: i32,
    ) -> Result<(), ResponseError> {
        match &self.publication {
            // Asked for, and the answer is on its way: not asked for again
            // (`ControlSession.java:861-867`).
            Publication::Pending(_) | Publication::InHand(_) => return Ok(()),
            // The reference's throw, delivered where the session already knows
            // what to do with it.
            Publication::Failed(message) => {
                return Err(ResponseError::new(message.clone()));
            }
            Publication::NotAskedFor => {}
        }

        let registration_id = publications
            .async_add_exclusive_publication(channel, stream_id, self.command_timeout)
            .map_err(|error| {
                ResponseError::new(format!(
                    "control response publication could not be added: {error}"
                ))
            })?;

        self.publication = Publication::Pending(registration_id);

        Ok(())
    }

    fn is_publication_ready<P: Publications>(&mut self, publications: &mut P) -> bool {
        let Publication::Pending(registration_id) = self.publication else {
            // In hand is ready; **a refusal is not**, and answering `true` for
            // one would walk the session past this check and leave it offering
            // into a publication that does not exist. `false` sends it back
            // through `add_publication`, which is where a refusal is turned
            // into the session's ending.
            return matches!(self.publication, Publication::InHand(_));
        };

        match publications.poll_exclusive_publication(registration_id) {
            AsyncAddPoll::Ready => {
                self.publication = Publication::InHand(registration_id);
                true
            }
            // The reference's `RESOURCE_TEMPORARILY_UNAVAILABLE`: wait and look
            // again (`ControlSession.java:876-880`).
            AsyncAddPoll::Awaiting => false,
            AsyncAddPoll::Failed(error) => {
                self.publication = Publication::Failed(format!(
                    "control response publication could not be created: {error}"
                ));
                false
            }
            AsyncAddPoll::Unknown => {
                self.publication = Publication::Failed(
                    "control response publication could not be created: registration is not this \
                     client's"
                        .to_owned(),
                );
                false
            }
        }
    }

    fn is_connected<P: Publications>(&self, publications: &P) -> bool {
        match self.publication {
            Publication::InHand(registration_id) => {
                publications.is_exclusive_connected(registration_id)
            }
            _ => false,
        }
    }

    fn publication_registration_id(&self) -> Option<i64> {
        self.publication()
    }

    fn max_payload_length<P: Publications>(&self, publications: &P) -> usize {
        match self.publication {
            Publication::InHand(registration_id) => {
                publications.max_payload_length(registration_id)
            }
            _ => 0,
        }
    }

    fn offer<P: Publications>(&mut self, publications: &mut P, response: &Response) -> Offered {
        let Publication::InHand(registration_id) = self.publication else {
            // The reference dereferences a null and throws
            // (`ControlResponseProxy.java:132`); the publication it would have
            // written to is one this session never got.
            return Offered::Fatal(ResponseError::new(RESPONSE_PUBLICATION_CLOSED_MSG));
        };

        let length = encode(&mut self.buffer, response);

        for _ in 0..SEND_ATTEMPTS {
            match publications.offer_exclusive(registration_id, &self.buffer[..length]) {
                Some(Appended::Ok { .. }) => return Offered::Sent,

                // Java's `CLOSED`: the client no longer holds it, which is what
                // a `None` from the offer means here.
                None => {
                    return Offered::Fatal(ResponseError::new(RESPONSE_PUBLICATION_CLOSED_MSG));
                }

                Some(Appended::NotConnected) => {
                    return Offered::Fatal(ResponseError::new(RESPONSE_NOT_CONNECTED_MSG));
                }

                Some(Appended::MaxPositionExceeded) => {
                    return Offered::Fatal(ResponseError::new(
                        RESPONSE_PUBLICATION_MAX_POSITION_MSG,
                    ));
                }

                // `BACK_PRESSURED` and the log turning over: the reference
                // retries these and so does this. Its `ADMIN_ACTION` is this
                // build's `EndOfLog` and `MidRotation`, and the two that have
                // no reference counterpart (`MessageTooLarge`, `Malformed`)
                // are retried for the same reason the reference retries what it
                // does not name: nothing here can mend them, and the turn after
                // this one is another chance.
                Some(_) => {}
            }
        }

        Offered::Retry
    }

    fn close_publication<P: Publications>(&mut self, publications: &mut P) {
        let Publication::InHand(registration_id) = self.publication else {
            // Nothing was obtained, so there is nothing to give back — the
            // reference's `asyncRemovePublication` arm
            // (`ControlSession.java:171-174`), which names the id it is still
            // waiting on. A pending add is cancelled by the client's own
            // deadline either way, and this one's answer is not wanted.
            self.publication = Publication::NotAskedFor;
            return;
        };

        publications.release_exclusive(registration_id, self.command_timeout);
        self.publication = Publication::NotAskedFor;
    }
}

/// Encode one response into `buffer`, answering with its length.
///
/// The header is at zero and the body at [`MESSAGE_HEADER_LENGTH`], which is
/// the arrangement every message in this protocol has and the one the decoder
/// reads back (`ControlResponseProxy.java:63-65`).
///
/// Every field is written, including the optional ones, so that a buffer reused
/// across sends cannot carry anything from the message before it.
fn encode(buffer: &mut Vec<u8>, response: &Response) -> usize {
    match response {
        Response::Control {
            control_session_id,
            correlation_id,
            relevant_id,
            code,
            message,
        } => {
            let error_message = message.as_deref().unwrap_or("").as_bytes();
            grow(
                buffer,
                MESSAGE_HEADER_LENGTH
                    + control_response_codec::SBE_BLOCK_LENGTH as usize
                    + VAR_DATA_LENGTH_PREFIX
                    + error_message.len(),
            );

            let encoder = ControlResponseEncoder::default()
                .wrap(WriteBuf::new(buffer), MESSAGE_HEADER_LENGTH);
            let mut header = encoder.header(0);
            let mut encoder = header.parent().unwrap();

            encoder
                .control_session_id(*control_session_id)
                .correlation_id(*correlation_id)
                .relevant_id(*relevant_id)
                .code(*code)
                .version(PROTOCOL_SEMANTIC_VERSION)
                .error_message(error_message);

            MESSAGE_HEADER_LENGTH + encoder.encoded_length()
        }

        Response::Challenge {
            control_session_id,
            correlation_id,
            encoded_challenge,
        } => {
            grow(
                buffer,
                MESSAGE_HEADER_LENGTH
                    + challenge_codec::SBE_BLOCK_LENGTH as usize
                    + VAR_DATA_LENGTH_PREFIX
                    + encoded_challenge.len(),
            );

            let encoder =
                ChallengeEncoder::default().wrap(WriteBuf::new(buffer), MESSAGE_HEADER_LENGTH);
            let mut header = encoder.header(0);
            let mut encoder = header.parent().unwrap();

            encoder
                .control_session_id(*control_session_id)
                .correlation_id(*correlation_id)
                // See the module note: the reference leaves this field as it
                // finds it, and this writes its null value.
                .version_opt(None)
                .encoded_challenge(encoded_challenge);

            MESSAGE_HEADER_LENGTH + encoder.encoded_length()
        }

        // The catalog's own bytes, and only the two ids written over them: the
        // reference offers the same body in two parts for the same reason —
        // it comes out of a mapped file (`ControlResponseProxy.java:54-89`).
        //
        // The block this writes is `RecordingDescriptor`'s, so the message
        // header the client reads says a descriptor is what follows and how
        // long its fixed part is; the body carries the rest, strings and all.
        Response::Descriptor {
            control_session_id,
            correlation_id,
            body,
        } => {
            grow(buffer, MESSAGE_HEADER_LENGTH + body.len());
            buffer[MESSAGE_HEADER_LENGTH..MESSAGE_HEADER_LENGTH + body.len()].copy_from_slice(body);

            let encoder = RecordingDescriptorEncoder::default()
                .wrap(WriteBuf::new(buffer), MESSAGE_HEADER_LENGTH);
            let mut header = encoder.header(0);
            let mut encoder = header.parent().unwrap();

            encoder
                .control_session_id(*control_session_id)
                .correlation_id(*correlation_id);

            MESSAGE_HEADER_LENGTH + body.len()
        }

        // A subscription descriptor: `controlSessionId`, `correlationId`,
        // `subscriptionId`, `streamId` and the channel, which is the only
        // variable-length field (`RecordingSubscriptionDescriptorEncoder`).
        Response::SubscriptionDescriptor {
            control_session_id,
            correlation_id,
            subscription_id,
            stream_id,
            channel,
        } => {
            grow(
                buffer,
                MESSAGE_HEADER_LENGTH
                    + recording_subscription_descriptor_codec::SBE_BLOCK_LENGTH as usize
                    + VAR_DATA_LENGTH_PREFIX
                    + channel.len(),
            );

            let encoder = RecordingSubscriptionDescriptorEncoder::default()
                .wrap(WriteBuf::new(buffer), MESSAGE_HEADER_LENGTH);
            let mut header = encoder.header(0);
            let mut encoder = header.parent().unwrap();

            encoder
                .control_session_id(*control_session_id)
                .correlation_id(*correlation_id)
                .subscription_id(*subscription_id)
                .stream_id(*stream_id)
                .stripped_channel(channel.as_bytes());

            MESSAGE_HEADER_LENGTH + encoder.encoded_length()
        }

        Response::Ping { control_session_id } => {
            grow(
                buffer,
                MESSAGE_HEADER_LENGTH + ping_codec::SBE_BLOCK_LENGTH as usize,
            );

            let encoder = PingEncoder::default().wrap(WriteBuf::new(buffer), MESSAGE_HEADER_LENGTH);
            let mut header = encoder.header(0);
            let mut encoder = header.parent().unwrap();

            encoder.control_session_id(*control_session_id);

            MESSAGE_HEADER_LENGTH + encoder.encoded_length()
        }

        // Fixed length: every field of a `RecordingSignalEvent` is in its block
        // (`RecordingSignalEventEncoder.BLOCK_LENGTH`), so there is no var-data
        // and no `grow` by anything but the block.
        Response::Signal {
            control_session_id,
            correlation_id,
            recording_id,
            subscription_id,
            position,
            signal,
        } => {
            grow(
                buffer,
                MESSAGE_HEADER_LENGTH + recording_signal_event_codec::SBE_BLOCK_LENGTH as usize,
            );

            let encoder = RecordingSignalEventEncoder::default()
                .wrap(WriteBuf::new(buffer), MESSAGE_HEADER_LENGTH);
            let mut header = encoder.header(0);
            let mut encoder = header.parent().unwrap();

            encoder
                .control_session_id(*control_session_id)
                .correlation_id(*correlation_id)
                .recording_id(*recording_id)
                .subscription_id(*subscription_id)
                .position(*position)
                .signal(*signal);

            MESSAGE_HEADER_LENGTH + encoder.encoded_length()
        }
    }
}

/// Make sure there is room for `length` bytes, and no more than there has to be.
fn grow(buffer: &mut Vec<u8>, length: usize) {
    if buffer.len() < length {
        buffer.resize(length, 0);
    }
}

/// A client of a media driver, seen as the publications a session answers on.
///
/// Each method is the reference's `Aeron` or `Publication` call under the name
/// this build gives it, and the one thing worth stating is what is *not* here:
/// polling the client. The reference's conductor invokes the client's agent
/// itself (`ArchiveConductor.java:377-380`), so a publication's answer is
/// visible by the time a session looks for it; this build's conductor polls the
/// client once a turn, and these methods only read what that poll brought in.
impl Publications for Client {
    fn async_add_exclusive_publication(
        &mut self,
        channel: &str,
        stream_id: i32,
        timeout: Duration,
    ) -> Result<i64, CommandError> {
        Client::async_add_exclusive_publication(self, channel, stream_id, timeout)
            .map(|add| add.registration_id())
    }

    fn poll_exclusive_publication(&mut self, registration_id: i64) -> AsyncAddPoll {
        // The handle is put back together from the id here and nowhere else:
        // the client's `async_add_poll` is keyed by the id, and the id is what
        // the archive kept.
        self.async_add_poll(AsyncAdd::publication(registration_id))
    }

    fn is_exclusive_connected(&self, registration_id: i64) -> bool {
        self.exclusive_publication(registration_id)
            .and_then(|publication| publication.is_connected())
            .unwrap_or(false)
    }

    fn max_payload_length(&self, registration_id: i64) -> usize {
        self.exclusive_publication(registration_id)
            .and_then(|publication| publication.max_payload_length())
            .unwrap_or(0)
    }

    fn offer_exclusive(&mut self, registration_id: i64, payload: &[u8]) -> Option<Appended> {
        Client::offer_exclusive(self, registration_id, payload)
    }

    fn release_exclusive(&mut self, registration_id: i64, timeout: Duration) {
        // `revokeOnClose()` then close (`ControlSession.java:166-170`). The
        // reference's close waits for the driver to answer; a poll-driven
        // conductor cannot wait, and the client's own deadline is what finishes
        // a removal that is never polled for.
        self.revoke_publication_on_close(registration_id);
        let _ = Client::async_remove_publication(self, registration_id, timeout);
    }

    fn async_remove_publication(&mut self, registration_id: i64, timeout: Duration) {
        let _ = Client::async_remove_publication(self, registration_id, timeout);
    }
}

/// The same client, seen by a replay.
///
/// Separate from the `Publications` impl above for the reason the trait is
/// separate: past the registration id the two have nothing in common — a
/// control session offers an encoded `Response`, a replay offers a block of
/// recorded frames — and a replay reads four header words the control session
/// never asks for.
impl ReplayPublications for Client {
    fn is_connected(&self, registration_id: i64) -> bool {
        self.exclusive_publication(registration_id)
            .and_then(|publication| publication.is_connected())
            .unwrap_or(false)
    }

    fn facts(&self, registration_id: i64) -> Option<PublicationFacts> {
        let publication = self.exclusive_publication(registration_id)?;

        Some(PublicationFacts {
            session_id: publication.session_id(),
            stream_id: publication.stream_id(),
            position_bits_to_shift: publication.position_bits_to_shift(),
            initial_term_id: publication.initial_term_id(),
        })
    }

    fn available_window(&self, registration_id: i64) -> Option<i64> {
        Client::available_window(self, registration_id)
    }

    fn offer_block(&mut self, registration_id: i64, block: &[u8]) -> Option<Appended> {
        Client::offer_block_exclusive(self, registration_id, block)
    }

    fn append_padding(&mut self, registration_id: i64, length: usize) -> Option<Appended> {
        Client::append_padding_exclusive(self, registration_id, length)
    }

    fn release_publication(&mut self, registration_id: i64, timeout: Duration) {
        // `revokeOnClose()` then an **asynchronous** removal, which is the same
        // command [`Publications::release_exclusive`] sends and the same one the
        // reference sends — marked, then closed, with the driver's answer left
        // for a later poll.
        //
        // It must not be `Client::revoke_publication`, which waits for the
        // driver: the archive's conductor and the driver share a thread in
        // `deepmsg-archiving-media-driver`, so a command that waits for the
        // driver is a command that waits for the turn that would answer it. The
        // stall is not merely slow — the driver's clock jumps by the wait, every
        // network publication's receivers expire together, and the maintenance
        // pass then writes `is_connected = 0` into a publication whose subscriber
        // is still there. That is what killed the control session that asked for
        // the replay's stop.
        self.revoke_publication_on_close(registration_id);
        let _ = Client::async_remove_publication(self, registration_id, timeout);
    }

    fn close_publication(&mut self, registration_id: i64, timeout: Duration) {
        // A plain removal, with no revoke flag: the driver's linger is what
        // keeps the publication alive long enough to retransmit its tail.
        let _ = Client::async_remove_publication(self, registration_id, timeout);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use deepmsg_codec::archive::challenge_codec::ChallengeDecoder;
    use deepmsg_codec::archive::control_response_code::ControlResponseCode;
    use deepmsg_codec::archive::control_response_codec::ControlResponseDecoder;
    use deepmsg_codec::archive::message_header_codec::MessageHeaderDecoder;
    use deepmsg_codec::archive::ping_codec::PingDecoder;
    use deepmsg_codec::archive::recording_signal::RecordingSignal;
    use deepmsg_codec::archive::recording_signal_event_codec::RecordingSignalEventDecoder;
    use deepmsg_codec::archive::{ReadBuf, SBE_SCHEMA_ID};
    use deepmsg_core::logbuffer::position::Position;

    /// The registration id the fake client answers an add with.
    const REGISTRATION_ID: i64 = 1;

    /// A publications side that answers each offer with what the test told it
    /// to, and writes down what it was offered.
    #[derive(Default)]
    struct FakePublications {
        /// One answer per offer, and the last one repeats once the list runs
        /// out.
        answers: Vec<Option<Appended>>,
        offered: Vec<Vec<u8>>,
        /// How many publications were asked for, which the reference asks for
        /// at most once (`ControlSession.java:861-867`).
        adds: usize,
        /// What the driver says about the publication, which is `Ready` unless
        /// a test is about the other two answers.
        poll: Poll,
    }

    /// What `poll_exclusive_publication` answers, as a copyable stand-in for
    /// [`AsyncAddPoll`] — which is not `Clone`, and should not be, since
    /// `CommandError` can carry an `io::Error`.
    #[derive(Clone, Copy, Default, PartialEq, Eq)]
    enum Poll {
        #[default]
        Ready,
        /// `RESOURCE_TEMPORARILY_UNAVAILABLE`, which the reference waits on.
        Awaiting,
        /// The driver refused it, which the reference **throws** on
        /// (`ControlSession.java:876-881`).
        Refused,
    }

    impl FakePublications {
        fn last_offered(&self) -> &[u8] {
            self.offered.last().expect("something was offered")
        }
    }

    impl Publications for FakePublications {
        fn async_add_exclusive_publication(
            &mut self,
            _channel: &str,
            _stream_id: i32,
            _timeout: Duration,
        ) -> Result<i64, CommandError> {
            self.adds += 1;
            Ok(REGISTRATION_ID)
        }

        fn poll_exclusive_publication(&mut self, _registration_id: i64) -> AsyncAddPoll {
            match self.poll {
                Poll::Ready => AsyncAddPoll::Ready,
                Poll::Awaiting => AsyncAddPoll::Awaiting,
                Poll::Refused => AsyncAddPoll::Failed(CommandError::Encoding),
            }
        }

        fn async_remove_publication(&mut self, _registration_id: i64, _timeout: Duration) {}

        fn is_exclusive_connected(&self, _registration_id: i64) -> bool {
            true
        }

        fn max_payload_length(&self, _registration_id: i64) -> usize {
            1024
        }

        fn offer_exclusive(&mut self, _registration_id: i64, payload: &[u8]) -> Option<Appended> {
            self.offered.push(payload.to_vec());

            let index = self.offered.len() - 1;
            self.answers
                .get(index)
                .or_else(|| self.answers.last())
                .copied()
                .unwrap_or(Some(Appended::Ok {
                    position: Position::from_raw(1),
                    term_offset: 0,
                }))
        }

        fn release_exclusive(&mut self, _registration_id: i64, _timeout: Duration) {}
    }

    /// A proxy whose publication is already in hand.
    fn a_proxy() -> (ControlResponseProxy, FakePublications) {
        let mut proxy = ControlResponseProxy::new(Duration::from_secs(1));
        let mut publications = FakePublications::default();

        proxy
            .add_publication(&mut publications, "aeron:udp?endpoint=localhost:0", 20)
            .expect("the fake client accepts the add");

        assert!(
            proxy.is_publication_ready(&mut publications),
            "and answers it at once"
        );

        (proxy, publications)
    }

    fn a_control_response(message: Option<&str>) -> Response {
        Response::Control {
            control_session_id: 7,
            correlation_id: 11,
            relevant_id: 13,
            code: ControlResponseCode::ERROR,
            message: message.map(str::to_owned),
        }
    }

    /// The `ControlResponse` a client reads back is the one the session queued,
    /// down to the protocol version the reference stamps on it.
    #[test]
    fn a_control_response_decodes_back_to_what_was_sent() {
        let (mut proxy, mut publications) = a_proxy();

        assert_eq!(
            Offered::Sent,
            proxy.offer(
                &mut publications,
                &a_control_response(Some("unauthorised action"))
            )
        );

        let payload = publications.last_offered();
        let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(payload), 0);
        assert_eq!(SBE_SCHEMA_ID, header.schema_id());

        let mut decoder = ControlResponseDecoder::default().header(header, 0);
        assert_eq!(7, decoder.control_session_id());
        assert_eq!(11, decoder.correlation_id());
        assert_eq!(13, decoder.relevant_id());
        assert_eq!(ControlResponseCode::ERROR, decoder.code());
        assert_eq!(Some(PROTOCOL_SEMANTIC_VERSION), decoder.version());

        let coordinates = decoder.error_message_decoder();
        assert_eq!(
            b"unauthorised action",
            decoder.error_message_slice(coordinates)
        );
    }

    /// An OK carries no message, and its var-data field is empty rather than
    /// absent: the reference's `sendOkResponse` passes a null for it
    /// (`ControlSession.java:682-690`).
    #[test]
    fn an_ok_carries_an_empty_message() {
        let (mut proxy, mut publications) = a_proxy();

        let ok = Response::Control {
            control_session_id: 7,
            correlation_id: 11,
            relevant_id: 7,
            code: ControlResponseCode::OK,
            message: None,
        };
        assert_eq!(Offered::Sent, proxy.offer(&mut publications, &ok));

        let payload = publications.last_offered();
        let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(payload), 0);
        let mut decoder = ControlResponseDecoder::default().header(header, 0);

        assert_eq!(ControlResponseCode::OK, decoder.code());
        let coordinates = decoder.error_message_decoder();
        assert!(decoder.error_message_slice(coordinates).is_empty());
    }

    /// A challenge carries its bytes and the correlation id it answers.
    #[test]
    fn a_challenge_decodes_back_to_what_was_sent() {
        let (mut proxy, mut publications) = a_proxy();

        let challenge = Response::Challenge {
            control_session_id: 7,
            correlation_id: 11,
            encoded_challenge: b"a challenge".to_vec(),
        };
        assert_eq!(Offered::Sent, proxy.offer(&mut publications, &challenge));

        let payload = publications.last_offered();
        let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(payload), 0);
        assert_eq!(SBE_SCHEMA_ID, header.schema_id());

        let mut decoder = ChallengeDecoder::default().header(header, 0);
        assert_eq!(7, decoder.control_session_id());
        assert_eq!(11, decoder.correlation_id());
        assert_eq!(None, decoder.version(), "the field is written null");

        let coordinates = decoder.encoded_challenge_decoder();
        assert_eq!(b"a challenge", decoder.encoded_challenge_slice(coordinates));
    }

    /// A descriptor is a `RecordingDescriptor` message whose body is the
    /// **catalog's own bytes** — so this one is built by a catalog rather than
    /// by the test, which is the whole point: the two halves have to fit without
    /// either of them knowing about the other
    /// (`Catalog.wrapDescriptor` / `ControlResponseProxy.sendDescriptor`,
    /// `Catalog.java:464-493` and `ControlResponseProxy.java:54-89`).
    #[test]
    fn a_catalogs_descriptor_body_is_a_message() {
        use crate::catalog::{Catalog, DEFAULT_CAPACITY, Recording};
        use crate::mark::tests::TempDir;
        use deepmsg_codec::archive::recording_descriptor_codec::{
            self, RecordingDescriptorDecoder,
        };

        let dir = TempDir::new();
        let mut catalog = Catalog::create(dir.path(), DEFAULT_CAPACITY, 100).expect("a catalog");

        let written = Recording {
            recording_id: 0,
            start_timestamp: 1_700_000_000_000,
            stop_timestamp: 1_700_000_001_000,
            start_position: 4096,
            stop_position: 8192,
            initial_term_id: 7,
            segment_file_length: 128 * 1024,
            term_buffer_length: 64 * 1024,
            mtu_length: 1408,
            session_id: 1001,
            stream_id: 33,
            stripped_channel: "aeron:udp?endpoint=localhost:3333".to_owned(),
            original_channel: "aeron:udp?endpoint=localhost:3333".to_owned(),
            source_identity: "aeron:ipc".to_owned(),
        };
        let recording_id = catalog.add_recording(&written).expect("added");
        let body = catalog
            .descriptor_body(recording_id)
            .expect("a body")
            .expect("the record is there");

        let (mut proxy, mut publications) = a_proxy();
        assert_eq!(
            Offered::Sent,
            proxy.offer(
                &mut publications,
                &Response::Descriptor {
                    control_session_id: 7,
                    correlation_id: 11,
                    body,
                }
            )
        );

        let payload = publications.last_offered();
        let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(payload), 0);
        assert_eq!(SBE_SCHEMA_ID, header.schema_id());
        assert_eq!(
            recording_descriptor_codec::SBE_TEMPLATE_ID,
            header.template_id(),
            "the client switches on this to know a descriptor arrived"
        );

        let mut decoder = RecordingDescriptorDecoder::default().header(header, 0);
        assert_eq!(7, decoder.control_session_id());
        assert_eq!(11, decoder.correlation_id());
        assert_eq!(recording_id, decoder.recording_id());
        assert_eq!(written.start_timestamp, decoder.start_timestamp());
        assert_eq!(written.stop_timestamp, decoder.stop_timestamp());
        assert_eq!(written.start_position, decoder.start_position());
        assert_eq!(written.stop_position, decoder.stop_position());
        assert_eq!(written.initial_term_id, decoder.initial_term_id());
        assert_eq!(written.segment_file_length, decoder.segment_file_length());
        assert_eq!(written.term_buffer_length, decoder.term_buffer_length());
        assert_eq!(written.mtu_length, decoder.mtu_length());
        assert_eq!(written.session_id, decoder.session_id());
        assert_eq!(written.stream_id, decoder.stream_id());

        let coordinates = decoder.stripped_channel_decoder();
        assert_eq!(
            written.stripped_channel.as_bytes(),
            decoder.stripped_channel_slice(coordinates)
        );
        let coordinates = decoder.original_channel_decoder();
        assert_eq!(
            written.original_channel.as_bytes(),
            decoder.original_channel_slice(coordinates)
        );
        let coordinates = decoder.source_identity_decoder();
        assert_eq!(
            written.source_identity.as_bytes(),
            decoder.source_identity_slice(coordinates)
        );
    }

    /// A ping carries the session and nothing else.
    #[test]
    fn a_ping_decodes_back_to_what_was_sent() {
        let (mut proxy, mut publications) = a_proxy();

        let ping = Response::Ping {
            control_session_id: 7,
        };
        assert_eq!(Offered::Sent, proxy.offer(&mut publications, &ping));

        let payload = publications.last_offered();
        let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(payload), 0);
        let decoder = PingDecoder::default().header(header, 0);

        assert_eq!(7, decoder.control_session_id());
    }

    /// The signal is the third message a session can send, and the only one
    /// that is not an answer — so it is worth checking on the wire rather than
    /// by name.
    #[test]
    fn a_signal_decodes_back_to_what_was_sent() {
        let (mut proxy, mut publications) = a_proxy();

        let signal = Response::Signal {
            control_session_id: 7,
            correlation_id: 11,
            recording_id: 3,
            subscription_id: 5,
            position: 4096,
            signal: RecordingSignal::EXTEND,
        };
        assert_eq!(Offered::Sent, proxy.offer(&mut publications, &signal));

        let payload = publications.last_offered();
        let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(payload), 0);

        assert_eq!(
            recording_signal_event_codec::SBE_TEMPLATE_ID,
            header.template_id()
        );
        assert_eq!(
            recording_signal_event_codec::SBE_BLOCK_LENGTH,
            header.block_length()
        );

        let decoder = RecordingSignalEventDecoder::default().header(header, 0);

        assert_eq!(7, decoder.control_session_id());
        assert_eq!(11, decoder.correlation_id());
        assert_eq!(3, decoder.recording_id());
        assert_eq!(5, decoder.subscription_id());
        assert_eq!(4096, decoder.position());
        assert_eq!(RecordingSignal::EXTEND, decoder.signal());
    }

    /// The reused buffer is not allowed to show through: a challenge after a
    /// response with a long error message carries none of it.
    ///
    /// This is the deviation the module note describes, and the test is what
    /// keeps it from being a claim: the reference never writes the challenge's
    /// `version` field, so what it carries there is the previous message's
    /// bytes.
    #[test]
    fn a_message_carries_nothing_of_the_one_before_it() {
        let (mut proxy, mut publications) = a_proxy();

        let long = "a".repeat(400);
        assert_eq!(
            Offered::Sent,
            proxy.offer(&mut publications, &a_control_response(Some(&long)))
        );
        assert_eq!(400, control_response_length(publications.last_offered()));

        let challenge = Response::Challenge {
            control_session_id: 7,
            correlation_id: 11,
            encoded_challenge: Vec::new(),
        };
        assert_eq!(Offered::Sent, proxy.offer(&mut publications, &challenge));

        let payload = publications.last_offered();
        assert_eq!(
            MESSAGE_HEADER_LENGTH
                + challenge_codec::SBE_BLOCK_LENGTH as usize
                + VAR_DATA_LENGTH_PREFIX,
            payload.len(),
            "the challenge is its own length, not the message before it"
        );

        let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(payload), 0);
        assert_eq!(challenge_codec::SBE_TEMPLATE_ID, header.template_id());
        assert_eq!(
            None,
            ChallengeDecoder::default().header(header, 0).version()
        );
    }

    fn control_response_length(payload: &[u8]) -> usize {
        let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(payload), 0);
        let mut decoder = ControlResponseDecoder::default().header(header, 0);
        let coordinates = decoder.error_message_decoder();

        decoder.error_message_slice(coordinates).len()
    }

    /// A publication that is not connected ends the session, in the reference's
    /// words (`ControlResponseProxy.java:244-248`).
    #[test]
    fn a_not_connected_publication_ends_the_session() {
        let (mut proxy, mut publications) = a_proxy();
        publications.answers = vec![Some(Appended::NotConnected)];

        assert_eq!(
            Offered::Fatal(ResponseError::new(RESPONSE_NOT_CONNECTED_MSG)),
            proxy.offer(&mut publications, &a_control_response(None))
        );
        assert_eq!(1, publications.offered.len(), "and it is not tried again");
    }

    /// So does one the client no longer holds — Java's `CLOSED`
    /// (`ControlResponseProxy.java:250-254`).
    #[test]
    fn a_publication_that_is_gone_ends_the_session() {
        let (mut proxy, mut publications) = a_proxy();
        publications.answers = vec![None];

        assert_eq!(
            Offered::Fatal(ResponseError::new(RESPONSE_PUBLICATION_CLOSED_MSG)),
            proxy.offer(&mut publications, &a_control_response(None))
        );
    }

    /// And one at its maximum position (`ControlResponseProxy.java:256-261`).
    #[test]
    fn a_publication_at_its_maximum_position_ends_the_session() {
        let (mut proxy, mut publications) = a_proxy();
        publications.answers = vec![Some(Appended::MaxPositionExceeded)];

        assert_eq!(
            Offered::Fatal(ResponseError::new(RESPONSE_PUBLICATION_MAX_POSITION_MSG)),
            proxy.offer(&mut publications, &a_control_response(None))
        );
    }

    /// A full window is not a reason to end anything: the reference tries three
    /// times and leaves the response where it is (`:129-143`), and the next
    /// turn is another three tries.
    #[test]
    fn a_full_window_is_tried_three_times_and_then_left_alone() {
        let (mut proxy, mut publications) = a_proxy();
        publications.answers = vec![Some(Appended::BackPressured)];

        assert_eq!(
            Offered::Retry,
            proxy.offer(&mut publications, &a_control_response(None))
        );
        assert_eq!(SEND_ATTEMPTS, publications.offered.len());
    }

    /// A log turning over is the same kind of answer — the reference's
    /// `ADMIN_ACTION`, this build's two ways of saying the term is being
    /// rotated.
    #[test]
    fn a_log_mid_rotation_is_a_retry_too() {
        let (mut proxy, mut publications) = a_proxy();
        publications.answers = vec![Some(Appended::EndOfLog), Some(Appended::MidRotation)];

        assert_eq!(
            Offered::Retry,
            proxy.offer(&mut publications, &a_control_response(None))
        );
        assert_eq!(SEND_ATTEMPTS, publications.offered.len());
    }

    /// A second attempt can take where the first did not, which is what the
    /// three are for.
    #[test]
    fn a_second_attempt_can_send_what_the_first_could_not() {
        let (mut proxy, mut publications) = a_proxy();
        publications.answers = vec![
            Some(Appended::BackPressured),
            Some(Appended::BackPressured),
            Some(Appended::Ok {
                position: Position::from_raw(1),
                term_offset: 0,
            }),
        ];

        assert_eq!(
            Offered::Sent,
            proxy.offer(&mut publications, &a_control_response(None))
        );
        assert_eq!(SEND_ATTEMPTS, publications.offered.len());
    }

    /// A publication the driver **refuses** ends the session, rather than
    /// leaving it polling a registration that will never answer.
    ///
    /// The reference throws here (`ControlSession.java:876-881` rethrows
    /// anything that is not `RESOURCE_TEMPORARILY_UNAVAILABLE`), and the throw
    /// climbs out of `doWork`. This build cannot throw out of a turn, so
    /// `is_publication_ready` keeps the reason and the next `add_publication` —
    /// which the session makes every turn it is not ready — answers with it,
    /// which the session already knows how to end on.
    #[test]
    fn a_refused_publication_is_answered_with_its_reason() {
        let mut proxy = ControlResponseProxy::new(Duration::from_secs(1));
        let mut publications = FakePublications {
            poll: Poll::Refused,
            ..FakePublications::default()
        };

        proxy
            .add_publication(&mut publications, "aeron:ipc", 20)
            .expect("added");

        assert!(
            !proxy.is_publication_ready(&mut publications),
            "a refusal is not readiness"
        );

        let error = proxy
            .add_publication(&mut publications, "aeron:ipc", 20)
            .expect_err("and the session is told why");

        assert!(
            error
                .message()
                .starts_with("control response publication could not be created"),
            "{error}"
        );
        assert_eq!(1, publications.adds, "and nothing was asked for again");

        // Asked again — which the session does every turn it is not ready — it
        // is still not ready. Answering `true` for a refusal would walk the
        // session past this check and leave it offering into a publication that
        // does not exist, which is the silence this path exists to avoid.
        assert!(!proxy.is_publication_ready(&mut publications));
        assert_eq!(None, proxy.publication(), "and it is not in hand");
    }

    /// A driver that has not answered is **not** a refusal: the session waits,
    /// and the registration is not asked for a second time.
    #[test]
    fn a_publication_the_driver_has_not_answered_yet_is_waited_on() {
        let mut proxy = ControlResponseProxy::new(Duration::from_secs(1));
        let mut publications = FakePublications {
            poll: Poll::Awaiting,
            ..FakePublications::default()
        };

        proxy
            .add_publication(&mut publications, "aeron:ipc", 20)
            .expect("added");

        assert!(!proxy.is_publication_ready(&mut publications));
        proxy
            .add_publication(&mut publications, "aeron:ipc", 20)
            .expect("waited on, not refused");

        assert_eq!(1, publications.adds, "asked for once");
        assert_eq!(None, proxy.publication(), "and not in hand");
    }

    /// Nothing is asked for twice while the driver has not answered
    /// (`ControlSession.java:861-867`).
    #[test]
    fn a_publication_already_asked_for_is_not_asked_for_again() {
        let mut proxy = ControlResponseProxy::new(Duration::from_secs(1));
        let mut publications = FakePublications::default();

        proxy
            .add_publication(&mut publications, "aeron:ipc", 20)
            .expect("added");
        proxy
            .add_publication(&mut publications, "aeron:ipc", 20)
            .expect("and again");

        assert_eq!(1, publications.adds, "asked for once, not twice");
        assert!(
            proxy.is_publication_ready(&mut publications),
            "and the answer is taken up"
        );
        assert_eq!(Some(REGISTRATION_ID), proxy.publication());
    }
}
