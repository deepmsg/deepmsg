//! The requests that arrive, and which session each one belongs to.
//!
//! The reference's `ControlSessionAdapter` is a `FragmentHandler` the conductor
//! polls (`ControlSessionAdapter.java:77`): it reads two subscriptions, turns
//! the fragments back into whole messages, and switches on the message's
//! `templateId` to decide what the request is. This is the same class of work
//! with the same switch, and two of its decisions are worth reading before the
//! code:
//!
//! * **Which image a session belongs to is part of the session's identity.**
//!   Every request that is not a connect goes through
//!   [`ControlAdapter::on_message`]'s gate, and a request that arrives on a
//!   *different* image than the one its session was created from is refused
//!   without a word to the client (`ControlSessionAdapter.java:1198-1203`) —
//!   the reference logs it and drops it. A session created over UDP is
//!   therefore not drivable over IPC, and the two control subscriptions, which
//!   share stream 10, are what makes that matter
//!   (`ArchiveConductor.java:239-240`).
//! * **Authorisation is asked once per request, by template id**, and a refusal
//!   is the one gate that answers the client: `ERROR` carrying
//!   `ArchiveException.UNAUTHORISED_ACTION` as the relevant id and
//!   `"unauthorised action"` as the message (`:1205-1216`).
//!
//! # Two subscriptions, one assembler, ten fragments each
//!
//! `poll()` reads the control subscription **if there is one** and then the
//! local one, each for `FRAGMENT_LIMIT` fragments, both through the *same*
//! assembler (`ControlSessionAdapter.java:87`, `:107-119`). The local
//! subscription is the one the reference always creates
//! (`ArchiveConductor.java:240`); the remote one is the one
//! `aeron.archive.control.channel.enabled` can take away.
//!
//! # What is not the reference's shape, and why
//!
//! * **The sessions are not here.** The reference's adapter holds a map from
//!   session id to `(image, controlSession)` (`:88`) while the conductor's
//!   `SessionWorker` holds the same sessions in its own list
//!   (`SessionWorker.java:56-81`) — two owners of one object, which Java
//!   permits and this language does not. The split here is the one the borrow
//!   checker forces and nothing more: the adapter keeps each session's
//!   **identity**, and [`ControlPlane`] is asked for everything that has to
//!   touch the session itself.
//!
//!   That is also why the control plane is **lent per call** rather than held.
//!   The reference hands its conductor to the adapter's constructor and the
//!   conductor keeps the adapter (`ArchiveConductor.java:242-243`) — one
//!   object reachable from the other both ways, which is free in Java and
//!   impossible for a value. The client's publication side gets the same
//!   treatment for the same reason.
//! * **The image is identified by what the driver assigned it.** The reference
//!   compares `Image` *objects*; this build's `Image` is borrowed from the
//!   subscription for the length of a poll and cannot be held. [`ImageId`] is
//!   what the driver put on the publication, which is the same distinction.
//! * **A message's schema is refused with a value, not an exception.** The
//!   reference throws `ArchiveException` out of `onFragment` (`:136`),
//!   which the archive's main catches and exits on; [`ControlError`] is that
//!   throw, one stack frame earlier.
//! * **The decoders are built per message rather than held.** The reference's
//!   `ControlRequestDecoders` preallocates one per template
//!   (`ControlRequestDecoders.java:60-104`) because each is an object holding a
//!   buffer reference it must be `wrap`ped onto afresh anyway. Here a decoder
//!   *is* the offsets — `Copy` to a pair of `usize` — so building one is the
//!   wrap the reference performs, and there is no allocation to hoist.
//! * **The log lines name the image differently.** The reference prints
//!   `source=image.sourceIdentity()`; that string belongs to the `Image`, which
//!   this adapter does not hold, so the lines carry the publication's
//!   correlation id and session instead. The reference's log text is not a
//!   contract — nothing reads it but a person.

use std::collections::HashMap;
use std::fmt;

use deepmsg_client::client::Client;
use deepmsg_client::fragment_assembler::{FragmentAssembler, Message};
use deepmsg_client::image::Fragment;
use deepmsg_codec::archive::archive_id_request_codec::ArchiveIdRequestDecoder;
use deepmsg_codec::archive::auth_connect_request_codec::AuthConnectRequestDecoder;
use deepmsg_codec::archive::challenge_response_codec::ChallengeResponseDecoder;
use deepmsg_codec::archive::close_session_request_codec::CloseSessionRequestDecoder;
use deepmsg_codec::archive::keep_alive_request_codec::KeepAliveRequestDecoder;
use deepmsg_codec::archive::message_header_codec::{self, MessageHeaderDecoder};
use deepmsg_codec::archive::{
    ReadBuf, SBE_SCHEMA_ID, archive_id_request_codec, auth_connect_request_codec,
    challenge_response_codec, close_session_request_codec, keep_alive_request_codec,
};

use crate::server::auth::AuthorisationService;
use crate::server::control_session::SESSION_CLOSED_MSG;

/// How many fragments one subscription is read for in a turn
/// (`ControlSessionAdapter.java:79`).
pub const FRAGMENT_LIMIT: usize = 10;

/// `ArchiveException.UNAUTHORISED_ACTION` (`client/ArchiveException.java:94`),
/// the relevant id of the one `ERROR` a gate here sends.
///
/// The rest of that class's table is the client track's: it is what a *client*
/// turns a response's relevant id back into, and this slice needs the one
/// number it sends. The other id that class has is its `GENERIC`, zero
/// (`:29`) — what a session sends when its error is about nothing in
/// particular.
pub const UNAUTHORISED_ACTION: i32 = 13;

/// Where the error text of a refusal comes from
/// (`ControlSessionAdapter.java:1212-1213`).
pub const UNAUTHORISED_ACTION_MSG: &str = "unauthorised action";

/// Which control image a request arrived on.
///
/// The reference compares the `Image` **object** the request arrived on with
/// the one its session was created from (`ControlSessionAdapter.java:1198`) —
/// object identity, and the only thing that keeps a session made over the UDP
/// control channel from being driven by a request that came in over IPC. There
/// is no object to compare here: an `Image` is borrowed from the subscription
/// for the length of a poll, so what is kept is what the driver put on the
/// publication — its registration id, which is `Image.correlationId()`, the id
/// the conductor writes into a response channel that asked for one
/// (`ArchiveConductor.java:478-481`) — together with the session the driver
/// gave it, which is what a log line wants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ImageId {
    /// The **publication's** registration id (`Image.correlationId()`).
    correlation_id: i64,
    /// The session id the driver gave that publication.
    session_id: i32,
}

impl ImageId {
    /// Name an image by the two ids the driver assigned it.
    pub const fn new(correlation_id: i64, session_id: i32) -> Self {
        Self {
            correlation_id,
            session_id,
        }
    }

    /// The publication's registration id.
    pub const fn correlation_id(self) -> i64 {
        self.correlation_id
    }

    /// The publication's session id.
    pub const fn session_id(self) -> i32 {
        self.session_id
    }
}

impl fmt::Display for ImageId {
    /// The part of a warning that says which image it was about. See the module
    /// note for why this is not `sourceIdentity()`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "correlationId={} sessionId={}",
            self.correlation_id, self.session_id
        )
    }
}

/// The parts of an `AuthConnectRequest` a session is built from
/// (`ControlSessionAdapter.java:766-802`).
///
/// The two strings are decoded here rather than in the conductor because that
/// is where the reference decodes them: its generated decoder hands back
/// `String`s and the adapter passes them on. They are declared US-ASCII and
/// decoded leniently, which is what a Java `String` built from bytes does with
/// them too.
#[derive(Debug, Clone, Copy)]
pub struct ConnectRequest<'a> {
    /// The connect's correlation id, which its answer echoes.
    pub correlation_id: i64,
    /// The stream the client wants its responses on.
    pub response_stream_id: i32,
    /// The client's protocol version, whose **major** decides acceptance
    /// (`ArchiveConductor.java:483-488`). Absent reads as zero, as it does in
    /// the reference's decoder.
    pub version: i32,
    /// The channel it wants them on.
    pub response_channel: &'a str,
    /// What it offered to be authenticated with.
    pub encoded_credentials: &'a [u8],
    /// What it calls itself, which the session counter's label carries.
    pub client_info: &'a str,
}

/// A message the adapter will not read, and why.
///
/// Each of these is an exception in the reference — thrown out of `onFragment`
/// and out of `ArchiveConductor.doWork` (`:136-139`), which the archive's
/// `main` catches and exits on. A value is the same decision taken one frame
/// earlier, where the conductor can still name what it was doing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlError {
    /// A message shorter than a message header, which cannot name a schema.
    /// The reference reads the header anyway and its buffer throws.
    ShortMessage {
        /// How long the message was.
        length: usize,
    },
    /// The message belongs to another schema (`ControlSessionAdapter.java:136`).
    UnexpectedSchemaId {
        /// The archive schema's id.
        expected: u16,
        /// What the message carried.
        actual: u16,
    },
}

impl fmt::Display for ControlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ShortMessage { length } => write!(
                f,
                "control message of {length} bytes is shorter than a message header"
            ),
            Self::UnexpectedSchemaId { expected, actual } => {
                write!(f, "expected schemaId={expected}, actual={actual}")
            }
        }
    }
}

impl std::error::Error for ControlError {}

/// The archive's control plane, as the adapter uses it.
///
/// Every method is one thing the reference's adapter does to a `ControlSession`
/// or to the conductor, with the same caller and the same arguments. The
/// methods that read or move a session exist here rather than on
/// [`crate::server::control_session::ControlSession`] for the one reason the
/// module note gives: the sessions live with the conductor, and the adapter has
/// their ids.
///
/// **It is lent per call**, not held — see the module note. A caller therefore
/// passes the same control plane to [`ControlAdapter::poll`] and to
/// [`ControlAdapter::on_message`] every turn, and may use it in between.
pub trait ControlPlane {
    /// A connect request, which is the only request that makes a session
    /// (`ControlSessionAdapter.java:766-802`). Answers with the id the client
    /// will be answered on (`ArchiveConductor.java:490`).
    fn new_session(&mut self, image: ImageId, request: ConnectRequest<'_>, now_ms: i64) -> i64;

    /// What the authenticator vouched for, for the authorisation gate
    /// (`ControlSessionAdapter.java:1206`). `None` until it has vouched for
    /// anything.
    fn session_principal(&self, session_id: i64) -> Option<&[u8]>;

    /// `ControlSession.abort(reason)` (`ControlSession.java:145-157`).
    fn abort_session(&mut self, session_id: i64, reason: &str);

    /// `ControlSession.onChallengeResponse` (`ControlSession.java:308-315`).
    fn on_challenge_response(
        &mut self,
        session_id: i64,
        correlation_id: i64,
        encoded_credentials: &[u8],
        now_ms: i64,
    );

    /// `ControlSession.onKeepAlive` (`ControlSession.java:317-320`), whose
    /// whole body is `attemptToActivate()`.
    fn on_keep_alive(&mut self, session_id: i64);

    /// `ControlSession.onArchiveId` (`ControlSession.java:541-548`), which the
    /// conductor answers with `ctx.archiveId()`
    /// (`ArchiveConductor.java:525-528`).
    fn on_archive_id(&mut self, session_id: i64, correlation_id: i64, now_ms: i64);

    /// `ControlSession.sendErrorResponse(correlationId, relevantId, message)`
    /// (`ControlSession.java:697-701`). The relevant id is the wire field's
    /// type; the reference's own error codes are `int`s it widens on the way in.
    fn send_error_response(
        &mut self,
        session_id: i64,
        correlation_id: i64,
        relevant_id: i64,
        message: &str,
        now_ms: i64,
    );

    /// `ArchiveConductor.logWarning` (`ArchiveConductor.java:443-446`), which
    /// the reference routes into the archive's error handler.
    fn log_warning(&mut self, message: &str);
}

/// The fragments of a client's control requests, and the sessions they belong
/// to.
pub struct ControlAdapter<A: AuthorisationService> {
    /// The control subscription, absent when `aeron.archive.control.channel
    /// .enabled` is false (`ArchiveConductor.java:227-237`).
    remote_subscription_id: Option<i64>,
    /// The local one, which is always there (`:239-240`).
    local_subscription_id: i64,
    /// Who decides whether a request may be performed at all.
    ///
    /// The reference hands this to the adapter's constructor and nothing else
    /// (`:243`), so it is the one collaborator the adapter does own.
    authorisation: A,

    /// `controlSessionByIdMap` (`ControlSessionAdapter.java:88`), holding the
    /// half of each entry that is the adapter's to hold — see the module note.
    sessions: HashMap<i64, ImageId>,
    /// The one assembler both subscriptions are read through
    /// (`ControlSessionAdapter.java:87`).
    ///
    /// It is the adapter's and not the subscriptions': the reference hands one
    /// assembler to both polls, and the messages it delivers are keyed by the
    /// publication's session, so a shared assembler is what keeps one
    /// subscription's half-read message from being completed by the other's.
    assembler: FragmentAssembler,
    /// The images of the subscription being polled, gathered before the poll
    /// starts.
    ///
    /// [`Client::poll_image`] names the image it reads by id and needs the
    /// client mutably, so the ids have to be taken out of the subscription
    /// first and the borrow dropped. The list is reused between polls, which is
    /// what keeps this from being an allocation per poll per image.
    images: Vec<ImageId>,
}

impl<A: AuthorisationService> ControlAdapter<A> {
    /// An adapter over two subscriptions, one of which may not exist.
    pub fn new(
        remote_subscription_id: Option<i64>,
        local_subscription_id: i64,
        authorisation: A,
    ) -> Self {
        Self {
            remote_subscription_id,
            local_subscription_id,
            authorisation,
            sessions: HashMap::new(),
            assembler: FragmentAssembler::new(),
            images: Vec::new(),
        }
    }

    /// Read both subscriptions, in the reference's order, and dispatch what
    /// arrives (`ControlSessionAdapter.java:107-119`).
    ///
    /// The remote subscription first and the local one second, each for
    /// [`FRAGMENT_LIMIT`] fragments — the limit is per subscription, not shared
    /// between them, which is what the reference's two separate `poll` calls
    /// say.
    ///
    /// # Errors
    ///
    /// [`ControlError`] for a message the archive will not read. The reference
    /// throws from here, so a turn that fails is a turn the caller should not
    /// continue past.
    pub fn poll<C: ControlPlane>(
        &mut self,
        client: &mut Client,
        control: &mut C,
        now_ms: i64,
    ) -> Result<usize, ControlError> {
        let mut fragments = 0;

        if let Some(subscription_id) = self.remote_subscription_id {
            fragments += self.poll_subscription(client, control, subscription_id, now_ms)?;
        }

        fragments += self.poll_subscription(client, control, self.local_subscription_id, now_ms)?;

        Ok(fragments)
    }

    /// Read one subscription's images for up to [`FRAGMENT_LIMIT`] fragments
    /// between them.
    fn poll_subscription<C: ControlPlane>(
        &mut self,
        client: &mut Client,
        control: &mut C,
        subscription_id: i64,
        now_ms: i64,
    ) -> Result<usize, ControlError> {
        self.images.clear();
        if let Some(subscription) = client.subscription(subscription_id) {
            self.images.extend(
                subscription
                    .images()
                    .iter()
                    .map(|image| ImageId::new(image.registration_id(), image.session_id())),
            );
        }

        let mut fragments = 0;
        // The first refusal ends the dispatch of what is already in hand, the
        // way the reference's throw ends the poll. The rest of the fragments
        // are still read — they are in the log buffer and reading them moves
        // the reader's position, which the publisher's window depends on — and
        // the error is handed back once the subscription is drained.
        let mut failure = None;

        // Borrowed apart for the reason `Subscription::poll_messages` does it:
        // the images are what is read, the assembler is where their fragments
        // go, and the rest is what a whole message is dispatched with.
        let Self {
            images,
            assembler,
            sessions,
            authorisation,
            ..
        } = self;

        for image in images.iter().copied() {
            let remaining = FRAGMENT_LIMIT.saturating_sub(fragments);
            if 0 == remaining {
                break;
            }

            let read = client.poll_image(
                subscription_id,
                image.correlation_id(),
                remaining,
                |fragment: &Fragment<'_>| {
                    assembler.push(fragment, &mut |message: Message<'_>| {
                        if failure.is_none() {
                            failure =
                                dispatch(message, image, sessions, control, authorisation, now_ms)
                                    .err();
                        }
                    });
                },
            );

            // `None` is an image the client no longer holds — one that went
            // away between being listed and being read. There is nothing to
            // read and nothing to say about it.
            match read {
                Some(count) => fragments += count,
                None => continue,
            }
        }

        match failure {
            Some(error) => Err(error),
            None => Ok(fragments),
        }
    }

    /// Dispatch one whole control message (`ControlSessionAdapter.java:126-1130`).
    ///
    /// Public because it is the whole of what the adapter decides: a caller
    /// with a message and the image it arrived on can put it through here
    /// without a driver, and the tests below do.
    ///
    /// # Errors
    ///
    /// [`ControlError`] for a message the archive will not read.
    pub fn on_message<C: ControlPlane>(
        &mut self,
        control: &mut C,
        image: ImageId,
        message: Message<'_>,
        now_ms: i64,
    ) -> Result<(), ControlError> {
        let Self {
            sessions,
            authorisation,
            ..
        } = self;

        dispatch(message, image, sessions, control, authorisation, now_ms)
    }

    /// Which image a session was created on, if it is still known.
    pub fn session_image(&self, session_id: i64) -> Option<ImageId> {
        self.sessions.get(&session_id).copied()
    }

    /// How many sessions the adapter is holding.
    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }

    /// Forget a session, answering with the image it was created on.
    ///
    /// This is the map's half of `removeControlSession`
    /// (`ControlSessionAdapter.java:1150-1163`). The other half belongs to the
    /// conductor and is the reason the image comes back: an aborted session
    /// **rejects its image** (`:1152-1155`), which is a command to the driver
    /// and needs the image's position, and the session counter is the
    /// conductor's to release.
    pub fn remove_session(&mut self, session_id: i64) -> Option<ImageId> {
        self.sessions.remove(&session_id)
    }

    /// What has been read and reassembled so far, and what was abandoned on the
    /// way (`FragmentAssembler`'s own counts).
    pub const fn assembler(&self) -> &FragmentAssembler {
        &self.assembler
    }
}

/// The switch, and the two gates in front of most of it.
fn dispatch<C: ControlPlane, A: AuthorisationService>(
    message: Message<'_>,
    image: ImageId,
    sessions: &mut HashMap<i64, ImageId>,
    control: &mut C,
    authorisation: &A,
    now_ms: i64,
) -> Result<(), ControlError> {
    let payload = message.payload;

    // The reference reads the header without looking at the length first and
    // lets its buffer throw for a message too short to hold one. This build's
    // generated reader panics on an index past the end instead of reporting it,
    // so the length is checked here, before anything is read.
    if payload.len() < message_header_codec::ENCODED_LENGTH {
        return Err(ControlError::ShortMessage {
            length: payload.len(),
        });
    }

    let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(payload), 0);

    let schema_id = header.schema_id();
    if schema_id != SBE_SCHEMA_ID {
        return Err(ControlError::UnexpectedSchemaId {
            expected: SBE_SCHEMA_ID,
            actual: schema_id,
        });
    }

    let template_id = header.template_id();

    match template_id {
        // The only request that makes a session (`ControlSessionAdapter.java:766`).
        auth_connect_request_codec::SBE_TEMPLATE_ID => {
            let mut decoder = AuthConnectRequestDecoder::default().header(header, 0);

            // Every `_decoder` call moves the decoder's limit over its own
            // length, so they are all made before any slice is taken: a slice
            // borrows the decoder for as long as the message lives.
            let channel_coordinates = decoder.response_channel_decoder();
            let credentials_coordinates = decoder.encoded_credentials_decoder();
            let client_info_coordinates = decoder.client_info_decoder();

            let encoded_credentials = decoder.encoded_credentials_slice(credentials_coordinates);
            let response_channel =
                String::from_utf8_lossy(decoder.response_channel_slice(channel_coordinates));
            let client_info =
                String::from_utf8_lossy(decoder.client_info_slice(client_info_coordinates));

            let request = ConnectRequest {
                correlation_id: decoder.correlation_id(),
                response_stream_id: decoder.response_stream_id(),
                version: decoder.version().unwrap_or(0),
                response_channel: &response_channel,
                encoded_credentials,
                client_info: &client_info,
            };

            let session_id = control.new_session(image, request, now_ms);
            sessions.insert(session_id, image);
        }

        // Not gated on the session, the image, or being ACTIVE: a session that
        // is being challenged has no principal yet, so there is nothing to
        // authorise, and the reference looks the session up in its map directly
        // (`ControlSessionAdapter.java:804-833`). A challenge answered on
        // another image is therefore *taken*, which is the reference's
        // behaviour and not an oversight to fix here.
        challenge_response_codec::SBE_TEMPLATE_ID => {
            let mut decoder = ChallengeResponseDecoder::default().header(header, 0);

            let control_session_id = decoder.control_session_id();
            let correlation_id = decoder.correlation_id();
            let coordinates = decoder.encoded_credentials_decoder();
            let encoded_credentials = decoder.encoded_credentials_slice(coordinates);

            if sessions.contains_key(&control_session_id) {
                control.on_challenge_response(
                    control_session_id,
                    correlation_id,
                    encoded_credentials,
                    now_ms,
                );
            }
        }

        keep_alive_request_codec::SBE_TEMPLATE_ID => {
            let decoder = KeepAliveRequestDecoder::default().header(header, 0);

            let control_session_id = decoder.control_session_id();
            let correlation_id = decoder.correlation_id();

            if let Some(session_id) = gate(
                sessions,
                control,
                authorisation,
                image,
                control_session_id,
                template_id,
                correlation_id,
                now_ms,
            ) {
                control.on_keep_alive(session_id);
            }
        }

        archive_id_request_codec::SBE_TEMPLATE_ID => {
            let decoder = ArchiveIdRequestDecoder::default().header(header, 0);

            let control_session_id = decoder.control_session_id();
            let correlation_id = decoder.correlation_id();

            if let Some(session_id) = gate(
                sessions,
                control,
                authorisation,
                image,
                control_session_id,
                template_id,
                correlation_id,
                now_ms,
            ) {
                control.on_archive_id(session_id, correlation_id, now_ms);
            }
        }

        // Like the challenge answer, not gated — but this one is not silent
        // either: the reference asks the map directly, tests the image itself
        // and says nothing at all when either fails
        // (`ControlSessionAdapter.java:144-160`).
        close_session_request_codec::SBE_TEMPLATE_ID => {
            let decoder = CloseSessionRequestDecoder::default().header(header, 0);

            let control_session_id = decoder.control_session_id();
            if sessions.get(&control_session_id) == Some(&image) {
                control.abort_session(control_session_id, SESSION_CLOSED_MSG);
            }
        }

        // Everything else is a request this slice does not answer yet: the
        // recording, replay, listing and replication families arrive with the
        // slices that can carry them out. The reference's switch has no default
        // arm, so an unknown template id is silent there; here it is named,
        // because a client is waiting for an answer and a request nobody
        // answers is a client timing out rather than a request that never came.
        _ => {
            control.log_warning(&format!(
                "control request for unimplemented templateId={template_id} \
                 image={image}"
            ));
        }
    }

    Ok(())
}

/// The two gates every request but a connect, a challenge answer and a close
/// passes through (`ControlSessionAdapter.java:1192-1216`).
///
/// Answers with the session id when the request may go ahead.
#[allow(clippy::too_many_arguments)] // one per thing the request carried
fn gate<C: ControlPlane, A: AuthorisationService>(
    sessions: &HashMap<i64, ImageId>,
    control: &mut C,
    authorisation: &A,
    image: ImageId,
    control_session_id: i64,
    template_id: u16,
    correlation_id: i64,
    now_ms: i64,
) -> Option<i64> {
    let Some(known_image) = sessions.get(&control_session_id) else {
        control.log_warning(&format!(
            "control request for unknown session: controlSessionId={control_session_id} \
             templateId={template_id}"
        ));
        return None;
    };

    // A session belongs to the image it was opened on. A request that arrives
    // on another one is dropped in silence — the client is told nothing, which
    // is what the reference does and what makes this look like a request that
    // never arrived.
    if *known_image != image {
        control.log_warning(&format!(
            "unauthorised archive action={template_id} \
             controlSessionId={control_session_id} source={image}"
        ));
        return None;
    }

    let principal = control.session_principal(control_session_id);
    if !authorisation.is_authorised(i32::from(SBE_SCHEMA_ID), i32::from(template_id), principal) {
        control.log_warning(&format!(
            "unauthorised archive action={template_id} \
             controlSessionId={control_session_id} source={image}"
        ));

        control.send_error_response(
            control_session_id,
            correlation_id,
            i64::from(UNAUTHORISED_ACTION),
            UNAUTHORISED_ACTION_MSG,
            now_ms,
        );

        return None;
    }

    Some(control_session_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::cell::RefCell;

    use crate::server::auth::{AllowAll, DenyAll};

    use deepmsg_codec::archive::WriteBuf;
    use deepmsg_codec::archive::auth_connect_request_codec::AuthConnectRequestEncoder;
    use deepmsg_codec::archive::challenge_response_codec::ChallengeResponseEncoder;
    use deepmsg_codec::archive::close_session_request_codec::CloseSessionRequestEncoder;
    use deepmsg_codec::archive::keep_alive_request_codec::KeepAliveRequestEncoder;
    use deepmsg_codec::archive::start_recording_request_codec::{
        SBE_TEMPLATE_ID as START_RECORDING, StartRecordingRequestEncoder,
    };

    use deepmsg_client::fragment_assembler::MessageHeader;

    /// Where the body of a hand-built message starts.
    const BODY: usize = message_header_codec::ENCODED_LENGTH;

    /// The image a session is created on, and another one it is not.
    const IMAGE: ImageId = ImageId::new(11, 22);
    const OTHER_IMAGE: ImageId = ImageId::new(33, 44);

    /// What the adapter asked the control plane to do, in order.
    #[derive(Debug, Clone, PartialEq, Eq)]
    enum Call {
        NewSession {
            session_id: i64,
            image: ImageId,
            correlation_id: i64,
            response_stream_id: i32,
            version: i32,
            response_channel: String,
            encoded_credentials: Vec<u8>,
            client_info: String,
        },
        Abort {
            session_id: i64,
            reason: String,
        },
        ChallengeResponse {
            session_id: i64,
            correlation_id: i64,
            encoded_credentials: Vec<u8>,
        },
        KeepAlive {
            session_id: i64,
        },
        ArchiveId {
            session_id: i64,
            correlation_id: i64,
        },
        ErrorResponse {
            session_id: i64,
            correlation_id: i64,
            relevant_id: i64,
            message: String,
        },
    }

    /// A control plane that writes down what it was asked and hands out session
    /// ids from one.
    #[derive(Default)]
    struct Recorder {
        calls: Vec<Call>,
        warnings: Vec<String>,
        /// What the sessions' authenticator is taken to have vouched for.
        principal: Option<Vec<u8>>,
        next_session_id: i64,
    }

    impl ControlPlane for Recorder {
        fn new_session(
            &mut self,
            image: ImageId,
            request: ConnectRequest<'_>,
            _now_ms: i64,
        ) -> i64 {
            self.next_session_id += 1;

            self.calls.push(Call::NewSession {
                session_id: self.next_session_id,
                image,
                correlation_id: request.correlation_id,
                response_stream_id: request.response_stream_id,
                version: request.version,
                response_channel: request.response_channel.to_owned(),
                encoded_credentials: request.encoded_credentials.to_vec(),
                client_info: request.client_info.to_owned(),
            });

            self.next_session_id
        }

        fn session_principal(&self, _session_id: i64) -> Option<&[u8]> {
            self.principal.as_deref()
        }

        fn abort_session(&mut self, session_id: i64, reason: &str) {
            self.calls.push(Call::Abort {
                session_id,
                reason: reason.to_owned(),
            });
        }

        fn on_challenge_response(
            &mut self,
            session_id: i64,
            correlation_id: i64,
            encoded_credentials: &[u8],
            _now_ms: i64,
        ) {
            self.calls.push(Call::ChallengeResponse {
                session_id,
                correlation_id,
                encoded_credentials: encoded_credentials.to_vec(),
            });
        }

        fn on_keep_alive(&mut self, session_id: i64) {
            self.calls.push(Call::KeepAlive { session_id });
        }

        fn on_archive_id(&mut self, session_id: i64, correlation_id: i64, _now_ms: i64) {
            self.calls.push(Call::ArchiveId {
                session_id,
                correlation_id,
            });
        }

        fn send_error_response(
            &mut self,
            session_id: i64,
            correlation_id: i64,
            relevant_id: i64,
            message: &str,
            _now_ms: i64,
        ) {
            self.calls.push(Call::ErrorResponse {
                session_id,
                correlation_id,
                relevant_id,
                message: message.to_owned(),
            });
        }

        fn log_warning(&mut self, message: &str) {
            self.warnings.push(message.to_owned());
        }
    }

    /// One authorisation question: the two ids and the principal it was asked
    /// about.
    type Question = (i32, i32, Option<Vec<u8>>);

    /// An authorisation service that writes down what it was asked and answers
    /// `true`.
    #[derive(Default)]
    struct Recording {
        calls: RefCell<Vec<Question>>,
    }

    impl AuthorisationService for Recording {
        fn is_authorised(
            &self,
            protocol_id: i32,
            action_id: i32,
            encoded_principal: Option<&[u8]>,
        ) -> bool {
            self.calls.borrow_mut().push((
                protocol_id,
                action_id,
                encoded_principal.map(<[u8]>::to_vec),
            ));
            true
        }
    }

    /// An adapter over a `Recorder`, with the authorisation service given.
    fn adapter_with<A: AuthorisationService>(authorisation: A) -> (ControlAdapter<A>, Recorder) {
        (
            ControlAdapter::new(Some(7), 8, authorisation),
            Recorder::default(),
        )
    }

    /// The message the tests dispatch, as the assembler would hand it over.
    ///
    /// The header is the publication's, and the adapter reads the image from
    /// its own argument rather than this — the two are separate on purpose, so
    /// that a test that gets them out of step is testing what it means to.
    fn message(payload: &[u8]) -> Message<'_> {
        Message {
            header: MessageHeader {
                session_id: IMAGE.session_id(),
                stream_id: 10,
                term_offset: 0,
                flags: 0,
                position: 0,
                frame_length: 0,
                fragmented_frame_length: 0,
            },
            payload,
        }
    }

    /// A connect request, encoded by the same codec a client writes it with.
    fn auth_connect(
        correlation_id: i64,
        response_stream_id: i32,
        version: i32,
        response_channel: &str,
        encoded_credentials: &[u8],
        client_info: &str,
    ) -> Vec<u8> {
        let mut buffer = vec![0u8; 1024];

        let length = {
            let encoder =
                AuthConnectRequestEncoder::default().wrap(WriteBuf::new(&mut buffer), BODY);
            let mut header = encoder.header(0);
            let mut encoder = header.parent().unwrap();

            encoder
                .correlation_id(correlation_id)
                .response_stream_id(response_stream_id)
                .version(version)
                .response_channel(response_channel.as_bytes())
                .encoded_credentials(encoded_credentials)
                .client_info(client_info.as_bytes());

            BODY + encoder.encoded_length()
        };

        buffer.truncate(length);
        buffer
    }

    /// A connect whose only interesting field is the correlation id.
    fn a_connect(correlation_id: i64) -> Vec<u8> {
        auth_connect(
            correlation_id,
            20,
            0x0001_0000,
            "aeron:udp?endpoint=localhost:0",
            &[],
            "",
        )
    }

    fn keep_alive(control_session_id: i64, correlation_id: i64) -> Vec<u8> {
        let mut buffer = vec![0u8; 64];

        let length = {
            let encoder = KeepAliveRequestEncoder::default().wrap(WriteBuf::new(&mut buffer), BODY);
            let mut header = encoder.header(0);
            let mut encoder = header.parent().unwrap();
            encoder
                .control_session_id(control_session_id)
                .correlation_id(correlation_id);
            BODY + encoder.encoded_length()
        };

        buffer.truncate(length);
        buffer
    }

    fn close_session(control_session_id: i64) -> Vec<u8> {
        let mut buffer = vec![0u8; 64];

        let length = {
            let encoder =
                CloseSessionRequestEncoder::default().wrap(WriteBuf::new(&mut buffer), BODY);
            let mut header = encoder.header(0);
            let mut encoder = header.parent().unwrap();
            encoder.control_session_id(control_session_id);
            BODY + encoder.encoded_length()
        };

        buffer.truncate(length);
        buffer
    }

    fn challenge_answer(
        control_session_id: i64,
        correlation_id: i64,
        encoded_credentials: &[u8],
    ) -> Vec<u8> {
        let mut buffer = vec![0u8; 256];

        let length = {
            let encoder =
                ChallengeResponseEncoder::default().wrap(WriteBuf::new(&mut buffer), BODY);
            let mut header = encoder.header(0);
            let mut encoder = header.parent().unwrap();
            encoder
                .control_session_id(control_session_id)
                .correlation_id(correlation_id)
                .encoded_credentials(encoded_credentials);
            BODY + encoder.encoded_length()
        };

        buffer.truncate(length);
        buffer
    }

    /// Open a session on [`IMAGE`] and hand back its id.
    fn an_open_session<A: AuthorisationService>(
        adapter: &mut ControlAdapter<A>,
        control: &mut Recorder,
    ) -> i64 {
        let connect = a_connect(1);
        adapter
            .on_message(control, IMAGE, message(&connect), 0)
            .expect("a connect is read");

        let Call::NewSession { session_id, .. } = control.calls[0] else {
            panic!("the connect did not make a session");
        };

        session_id
    }

    /// The connect is the request that makes a session, and everything it
    /// carried reaches the control plane (`ControlSessionAdapter.java:766-802`).
    #[test]
    fn a_connect_makes_a_session_from_what_it_carried() {
        let (mut adapter, mut control) = adapter_with(AllowAll);
        let connect = auth_connect(
            17,
            21,
            0x0001_0002,
            "aeron:udp?endpoint=localhost:0|control-mode=response",
            b"credentials",
            "a client",
        );

        adapter
            .on_message(&mut control, IMAGE, message(&connect), 1_000)
            .expect("read");

        assert_eq!(
            vec![Call::NewSession {
                session_id: 1,
                image: IMAGE,
                correlation_id: 17,
                response_stream_id: 21,
                version: 0x0001_0002,
                response_channel: "aeron:udp?endpoint=localhost:0|control-mode=response".to_owned(),
                encoded_credentials: b"credentials".to_vec(),
                client_info: "a client".to_owned(),
            }],
            control.calls
        );
        assert_eq!(Some(IMAGE), adapter.session_image(1));
        assert!(control.warnings.is_empty());
    }

    /// An empty credentials blob and an absent client info are read as the
    /// empty ones rather than left undecoded — the reference decodes all three
    /// var-data fields unconditionally (`ControlSessionAdapter.java:775-793`).
    #[test]
    fn a_connect_with_empty_var_data_is_still_read() {
        let (mut adapter, mut control) = adapter_with(AllowAll);
        let connect = auth_connect(1, 20, 0x0001_0000, "aeron:ipc", &[], "");

        adapter
            .on_message(&mut control, IMAGE, message(&connect), 0)
            .expect("read");

        let Call::NewSession {
            encoded_credentials,
            client_info,
            response_channel,
            ..
        } = &control.calls[0]
        else {
            panic!("not a connect: {:?}", control.calls);
        };

        assert!(encoded_credentials.is_empty());
        assert!(client_info.is_empty());
        assert_eq!("aeron:ipc", response_channel);
    }

    /// A client that says nothing about its version is read as version zero,
    /// which is what the reference's decoder does with an absent optional field
    /// and what makes it fail the archive's version gate.
    #[test]
    fn an_absent_version_reads_as_zero() {
        let (mut adapter, mut control) = adapter_with(AllowAll);

        let mut buffer = vec![0u8; 512];
        let length = {
            let encoder =
                AuthConnectRequestEncoder::default().wrap(WriteBuf::new(&mut buffer), BODY);
            let mut header = encoder.header(0);
            let mut encoder = header.parent().unwrap();
            encoder
                .correlation_id(1)
                .response_stream_id(20)
                .version_opt(None)
                .response_channel(b"aeron:ipc")
                .encoded_credentials(&[])
                .client_info(&[]);
            BODY + encoder.encoded_length()
        };
        buffer.truncate(length);

        adapter
            .on_message(&mut control, IMAGE, message(&buffer), 0)
            .expect("read");

        let Call::NewSession { version, .. } = control.calls[0] else {
            panic!("not a connect");
        };
        assert_eq!(0, version);
    }

    /// A keep-alive reaches the session it names, and activating is the whole
    /// of what a session does with one (`ControlSession.java:317-320`).
    #[test]
    fn a_keep_alive_reaches_the_session_it_names() {
        let (mut adapter, mut control) = adapter_with(AllowAll);
        let session_id = an_open_session(&mut adapter, &mut control);

        let payload = keep_alive(session_id, 99);
        adapter
            .on_message(&mut control, IMAGE, message(&payload), 2_000)
            .expect("read");

        assert_eq!(
            vec![Call::KeepAlive { session_id }],
            control.calls[1..],
            "the connect is the first call"
        );
    }

    /// An archive id request reaches the session, which asks the conductor for
    /// the id (`ControlSession.java:541-548`).
    #[test]
    fn an_archive_id_request_reaches_the_session() {
        let (mut adapter, mut control) = adapter_with(AllowAll);
        let session_id = an_open_session(&mut adapter, &mut control);

        let payload = archive_id_request(session_id, 7);
        adapter
            .on_message(&mut control, IMAGE, message(&payload), 2_000)
            .expect("read");

        assert_eq!(
            vec![Call::ArchiveId {
                session_id,
                correlation_id: 7,
            }],
            control.calls[1..]
        );
    }

    /// A close names a session and ends it, with the reference's reason
    /// (`ControlSessionAdapter.java:144-160`).
    #[test]
    fn a_close_ends_the_session_it_names() {
        let (mut adapter, mut control) = adapter_with(AllowAll);
        let session_id = an_open_session(&mut adapter, &mut control);

        let payload = close_session(session_id);
        adapter
            .on_message(&mut control, IMAGE, message(&payload), 3_000)
            .expect("read");

        assert_eq!(
            vec![Call::Abort {
                session_id,
                reason: SESSION_CLOSED_MSG.to_owned(),
            }],
            control.calls[1..]
        );
    }

    /// A close that names an image the session is not on changes nothing, and
    /// nothing is said to the client about it
    /// (`ControlSessionAdapter.java:155-158`).
    #[test]
    fn a_close_from_another_image_is_ignored() {
        let (mut adapter, mut control) = adapter_with(AllowAll);
        let session_id = an_open_session(&mut adapter, &mut control);

        let payload = close_session(session_id);
        adapter
            .on_message(&mut control, OTHER_IMAGE, message(&payload), 3_000)
            .expect("read");

        assert_eq!(1, control.calls.len(), "only the connect");
        assert_eq!(Some(IMAGE), adapter.session_image(session_id));
        assert!(control.warnings.is_empty(), "and no word about it");
    }

    /// The first gate: a request that arrives on an image other than the one
    /// its session was created on is dropped, and the client is not told
    /// (`ControlSessionAdapter.java:1196-1203`).
    #[test]
    fn a_request_on_another_image_is_dropped_in_silence() {
        let (mut adapter, mut control) = adapter_with(AllowAll);
        let session_id = an_open_session(&mut adapter, &mut control);

        let payload = keep_alive(session_id, 99);
        adapter
            .on_message(&mut control, OTHER_IMAGE, message(&payload), 2_000)
            .expect("read");

        assert_eq!(1, control.calls.len(), "only the connect");
        assert_warned_unauthorised(&control);
    }

    /// A request that names no session at all warns and is dropped — the
    /// reference's third outcome, and the one a client cannot tell apart from a
    /// request that never arrived (`ControlSessionAdapter.java:1219-1224`).
    #[test]
    fn a_request_for_an_unknown_session_is_dropped() {
        let (mut adapter, mut control) = adapter_with(AllowAll);

        let payload = keep_alive(4_242, 99);
        adapter
            .on_message(&mut control, IMAGE, message(&payload), 2_000)
            .expect("read");

        assert!(control.calls.is_empty());
        assert_eq!(1, control.warnings.len());
        assert!(
            control.warnings[0].starts_with("control request for unknown session:"),
            "{:?}",
            control.warnings
        );
    }

    /// The second gate: a refusal **is** answered, with
    /// `UNAUTHORISED_ACTION` as the relevant id and the reference's message
    /// (`ControlSessionAdapter.java:1205-1216`).
    #[test]
    fn a_denied_request_is_answered_with_the_error_the_reference_sends() {
        let (mut adapter, mut control) = adapter_with(DenyAll);
        let session_id = an_open_session(&mut adapter, &mut control);

        let payload = keep_alive(session_id, 99);
        adapter
            .on_message(&mut control, IMAGE, message(&payload), 2_000)
            .expect("read");

        assert_eq!(
            vec![Call::ErrorResponse {
                session_id,
                correlation_id: 99,
                relevant_id: i64::from(UNAUTHORISED_ACTION),
                message: UNAUTHORISED_ACTION_MSG.to_owned(),
            }],
            control.calls[1..],
            "refused before it reached the session, and the refusal was sent"
        );
        assert_warned_unauthorised(&control);
    }

    /// The authorisation service is asked about the **schema** and the
    /// **template** — which is what its two ids are for — and about what the
    /// session's authenticator vouched for
    /// (`ControlSessionAdapter.java:1206-1207`).
    #[test]
    fn the_authorisation_service_is_asked_about_the_template_and_the_principal() {
        let (mut adapter, mut control) = adapter_with(Recording::default());
        control.principal = Some(b"a principal".to_vec());
        let session_id = an_open_session(&mut adapter, &mut control);
        adapter.authorisation.calls.borrow_mut().clear();

        let payload = keep_alive(session_id, 99);
        adapter
            .on_message(&mut control, IMAGE, message(&payload), 2_000)
            .expect("read");

        assert_eq!(
            vec![(
                i32::from(SBE_SCHEMA_ID),
                i32::from(keep_alive_request_codec::SBE_TEMPLATE_ID),
                Some(b"a principal".to_vec()),
            )],
            *adapter.authorisation.calls.borrow()
        );
    }

    /// The challenge answer is **not** gated on anything but the session being
    /// known: a session being challenged has no principal to authorise against,
    /// and the reference looks it up in its map directly
    /// (`ControlSessionAdapter.java:804-833`).
    #[test]
    fn a_challenge_answer_is_not_gated() {
        let (mut adapter, mut control) = adapter_with(DenyAll);
        let session_id = an_open_session(&mut adapter, &mut control);

        let payload = challenge_answer(session_id, 5, b"an answer");
        adapter
            .on_message(&mut control, IMAGE, message(&payload), 2_000)
            .expect("read");

        assert_eq!(
            vec![Call::ChallengeResponse {
                session_id,
                correlation_id: 5,
                encoded_credentials: b"an answer".to_vec(),
            }],
            control.calls[1..]
        );
    }

    /// A message in another schema stops the turn. The reference throws
    /// (`ControlSessionAdapter.java:136`), which the archive's main exits
    /// on; this is the same statement as a value, one frame earlier.
    #[test]
    fn a_message_in_another_schema_is_refused() {
        let (mut adapter, mut control) = adapter_with(AllowAll);
        let mut connect = a_connect(1);
        // `schemaId` is the third of the header's four fields.
        connect[4..6].copy_from_slice(&999u16.to_le_bytes());

        let error = adapter
            .on_message(&mut control, IMAGE, message(&connect), 0)
            .expect_err("refused");

        assert_eq!(
            ControlError::UnexpectedSchemaId {
                expected: SBE_SCHEMA_ID,
                actual: 999,
            },
            error
        );
        assert!(control.calls.is_empty());
    }

    /// A message too short to carry a header cannot name a schema, and reading
    /// one would index past the end of the buffer rather than throw.
    #[test]
    fn a_message_shorter_than_a_header_is_refused() {
        let (mut adapter, mut control) = adapter_with(AllowAll);

        let error = adapter
            .on_message(&mut control, IMAGE, message(&[1, 2, 3]), 0)
            .expect_err("refused");

        assert_eq!(ControlError::ShortMessage { length: 3 }, error);
    }

    /// A request this slice does not answer is named rather than dropped in
    /// silence: the reference's switch has no default arm
    /// (`ControlSessionAdapter.java:142-1129`), and a client waiting on a
    /// request nobody answers is a client timing out.
    #[test]
    fn a_template_this_slice_does_not_answer_is_named() {
        let (mut adapter, mut control) = adapter_with(AllowAll);

        let mut buffer = vec![0u8; 128];
        let length = {
            let encoder =
                StartRecordingRequestEncoder::default().wrap(WriteBuf::new(&mut buffer), BODY);
            let mut header = encoder.header(0);
            let mut encoder = header.parent().unwrap();
            encoder
                .control_session_id(1)
                .correlation_id(2)
                .stream_id(3)
                .channel(b"aeron:ipc");
            BODY + encoder.encoded_length()
        };
        buffer.truncate(length);

        adapter
            .on_message(&mut control, IMAGE, message(&buffer), 0)
            .expect("read");

        assert!(control.calls.is_empty());
        assert_eq!(1, control.warnings.len());
        assert!(
            control.warnings[0].contains(&format!("templateId={START_RECORDING}")),
            "{:?}",
            control.warnings
        );
    }

    /// Forgetting a session answers with the image it was on, because the
    /// conductor rejects that image when the session was aborted
    /// (`ControlSessionAdapter.java:1150-1155`).
    #[test]
    fn forgetting_a_session_hands_back_its_image() {
        let (mut adapter, mut control) = adapter_with(AllowAll);
        let session_id = an_open_session(&mut adapter, &mut control);

        assert_eq!(Some(IMAGE), adapter.remove_session(session_id));
        assert_eq!(None, adapter.session_image(session_id));
        assert_eq!(0, adapter.session_count());

        // And a request for it is now a request for an unknown session.
        let payload = keep_alive(session_id, 99);
        adapter
            .on_message(&mut control, IMAGE, message(&payload), 2_000)
            .expect("read");
        assert!(
            control.warnings[0].starts_with("control request for unknown session:"),
            "{:?}",
            control.warnings
        );
    }

    /// Two sessions on two images are told apart, which is the whole of what
    /// the first gate is for: the UDP control subscription and the local IPC
    /// one are two images on one stream
    /// (`ArchiveConductor.java:239-240`).
    #[test]
    fn two_images_are_two_sessions() {
        let (mut adapter, mut control) = adapter_with(AllowAll);

        let connect = a_connect(1);
        adapter
            .on_message(&mut control, IMAGE, message(&connect), 0)
            .expect("read");
        adapter
            .on_message(&mut control, OTHER_IMAGE, message(&connect), 0)
            .expect("read");

        assert_eq!(2, adapter.session_count());
        assert_eq!(Some(IMAGE), adapter.session_image(1));
        assert_eq!(Some(OTHER_IMAGE), adapter.session_image(2));

        let payload = keep_alive(1, 99);
        adapter
            .on_message(&mut control, IMAGE, message(&payload), 1_000)
            .expect("read");
        assert_eq!(vec![Call::KeepAlive { session_id: 1 }], control.calls[2..]);
    }

    fn archive_id_request(control_session_id: i64, correlation_id: i64) -> Vec<u8> {
        use deepmsg_codec::archive::archive_id_request_codec::ArchiveIdRequestEncoder;

        let mut buffer = vec![0u8; 64];
        let length = {
            let encoder = ArchiveIdRequestEncoder::default().wrap(WriteBuf::new(&mut buffer), BODY);
            let mut header = encoder.header(0);
            let mut encoder = header.parent().unwrap();
            encoder
                .control_session_id(control_session_id)
                .correlation_id(correlation_id);
            BODY + encoder.encoded_length()
        };
        buffer.truncate(length);
        buffer
    }

    /// Both warnings in the gate are the reference's one line
    /// (`ControlSessionAdapter.java:1200-1202`, `:1209-1213`).
    fn assert_warned_unauthorised(control: &Recorder) {
        assert_eq!(1, control.warnings.len());
        assert!(
            control.warnings[0].starts_with("unauthorised archive action="),
            "{:?}",
            control.warnings
        );
    }
}
