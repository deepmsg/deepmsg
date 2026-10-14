//! The archive's **async client** — the self-healing one
//! (`aeron_archive_async_client.c`, 555 lines).
//!
//! # What it is for
//!
//! The synchronous client is a request and a wait: a caller that wants an
//! answer owns the thread until it has one. This client is the opposite end of
//! that bargain — **the caller polls it, and the answers arrive as callbacks**:
//! every control response and every recording descriptor the archive sends is
//! handed to a [`Listener`] as it is read. Nothing here blocks, so it is the
//! client a state machine can be built on, and the persistent subscription is
//! one (`…_persistent_subscription.c`, a later commit of this slice).
//!
//! # The four states, and the one that heals
//!
//! `CONNECTING → CONNECTED → DISCONNECTED → CONNECTING …`, with `CLOSED`
//! terminal (`:30-37`). The healing is in two places and neither of them waits:
//!
//! * `CONNECTED` asks its own subscription whether it is still connected, and
//!   drops to `DISCONNECTED` the turn it is not (`:205-208`) — and every
//!   `try_send_*` that is refused with "not connected" does the same
//!   (`:264-267`, and four more like it);
//! * `DISCONNECTED` closes what it held, tells the listener, and goes straight
//!   back to `CONNECTING` (`:226-233`).
//!
//! **There is no backoff, no sleep and no retry counter anywhere in it**, and
//! that is not an omission to improve on: the reference has one constant in the
//! whole file, the ten fragments a turn (`:211-215`), and a caller that wants a
//! slower reconnect owns the loop that calls [`poll`](AsyncArchiveClient::poll).
//! A connect that fails is reported and **stays in `CONNECTING`** (`:188-196`),
//! so the next turn tries again.
//!
//! # Nothing is re-sent, and that is the contract
//!
//! A reconnect **does not re-issue requests**. The reference keeps no record of
//! what was in flight — `try_send_*` answers `false` and says so — because the
//! only caller that matters is a state machine that knows what it was doing and
//! re-issues it itself (`…_persistent_subscription.c:1123-1135`). Building a
//! resend list in here would be a second, worse copy of that knowledge.
//!
//! # The decode is this client's own
//!
//! The reference's synchronous client reads answers through
//! `aeron_archive_control_response_poller` and dispatches from its slot; this
//! one wraps the SBE messages inline (`:370-555`) because it wants **a
//! callback per message**, and because one subscription carries two templates:
//! a control response *and* a recording descriptor arrive on the same channel.
//! So does this build's, for the same reason.

use core::fmt;

use deepmsg_client::client::Client;
use deepmsg_client::fragment_assembler::{Action, ControlledHandler, Message};
use deepmsg_codec::archive::SBE_SCHEMA_ID;
use deepmsg_codec::archive::control_response_code::ControlResponseCode;
use deepmsg_codec::archive::control_response_codec::{
    ControlResponseDecoder, SBE_BLOCK_LENGTH as CONTROL_RESPONSE_BLOCK_LENGTH,
};
use deepmsg_codec::archive::recording_descriptor_codec::{
    RecordingDescriptorDecoder, SBE_BLOCK_LENGTH as RECORDING_DESCRIPTOR_BLOCK_LENGTH,
};
use deepmsg_core::logbuffer::append::Appended;

use crate::client::archive::archive_to_client_error_code;
use crate::client::async_connect::{AsyncConnect, ConnectError, Credentials, Polled};
use crate::client::context::ArchiveContext;
use crate::client::descriptor_poller::RecordingDescriptor;
use crate::client::poller::{holds, sbe_header};
use crate::client::proxy::{ArchiveProxy, ProxyError, ReplayParams};

/// `AERON_ARCHIVE_ASYNC_CLIENT_FRAGMENT_LIMIT` — how many fragments a
/// `CONNECTED` turn reads (`:211-215`), and the only constant in the reference's
/// client.
const FRAGMENT_LIMIT: usize = 10;

/// `AERON_ARCHIVE_CLIENT_CONTROL_RESPONSE_SBE_TEMPLATE_ID`.
const CONTROL_RESPONSE_TEMPLATE_ID: u16 = 1;

/// `AERON_ARCHIVE_CLIENT_RECORDING_DESCRIPTOR_SBE_TEMPLATE_ID`
/// (`…_descriptor_poller.rs`'s, and the archive's own schema).
const RECORDING_DESCRIPTOR_TEMPLATE_ID: u16 = 22;

/// Where this client is (`:30-37`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    /// Connecting, or about to: a connect that failed is reported and this is
    /// where the client stays, so the next turn tries again (`:188-196`).
    Connecting,
    /// Connected, and reading.
    Connected,
    /// The control channel went away. The next turn is a reconnect.
    Disconnected,
    /// Terminal — after a malformed message (`:217-221`).
    Closed,
}

/// One answer from the archive, as a listener is handed it.
///
/// The three fields are the reference's `on_control_response` arguments
/// (`…_async_client.h:32-37`) with `clientd` left out, which is Rust's business
/// rather than the callback's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControlResponse<'a> {
    /// Which request this answers.
    pub correlation_id: i64,
    /// What it is about. **On an `ERROR` this is not the archive's own id**:
    /// the reference maps it through `aeron_archive_client_map_archive_to_client_error_code`
    /// (`:459-461`) before the listener sees it, so an error answer carries a
    /// *client* error code and every other answer carries the archive's
    /// relevant id unchanged.
    pub relevant_id: i64,
    /// `OK`, `ERROR`, or one of the archive's refusals that is not a failure.
    pub code: ControlResponseCode,
    /// The archive's message, empty when it sent none (`:452-468`).
    pub error_message: &'a str,
}

/// Why this client stopped, as [`Listener::on_error`] is handed it.
#[derive(Debug)]
pub enum AsyncClientError {
    /// The connect failed. **Not terminal**: the client stays in
    /// [`State::Connecting`] and the next turn tries again.
    Connect(ConnectError),
    /// A message on the control channel could not be read, which
    /// **is** terminal (`:217-221`) — the reference closes the client and says
    /// so, because a channel whose framing is wrong cannot be resynchronised.
    MalformedMessage,
}

impl fmt::Display for AsyncClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connect(error) => write!(f, "{error}"),
            Self::MalformedMessage => write!(f, "unable to wrap buffer"),
        }
    }
}

impl std::error::Error for AsyncClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Connect(error) => Some(error),
            Self::MalformedMessage => None,
        }
    }
}

/// What an application is told as the client runs
/// (`aeron_archive_async_client_listener_t`, `…_async_client.h:25-40`).
///
/// Every method has a default that does nothing, because the reference's
/// listener is a struct of function pointers of which any may be null
/// (`:591`, `:611`, `:624` and the rest).
pub trait Listener {
    /// The connect is over and the archive answered. The session id is
    /// available from here on.
    fn on_connected(&mut self) {}

    /// The control channel went away. A reconnect follows without being asked
    /// for — this is a state, not a failure.
    fn on_disconnected(&mut self) {}

    /// An answer to one of this client's requests.
    fn on_control_response(&mut self, response: &ControlResponse<'_>);

    /// A recording the archive described. Arrives on the **same** subscription
    /// as the control responses, which is why this client decodes both.
    fn on_recording_descriptor(&mut self, descriptor: &RecordingDescriptor);

    /// A refusal, of one of the two kinds [`AsyncClientError`] names.
    fn on_error(&mut self, error: &AsyncClientError);
}

/// The archive's async client: a connection that heals, a decode of what
/// arrives on it, and nothing else.
pub struct AsyncArchiveClient {
    state: State,
    /// The driver, **made again on every connect and dropped on every
    /// disconnect** (`:154-168`).
    ///
    /// This is the reference's `had_aeron`/`has_aeron` reset written out. Its
    /// client makes an Aeron client when the context has none, then sets
    /// `context->aeron = NULL` again so that the *next* connect makes a fresh
    /// one — and the reason is the one case that matters: the archive is a
    /// process, and `testAeronArchiveAsyncClient` reconnects to it by **killing
    /// it and starting another** (`…_async_client_test.cpp:583-590`). A client
    /// value that outlived that would be mapped onto a CnC file the new driver
    /// has replaced.
    client: Option<Client>,
    context: ArchiveContext,
    credentials: Box<dyn Credentials>,
    listener: Box<dyn Listener>,
    /// The connect in progress. `None` between connects, and made again on the
    /// turn after a disconnect — the reference frees its own on completion and
    /// makes a new one, which is the same shape.
    connect: Option<AsyncConnect>,
    /// The connected archive: the response subscription, the request
    /// publication and the proxy over it (`client->archive`, `:59-60`).
    connected: Option<Connected>,
}

impl fmt::Debug for AsyncArchiveClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AsyncArchiveClient")
            .field("state", &self.state)
            .field("control_session_id", &self.control_session_id())
            .finish_non_exhaustive()
    }
}

/// What a finished connect gave this client — the half of [`Connected`] it
/// keeps, which is the subscription to read and the proxy to write with.
///
/// [`Connected`]: crate::client::async_connect::Connected
struct Connected {
    subscription: i64,
    proxy: ArchiveProxy,
    control_session_id: i64,
}

impl AsyncArchiveClient {
    /// Make a client that connects to what `context` names, over the driver the
    /// context's `aeron_dir` names.
    ///
    /// **It owns that connection**: the `Client` is made on each connect and
    /// dropped on each disconnect, which is what lets a reconnect survive the
    /// driver being restarted (see the field's note).
    ///
    /// The reference also lets a caller put an Aeron client of its own on the
    /// context and keep it across reconnects (`:154-168`, the `had_aeron` half).
    /// **That shape is not here yet**, and deliberately: it is the persistent
    /// subscription's (`…_persistent_subscription.c:1001`) and what it needs
    /// from it is not known until that state machine is written. Recorded rather
    /// than guessed at.
    #[must_use]
    pub fn new(
        context: ArchiveContext,
        credentials: Box<dyn Credentials>,
        listener: Box<dyn Listener>,
    ) -> Self {
        // **Connected here rather than at the first turn**, so that a caller
        // that has to reach the driver *now* — the persistent subscription
        // allocates its four counters at create
        // (`…_persistent_subscription.c:541-584`) — can. A driver that is not up
        // is not an error: `connecting` makes the connection again on every
        // connect, so `None` here is the same state the first poll would have
        // found.
        let client = Client::connect(&std::path::PathBuf::from(&context.aeron_dir)).ok();

        Self {
            state: State::Connecting,
            client,
            context,
            credentials,
            listener,
            connect: None,
            connected: None,
        }
    }

    /// Where the client is (`aeron_archive_async_client_is_connected`, `:237`).
    #[must_use]
    pub const fn state(&self) -> State {
        self.state
    }

    /// Whether it is connected (`:237-240`).
    #[must_use]
    pub const fn is_connected(&self) -> bool {
        matches!(self.state, State::Connected)
    }

    /// Whether it is closed (`:242-245`).
    #[must_use]
    pub const fn is_closed(&self) -> bool {
        matches!(self.state, State::Closed)
    }

    /// The driver connection this client holds.
    ///
    /// For a caller that is talking to the same driver about something else —
    /// the persistent subscription, whose live and replay subscriptions are its
    /// own but whose driver is this one's — so that there is one connection
    /// rather than two. `None` before the first connect, and on the turn after
    /// a disconnect, while the next one is being made.
    ///
    /// [`NULL_VALUE`]: crate::client::proxy::NULL_VALUE
    pub fn client_mut(&mut self) -> Option<&mut Client> {
        self.client.as_mut()
    }

    /// Which session the archive named, or [`NULL_VALUE`] when there is none
    /// (`:247-253`).
    ///
    /// [`NULL_VALUE`]: crate::client::proxy::NULL_VALUE
    #[must_use]
    pub fn control_session_id(&self) -> i64 {
        match &self.connected {
            Some(connected) if matches!(self.state, State::Connected) => {
                connected.control_session_id
            }
            _ => crate::client::proxy::NULL_VALUE,
        }
    }

    /// Take one turn: connect, or read what arrived, or reconnect.
    ///
    /// Answers how much work it did, which is what the reference's conductor
    /// adds to the same count it takes from everything else in its cycle
    /// (`:151`, `:170`). A caller that only wants the side effects may ignore
    /// it.
    pub fn poll(&mut self) -> usize {
        let work = match self.state {
            State::Connecting => self.connecting(),
            State::Connected => self.connected(),
            State::Disconnected => self.disconnected(),
            State::Closed => 0,
        };

        // And the driver gets a turn, which is the other half of every turn this
        // client takes.
        //
        // The reference does not write this line because it does not have to:
        // its client lives inside a conductor, whose cycle drives the Aeron
        // client for it (`aeron_main_do_work`, and `…_persistent_subscription.c:2276-2279`
        // is where the persistent subscription says so out loud). This client
        // **owns** its driver connection, so there is nobody else to give it one
        // — and without this, nothing moves at all: the connect would sit
        // waiting for a request that was never handed to the driver.
        if let Some(client) = self.client.as_mut() {
            client.poll();
        }

        work
    }

    /// `CONNECTING` (`:148-199`): start a connect if there is not one, then give
    /// it a turn.
    fn connecting(&mut self) -> usize {
        let mut work = 0;

        // The driver first: a connect needs somewhere to connect through, and a
        // driver that is not up is not a failed connect — it is a reason to come
        // back next turn, which is what staying in `CONNECTING` means.
        if self.client.is_none() {
            let aeron_dir = std::path::PathBuf::from(&self.context.aeron_dir);

            match Client::connect(&aeron_dir) {
                Ok(client) => self.client = Some(client),
                Err(error) => {
                    self.listener
                        .on_error(&AsyncClientError::Connect(ConnectError::Client(format!(
                            "{error}"
                        ))));

                    return 1;
                }
            }

            work += 1;
        }

        if self.connect.is_none() {
            let client = self.client.as_mut().expect("made one just above");

            match AsyncConnect::new(&self.context, client, self.credentials.as_mut()) {
                Ok(connect) => self.connect = Some(connect),
                Err(error) => {
                    self.close();
                    self.listener.on_error(&AsyncClientError::Connect(error));
                    return 1;
                }
            }

            work += 1;
        }

        let before = self.connect.as_ref().expect("just made one").state();

        let (Some(connect), Some(client)) = (self.connect.as_mut(), self.client.as_mut()) else {
            return work;
        };

        match connect.poll(client, self.credentials.as_mut()) {
            Ok(Polled::Awaiting) => {
                // Still going. The reference counts a step that moved as work
                // (`:175-181`), which is how its conductor stays busy; the
                // comparison is the same one.
                if self.connect.as_ref().expect("still here").state() != before {
                    work += 1;
                }
            }
            Ok(Polled::Done(connected)) => {
                self.connect = None;
                self.connected = Some(Connected {
                    subscription: connected.subscription,
                    proxy: connected.proxy,
                    control_session_id: connected.control_session_id,
                });
                self.state = State::Connected;
                self.listener.on_connected();

                work += 1;
            }
            Err(error) => {
                // The connect is over and it failed. **The state does not
                // change**: the next turn makes another one (`:188-196`).
                self.connect = None;
                self.listener.on_error(&AsyncClientError::Connect(error));

                work += 1;
            }
        }

        work
    }

    /// `CONNECTED` (`:201-224`): is the channel still there, and what arrived
    /// on it.
    fn connected(&mut self) -> usize {
        let Some(connected) = self.connected.as_ref() else {
            // Unreachable from the states, and a silent wrong answer would be
            // worse than a loud one.
            self.state = State::Disconnected;
            return 1;
        };

        let subscription = connected.subscription;
        let control_session_id = connected.control_session_id;

        let Some(client) = self.client.as_mut() else {
            self.state = State::Disconnected;
            return 1;
        };

        let still_connected = client
            .subscription(subscription)
            .is_some_and(deepmsg_client::subscription::Subscription::is_connected);

        if !still_connected {
            self.state = State::Disconnected;
            return 1;
        }

        let mut decoder = Decoder {
            control_session_id,
            listener: self.listener.as_mut(),
            malformed: false,
        };

        let fragments =
            client.poll_subscription_controlled(subscription, FRAGMENT_LIMIT, &mut decoder);

        if 0 < fragments && decoder.malformed {
            self.close();
            self.listener.on_error(&AsyncClientError::MalformedMessage);
        }

        fragments
    }

    /// `DISCONNECTED` (`:226-233`): let go of the archive, say so, and go back
    /// to connecting.
    fn disconnected(&mut self) -> usize {
        self.connected = None;
        // And the connection goes with it: the next connect makes another one,
        // which is the reference's reset (`:154-168`) and what makes a driver
        // that died and came back something this client recovers from.
        self.client = None;
        self.listener.on_disconnected();
        self.state = State::Connecting;

        1
    }

    /// Close, and say nothing about it: the callers all report a reason
    /// themselves (`:182-186`, `:217-221`).
    ///
    /// Reachable from this crate's persistent subscription, which closes the
    /// archive client as a side effect of failing
    /// (`aeron_archive_persistent_subscription.c:72-86`).
    pub(crate) fn close(&mut self) {
        self.connected = None;
        self.connect = None;
        self.client = None;
        self.state = State::Closed;
    }

    /// Send one of this client's five requests, or answer that it cannot be
    /// sent (`:255-367`).
    ///
    /// A refusal that is the *channel's* — `Appended::NotConnected`, the
    /// reference's `ENOTCONN` — moves the client to [`State::Disconnected`], so
    /// the next turn reconnects. Every other refusal is just `false`.
    fn try_send(
        &mut self,
        send: impl FnOnce(&mut ArchiveProxy, &Client) -> Result<(), ProxyError>,
    ) -> bool {
        if !matches!(self.state, State::Connected) {
            return false;
        }

        let (Some(connected), Some(client)) = (self.connected.as_mut(), self.client.as_ref())
        else {
            return false;
        };

        match send(&mut connected.proxy, client) {
            Ok(()) => true,
            Err(ProxyError::Offer(Appended::NotConnected)) => {
                self.state = State::Disconnected;
                false
            }
            Err(_) => false,
        }
    }

    /// Ask what recordings match `recording_id` (`:255-272`).
    ///
    /// The answer is a **recording descriptor**, not a control response: it
    /// arrives at [`Listener::on_recording_descriptor`].
    pub fn try_send_list_recording_request(
        &mut self,
        correlation_id: i64,
        recording_id: i64,
    ) -> bool {
        self.try_send(|proxy, client| proxy.list_recording(client, correlation_id, recording_id))
    }

    /// Ask how far a recording has got (`:274-295`). The answer is a control
    /// response.
    pub fn try_send_max_recorded_position_request(
        &mut self,
        correlation_id: i64,
        recording_id: i64,
    ) -> bool {
        self.try_send(|proxy, client| {
            proxy.get_max_recorded_position(client, correlation_id, recording_id)
        })
    }

    /// Ask for a replay token (`:297-317`), which is what a replay over a
    /// response channel needs first.
    pub fn try_send_replay_token_request(
        &mut self,
        correlation_id: i64,
        recording_id: i64,
    ) -> bool {
        self.try_send(|proxy, client| {
            proxy.request_replay_token(client, correlation_id, recording_id)
        })
    }

    /// Ask for a replay (`:319-348`).
    ///
    /// `proxy` is the reference's optional second proxy: a replay over a
    /// **response channel** goes out on a publication the caller made, not on
    /// the control request publication, so the caller hands that one in
    /// (`…_persistent_subscription.c:1705-1723`). `None` uses this client's own,
    /// which is every other replay.
    pub fn try_send_replay_request(
        &mut self,
        proxy: Option<&mut ArchiveProxy>,
        correlation_id: i64,
        recording_id: i64,
        replay_channel: &str,
        replay_stream_id: i32,
        params: &ReplayParams,
    ) -> bool {
        match proxy {
            Some(proxy) => {
                let (Some(client), true) =
                    (self.client.as_ref(), matches!(self.state, State::Connected))
                else {
                    return false;
                };

                match proxy.replay(
                    client,
                    correlation_id,
                    recording_id,
                    replay_channel,
                    replay_stream_id,
                    params,
                ) {
                    Ok(()) => true,
                    Err(ProxyError::Offer(Appended::NotConnected)) => {
                        self.state = State::Disconnected;
                        false
                    }
                    Err(_) => false,
                }
            }
            None => self.try_send(|proxy, client| {
                proxy.replay(
                    client,
                    correlation_id,
                    recording_id,
                    replay_channel,
                    replay_stream_id,
                    params,
                )
            }),
        }
    }

    /// Stop a replay (`:350-367`).
    pub fn try_send_stop_replay_request(
        &mut self,
        correlation_id: i64,
        replay_session_id: i64,
    ) -> bool {
        self.try_send(|proxy, client| proxy.stop_replay(client, correlation_id, replay_session_id))
    }
}

/// The decode (`:370-555`): one callback per message, over the two templates
/// that share this subscription.
///
/// A struct of its own rather than a method, because the scan borrows the client
/// mutably and the listener is a field of the same value — and because the
/// reference's handler is a function with a `clientd` for the same reason.
struct Decoder<'a> {
    control_session_id: i64,
    listener: &'a mut dyn Listener,
    /// Set when a message could not be read, which is what the caller turns into
    /// a close (`:217-221`). Scoped to the poll rather than sticky: the poll
    /// that sees it closes the client, so the two differ only in what a second
    /// poll would say, and this one says the truth about its own turn.
    malformed: bool,
}

impl ControlledHandler for Decoder<'_> {
    fn on_message(&mut self, message: Message<'_>) -> Action {
        let payload = message.payload;

        let Some(header) = sbe_header(payload) else {
            self.malformed = true;
            return Action::Break;
        };

        if header.schema_id() != SBE_SCHEMA_ID {
            self.malformed = true;
            return Action::Break;
        }

        match header.template_id() {
            CONTROL_RESPONSE_TEMPLATE_ID => {
                if !holds(payload, CONTROL_RESPONSE_BLOCK_LENGTH) {
                    self.malformed = true;
                    return Action::Break;
                }

                let mut response = ControlResponseDecoder::default().header(header, 0);

                // **Somebody else's session is not this client's answer.** The
                // reconnecting client is the reason: an answer to the session it
                // had before it reconnected arrives on the same channel, and a
                // caller that dispatched it would be answering a request it made
                // against a session that no longer exists (`:417-419`).
                if response.control_session_id() != self.control_session_id {
                    return Action::Continue;
                }

                let correlation_id = response.correlation_id();
                let relevant_id = response.relevant_id();
                let code = response.code();

                let coordinates = response.error_message_decoder();
                let error_message = response.error_message_slice(coordinates);

                // On an `ERROR` the relevant id is the archive's error code, and
                // what a caller wants is the client's (`:459-461`). Every other
                // code passes it through: `RECORDING_UNKNOWN` carries the
                // recording id, and `OK` carries whatever the request was about.
                let relevant_id = if ControlResponseCode::ERROR == code {
                    archive_to_client_error_code(relevant_id)
                } else {
                    relevant_id
                };

                self.listener.on_control_response(&ControlResponse {
                    correlation_id,
                    relevant_id,
                    code,
                    error_message: &String::from_utf8_lossy(error_message),
                });

                Action::Continue
            }
            RECORDING_DESCRIPTOR_TEMPLATE_ID => {
                if !holds(payload, RECORDING_DESCRIPTOR_BLOCK_LENGTH) {
                    self.malformed = true;
                    return Action::Break;
                }

                let mut descriptor = RecordingDescriptorDecoder::default().header(header, 0);

                // The session is filtered and the correlation id is **not**: a
                // descriptor belongs to whichever request asked for it, and that
                // is the listener's to read off the descriptor (`:486-488`).
                if descriptor.control_session_id() != self.control_session_id {
                    return Action::Continue;
                }

                // The three variable fields, in the schema's declaration order —
                // the accessors are a cursor that advances, so a later read of an
                // earlier one is a different field's bytes.
                let stripped = descriptor.stripped_channel_decoder();
                let original = descriptor.original_channel_decoder();
                let identity = descriptor.source_identity_decoder();

                let recording = RecordingDescriptor {
                    control_session_id: self.control_session_id,
                    correlation_id: descriptor.correlation_id(),
                    recording_id: descriptor.recording_id(),
                    start_timestamp: descriptor.start_timestamp(),
                    stop_timestamp: descriptor.stop_timestamp(),
                    start_position: descriptor.start_position(),
                    stop_position: descriptor.stop_position(),
                    initial_term_id: descriptor.initial_term_id(),
                    segment_file_length: descriptor.segment_file_length(),
                    term_buffer_length: descriptor.term_buffer_length(),
                    mtu_length: descriptor.mtu_length(),
                    session_id: descriptor.session_id(),
                    stream_id: descriptor.stream_id(),
                    stripped_channel: String::from_utf8_lossy(
                        descriptor.stripped_channel_slice(stripped),
                    )
                    .into_owned(),
                    original_channel: String::from_utf8_lossy(
                        descriptor.original_channel_slice(original),
                    )
                    .into_owned(),
                    source_identity: String::from_utf8_lossy(
                        descriptor.source_identity_slice(identity),
                    )
                    .into_owned(),
                };

                self.listener.on_recording_descriptor(&recording);

                Action::Continue
            }
            // Not this client's business, and not an error: the reference's
            // switch has no default arm and simply moves on (`:543-553`).
            _ => Action::Continue,
        }
    }
}
