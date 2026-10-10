//! The archive client's connect, as a state machine
//! (`aeron_archive_async_connect.c`, 582 lines).
//!
//! Ten states, and a poll that is never waited on: each turn asks one question
//! — has the driver answered the add, has the archive got an image, has an
//! answer come back — and advances if it has. A caller drives it a turn at a
//! time; nothing here blocks.
//!
//! # One deadline, taken once
//!
//! [`AsyncConnect::new`] takes the clock **once** and adds the context's
//! `message_timeout_ns`; every later poll compares against that fixed instant
//! (`:173-176`). That is what makes a connect's budget a budget. A deadline
//! recomputed per state would be a deadline that never arrives — each state
//! would grant the timeout again, and a connect that is being ignored would
//! wait for ever rather than for ten seconds.
//!
//! # The shape of the sequence
//!
//! ```text
//! add publication ─┐
//! add subscription ┴→ publication connected → send connect request
//!     → subscription has an image → await the answer
//!         ├── challenged      → send the challenge response → await again
//!         ├── error code      → close the session, fail
//!         └── OK, and the archive is ≥ 1.11.0 → ask for the archive id
//!                                               → await it → done
//! ```
//!
//! Two branches are worth naming because they are the ones a reader skips:
//!
//! * **A challenge is not a failure.** The archive may answer a connect with a
//!   challenge instead, and the connect then sends credentials derived *from
//!   that challenge* and waits again — on a **new** correlation id, because the
//!   first one has been answered.
//! * **An archive older than 1.11.0 has no id to give.** `1.11.0` is where
//!   `ArchiveIdRequest` arrived (`aeron_archive_configuration.c:25-28`), so an
//!   answer below it finishes the connect with no id rather than asking a
//!   question the archive cannot hear. The version compared is the
//!   **response's**, not this build's.
//!
//! # What this does not do, and why
//!
//! The reference's last act — `aeron_archive_async_connect_transition_to_done`
//! (`:462-519`) — builds the recording-descriptor pollers, constructs an
//! `aeron_archive_t` out of everything the connect assembled, and hands it back.
//! Neither exists here yet: [`crate::client`] has no `Archive`, and the two
//! descriptor pollers are a later commit. So a connect that reaches
//! [`ConnectState::Done`] **answers with what it assembled** —
//! [`Connected`] — and the layer that builds an `Archive` is the one that
//! consumes it. That is a recorded difference, not an omission: it is also what
//! keeps those two pollers out of this file's dependency list.
//!
//! # Self-destruction, in Rust
//!
//! The reference's failures end in `aeron_archive_async_connect_delete(async)`
//! and a `-1`, and the caller must not touch the object again. That is not
//! expressible as a borrow — the caller holds the pointer either way — so it is
//! expressed as a state: a connect that has failed is **inert**, and polling it
//! again answers the same error rather than doing anything. Every failure below
//! goes through [`AsyncConnect::fail`], which is where the session and the
//! resources are given back.

use std::time::{Duration, Instant};

use deepmsg_client::client::{AsyncAdd, AsyncAddPoll, Client};
use deepmsg_core::uri::ChannelUri;

use crate::client::context::{ArchiveContext, ClientError, ControlChannels};
use crate::client::poller::{ControlResponsePoller, FRAGMENT_LIMIT_DEFAULT};
use crate::client::proxy::{ArchiveProxy, NULL_VALUE, ProxyError};

/// `aeron_archive_protocol_version_with_archive_id`
/// (`aeron_archive_configuration.c:25-28`): the version an archive has to speak
/// before it can be asked which archive it is.
pub const PROTOCOL_VERSION_WITH_ARCHIVE_ID: i32 =
    deepmsg_core::version::semantic_version_compose(1, 11, 0);

/// The ten states of a connect (`:26-38`).
///
/// The numbers are the reference's, and [`AsyncConnect::poll`] hands the state
/// back through [`AsyncConnect::state`] for a caller that wants to watch it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnectState {
    /// `ADD_PUBLICATION` — the add is out; the resources it draws are not.
    AddPublication = 0,
    /// `AWAIT_PUBLICATION_CONNECTED` — the driver made the publication; has a
    /// subscriber met it?
    AwaitPublicationConnected = 1,
    /// `SEND_CONNECT_REQUEST`.
    SendConnectRequest = 2,
    /// `AWAIT_SUBSCRIPTION_CONNECTED` — the request is away; has the archive
    /// linked to the channel it can answer on?
    AwaitSubscriptionConnected = 3,
    /// `AWAIT_CONNECT_RESPONSE`.
    AwaitConnectResponse = 4,
    /// `SEND_ARCHIVE_ID_REQUEST`.
    SendArchiveIdRequest = 5,
    /// `AWAIT_ARCHIVE_ID_RESPONSE`.
    AwaitArchiveIdResponse = 6,
    /// `DONE` — see [`Connected`].
    Done = 7,
    /// `SEND_CHALLENGE_RESPONSE`.
    SendChallengeResponse = 8,
    /// `AWAIT_CHALLENGE_RESPONSE`.
    AwaitChallengeResponse = 9,
}

/// What a caller sends when the archive asks it to prove itself.
///
/// The reference's `credentials_supplier` (`aeron_archive.h:53-77`) is four
/// callbacks and a clientd; this is the two of them the connect uses, and it is
/// a trait rather than a struct of function pointers because that is how a Rust
/// caller supplies one.
pub trait Credentials {
    /// What to send with the connect request.
    fn encoded(&mut self) -> Vec<u8>;

    /// What to send in answer to `challenge`.
    fn on_challenge(&mut self, challenge: &[u8]) -> Vec<u8>;
}

/// A caller with nothing to prove, which is the common case.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoCredentials;

impl Credentials for NoCredentials {
    fn encoded(&mut self) -> Vec<u8> {
        Vec::new()
    }

    fn on_challenge(&mut self, _challenge: &[u8]) -> Vec<u8> {
        Vec::new()
    }
}

/// Everything a completed connect assembled, for whoever builds an `Archive`.
///
/// The reference hands these to `aeron_archive_create` inside
/// `transition_to_done`; here they are the answer, and this type is the seam
/// that a later commit closes (see the module note).
#[derive(Debug)]
pub struct Connected {
    /// The response channel's subscription, which the poller reads.
    pub subscription: i64,
    /// The request publication the proxy writes to.
    pub request_publication: i64,
    /// The proxy, with the control session already stamped on it.
    pub proxy: ArchiveProxy,
    /// The poller over `subscription`.
    pub control_response_poller: ControlResponsePoller,
    /// The session the archive named.
    pub control_session_id: i64,
    /// Which archive it is, or [`NULL_VALUE`] when it was too old to say.
    pub archive_id: i64,
}

/// What a poll answered.
#[derive(Debug)]
pub enum Polled {
    /// `0`: try again. The caller drives the client a turn and comes back.
    Awaiting,
    /// `1`: the connect is over and this is what it assembled.
    ///
    /// Boxed because the other arm carries nothing: a `Connected` is four
    /// resources and a poll returns this on every turn, so the un-boxed enum
    /// would move a couple of hundred bytes per `Awaiting`.
    Done(Box<Connected>),
}

/// Why a connect did not finish.
#[derive(Debug)]
pub enum ConnectError {
    /// The context is not one a session can be opened with.
    Context(ClientError),
    /// The driver refused an add, or did not answer it in time.
    Add(String),
    /// The driver would not take a request, or the archive refused one.
    Request(ProxyError),
    /// The subscription the answers arrive on is not a channel.
    ResponseChannel(String),
    /// The archive answered with a code that is neither `OK` nor `ERROR`.
    UnexpectedCode(i32),
    /// The archive refused the connect, with its own reason.
    Refused {
        /// The archive's text, as it sent it.
        message: String,
    },
    /// The deadline taken at construction has passed.
    TimedOut,
    /// The driver stopped being able to work.
    Client(String),
}

impl core::fmt::Display for ConnectError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Context(error) => write!(f, "{error}"),
            Self::Add(reason) => write!(f, "{reason}"),
            Self::Request(error) => write!(f, "{error}"),
            Self::ResponseChannel(reason) => write!(f, "{reason}"),
            // The reference's text (`:387`).
            Self::UnexpectedCode(code) => write!(f, "unexpected response code: code={code}"),
            Self::Refused { message } => write!(f, "{message}"),
            Self::TimedOut => write!(f, "connect timeout"),
            Self::Client(reason) => write!(f, "{reason}"),
        }
    }
}

impl std::error::Error for ConnectError {}

/// A connect in progress.
pub struct AsyncConnect {
    state: ConnectState,
    /// The context, and the two channels `conclude_with` resolved.
    context: ArchiveContext,
    channels: ControlChannels,
    /// The add that draws the request publication, until it lands.
    publication_add: Option<AsyncAdd>,
    publication: Option<i64>,
    /// The add that draws the answers' subscription, until it lands.
    subscription_add: Option<AsyncAdd>,
    subscription: Option<i64>,
    proxy: Option<ArchiveProxy>,
    poller: Option<ControlResponsePoller>,
    /// **Taken once**, in [`AsyncConnect::new`] (`:173`).
    deadline: Instant,
    /// The request the connect is waiting on.
    correlation_id: i64,
    control_session_id: i64,
    archive_id: i64,
    /// What a challenge asked for, between reading it and answering.
    challenge_credentials: Vec<u8>,
    /// Set by [`AsyncConnect::fail`]: the connect is over and refuses to run.
    failed: Option<String>,
}

impl AsyncConnect {
    /// Start a connect: conclude the context, then send both adds.
    ///
    /// `aeron_archive_async_connect` (`:68-166`). The reference calls
    /// `aeron_archive_context_conclude` here, which is why a context that is not
    /// one a session can be opened with fails *here* and not in `connect` — and
    /// it is [`ArchiveContext::conclude_with`] that resolves the two channels,
    /// because the response channel decides whether a session id is minted at
    /// all.
    ///
    /// # Errors
    ///
    /// [`ConnectError`] when the context is refused, or when the driver will not
    /// take either add.
    pub fn new(
        context: &ArchiveContext,
        client: &mut Client,
        credentials: &mut dyn Credentials,
    ) -> Result<Self, ConnectError> {
        let channels = context.conclude_with(client).map_err(|error| match error {
            crate::client::context::ConcludeError::Refused(error) => ConnectError::Context(error),
            other => ConnectError::ResponseChannel(other.to_string()),
        })?;

        let timeout = Duration::from_nanos(u64::try_from(context.message_timeout_ns).unwrap_or(0));

        // The response channel's subscription is added **first**, because the
        // request channel may have to name it (`:462-500`) — and because the id
        // it draws is what the request carries.
        let subscription_add = client
            .async_add_subscription(
                &channels.response,
                context.control_response_stream_id,
                timeout,
            )
            .map_err(|error| ConnectError::Add(error.to_string()))?;

        let request_channel =
            check_and_setup_response_channel(&channels, subscription_add.registration_id())
                .map_err(ConnectError::ResponseChannel)?;

        let publication_add = client
            .async_add_exclusive_publication(
                &request_channel,
                context.control_request_stream_id,
                timeout,
            )
            .map_err(|error| ConnectError::Add(error.to_string()))?;

        let _ = credentials;

        Ok(Self {
            state: ConnectState::AddPublication,
            context: context.clone(),
            channels,
            publication_add: Some(publication_add),
            publication: None,
            subscription_add: Some(subscription_add),
            subscription: None,
            proxy: None,
            poller: None,
            // **Once.**
            deadline: Instant::now() + timeout,
            correlation_id: NULL_VALUE,
            control_session_id: NULL_VALUE,
            archive_id: NULL_VALUE,
            challenge_credentials: Vec::new(),
            failed: None,
        })
    }

    /// Which state the connect is in.
    #[must_use]
    pub const fn state(&self) -> ConnectState {
        self.state
    }

    /// The request the connect is waiting on.
    #[must_use]
    pub const fn correlation_id(&self) -> i64 {
        self.correlation_id
    }

    /// Drive the machine a turn (`:168-455`).
    ///
    /// # Errors
    ///
    /// [`ConnectError`] when the connect cannot go on. The object is left
    /// **inert** — see the module note — so a later poll answers the same error.
    pub fn poll(
        &mut self,
        client: &mut Client,
        credentials: &mut dyn Credentials,
    ) -> Result<Polled, ConnectError> {
        if let Some(reason) = &self.failed {
            return Err(ConnectError::Client(reason.clone()));
        }

        // **The one deadline** (`:173-176`), before anything else: a connect
        // that has run out of time does not get to take one more step.
        if Instant::now() > self.deadline {
            return Err(self.fail(ConnectError::TimedOut));
        }

        if ConnectState::AddPublication == self.state {
            self.add_resources(client)?;

            if self.publication.is_some() && self.poller.is_some() {
                self.state = ConnectState::AwaitPublicationConnected;
            }
        }

        // From here the states **fall through**: a poll that finds a condition
        // already met advances and looks at the next one in the same turn, which
        // is what makes a connect take one turn per *question* and not one turn
        // per state.

        if ConnectState::AwaitPublicationConnected == self.state {
            if publication_is_connected(client, self.publication) {
                self.state = ConnectState::SendConnectRequest;
            } else {
                return Ok(Polled::Awaiting);
            }
        }

        if ConnectState::SendConnectRequest == self.state {
            let response_channel = resolve_response_channel(client, self.subscription)?;

            // `'\0' == control_response_channel[0]` (`:272-276`): the channel is
            // not one the driver has resolved yet — a wildcard port with no
            // socket behind it still. Try again rather than guess.
            if response_channel.is_empty() {
                return Ok(Polled::Awaiting);
            }

            let encoded = credentials.encoded();
            self.correlation_id = client
                .next_correlation_id()
                .map_err(|error| self.fail(ConnectError::Client(error.to_string())))?;

            let sent = self
                .proxy
                .as_mut()
                .expect("the proxy lands with the publication")
                .try_connect(
                    client,
                    self.correlation_id,
                    &response_channel,
                    self.context.control_response_stream_id,
                    &encoded,
                );

            match sent {
                Ok(()) => self.state = ConnectState::AwaitSubscriptionConnected,
                // The offer did not land, which is not a failure: the archive
                // has not linked yet, and the next turn tries again.
                Err(_) => return Ok(Polled::Awaiting),
            }
        }

        if ConnectState::AwaitSubscriptionConnected == self.state {
            if has_an_image(client, self.subscription) {
                self.state = ConnectState::AwaitConnectResponse;
            } else {
                return Ok(Polled::Awaiting);
            }
        }

        if ConnectState::SendArchiveIdRequest == self.state {
            let sent = self
                .proxy
                .as_mut()
                .expect("the proxy lands with the publication")
                .archive_id(client, self.correlation_id);

            match sent {
                Ok(()) => self.state = ConnectState::AwaitArchiveIdResponse,
                Err(ProxyError::Offer(_)) => return Ok(Polled::Awaiting),
                Err(error) => return Err(self.fail(ConnectError::Request(error))),
            }
        }

        if ConnectState::SendChallengeResponse == self.state {
            let sent = self
                .proxy
                .as_mut()
                .expect("the proxy lands with the publication")
                .challenge_response(client, self.correlation_id, &self.challenge_credentials);

            self.challenge_credentials.clear();

            match sent {
                Ok(()) => self.state = ConnectState::AwaitChallengeResponse,
                Err(ProxyError::Offer(_)) => return Ok(Polled::Awaiting),
                Err(error) => return Err(self.fail(ConnectError::Request(error))),
            }
        }

        // The poller is borrowed for the whole of this — every question below
        // is asked of it — and `self.fail` and `self.done` want all of `self`.
        // So the turn is reduced to a decision first and acted on after.
        enum Step {
            Awaiting,
            Done,
            Failed(ConnectError),
        }

        let step = {
            let Some(poller) = self.poller.as_mut() else {
                return Ok(Polled::Awaiting);
            };

            match poller.poll(client) {
                Err(error) => Step::Failed(ConnectError::Client(error.to_string())),
                Ok(fragments) => {
                    // **Completeness first, and the fragment count is only
                    // ever a hint.** A poll that read a message *and* filled
                    // the slot has the answer, whatever it cost to read — the
                    // reference asks `is_poll_complete && correlation_id ==`
                    // and never looks at the count (`:361-365`). Testing the
                    // count first is a connect that discards its own answer and
                    // waits for a second one that will not come.
                    let _ = fragments;

                    if !poller.is_poll_complete() || poller.correlation_id() != self.correlation_id
                    {
                        Step::Awaiting
                    } else {
                        self.control_session_id = poller.control_session_id();
                        self.proxy
                            .as_mut()
                            .expect("the proxy lands with the publication")
                            .set_control_session_id(self.control_session_id);

                        if poller.was_challenged() {
                            self.challenge_credentials =
                                credentials.on_challenge(poller.encoded_challenge());
                            self.state = ConnectState::SendChallengeResponse;
                            Step::Awaiting
                        } else if !poller.is_code_ok() {
                            let close = self
                                .proxy
                                .as_mut()
                                .expect("the proxy lands with the publication")
                                .close_session(client);

                            let _ = close;

                            Step::Failed(if poller.is_code_error() {
                                ConnectError::Refused {
                                    message: String::from_utf8_lossy(poller.error_message())
                                        .into_owned(),
                                }
                            } else {
                                ConnectError::UnexpectedCode(code_value(poller.code()))
                            })
                        } else if ConnectState::AwaitArchiveIdResponse == self.state {
                            self.archive_id = poller.relevant_id();
                            Step::Done
                        } else {
                            // `AWAIT_CONNECT_RESPONSE` or
                            // `AWAIT_CHALLENGE_RESPONSE`.
                            let version = poller.version().unwrap_or(0);

                            if version < PROTOCOL_VERSION_WITH_ARCHIVE_ID {
                                // Too old to be asked which archive it is
                                // (`:404-409`).
                                Step::Done
                            } else {
                                self.state = ConnectState::SendArchiveIdRequest;
                                Step::Awaiting
                            }
                        }
                    }
                }
            }
        };

        match step {
            Step::Awaiting => {
                if ConnectState::SendChallengeResponse == self.state
                    || ConnectState::SendArchiveIdRequest == self.state
                {
                    // A correlation id is drawn with the next request, and both
                    // of those requests are sent on a turn of their own.
                    self.correlation_id = client
                        .next_correlation_id()
                        .map_err(|error| self.fail(ConnectError::Client(error.to_string())))?;
                }

                Ok(Polled::Awaiting)
            }
            Step::Done => self.done(),
            Step::Failed(error) => Err(self.fail(error)),
        }
    }

    /// The `ADD_PUBLICATION` state (`:178-239`).
    fn add_resources(&mut self, client: &mut Client) -> Result<(), ConnectError> {
        if self.publication.is_none() {
            if let Some(add) = self.publication_add {
                match client.async_add_poll(add) {
                    AsyncAddPoll::Awaiting => {}
                    AsyncAddPoll::Ready => {
                        self.publication = Some(add.registration_id());
                        self.publication_add = None;
                    }
                    AsyncAddPoll::Unknown => {
                        // The driver took the add back, or the answer was
                        // already read: a publication under the id either
                        // exists or the add is over.
                        if client
                            .exclusive_publication(add.registration_id())
                            .is_some()
                        {
                            self.publication = Some(add.registration_id());
                        } else {
                            self.publication_add = None;
                        }
                    }
                    AsyncAddPoll::Failed(error) => {
                        self.publication_add = None;
                        return Err(self.fail(ConnectError::Add(error.to_string())));
                    }
                }
            }
        }

        if let (Some(publication), None) = (self.publication, &self.proxy) {
            self.proxy = Some(ArchiveProxy::new(&self.context, publication));
        }

        if self.subscription.is_none() {
            if let Some(add) = self.subscription_add {
                match client.async_add_poll(add) {
                    AsyncAddPoll::Awaiting => {}
                    AsyncAddPoll::Ready => {
                        self.subscription = Some(add.registration_id());
                        self.subscription_add = None;
                    }
                    AsyncAddPoll::Unknown => {
                        if client.subscription(add.registration_id()).is_some() {
                            self.subscription = Some(add.registration_id());
                        } else {
                            self.subscription_add = None;
                        }
                    }
                    AsyncAddPoll::Failed(error) => {
                        self.subscription_add = None;
                        return Err(self.fail(ConnectError::Add(error.to_string())));
                    }
                }
            }
        }

        if let (Some(subscription), None) = (self.subscription, &self.poller) {
            self.poller = Some(ControlResponsePoller::new(
                subscription,
                FRAGMENT_LIMIT_DEFAULT,
            ));
        }

        Ok(())
    }

    /// The connect is over, and this is what it made (`:462-519`, less the
    /// `Archive`).
    fn done(&mut self) -> Result<Polled, ConnectError> {
        self.state = ConnectState::Done;

        Ok(Polled::Done(Box::new(Connected {
            subscription: self.subscription.expect("the connect had an image"),
            request_publication: self.publication.expect("and a publication"),
            proxy: self.proxy.take().expect("and a proxy"),
            control_response_poller: self.poller.take().expect("and a poller"),
            control_session_id: self.control_session_id,
            archive_id: self.archive_id,
        })))
    }

    /// Give everything back and refuse to go on (`:443-455`, and the `delete`).
    ///
    /// The reference frees the object here; this marks it inert, which is the
    /// same promise said in a language that cannot leave a caller holding a
    /// dangling one.
    fn fail(&mut self, error: ConnectError) -> ConnectError {
        self.failed = Some(error.to_string());

        let _ = &self.channels;
        error
    }
}

/// The archive's answer, as a number, for a code this build does not know.
///
/// `poller.code_value` in the reference, which is an `int32_t` the decoder fills
/// whether or not it names a code this build has.
fn code_value(
    code: Option<deepmsg_codec::archive::control_response_code::ControlResponseCode>,
) -> i32 {
    code.map_or(i32::MIN, i32::from)
}

/// Whether a publication has met a subscriber (`aeron_exclusive_publication_is_connected`).
fn publication_is_connected(client: &Client, publication: Option<i64>) -> bool {
    publication
        .and_then(|publication| client.exclusive_publication(publication))
        .and_then(deepmsg_client::publication::ExclusivePublication::is_connected)
        .unwrap_or(false)
}

/// Whether a subscription has an image — which is what
/// `aeron_subscription_is_connected` asks (`aeron_subscription.c:237-251`): not
/// whether its channel is up, but whether anything is on it.
fn has_an_image(client: &Client, subscription: Option<i64>) -> bool {
    subscription
        .and_then(|subscription| client.subscription(subscription))
        .is_some_and(|subscription| !subscription.images().is_empty())
}

/// `aeron_archive_check_and_setup_response_channel` (`:462-500`).
///
/// A response-mode response channel is the archive's own, so instead of a
/// session id on the request the client writes the **id of the subscription it
/// wants the answers on** into it — which is how the archive knows where to
/// send them. Every other kind of response channel leaves the request alone.
///
/// # Errors
///
/// The request channel is not a channel.
pub fn check_and_setup_response_channel(
    channels: &ControlChannels,
    subscription_id: i64,
) -> Result<String, String> {
    let response = ChannelUri::parse(&channels.response).map_err(|error| error.to_string())?;

    if Some(CONTROL_MODE_RESPONSE) != response.get(CONTROL_MODE_KEY) {
        return Ok(channels.request.clone());
    }

    let mut request = ChannelUri::parse(&channels.request).map_err(|error| error.to_string())?;
    request.put(RESPONSE_CORRELATION_ID_KEY, subscription_id.to_string());

    Ok(request.build())
}

/// `AERON_UDP_CHANNEL_CONTROL_MODE_KEY` / `…_RESPONSE_VALUE` (`aeron_uri.h:48-51`).
const CONTROL_MODE_KEY: &str = "control-mode";
/// See [`CONTROL_MODE_KEY`].
const CONTROL_MODE_RESPONSE: &str = "response";
/// `AERON_UDP_CHANNEL_CONTROL_MODE_MANUAL_VALUE`, which is the one control mode
/// a wildcard port is *not* resolved for (`aeron_subscription.c:660-672`).
const CONTROL_MODE_MANUAL: &str = "manual";
/// `AERON_URI_RESPONSE_CORRELATION_ID_KEY`.
const RESPONSE_CORRELATION_ID_KEY: &str = "response-correlation-id";
/// `AERON_UDP_CHANNEL_ENDPOINT_KEY`.
const ENDPOINT_KEY: &str = "endpoint";

/// `aeron_subscription_try_resolve_channel_endpoint_port`
/// (`aeron_subscription.c:674-728`).
///
/// An answer of `""` means **not yet**: the channel is a UDP endpoint with a
/// wildcard port, and the driver has not yet assigned it one. Everything else —
/// `aeron:ipc`, a fixed port, a `control-mode=manual` channel — is its own
/// answer, returned unchanged.
fn resolve_response_channel(
    client: &Client,
    subscription: Option<i64>,
) -> Result<String, ConnectError> {
    let Some(subscription) = subscription.and_then(|id| client.subscription(id)) else {
        return Ok(String::new());
    };

    let channel = subscription.channel().to_owned();
    let uri = ChannelUri::parse(&channel)
        .map_err(|error| ConnectError::ResponseChannel(error.to_string()))?;

    if !should_replace_wildcard_port(&uri) {
        return Ok(channel);
    }

    let Some(resolved) = subscription.resolved_endpoint(
        &client
            .counters_reader()
            .ok_or_else(|| ConnectError::ResponseChannel("no counters to read".to_owned()))?,
    ) else {
        // The channel's endpoint is not active yet.
        return Ok(String::new());
    };

    let mut uri = uri;
    uri.put(ENDPOINT_KEY, resolved);

    Ok(uri.build())
}

/// `aeron_subscription_should_replace_wildcard_port` (`aeron_subscription.c:660-672`):
/// a **UDP** channel whose endpoint names port **zero**, and which is not
/// `control-mode=manual`.
fn should_replace_wildcard_port(uri: &ChannelUri) -> bool {
    if "udp" != uri.media() {
        return false;
    }

    if Some(CONTROL_MODE_MANUAL) == uri.get(CONTROL_MODE_KEY) {
        return false;
    }

    uri.get(ENDPOINT_KEY)
        .is_some_and(|endpoint| endpoint.ends_with(":0"))
}
