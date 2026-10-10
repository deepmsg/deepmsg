//! The archive client: the object a caller holds, and the four ways it waits
//! (`aeron_archive_client.c`, 2617 lines).
//!
//! # The shape of every operation
//!
//! Draw a correlation id, encode a request, offer it, then **wait for the answer
//! that echoes that id**. The waiting is the interesting part and there are four
//! shapes of it, all built on one loop:
//!
//! * [`Archive::poll_next_response`] reads until the slot holds something. It is
//!   the loop, and the two things it does that a reader skips are worth naming:
//!   a **recording signal is dispatched and the loop continues** — a signal is
//!   not an answer, and a wait that returned it would report success with
//!   somebody else's correlation id; and a **stale answer for another request is
//!   handed to the error handler** rather than dropped, because the alternative
//!   is a client that never learns the archive refused something.
//! * [`Archive::wait_for_response`] is the ordinary one: the answer, or an error.
//! * [`Archive::wait_for_response_allowing_error`] is the `try_*` shape
//!   (`:147-215`): an error whose `relevantId` is the **one the caller expected**
//!   is not an error at all but a `false` — a stop-subscription that says the
//!   subscription is already gone has done what it was asked to do.
//! * [`Archive::poll_for_recording_signals`] and
//!   [`Archive::check_for_error_response`] are the same read without a request:
//!   "tell me what has happened".
//!
//! # A deadline per operation, and a deadline per connect
//!
//! Each wait takes the clock **once** and holds it for that operation
//! (`:157`, `:2095`). The connect, one layer down, does the same for itself
//! (`AsyncConnect`). The two are not the same deadline and are not meant to be:
//! a connect's budget covers making the session, and an operation's covers
//! waiting for one answer.
//!
//! # Two error codes for one refusal
//!
//! The archive speaks codes `0..=16` and the client speaks `200..=216`
//! ([`archive_to_client_error_code`], `:2609-2616`). A caller matches on the
//! client's; the wire carries the archive's; and [`ArchiveError::Refused`]
//! carries both so that neither has to be re-derived.
//!
//! # Where the requests are
//!
//! Most of the family is here, and most of it is the same five lines over
//! [`Archive::offer_and_wait`]: the positions, the four ways to stop, the
//! segments, the replica and replay controls. The listings are the ones that are
//! not — they are waits over a **poller**, because a listing is not one answer
//! (`Archive::poll_for_descriptors`,
//! [`RecordingDescriptorPoller`](crate::client::descriptor_poller::RecordingDescriptorPoller),
//! and their subscription twins).
//!
//! ## What is not here, and why each one is not
//!
//! * **`start_replay` and `replay`** (`:1364-1528`): the next commit. They are
//!   not five lines — `aeron_archive_start_replay_locked` builds a replay channel
//!   out of a `ReplayParams`, which is a string-builder port of its own.
//! * **`aeron_archive_add_recorded_publication`** and its exclusive twin
//!   (`:582-737`): they add a publication **through the client** and then record
//!   the session it was given, so they need a way to report "the client could not
//!   add it" — and [`ArchiveError`] has no variant for that, its
//!   [`Request`](ArchiveError::Request) holding a [`ProxyError`]. The second
//!   thing they do has no counterpart either: the non-exclusive one refuses a
//!   publication the driver already had, by comparing `original_registration_id`
//!   with `registration_id`, and our driver's `ON_PUBLICATION_READY`
//!   (`crates/cnc/src/command.rs:842-860`) carries the second and not the first.
//! * **`aeron_archive_stop_recording_publication`** and its two siblings
//!   (`:1074-1126`): all three are [`channel_with_session_id`] and
//!   [`Archive::stop_recording_channel_and_stream`], and nothing else. The
//!   reference's versions read the channel out of
//!   `aeron_publication_constants_t`; our `deepmsg_client`'s `Publication` carries
//!   `registration_id`, `session_id` and `stream_id`
//!   (`crates/client/src/publication.rs:37-52`) and **not** its channel, so there
//!   is no handle to do that from — the caller has the channel and composes the
//!   two.

use std::time::{Duration, Instant};

use deepmsg_client::client::{Client, CommandError};
use deepmsg_core::uri::{ChannelUri, UriError};

use crate::client::async_connect::{AsyncConnect, ConnectError, Connected, Credentials, Polled};
use crate::client::context::{ArchiveContext, SESSION_ID_KEY};
use crate::client::descriptor_poller::{
    FRAGMENT_LIMIT_DEFAULT as DESCRIPTOR_FRAGMENT_LIMIT_DEFAULT, RecordingDescriptorConsumer,
    RecordingDescriptorPoller,
};
use crate::client::poller::{ControlResponseError, ControlResponsePoller};
use crate::client::proxy::{ArchiveProxy, ProxyError, ReplayParams, ReplicationParams};
use crate::client::subscription_descriptor_poller::{
    FRAGMENT_LIMIT_DEFAULT as SUBSCRIPTION_DESCRIPTOR_FRAGMENT_LIMIT_DEFAULT,
    RecordingSubscriptionDescriptorConsumer, RecordingSubscriptionDescriptorPoller,
};

/// What a client is told when an answer arrives that it was not waiting for.
///
/// `aeron_archive_context_error_handler_func_t`, and a **plain function
/// pointer** rather than a boxed closure: the reference's handlers are function
/// pointers its context copies, so this is the shape that keeps the translation
/// honest — and a `Box<dyn FnMut>` could not be `Copy` besides.
///
/// The `error_code` is always `AERON_ERROR_CODE_GENERIC_ERROR`: the reference
/// passes 0 whatever went wrong (`aeron_archive_context.c:491`). The message is
/// the formatted `"response for correlationId=…, errorCode=…, error: …"` that a
/// C client's `aeron_errmsg` would print.
pub type ErrorHandler = fn(error_code: i32, message: &str);

/// What a client is told when a recording's lifecycle changes
/// (`aeron_archive_recording_signal_t`).
///
/// A function pointer for the reason [`ErrorHandler`] is one.
pub type RecordingSignalHandler = fn(&RecordingSignal);

/// One recording lifecycle signal, as a handler sees it.
///
/// The reference's five fields (`aeron_archive_recording_signal.h`), all of them
/// out of the `RecordingSignalEvent` (24) the poller read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecordingSignal {
    /// The session the signal was sent on.
    pub control_session_id: i64,
    /// The recording it is about.
    pub recording_id: i64,
    /// The recording subscription it is about.
    pub subscription_id: i64,
    /// Where the recording had got to.
    pub position: i64,
    /// Which signal it was — `START`, `STOP`, `DELETE` and the rest.
    pub signal: deepmsg_codec::archive::recording_signal::RecordingSignal,
}

/// `ARCHIVE_ERROR_CODE_GENERIC` (`:40`): where the codes the client remaps
/// begin, and the first of the sixteen.
const ARCHIVE_ERROR_CODE_GENERIC: i64 = 0;

/// `ARCHIVE_ERROR_CODE_INVALID_POSITION` (`:55`): the last of them.
const ARCHIVE_ERROR_CODE_INVALID_POSITION: i64 = 16;

/// `ARCHIVE_ERROR_CODE_UNKNOWN_SUBSCRIPTION` (`:43`): what the three
/// `try_stop_recording_*` forms expect, and the only refusal they do not mind.
///
/// The **archive's** domain rather than the client's: it is the number a
/// response carries as its `relevantId`, which is what
/// [`Archive::wait_for_response_allowing_error`] compares against.
const UNKNOWN_SUBSCRIPTION: i64 = 4;

/// `ARCHIVE_ERROR_CODE_UNKNOWN_REPLICATION` (`:51`): the same for
/// `tryStopReplication`.
const UNKNOWN_REPLICATION: i64 = 12;

/// `AERON_ERROR_CODE_GENERIC_ERROR`, which is what the error handler is always
/// told (`aeron_archive_context.c:491`).
const ERROR_CODE_GENERIC: i32 = 0;

/// A channel with a `session-id` put on it
/// (`aeron_archive_channel_with_session_id`, `:2592-2607`).
///
/// **This is the whole of what "record *that* publication" means.** A
/// publication's own channel does not name the session the driver gave it, and
/// the archive records a channel as it is written — so a caller that wants one
/// particular publication recorded has to say which session. Both
/// `aeron_archive_add_recorded_publication` and
/// `aeron_archive_stop_recording_publication_constants` are this call and one
/// more, which is why it is here rather than inside either of them.
///
/// **One deviation, and it is in the parameter order written back.** The
/// reference parses into a `ChannelUriStringBuilder` and prints its fields in a
/// fixed order (`ChannelUriStringBuilder.java:2451-2512`); [`ChannelUri::build`]
/// prints the parameters it read, in the order it read them, with this one put
/// back where it was or appended. A channel that had its parameters in another
/// order comes back with them kept, which is a round trip rather than a
/// normalisation.
///
/// # Errors
///
/// [`UriError`] when the channel is not an `aeron:` channel at all.
pub fn channel_with_session_id(channel: &str, session_id: i32) -> Result<String, UriError> {
    let mut uri = ChannelUri::parse(channel)?;
    uri.put(SESSION_ID_KEY, session_id.to_string());

    Ok(uri.build())
}

/// The archive's code, in the client's domain (`:2609-2616`).
///
/// `0..=16` becomes `200..=216` and anything else is passed through untouched,
/// because a code the *client* raised is already a client code and remapping it
/// would make it mean something else.
#[must_use]
pub const fn archive_to_client_error_code(error_code: i64) -> i64 {
    if ARCHIVE_ERROR_CODE_GENERIC <= error_code && error_code <= ARCHIVE_ERROR_CODE_INVALID_POSITION
    {
        error_code + 200
    } else {
        error_code
    }
}

/// Why an operation did not finish.
#[derive(Debug)]
pub enum ArchiveError {
    /// The archive refused, with its own code — which is on the wire — and the
    /// client's, which is what a caller matches on.
    Refused {
        /// The request it refused.
        correlation_id: i64,
        /// The archive's own code, `0..=16`.
        archive_error_code: i64,
        /// Its reason, as it sent it.
        message: String,
    },
    /// The subscription the answers arrive on has no image, so nothing is
    /// coming (`:131-135`).
    NotConnected,
    /// Nothing came back before the operation's deadline (`:137-141`).
    TimedOut {
        /// The reference's own name for the operation, in the message it writes.
        operation: String,
        /// The request that got no answer.
        correlation_id: i64,
    },
    /// An answer arrived that was neither `OK` nor `ERROR` (`:204-207`).
    UnexpectedCode(i32),
    /// A message on the control subscription was not a well-formed archive
    /// message.
    Malformed,
    /// The request could not be published.
    Request(ProxyError),
    /// **The client's half** of an operation could not be done: a publication
    /// added, a subscription made.
    ///
    /// The reference has no case to port. Its `aeron_async_add_publication` sets
    /// the errno and its archive functions `AERON_APPEND_ERR`, so a C caller has
    /// **one** error to read where these two halves are two types here — and
    /// what a caller does about a driver that refused to make a publication is
    /// not what it does about an archive that refused a request.
    Client(CommandError),
    /// A channel the caller gave was **not a channel**, which is what
    /// [`channel_with_session_id`] is asked to put a session on.
    ///
    /// The reference has no case: its channel helper answers `-1` and sets the
    /// errno, and the callers around it prepend `AERON_APPEND_ERR("%s", "")`.
    /// Which parse failed is the one thing a caller can act on, so it is carried.
    Channel(UriError),
}

impl ArchiveError {
    /// The code a caller matches on, in the client's domain.
    #[must_use]
    pub const fn client_error_code(&self) -> Option<i64> {
        match self {
            Self::Refused {
                archive_error_code, ..
            } => Some(archive_to_client_error_code(*archive_error_code)),
            _ => None,
        }
    }
}

impl core::fmt::Display for ArchiveError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            // The reference's text (`:182-187`), verbatim — it is what a C
            // client's errmsg prints and what a caller greps for.
            Self::Refused {
                correlation_id,
                archive_error_code,
                message,
            } => write!(
                f,
                "response for correlationId={correlation_id}, errorCode={archive_error_code}, error: {message}"
            ),
            Self::NotConnected => write!(f, "subscription to archive is not connected"),
            Self::TimedOut {
                operation,
                correlation_id,
            } => write!(
                f,
                "{operation} awaiting response - correlationId={correlation_id}"
            ),
            Self::UnexpectedCode(code) => write!(f, "unexpected response code: {code}"),
            Self::Malformed => write!(f, "malformed message on the control subscription"),
            Self::Request(error) => write!(f, "{error}"),
            Self::Client(error) => write!(f, "{error}"),
            Self::Channel(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for ArchiveError {}

/// A connected archive: a session, the publication requests go out on, and the
/// subscription the answers come back on.
///
/// `aeron_archive_t`, less the two descriptor pollers — those arrive with the
/// listings that need them, and `Connected` is the seam they will come through.
#[derive(Debug)]
pub struct Archive {
    /// The context this archive was opened with. The reference owns a
    /// *duplicate* of the caller's (`:330`); here it is moved in, which is the
    /// same promise without the copy.
    context: ArchiveContext,
    proxy: ArchiveProxy,
    /// The subscription the answers arrive on.
    subscription: i64,
    control_response_poller: ControlResponsePoller,
    /// Reads a listing. The reference makes both this and its subscription twin
    /// in `transition_to_done`; this one is made where the connect's answer is
    /// turned into an `Archive`, which is the same place with a different name.
    recording_descriptor_poller: RecordingDescriptorPoller,
    /// Reads a listing **of the archive's subscriptions**, which is a listing of
    /// a different thing on the same subscription — see
    /// [`Archive::poll_for_subscription_descriptors`].
    recording_subscription_descriptor_poller: RecordingSubscriptionDescriptorPoller,
    control_session_id: i64,
    archive_id: i64,
    /// The two callbacks, per connection rather than per configuration.
    ///
    /// **A recorded difference.** The reference keeps these on the *context* and
    /// its `duplicate` copies them — and its own case asserts the copy's is
    /// equal to the original's (`aeron_archive_test.cpp:3388`). Rust cannot make
    /// that claim: `rustc` warns that function pointer comparisons "do not
    /// produce meaningful results since their addresses are not guaranteed to be
    /// unique", so the assertion has no counterpart here whichever type they
    /// live on. Keeping them off the context is the better half of that trade:
    /// the context stays a value that two of this slice's criteria compare
    /// (`shouldDuplicateContext`, in P2-C1's second commit), and these are
    /// per-connection in every way that matters — they are handed to an archive,
    /// not to the configuration it was built from.
    handlers: Handlers,
}

/// The callbacks an [`Archive`] dispatches to.
#[derive(Clone, Copy, Debug, Default)]
pub struct Handlers {
    /// Told when an answer arrives that this client was not waiting for.
    pub error: Option<ErrorHandler>,
    /// Told when a recording's lifecycle changes.
    pub recording_signal: Option<RecordingSignalHandler>,
}

impl Archive {
    /// Connect, and wait for it (`aeron_archive_connect`, `:264-308`).
    ///
    /// The synchronous face of [`AsyncConnect`]: poll it, drive the client,
    /// idle, and come back — until it finishes or fails. The reference's loop
    /// idles only when the state has not *changed* (`:280-287`), which is the
    /// difference between a connect that burns a core and one that does not.
    ///
    /// # Errors
    ///
    /// [`ConnectError`], from the layer below.
    pub fn connect(
        context: &ArchiveContext,
        client: &mut Client,
        credentials: &mut dyn Credentials,
        handlers: Handlers,
    ) -> Result<Self, ConnectError> {
        let mut connect = AsyncConnect::new(context, client, credentials)?;

        loop {
            match connect.poll(client, credentials) {
                Ok(Polled::Done(connected)) => {
                    return Ok(Self::from_connected(context.clone(), *connected, handlers));
                }
                Ok(Polled::Awaiting) => {}
                Err(error) => return Err(error),
            }

            client.poll();
            idle();
        }
    }

    /// What a finished connect assembled.
    fn from_connected(context: ArchiveContext, connected: Connected, handlers: Handlers) -> Self {
        let recording_descriptor_poller = RecordingDescriptorPoller::new(
            connected.subscription,
            connected.control_session_id,
            DESCRIPTOR_FRAGMENT_LIMIT_DEFAULT,
            // The signals a listing sees go where every other one goes, so the
            // handler is the same one — defaulted to a no-op when there is none,
            // because the poller dispatches unconditionally.
            handlers.recording_signal.unwrap_or(ignore_signal),
        );

        let recording_subscription_descriptor_poller = RecordingSubscriptionDescriptorPoller::new(
            connected.subscription,
            connected.control_session_id,
            SUBSCRIPTION_DESCRIPTOR_FRAGMENT_LIMIT_DEFAULT,
            handlers.recording_signal.unwrap_or(ignore_signal),
        );

        Self {
            context,
            recording_descriptor_poller,
            recording_subscription_descriptor_poller,
            handlers,
            proxy: connected.proxy,
            subscription: connected.subscription,
            control_response_poller: connected.control_response_poller,
            control_session_id: connected.control_session_id,
            archive_id: connected.archive_id,
        }
    }

    /// Which archive this is (`aeron_archive_get_archive_id`, `:2069-2086`).
    #[must_use]
    pub const fn archive_id(&self) -> i64 {
        self.archive_id
    }

    /// The session every request is addressed to (`:403-406`).
    #[must_use]
    pub const fn control_session_id(&self) -> i64 {
        self.control_session_id
    }

    /// The context this archive was opened with.
    #[must_use]
    pub const fn context(&self) -> &ArchiveContext {
        &self.context
    }

    /// The subscription the answers arrive on.
    #[must_use]
    pub const fn subscription(&self) -> i64 {
        self.subscription
    }

    /// The proxy, for a caller that needs to send something itself.
    #[must_use]
    pub const fn proxy(&self) -> &ArchiveProxy {
        &self.proxy
    }

    /// Leave, and tell the archive so (`aeron_archive_close`, `:347-386`).
    ///
    /// The session is closed **only if the request publication is connected** —
    /// closing a session over a publication nobody is reading is an offer that
    /// cannot land, and the reference's own condition (`:351-354`) is that it
    /// does not try.
    pub fn close(&mut self, client: &Client) {
        let connected = client
            .exclusive_publication(self.proxy.request_publication())
            .and_then(deepmsg_client::publication::ExclusivePublication::is_connected)
            .unwrap_or(false);

        if connected {
            let _ = self.proxy.close_session(client);
        }
    }

    /// The next correlation id, drawn from the client (`:408-411`).
    ///
    /// # Errors
    ///
    /// When the client's command ring cannot be read.
    pub fn next_correlation_id(&self, client: &mut Client) -> Result<i64, ArchiveError> {
        client
            .next_correlation_id()
            .map_err(|_| ArchiveError::Request(ProxyError::Closed))
    }

    // -----------------------------------------------------------------------
    // The four ways of waiting
    // -----------------------------------------------------------------------

    /// Read until the slot holds something (`aeron_archive_poll_next_response`,
    /// `:89-146`).
    ///
    /// **A recording signal is not an answer.** One for this session is
    /// dispatched to the handler and the loop goes round again — so an operation
    /// that is waiting for a response while a recording starts and stops still
    /// gets its response, and the signals still arrive. Returning it instead
    /// would hand the caller a correlation id it never asked about and call it
    /// success.
    ///
    /// **An answer for another correlation id is not this operation's either** —
    /// but that is the caller's test, above this loop; here every completed read
    /// is simply a completed read.
    fn poll_next_response(
        &mut self,
        client: &mut Client,
        operation_name: &str,
        correlation_id: i64,
        deadline: Instant,
    ) -> Result<(), ArchiveError> {
        loop {
            let fragments = self
                .control_response_poller
                .poll(client)
                .map_err(map_poller_error)?;

            if self.control_response_poller.is_poll_complete() {
                if self.control_response_poller.is_recording_signal()
                    && self.control_response_poller.control_session_id() == self.control_session_id
                {
                    self.dispatch_recording_signal();

                    continue;
                }

                return Ok(());
            }

            if fragments > 0 {
                continue;
            }

            if !self.subscription_is_connected(client) {
                return Err(ArchiveError::NotConnected);
            }

            if Instant::now() > deadline {
                return Err(ArchiveError::TimedOut {
                    operation: operation_name.to_owned(),
                    correlation_id,
                });
            }

            idle();
            client.poll();
        }
    }

    /// Wait for the answer to `correlation_id`, and answer its `relevantId`
    /// (`aeron_archive_poll_for_response`, `:2088-2152`).
    ///
    /// # Errors
    ///
    /// [`ArchiveError::Refused`] when the archive sent an `ERROR` for this
    /// request; anything else the loop found.
    pub fn wait_for_response(
        &mut self,
        client: &mut Client,
        operation_name: &str,
        correlation_id: i64,
    ) -> Result<i64, ArchiveError> {
        let deadline = Instant::now() + self.operation_timeout();

        loop {
            self.poll_next_response(client, operation_name, correlation_id, deadline)?;

            // An answer for a *session this client is not*  is not this
            // operation's, and neither is one for another correlation id: both
            // are somebody else's traffic on a shared subscription.
            if self.control_response_poller.control_session_id() != self.control_session_id {
                client.poll();

                continue;
            }

            let poller_correlation = self.control_response_poller.correlation_id();

            if self.control_response_poller.is_code_error() {
                if poller_correlation == correlation_id {
                    return Err(self.refused(correlation_id));
                }

                self.handle_error_with_handler(client);

                continue;
            }

            if poller_correlation == correlation_id {
                if !self.control_response_poller.is_code_ok() {
                    return Err(ArchiveError::UnexpectedCode(code_value(
                        self.control_response_poller.code(),
                    )));
                }

                return Ok(self.control_response_poller.relevant_id());
            }
        }
    }

    /// The same, for a caller that **expects** one error (`:147-215`).
    ///
    /// Answers `true` when the archive said `OK`, and `false` when it refused
    /// with exactly `expected_error_code` — which is not a failure. The `try_*`
    /// requests are the callers (`try_stop_recording_subscription` expects
    /// `UNKNOWN_SUBSCRIPTION`): "stop it" against something already stopped has
    /// done what it was asked to.
    ///
    /// # Errors
    ///
    /// Any refusal that is **not** the expected one.
    pub fn wait_for_response_allowing_error(
        &mut self,
        client: &mut Client,
        operation_name: &str,
        correlation_id: i64,
        expected_error_code: i64,
    ) -> Result<bool, ArchiveError> {
        let deadline = Instant::now() + self.operation_timeout();

        loop {
            self.poll_next_response(client, operation_name, correlation_id, deadline)?;

            if self.control_response_poller.control_session_id() != self.control_session_id {
                client.poll();

                continue;
            }

            let poller_correlation = self.control_response_poller.correlation_id();

            if self.control_response_poller.is_code_error() {
                if poller_correlation == correlation_id {
                    if self.control_response_poller.relevant_id() == expected_error_code {
                        return Ok(false);
                    }

                    return Err(self.refused(correlation_id));
                }

                self.handle_error_with_handler(client);

                continue;
            }

            if poller_correlation == correlation_id {
                if !self.control_response_poller.is_code_ok() {
                    return Err(ArchiveError::UnexpectedCode(code_value(
                        self.control_response_poller.code(),
                    )));
                }

                return Ok(true);
            }
        }
    }

    /// Read once and report what happened (`aeron_archive_poll_for_recording_signals`,
    /// `:413-464`).
    ///
    /// One poll, no waiting: answers how many recording signals were dispatched.
    /// An `ERROR` that belongs to nobody this caller asked about goes to the
    /// error handler, or — with no handler — comes back as this call's error,
    /// because a refusal nobody looks at is a refusal nobody acts on.
    ///
    /// # Errors
    ///
    /// [`ArchiveError::Refused`] for an error response when there is no error
    /// handler to take it.
    pub fn poll_for_recording_signals(&mut self, client: &mut Client) -> Result<u32, ArchiveError> {
        self.control_response_poller
            .poll(client)
            .map_err(map_poller_error)?;

        if !self.control_response_poller.is_poll_complete()
            || self.control_response_poller.control_session_id() != self.control_session_id
        {
            return Ok(0);
        }

        if self.control_response_poller.is_control_response()
            && self.control_response_poller.is_code_error()
        {
            let correlation_id = self.control_response_poller.correlation_id();

            if self.handlers.error.is_none() {
                return Err(self.refused(correlation_id));
            }

            self.handle_error_with_handler(client);

            return Ok(0);
        }

        if self.control_response_poller.is_recording_signal() {
            self.dispatch_recording_signal();

            return Ok(1);
        }

        Ok(0)
    }

    /// Read once and report a refusal, if there is one (`:515-579`).
    ///
    /// The read a caller drives when it has nothing to wait for.
    ///
    /// # Errors
    ///
    /// [`ArchiveError::NotConnected`] when the answers' subscription has no
    /// image, and [`ArchiveError::Refused`] for an error response with no error
    /// handler to take it.
    pub fn check_for_error_response(&mut self, client: &mut Client) -> Result<(), ArchiveError> {
        if !self.subscription_is_connected(client) {
            if self.handlers.error.is_none() {
                return Err(ArchiveError::NotConnected);
            }

            self.invoke_error_handler(ERROR_CODE_GENERIC, "not connected");

            return Ok(());
        }

        self.control_response_poller
            .poll(client)
            .map_err(map_poller_error)?;

        if !self.control_response_poller.is_poll_complete()
            || self.control_response_poller.control_session_id() != self.control_session_id
        {
            return Ok(());
        }

        if self.control_response_poller.is_control_response()
            && self.control_response_poller.is_code_error()
        {
            let correlation_id = self.control_response_poller.correlation_id();

            if self.handlers.error.is_none() {
                return Err(self.refused(correlation_id));
            }

            self.handle_error_with_handler(client);
        } else if self.control_response_poller.is_recording_signal() {
            self.dispatch_recording_signal();
        }

        Ok(())
    }

    /// Read a listing, handing each descriptor to `consumer`
    /// (`aeron_archive_poll_for_descriptors`, `:2153-2222`).
    ///
    /// **This wait's deadline re-arms on every descriptor.** The others take the
    /// clock once and hold it, but a listing that ran short would be given the
    /// whole timeout again after each of the many descriptors it did get
    /// (`:2186-2191`) — so the budget is "the timeout since the last thing
    /// arrived", not "since the request went out". A large listing on a busy
    /// archive is the case that tells the two apart.
    ///
    /// Answers how many descriptors reached the consumer, which is **not** the
    /// count asked for: the archive may have fewer, and says so with
    /// `RECORDING_UNKNOWN`.
    ///
    /// # Errors
    ///
    /// [`ArchiveError::NotConnected`], [`ArchiveError::TimedOut`], and whatever
    /// the poller found.
    pub fn poll_for_descriptors(
        &mut self,
        client: &mut Client,
        operation_name: &str,
        correlation_id: i64,
        record_count: i32,
        consumer: RecordingDescriptorConsumer,
    ) -> Result<i32, ArchiveError> {
        self.recording_descriptor_poller
            .reset(correlation_id, record_count, consumer);

        let mut deadline = Instant::now() + self.operation_timeout();
        let mut previous_remaining = record_count;

        loop {
            let fragments = self
                .recording_descriptor_poller
                .poll(client)
                .map_err(map_poller_error)?;

            let remaining = self.recording_descriptor_poller.remaining_record_count();

            if self.recording_descriptor_poller.is_dispatch_complete() {
                return Ok(record_count - remaining);
            }

            if remaining != previous_remaining {
                previous_remaining = remaining;
                deadline = Instant::now() + self.operation_timeout();
            }

            if fragments > 0 {
                client.poll();

                continue;
            }

            if !self.subscription_is_connected(client) {
                return Err(ArchiveError::NotConnected);
            }

            if Instant::now() > deadline {
                return Err(ArchiveError::TimedOut {
                    operation: operation_name.to_owned(),
                    correlation_id,
                });
            }

            idle();
            client.poll();
        }
    }

    /// Read a listing **of the archive's subscriptions**, handing each
    /// descriptor to `consumer`
    /// (`aeron_archive_poll_for_subscription_descriptors`, `:2201-2262`).
    ///
    /// [`Self::poll_for_descriptors`]'s twin, down to the deadline that re-arms
    /// on every descriptor. What differs is which poller reads — this one reads
    /// [`RecordingSubscriptionDescriptorPoller`], whose terminator is
    /// `SUBSCRIPTION_UNKNOWN` rather than `RECORDING_UNKNOWN`.
    ///
    /// # Errors
    ///
    /// [`ArchiveError::NotConnected`], [`ArchiveError::TimedOut`], and whatever
    /// the poller found.
    pub fn poll_for_subscription_descriptors(
        &mut self,
        client: &mut Client,
        operation_name: &str,
        correlation_id: i64,
        subscription_count: i32,
        consumer: RecordingSubscriptionDescriptorConsumer,
    ) -> Result<i32, ArchiveError> {
        self.recording_subscription_descriptor_poller.reset(
            correlation_id,
            subscription_count,
            consumer,
        );

        let mut deadline = Instant::now() + self.operation_timeout();
        let mut previous_remaining = subscription_count;

        loop {
            let fragments = self
                .recording_subscription_descriptor_poller
                .poll(client)
                .map_err(map_poller_error)?;

            let remaining = self
                .recording_subscription_descriptor_poller
                .remaining_subscription_count();

            if self
                .recording_subscription_descriptor_poller
                .is_dispatch_complete()
            {
                return Ok(subscription_count - remaining);
            }

            if remaining != previous_remaining {
                previous_remaining = remaining;
                deadline = Instant::now() + self.operation_timeout();
            }

            if fragments > 0 {
                client.poll();

                continue;
            }

            if !self.subscription_is_connected(client) {
                return Err(ArchiveError::NotConnected);
            }

            if Instant::now() > deadline {
                return Err(ArchiveError::TimedOut {
                    operation: operation_name.to_owned(),
                    correlation_id,
                });
            }

            idle();
            client.poll();
        }
    }

    // -----------------------------------------------------------------------
    // The requests these waits are exercised with
    // -----------------------------------------------------------------------

    /// Start a recording, and answer the **subscription id** the archive made
    /// (`aeron_archive_start_recording`, `:738-774`).
    ///
    /// # Errors
    ///
    /// Whatever [`Self::offer_and_wait`] found.
    pub fn start_recording(
        &mut self,
        client: &mut Client,
        recording_channel: &str,
        recording_stream_id: i32,
        local_source: bool,
        auto_stop: bool,
    ) -> Result<i64, ArchiveError> {
        self.offer_and_wait(
            client,
            "AeronArchive::startRecording",
            |client, proxy, id| {
                proxy.start_recording(
                    client,
                    id,
                    recording_channel,
                    recording_stream_id,
                    local_source,
                    auto_stop,
                )
            },
        )
    }

    /// List recordings, from a cursor onward
    /// (`aeron_archive_list_recordings`, `:1206-1247`).
    ///
    /// # Errors
    ///
    /// Whatever the listing found.
    pub fn list_recordings(
        &mut self,
        client: &mut Client,
        from_recording_id: i64,
        record_count: i32,
        consumer: RecordingDescriptorConsumer,
    ) -> Result<i32, ArchiveError> {
        let correlation_id = self.next_correlation_id(client)?;

        self.proxy
            .list_recordings(client, correlation_id, from_recording_id, record_count)
            .map_err(ArchiveError::Request)?;

        self.poll_for_descriptors(
            client,
            "AeronArchive::listRecordings",
            correlation_id,
            record_count,
            consumer,
        )
    }

    /// One recording, named by id (`aeron_archive_list_recording`, `:1165-1204`).
    ///
    /// The count this asks in for is **`1`** and not a parameter: the caller
    /// named the recording, so there is nothing to page through. A recording the
    /// archive does not hold is not a refusal here — the archive answers
    /// `RECORDING_UNKNOWN`, which ends the listing, so this comes back `Ok(0)`
    /// with nothing handed to the consumer.
    ///
    /// # Errors
    ///
    /// Whatever the listing found.
    pub fn list_recording(
        &mut self,
        client: &mut Client,
        recording_id: i64,
        consumer: RecordingDescriptorConsumer,
    ) -> Result<i32, ArchiveError> {
        let correlation_id = self.next_correlation_id(client)?;

        self.proxy
            .list_recording(client, correlation_id, recording_id)
            .map_err(ArchiveError::Request)?;

        self.poll_for_descriptors(
            client,
            "AeronArchive::listRecording",
            correlation_id,
            1,
            consumer,
        )
    }

    /// A listing narrowed to one channel and stream
    /// (`aeron_archive_list_recordings_for_uri`, `:1249-1291`).
    ///
    /// # Errors
    ///
    /// Whatever the listing found.
    pub fn list_recordings_for_uri(
        &mut self,
        client: &mut Client,
        from_recording_id: i64,
        record_count: i32,
        channel_fragment: &str,
        stream_id: i32,
        consumer: RecordingDescriptorConsumer,
    ) -> Result<i32, ArchiveError> {
        let correlation_id = self.next_correlation_id(client)?;

        self.proxy
            .list_recordings_for_uri(
                client,
                correlation_id,
                from_recording_id,
                record_count,
                channel_fragment,
                stream_id,
            )
            .map_err(ArchiveError::Request)?;

        self.poll_for_descriptors(
            client,
            "AeronArchive::listRecordingsForUri",
            correlation_id,
            record_count,
            consumer,
        )
    }

    /// The archive's recording subscriptions, from a cursor onward
    /// (`aeron_archive_list_recording_subscriptions`, `:1626-1673`).
    ///
    /// The reference takes a `stream_id` and an `apply_stream_id` — a `bool`
    /// rather than a null because the field is an `int32` on the wire and `0` is
    /// a stream id — and `false` makes the id a value nothing reads. That pair
    /// is [`Option`] here: `Some(0)` is stream `0` and `None` is "any stream",
    /// which is the two states that mean something, and the wire still carries
    /// both fields the way it always did.
    ///
    /// # Errors
    ///
    /// Whatever the listing found.
    pub fn list_recording_subscriptions(
        &mut self,
        client: &mut Client,
        pseudo_index: i32,
        subscription_count: i32,
        channel_fragment: &str,
        stream_id: Option<i32>,
        consumer: RecordingSubscriptionDescriptorConsumer,
    ) -> Result<i32, ArchiveError> {
        let correlation_id = self.next_correlation_id(client)?;

        self.proxy
            .list_recording_subscriptions(
                client,
                correlation_id,
                pseudo_index,
                subscription_count,
                channel_fragment,
                stream_id.unwrap_or(0),
                stream_id.is_some(),
            )
            .map_err(ArchiveError::Request)?;

        self.poll_for_subscription_descriptors(
            client,
            "AeronArchive::listRecordingSubscriptions",
            correlation_id,
            subscription_count,
            consumer,
        )
    }

    /// The newest recording at or after `min_recording_id` whose session, stream
    /// and channel match
    /// (`aeron_archive_find_last_matching_recording`, `:1127-1163`).
    ///
    /// `-1` here is an **answer**, not a failure: it is the archive saying there
    /// is no such recording, and of the questions about a recording this is the
    /// one whose "not found" is a value (`ArchiveConductor.java:754-757`).
    /// A `min_recording_id` below zero is a different matter — the archive
    /// **refuses** it (`:749-753`), and that comes back as
    /// [`ArchiveError::Refused`].
    ///
    /// # Errors
    ///
    /// Whatever [`Self::offer_and_wait`] found.
    pub fn find_last_matching_recording(
        &mut self,
        client: &mut Client,
        min_recording_id: i64,
        channel_fragment: &str,
        stream_id: i32,
        session_id: i32,
    ) -> Result<i64, ArchiveError> {
        self.offer_and_wait(
            client,
            "AeronArchive::findLastMatchingRecording",
            |client, proxy, id| {
                proxy.find_last_matching_recording(
                    client,
                    id,
                    min_recording_id,
                    channel_fragment,
                    stream_id,
                    session_id,
                )
            },
        )
    }

    // -----------------------------------------------------------------------
    // Stopping what is running
    // -----------------------------------------------------------------------
    //
    // Four ways to stop a recording, and they are not four names for one thing:
    // a **subscription** is what the archive opened, a **channel and stream** is
    // what the caller asked for, an **identity** is the recording, and each of
    // the last three has a `try_` form whose "there was nothing to stop" is a
    // `false` rather than an error. `aeron_archive_client.c:904-1070`.

    /// Stop recording by the subscription the archive opened
    /// (`:904-933`).
    ///
    /// # Errors
    ///
    /// Whatever [`Self::offer_and_wait`] found.
    pub fn stop_recording_subscription(
        &mut self,
        client: &mut Client,
        subscription_id: i64,
    ) -> Result<(), ArchiveError> {
        self.offer_and_wait(
            client,
            "AeronArchive::stopRecordingSubscription",
            |client, proxy, id| proxy.stop_recording_subscription(client, id, subscription_id),
        )
        .map(|_| ())
    }

    /// The same, for a caller that does not mind there being nothing to stop
    /// (`:935-966`).
    ///
    /// Answers whether it stopped one: a refusal the archive words as
    /// `UNKNOWN_SUBSCRIPTION` is the caller's expected answer rather than this
    /// call's failure.
    ///
    /// # Errors
    ///
    /// Any refusal that is **not** that one.
    pub fn try_stop_recording_subscription(
        &mut self,
        client: &mut Client,
        subscription_id: i64,
    ) -> Result<bool, ArchiveError> {
        self.offer_and_wait_allowing_error(
            client,
            "AeronArchive::tryStopRecordingSubscription",
            UNKNOWN_SUBSCRIPTION,
            |client, proxy, id| proxy.stop_recording_subscription(client, id, subscription_id),
        )
    }

    /// Stop recording by the channel and stream it was asked for
    /// (`:968-999`).
    ///
    /// # Errors
    ///
    /// Whatever [`Self::offer_and_wait`] found.
    pub fn stop_recording_channel_and_stream(
        &mut self,
        client: &mut Client,
        channel: &str,
        stream_id: i32,
    ) -> Result<(), ArchiveError> {
        self.offer_and_wait(
            client,
            "AeronArchive::stopRecording",
            |client, proxy, id| proxy.stop_recording(client, id, channel, stream_id),
        )
        .map(|_| ())
    }

    /// The same, for a caller that does not mind there being nothing to stop
    /// (`:1001-1034`).
    ///
    /// # Errors
    ///
    /// Any refusal that is not `UNKNOWN_SUBSCRIPTION`.
    pub fn try_stop_recording_channel_and_stream(
        &mut self,
        client: &mut Client,
        channel: &str,
        stream_id: i32,
    ) -> Result<bool, ArchiveError> {
        self.offer_and_wait_allowing_error(
            client,
            "AeronArchive::tryStopRecordingChannelAndStream",
            UNKNOWN_SUBSCRIPTION,
            |client, proxy, id| proxy.stop_recording(client, id, channel, stream_id),
        )
    }

    /// Stop recording by the recording's own id (`:1036-1072`).
    ///
    /// **This one is a plain wait**, and it is the reference's shape rather than
    /// an oversight: the archive answers `OK` with a `relevantId` that is zero
    /// when there was nothing to stop, so the caller's "did it" is read out of a
    /// response that is not a refusal. The `try_` in the name and the absence of
    /// an expected error code are both the reference's.
    ///
    /// # Errors
    ///
    /// Whatever [`Self::offer_and_wait`] found.
    pub fn try_stop_recording_by_identity(
        &mut self,
        client: &mut Client,
        recording_id: i64,
    ) -> Result<bool, ArchiveError> {
        self.offer_and_wait(
            client,
            "AeronArchive::tryStopRecordingByIdentity",
            |client, proxy, id| proxy.stop_recording_by_identity(client, id, recording_id),
        )
        .map(|relevant_id| relevant_id != 0)
    }

    /// Cut a recording's log down to its first `position`
    /// (`aeron_archive_truncate_recording`, `:1530-1562`).
    ///
    /// Answers the count the archive reports — see the reference's own field,
    /// `count_p`.
    ///
    /// # Errors
    ///
    /// Whatever [`Self::offer_and_wait`] found.
    pub fn truncate_recording(
        &mut self,
        client: &mut Client,
        recording_id: i64,
        position: i64,
    ) -> Result<i64, ArchiveError> {
        self.offer_and_wait(
            client,
            "AeronArchive::truncateRecording",
            |client, proxy, id| proxy.truncate_recording(client, id, recording_id, position),
        )
    }

    /// Stop one replay, by the session the archive gave it
    /// (`aeron_archive_stop_replay`, `:1564-1593`).
    ///
    /// # Errors
    ///
    /// Whatever [`Self::offer_and_wait`] found.
    pub fn stop_replay(
        &mut self,
        client: &mut Client,
        replay_session_id: i64,
    ) -> Result<(), ArchiveError> {
        self.offer_and_wait(client, "AeronArchive::stopReplay", |client, proxy, id| {
            proxy.stop_replay(client, id, replay_session_id)
        })
        .map(|_| ())
    }

    /// Stop every replay of one recording (`:1595-1624`).
    ///
    /// # Errors
    ///
    /// Whatever [`Self::offer_and_wait`] found.
    pub fn stop_all_replays(
        &mut self,
        client: &mut Client,
        recording_id: i64,
    ) -> Result<(), ArchiveError> {
        self.offer_and_wait(
            client,
            "AeronArchive::stopAllReplays",
            |client, proxy, id| proxy.stop_all_replays(client, id, recording_id),
        )
        .map(|_| ())
    }

    /// Forget a recording entirely (`aeron_archive_purge_recording`,
    /// `:1675-1705`).
    ///
    /// Answers the number of segments the archive deleted.
    ///
    /// # Errors
    ///
    /// Whatever [`Self::offer_and_wait`] found.
    pub fn purge_recording(
        &mut self,
        client: &mut Client,
        recording_id: i64,
    ) -> Result<i64, ArchiveError> {
        self.offer_and_wait(
            client,
            "AeronArchive::purgeRecording",
            |client, proxy, id| proxy.purge_recording(client, id, recording_id),
        )
    }

    /// Keep recording a stopped recording's channel as a **new** recording
    /// (`aeron_archive_extend_recording`, `:1707-1745`).
    ///
    /// Answers the new recording's **subscription** id, which is what the
    /// reference's `subscription_id_p` is — not the recording id, which the
    /// caller does not learn from this answer.
    ///
    /// `local_source` is the reference's way of saying the source is this
    /// process's own publication rather than one somewhere else.
    ///
    /// # Errors
    ///
    /// Whatever [`Self::offer_and_wait`] found.
    pub fn extend_recording(
        &mut self,
        client: &mut Client,
        recording_id: i64,
        recording_channel: &str,
        recording_stream_id: i32,
        local_source: bool,
        auto_stop: bool,
    ) -> Result<i64, ArchiveError> {
        self.offer_and_wait(
            client,
            "AeronArchive::extendRecording",
            |client, proxy, id| {
                proxy.extend_recording(
                    client,
                    recording_id,
                    recording_channel,
                    recording_stream_id,
                    local_source,
                    auto_stop,
                    id,
                )
            },
        )
    }

    /// Ask another archive to replicate a recording here
    /// (`aeron_archive_replicate`, `:1747-1783`).
    ///
    /// Answers the replication's id, which is what a caller stops it with.
    ///
    /// # Errors
    ///
    /// Whatever [`Self::offer_and_wait`] found.
    pub fn replicate(
        &mut self,
        client: &mut Client,
        src_recording_id: i64,
        src_control_stream_id: i32,
        src_control_channel: &str,
        params: &ReplicationParams,
    ) -> Result<i64, ArchiveError> {
        self.offer_and_wait(client, "AeronArchive::replicate", |client, proxy, id| {
            proxy.replicate(
                client,
                id,
                src_recording_id,
                src_control_stream_id,
                src_control_channel,
                params,
            )
        })
    }

    /// Stop one replication (`:1785-1814`).
    ///
    /// # Errors
    ///
    /// Whatever [`Self::offer_and_wait`] found.
    pub fn stop_replication(
        &mut self,
        client: &mut Client,
        replication_id: i64,
    ) -> Result<(), ArchiveError> {
        self.offer_and_wait(
            client,
            "AeronArchive::stopReplication",
            |client, proxy, id| proxy.stop_replication(client, id, replication_id),
        )
        .map(|_| ())
    }

    /// The same, for a caller that does not mind there being nothing to stop
    /// (`:1816-1847`).
    ///
    /// # Errors
    ///
    /// Any refusal that is not `UNKNOWN_REPLICATION`.
    pub fn try_stop_replication(
        &mut self,
        client: &mut Client,
        replication_id: i64,
    ) -> Result<bool, ArchiveError> {
        self.offer_and_wait_allowing_error(
            client,
            "AeronArchive::tryStopReplication",
            UNKNOWN_REPLICATION,
            |client, proxy, id| proxy.stop_replication(client, id, replication_id),
        )
    }

    // -----------------------------------------------------------------------
    // Segments
    // -----------------------------------------------------------------------
    //
    // The five of `aeron_archive_client.c:1849-1990`. Four of them answer a
    // **count** — how many segments the archive moved — and `detachSegments` is
    // the one that does not, which is the reference's `NULL` rather than a
    // choice here.

    /// Detach a recording's segments from `new_start_position` onward
    /// (`:1849-1880`).
    ///
    /// # Errors
    ///
    /// Whatever [`Self::offer_and_wait`] found.
    pub fn detach_segments(
        &mut self,
        client: &mut Client,
        recording_id: i64,
        new_start_position: i64,
    ) -> Result<(), ArchiveError> {
        self.offer_and_wait(
            client,
            "AeronArchive::detachSegments",
            |client, proxy, id| proxy.detach_segments(client, id, recording_id, new_start_position),
        )
        .map(|_| ())
    }

    /// Delete the segments [`Self::detach_segments`] detached
    /// (`:1882-1912`).
    ///
    /// # Errors
    ///
    /// Whatever [`Self::offer_and_wait`] found.
    pub fn delete_detached_segments(
        &mut self,
        client: &mut Client,
        recording_id: i64,
    ) -> Result<i64, ArchiveError> {
        self.offer_and_wait(
            client,
            "AeronArchive::deleteDetachedSegments",
            |client, proxy, id| proxy.delete_detached_segments(client, id, recording_id),
        )
    }

    /// Take a recording's **own** segments out from `new_start_position` onward
    /// (`:1914-1946`).
    ///
    /// # Errors
    ///
    /// Whatever [`Self::offer_and_wait`] found.
    pub fn purge_segments(
        &mut self,
        client: &mut Client,
        recording_id: i64,
        new_start_position: i64,
    ) -> Result<i64, ArchiveError> {
        self.offer_and_wait(
            client,
            "AeronArchive::purgeSegments",
            |client, proxy, id| proxy.purge_segments(client, id, recording_id, new_start_position),
        )
    }

    /// Put a detached recording's segments back (`:1948-1978`).
    ///
    /// # Errors
    ///
    /// Whatever [`Self::offer_and_wait`] found.
    pub fn attach_segments(
        &mut self,
        client: &mut Client,
        recording_id: i64,
    ) -> Result<i64, ArchiveError> {
        self.offer_and_wait(
            client,
            "AeronArchive::attachSegments",
            |client, proxy, id| proxy.attach_segments(client, id, recording_id),
        )
    }

    /// Move one recording's segments onto another recording
    /// (`:1980-2008`).
    ///
    /// # Errors
    ///
    /// Whatever [`Self::offer_and_wait`] found.
    pub fn migrate_segments(
        &mut self,
        client: &mut Client,
        src_recording_id: i64,
        dst_recording_id: i64,
    ) -> Result<i64, ArchiveError> {
        self.offer_and_wait(
            client,
            "AeronArchive::migrateSegments",
            |client, proxy, id| {
                proxy.migrate_segments(client, id, src_recording_id, dst_recording_id)
            },
        )
    }

    /// Change the channel a stopped recording claims to have been made on
    /// (`aeron_archive_update_channel`, `:2010-2040`).
    ///
    /// # Errors
    ///
    /// Whatever [`Self::offer_and_wait`] found.
    pub fn update_channel(
        &mut self,
        client: &mut Client,
        recording_id: i64,
        new_channel: &str,
    ) -> Result<(), ArchiveError> {
        self.offer_and_wait(
            client,
            "AeronArchive::updateChannel",
            |client, proxy, id| proxy.update_channel(client, id, recording_id, new_channel),
        )
        .map(|_| ())
    }

    /// Where a recording has got to (`aeron_archive_get_recording_position`,
    /// `:776-806`).
    ///
    /// # Errors
    ///
    /// Whatever [`Self::offer_and_wait`] found.
    pub fn get_recording_position(
        &mut self,
        client: &mut Client,
        recording_id: i64,
    ) -> Result<i64, ArchiveError> {
        self.offer_and_wait(
            client,
            "AeronArchive::getRecordingPosition",
            |client, proxy, id| proxy.get_recording_position(client, id, recording_id),
        )
    }

    /// Where a recording begins (`aeron_archive_get_start_position`, `:808-837`).
    ///
    /// The catalog's start position: for most recordings it is the log's first
    /// position, and it is the one question of the four that a recording in
    /// flight and one that has stopped answer the same way.
    ///
    /// # Errors
    ///
    /// Whatever [`Self::offer_and_wait`] found.
    pub fn get_start_position(
        &mut self,
        client: &mut Client,
        recording_id: i64,
    ) -> Result<i64, ArchiveError> {
        self.offer_and_wait(
            client,
            "AeronArchive::getStartPosition",
            |client, proxy, id| proxy.get_start_position(client, id, recording_id),
        )
    }

    /// Where a recording stopped (`aeron_archive_get_stop_position`, `:840-869`).
    ///
    /// **This is the one to ask about a recording that is not in flight.**
    /// [`Self::get_recording_position`] answers a **live** position, so the
    /// archive's answer for a recording that has stopped is `NULL_POSITION` —
    /// an answer rather than a refusal (`ArchiveConductor.java:1169-1175`).
    /// Where a recording *ended* is the catalog's stop position, which is what
    /// this asks for.
    ///
    /// # Errors
    ///
    /// Whatever [`Self::offer_and_wait`] found.
    pub fn get_stop_position(
        &mut self,
        client: &mut Client,
        recording_id: i64,
    ) -> Result<i64, ArchiveError> {
        self.offer_and_wait(
            client,
            "AeronArchive::getStopPosition",
            |client, proxy, id| proxy.get_stop_position(client, id, recording_id),
        )
    }

    /// The furthest a recording ever reached
    /// (`aeron_archive_get_max_recorded_position`, `:872-901`).
    ///
    /// Not the stop position, though for a recording that has stopped they are
    /// the same number: a recording still in flight has both, and this is the
    /// one that does not move backwards. The proxy's note beside the encoder has
    /// the reference's own reason.
    ///
    /// # Errors
    ///
    /// Whatever [`Self::offer_and_wait`] found.
    pub fn get_max_recorded_position(
        &mut self,
        client: &mut Client,
        recording_id: i64,
    ) -> Result<i64, ArchiveError> {
        self.offer_and_wait(
            client,
            "AeronArchive::getMaxRecordedPosition",
            |client, proxy, id| proxy.get_max_recorded_position(client, id, recording_id),
        )
    }

    // -----------------------------------------------------------------------
    // Replays
    // -----------------------------------------------------------------------
    //
    // `aeron_archive_client.c:1364-1528`. The two are one operation and one
    // convenience: `start_replay` is the request, and `replay` is "and give me a
    // subscription on the channel it is replaying onto", which is what a caller
    // almost always wants and is why it exists separately.

    /// Start replaying a recording onto a channel
    /// (`aeron_archive_start_replay`, `:1364-1379` over `:1297-1360`).
    ///
    /// Answers the **replay's** session id — the id [`Self::stop_replay`] takes,
    /// and not the recording's.
    ///
    /// **One branch is not here.** When the replay channel says
    /// `control-mode=response` the reference goes to
    /// `aeron_archive_start_replay_via_response_channel` (`:2296-2591`, with its
    /// two siblings), a replay that is delivered over a response channel with a
    /// token of its own — a mechanism this client does not have. A caller here
    /// gets the plain branch whatever the channel says, which is a difference a
    /// caller can see: the reference would have answered with the response
    /// channel's replay.
    ///
    /// # Errors
    ///
    /// Whatever [`Self::offer_and_wait`] found.
    pub fn start_replay(
        &mut self,
        client: &mut Client,
        recording_id: i64,
        replay_channel: &str,
        replay_stream_id: i32,
        params: &ReplayParams,
    ) -> Result<i64, ArchiveError> {
        self.offer_and_wait(client, "AeronArchive::startReplay", |client, proxy, id| {
            proxy.replay(
                client,
                id,
                recording_id,
                replay_channel,
                replay_stream_id,
                params,
            )
        })
    }

    /// Start a replay **and subscribe to it**
    /// (`aeron_archive_replay`, `:1512-1528` over `:1381-1510`).
    ///
    /// Answers the replay subscription's registration id. The subscription is on
    /// the replay channel **with the replay's session id on it**, which is the
    /// whole of what makes it a subscription to *this* replay rather than to
    /// whatever else is on that channel.
    ///
    /// # Errors
    ///
    /// Whatever [`Self::start_replay`] found, [`UriError`] for a channel that is
    /// not one, and [`ArchiveError::Client`] when the client could not take the
    /// subscription.
    pub fn replay(
        &mut self,
        client: &mut Client,
        recording_id: i64,
        replay_channel: &str,
        replay_stream_id: i32,
        params: &ReplayParams,
    ) -> Result<i64, ArchiveError> {
        let replay_session_id = self.start_replay(
            client,
            recording_id,
            replay_channel,
            replay_stream_id,
            params,
        )?;

        // **The cast is the point, not a loss.** A replay's session id is a
        // composition — `(replayId << 32) | replayPublication.sessionId()`
        // (`ArchiveConductor.java:956`) — so its **low 32 bits are the session
        // the replay's publication was given**, which is what a subscription has
        // to name to receive it. The reference truncates for exactly that reason
        // and in both languages: `(int32_t)replay_session_id` there
        // (`:1476`) and `(int) pollForResponse(...)` here
        // (`AeronArchive.java:890-891`).
        let session_id = replay_session_id as i32;
        let replay_channel_with_session =
            channel_with_session_id(replay_channel, session_id).map_err(ArchiveError::Channel)?;

        client
            .add_subscription(
                &replay_channel_with_session,
                replay_stream_id,
                self.operation_timeout(),
            )
            .map_err(ArchiveError::Client)
    }

    // -----------------------------------------------------------------------
    // A publication the archive is asked to record
    // -----------------------------------------------------------------------
    //
    // `aeron_archive_client.c:582-737`. These two are the reason
    // [`channel_with_session_id`] is public: a publication's channel does not
    // name the session the driver gave it, so "record this publication" can only
    // be said by naming both.

    /// Add a publication **and ask the archive to record it**
    /// (`aeron_archive_add_recorded_publication`, `:582-661`).
    ///
    /// Answers the publication's registration id. What makes it *recorded* is
    /// that the channel handed to [`Self::start_recording`] is this one with the
    /// publication's own session id on it — see [`channel_with_session_id`].
    ///
    /// **One check of the reference's is not here.** A publication the driver
    /// already had comes back from the reference with a
    /// `original_registration_id` that differs from its `registration_id`, and
    /// that is a call it refuses; our driver's `ON_PUBLICATION_READY`
    /// (`crates/cnc/src/command.rs:842-860`) carries the second and not the
    /// first, so there is nothing to compare and a caller adding the same channel
    /// twice gets a second publication rather than an error.
    ///
    /// # Errors
    ///
    /// [`ArchiveError::Client`] when the client could not add it, [`UriError`]
    /// for a channel that is not one, and whatever [`Self::start_recording`]
    /// found.
    pub fn add_recorded_publication(
        &mut self,
        client: &mut Client,
        channel: &str,
        stream_id: i32,
    ) -> Result<i64, ArchiveError> {
        let registration_id = client
            .add_publication(channel, stream_id, self.operation_timeout())
            .map_err(ArchiveError::Client)?;

        self.record_the_publication(client, registration_id, channel, stream_id)?;

        Ok(registration_id)
    }

    /// The same, for a publication only this client writes to
    /// (`aeron_archive_add_recorded_exclusive_publication`, `:663-737`).
    ///
    /// The reference has no `original_registration_id` check on this one — an
    /// exclusive publication is never one the driver already had — so the one
    /// thing missing from its sibling is not missing from this.
    ///
    /// # Errors
    ///
    /// The same as [`Self::add_recorded_publication`].
    pub fn add_recorded_exclusive_publication(
        &mut self,
        client: &mut Client,
        channel: &str,
        stream_id: i32,
    ) -> Result<i64, ArchiveError> {
        let registration_id = client
            .add_exclusive_publication(channel, stream_id, self.operation_timeout())
            .map_err(ArchiveError::Client)?;

        self.record_the_publication(client, registration_id, channel, stream_id)?;

        Ok(registration_id)
    }

    /// The half the two above share: read the session off the publication that
    /// was just made and ask the archive to record **that** session.
    fn record_the_publication(
        &mut self,
        client: &mut Client,
        registration_id: i64,
        channel: &str,
        stream_id: i32,
    ) -> Result<(), ArchiveError> {
        let session_id = client
            .publication(registration_id)
            .ok_or(ArchiveError::Client(CommandError::Encoding))?
            .session_id();

        let recording_channel =
            channel_with_session_id(channel, session_id).map_err(ArchiveError::Channel)?;

        // The reference answers the subscription id with `NULL` here — it is the
        // caller's publication that is the answer, not this — so it is dropped.
        self.start_recording(client, &recording_channel, stream_id, true, false)
            .map(|_| ())
    }

    /// Draw a correlation id, offer a request, and wait for its answer.
    ///
    /// Every operation in this client is these five lines, which is why they are
    /// one function: the four waits above are where the differences live.
    fn offer_and_wait(
        &mut self,
        client: &mut Client,
        operation_name: &str,
        offer: impl FnOnce(&Client, &mut ArchiveProxy, i64) -> Result<(), ProxyError>,
    ) -> Result<i64, ArchiveError> {
        let correlation_id = self.next_correlation_id(client)?;

        // The offer borrows the client and the proxy only for as long as it
        // takes to publish; the wait below wants the client mutably, so the two
        // borrows have to be in that order rather than held together.
        offer(client, &mut self.proxy, correlation_id).map_err(ArchiveError::Request)?;

        self.wait_for_response(client, operation_name, correlation_id)
    }

    /// [`Self::offer_and_wait`] for a caller that **expects** one refusal.
    ///
    /// The `try_stop_*` family is the whole of the demand: "stop it" against
    /// something already stopped has done what it was asked to.
    fn offer_and_wait_allowing_error(
        &mut self,
        client: &mut Client,
        operation_name: &str,
        expected_error_code: i64,
        offer: impl FnOnce(&Client, &mut ArchiveProxy, i64) -> Result<(), ProxyError>,
    ) -> Result<bool, ArchiveError> {
        let correlation_id = self.next_correlation_id(client)?;

        offer(client, &mut self.proxy, correlation_id).map_err(ArchiveError::Request)?;

        self.wait_for_response_allowing_error(
            client,
            operation_name,
            correlation_id,
            expected_error_code,
        )
    }

    // -----------------------------------------------------------------------
    // What the two handlers are told
    // -----------------------------------------------------------------------

    /// Hand a recording signal to the handler, if there is one (`:74-87`).
    fn dispatch_recording_signal(&self) {
        let Some(handler) = self.handlers.recording_signal else {
            return;
        };

        handler(&RecordingSignal {
            control_session_id: self.control_response_poller.control_session_id(),
            recording_id: self.control_response_poller.recording_id(),
            subscription_id: self.control_response_poller.subscription_id(),
            position: self.control_response_poller.position(),
            signal: self
                .control_response_poller
                .recording_signal()
                .unwrap_or(deepmsg_codec::archive::recording_signal::RecordingSignal::NullVal),
        });
    }

    /// Hand a refusal for somebody else's request to the handler, if there is
    /// one (`:67-72`).
    fn handle_error_with_handler(&self, client: &mut Client) {
        if self.handlers.error.is_none() {
            client.poll();

            return;
        }

        self.invoke_error_handler(
            ERROR_CODE_GENERIC,
            &format!(
                "response for correlationId={}, errorCode={}, error: {}",
                self.control_response_poller.correlation_id(),
                self.control_response_poller.relevant_id(),
                String::from_utf8_lossy(self.control_response_poller.error_message())
            ),
        );

        client.poll();
    }

    /// The reference's `aeron_archive_context_invoke_error_handler`
    /// (`aeron_archive_context.c:472-494`), including its text.
    fn invoke_error_handler(&self, error_code: i32, message: &str) {
        if let Some(handler) = self.handlers.error {
            handler(error_code, message);
        }
    }

    /// This archive's refusal, as [`ArchiveError::Refused`].
    fn refused(&self, correlation_id: i64) -> ArchiveError {
        ArchiveError::Refused {
            correlation_id,
            archive_error_code: self.control_response_poller.relevant_id(),
            message: String::from_utf8_lossy(self.control_response_poller.error_message())
                .into_owned(),
        }
    }

    /// Whether the answers' subscription has an image.
    ///
    /// `aeron_subscription_is_connected`, which asks whether anything is **on**
    /// the subscription rather than whether its channel is up.
    fn subscription_is_connected(&self, client: &Client) -> bool {
        client
            .subscription(self.subscription)
            .is_some_and(|subscription| !subscription.images().is_empty())
    }

    /// How long one operation may take (`ctx->message_timeout_ns`).
    fn operation_timeout(&self) -> Duration {
        Duration::from_nanos(u64::try_from(self.context.message_timeout_ns).unwrap_or(0))
    }
}

/// The reference's `aeron_archive_idle`: one turn of the context's idle
/// strategy.
///
/// The context has none yet, so this yields — which is what the default backoff
/// does first, and what keeps a wait from being a busy loop on a core. The
/// waits above are the **synchronous** face: the caller owns its thread, and
/// sleeping on it is what the reference's `sleeping` strategy would do too.
fn idle() {
    std::thread::yield_now();
}

/// A signal handler for an archive that has none.
///
/// The descriptor poller dispatches unconditionally, so it needs *a* function
/// where a caller gave no handler — the reference has the same shape in the
/// other direction, where `aeron_archive_recording_signal_dispatch_signal`
/// checks for null and this cannot.
fn ignore_signal(_signal: &crate::client::archive::RecordingSignal) {}

/// The poller's failure, as this layer's.
fn map_poller_error(error: ControlResponseError) -> ArchiveError {
    match error {
        ControlResponseError::MalformedMessage => ArchiveError::Malformed,
    }
}

/// An answer's code as a number, for the one it does not name.
fn code_value(
    code: Option<deepmsg_codec::archive::control_response_code::ControlResponseCode>,
) -> i32 {
    code.map_or(i32::MIN, i32::from)
}
