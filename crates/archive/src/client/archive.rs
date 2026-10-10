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
//! This commit has the object, the four waits, and the three requests they are
//! exercised with. The rest of the family — the listings, the replays, the
//! segment operations — is the next one, and every one of them is the same five
//! lines over [`Archive::offer_and_wait`].

use std::time::{Duration, Instant};

use deepmsg_client::client::Client;

use crate::client::async_connect::{AsyncConnect, ConnectError, Connected, Credentials, Polled};
use crate::client::context::ArchiveContext;
use crate::client::poller::{ControlResponseError, ControlResponsePoller};
use crate::client::proxy::{ArchiveProxy, ProxyError};

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

/// `AERON_ERROR_CODE_GENERIC_ERROR`, which is what the error handler is always
/// told (`aeron_archive_context.c:491`).
const ERROR_CODE_GENERIC: i32 = 0;

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
        Self {
            context,
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

    // -----------------------------------------------------------------------
    // The three requests these waits are exercised with
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

    /// Stop every recording on a channel and stream
    /// (`aeron_archive_stop_recording_channel_and_stream`, `:968-999`).
    ///
    /// # Errors
    ///
    /// Whatever [`Self::offer_and_wait`] found.
    pub fn stop_recording(
        &mut self,
        client: &mut Client,
        channel: &str,
        stream_id: i32,
    ) -> Result<(), ArchiveError> {
        self.offer_and_wait(
            client,
            "AeronArchive::stopRecording",
            |client, proxy, id| proxy.stop_recording(client, id, channel, stream_id),
        )?;

        Ok(())
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
