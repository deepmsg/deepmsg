//! One client's control session, from the connect it opens with to the answer
//! it gets.
//!
//! The reference's `ControlSession` is a `Session` — an `Agent` the conductor
//! drives (`ArchiveConductor.java:395`, `SessionWorker.java:56-81`) — and it is
//! **always driven on the conductor's thread**, which it asserts
//! (`ControlSession.java:834-841`). Everything here assumes the same caller.
//!
//! # The state machine
//!
//! Eight states, in the reference's order and with no numbers attached
//! (`ControlSession.java:64-67`):
//!
//! ```text
//! INIT ──publication ready──► CONNECTING ──isConnected──► CONNECTED
//!                                                             │
//!                        ┌────────── authenticator ───────────┤
//!                        ▼                                    ▼
//!                   CHALLENGED                          AUTHENTICATED
//!                        │                                    │
//!                        └──────── authenticate ─────────────►│
//!                                                             ▼
//!                                                          ACTIVE
//!                   (reject) ──► REJECTED ──► DONE      DONE is terminal
//! ```
//!
//! `CONNECTED` is a **transport** fact, not an application one: it is
//! `controlPublication.isConnected()`, which says a subscriber has attached to
//! the response publication and nothing about the client having heard anything
//! (`ControlSession.java:917`).
//!
//! # The deadline
//!
//! There is one wall-clock guard and it is easy to misread:
//! [`ControlSession::activity_deadline_ms`] is **not** an idle timeout. It is
//! armed only while a response is *owed* and unsent — `updateActivityDeadline`
//! does nothing unless the deadline is currently clear
//! (`ControlSession.java:1062-1068`) — and cleared by every successful send
//! (`:729`, `:975`, `:994`, `:1017`, `:1028`). What it bounds is therefore "how
//! long a pending response may sit", not "how long a quiet client may live".
//!
//! # Two shapes that are not the reference's
//!
//! Both are forced by Rust having no `this` to hand out, and neither changes
//! behaviour:
//!
//! * **The authenticator answers with a value** ([`Answer`]) where the
//!   reference hands it a `SessionProxy` callback into the session
//!   (`ControlSessionProxy.java:22-95`). Two `&mut` to one session is not a
//!   thing this language has; the three things the callback can do are the
//!   three variants.
//! * **The queues hold the response, not a closure.** Java queues a
//!   `BooleanSupplier` that encodes on the way out
//!   (`ControlSession.java:721-726`); here the [`Response`] is what is queued
//!   and the [`Egress`] encodes it when the turn comes. The ordering and the
//!   one-attempt-per-turn policy are the reference's.

use std::fmt;
use std::time::Duration;

use deepmsg_client::client::CommandError;
use deepmsg_codec::archive::control_response_code::ControlResponseCode;
use deepmsg_core::logbuffer::append::Appended;

/// Where a session is (`ControlSession.java:64-67`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    /// Waiting for the response publication the driver was asked for.
    Init,
    /// The publication exists; waiting for it to have a subscriber.
    Connecting,
    /// Connected; the authenticator is being offered the session.
    Connected,
    /// A challenge was sent and its answer is awaited.
    Challenged,
    /// Authenticated; the client is still being told so.
    Authenticated,
    /// The session is usable.
    Active,
    /// Authentication refused; the client is being told so.
    Rejected,
    /// Over, for one of the reasons in [`ControlSession::abort_reason`].
    Done,
}

impl State {
    /// The reference's spelling, which is what its log and abort messages
    /// carry.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Init => "INIT",
            Self::Connecting => "CONNECTING",
            Self::Connected => "CONNECTED",
            Self::Challenged => "CHALLENGED",
            Self::Authenticated => "AUTHENTICATED",
            Self::Active => "ACTIVE",
            Self::Rejected => "REJECTED",
            Self::Done => "DONE",
        }
    }
}

/// A session's reason for being done, in the reference's words
/// (`ControlSession.java:54-56`, `:217-221`, `:1005-1009`).
pub const SESSION_CLOSED_MSG: &str = "session closed";
/// See [`SESSION_CLOSED_MSG`].
pub const RESPONSE_NOT_CONNECTED_MSG: &str = "control response publication is not connected";
/// See [`SESSION_CLOSED_MSG`]. Not raised here yet: nothing in this slice can
/// be the image that went away — the conductor is what knows about images.
pub const REQUEST_IMAGE_NOT_AVAILABLE_MSG: &str = "control request publication image unavailable";
/// See [`SESSION_CLOSED_MSG`], and this one is raised: the client no longer
/// holds the response publication (`ControlResponseProxy.java:252`).
pub const RESPONSE_PUBLICATION_CLOSED_MSG: &str = "control response publication is closed";
/// See [`SESSION_CLOSED_MSG`] (`ControlResponseProxy.java:258`).
pub const RESPONSE_PUBLICATION_MAX_POSITION_MSG: &str =
    "control response publication is at max position";

/// How often `CONNECTED`, `AUTHENTICATED` and `REJECTED` repeat themselves
/// (`ControlSession.java:57`).
const RESEND_INTERVAL_MS: i64 = 200;

/// What a session owes the client, as the queues carry it.
///
/// The variants are the reference's three egress paths for this slice —
/// `sendResponse` (`ControlResponseProxy.java:110-144`), `sendChallenge`
/// (`:146-159`) and `sendPing` (`:201-222`). Signals and descriptors belong to
/// the sessions that send them and arrive with those.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Response {
    /// A `ControlResponse`: the OK, the ERROR and the two "unknown" answers are
    /// this one message with different codes (`ControlSession.java:682-711`).
    Control {
        /// The session this answers on, which the reference passes on every
        /// call (`ControlResponseProxy.java:110-117`).
        control_session_id: i64,
        /// The request's correlation id.
        correlation_id: i64,
        /// The id the answer is *about* — a recording, a session, or 0.
        relevant_id: i64,
        /// `OK`, `ERROR`, `RECORDING_UNKNOWN` or `SUBSCRIPTION_UNKNOWN`.
        code: ControlResponseCode,
        /// The message an ERROR carries; the reference sends `null` otherwise.
        message: Option<String>,
    },
    /// A `Challenge` (`ControlResponseProxy.java:146-159`).
    Challenge {
        /// The session being challenged.
        control_session_id: i64,
        /// The correlation id the challenge answers: the session's own, which
        /// is the connect's until a challenge answer replaces it
        /// (`ControlSessionProxy.java:53-57`, `ControlSession.java:312`).
        correlation_id: i64,
        /// The opaque bytes the challenge carries.
        encoded_challenge: Vec<u8>,
    },
    /// A `Ping`, which a liveness check sends and nothing logs
    /// (`ControlResponseProxy.java:201-222`).
    Ping {
        /// The session being kept alive.
        control_session_id: i64,
    },
}

/// The client's publication side, as a session's egress uses it.
///
/// Every method is one call the reference's `ControlResponseProxy` makes on an
/// `Aeron` client or on the `Publication` it was handed
/// (`ControlResponseProxy.java:34-273`), named by the registration id the
/// `ADD_*` drew instead of by an object reference. That id is the whole of what
/// can cross a turn boundary here: the publication itself lives inside the
/// client, which is the caller's, so it is lent to the egress rather than held
/// by it.
pub trait Publications {
    /// `aeron.asyncAddExclusivePublication(channel, streamId)`, which answers
    /// with a handle rather than waiting (`ControlSession.java:863-865`).
    ///
    /// `timeout` is the one thing the reference has no equivalent of: its
    /// asynchronous add never blocks, and this one waits for the command to
    /// reach the driver before answering.
    fn async_add_exclusive_publication(
        &mut self,
        channel: &str,
        stream_id: i32,
        timeout: Duration,
    ) -> Result<i64, CommandError>;

    /// `aeron.getExclusivePublication(registrationId)`: whether the driver has
    /// answered yet, draining the client on the way as Java's does.
    ///
    /// `false` is Java's `RESOURCE_TEMPORARILY_UNAVAILABLE`, and the reference
    /// treats it by **forgetting the registration id and asking again**
    /// (`ControlSession.java:874-881`) — so a slow driver is answered with
    /// several publications, only the first of which the later ones can be
    /// refused for.
    fn poll_exclusive_publication(&mut self, registration_id: i64) -> bool;

    /// `controlPublication.isConnected()` (`ControlSession.java:917`, `:1005`).
    fn is_exclusive_connected(&self, registration_id: i64) -> bool;

    /// `controlPublication.maxPayloadLength()` (`ControlSession.java:812-815`),
    /// which the descriptor sends are bounded by.
    fn max_payload_length(&self, registration_id: i64) -> usize;

    /// One `offer`. `None` is Java's `CLOSED` — there is no such publication
    /// any more — and everything else is [`Appended`]'s own answer, which the
    /// proxy is what turns into a position, a retry or an abort.
    fn offer_exclusive(&mut self, registration_id: i64, payload: &[u8]) -> Option<Appended>;

    /// `revokeOnClose()` and close, which is what a session's close does first
    /// (`ControlSession.java:163-174`).
    fn release_exclusive(&mut self, registration_id: i64, timeout: Duration);
}

/// What became of one response, as the reference's `checkResult` reads it
/// (`ControlResponseProxy.java:242-262`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Offered {
    /// It is out. Java's `position > 0`.
    Sent,
    /// Nothing went out and nothing is wrong: the window is used up, or the log
    /// is turning over. Java's `BACK_PRESSURED` and `ADMIN_ACTION`, which its
    /// proxy retries and the next turn retries again.
    Retry,
    /// The session is over, in the reference's words: the publication is not
    /// connected, is closed, or is at its maximum position. Java's `checkResult`
    /// ends the session *and* raises an `ArchiveEvent`; the message here is that
    /// event, and ending the session is the session's to do — the egress does
    /// not own it.
    Fatal(ResponseError),
}

/// Why a session's own egress is ending it (`ControlResponseProxy.java:246`,
/// `:252`, `:258`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResponseError(String);

impl ResponseError {
    /// A reason, in the reference's words.
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }

    /// The reason as the reference words it.
    pub fn message(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ResponseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ResponseError {}

/// What a session needs of the publication it answers on.
///
/// `ControlResponseProxy` writes to it directly with no queue of its own
/// (`ControlResponseProxy.java:34-273`), so this is the whole of the session's
/// egress — and the publications it writes through are lent per call, for the
/// reason [`Publications`] gives.
pub trait Egress {
    /// `aeron.asyncAddExclusivePublication(channel, streamId)`: the registration
    /// id, at once (`ControlSession.java:863-865`). Java lets a refusal climb
    /// out of `doWork`; here it ends the session that asked, with the client's
    /// own words.
    ///
    /// Nothing comes back but success: which publication this is belongs to the
    /// egress, and the reference's session, which keeps the registration id,
    /// has no use for it either.
    fn add_publication<P: Publications>(
        &mut self,
        publications: &mut P,
        channel: &str,
        stream_id: i32,
    ) -> Result<(), ResponseError>;

    /// `aeron.getExclusivePublication(registrationId)`: whether the driver has
    /// answered yet, draining the client on the way as Java's does.
    ///
    /// `false` is Java's `RESOURCE_TEMPORARILY_UNAVAILABLE`, and the reference
    /// treats it by **forgetting the registration id and asking again**
    /// (`ControlSession.java:874-881`) — so a slow driver is answered with
    /// several publications, only the first of which the later ones can be
    /// refused for.
    fn is_publication_ready<P: Publications>(&mut self, publications: &mut P) -> bool;

    /// `controlPublication.isConnected()` (`ControlSession.java:917`, `:1005`).
    fn is_connected<P: Publications>(&self, publications: &P) -> bool;

    /// `controlPublication.maxPayloadLength()` (`ControlSession.java:812-815`),
    /// which the descriptor sends are bounded by.
    fn max_payload_length<P: Publications>(&self, publications: &P) -> usize;

    /// One `offer`, and what the reference's `checkResult` makes of it.
    fn offer<P: Publications>(&mut self, publications: &mut P, response: &Response) -> Offered;

    /// `revokeOnClose()` and close, which is what a session's close does first
    /// (`ControlSession.java:163-174`).
    fn close_publication<P: Publications>(&mut self, publications: &mut P);
}

/// What an authenticator decided, as the reference's `SessionProxy` would have
/// carried it out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Answer {
    /// Nothing to do — the reference's callback simply returning.
    None,
    /// `sessionProxy.challenge(bytes)`: send a challenge and move to
    /// `CHALLENGED` (`ControlSessionProxy.java:51-64`).
    Challenge(Vec<u8>),
    /// `sessionProxy.authenticate(principal)`: send OK and move to
    /// `AUTHENTICATED` (`:70-85`).
    Authenticate(Vec<u8>),
    /// `sessionProxy.reject()`: move to `REJECTED` (`:91-94`).
    Reject,
}

/// The archive's authenticator, as far as a session sees it
/// (`Authenticator.java:26-69`).
pub trait Authenticator {
    /// A connect request arrived (`Authenticator.java:36`).
    fn on_connect_request(&mut self, session_id: i64, encoded_credentials: &[u8], now_ms: i64);

    /// The publication is connected: offer the session
    /// (`ControlSession.java:945`).
    fn on_connected_session(&mut self, session_id: i64, now_ms: i64) -> Answer;

    /// Still challenged; the reference calls this every turn
    /// (`ControlSession.java:954-958`).
    fn on_challenged_session(&mut self, session_id: i64, now_ms: i64) -> Answer;

    /// The client answered the challenge
    /// (`ControlSession.java:313`).
    fn on_challenge_response(
        &mut self,
        session_id: i64,
        encoded_credentials: &[u8],
        now_ms: i64,
    ) -> Answer;
}

/// One control session.
pub struct ControlSession<E: Egress, A: Authenticator> {
    /// The id the client is answered with, which the conductor allocates.
    session_id: i64,
    /// The connect request's correlation id, which the connect answer echoes.
    connect_correlation_id: i64,
    /// The channel the response publication is made on. The conductor derives
    /// it from the client's own channel (`ArchiveConductor.java:458-481`).
    response_channel: String,
    /// ...and its stream id, from the client's request.
    response_stream_id: i32,
    /// Set when the client's protocol major is not this archive's
    /// (`ArchiveConductor.java:483-488`): the connect answer is an ERROR
    /// instead, and the session never activates (`:933-942`, `:1070-1076`).
    invalid_version_message: Option<String>,
    /// How long a pending response may sit (`Archive.java:502`).
    connect_timeout_ms: i64,
    /// How often an active session is pinged (`Archive.java:520`).
    liveness_check_interval_ms: i64,

    state: State,
    egress: E,
    authenticator: A,

    /// `NULL_VALUE` in the reference; the deadline is the one place its
    /// sentinel is load-bearing, so it is an `Option` here.
    activity_deadline_ms: Option<i64>,
    /// Until this, the session says nothing (`ControlSession.java:89`, `:930`).
    resend_deadline_ms: i64,
    /// When the next liveness ping is due (`:129`, `:983-986`).
    liveness_check_deadline_ms: i64,
    /// What the authenticator vouched for, once it has
    /// (`ControlSession.java:824`). The adapter checks it against the
    /// authorisation service on every request
    /// (`ControlSessionAdapter.java:1206-1216`).
    encoded_principal: Option<Vec<u8>>,
    /// Why the session is done, once it is.
    abort_reason: Option<String>,

    /// Staged responses, sent in order, one attempt per turn
    /// (`ControlSession.java:79-80`, `:1001-1039`).
    ///
    /// The reference keeps a *sync* and an *async* queue and drains the sync
    /// one first because the async one is filled from another thread (a
    /// replay's own). This build's conductor is one thread, and the second
    /// queue arrives with the sessions that fill it from elsewhere — the field
    /// is one list until then, and what the ordering protects is that a
    /// response queued before another goes out before it.
    sync_responses: Vec<Response>,
}

impl<E: Egress, A: Authenticator> ControlSession<E, A> {
    /// A session in `INIT`, with the deadlines the reference starts them at
    /// (`ControlSession.java:128-129`).
    #[allow(clippy::too_many_arguments)] // one per thing the conductor decided
    pub fn new(
        session_id: i64,
        connect_correlation_id: i64,
        response_channel: String,
        response_stream_id: i32,
        invalid_version_message: Option<String>,
        connect_timeout_ms: i64,
        liveness_check_interval_ms: i64,
        now_ms: i64,
        egress: E,
        authenticator: A,
    ) -> Self {
        Self {
            session_id,
            connect_correlation_id,
            response_channel,
            response_stream_id,
            invalid_version_message,
            connect_timeout_ms,
            liveness_check_interval_ms,
            state: State::Init,
            egress,
            authenticator,
            activity_deadline_ms: Some(now_ms + connect_timeout_ms),
            resend_deadline_ms: 0,
            liveness_check_deadline_ms: now_ms + liveness_check_interval_ms,
            encoded_principal: None,
            abort_reason: None,
            sync_responses: Vec::new(),
        }
    }

    /// The id this session is known by.
    pub const fn session_id(&self) -> i64 {
        self.session_id
    }

    /// Where it is.
    pub const fn state(&self) -> State {
        self.state
    }

    /// Whether the conductor may forget it (`ControlSession.java:203-206`).
    pub const fn is_done(&self) -> bool {
        matches!(self.state, State::Done)
    }

    /// What the authenticator vouched for, if it has.
    pub fn encoded_principal(&self) -> Option<&[u8]> {
        self.encoded_principal.as_deref()
    }

    /// Why it is done, if it is.
    pub fn abort_reason(&self) -> Option<&str> {
        self.abort_reason.as_deref()
    }

    /// When a pending response must be out by, or [`None`] for "nothing is
    /// owed" (`ControlSession.java:1057-1068`).
    pub const fn activity_deadline_ms(&self) -> Option<i64> {
        self.activity_deadline_ms
    }

    /// End the session, with the reference's rule for what may overwrite what
    /// (`ControlSession.java:146-151`): a second abort reason is taken unless
    /// the session is already done and this is a plain close.
    pub fn abort(&mut self, reason: &str) {
        if self.state != State::Done
            || (self.abort_reason.is_some() && reason == SESSION_CLOSED_MSG)
        {
            self.abort_reason = Some(reason.to_owned());
            self.state = State::Done;
        }
    }

    /// One turn (`ControlSession.java:212-271`).
    ///
    /// Returns the work done, for the cycle counter.
    pub fn do_work<P: Publications>(&mut self, now_ms: i64, publications: &mut P) -> usize {
        let mut work = 0;

        if self.has_no_activity(now_ms) {
            // The two messages differ by state, and the ACTIVE one does not say
            // what the number is — that is the reference's text
            // (`ControlSession.java:217-221`).
            let reason = if self.state == State::Active {
                format!(
                    "failed to send response for more than connectTimeoutMs={}",
                    self.connect_timeout_ms
                )
            } else {
                format!(
                    "failed to establish initial connection: state={}",
                    self.state.name()
                )
            };
            self.abort(&reason);
            work += 1;
        }

        work += match self.state {
            State::Init => self.init(publications),
            State::Connecting => self.wait_for_connection(publications),
            State::Connected => self.send_connect_response(now_ms, publications),
            State::Challenged => self.wait_for_challenge_response(now_ms, publications),
            State::Authenticated => self.wait_for_request(now_ms, publications),
            State::Active => {
                self.perform_liveness_check(now_ms, publications)
                    + self.send_responses(publications)
            }
            State::Rejected => self.send_reject(now_ms, publications),
            State::Done => 0,
        };

        work
    }

    /// The connect answer's second half: a session becomes usable when the
    /// first authenticated request arrives (`ControlSession.java:1070-1076`).
    pub fn attempt_to_activate(&mut self) {
        if self.state == State::Authenticated && self.invalid_version_message.is_none() {
            self.state = State::Active;
        }
    }

    /// The client answered a challenge (`ControlSession.java:308-315`), which
    /// is the one request that is not gated on being `ACTIVE`.
    pub fn on_challenge_response<P: Publications>(
        &mut self,
        correlation_id: i64,
        encoded_credentials: &[u8],
        now_ms: i64,
        publications: &mut P,
    ) {
        if self.state != State::Challenged {
            return;
        }

        self.connect_correlation_id = correlation_id;
        let answer =
            self.authenticator
                .on_challenge_response(self.session_id, encoded_credentials, now_ms);
        self.apply(answer, publications);
    }

    /// Queue an OK (`ControlSession.java:682-690`).
    pub fn send_ok_response<P: Publications>(
        &mut self,
        correlation_id: i64,
        relevant_id: i64,
        now_ms: i64,
        publications: &mut P,
    ) {
        self.send_response(
            correlation_id,
            relevant_id,
            ControlResponseCode::OK,
            None,
            now_ms,
            publications,
        );
    }

    /// Queue an ERROR (`ControlSession.java:692-701`).
    ///
    /// The reference has two of these: one that names no relevant id and sends
    /// its `GENERIC` — zero (`client/ArchiveException.java:29`) — and one that
    /// carries whatever the caller says the error is about
    /// (`:697-701`). A denial of an unauthorised action is the second kind and
    /// says `UNAUTHORISED_ACTION` (`ControlSessionAdapter.java:1209-1211`), so
    /// the id is a parameter here rather than a zero written in.
    pub fn send_error_response<P: Publications>(
        &mut self,
        correlation_id: i64,
        relevant_id: i64,
        message: &str,
        now_ms: i64,
        publications: &mut P,
    ) {
        self.send_response(
            correlation_id,
            relevant_id,
            ControlResponseCode::ERROR,
            Some(message.to_owned()),
            now_ms,
            publications,
        );
    }

    /// Give the publication back (`ControlSession.java:163-174`). Closing the
    /// session's counter and telling the adapter belong to the conductor and
    /// the counters.
    pub fn close<P: Publications>(&mut self, publications: &mut P) {
        self.egress.close_publication(publications);
        self.sync_responses.clear();
    }

    /// `INIT`: ask for the response publication, and take it up when the driver
    /// answers (`ControlSession.java:855-911`).
    ///
    /// The session's own counter is allocated and bound to this publication by
    /// the conductor (`ArchiveConductor.java:493-498`), and belongs to the
    /// counters module.
    fn init<P: Publications>(&mut self, publications: &mut P) -> usize {
        if !self.egress.is_publication_ready(publications) {
            // The reference forgets the registration id here and asks again
            // next turn (`:874-881`), which is what makes a slow driver answer
            // one request with several publications.
            if let Err(error) = self.egress.add_publication(
                publications,
                &self.response_channel,
                self.response_stream_id,
            ) {
                // Java's exception out of `asyncAddExclusivePublication` climbs
                // out of `doWork` and takes the process with it; a session that
                // can be ended without the archive is ended instead, with the
                // client's own words as its reason.
                self.abort(error.message());
            }

            return 1;
        }

        self.state = State::Connecting;
        self.resend_deadline_ms = 0;
        1
    }

    /// `CONNECTING`: the publication has a subscriber (`ControlSession.java:913-924`).
    fn wait_for_connection<P: Publications>(&mut self, publications: &mut P) -> usize {
        if self.egress.is_connected(publications) {
            self.state = State::Connected;
            return 1;
        }

        0
    }

    /// `CONNECTED`: tell the authenticator, or refuse the client's version
    /// (`ControlSession.java:926-952`).
    fn send_connect_response<P: Publications>(
        &mut self,
        now_ms: i64,
        publications: &mut P,
    ) -> usize {
        if now_ms <= self.resend_deadline_ms {
            return 0;
        }

        self.resend_deadline_ms = now_ms + RESEND_INTERVAL_MS;

        let answer = match &self.invalid_version_message {
            Some(message) => {
                let message = message.clone();
                self.send_response(
                    self.connect_correlation_id,
                    self.session_id,
                    ControlResponseCode::ERROR,
                    Some(message),
                    now_ms,
                    publications,
                );
                Answer::None
            }
            None => self
                .authenticator
                .on_connected_session(self.session_id, now_ms),
        };
        self.apply(answer, publications);

        1
    }

    /// `CHALLENGED`: the reference asks again every turn, with no resend
    /// interval (`ControlSession.java:954-958`).
    fn wait_for_challenge_response<P: Publications>(
        &mut self,
        now_ms: i64,
        publications: &mut P,
    ) -> usize {
        let answer = self
            .authenticator
            .on_challenged_session(self.session_id, now_ms);
        self.apply(answer, publications);
        1
    }

    /// `AUTHENTICATED`: keep saying so until a request arrives
    /// (`ControlSession.java:960-981`).
    fn wait_for_request<P: Publications>(&mut self, now_ms: i64, publications: &mut P) -> usize {
        if now_ms <= self.resend_deadline_ms {
            return 0;
        }

        self.resend_deadline_ms = now_ms + RESEND_INTERVAL_MS;
        self.send_response(
            self.connect_correlation_id,
            self.session_id,
            ControlResponseCode::OK,
            None,
            now_ms,
            publications,
        );
        1
    }

    /// `ACTIVE`: the liveness ping (`ControlSession.java:983-999`).
    fn perform_liveness_check<P: Publications>(
        &mut self,
        now_ms: i64,
        publications: &mut P,
    ) -> usize {
        if self.liveness_check_deadline_ms - now_ms >= 0 {
            return 0;
        }

        self.liveness_check_deadline_ms = now_ms + self.liveness_check_interval_ms;

        let ping = Response::Ping {
            control_session_id: self.session_id,
        };
        match self.egress.offer(publications, &ping) {
            Offered::Sent => self.activity_deadline_ms = None,
            Offered::Retry => self.update_activity_deadline(now_ms),
            Offered::Fatal(error) => {
                let reason = error.message().to_owned();
                self.abort(&reason);
            }
        }

        1
    }

    /// `ACTIVE`: drain what is owed (`ControlSession.java:1001-1039`).
    ///
    /// One attempt at the head per turn, and no arming on failure: the
    /// reference arms when a response is *queued* (`:721-726`), and the one
    /// place that re-arms from here is the async queue's failed send
    /// (`:1033`), which arrives with the sessions that fill it from another
    /// thread.
    fn send_responses<P: Publications>(&mut self, publications: &mut P) -> usize {
        if !self.egress.is_connected(publications) {
            self.abort(RESPONSE_NOT_CONNECTED_MSG);
            return 1;
        }

        // The outcome is taken out of the borrow of the queue before anything
        // is done about it: one arm takes the head off, and a queue borrowed
        // for the length of its own `first()` cannot be shortened.
        let Some(owed) = self.sync_responses.first() else {
            return 0;
        };
        let outcome = self.egress.offer(publications, owed);

        match outcome {
            Offered::Sent => {
                self.sync_responses.remove(0);
                self.activity_deadline_ms = None;
                1
            }
            Offered::Retry => 0,
            Offered::Fatal(error) => {
                let reason = error.message().to_owned();
                self.abort(&reason);
                1
            }
        }
    }

    /// `REJECTED`: keep saying that too (`ControlSession.java:1041-1055`).
    fn send_reject<P: Publications>(&mut self, now_ms: i64, publications: &mut P) -> usize {
        if now_ms <= self.resend_deadline_ms {
            return 0;
        }

        self.resend_deadline_ms = now_ms + RESEND_INTERVAL_MS;
        self.send_response(
            self.connect_correlation_id,
            self.session_id,
            ControlResponseCode::ERROR,
            Some(SESSION_REJECTED_MSG.to_owned()),
            now_ms,
            publications,
        );
        1
    }

    /// The one response path (`ControlSession.java:713-731`): send it now, and
    /// if anything is already queued or the send fails, queue it and arm the
    /// deadline instead.
    fn send_response<P: Publications>(
        &mut self,
        correlation_id: i64,
        relevant_id: i64,
        code: ControlResponseCode,
        message: Option<String>,
        now_ms: i64,
        publications: &mut P,
    ) {
        let response = Response::Control {
            control_session_id: self.session_id,
            correlation_id,
            relevant_id,
            code,
            message,
        };

        let sent = if self.sync_responses.is_empty() {
            match self.egress.offer(publications, &response) {
                Offered::Sent => true,
                Offered::Retry => false,
                // The reference's `checkResult` ends the session and raises the
                // event; a response that would not go out is not queued behind
                // the reason the session is over.
                Offered::Fatal(error) => {
                    let reason = error.message().to_owned();
                    self.abort(&reason);
                    return;
                }
            }
        } else {
            false
        };

        if sent {
            self.activity_deadline_ms = None;
        } else {
            self.update_activity_deadline(now_ms);
            self.sync_responses.push(response);
        }
    }

    /// Carry out what an authenticator decided (`ControlSessionProxy.java:51-94`).
    ///
    /// The reference's proxy sends **first** and moves the session only if the
    /// send took — a session whose answer would not go out stays where it is
    /// and is offered again on the next turn. `reject` is the one that moves
    /// unconditionally (`:91-94`).
    fn apply<P: Publications>(&mut self, answer: Answer, publications: &mut P) {
        match answer {
            Answer::None => {}
            Answer::Challenge(encoded_challenge) => {
                let challenge = Response::Challenge {
                    control_session_id: self.session_id,
                    correlation_id: self.connect_correlation_id,
                    encoded_challenge,
                };
                if let Offered::Sent = self.egress.offer(publications, &challenge) {
                    self.state = State::Challenged;
                }
            }
            Answer::Authenticate(encoded_principal) => {
                // `authenticate` answers with OK **directly**, not through the
                // session's queue (`ControlSessionProxy.java:70-78`), and the
                // relevant id is the session's own.
                let ok = Response::Control {
                    control_session_id: self.session_id,
                    correlation_id: self.connect_correlation_id,
                    relevant_id: self.session_id,
                    code: ControlResponseCode::OK,
                    message: None,
                };
                if let Offered::Sent = self.egress.offer(publications, &ok) {
                    self.encoded_principal = Some(encoded_principal);
                    self.activity_deadline_ms = None;
                    self.state = State::Authenticated;
                }
            }
            Answer::Reject => self.state = State::Rejected,
        }
    }

    /// `hasNoActivity` (`ControlSession.java:1057-1060`).
    fn has_no_activity(&self, now_ms: i64) -> bool {
        self.activity_deadline_ms
            .is_some_and(|deadline| now_ms > deadline)
    }

    /// `updateActivityDeadline` (`ControlSession.java:1062-1068`): **only** from
    /// clear, so a deadline already armed is never pushed out.
    fn update_activity_deadline(&mut self, now_ms: i64) {
        if self.activity_deadline_ms.is_none() {
            self.activity_deadline_ms = Some(now_ms + self.connect_timeout_ms);
        }
    }
}

/// The reference's message for a refused challenge
/// (`ControlSession.java:831`, `:1041-1055`).
const SESSION_REJECTED_MSG: &str = "authentication rejected";

#[cfg(test)]
mod tests {
    use super::*;

    /// An egress that records what it was asked to send, and can be told
    /// whether the publication exists and has a subscriber.
    #[derive(Default)]
    struct FakeEgress {
        publications_added: Vec<(String, i32)>,
        ready: bool,
        connected: bool,
        offered: Vec<Response>,
        /// Refuse the direct offer, the way a full ring would.
        refuse_offers: bool,
        closed: bool,
    }

    impl Egress for FakeEgress {
        fn add_publication<P: Publications>(
            &mut self,
            _publications: &mut P,
            channel: &str,
            stream_id: i32,
        ) -> Result<(), ResponseError> {
            self.publications_added
                .push((channel.to_owned(), stream_id));
            self.ready = true;
            self.connected = true;
            Ok(())
        }

        fn is_publication_ready<P: Publications>(&mut self, _publications: &mut P) -> bool {
            self.ready
        }

        fn is_connected<P: Publications>(&self, _publications: &P) -> bool {
            self.connected
        }

        fn max_payload_length<P: Publications>(&self, _publications: &P) -> usize {
            1024
        }

        fn offer<P: Publications>(
            &mut self,
            _publications: &mut P,
            response: &Response,
        ) -> Offered {
            if self.refuse_offers {
                return Offered::Retry;
            }

            self.offered.push(response.clone());
            Offered::Sent
        }

        fn close_publication<P: Publications>(&mut self, _publications: &mut P) {
            self.closed = true;
        }
    }

    /// The publications a session's egress writes through.
    ///
    /// The fake egress *is* the publication as far as a session can see, so
    /// nothing ever reaches this — which is why every method here is a panic
    /// rather than a plausible answer: a test that got here would be testing
    /// something other than what it meant to.
    struct NoPublications;

    impl Publications for NoPublications {
        fn async_add_exclusive_publication(
            &mut self,
            _channel: &str,
            _stream_id: i32,
            _timeout: Duration,
        ) -> Result<i64, CommandError> {
            panic!("the fake egress is the publication; nothing asks the client")
        }

        fn poll_exclusive_publication(&mut self, _registration_id: i64) -> bool {
            panic!("the fake egress is the publication; nothing asks the client")
        }

        fn is_exclusive_connected(&self, _registration_id: i64) -> bool {
            panic!("the fake egress is the publication; nothing asks the client")
        }

        fn max_payload_length(&self, _registration_id: i64) -> usize {
            panic!("the fake egress is the publication; nothing asks the client")
        }

        fn offer_exclusive(&mut self, _registration_id: i64, _payload: &[u8]) -> Option<Appended> {
            panic!("the fake egress is the publication; nothing asks the client")
        }

        fn release_exclusive(&mut self, _registration_id: i64, _timeout: Duration) {
            panic!("the fake egress is the publication; nothing asks the client")
        }
    }

    #[derive(Default)]
    struct FakeAuthenticator {
        answers: Vec<Answer>,
        connect_requests: usize,
    }

    impl FakeAuthenticator {
        fn with(answers: &[Answer]) -> Self {
            Self {
                answers: answers.to_vec(),
                connect_requests: 0,
            }
        }

        fn next(&mut self) -> Answer {
            if self.answers.is_empty() {
                Answer::None
            } else {
                self.answers.remove(0)
            }
        }
    }

    impl Authenticator for FakeAuthenticator {
        fn on_connect_request(&mut self, _session_id: i64, _credentials: &[u8], _now_ms: i64) {
            self.connect_requests += 1;
        }

        fn on_connected_session(&mut self, _session_id: i64, _now_ms: i64) -> Answer {
            self.next()
        }

        fn on_challenged_session(&mut self, _session_id: i64, _now_ms: i64) -> Answer {
            self.next()
        }

        fn on_challenge_response(
            &mut self,
            _session_id: i64,
            _credentials: &[u8],
            _now_ms: i64,
        ) -> Answer {
            self.next()
        }
    }

    /// A session whose publication is already there, connected and answering.
    fn active_session() -> ControlSession<FakeEgress, FakeAuthenticator> {
        // The publication is there from the first turn, so INIT is one turn.
        let egress = FakeEgress {
            ready: true,
            connected: true,
            ..FakeEgress::default()
        };

        let mut session = ControlSession::new(
            7,
            42,
            "aeron:ipc?term-length=64k".to_owned(),
            20,
            None,
            5_000,
            1_000,
            0,
            egress,
            FakeAuthenticator::with(&[Answer::Authenticate(Vec::new())]),
        );

        // INIT -> CONNECTING
        session.do_work(1, &mut NoPublications);
        // CONNECTING -> CONNECTED
        session.do_work(1, &mut NoPublications);
        // CONNECTED -> AUTHENTICATED, and the OK goes out
        session.do_work(201, &mut NoPublications);
        session.attempt_to_activate();

        assert_eq!(session.state(), State::Active);
        session
    }

    #[test]
    fn a_session_walks_the_states_in_the_references_order() {
        let mut session = ControlSession::new(
            1,
            42,
            "aeron:ipc".to_owned(),
            20,
            None,
            5_000,
            1_000,
            0,
            FakeEgress::default(),
            FakeAuthenticator::with(&[Answer::Authenticate(Vec::new())]),
        );

        assert_eq!(session.state(), State::Init);

        // Nothing is ready yet, so the publication is asked for and INIT is
        // where it stays.
        session.do_work(1, &mut NoPublications);
        assert_eq!(session.state(), State::Init);

        // The driver answers.
        session.egress.ready = true;
        session.do_work(1, &mut NoPublications);
        assert_eq!(session.state(), State::Connecting);

        // ...and a subscriber attaches. That is the whole of CONNECTING's
        // question, so one turn is enough (`ControlSession.java:913-924`).
        session.egress.connected = true;
        session.do_work(1, &mut NoPublications);
        assert_eq!(session.state(), State::Connected);

        // The authenticator is offered the session on the next turn, at the
        // first moment the resend interval allows.
        session.do_work(201, &mut NoPublications);
        assert_eq!(session.state(), State::Authenticated);

        session.attempt_to_activate();
        assert_eq!(session.state(), State::Active);
    }

    #[test]
    fn a_slow_driver_is_asked_again() {
        // `ControlSession.java:874-881`: the registration id is forgotten and
        // another publication asked for. It is why a driver that takes its time
        // gets more than one publication to answer.
        let mut session = ControlSession::new(
            1,
            42,
            "aeron:ipc?term-length=64k".to_owned(),
            20,
            None,
            5_000,
            1_000,
            0,
            FakeEgress::default(),
            FakeAuthenticator::default(),
        );

        session.egress.ready = false;
        session.do_work(1, &mut NoPublications);
        session.egress.ready = false;
        session.do_work(1, &mut NoPublications);

        assert_eq!(session.egress.publications_added.len(), 2);
        assert_eq!(
            session.egress.publications_added[0],
            ("aeron:ipc?term-length=64k".to_owned(), 20)
        );
    }

    #[test]
    fn the_connect_answer_is_resent_every_two_hundred_milliseconds() {
        let mut egress = FakeEgress {
            ready: true,
            connected: true,
            ..FakeEgress::default()
        };
        egress.ready = true;

        let mut session = ControlSession::new(
            1,
            42,
            "aeron:ipc".to_owned(),
            20,
            None,
            5_000,
            1_000,
            0,
            egress,
            FakeAuthenticator::with(&[Answer::Authenticate(Vec::new())]),
        );

        session.do_work(1, &mut NoPublications);
        session.do_work(1, &mut NoPublications);
        session.do_work(1, &mut NoPublications);
        assert_eq!(session.state(), State::Authenticated);

        // The first answer went out with the transition: `authenticate`
        // answers as it moves the session (`ControlSessionProxy.java:70-78`).
        let after_connect = session.egress.offered.len();
        assert_eq!(after_connect, 1);

        // Too soon: the interval runs from the turn that answered, which was
        // `now = 1`.
        session.do_work(200, &mut NoPublications);
        assert_eq!(session.egress.offered.len(), after_connect);

        // The interval has passed.
        session.do_work(202, &mut NoPublications);
        assert_eq!(session.egress.offered.len(), after_connect + 1);

        // And what it sends is an OK carrying the connect's correlation id and
        // the session's own id (`ControlSession.java:964-977`).
        assert_eq!(
            session.egress.offered.last(),
            Some(&Response::Control {
                control_session_id: 1,
                correlation_id: 42,
                relevant_id: 1,
                code: ControlResponseCode::OK,
                message: None,
            })
        );
    }

    #[test]
    fn a_challenge_pauses_until_it_is_answered() {
        let mut egress = FakeEgress {
            ready: true,
            connected: true,
            ..FakeEgress::default()
        };
        egress.ready = true;

        let mut session = ControlSession::new(
            1,
            42,
            "aeron:ipc".to_owned(),
            20,
            None,
            5_000,
            1_000,
            0,
            egress,
            FakeAuthenticator::with(&[
                Answer::Challenge(b"challenge!".to_vec()),
                Answer::Authenticate(Vec::new()),
            ]),
        );

        session.do_work(1, &mut NoPublications);
        session.do_work(1, &mut NoPublications);
        session.do_work(201, &mut NoPublications);

        assert_eq!(session.state(), State::Challenged);
        assert_eq!(
            session.egress.offered.last(),
            Some(&Response::Challenge {
                control_session_id: 1,
                correlation_id: 42,
                encoded_challenge: b"challenge!".to_vec(),
            })
        );

        // Being challenged is not being usable: a request now does nothing.
        session.attempt_to_activate();
        assert_eq!(session.state(), State::Challenged);

        session.on_challenge_response(99, b"answer", 300, &mut NoPublications);
        assert_eq!(session.state(), State::Authenticated);

        session.attempt_to_activate();
        assert_eq!(session.state(), State::Active);
    }

    #[test]
    fn a_refused_client_keeps_being_told_so() {
        let mut egress = FakeEgress {
            ready: true,
            connected: true,
            ..FakeEgress::default()
        };
        egress.ready = true;

        let mut session = ControlSession::new(
            1,
            42,
            "aeron:ipc".to_owned(),
            20,
            None,
            5_000,
            1_000,
            0,
            egress,
            FakeAuthenticator::with(&[Answer::Reject]),
        );

        session.do_work(1, &mut NoPublications);
        session.do_work(1, &mut NoPublications);
        session.do_work(201, &mut NoPublications);
        assert_eq!(session.state(), State::Rejected);

        let sent = session.egress.offered.len();
        session.do_work(402, &mut NoPublications);
        assert_eq!(session.egress.offered.len(), sent + 1);
        assert_eq!(
            session.egress.offered.last(),
            Some(&Response::Control {
                control_session_id: 1,
                correlation_id: 42,
                relevant_id: 1,
                code: ControlResponseCode::ERROR,
                message: Some("authentication rejected".to_owned()),
            })
        );
    }

    #[test]
    fn an_invalid_client_version_is_an_error_instead_of_a_connection() {
        // `ArchiveConductor.java:483-488` computes the message,
        // `ControlSession.java:933-942` sends it, and `:1072` refuses to
        // activate afterwards.
        let mut egress = FakeEgress {
            ready: true,
            connected: true,
            ..FakeEgress::default()
        };
        egress.ready = true;

        let mut session = ControlSession::new(
            1,
            42,
            "aeron:ipc".to_owned(),
            20,
            Some("invalid client version 9.9.9, archive is 1.12.0".to_owned()),
            5_000,
            1_000,
            0,
            egress,
            FakeAuthenticator::with(&[Answer::Authenticate(Vec::new())]),
        );

        session.do_work(1, &mut NoPublications);
        session.do_work(1, &mut NoPublications);
        session.do_work(201, &mut NoPublications);

        assert_eq!(
            session.egress.offered.last(),
            Some(&Response::Control {
                control_session_id: 1,
                correlation_id: 42,
                relevant_id: 1,
                code: ControlResponseCode::ERROR,
                message: Some("invalid client version 9.9.9, archive is 1.12.0".to_owned()),
            }),
            "the authenticator is not consulted at all"
        );
        assert_eq!(
            session.authenticator.connect_requests, 0,
            "and it was never offered a connect request"
        );

        session.attempt_to_activate();
        assert_eq!(session.state(), State::Connected);
    }

    #[test]
    fn a_response_queued_behind_another_is_not_offered_out_of_order() {
        let mut session = active_session();
        // The connect answer is already in the list; what is being watched is
        // what these two turns send.
        let before = session.egress.offered.len();

        // Nothing can go out directly, so both are queued — which is the only
        // way the ordering rule has anything to order
        // (`ControlSession.java:721-726`).
        session.egress.refuse_offers = true;
        session.send_ok_response(10, 0, 1_000, &mut NoPublications);
        session.send_ok_response(11, 0, 1_000, &mut NoPublications);

        assert_eq!(session.sync_responses.len(), 2, "both are owed");

        // A turn drains one, and it is the one that was queued first. The
        // liveness ping shares this list, so the assertion is about the
        // responses.
        session.egress.refuse_offers = false;
        session.do_work(1_001, &mut NoPublications);
        assert_eq!(correlations(&session.egress.offered[before..]), vec![10]);
        assert_eq!(session.sync_responses.len(), 1);

        session.do_work(1_002, &mut NoPublications);
        assert_eq!(
            correlations(&session.egress.offered[before..]),
            vec![10, 11]
        );
        assert!(session.sync_responses.is_empty());
    }

    /// The correlation ids of the control responses that went out, in order.
    fn correlations(offered: &[Response]) -> Vec<i64> {
        offered
            .iter()
            .filter_map(|response| match response {
                Response::Control { correlation_id, .. } => Some(*correlation_id),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn the_activity_deadline_is_armed_by_a_pending_response_and_cleared_by_a_sent_one() {
        let mut session = active_session();
        assert_eq!(session.activity_deadline_ms(), None, "nothing is owed");

        session.egress.refuse_offers = true;
        session.send_ok_response(10, 0, 1_000, &mut NoPublications);

        assert_eq!(
            session.activity_deadline_ms(),
            Some(6_000),
            "armed from the connect timeout"
        );

        // Arming again does not push it out (`ControlSession.java:1064-1067`).
        session.send_ok_response(11, 0, 2_000, &mut NoPublications);
        assert_eq!(session.activity_deadline_ms(), Some(6_000));

        // A send that takes clears it, and the next pending one arms a fresh
        // one.
        session.egress.refuse_offers = false;
        session.do_work(3_000, &mut NoPublications);
        assert_eq!(session.activity_deadline_ms(), None);

        session.egress.refuse_offers = true;
        session.send_ok_response(12, 0, 4_000, &mut NoPublications);
        assert_eq!(session.activity_deadline_ms(), Some(9_000));
    }

    #[test]
    fn a_session_that_cannot_get_its_answer_out_is_aborted_in_the_references_words() {
        let mut session = active_session();
        session.egress.refuse_offers = true;
        session.send_ok_response(10, 0, 1_000, &mut NoPublications);

        // The deadline is 6_000 and the session is ACTIVE, so the message is
        // the ACTIVE one — which does not say what the number is
        // (`ControlSession.java:217-221`).
        session.do_work(6_001, &mut NoPublications);

        assert_eq!(session.state(), State::Done);
        assert_eq!(
            session.abort_reason(),
            Some("failed to send response for more than connectTimeoutMs=5000")
        );
    }

    #[test]
    fn a_session_that_never_connects_is_aborted_with_its_state_named() {
        let mut session = ControlSession::new(
            1,
            42,
            "aeron:ipc".to_owned(),
            20,
            None,
            5_000,
            1_000,
            0,
            FakeEgress::default(),
            FakeAuthenticator::default(),
        );

        session.egress.ready = false;
        session.do_work(1, &mut NoPublications);
        assert_eq!(session.state(), State::Init);

        session.do_work(5_001, &mut NoPublications);

        assert_eq!(session.state(), State::Done);
        assert_eq!(
            session.abort_reason(),
            Some("failed to establish initial connection: state=INIT")
        );
    }

    #[test]
    fn an_active_session_whose_publication_lost_its_subscriber_says_so() {
        let mut session = active_session();

        session.egress.connected = false;
        session.do_work(1_001, &mut NoPublications);

        assert_eq!(session.state(), State::Done);
        assert_eq!(session.abort_reason(), Some(RESPONSE_NOT_CONNECTED_MSG));
    }

    #[test]
    fn an_active_session_pings_on_its_liveness_interval() {
        let mut session = active_session();
        let before = session.egress.offered.len();

        // The interval is a second and the session started at 0; nothing is
        // due yet.
        session.do_work(500, &mut NoPublications);
        assert_eq!(session.egress.offered.len(), before);

        session.do_work(1_001, &mut NoPublications);
        assert_eq!(session.egress.offered.len(), before + 1);
        assert_eq!(
            session.egress.offered.last(),
            Some(&Response::Ping {
                control_session_id: 7
            })
        );

        // Once per interval, not once per turn.
        session.do_work(1_500, &mut NoPublications);
        assert_eq!(session.egress.offered.len(), before + 1);
        session.do_work(2_002, &mut NoPublications);
        assert_eq!(session.egress.offered.len(), before + 2);
    }

    #[test]
    fn a_done_session_does_nothing_more() {
        let mut session = active_session();
        session.abort(SESSION_CLOSED_MSG);
        assert!(session.is_done());

        let before = session.egress.offered.len();
        assert_eq!(session.do_work(1_000, &mut NoPublications), 0);
        assert_eq!(session.egress.offered.len(), before);

        // A second reason is not taken over the first, except for a plain
        // close, which is the reference's rule (`ControlSession.java:146-151`).
        session.abort("something else");
        assert_eq!(session.abort_reason(), Some(SESSION_CLOSED_MSG));
        session.abort(SESSION_CLOSED_MSG);
        assert_eq!(session.abort_reason(), Some(SESSION_CLOSED_MSG));
    }

    #[test]
    fn closing_gives_the_publication_back() {
        let mut session = active_session();

        session.close(&mut NoPublications);

        assert!(session.egress.closed);
        assert!(session.sync_responses.is_empty());
    }
}
