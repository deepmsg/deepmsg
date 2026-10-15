//! The persistent subscription's state machine — **the replay half** of the
//! main path (`aeron_archive_persistent_subscription.c`, 2429 lines).
//!
//! # What a persistent subscription is
//!
//! A recording replayed into a subscription that becomes the live stream when
//! it has caught up, and that heals when either end of it goes away. The
//! application sees none of that: it polls this object, hands it a fragment
//! handler, and reads the recording's messages followed by the live stream's,
//! in order and without a seam.
//!
//! # The path this file walks, and the half it does not
//!
//! `AWAIT_ARCHIVE_CONNECTION → SEND_LIST_RECORDING_REQUEST →
//! AWAIT_LIST_RECORDING_RESPONSE → (SEND_REPLAY_REQUEST → AWAIT_REPLAY_RESPONSE)?
//! → ADD_REPLAY_SUBSCRIPTION → AWAIT_REPLAY_SUBSCRIPTION →
//! (AWAIT_REPLAY_CHANNEL_ENDPOINT)? → SEND_REPLAY_REQUEST → AWAIT_REPLAY_RESPONSE
//! → REPLAY`, which is everything up to a recorded message reaching the
//! application. The two `?`s are the reference's two orderings: a channel that
//! names its session can be subscribed to after the replay is asked for, and one
//! that says `endpoint=…:0` has to be subscribed to *first* so that the port the
//! driver picks is the one the archive is told to replay to (`:1035-1050`,
//! `:1370-1371`, `:1490-1492`).
//!
//! # The third replay channel shape: a **response channel**
//!
//! A replay channel that carries `control-mode=response` is asked for over a
//! publication of the *client's* own, on the archive's control request channel
//! (`:1602-1657`) — and a replay made over one needs a **replay token** first
//! (`:1729-1817`), which is what makes an image that the archive made for
//! somebody else's session findable. Those four states and the three things they
//! own — the publication, the proxy over it, and the token — are
//! [`State::AddRequestPublication`] through [`State::AwaitReplayToken`].
//!
//! # The live half, and the two self-healing edges
//!
//! The catch-up gate (`:1987-1990`), the live subscription's three states,
//! `ATTEMPT_SWITCH`, and the two edges back — the live image going away
//! (`:2247-2268`) and the archive going away (`:804-829`) — are all here. What
//! those edges do *next* is what the remaining commits of this slice add: the
//! exit from `AWAIT_LIVE` is where the reference can retry a
//! `RESOURCE_TEMPORARILY_UNAVAILABLE`, and its `clean_up_replay` — the
//! `stop_replay` request — belongs with the closing family.
//!
//! # The switch is positions, not transports
//!
//! ★ `activeTransportCount() >= 2` — the gate the reference's **replay merge**
//! uses (`aeron_archive_replay_merge.c:831`) — is **not** part of this machine
//! and must not be ported into it. Here, "has the replay caught up?" is answered
//! by asking the archive for `max_recorded_position` and comparing it with the
//! replay image's position (`:683-698`), with a quarter of the recording's term
//! buffer as the margin (`:1157-1159`). Two mechanisms for two jobs, and the
//! merge's is the one that needs a transport count.
//!
//! # Where the answers come from
//!
//! This machine never reads the control channel. The **async client** does, and
//! this object is its listener (`:994-999`, `:831-860`): every control response
//! and every recording descriptor arrives at [`Ops`] and is filed by
//! correlation id. That is why there is an [`Rc`] here — the listener is owned
//! by the async client, and this object has to read what it wrote — and why
//! every borrow of it is taken and dropped around one call.

use core::fmt;
use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::time::{Duration, Instant};

use deepmsg_client::client::{AsyncAdd, AsyncAddPoll, CommandError};
use deepmsg_client::fragment_assembler::{Action, ControlledHandler, Message};
use deepmsg_codec::archive::control_response_code::ControlResponseCode;
use deepmsg_core::uri::{ChannelUri, UriError};

use crate::client::async_client::{
    AsyncArchiveClient, AsyncClientError, ControlResponse, Listener,
};
use crate::client::async_connect::Credentials;
use crate::client::descriptor_poller::RecordingDescriptor;
use crate::client::persistent_subscription::context::{
    ConcludeError, Concluded, FROM_LIVE, FROM_START, PersistentSubscriptionContext,
};
use crate::client::persistent_subscription::counters::{Counters, Identity, NOTHING_JOINED};
use crate::client::proxy::{ArchiveProxy, NULL_VALUE, ReplayParams};

/// `ARCHIVE_REPLAY_ALL_AND_FOLLOW` (`aeron_archive.h:35`): replay everything
/// there is and keep following it. A replay that stopped at the end of the
/// recording could never become the live stream.
const ALL_AND_FOLLOW: i64 = -1;

/// `SESSION_ID_KEY` on a replay channel — the session the replay publication
/// will have, written in once the archive has named it (`:1448-1471`).
const SESSION_ID_KEY: &str = "session-id";

/// The media of a channel that stays inside this process (`media` in a URI).
const IPC_MEDIA: &str = "ipc";

/// `AERON_URI_RESPONSE_CORRELATION_ID_KEY` (`:1615`): which of the client's
/// subscriptions a response publication answers.
const RESPONSE_CORRELATION_ID_KEY: &str = "response-correlation-id";

/// `AERON_URI_TERM_LENGTH_KEY` (`:1619`).
const TERM_LENGTH_KEY: &str = "term-length";

/// `AERON_URI_SPIES_SIMULATE_CONNECTION_KEY` (`:1620`).
const SPIES_SIMULATE_CONNECTION_KEY: &str = "spies-simulate-connection";

/// The channel a response-channel replay's request publication is made on
/// (`:1610-1621`).
///
/// **Its own function because it is the one part of those four states that is
/// arithmetic rather than waiting**, and because what it does to the control
/// request channel is easy to read past: four keys come *off* and three go on.
/// A channel that kept its `session-id` would be a publication claiming to be
/// the archive's control channel; one that kept `term-id`/`term-offset` would
/// claim a position in it.
///
/// # Errors
///
/// [`UriError`] when the control request channel is not a channel at all — the
/// one failure the reference can have here too, since it builds this string from
/// the context's own setting (`:1613`).
fn request_publication_channel(
    control_channel: &str,
    term_buffer_length: i32,
    subscription_id: i64,
) -> Result<String, UriError> {
    let mut uri = ChannelUri::parse(control_channel)?;

    for key in REQUEST_PUBLICATION_REMOVED_KEYS {
        uri.remove(key);
    }

    uri.put(RESPONSE_CORRELATION_ID_KEY, subscription_id.to_string());
    uri.put(TERM_LENGTH_KEY, term_buffer_length.to_string());
    uri.put(SPIES_SIMULATE_CONNECTION_KEY, "false");

    Ok(uri.build())
}

/// The four the request publication's channel has **taken off** it (`:1614`,
/// `:1616-1618`).
///
/// The reference writes each with a null value, and a null value in a
/// `aeron_uri_string_builder` is not an empty one: it **removes** the key
/// (`aeron_uri_string_builder.c:180-183`). So the publication starts from the
/// control request channel with its session and its term position forgotten —
/// which is what makes it its own publication rather than a second view of the
/// archive's control channel.
const REQUEST_PUBLICATION_REMOVED_KEYS: [&str; 4] =
    ["session-id", "initial-term-id", "term-id", "term-offset"];

/// How long a subscription of this build's waits for its own driver commands.
/// Not the reference's `message_timeout_ns`, which is how long it waits for the
/// *archive*.
const COMMAND_TIMEOUT: Duration = Duration::from_secs(10);

/// The reference's eighteen states, in its order
/// (`…_persistent_subscription.h:75-98`).
///
/// Every one of them is here, so [`State::as_counter_value`] has no gaps left —
/// and no state is present-and-unreachable, which is the other way a list like
/// this lies: a state nothing can enter is a claim nothing checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    /// Waiting for the archive connection the async client is making.
    AwaitArchiveConnection,
    /// Asking for the recording's descriptor.
    SendListRecordingRequest,
    /// Waiting for that descriptor.
    AwaitListRecordingResponse,
    /// Asking for a replay.
    SendReplayRequest,
    /// Waiting for the replay's answer, which carries the replay session id.
    AwaitReplayResponse,
    /// Putting a subscription on the replay channel.
    AddReplaySubscription,
    /// Waiting for the driver to make that subscription.
    AwaitReplaySubscription,
    /// A replay channel that named `endpoint=…:0`, waiting for the port the
    /// driver picked.
    AwaitReplayChannelEndpoint,
    /// A **response channel** replay: putting an exclusive publication of our
    /// own on the archive's control request channel, which is where the replay
    /// request will go out from (`:1602-1657`).
    AddRequestPublication,
    /// Waiting for the driver to make that publication.
    AwaitRequestPublication,
    /// Asking the archive for the replay token this recording's replay is
    /// addressed by (`:1729-1758`).
    SendReplayTokenRequest,
    /// Waiting for that token (`:1760-1816`).
    AwaitReplayToken,
    /// Reading the replay, and deciding when it has caught up.
    Replay,
    /// Putting a subscription on the live channel, for a subscription that has
    /// nothing to replay — a start position at (or past) the end of the
    /// recording.
    AddLiveSubscription,
    /// Waiting for the live stream's image.
    AwaitLive,
    /// The live subscription is up: aligning the replay's position with the
    /// live image's, so that the seam has no message twice and none missing.
    AttemptSwitch,
    /// Reading the live stream.
    Live,
    /// Terminal.
    Failed,
}

impl State {
    /// What the state counter reads: **the enum's own value**
    /// (`…_persistent_subscription.h:76-97`), which is what the reference's
    /// tests and a reader of the counters file go by.
    ///
    /// The enum is the reference's, in its order, so these are its values and
    /// not a count: `REPLAY` is twelve because eleven states come before it, and
    /// a counter that said eight would agree with nothing else. Writing them out
    /// rather than counting is what makes that checkable by eye.
    #[must_use]
    pub const fn as_counter_value(self) -> i64 {
        match self {
            Self::AwaitArchiveConnection => 0,
            Self::SendListRecordingRequest => 1,
            Self::AwaitListRecordingResponse => 2,
            Self::SendReplayRequest => 3,
            Self::AwaitReplayResponse => 4,
            Self::AddReplaySubscription => 5,
            Self::AwaitReplaySubscription => 6,
            Self::AwaitReplayChannelEndpoint => 7,
            Self::AddRequestPublication => 8,
            Self::AwaitRequestPublication => 9,
            Self::SendReplayTokenRequest => 10,
            Self::AwaitReplayToken => 11,
            Self::Replay => 12,
            Self::AttemptSwitch => 13,
            Self::AddLiveSubscription => 14,
            Self::AwaitLive => 15,
            Self::Live => 16,
            Self::Failed => 17,
        }
    }
}

/// What an application hears about
/// (`aeron_archive_persistent_subscription_listener_t`, `:1355-1397`).
///
/// Every method has a default: the reference's listener is a struct of function
/// pointers of which any may be null, and `shouldNotRequireEventListener`
/// (`…_persistent_subscription_test.cpp:2878`) is the case that says so.
pub trait SubscriptionListener {
    /// The recording has been caught up with and the live stream is being read.
    fn on_live_joined(&mut self) {}

    /// The live stream went away; a replay of what was missed follows without
    /// being asked for.
    fn on_live_left(&mut self) {}

    /// A refusal that is not (yet) terminal — the reference calls it for a
    /// subscription the driver would not make just now and for an image that
    /// did not arrive in time (`:786-798`, `:1531-1543`).
    fn on_error(&mut self, error: &SubscriptionError);
}

/// Why a persistent subscription refused something.
#[derive(Debug)]
pub enum SubscriptionError {
    /// The async client failed.
    Archive(AsyncClientError),
    /// The archive answered a request with something other than `OK`.
    Refused {
        /// Which request, named the way the reference's message names it.
        request: &'static str,
        /// The archive's own message.
        message: String,
    },
    /// The recording is not the one this subscription was configured for.
    Descriptor(String),
    /// The driver would not do something it was asked to.
    Client(CommandError),
}

impl fmt::Display for SubscriptionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Archive(error) => write!(f, "{error}"),
            Self::Refused { request, message } => write!(f, "{request} failed: {message}"),
            Self::Descriptor(reason) => write!(f, "{reason}"),
            Self::Client(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for SubscriptionError {}

/// Why a persistent subscription could not be made.
#[derive(Debug)]
pub enum NewError {
    /// The context is not one a subscription can be made with.
    Context(ConcludeError),
    /// The driver could not be reached, or would not take the counters.
    Subscription(SubscriptionError),
}

impl fmt::Display for NewError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Context(error) => write!(f, "{error}"),
            Self::Subscription(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for NewError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Context(error) => Some(error),
            Self::Subscription(error) => Some(error),
        }
    }
}

impl From<ConcludeError> for NewError {
    fn from(error: ConcludeError) -> Self {
        Self::Context(error)
    }
}

/// The sub-machine that asks the archive how far the recording has got, so that
/// "has the replay caught up?" has an answer (`:604-698`).
///
/// It is a sub-machine rather than a request because the answer has to be
/// *rechecked*: an answer taken once and kept would be a recording that has
/// moved on since, and one taken every turn would be a request per turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MaxRecordedPosition {
    /// Nothing asked yet, or the last answer was caught up with.
    Idle,
    /// The request has been sent and nothing has come back.
    Awaiting {
        /// What it was sent with.
        correlation_id: i64,
        /// When its answer stops being waited for.
        deadline: Instant,
    },
    /// The answer named a position the replay has not reached: the replay has
    /// to read what it has before it is worth asking again (`:671-681`).
    RecheckRequired {
        /// The position the answer named.
        max_recorded_position: i64,
    },
}

/// What the archive has answered, filed by correlation id — the operation slots
/// the reference's `on_archive_control_response` demultiplexes into
/// (`:831-860`). Four of them, one per request this machine makes; the fifth
/// request, `stop_replay`, is sent and not waited on (`:741-760`).
#[derive(Debug, Default)]
struct Ops {
    list_recording: ListRecording,
    replay: Replay,
    replay_token: ReplayToken,
    max_recorded_position: MaxPosition,
}

/// One `get_max_recorded_position` request and what came back (`:59-74`).
#[derive(Debug, Default)]
struct MaxPosition {
    correlation_id: i64,
    received: bool,
    code: Option<ControlResponseCode>,
    max_recorded_position: i64,
}

/// One `list_recording` request and the descriptor it was answered with
/// (`list_recording_request_t`, `…_persistent_subscription.h:107-119`).
///
/// **Answered two ways, and only one of them is a descriptor.** The request goes
/// out with `remaining = 1` (`:1145`); the descriptor handler is the only thing
/// that counts it down (`:877-879`); a *control response* sets the answer flag
/// without touching the count (`:845-848`). The archive sends one whenever it
/// refuses the request because a listing of its own is already in flight
/// (`ACTIVE_LISTING`) — a normal outcome of asking faster than an answer comes
/// back — and `validate_descriptor` then reads the count, not the flag, and
/// finds a reply that is not a descriptor (`:1202`, `:1292-1306`).
///
/// The reference's shared op also carries `code`, `relevant_id` and
/// `error_message` through that same dispatcher, and **reads none of them for
/// this op**: nothing in `…_persistent_subscription.c` reads them off a
/// `list_recording_request`. This build does not carry write-only fields.
#[derive(Debug, Default)]
struct ListRecording {
    correlation_id: i64,
    /// **An answer**, of either kind — not necessarily a descriptor. What it
    /// was is [`Self::remaining`]'s business.
    received: bool,
    /// What the request is still waiting for: `1` from the send until the
    /// descriptor arrives (`:1145`, `:877-879`). The reference leaves this
    /// unset until the send, and this is what an unset one reads as.
    remaining: i32,
    start_position: i64,
    stop_position: i64,
    term_buffer_length: i32,
    stream_id: i32,
}

/// One `replay` request and what came back (`replay_request_t`, `:46-57`).
#[derive(Debug, Default)]
struct Replay {
    correlation_id: i64,
    received: bool,
    code: Option<ControlResponseCode>,
    relevant_id: i64,
    error_message: String,
}

/// One `request_replay_token` and what came back
/// (`replay_token_request`, `…_persistent_subscription.h:136`) — the same four
/// things a `replay` is answered with, because it is the same kind of operation
/// (`async_archive_op_t`, `:59-74`). The **relevant id is the token**, which is
/// the one difference that matters.
#[derive(Debug, Default)]
struct ReplayToken {
    correlation_id: i64,
    received: bool,
    code: Option<ControlResponseCode>,
    relevant_id: i64,
    error_message: String,
}

/// The listener the async client holds for this subscription: file what came
/// back by correlation id, and pass a failure on to the application.
struct ArchiveListener {
    ops: Rc<RefCell<Ops>>,
    /// The async client lost the archive. **Shared for the same reason the
    /// failure is**: this callback runs inside `AsyncArchiveClient::poll`, and
    /// what to do about it is the state machine's (`on_archive_disconnected`,
    /// `:804-829`).
    disconnected: Rc<Cell<bool>>,
    /// The async client failed — a connect that did not finish, or a message it
    /// could not read. **Shared rather than acted on here**: the application's
    /// listener belongs to the subscription, which is the only thing that
    /// knows whether the failure is terminal, and this callback runs inside
    /// `AsyncArchiveClient::poll`, where the state machine is not.
    failure: Rc<RefCell<Option<String>>>,
}

impl Listener for ArchiveListener {
    fn on_disconnected(&mut self) {
        self.disconnected.set(true);
    }

    fn on_control_response(&mut self, response: &ControlResponse<'_>) {
        let mut ops = self.ops.borrow_mut();

        if response.correlation_id == ops.replay.correlation_id
            && NULL_VALUE != response.correlation_id
        {
            ops.replay.received = true;
            ops.replay.code = Some(response.code);
            ops.replay.relevant_id = response.relevant_id;
            ops.replay.error_message = response.error_message.to_owned();
        }

        if response.correlation_id == ops.replay_token.correlation_id
            && NULL_VALUE != response.correlation_id
        {
            ops.replay_token.received = true;
            ops.replay_token.code = Some(response.code);
            ops.replay_token.relevant_id = response.relevant_id;
            ops.replay_token.error_message = response.error_message.to_owned();
        }

        if response.correlation_id == ops.max_recorded_position.correlation_id
            && NULL_VALUE != response.correlation_id
        {
            ops.max_recorded_position.received = true;
            ops.max_recorded_position.code = Some(response.code);
            ops.max_recorded_position.max_recorded_position = response.relevant_id;
        }

        // ★ The **fourth** dispatcher the reference has and this build did not
        // (`:845-848`). Without it an archive that refuses a `list_recording`
        // request — `ACTIVE_LISTING`, which is what asking faster than an answer
        // comes back produces — has its answer dropped on the floor, and the
        // subscription waits out a whole message timeout for a reply it is
        // already holding.
        //
        // `received` only: what the answer was is `remaining`'s to say
        // (`:877-879`), and it is not counted down for this one.
        if response.correlation_id == ops.list_recording.correlation_id
            && NULL_VALUE != response.correlation_id
        {
            ops.list_recording.received = true;
        }
    }

    fn on_recording_descriptor(&mut self, descriptor: &RecordingDescriptor) {
        let mut ops = self.ops.borrow_mut();

        if descriptor.correlation_id == ops.list_recording.correlation_id {
            // The descriptor is what `remaining` was counting (`:877-879`): it
            // is counted down here and the answer is only in once it reaches
            // zero. The numbers underneath are copied either way, as the
            // reference copies them.
            ops.list_recording.remaining -= 1;

            if 0 == ops.list_recording.remaining {
                ops.list_recording.received = true;
            }

            ops.list_recording.start_position = descriptor.start_position;
            ops.list_recording.stop_position = descriptor.stop_position;
            ops.list_recording.term_buffer_length = descriptor.term_buffer_length;
            ops.list_recording.stream_id = descriptor.stream_id;
        }
    }

    fn on_error(&mut self, error: &AsyncClientError) {
        *self.failure.borrow_mut() = Some(format!("{error}"));
    }
}

/// A subscription that replays a recording. See the module header for the path
/// it walks.
pub struct PersistentSubscription {
    state: State,
    client: AsyncArchiveClient,
    context: PersistentSubscriptionContext,
    /// What the context concluded to: the rewritten replay channel, and the ids
    /// and channel the descriptor and the replay are checked against. Kept
    /// rather than re-derived, because `conclude` is the one place that decides
    /// them (and it rewrites the channel on the way through).
    concluded: Concluded,
    counters: Counters,
    ops: Rc<RefCell<Ops>>,
    /// Set by the async client's listener when *it* fails; read and cleared by
    /// the next turn. See [`ArchiveListener::failure`].
    archive_failure: Rc<RefCell<Option<String>>>,
    /// Set by the async client's listener when the archive goes away; read and
    /// cleared by the next turn.
    archive_disconnected: Rc<Cell<bool>>,
    listener: Option<Box<dyn SubscriptionListener>>,
    /// How long any answer from the **archive** may take (`message_timeout_ns`,
    /// `:1015`) — snapped at create, because the reference reads it once.
    message_timeout: Duration,
    /// Where in the stream the application is: the replay's position now, the
    /// live stream's later. Starts at the context's start position.
    position: i64,
    /// Where the live stream had got to the last time it was looked at, which
    /// is what keeps a replay from being started behind it (`:1223`).
    last_observed_live_position: i64,
    /// What the join difference reads (`:909-919`); `INT64_MIN` until a join.
    join_difference: i64,
    /// The replay channel as it will be subscribed to, which for a channel that
    /// names its session is the configured one **with the id written in**.
    replay_channel_uri: String,
    replay_channel_type: ReplayChannelType,
    /// The subscription's registration id once the driver has made it.
    replay_subscription: Option<i64>,
    /// The `ADD_SUBSCRIPTION` in flight, whose registration id the driver will
    /// answer under (`:1494`).
    replay_subscription_add: Option<i64>,
    /// The session the archive named for this replay (`:1446`).
    replay_session_id: i64,
    /// The `ADD_EXCLUSIVE_PUBLICATION` a **response channel** replay is waiting
    /// on, whose registration id the driver will answer under (`:1638-1655`).
    request_publication_add: Option<AsyncAdd>,
    /// That publication, once the driver has made it (`:1671`). A registration
    /// id and not the object: the client holds the publication, and every use of
    /// it — the proxy's offers, the removal — goes through the client.
    request_publication: Option<i64>,
    /// The proxy over that publication, which is where the replay request goes
    /// out instead of on the async client's own (`:1705-1723`).
    response_channel_proxy: Option<ArchiveProxy>,
    /// What the archive named this replay's token (`:1804`), sent with the
    /// replay request so the archive can find the image it made for us.
    replay_token: i64,
    /// Whether the replay channel is IPC, which is the one thing that decides
    /// whether the replay request goes out before the channel's endpoint is
    /// known or after it (`:1806-1813`).
    replay_channel_is_ipc: bool,
    /// The deadline of whatever is being waited for, or `None`.
    deadline: Option<Instant>,
    /// The margin the catch-up check allows: a quarter of the recording's term
    /// buffer (`:1157-1159`).
    close_enough_threshold: i64,
    /// The `max_recorded_position` sub-machine.
    max_recorded_position: MaxRecordedPosition,
    /// Where the live stream takes over, found by the live side of the
    /// alignment; [`NULL_VALUE`] until it is (`:2002-2017`).
    next_live_position: i64,
    /// The live channel's subscription, once the driver has made it.
    live_subscription: Option<i64>,
    /// The `ADD_SUBSCRIPTION` on the live channel that is in flight.
    live_subscription_add: Option<i64>,
    /// When the live subscription's image should have appeared by (`:779-784`).
    live_image_deadline: Option<Instant>,
    /// Set when that deadline has already been reported, so that the listener is
    /// told **once per deadline** rather than on every turn after it (`:783`,
    /// `:789`).
    live_image_deadline_breached: bool,
    /// How it failed, if it did (`:52-57`).
    failure: Option<String>,
}

/// Which shape the replay channel is (`REPLAY_CHANNEL_*`, `:1035-1050`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplayChannelType {
    /// The channel names a session (`session-id=…`), so the subscription goes on
    /// it after the replay has been asked for.
    SessionSpecific,
    /// `endpoint=…:0`: the driver picks the port, so the subscription goes up
    /// first and the archive is told where to replay to.
    DynamicPort,
    /// `control-mode=response`: the replay request goes out over a publication
    /// of the client's own, on the archive's control request channel, and is
    /// addressed by a **replay token** the archive hands out (`:1037-1040`).
    ResponseChannel,
}

impl PersistentSubscription {
    /// Create one, which is `conclude` + the four counters + the async client
    /// (`:954-1105`).
    ///
    /// The **listener is an argument** rather than a field of the context, the
    /// way the reference has it (`context_set_listener`, `:268-281`): in C the
    /// context is where a caller can put one before it has anything else,
    /// and here the call that makes the subscription is that place.
    ///
    /// # Errors
    ///
    /// [`NewError::Context`] for one of the context's ten refusals, and
    /// [`NewError::Subscription`] for a driver that will not take the counters.
    pub fn new(
        context: PersistentSubscriptionContext,
        credentials: Box<dyn Credentials>,
        listener: Option<Box<dyn SubscriptionListener>>,
    ) -> Result<Self, NewError> {
        let concluded = context.conclude()?;

        let archive_context = context
            .archive_context
            .clone()
            .expect("conclude refuses a context without one");

        let ops = Rc::new(RefCell::new(Ops::default()));
        let failure = Rc::new(RefCell::new(None));
        let disconnected = Rc::new(Cell::new(false));

        let mut client = AsyncArchiveClient::new(
            archive_context,
            credentials,
            Box::new(ArchiveListener {
                ops: Rc::clone(&ops),
                failure: Rc::clone(&failure),
                disconnected: Rc::clone(&disconnected),
            }),
        );

        // **The counters come before the subscription does**, because a state
        // transition writes one and there is a transition at the end of create
        // (`:1058-1060`). The connection they are allocated over is the async
        // client's own, which is the one connection this object has.
        let counters = {
            let Some(driver) = client.client_mut() else {
                return Err(NewError::Subscription(SubscriptionError::Refused {
                    request: "create",
                    message: "there is no driver to allocate the counters on".to_owned(),
                }));
            };

            let identity = Identity {
                replay_channel: &concluded.replay_channel,
                replay_stream_id: concluded.replay_stream_id,
                live_channel: &concluded.live_channel,
                live_stream_id: concluded.live_stream_id,
            };

            Counters::set_up(driver, &identity, context.counters.clone(), COMMAND_TIMEOUT)
                .map_err(|error| NewError::Subscription(SubscriptionError::Client(error)))?
        };

        let message_timeout_ns = context
            .archive_context
            .as_ref()
            .map_or(0, |archive| archive.message_timeout_ns);

        let mut subscription = Self {
            state: State::AwaitArchiveConnection,
            client,
            context,
            counters,
            ops,
            archive_failure: failure,
            archive_disconnected: Rc::clone(&disconnected),
            listener,
            message_timeout: Duration::from_nanos(u64::try_from(message_timeout_ns).unwrap_or(0)),
            position: concluded.start_position,
            last_observed_live_position: NULL_VALUE,
            join_difference: NOTHING_JOINED,
            replay_channel_uri: concluded.replay_channel.clone(),
            replay_channel_type: replay_channel_type(&concluded.replay_channel),
            replay_subscription: None,
            replay_subscription_add: None,
            replay_session_id: NULL_VALUE,
            request_publication_add: None,
            request_publication: None,
            response_channel_proxy: None,
            replay_token: NULL_VALUE,
            // Read off the **configured** channel, as the reference does at
            // create (`:1035`), and not off `replay_channel_uri` — which for a
            // session-specific channel is rewritten with the session id later.
            replay_channel_is_ipc: ChannelUri::parse(&concluded.replay_channel)
                .is_ok_and(|uri| uri.media() == IPC_MEDIA),
            deadline: None,
            close_enough_threshold: 0,
            max_recorded_position: MaxRecordedPosition::Idle,
            next_live_position: NULL_VALUE,
            live_subscription: None,
            live_subscription_add: None,
            live_image_deadline: None,
            live_image_deadline_breached: false,
            failure: None,
            concluded,
        };

        subscription.transition(State::AwaitArchiveConnection);

        Ok(subscription)
    }

    /// Close it, giving everything it holds back
    /// (`aeron_archive_persistent_subscription_close`, `:1074-1097`).
    ///
    /// The order is the reference's, and it is the order of **dependency**: the
    /// live subscription and the request publication go first because they are
    /// the ones that can be *asking* something, then the replay is stopped
    /// — which needs the connection — and only then is the replay subscription
    /// handed back and the client closed under it.
    ///
    /// ★ What the reference does that a `Drop` cannot: it is **explicit**. Its
    /// close returns an `int` a caller is meant to read, and everything it frees
    /// is freed in a known order rather than whenever the value happens to go
    /// out of scope. The same is offered here, and `Drop` is deliberately
    /// **not** implemented for this type: a subscription that closed itself
    /// would hide the one thing a caller has to decide — whether the application
    /// is done with it — behind when a local variable happened to die.
    pub fn close(mut self) {
        self.clean_up_live_subscription();
        self.clean_up_request_publication();
        self.clean_up_replay();
        self.clean_up_replay_subscription();

        self.client.close();
    }

    /// Where the machine is.
    #[must_use]
    pub const fn state(&self) -> State {
        self.state
    }

    /// Whether the recording is being replayed (`:2382-2387`).
    ///
    /// **`ATTEMPT_SWITCH` counts.** It is still the replay delivering messages —
    /// the switch is the replay finishing, not the live stream starting — and
    /// the reference's own case asserts this predicate while the machine is
    /// exactly there.
    #[must_use]
    pub const fn is_replaying(&self) -> bool {
        matches!(self.state, State::Replay | State::AttemptSwitch)
    }

    /// Whether the live stream is being read (`:2377-2380`).
    #[must_use]
    pub const fn is_live(&self) -> bool {
        matches!(self.state, State::Live)
    }

    /// Whether it has failed (`:2389-2392`).
    #[must_use]
    pub const fn has_failed(&self) -> bool {
        matches!(self.state, State::Failed)
    }

    /// How it failed, if it did (`:2394-2417`).
    #[must_use]
    pub fn failure_reason(&self) -> Option<&str> {
        self.failure.as_deref()
    }

    /// How far the live stream was ahead when it was joined, or
    /// [`NOTHING_JOINED`] (`:2419-2423`).
    #[must_use]
    pub const fn join_difference(&self) -> i64 {
        self.join_difference
    }

    /// How long an answer may take, for a test that wants a deadline to pass
    /// without waiting for the real one (`:2425-2429`).
    pub fn set_message_timeout_ns_for_testing(&mut self, timeout_ns: i64) {
        self.message_timeout = Duration::from_nanos(u64::try_from(timeout_ns).unwrap_or(0));
    }

    /// Take a turn: the archive client first, then whichever state is current
    /// (`:2270-2341`).
    ///
    /// The application's fragment handler is handed every message the replay
    /// produces, in order. What the state machine does with the handler is the
    /// reference's too: in `REPLAY` it is the handler the poll runs with
    /// (`:1983`, `:1854-1873`).
    pub fn controlled_poll<H: ControlledHandler>(
        &mut self,
        handler: &mut H,
        fragment_limit: usize,
    ) -> usize {
        // **The archive client is polled first, in every state** (`:2281`) —
        // including `FAILED`, which is why it is not inside the match.
        let mut work = self.client.poll();

        // And what it made of the turn. The reference's `on_archive_error`
        // (`:884-907`) is called from inside that poll; this is the same
        // decision taken one frame out, so that the application's listener is
        // reached from the state machine rather than from the callback.
        if self.archive_disconnected.replace(false) {
            self.on_archive_disconnected();
        }

        let archive_failure = self.archive_failure.borrow_mut().take();

        if let Some(message) = archive_failure {
            if !matches!(self.state, State::Failed) {
                self.fail(SubscriptionError::Refused {
                    request: "archive client",
                    message,
                });
            }

            return work + 1;
        }

        work += match self.state {
            State::AwaitArchiveConnection => self.await_archive_connection(),
            State::SendListRecordingRequest => self.send_list_recording_request(),
            State::AwaitListRecordingResponse => self.await_list_recording_response(),
            State::SendReplayRequest => self.send_replay_request(),
            State::AwaitReplayResponse => self.await_replay_response(),
            State::AddReplaySubscription => self.add_replay_subscription(),
            State::AwaitReplaySubscription => self.await_replay_subscription(),
            State::AwaitReplayChannelEndpoint => self.await_replay_channel_endpoint(),
            State::AddRequestPublication => self.add_request_publication(),
            State::AwaitRequestPublication => self.await_request_publication(),
            State::SendReplayTokenRequest => self.send_replay_token_request(),
            State::AwaitReplayToken => self.await_replay_token(),
            State::Replay => self.replay(handler, fragment_limit),
            State::AddLiveSubscription => self.add_live_subscription(),
            State::AwaitLive => self.await_live(),
            State::AttemptSwitch => self.attempt_switch(handler, fragment_limit),
            State::Live => self.live(handler, fragment_limit),
            State::Failed => 0,
        };

        work
    }

    /// `AWAIT_ARCHIVE_CONNECTION` (`:1110-1121`).
    fn await_archive_connection(&mut self) -> usize {
        if !self.client.is_connected() {
            return 0;
        }

        self.transition(State::SendListRecordingRequest);

        1
    }

    /// `SEND_LIST_RECORDING_REQUEST` (`:1123-1150`).
    fn send_list_recording_request(&mut self) -> usize {
        let Ok(correlation_id) = self.next_correlation_id() else {
            return 0;
        };

        let recording_id = self.context.recording_id;
        let sent = self
            .client
            .try_send_list_recording_request(correlation_id, recording_id);

        if !sent {
            // The reference distinguishes the two reasons a send did not
            // happen: a channel that is up but busy is retried next turn, one
            // that is gone means the connection is (`:1130-1140`).
            if self.client.is_connected() {
                return 0;
            }

            self.transition(State::AwaitArchiveConnection);

            return 1;
        }

        self.ops.borrow_mut().list_recording = ListRecording {
            correlation_id,
            // `:1145`: what this counts is the descriptor, and the send is what
            // starts the count.
            remaining: 1,
            ..ListRecording::default()
        };
        self.deadline = Some(Instant::now() + self.message_timeout);
        self.transition(State::AwaitListRecordingResponse);

        1
    }

    /// `AWAIT_LIST_RECORDING_RESPONSE` (`:1312-1351`).
    fn await_list_recording_response(&mut self) -> usize {
        if !self.ops.borrow().list_recording.received {
            if !self.deadline_passed() {
                return 0;
            }

            self.transition(if self.client.is_connected() {
                State::SendListRecordingRequest
            } else {
                State::AwaitArchiveConnection
            });

            return 1;
        }

        if !self.validate_descriptor() {
            return 1;
        }

        // **When there is nothing to replay, the live stream is joined
        // directly** (`:1339-1341`): the subscription starts at the live
        // position, or the recording stopped at the position it starts from, and
        // in both cases a replay would deliver nothing — so the live
        // subscription is the whole of the path.
        let stop_position = self.ops.borrow().list_recording.stop_position;

        if FROM_LIVE == self.position
            || (NULL_VALUE != stop_position && self.position == stop_position)
        {
            self.transition(State::AddLiveSubscription);

            return 1;
        }

        self.set_up_replay();

        1
    }

    /// The question `validate_descriptor` asks before any of the numbers
    /// (`:1202`, `:1292-1306`): **an answer that is not a descriptor is not a
    /// descriptor**, whatever it says.
    ///
    /// A value rather than a branch, so that it can be judged without a
    /// subscription — which is what the criterion beside it needs. The shape
    /// this build used to have (any answer made the numbers usable) is what
    /// turned an `ACTIVE_LISTING` refusal into a ten-second wait: the refusal
    /// set the answer flag while the numbers under it had never been written.
    ///
    /// The message is the reference's, wrong as it reads here: what reaches it
    /// is the refusal, and the reference calls that "unknown recording id".
    fn missing_descriptor(remaining: i32, recording_id: i64) -> Result<(), SubscriptionError> {
        if 0 == remaining {
            return Ok(());
        }

        Err(SubscriptionError::Descriptor(format!(
            "unknown recording id: {recording_id}"
        )))
    }

    /// `validate_descriptor` (`:1197-1310`): the recording has to be the one
    /// this subscription was configured for, and the position has to be one the
    /// recording has.
    ///
    /// Answers whether the descriptor is usable; every refusal is terminal.
    fn validate_descriptor(&mut self) -> bool {
        let (remaining, start_position, stop_position, stream_id) = {
            let list = &self.ops.borrow().list_recording;

            (
                list.remaining,
                list.start_position,
                list.stop_position,
                list.stream_id,
            )
        };

        if let Err(error) = Self::missing_descriptor(remaining, self.context.recording_id) {
            self.fail(error);

            return false;
        }

        let live_stream_id = self.context.live_stream_id;

        if live_stream_id != stream_id {
            self.fail(SubscriptionError::Descriptor(format!(
                "Requested live stream with ID: {live_stream_id} does not match stream ID: \
                 {stream_id} for recording: {}",
                self.context.recording_id
            )));

            return false;
        }

        // A recording that stopped before the live stream was seen cannot be
        // replayed into it: there is a gap the archive does not have (`:1223`).
        if NULL_VALUE != stop_position && self.last_observed_live_position > stop_position {
            self.fail(SubscriptionError::Descriptor(format!(
                "recording {} stopped at position={stop_position} which is earlier than last \
                 observed live position={}",
                self.context.recording_id, self.last_observed_live_position
            )));

            return false;
        }

        if self.position >= 0 && self.position < start_position {
            self.fail(SubscriptionError::Descriptor(format!(
                "requested replay start position={} is less than recording start position=\
                 {start_position} for recording {}",
                self.position, self.context.recording_id
            )));

            return false;
        }

        if self.position >= 0 && NULL_VALUE != stop_position && self.position > stop_position {
            self.fail(SubscriptionError::Descriptor(format!(
                "requested replay position={} must be less than the limit position={stop_position}",
                self.position
            )));

            return false;
        }

        // `FROM_START` is a question the descriptor has just answered
        // (`:1287-1290`). `FROM_LIVE` is not, and `new` refused it already.
        if self.position == FROM_START {
            self.position = start_position;
        }

        true
    }

    /// `set_up_replay` (`:1164-1177`): the state every path back into a replay
    /// goes through, so that the catch-up bookkeeping is never half-reset.
    ///
    fn set_up_replay(&mut self) {
        self.set_join_difference(NOTHING_JOINED);
        self.close_enough_threshold =
            i64::from(self.ops.borrow().list_recording.term_buffer_length >> 2);
        self.max_recorded_position = MaxRecordedPosition::Idle;
        self.next_live_position = NULL_VALUE;

        self.transition(match self.replay_channel_type {
            ReplayChannelType::SessionSpecific => State::SendReplayRequest,
            _ => State::AddReplaySubscription,
        });
    }

    /// `SEND_REPLAY_REQUEST` (`:1353-1400`).
    ///
    /// Three callers, and they arrive with different things in hand: a
    /// session-specific channel comes here straight from `set_up_replay`, a
    /// channel the driver picked a port for from `AWAIT_REPLAY_CHANNEL_ENDPOINT`,
    /// and a **response channel** from `AWAIT_REPLAY_TOKEN` — the last with the
    /// publication it made and the token it was given (`:1364-1368`).
    fn send_replay_request(&mut self) -> usize {
        let Ok(correlation_id) = self.next_correlation_id() else {
            return 0;
        };

        let recording_id = self.context.recording_id;
        let replay_stream_id = self.context.replay_stream_id;

        // A channel that names its session is asked for by the channel that was
        // *configured*; one that does not is asked for by the channel the driver
        // has just resolved (`:1370-1371`).
        let channel = match self.replay_channel_type {
            ReplayChannelType::SessionSpecific => self.concluded.replay_channel.clone(),
            _ => self.replay_channel_uri.clone(),
        };

        let params = ReplayParams {
            position: self.position,
            length: ALL_AND_FOLLOW,
            // Zero unless this is a response-channel replay, which is the only
            // kind of replay that has one (`:1367`).
            replay_token: self.replay_token,
            ..ReplayParams::default()
        };

        let response_channel = ReplayChannelType::ResponseChannel == self.replay_channel_type;

        let sent = {
            // Split into two borrows rather than taken as one `self`: a
            // response-channel replay goes out **through** the client on a
            // publication the client owns, and the proxy that writes into it is
            // this object's other half.
            let Self {
                client,
                response_channel_proxy,
                ..
            } = self;

            match (response_channel, response_channel_proxy.as_mut()) {
                (true, Some(proxy)) => client.try_send_replay_request(
                    Some(proxy),
                    correlation_id,
                    recording_id,
                    &channel,
                    replay_stream_id,
                    &params,
                ),
                _ => client.try_send_replay_request(
                    None,
                    correlation_id,
                    recording_id,
                    &channel,
                    replay_stream_id,
                    &params,
                ),
            }
        };

        if !sent {
            if self.client.is_connected() {
                return 0;
            }

            self.clean_up_request_publication();
            self.clean_up_replay_subscription();
            self.transition(State::AwaitArchiveConnection);

            return 1;
        }

        self.ops.borrow_mut().replay = Replay {
            correlation_id,
            ..Replay::default()
        };
        self.deadline = Some(Instant::now() + self.message_timeout);
        self.transition(State::AwaitReplayResponse);

        1
    }

    /// `AWAIT_REPLAY_RESPONSE` (`:1402-1483`).
    fn await_replay_response(&mut self) -> usize {
        let (received, code, relevant_id, message) = {
            let replay = &self.ops.borrow().replay;

            (
                replay.received,
                replay.code,
                replay.relevant_id,
                replay.error_message.clone(),
            )
        };

        if !received {
            if !self.deadline_passed() {
                return 0;
            }

            self.clean_up_request_publication();
            self.clean_up_replay_subscription();

            if self.client.is_connected() {
                self.set_up_replay();
            } else {
                self.transition(State::AwaitArchiveConnection);
            }

            return 1;
        }

        if Some(ControlResponseCode::OK) != code {
            self.clean_up_request_publication();
            self.clean_up_replay_subscription();
            self.fail(SubscriptionError::Refused {
                request: "Replay request",
                message,
            });

            return 1;
        }

        self.replay_session_id = relevant_id;

        if ReplayChannelType::SessionSpecific == self.replay_channel_type {
            // The channel the subscription goes on is the configured one with
            // the session the archive just named written into it (`:1448-1471`)
            // — that is what makes the replay publication's image findable by
            // session id later, and what keeps two replays on one channel apart.
            let Ok(mut uri) = ChannelUri::parse(&self.replay_channel_uri) else {
                self.fail(SubscriptionError::Refused {
                    request: "Replay request",
                    message: format!(
                        "replay channel is not a channel: {}",
                        self.replay_channel_uri
                    ),
                });

                return 1;
            };

            // ★ **The low half of it, truncated.** A replay session id is
            // `(replayId << 32) | sessionId` — sixty-four bits — and a channel's
            // `session-id` is thirty-two, so the reference writes
            // `(int32_t)ps->replay_session_id` (`:1448-1471`) and a driver that
            // is given the whole thing refuses the channel. The image is looked
            // up by the same truncated id in `REPLAY`, for the same reason.
            uri.put(SESSION_ID_KEY, (self.replay_session_id as i32).to_string());
            self.replay_channel_uri = uri.build();

            self.transition(State::AddReplaySubscription);
        } else {
            // **The publication goes back as soon as the replay is agreed to**
            // (`:1474`): it was how the request was asked, and the replay itself
            // arrives on the replay subscription. A response-channel replay that
            // kept it would be holding a publication open for nothing.
            self.clean_up_request_publication();

            self.deadline = Some(Instant::now() + self.message_timeout);
            self.transition(State::Replay);
        }

        1
    }

    /// `ADD_REPLAY_SUBSCRIPTION` (`:1485-1517`).
    fn add_replay_subscription(&mut self) -> usize {
        let channel = match self.replay_channel_type {
            ReplayChannelType::SessionSpecific => self.replay_channel_uri.clone(),
            _ => self.concluded.replay_channel.clone(),
        };

        let replay_stream_id = self.concluded.replay_stream_id;

        let Some(driver) = self.client.client_mut() else {
            self.transition(State::AwaitArchiveConnection);

            return 1;
        };

        // **Started and not waited on**, which is why the reference has two
        // states here: a poll that blocked on the driver would make a caller's
        // turn as long as the driver's answer, and the retry the next state does
        // for a `RESOURCE_TEMPORARILY_UNAVAILABLE` needs the request to be
        // re-issuable rather than already failed.
        match driver.submit_add_subscription(&channel, replay_stream_id, COMMAND_TIMEOUT) {
            Ok(registration_id) => {
                self.replay_subscription_add = Some(registration_id);
                self.transition(State::AwaitReplaySubscription);
            }
            Err(error) => {
                self.fail(SubscriptionError::Client(error));
            }
        }

        1
    }

    /// `AWAIT_REPLAY_SUBSCRIPTION` (`:1519-1583`).
    fn await_replay_subscription(&mut self) -> usize {
        let Some(registration_id) = self.replay_subscription_add else {
            return 0;
        };

        let outcome = match self.client.client_mut() {
            Some(driver) => driver.async_add_poll(AsyncAdd::subscription(registration_id)),
            None => AsyncAddPoll::Awaiting,
        };

        match outcome {
            AsyncAddPoll::Awaiting | AsyncAddPoll::Unknown => 0,
            AsyncAddPoll::Ready => {
                self.replay_subscription_add = None;
                self.replay_subscription = Some(registration_id);

                match self.replay_channel_type {
                    ReplayChannelType::SessionSpecific => {
                        self.deadline = Some(Instant::now() + self.message_timeout);
                        self.transition(State::Replay);
                    }
                    ReplayChannelType::DynamicPort => {
                        self.transition(State::AwaitReplayChannelEndpoint);
                    }
                    // A response channel has no endpoint to resolve — the replay
                    // is addressed to a publication, not to an address — so the
                    // next thing is the publication that carries the request
                    // (`:1574-1577`).
                    ReplayChannelType::ResponseChannel => {
                        self.transition(State::AddRequestPublication);
                    }
                }

                1
            }
            AsyncAddPoll::Failed(error) => {
                self.replay_subscription_add = None;
                self.clean_up_replay_subscription();
                self.fail(SubscriptionError::Client(error));

                1
            }
        }
    }

    /// `AWAIT_REPLAY_CHANNEL_ENDPOINT` (`:1585-1601`): the driver picked a port
    /// for `endpoint=…:0`, and the archive has to be told which one.
    fn await_replay_channel_endpoint(&mut self) -> usize {
        let Some(registration_id) = self.replay_subscription else {
            return 0;
        };

        let Some(driver) = self.client.client_mut() else {
            return 0;
        };

        let resolved = driver
            .subscription(registration_id)
            .and_then(|subscription| {
                driver
                    .counters_reader()
                    .and_then(|counters| subscription.resolved_endpoint(&counters))
            });

        let Some(resolved) = resolved else {
            // The endpoint's status counter is not active yet, so the port is
            // not known. The reference polls until it is (`:1585-1601`).
            return 0;
        };

        let Ok(mut uri) = ChannelUri::parse(&self.replay_channel_uri) else {
            self.fail(SubscriptionError::Refused {
                request: "replay channel",
                message: format!(
                    "replay channel is not a channel: {}",
                    self.replay_channel_uri
                ),
            });

            return 1;
        };

        uri.put("endpoint", resolved);
        self.replay_channel_uri = uri.build();

        self.transition(State::SendReplayRequest);

        1
    }

    /// `ADD_REQUEST_PUBLICATION` (`:1602-1657`).
    ///
    /// The publication the replay request goes out on when the replay channel is
    /// a **response channel**. It is built on the archive's own control request
    /// channel — the one it is listening on — with four things taken off and
    /// three put on (`:1613-1621`): the client's replay subscription is named as
    /// what this publication answers (`response-correlation-id`), the term
    /// length is the archive's control term length, and
    /// `spies-simulate-connection` is **off**, because this publication has one
    /// peer and is not a spy's view of a stream.
    ///
    /// ★ The four removals are the part that reads as a no-op and is not: the
    /// reference passes a **null value** to each, and a null value in a URI
    /// string builder *removes* the key rather than writing an empty one
    /// (`aeron_uri_string_builder.c:180-183`). Leaving `session-id` in place
    /// would make this publication claim to continue the archive's control
    /// channel's session, and leaving `term-id`/`term-offset` would make it
    /// claim a position in it.
    fn add_request_publication(&mut self) -> usize {
        // Taken out and the borrow dropped before anything below needs `self`
        // mutably.
        let control = self.context.archive_context.as_ref().map(|archive| {
            (
                archive.control_request_channel.clone(),
                archive.control_request_stream_id,
                archive.control_term_buffer_length,
            )
        });

        let Some((Some(channel), stream_id, term_length)) = control else {
            self.fail(SubscriptionError::Refused {
                request: "request publication",
                message: "the archive context names no control request channel".to_owned(),
            });

            return 1;
        };

        let Some(subscription_id) = self.replay_subscription else {
            return 0;
        };

        let Ok(channel) = request_publication_channel(&channel, term_length, subscription_id)
        else {
            self.fail(SubscriptionError::Refused {
                request: "request publication",
                message: format!("control request channel is not a channel: {channel}"),
            });

            return 1;
        };

        // **Started and not waited on**, like the replay subscription's add and
        // for the same reason: a poll that blocked would make a caller's turn as
        // long as the driver's answer.
        let added = match self.client.client_mut() {
            Some(driver) => {
                driver.async_add_exclusive_publication(&channel, stream_id, COMMAND_TIMEOUT)
            }
            None => Err(CommandError::Encoding),
        };

        match added {
            Ok(add) => {
                self.request_publication_add = Some(add);
                self.transition(State::AwaitRequestPublication);
            }
            Err(error) => self.fail(SubscriptionError::Client(error)),
        }

        1
    }

    /// `AWAIT_REQUEST_PUBLICATION` (`:1659-1727`).
    ///
    /// When the driver has made the publication, the client builds a proxy over
    /// it and **tells it the session the archive named** — the same session the
    /// async client was given at connect (`:1721-1723`) — because a request sent
    /// on this publication has to look like it comes from the same client the
    /// connect came from, and a fresh publication would otherwise carry nothing
    /// that says so.
    ///
    /// ★ **One deviation, and it is the one `AWAIT_LIVE` already records**: a
    /// `RESOURCE_TEMPORARILY_UNAVAILABLE` here is retried by the reference, by
    /// going back to `set_up_replay` (`:1680-1691`), and anything else is
    /// terminal. This build's [`CommandError`] does not carry *which* kind of
    /// refusal it was, so every failure is terminal — retrying all of them would
    /// spin forever on a permanent one, and a live-lock tells a caller less than
    /// a failure does.
    fn await_request_publication(&mut self) -> usize {
        let Some(add) = self.request_publication_add else {
            return 0;
        };

        let outcome = match self.client.client_mut() {
            Some(driver) => driver.async_add_poll(add),
            None => AsyncAddPoll::Awaiting,
        };

        match outcome {
            AsyncAddPoll::Awaiting | AsyncAddPoll::Unknown => 0,

            AsyncAddPoll::Failed(error) => {
                self.request_publication_add = None;
                self.clean_up_replay_subscription();
                self.fail(SubscriptionError::Client(error));

                1
            }

            AsyncAddPoll::Ready => {
                let registration_id = add.registration_id();
                self.request_publication_add = None;
                self.request_publication = Some(registration_id);

                let Some(archive_context) = self.context.archive_context.clone() else {
                    self.fail(SubscriptionError::Refused {
                        request: "request publication",
                        message: "the archive context names no control request channel".to_owned(),
                    });

                    return 1;
                };

                let mut proxy = ArchiveProxy::new(&archive_context, registration_id);
                proxy.set_control_session_id(self.client.control_session_id());

                self.response_channel_proxy = Some(proxy);
                self.transition(State::SendReplayTokenRequest);

                1
            }
        }
    }

    /// `SEND_REPLAY_TOKEN_REQUEST` (`:1729-1758`).
    fn send_replay_token_request(&mut self) -> usize {
        let Ok(correlation_id) = self.next_correlation_id() else {
            return 0;
        };

        let recording_id = self.context.recording_id;

        if !self
            .client
            .try_send_replay_token_request(correlation_id, recording_id)
        {
            if self.client.is_connected() {
                return 0;
            }

            self.clean_up_request_publication();
            self.clean_up_replay_subscription();
            self.transition(State::AwaitArchiveConnection);

            return 1;
        }

        self.ops.borrow_mut().replay_token = ReplayToken {
            correlation_id,
            ..ReplayToken::default()
        };
        self.deadline = Some(Instant::now() + self.message_timeout);
        self.transition(State::AwaitReplayToken);

        1
    }

    /// `AWAIT_REPLAY_TOKEN` (`:1760-1816`).
    ///
    /// The token is what the replay request is addressed by, so it is kept and
    /// sent with it (`:1367`). Where the machine goes next is the one thing IPC
    /// decides: an IPC replay channel is subscribed to **before** the replay is
    /// asked for, so with one there is nothing left to resolve and the request
    /// can go; anything else has an endpoint the driver has to pick first
    /// (`:1806-1813`).
    fn await_replay_token(&mut self) -> usize {
        let (received, code, relevant_id, message) = {
            let token = &self.ops.borrow().replay_token;

            (
                token.received,
                token.code,
                token.relevant_id,
                token.error_message.clone(),
            )
        };

        if !received {
            if !self.deadline_passed() {
                return 0;
            }

            self.clean_up_request_publication();
            self.clean_up_replay_subscription();

            if self.client.is_connected() {
                self.set_up_replay();
            } else {
                self.transition(State::AwaitArchiveConnection);
            }

            return 1;
        }

        if Some(ControlResponseCode::OK) != code {
            self.clean_up_request_publication();
            self.clean_up_replay_subscription();

            // The reference's own words, and its shape: the archive's message
            // inside ours, and the **relevant id handed on as the code**
            // (`:1792-1799`), because that is where an `ERROR` response puts it.
            self.fail(SubscriptionError::Refused {
                request: "Replay token request",
                message: format!("Replay token request failed: {message}"),
            });

            return 1;
        }

        self.replay_token = relevant_id;

        if self.replay_channel_is_ipc {
            self.transition(State::SendReplayRequest);
        } else {
            self.transition(State::AwaitReplayChannelEndpoint);
        }

        1
    }

    /// `clean_up_request_publication` (`:700-723`): give back the publication a
    /// response-channel replay was asked over — **whether or not the driver ever
    /// made it**.
    ///
    /// The two halves are separate because they are separate in the client: an
    /// add whose answer has not been polled holds nothing, and is cancelled by
    /// the id it was made with; a publication that arrived is closed. Both are
    /// asked for by registration id, which is the one name the driver knows
    /// either of them by. The third thing is the proxy, which is this
    /// object's own and outlives neither.
    fn clean_up_request_publication(&mut self) {
        if let Some(add) = self.request_publication_add.take() {
            if let Some(driver) = self.client.client_mut() {
                let _ = driver.async_add_cancel(add);
            }
        }

        if let Some(registration_id) = self.request_publication.take() {
            if let Some(driver) = self.client.client_mut() {
                let _ = driver.remove_publication(registration_id, COMMAND_TIMEOUT);
            }
        }

        self.response_channel_proxy = None;
    }

    /// `REPLAY` (`:1876-2059`): read the replay, and decide when it has caught
    /// up.
    fn replay<H: ControlledHandler>(&mut self, handler: &mut H, fragment_limit: usize) -> usize {
        let Some(registration_id) = self.replay_subscription else {
            return 0;
        };

        // The same truncation the channel got, and for the same reason: the
        // image's session id is the thirty-two-bit one the driver was given.
        let session_id = self.replay_session_id as i32;

        let image_position = self
            .client
            .client_mut()
            .and_then(|driver| driver.subscription(registration_id))
            .and_then(|subscription| subscription.image_by_session_id(session_id))
            .map(deepmsg_client::image::Image::position);

        let Some(image_position) = image_position else {
            // The image has not appeared. The reference waits for it until the
            // deadline and then starts the replay over (`:1880-1903`).
            if self.deadline_passed() {
                self.clean_up_replay_subscription();
                self.set_up_replay();

                return 1;
            }

            return 0;
        };

        let fragments = {
            let Some(driver) = self.client.client_mut() else {
                return 0;
            };

            driver.poll_subscription_controlled(registration_id, fragment_limit, handler)
        };

        self.position = image_position;

        // `:1916-1957`: a live subscription asked for *during* a replay is
        // picked up here, because this is the state that asked for it. The
        // reference polls it in `REPLAY` rather than moving to a state of its
        // own — the two states that do that (`ADD_LIVE_SUBSCRIPTION`,
        // `AWAIT_LIVE`) are the *other* way into the live stream, the one a
        // subscription with nothing to replay takes.
        if self.live_subscription.is_none() {
            if let Some(registration_id) = self.live_subscription_add {
                let outcome = match self.client.client_mut() {
                    Some(driver) => driver.async_add_poll(AsyncAdd::subscription(registration_id)),
                    None => AsyncAddPoll::Awaiting,
                };

                match outcome {
                    AsyncAddPoll::Awaiting | AsyncAddPoll::Unknown => {}
                    AsyncAddPoll::Ready => {
                        self.live_subscription_add = None;
                        self.live_subscription = Some(registration_id);
                        self.set_live_image_deadline();
                    }
                    AsyncAddPoll::Failed(error) => {
                        self.live_subscription_add = None;
                        self.clean_up_replay_subscription();
                        self.fail(SubscriptionError::Client(error));

                        return 1;
                    }
                }
            }
        }

        // `:1959-1975`: with a live subscription up, this is no longer a replay
        // — it is an alignment. What the difference between the two positions
        // is gets counted, because it is the one number that says how far
        // behind the live stream this subscription was when it joined.
        if let Some(live_registration_id) = self.live_subscription {
            let live_position = self
                .client
                .client_mut()
                .and_then(|driver| driver.subscription(live_registration_id))
                .and_then(|subscription| subscription.images().first())
                .map(deepmsg_client::image::Image::position);

            if let Some(live_position) = live_position {
                self.set_join_difference(live_position - self.position);
                self.transition(State::AttemptSwitch);

                return 1;
            }

            if !self.live_image_deadline_breached && self.live_image_deadline_passed() {
                self.on_live_image_deadline_breached();
            }
        }

        // ★ **The gate that decides when this stops being a replay**
        // (`:1987-1996`): no live subscription yet, nothing being added, and the
        // recording has been caught up with. Note what is *not* asked here — a
        // transport count, which is the replay merge's question and not this
        // machine's.
        if self.live_subscription.is_none()
            && self.live_subscription_add.is_none()
            && self.max_recorded_position_is_caught_up(self.position)
            && !self.do_add_live_subscription()
        {
            self.clean_up_replay_subscription();

            return 1;
        }

        fragments
    }

    /// The `max_recorded_position` sub-machine (`:604-698`), which answers
    /// whether the replay has caught up with a recording that is still being
    /// written to.
    ///
    /// Three things about it are the reference's and each of them is why it is a
    /// sub-machine:
    ///
    /// * a request that has just been sent **never** counts as caught up
    ///   (`:604-620` always answers false), so the first turn after a request
    ///   costs a round trip rather than a wrong answer;
    /// * an answer is only good for the position the replay had when it was
    ///   *asked* — if the recording has moved on, the replay has to read what it
    ///   has before asking again (`:671-681`), which is what stops a busy
    ///   recording from being asked once per turn;
    /// * the margin is a quarter of the recording's term buffer
    ///   (`:1157-1159`), so "caught up" is "close enough that the live stream's
    ///   first message will arrive after the last replayed one".
    fn max_recorded_position_is_caught_up(&mut self, replayed_position: i64) -> bool {
        match self.max_recorded_position {
            MaxRecordedPosition::Idle => {
                self.request_max_recorded_position();

                false
            }
            MaxRecordedPosition::Awaiting { deadline, .. } => {
                let (received, code, max_recorded_position) = {
                    let op = &self.ops.borrow().max_recorded_position;

                    (op.received, op.code, op.max_recorded_position)
                };

                if received {
                    if Some(ControlResponseCode::OK) != code {
                        // Terminal, and not retried: an archive that refuses
                        // this request is an archive this subscription cannot
                        // reason about (`:634-644`).
                        self.fail(SubscriptionError::Refused {
                            request: "Get max recorded position request",
                            message: format!("the archive answered {code:?}"),
                        });

                        return false;
                    }

                    if replayed_position >= max_recorded_position - self.close_enough_threshold {
                        return true;
                    }

                    self.max_recorded_position = MaxRecordedPosition::RecheckRequired {
                        max_recorded_position,
                    };
                } else if Instant::now() >= deadline {
                    // The answer never came: ask again (`:660-663`).
                    self.max_recorded_position = MaxRecordedPosition::Idle;
                }

                false
            }
            MaxRecordedPosition::RecheckRequired {
                max_recorded_position,
            } => {
                if replayed_position >= max_recorded_position {
                    self.max_recorded_position = MaxRecordedPosition::Idle;
                }

                false
            }
        }
    }

    /// `request_max_position` (`:604-620`).
    fn request_max_recorded_position(&mut self) {
        let Ok(correlation_id) = self.next_correlation_id() else {
            return;
        };

        let recording_id = self.context.recording_id;

        if !self
            .client
            .try_send_max_recorded_position_request(correlation_id, recording_id)
        {
            // Not sent, so not waited for: the next turn tries again, which is
            // the same thing the reference does by leaving the sub-machine in
            // `REQUEST_MAX_POSITION`.
            return;
        }

        self.ops.borrow_mut().max_recorded_position = MaxPosition {
            correlation_id,
            ..MaxPosition::default()
        };
        self.max_recorded_position = MaxRecordedPosition::Awaiting {
            correlation_id,
            deadline: Instant::now() + self.message_timeout,
        };
    }

    /// `ADD_LIVE_SUBSCRIPTION` (`:2135-2144`).
    fn add_live_subscription(&mut self) -> usize {
        if self.do_add_live_subscription() {
            self.transition(State::AwaitLive);
        }

        1
    }

    /// `AWAIT_LIVE` (`:2146-2245`): wait for the live stream's image, and check
    /// the position it joined at against the one this subscription has read to.
    ///
    /// The three-way comparison is the reference's and each arm is a different
    /// answer to the same question — did the live stream start where this
    /// subscription left off (`:2202-2226`):
    ///
    /// * **behind**: the application would see a message twice and no alignment
    ///   can fix that, so it is terminal;
    /// * **ahead**: there is a gap this subscription has not read, so it goes
    ///   back for it — the same re-listing the healing edges use;
    /// * **equal**: this is where it was, and the live stream is its own from
    ///   here.
    fn await_live(&mut self) -> usize {
        if self.live_subscription.is_none() {
            let Some(registration_id) = self.live_subscription_add else {
                return 0;
            };

            let outcome = match self.client.client_mut() {
                Some(driver) => driver.async_add_poll(AsyncAdd::subscription(registration_id)),
                None => AsyncAddPoll::Awaiting,
            };

            match outcome {
                AsyncAddPoll::Awaiting | AsyncAddPoll::Unknown => return 0,
                AsyncAddPoll::Ready => {
                    self.live_subscription_add = None;
                    self.live_subscription = Some(registration_id);
                    self.set_live_image_deadline();
                }
                AsyncAddPoll::Failed(error) => {
                    // **A recorded deviation.** The reference retries a
                    // `RESOURCE_TEMPORARILY_UNAVAILABLE` and fails on anything
                    // else (`:2160-2171`); this build's `CommandError` does not
                    // carry which refusal it was, so every one of them is
                    // terminal. Retrying all of them would be a livelock on the
                    // permanent ones, and a livelock says less than a failure
                    // does.
                    self.live_subscription_add = None;
                    self.fail(SubscriptionError::Client(error));

                    return 1;
                }
            }
        }

        let Some(registration_id) = self.live_subscription else {
            return 0;
        };

        let live_position = self
            .client
            .client_mut()
            .and_then(|driver| driver.subscription(registration_id))
            .and_then(|subscription| subscription.images().first())
            .map(deepmsg_client::image::Image::position);

        let Some(live_position) = live_position else {
            if !self.live_image_deadline_breached && self.live_image_deadline_passed() {
                self.on_live_image_deadline_breached();
            }

            return 0;
        };

        self.advance_last_observed_live_position(live_position);

        if self.position >= 0 {
            if live_position < self.position {
                self.clean_up_live_subscription();
                self.fail(SubscriptionError::Descriptor(format!(
                    "ERROR - live stream joined at position {live_position} which is earlier than \
                     last seen position {}",
                    self.position
                )));

                return 1;
            }

            if live_position > self.position {
                self.refresh_recording_descriptor();

                return 1;
            }
        }

        self.position = live_position;
        self.set_join_difference(0);
        self.transition(State::Live);
        self.on_live_joined();

        1
    }

    /// `advance_last_observed_live_position` (`:1187-1195`): a high-water mark,
    /// never a step backwards — it is what keeps a replay from being started
    /// behind where the live stream has already been seen (`:1223`).
    fn advance_last_observed_live_position(&mut self, position: i64) {
        self.last_observed_live_position = self.last_observed_live_position.max(position);
    }

    /// `ATTEMPT_SWITCH` (`:2060-2133`): the replay has caught up and the live
    /// subscription is up, so the two are aligned and the live stream takes
    /// over.
    ///
    /// **The alignment is explicit**, which is what separates this machine from
    /// the replay merge: the merge deduplicates at the frame layer by trusting
    /// that a replay channel carries the live stream's session, and this asks
    /// instead. Two passes, one per side:
    ///
    /// 1. the **live** image is read until a message *past* where the replay has
    ///    got to arrives. That message is where the live stream would take over
    ///    (`next_live_position`), and it is left **unconsumed** — so it is not
    ///    lost, it is simply not read twice;
    /// 2. the **replay** image is read up to that position, delivering
    ///    everything before it as usual, and when the message *at* that position
    ///    arrives the replay stops — without consuming it, because the live
    ///    image is the one that will deliver it.
    ///
    /// Then the live stream takes over. If the two positions were already equal
    /// there is nothing to align and the switch is immediate (`:2072-2074`).
    fn attempt_switch<H: ControlledHandler>(
        &mut self,
        handler: &mut H,
        fragment_limit: usize,
    ) -> usize {
        let (Some(replay_registration_id), Some(live_registration_id)) =
            (self.replay_subscription, self.live_subscription)
        else {
            return 0;
        };

        let Some((replay_position, live_position)) = ({
            let driver = self.client.client_mut();

            driver.map(|driver| {
                let position = |registration_id: i64| {
                    driver
                        .subscription(registration_id)
                        .and_then(|subscription| subscription.images().first())
                        .map(deepmsg_client::image::Image::position)
                };

                (
                    position(replay_registration_id),
                    position(live_registration_id),
                )
            })
        }) else {
            return 0;
        };

        let (Some(replay_position), Some(live_position)) = (replay_position, live_position) else {
            return 0;
        };

        if replay_position == live_position {
            self.position = replay_position;
            self.set_join_difference(0);
            self.transition(State::Live);
            self.on_live_joined();

            return 1;
        }

        // 1. Where the live stream takes over, once per alignment.
        if NULL_VALUE == self.next_live_position {
            let mut probe = LiveCatchup {
                replay_position,
                found: None,
            };

            if let Some(driver) = self.client.client_mut() {
                driver.poll_subscription_controlled(
                    live_registration_id,
                    fragment_limit,
                    &mut probe,
                );
            }

            if let Some(next_live_position) = probe.found {
                self.next_live_position = next_live_position;
            }
        }

        // 2. The replay reads up to it, and stops there.
        let mut switch = ReplayCatchup {
            next_live_position: self.next_live_position,
            arrived: false,
            handler,
        };

        let fragments = match self.client.client_mut() {
            Some(driver) => driver.poll_subscription_controlled(
                replay_registration_id,
                fragment_limit,
                &mut switch,
            ),
            None => 0,
        };

        if switch.arrived {
            self.position = self.next_live_position;
            self.transition(State::Live);
            self.on_live_joined();
        }

        fragments
    }

    /// `do_add_live_subscription` (`:1818-1847`): let go of whatever live
    /// subscription there was and ask the driver for another.
    ///
    /// Answers whether the request went out. **Called from the catch-up gate
    /// without a state transition**: the machine stays in `REPLAY` and picks
    /// the add up from there (`:1916-1957`), which is why the same helper serves
    /// the `ADD_LIVE_SUBSCRIPTION` state with the live-start path.
    fn do_add_live_subscription(&mut self) -> bool {
        self.live_subscription = None;
        self.live_subscription_add = None;

        let channel = self.concluded.live_channel.clone();
        let stream_id = self.concluded.live_stream_id;

        let Some(driver) = self.client.client_mut() else {
            return false;
        };

        match driver.submit_add_subscription(&channel, stream_id, COMMAND_TIMEOUT) {
            Ok(registration_id) => {
                self.live_subscription_add = Some(registration_id);

                true
            }
            Err(error) => {
                self.fail(SubscriptionError::Client(error));

                false
            }
        }
    }

    /// `LIVE` (`:2247-2268`): read the live stream, and notice when it stops.
    fn live<H: ControlledHandler>(&mut self, handler: &mut H, fragment_limit: usize) -> usize {
        let Some(registration_id) = self.live_subscription else {
            return 0;
        };

        // ★ **The image is gone: the live stream stopped** (`:2255-2263`).
        //
        // The reference asks `aeron_image_is_closed(image)`; this build's
        // `Image` has no such flag — an image that can no longer be read is one
        // its subscription has let go of — so the question is whether the
        // subscription still has one. It had one when this state was entered,
        // which is what makes "no image now" mean closed rather than "not yet".
        //
        // What is remembered is where it got to, so that the replay which
        // follows resumes from there: the publisher stopping is not a message
        // lost, it is a stream that has to be re-read from the same place.
        let still_has_an_image = self
            .client
            .client_mut()
            .and_then(|driver| driver.subscription(registration_id))
            .is_some_and(|subscription| !subscription.images().is_empty());

        if !still_has_an_image {
            self.clean_up_live_subscription();
            self.refresh_recording_descriptor();
            self.on_live_left();

            return 1;
        }

        let Some(driver) = self.client.client_mut() else {
            return 0;
        };

        driver.poll_subscription_controlled(registration_id, fragment_limit, handler)
    }

    /// `refresh_recording_descriptor` (`:1181-1185`): go back to the beginning
    /// of the path, which is how this machine re-reads a recording whose
    /// descriptor has changed — or whose live stream has to be found again.
    ///
    /// Only the live subscription goes: the replay subscription and the position
    /// are what the re-listing is *for*, and the descriptor's own answer is what
    /// continues from where this left off.
    fn refresh_recording_descriptor(&mut self) {
        self.clean_up_live_subscription();
        self.transition(State::SendListRecordingRequest);
    }

    /// `on_live_left` (`:937-949`): count it, and tell the application.
    fn on_live_left(&mut self) {
        if let Some(driver) = self.client.client_mut() {
            if let Some(counters) = driver.counters_writable() {
                self.counters.note_live_left(&counters);
            }
        }

        if let Some(listener) = self.listener.as_mut() {
            listener.on_live_left();
        }
    }

    /// `set_live_image_deadline` (`:779-784`).
    fn set_live_image_deadline(&mut self) {
        self.live_image_deadline = Some(Instant::now() + self.message_timeout);
        self.live_image_deadline_breached = false;
    }

    /// Whether that deadline has passed.
    fn live_image_deadline_passed(&self) -> bool {
        self.live_image_deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
    }

    /// `on_live_image_deadline_breached` (`:786-798`): a **non-terminal** error,
    /// reported **once** — the latch is what stops a subscription that is
    /// waiting for an image from saying so on every turn.
    fn on_live_image_deadline_breached(&mut self) {
        self.live_image_deadline_breached = true;

        self.listener_on_error(&SubscriptionError::Descriptor(
            "No image became available on the live subscription within the message timeout."
                .to_owned(),
        ));
    }

    /// `on_live_joined` (`:921-935`): count it, and tell the application.
    fn on_live_joined(&mut self) {
        if let Some(driver) = self.client.client_mut() {
            if let Some(counters) = driver.counters_writable() {
                self.counters.note_live_joined(&counters);
            }
        }

        if let Some(listener) = self.listener.as_mut() {
            listener.on_live_joined();
        }
    }

    /// The application's listener, told about something that is not terminal.
    fn listener_on_error(&mut self, error: &SubscriptionError) {
        if let Some(listener) = self.listener.as_mut() {
            listener.on_error(error);
        }
    }

    /// **The archive went away** (`on_archive_disconnected`, `:804-829`): save
    /// where the replay had read to, let go of everything that belonged to that
    /// connection, and wait for another one.
    ///
    /// ★ **Three states are left alone, and each is a decision**:
    ///
    /// * `AWAIT_ARCHIVE_CONNECTION` has nothing to clean up — it is already
    ///   waiting;
    /// * `LIVE` **keeps reading**: the live subscription is the *driver's*, not
    ///   the archive's, so an archive that goes away does not stop the stream
    ///   the application is reading (`shouldContinueConsumingFromLiveWhileArchiveIsUnavailable`).
    ///   Nothing here asks the archive for anything, so nothing here needs it;
    /// * `ATTEMPT_SWITCH` is the same thing one step earlier — both images are
    ///   up and it is the driver that is delivering;
    /// * and `FAILED` is over.
    fn on_archive_disconnected(&mut self) {
        if matches!(
            self.state,
            State::AwaitArchiveConnection | State::AttemptSwitch | State::Live | State::Failed
        ) {
            return;
        }

        // Where the replay had read to, if it had. This is the position the
        // re-listing below will replay from, and it is why a disconnection the
        // application never hears about costs it no messages.
        let position = self
            .client
            .client_mut()
            .and_then(|driver| {
                let subscription = driver.subscription(self.replay_subscription?)?;

                subscription.image_by_session_id(self.replay_session_id as i32)
            })
            .map(deepmsg_client::image::Image::position);

        if let Some(position) = position {
            self.position = position;
        }

        self.clean_up_live_subscription();
        self.clean_up_request_publication();
        self.clean_up_replay();
        self.clean_up_replay_subscription();

        self.transition(State::AwaitArchiveConnection);
    }

    /// `clean_up_live_subscription` (`:747-777`): give the live subscription
    /// back to the driver, whichever way it was asked for.
    ///
    /// **Whichever way** is the part that is easy to get wrong: an add the driver
    /// has not answered yet is *cancelled*, and one that arrived is *removed*.
    /// Dropping the id of an add still in flight would leave the driver to make
    /// a subscription this machine no longer knows about — a leak with no name
    /// on it, and one nothing ever polls. The reference cancels
    /// (`aeron_async_add_subscription_cancel`, `:747-760`), and so does this,
    /// through the same call [`Self::clean_up_request_publication`] uses.
    fn clean_up_live_subscription(&mut self) {
        if let Some(registration_id) = self.live_subscription_add.take() {
            if let Some(driver) = self.client.client_mut() {
                let _ = driver.async_add_cancel(AsyncAdd::subscription(registration_id));
            }
        }

        if let Some(registration_id) = self.live_subscription.take() {
            if let Some(driver) = self.client.client_mut() {
                let _ = driver.remove_subscription(registration_id, COMMAND_TIMEOUT);
            }
        }
    }

    /// `transition` (`:72-86`): the state, the counter, and the one side effect
    /// that is easy to miss — **a terminal state closes the archive client**.
    fn transition(&mut self, state: State) {
        self.state = state;

        if let Some(driver) = self.client.client_mut() {
            if let Some(counters) = driver.counters_writable() {
                self.counters.set_state(&counters, state.as_counter_value());
            }
        }

        if matches!(state, State::Failed) {
            self.client.close();
        }
    }

    /// Fail, and say why: the state, the message the caller can read, and the
    /// listener (`on_terminal_error`, `:589-602`).
    fn fail(&mut self, error: SubscriptionError) {
        self.failure = Some(format!("{error}"));
        self.transition(State::Failed);

        if let Some(listener) = self.listener.as_mut() {
            listener.on_error(&error);
        }
    }

    /// `set_join_difference` (`:909-919`).
    fn set_join_difference(&mut self, value: i64) {
        self.join_difference = value;

        if let Some(driver) = self.client.client_mut() {
            if let Some(counters) = driver.counters_writable() {
                self.counters.set_join_difference(&counters, value);
            }
        }
    }

    /// `clean_up_replay` (`:741-760`): **tell the archive to stop replaying**,
    /// if a replay is what it is doing.
    ///
    /// The one cleanup here that is a request rather than a release: the replay
    /// is the *archive's* work, on the archive's side of the connection, and
    /// giving the local subscription back does not stop it. What the reference
    /// sends is a `stop_replay` naming the replay session id it was given
    /// (`:1446`), and it does not wait for an answer — the connection may be
    /// about to go away, which is exactly when this is called.
    ///
    /// A correlation id that cannot be drawn (no driver) skips the request and
    /// still clears the id: the machine has stopped replaying either way, and an
    /// id left behind would make the next cleanup try again.
    fn clean_up_replay(&mut self) {
        if NULL_VALUE == self.replay_session_id {
            return;
        }

        let replay_session_id = std::mem::replace(&mut self.replay_session_id, NULL_VALUE);

        if let Ok(correlation_id) = self.next_correlation_id() {
            let _ = self
                .client
                .try_send_stop_replay_request(correlation_id, replay_session_id);
        }
    }

    /// `clean_up_replay_subscription` (`:725-745`): give the subscription back
    /// to the driver, whichever way it was asked for — a pending add cancelled,
    /// one that arrived removed. See [`Self::clean_up_live_subscription`] for
    /// why the difference is worth the branch.
    fn clean_up_replay_subscription(&mut self) {
        if let Some(registration_id) = self.replay_subscription_add.take() {
            if let Some(driver) = self.client.client_mut() {
                let _ = driver.async_add_cancel(AsyncAdd::subscription(registration_id));
            }
        }

        if let Some(registration_id) = self.replay_subscription.take() {
            if let Some(driver) = self.client.client_mut() {
                let _ = driver.remove_subscription(registration_id, COMMAND_TIMEOUT);
            }
        }
    }

    /// Whether the deadline of whatever is being waited for has passed.
    ///
    /// The reference compares a nanosecond clock with `now - deadline >= 0`, so
    /// that a clock that wraps is handled by the signed difference (`:662` and
    /// seven more). An [`Instant`] cannot wrap, and this is the same question.
    fn deadline_passed(&self) -> bool {
        self.deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
    }

    /// The next correlation id, which comes from the **driver** and not from
    /// this client (`aeron_next_correlation_id`, `:1126`).
    fn next_correlation_id(&mut self) -> Result<i64, CommandError> {
        match self.client.client_mut() {
            Some(driver) => driver.next_correlation_id(),
            None => Err(CommandError::Encoding),
        }
    }
}

/// The **live** side of the alignment (`live_catchup_fragment_handler`,
/// `:2002-2017`): read the live image until a message past where the replay has
/// got to, and leave that one unconsumed.
///
/// The `ABORT` is the whole of it. The reference's note is worth keeping: this
/// handler does **not** deliver anything to the application — what it is looking
/// for is a position, not a message, and a live message delivered here would be
/// a message delivered twice.
struct LiveCatchup {
    /// Where the replay has read to.
    replay_position: i64,
    /// The first live position past it, if this poll found one.
    found: Option<i64>,
}

impl ControlledHandler for LiveCatchup {
    fn on_message(&mut self, message: Message<'_>) -> Action {
        if message.header.position <= self.replay_position {
            return Action::Continue;
        }

        self.found = Some(message.header.position);

        Action::Abort
    }
}

/// The **replay** side of the alignment
/// (`replay_catchup_controlled_fragment_handler`, `:2019-2037`): deliver what is
/// before the switch point, and stop at it.
struct ReplayCatchup<'a, H> {
    /// The live position the switch happens at, or [`NULL_VALUE`] when the live
    /// side has not found one yet — in which case this is an ordinary replay
    /// poll.
    next_live_position: i64,
    /// Whether the message at that position arrived, which is what the caller
    /// turns into the switch.
    arrived: bool,
    /// The application's own handler, which everything before the switch goes
    /// to.
    handler: &'a mut H,
}

impl<H: ControlledHandler> ControlledHandler for ReplayCatchup<'_, H> {
    fn on_message(&mut self, message: Message<'_>) -> Action {
        if NULL_VALUE != self.next_live_position
            && message.header.position == self.next_live_position
        {
            self.arrived = true;

            return Action::Abort;
        }

        self.handler.on_message(message)
    }
}

/// Which of the three shapes a replay channel is (`:1035-1050`).
///
/// The order of the tests is the reference's and it matters: `control-mode=`
/// wins over a wildcard endpoint, so a channel that says both is a response
/// channel.
fn replay_channel_type(channel: &str) -> ReplayChannelType {
    let Ok(uri) = ChannelUri::parse(channel) else {
        return ReplayChannelType::SessionSpecific;
    };

    if Some("response") == uri.get("control-mode") {
        return ReplayChannelType::ResponseChannel;
    }

    let wildcard = uri.media() == "udp"
        && uri
            .get("endpoint")
            .is_some_and(|endpoint| endpoint.len() >= 2 && endpoint.ends_with(":0"));

    if wildcard {
        ReplayChannelType::DynamicPort
    } else {
        ReplayChannelType::SessionSpecific
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `:1035-1050`, case for case: the response channel's test comes first, the
    /// wildcard port's second, and everything else names its session.
    #[test]
    fn the_three_replay_channel_shapes_are_told_apart_the_references_way() {
        for (channel, expected) in [
            ("aeron:ipc", ReplayChannelType::SessionSpecific),
            (
                "aeron:ipc?alias=replay|session-id=7",
                ReplayChannelType::SessionSpecific,
            ),
            (
                "aeron:udp?endpoint=localhost:1234",
                ReplayChannelType::SessionSpecific,
            ),
            (
                "aeron:udp?endpoint=localhost:0",
                ReplayChannelType::DynamicPort,
            ),
            (
                "aeron:udp?control-mode=response|control=localhost:10002",
                ReplayChannelType::ResponseChannel,
            ),
            // Both at once: the response channel wins (`:1037-1040`).
            (
                "aeron:udp?endpoint=localhost:0|control-mode=response",
                ReplayChannelType::ResponseChannel,
            ),
            // A manual channel's endpoint is not one the driver replaces
            // (`aeron_subscription.c:660-672`), so it is not a dynamic port —
            // and the reference's test asks only for `:0`, which this is not.
            (
                "aeron:udp?endpoint=localhost:10",
                ReplayChannelType::SessionSpecific,
            ),
        ] {
            assert_eq!(expected, replay_channel_type(channel), "{channel}");
        }
    }

    /// The state counter's values are the enum's
    /// (`…_persistent_subscription.h:76-97`), and the enum is the reference's
    /// eighteen in its order.
    #[test]
    fn the_state_counter_reads_the_enums_own_values() {
        assert_eq!(0, State::AwaitArchiveConnection.as_counter_value());
        assert_eq!(7, State::AwaitReplayChannelEndpoint.as_counter_value());
        assert_eq!(8, State::AddRequestPublication.as_counter_value());
        assert_eq!(9, State::AwaitRequestPublication.as_counter_value());
        assert_eq!(10, State::SendReplayTokenRequest.as_counter_value());
        assert_eq!(11, State::AwaitReplayToken.as_counter_value());
        assert_eq!(12, State::Replay.as_counter_value());
        assert_eq!(17, State::Failed.as_counter_value());
    }

    /// ★ The four keys come **off** the control request channel and three go on
    /// (`:1613-1621`).
    ///
    /// The reference passes a **null** value to each of the four, and a null
    /// value in its URI string builder removes the key rather than writing an
    /// empty one (`aeron_uri_string_builder.c:180-183`) — which is why this is a
    /// function with a test rather than five lines inside a state, and why a
    /// port that read the nulls as empty values would be wrong in a way only a
    /// channel with those keys set can show.
    #[test]
    fn the_request_publication_channel_drops_what_the_reference_drops() {
        let channel = request_publication_channel(
            "aeron:udp?endpoint=localhost:8010|session-id=6|initial-term-id=3|term-id=4|term-offset=64",
            65536,
            33,
        )
        .expect("the control request channel is a channel");

        let uri = ChannelUri::parse(&channel).expect("what was built is a channel");

        for key in REQUEST_PUBLICATION_REMOVED_KEYS {
            assert_eq!(None, uri.get(key), "{channel} still carries {key}");
        }

        assert_eq!(Some("33"), uri.get(RESPONSE_CORRELATION_ID_KEY));
        assert_eq!(Some("65536"), uri.get(TERM_LENGTH_KEY));
        assert_eq!(Some("false"), uri.get(SPIES_SIMULATE_CONNECTION_KEY));
        assert_eq!("udp", uri.media());
        assert_eq!(Some("localhost:8010"), uri.get("endpoint"));

        // Media and endpoint survive; a control request channel that is not a
        // channel at all is the one failure the reference can have here too.
        assert!(request_publication_channel("not-a-channel", 65536, 33).is_err());
    }

    fn listener(ops: Rc<RefCell<Ops>>) -> ArchiveListener {
        ArchiveListener {
            ops,
            disconnected: Rc::new(Cell::new(false)),
            failure: Rc::new(RefCell::new(None)),
        }
    }

    fn descriptor(correlation_id: i64) -> RecordingDescriptor {
        RecordingDescriptor {
            control_session_id: 3,
            correlation_id,
            recording_id: 42,
            start_timestamp: 1,
            stop_timestamp: 2,
            start_position: 0,
            stop_position: 1024,
            initial_term_id: 1,
            segment_file_length: 1024 * 1024,
            term_buffer_length: 64 * 1024,
            mtu_length: 1408,
            session_id: 11,
            stream_id: 1001,
            stripped_channel: "aeron:ipc".to_owned(),
            original_channel: "aeron:ipc".to_owned(),
            source_identity: String::new(),
        }
    }

    /// One request in flight, counted as a send counts it (`:1145`).
    fn asked(correlation_id: i64) -> Rc<RefCell<Ops>> {
        let ops = Rc::new(RefCell::new(Ops::default()));
        ops.borrow_mut().list_recording = ListRecording {
            correlation_id,
            remaining: 1,
            ..ListRecording::default()
        };

        ops
    }

    /// ★ The **fourth** dispatcher (`:845-848`). A `list_recording` request can
    /// be answered with a control response — the archive refuses it with
    /// `ACTIVE_LISTING` whenever a listing of its own is already in flight, which
    /// is what asking faster than an answer comes back produces — and that
    /// answer has to be **seen** without being mistaken for the descriptor the
    /// numbers underneath it need.
    ///
    /// The shape this build had before: the answer was dropped here, and the
    /// subscription then waited out a whole message timeout for a reply it was
    /// already holding.
    #[test]
    fn a_refused_list_recording_is_an_answer_that_is_not_a_descriptor() {
        let ops = asked(7);
        let mut listener = listener(Rc::clone(&ops));

        listener.on_control_response(&ControlResponse {
            correlation_id: 7,
            relevant_id: 0,
            code: ControlResponseCode::ERROR,
            error_message: "active listing already in progress",
        });

        assert!(
            ops.borrow().list_recording.received,
            "a control response answering the request in flight was dropped on the floor"
        );
        assert_eq!(
            1,
            ops.borrow().list_recording.remaining,
            "an answer that is not a descriptor was counted as one"
        );
        assert!(
            PersistentSubscription::missing_descriptor(
                ops.borrow().list_recording.remaining,
                ops.borrow().list_recording.correlation_id
            )
            .is_err(),
            "a refused request must not pass the descriptor check"
        );
    }

    /// The other half of the same split: the descriptor **is** what counts the
    /// answer in (`:877-879`), so the count has to move with it — an answer flag
    /// set without it would let the four numbers be read out of a struct that
    /// never received any.
    #[test]
    fn the_descriptor_is_what_counts_the_answer_in() {
        let ops = asked(7);
        let mut listener = listener(Rc::clone(&ops));

        listener.on_recording_descriptor(&descriptor(7));

        assert_eq!(
            0,
            ops.borrow().list_recording.remaining,
            "the descriptor is what the count was for"
        );
        assert!(ops.borrow().list_recording.received);
        assert!(PersistentSubscription::missing_descriptor(0, 42).is_ok());
    }

    /// The message is the reference's own (`:1292-1306`), wrong as it reads for
    /// the refusal that reaches it — the same `ACTIVE_LISTING` answer the
    /// archive sends, reported as an unknown recording id. Copied on purpose.
    #[test]
    fn a_request_with_no_descriptor_fails_the_reference_way() {
        let error = PersistentSubscription::missing_descriptor(1, 42)
            .expect_err("a request whose descriptor never came is not usable");

        assert_eq!("unknown recording id: 42", format!("{error}"));
        assert!(PersistentSubscription::missing_descriptor(0, 42).is_ok());
    }
}
