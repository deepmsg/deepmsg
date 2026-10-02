//! What a channel URI says about the publication it names.
//!
//! Mirrors `aeron_diver_uri_publication_params`
//! (`aeron-driver/src/main/c/uri/aeron_driver_uri.c:218-440`) together with the
//! readers it calls (`:24-205`). (M20)
//!
//! The shape of the work is *defaults, then overrides, then validation*: every
//! field starts from the driver's configuration and a URI parameter replaces
//! it, with the checks running where the reference runs them — which matters,
//! because two of the checks read a field another parameter may already have
//! changed. `pub-wnd` is validated against the final `mtu` and `term-length`
//! (`:280-302`), so a URI that sets both is checked against what it asked for
//! rather than against the driver's defaults.
//!
//! # What is not read
//!
//! The IPC path has no use for the UDP-only parameters (`endpoint`, `control`,
//! `ttl`, `so-sndbuf`, `rcvbuf`, `fc`, `gtag`, `cc`, `ats`, `nak-delay`,
//! `rcv-wnd`) and they are ignored here rather than refused, which is what the
//! reference does with them too: they reach the parser's generic parameter list
//! and the publication path never looks. `control-mode` and `tags` are read
//! *before* that list on an IPC channel (`aeron-client/src/main/c/uri/aeron_uri.c:195-220`),
//! and only `tags` has an effect on a publication.

use deepmsg_core::logbuffer::descriptor;

use crate::channel_uri::{ChannelUri, Transport, UriError, parse_base_zero};
use crate::config::{DriverConfig, InferableBoolean};

/// The parameter names this module reads
/// (`aeron-client/src/main/c/uri/aeron_uri.h:53-85`).
pub mod key {
    /// `session-id`: the session the publication is to be known by.
    pub const SESSION_ID: &str = "session-id";
    /// `tags`: `channel_tag,entity_tag`, the second half optional.
    pub const TAGS: &str = "tags";
    /// `linger`: how long a drained publication lingers before it goes.
    pub const LINGER: &str = "linger";
    /// `term-length`: the length of one term of the log buffer.
    pub const TERM_LENGTH: &str = "term-length";
    /// `max-resend`: retransmits before giving up on a term.
    pub const MAX_RESEND: &str = "max-resend";
    /// `mtu`: the largest frame, header included.
    pub const MTU: &str = "mtu";
    /// `pub-wnd`: how far ahead of the slowest reader a producer may run.
    pub const PUBLICATION_WINDOW: &str = "pub-wnd";
    /// `init-term-id`: the term id a resumed stream started at.
    pub const INITIAL_TERM_ID: &str = "init-term-id";
    /// `term-id`: the term a resumed stream resumes in.
    pub const TERM_ID: &str = "term-id";
    /// `term-offset`: the offset inside that term.
    pub const TERM_OFFSET: &str = "term-offset";
    /// `sparse`: whether the log buffer is left for the file system to fill.
    pub const SPARSE: &str = "sparse";
    /// `eos`: whether the end-of-stream position is signalled when the
    /// publication goes.
    pub const EOS: &str = "eos";
    /// `ssc`: spies simulate a connection.
    pub const SPIES_SIMULATE_CONNECTION: &str = "ssc";
    /// `nak-delay`: how long the receiver waits before asking again for a
    /// gap, in place of the adaptive delays it would otherwise use
    /// (`aeron-client/src/main/c/uri/aeron_uri.h:82`).
    pub const NAK_DELAY: &str = "nak-delay";
    /// `cc`: the congestion-control strategy the receiver's image is to use
    /// (`aeron-client/src/main/c/uri/aeron_uri.h:70`, and the same name in
    /// Java: `CommonContext.CONGESTION_CONTROL_PARAM_NAME`,
    /// `aeron-client/src/main/java/io/aeron/CommonContext.java:386`).
    pub const CONGESTION_CONTROL: &str = "cc";
    /// `response-correlation-id`: which request this channel answers.
    pub const RESPONSE_CORRELATION_ID: &str = "response-correlation-id";
    /// `untethered-window-limit-timeout`.
    pub const UNTETHERED_WINDOW_LIMIT_TIMEOUT: &str = "untethered-window-limit-timeout";
    /// `untethered-linger-timeout`.
    pub const UNTETHERED_LINGER_TIMEOUT: &str = "untethered-linger-timeout";
    /// `untethered-resting-timeout`.
    pub const UNTETHERED_RESTING_TIMEOUT: &str = "untethered-resting-timeout";
    /// `reliable`: whether the channel is reliable. No effect on IPC.
    pub const RELIABLE: &str = "reliable";
    /// `tether`: whether the subscription keeps its position whatever it costs.
    pub const TETHER: &str = "tether";
    /// `rejoin`: whether this is a re-join of a stream.
    pub const REJOIN: &str = "rejoin";
    /// `control-mode`: `response` makes a channel a response channel.
    pub const CONTROL_MODE: &str = "control-mode";
    /// `group`: whether the channel is to be treated as one of a group
    /// (`AERON_URI_GROUP_KEY`, `aeron-client/src/main/c/uri/aeron_uri.h:66`).
    ///
    /// It is a **subscription** parameter: a publisher is told whether its
    /// channel has group semantics by the channel itself
    /// (`aeron_network_publication.c:136`, `has_group_semantics`), while a
    /// subscriber may be reading an implicitly-unicast source it knows several
    /// others are reading too. `aeron_driver_uri.c:494-495` reads it in
    /// `aeron_driver_uri_subscription_params` and nowhere else.
    pub const GROUP: &str = "group";
}

/// `AERON_URI_PROTOTYPE_VALUE_CORRELATION_ID`
/// (`aeron-driver/src/main/c/uri/aeron_driver_uri.h:24`): what
/// `response-correlation-id=prototype` means.
pub const PROTOTYPE_CORRELATION_ID: i64 = -2;

/// `AERON_MAX_UDP_PAYLOAD_LENGTH`
/// (`aeron-client/src/main/c/concurrent/aeron_logbuffer_descriptor.h:38`).
pub const MAX_UDP_PAYLOAD_LENGTH: u64 = 65504;

/// `AERON_RETRANSMIT_HANDLER_MAX_RESEND_MAX`
/// (`aeron-driver/src/main/c/aeron_retransmit_handler.h:43`).
pub const MAX_RESEND_MAX: u64 = 256;

/// Where a publication's stream resumes.
///
/// The reference models this as a `has_position` flag plus three fields whose
/// values are meaningless when it is clear. Here the three travel together or
/// not at all, which is the invariant the reference's own validation enforces
/// (`aeron_driver_uri.c:311-345`: the three parameters "must be used as a
/// complete set").
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StartingPosition {
    /// The term id the stream started at.
    pub initial_term_id: i32,
    /// The term it resumes in.
    pub term_id: i32,
    /// How far into that term.
    pub term_offset: i64,
}

/// Everything the driver needs to create a publication from a channel URI.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PublicationParams {
    /// The length of one term.
    pub term_length: i32,
    /// Whether the URI named the term length; see [`Self::mtu_length_named`].
    pub term_length_named: bool,
    /// The largest frame, its 32-byte header included.
    pub mtu_length: i32,
    /// Whether the URI *named* the MTU. A publication being shared has to
    /// agree about an MTU that was asked for and says nothing about one that
    /// was not (`aeron_confirm_publication_match`,
    /// `aeron-driver/src/main/c/aeron_driver_conductor.c:1126-1136`).
    pub mtu_length_named: bool,
    /// How far ahead of the slowest reader a producer may run.
    pub publication_window_length: i32,
    /// Retransmits before giving up on a term. Always zero for IPC — nothing
    /// is retransmitted over shared memory — but it is in the metadata either
    /// way.
    pub max_resend: i32,
    /// `tags`'s second half, or `-1` where the URI named none.
    pub entity_tag: i64,
    /// Which request this channel answers, or `-1`.
    pub response_correlation_id: i64,
    /// Whether this publication is the *answer* half of a request, which is
    /// what `control-mode=response` on the publication's channel means
    /// (`aeron_driver_uri.c:247-250`).
    ///
    /// It is the opposite of what `response_correlation_id` says: a publication
    /// that is a response one is the answer, and one that is not but names a
    /// correlation id is the *question* — the one whose `SETUP` asks for a
    /// response channel.
    pub is_response: bool,
    /// The session the URI asked for. `None` means "pick one" — the driver
    /// speculates from the sessions already in use on this stream.
    pub session_id: Option<i32>,
    /// How long a drained publication lingers.
    pub linger_timeout_ns: i64,
    /// How long a subscription may stall the publisher limit before the
    /// publication stops counting it.
    pub untethered_window_limit_timeout_ns: i64,
    /// The same, for the lingering half of the tether cycle.
    pub untethered_linger_timeout_ns: i64,
    /// And for the resting half.
    pub untethered_resting_timeout_ns: i64,
    /// Whether the log buffer is left sparse.
    pub is_sparse: bool,
    /// Whether the publication signals end of stream as it goes.
    pub signal_eos: bool,
    /// Whether spies are counted as connected.
    pub spies_simulate_connection: bool,
    /// Where the stream resumes, if the URI said.
    pub starting_position: Option<StartingPosition>,
    /// The term id the publication's first term is known by: the URI's when a
    /// position was given, otherwise random — which is what makes two
    /// publications of the same name from two drivers different streams.
    pub initial_term_id: i32,
}

/// What a channel URI says about the subscription it names.
///
/// Mirrors `aeron_driver_uri_subscription_params`
/// (`aeron-driver/src/main/c/uri/aeron_driver_uri.c:443-530`). Most of these
/// flags matter to the network transport rather than to IPC — a shared-memory
/// image is neither reliable nor unreliable, and it never re-joins — but the
/// link records them and the tether flag decides which of them a reader is
/// allowed to be put aside for being slow.
///
/// # The defaults that are not configuration yet
///
/// Four defaults come from the reference's *context* and are constants here:
/// [`RELIABLE_STREAM_DEFAULT`], [`TETHER_SUBSCRIPTIONS_DEFAULT`],
/// [`REJOIN_STREAM_DEFAULT`] and the initial window length. The URI can
/// override every one of them per subscription, which is the part that
/// affects what the driver does; turning them into settings of this driver
/// belongs with the transport that reads them (P1-4), not with the flags.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SubscriptionParams {
    /// The session to read, or `None` for "whatever publishes this stream".
    pub session_id: Option<i32>,
    /// Whether the subscription asked to keep its position whatever it costs.
    /// A tethered reader is never put to rest for reading slowly — and
    /// **true** is the default (`AERON_TETHER_SUBSCRIPTIONS_DEFAULT`).
    pub is_tether: bool,
    /// Whether this is a re-join of a stream the subscription left.
    pub is_rejoin: bool,
    /// Whether the channel is reliable. No effect on IPC.
    pub is_reliable: bool,
    /// Whether the log buffer is sparse. No effect on IPC: the *publication*
    /// decides that, and it is already created by the time anyone subscribes.
    pub is_sparse: bool,
    /// Whether this subscription exists to carry the answers to a request.
    pub is_response: bool,
    /// How long the reader may stall the publisher limit before it stops
    /// counting, and the two timeouts either side of it.
    pub untethered_window_limit_timeout_ns: i64,
    /// See [`Self::untethered_window_limit_timeout_ns`].
    pub untethered_linger_timeout_ns: i64,
    /// See [`Self::untethered_window_limit_timeout_ns`].
    pub untethered_resting_timeout_ns: i64,
    /// The delay a gap is asked for after, when the channel named one
    /// (`nak-delay=`); [`None`] leaves the receiver the driver's own delays.
    ///
    /// Read here rather than at the image because this is where a channel URI
    /// is read at all — and the image is built later, off another thread, from
    /// what the subscription left behind.
    pub nak_delay_ns: Option<i64>,
    /// The strategy the image is to use for the window it advertises
    /// (`cc=`). Resolved and checked here for the same reason
    /// [`Self::nak_delay_ns`] is: a value this build cannot serve is a
    /// subscription that must not be created, not an image that quietly
    /// behaves like a different one.
    pub congestion_control: CongestionControl,
    /// What `group=` said, or the driver's own consideration when it said
    /// nothing: whether this channel is to be treated as one of a group even
    /// when the channel itself gives no sign of it.
    ///
    /// Read here and carried on the subscription **link**, which is where the
    /// image looks for it (`subscription_link->group`,
    /// `aeron_driver_conductor.c:6702-6703`) — the image is created from a
    /// `SETUP`, long after the subscription that will read it.
    pub group: InferableBoolean,
}

/// The congestion-control strategies a channel may name with `cc=`
/// (`aeron_congestion_control.c:165-205`).
///
/// The reference has three arms — nothing named or `static` gives the static
/// window strategy, `cubic` gives cubic, and anything else **fails the supplier
/// with no error set at all** (`:178-204`, whose `result` stays `-1`). This
/// build carries only the first, so the other two are refused where the
/// reference would build something else.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CongestionControl {
    /// `AERON_STATICWINDOWCONGESTIONCONTROL_CC_PARAM_VALUE` — and what a
    /// channel that names nothing gets.
    Static,
}

/// `AERON_STATICWINDOWCONGESTIONCONTROL_CC_PARAM_VALUE`
/// (`aeron-driver/src/main/c/aeron_congestion_control.c`).
pub const CONGESTION_CONTROL_STATIC: &str = "static";

/// `AERON_CUBICCONGESTIONCONTROL_CC_PARAM_VALUE`.
pub const CONGESTION_CONTROL_CUBIC: &str = "cubic";

/// `AERON_RELIABLE_STREAM_DEFAULT` (`aeron-driver/src/main/c/aeron_driver_context.c:213`).
pub const RELIABLE_STREAM_DEFAULT: bool = true;

/// `AERON_TETHER_SUBSCRIPTIONS_DEFAULT` (`aeron_driver_context.c:214`).
pub const TETHER_SUBSCRIPTIONS_DEFAULT: bool = true;

/// `AERON_REJOIN_STREAM_DEFAULT` (`aeron_driver_context.c:228`).
pub const REJOIN_STREAM_DEFAULT: bool = true;

/// The `control-mode` value that makes a channel a response channel
/// (`AERON_UDP_CHANNEL_CONTROL_MODE_RESPONSE_VALUE`,
/// `aeron-client/src/main/c/uri/aeron_uri.h:50`).
pub const CONTROL_MODE_RESPONSE: &str = "response";

impl SubscriptionParams {
    /// What a subscription gets when its channel names nothing: the driver's
    /// own defaults, which are **not** `Default::default()` — a tethered
    /// subscription is the default (`AERON_TETHER_SUBSCRIPTIONS_DEFAULT`,
    /// true), and a derived `Default` would say the opposite. The same trap
    /// `MaxStrategy` had.
    pub const fn defaults(config: &DriverConfig) -> Self {
        Self {
            session_id: None,
            is_tether: TETHER_SUBSCRIPTIONS_DEFAULT,
            is_rejoin: REJOIN_STREAM_DEFAULT,
            is_reliable: RELIABLE_STREAM_DEFAULT,
            is_sparse: config.term_buffer_sparse_file,
            is_response: false,
            untethered_window_limit_timeout_ns: config.untethered_window_limit_timeout_ns,
            untethered_linger_timeout_ns: config.untethered_linger_timeout_ns,
            untethered_resting_timeout_ns: config.untethered_resting_timeout_ns,
            nak_delay_ns: None,
            congestion_control: CongestionControl::Static,
            group: config.receiver_group_consideration,
        }
    }

    /// Read a channel URI into the parameters a subscription is created from.
    ///
    /// # Errors
    ///
    /// [`PublicationParamsError`] for a parameter the reference refuses.
    pub fn resolve(
        uri: &ChannelUri<'_>,
        config: &DriverConfig,
    ) -> Result<Self, PublicationParamsError> {
        let mut params = Self {
            session_id: None,
            is_tether: TETHER_SUBSCRIPTIONS_DEFAULT,
            is_rejoin: REJOIN_STREAM_DEFAULT,
            is_reliable: RELIABLE_STREAM_DEFAULT,
            is_sparse: config.term_buffer_sparse_file,
            is_response: is_response_channel(uri),
            untethered_window_limit_timeout_ns: config.untethered_window_limit_timeout_ns,
            untethered_linger_timeout_ns: config.untethered_linger_timeout_ns,
            untethered_resting_timeout_ns: config.untethered_resting_timeout_ns,
            nak_delay_ns: None,
            congestion_control: CongestionControl::Static,
            group: config.receiver_group_consideration,
        };

        params.group =
            InferableBoolean::parse(uri.value(key::GROUP), config.receiver_group_consideration);

        if let Some(reliable) = uri.bool(key::RELIABLE)? {
            params.is_reliable = reliable;
        }
        if let Some(sparse) = uri.bool(key::SPARSE)? {
            params.is_sparse = sparse;
        }
        if let Some(tether) = uri.bool(key::TETHER)? {
            params.is_tether = tether;
        }
        if let Some(rejoin) = uri.bool(key::REJOIN)? {
            params.is_rejoin = rejoin;
        }

        // The subscribe side takes a plain number: there is no `tag:` form
        // here, because a subscription is not choosing a publication to
        // continue (`aeron_driver_uri.c:159-168`).
        params.session_id = uri.i32(key::SESSION_ID)?;

        // `nak-delay` is a duration, and it is one of the two things a receiver
        // tunes per channel (`aeron_publication_image.c:100-118`). An
        // unparseable one fails here, where the reference fails the *image*
        // later with `EINVAL` and leaves the subscription standing — a
        // difference in when a client hears about it, not in whether the
        // channel is served.
        params.nak_delay_ns = uri.duration_ns(key::NAK_DELAY)?;

        // `cc` is checked for the values this build cannot serve. Naming
        // nothing means the reference's own default, which is the static window
        // — the one strategy that is implemented here — so naming it is
        // accepted too, and everything else is refused rather than served as
        // something it is not.
        if let Some(named) = uri.value(key::CONGESTION_CONTROL) {
            if !named.starts_with(CONGESTION_CONTROL_STATIC) {
                return Err(PublicationParamsError::CongestionControl {
                    value: named.to_owned(),
                });
            }
        }

        if let Some(window_limit) = uri.duration_ns(key::UNTETHERED_WINDOW_LIMIT_TIMEOUT)? {
            params.untethered_window_limit_timeout_ns = window_limit;
        }
        if let Some(linger) = uri.duration_ns(key::UNTETHERED_LINGER_TIMEOUT)? {
            if linger > 0 {
                params.untethered_linger_timeout_ns = linger;
            } else if params.untethered_linger_timeout_ns == -1 {
                params.untethered_linger_timeout_ns = params.untethered_window_limit_timeout_ns;
            }
        } else if params.untethered_linger_timeout_ns == -1 {
            params.untethered_linger_timeout_ns = params.untethered_window_limit_timeout_ns;
        }
        if let Some(resting) = uri.duration_ns(key::UNTETHERED_RESTING_TIMEOUT)? {
            params.untethered_resting_timeout_ns = resting;
        }

        Ok(params)
    }
}

/// Whether a channel is a response channel: `control-mode=response`.
///
/// Read from the URI's own `control-mode` field rather than from the parameter
/// list, because an IPC channel parses that one key specially
/// (`aeron-client/src/main/c/uri/aeron_uri.c:196-199`).
fn is_response_channel(uri: &ChannelUri<'_>) -> bool {
    uri.value(key::CONTROL_MODE) == Some(CONTROL_MODE_RESPONSE)
}

/// Why a URI could not be turned into publication parameters.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PublicationParamsError {
    /// The URI itself could not be read.
    Uri(UriError),
    /// `term-length` is not a power of two in `[64 KiB, 1 GiB]`
    /// (`aeron_logbuffer_check_term_length`,
    /// `aeron-client/src/main/c/concurrent/aeron_logbuffer_descriptor.c:31-57`).
    TermLength {
        /// What the URI said.
        value: u64,
    },
    /// `mtu` is not a multiple of 32 in `(32, 65504]`
    /// (`aeron_driver_context_validate_mtu_length`,
    /// `aeron-driver/src/main/c/aeron_driver_context.c:1445-1462`).
    Mtu {
        /// What the URI said.
        value: u64,
    },
    /// `max-resend` is outside `1..=256` (`aeron_driver_uri.c:53-87`).
    MaxResend {
        /// What the URI said.
        value: u64,
    },
    /// `pub-wnd` is below the MTU or above half the term
    /// (`aeron_driver_uri.c:274-302`).
    PublicationWindow {
        /// What the URI said.
        value: u64,
        /// The MTU it was checked against — possibly also from the URI.
        mtu_length: i32,
        /// And the term length.
        term_length: i32,
    },
    /// `response-correlation-id` is below `-1` (`aeron_driver_uri.c:206-235`).
    ResponseCorrelationId {
        /// What the URI said.
        value: i64,
    },
    /// The three position parameters were not a usable set.
    Position(PositionError),
    /// `entity-tag` is not a decimal number (`aeron_driver_uri.c:256-273`).
    EntityTag {
        /// What the URI said.
        value: String,
    },
    /// `session-id=tag:N` names a network publication, and no publication in
    /// the driver carries that tag
    /// (`aeron_driver_uri.c:184-187`: `must reference a network publication`).
    UnknownSessionIdTag {
        /// The tag the URI named.
        tag: i64,
    },
    /// `cc=` named a congestion-control strategy this build does not carry.
    ///
    /// The reference serves `cubic` and fails silently on anything else
    /// (`aeron_congestion_control.c:165-205`); this build carries the static
    /// window only, so every other name is refused rather than served as
    /// something it is not.
    CongestionControl {
        /// What the URI said.
        value: String,
    },
}

/// What was wrong with a starting position.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PositionError {
    /// Fewer than all three of `init-term-id`, `term-id` and `term-offset`.
    Incomplete,
    /// `term-offset` is past the end of the term. The reference allows it to
    /// be exactly the term length (`aeron_driver_uri.c:329`), which is a
    /// boundary one byte past anything writable — reproduced rather than
    /// corrected, since it is a URI a reference driver accepts.
    TermOffsetOutOfRange {
        /// What the URI said.
        term_offset: u64,
        /// The term it is checked against.
        term_length: i32,
    },
    /// `term-offset` is not on a frame boundary.
    TermOffsetMisaligned {
        /// What the URI said.
        term_offset: u64,
    },
    /// The term ids are not in order: `term-id` is before `init-term-id`, or
    /// so far after it that the difference does not fit in an `i32`
    /// (`aeron_sub_wrap_i32`, `aeron-client/src/main/c/util/aeron_math.h:31-38`).
    TermIdsTooFarApart {
        /// The term the stream started at.
        initial_term_id: i32,
        /// The term it would resume in.
        term_id: i32,
    },
}

impl std::fmt::Display for PublicationParamsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Uri(error) => write!(f, "{error}"),
            Self::TermLength { value } => write!(
                f,
                "term-length={value} must be a power of two between {} and {}",
                descriptor::TERM_MIN_LENGTH,
                descriptor::TERM_MAX_LENGTH
            ),
            Self::Mtu { value } => write!(
                f,
                "mtu={value} must be a multiple of {} and at most {MAX_UDP_PAYLOAD_LENGTH}",
                descriptor::FRAME_ALIGNMENT
            ),
            Self::MaxResend { value } => {
                write!(
                    f,
                    "max-resend={value} must be between 1 and {MAX_RESEND_MAX}"
                )
            }
            Self::PublicationWindow {
                value,
                mtu_length,
                term_length,
            } => write!(
                f,
                "pub-wnd={value} must be at least the mtu={mtu_length} and at most half the \
                 term-length={term_length}"
            ),
            Self::ResponseCorrelationId { value } => write!(
                f,
                "response-correlation-id={value} must be a number at least -1, or `prototype`"
            ),
            Self::EntityTag { value } => write!(f, "entity tag `{value}` is not a number"),
            Self::UnknownSessionIdTag { tag } => write!(
                f,
                "session-id=tag:{tag} must reference a network publication"
            ),
            Self::CongestionControl { value } => write!(
                f,
                "cc={value} names a congestion control this driver does not serve; \
                 the only strategy it carries is `{CONGESTION_CONTROL_STATIC}`"
            ),
            Self::Position(error) => write!(f, "{error}"),
        }
    }
}

impl std::fmt::Display for PositionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Incomplete => write!(
                f,
                "{} {} {} must be used as a complete set",
                key::INITIAL_TERM_ID,
                key::TERM_ID,
                key::TERM_OFFSET
            ),
            Self::TermOffsetOutOfRange {
                term_offset,
                term_length,
            } => write!(
                f,
                "{}={term_offset} is past the {}={term_length}",
                key::TERM_OFFSET,
                key::TERM_LENGTH
            ),
            Self::TermOffsetMisaligned { term_offset } => write!(
                f,
                "{}={term_offset} must be a multiple of {}",
                key::TERM_OFFSET,
                descriptor::FRAME_ALIGNMENT
            ),
            Self::TermIdsTooFarApart {
                initial_term_id,
                term_id,
            } => write!(
                f,
                "{}={term_id} is not at or after {}={initial_term_id}",
                key::TERM_ID,
                key::INITIAL_TERM_ID
            ),
        }
    }
}

impl std::error::Error for PublicationParamsError {}

impl From<UriError> for PublicationParamsError {
    fn from(error: UriError) -> Self {
        Self::Uri(error)
    }
}

/// What a `session-id=tag:N` channel takes from the publication it names: the
/// session, and the two lengths that make the two publications the same shape
/// (`aeron_driver_uri.c:188-190`, which copies all three out of the
/// `aeron_network_publication_t`).
///
/// The lengths are copied rather than compared, and the flags that say the URI
/// *named* them are left alone: that is how two channels that share a session
/// come out identical enough for `aeron_confirm_publication_match` to let the
/// second one link to the first.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TaggedPublication {
    /// The session the publication runs under.
    pub session_id: i32,
    /// Its mtu.
    pub mtu_length: i32,
    /// The length of one of its terms.
    pub term_length: i32,
}

impl PublicationParams {
    /// Read a channel URI into the parameters a publication is created from.
    ///
    /// `config` supplies every default; the URI overrides the ones it names.
    /// `find_by_tag` answers what `session-id=tag:N` names — the reference
    /// hands the reader the whole conductor for this one question
    /// (`aeron_driver_uri.c:163`), so the lookup is a parameter here rather
    /// than a field.
    ///
    /// # Errors
    ///
    /// [`PublicationParamsError`] for a parameter the reference would refuse
    /// too, and for a URI this build cannot serve.
    pub fn resolve(
        uri: &ChannelUri<'_>,
        config: &DriverConfig,
        find_by_tag: impl Fn(i64) -> Option<TaggedPublication>,
    ) -> Result<Self, PublicationParamsError> {
        // The defaults, in the reference's order and from the same settings
        // (`aeron_driver_uri.c:224-254`). Three of them are chosen by
        // **transport**, which the reference spells as a ternary on every line
        // (`:230`, `:232`, `:295`): a network publication is sized like a
        // network publication, and one that names none of `term-length`,
        // `mtu=` or `pub-wnd` gets the network defaults rather than the IPC
        // ones. Reading them from the IPC settings for a UDP channel is a
        // 64 MiB term where the reference has 16 MiB — visible in the SETUP
        // frame and in the log file's own length.
        let is_ipc = uri.transport() == Transport::Ipc;
        let mut params = Self {
            term_length: if is_ipc {
                config.ipc_term_buffer_length
            } else {
                config.term_buffer_length
            },
            term_length_named: false,
            mtu_length: if is_ipc {
                config.ipc_mtu_length
            } else {
                config.mtu_length
            },
            mtu_length_named: false,
            publication_window_length: 0,
            // The reference leaves this at zero and reads `context->max_resend`
            // when the channel named none (`aeron_network_publication.c:144`),
            // so zero here would be a driver that never retransmits. The
            // setting is what the channel parameter overrides below.
            max_resend: config.max_resend,
            entity_tag: -1,
            response_correlation_id: -1,
            is_response: false,
            session_id: None,
            linger_timeout_ns: config.publication_linger_timeout_ns,
            untethered_window_limit_timeout_ns: config.untethered_window_limit_timeout_ns,
            untethered_linger_timeout_ns: config.untethered_linger_timeout_ns,
            untethered_resting_timeout_ns: config.untethered_resting_timeout_ns,
            is_sparse: config.term_buffer_sparse_file,
            signal_eos: true,
            spies_simulate_connection: config.spies_simulate_connection,
            starting_position: None,
            initial_term_id: 0,
        };

        params.read_session_id(uri, find_by_tag)?;
        params.read_entity_tag(uri)?;

        if let Some(linger) = uri.duration_ns(key::LINGER)? {
            params.linger_timeout_ns = linger;
        }
        if let Some(term_length) = uri.size(key::TERM_LENGTH)? {
            params.term_length = check_term_length(term_length)?;
            params.term_length_named = true;
        }
        if let Some(max_resend) = uri.size(key::MAX_RESEND)? {
            if !(1..=MAX_RESEND_MAX).contains(&max_resend) {
                return Err(PublicationParamsError::MaxResend { value: max_resend });
            }
            #[allow(clippy::cast_possible_truncation)] // bounded by MAX_RESEND_MAX
            {
                params.max_resend = max_resend as i32;
            }
        }
        if let Some(mtu) = uri.size(key::MTU)? {
            params.mtu_length = check_mtu(mtu)?;
            params.mtu_length_named = true;
        }

        // The window comes from the *final* term length, and the URI's own
        // `pub-wnd` is then checked against the final MTU and term length
        // (`aeron_driver_uri.c:280-302`).
        params.publication_window_length = producer_window_length(
            if is_ipc {
                config.ipc_publication_window_length
            } else {
                config.publication_window_length
            },
            params.term_length,
        );
        if let Some(window) = uri.size(key::PUBLICATION_WINDOW)? {
            if window < u64::try_from(params.mtu_length).unwrap_or(u64::MAX)
                || window > u64::try_from(params.term_length >> 1).unwrap_or(u64::MAX)
            {
                return Err(PublicationParamsError::PublicationWindow {
                    value: window,
                    mtu_length: params.mtu_length,
                    term_length: params.term_length,
                });
            }
            #[allow(clippy::cast_possible_truncation)] // bounded by half a term length
            {
                params.publication_window_length = window as i32;
            }
        }

        params.starting_position = read_position(uri, params.term_length)?;
        params.initial_term_id = match params.starting_position {
            Some(position) => position.initial_term_id,
            None => crate::sys::random_i32(),
        };

        if let Some(is_sparse) = uri.bool(key::SPARSE)? {
            params.is_sparse = is_sparse;
        }
        if let Some(signal_eos) = uri.bool(key::EOS)? {
            params.signal_eos = signal_eos;
        }
        if let Some(spies) = uri.bool(key::SPIES_SIMULATE_CONNECTION)? {
            params.spies_simulate_connection = spies;
        }
        params.response_correlation_id = read_response_correlation_id(uri)?;
        params.is_response = is_response_channel(uri);

        if let Some(window_limit) = uri.duration_ns(key::UNTETHERED_WINDOW_LIMIT_TIMEOUT)? {
            params.untethered_window_limit_timeout_ns = window_limit;
        }
        // The linger timeout defaults to the window limit one, but only when
        // the driver's own setting says "unset" — `-1` in the reference
        // (`aeron_driver_uri.c:414-427`), whose `else if` is what makes a
        // configured value survive a URI that says nothing.
        if let Some(linger) = uri.duration_ns(key::UNTETHERED_LINGER_TIMEOUT)? {
            if linger > 0 {
                params.untethered_linger_timeout_ns = linger;
            } else if params.untethered_linger_timeout_ns == -1 {
                params.untethered_linger_timeout_ns = params.untethered_window_limit_timeout_ns;
            }
        } else if params.untethered_linger_timeout_ns == -1 {
            params.untethered_linger_timeout_ns = params.untethered_window_limit_timeout_ns;
        }
        if let Some(resting) = uri.duration_ns(key::UNTETHERED_RESTING_TIMEOUT)? {
            params.untethered_resting_timeout_ns = resting;
        }

        Ok(params)
    }

    /// `session-id`, which is a number, or `tag:N` naming a network
    /// publication whose session this one shares
    /// (`aeron_driver_uri.c:162-204`).
    ///
    /// `tag:` copies the publication's session **and its two lengths** — the
    /// mtu and the term — which is what makes a second channel that names a
    /// tag come out structurally identical to the first, and what
    /// `shouldMergeStreamsFromMultiplePublicationsWithSameParams` is built on.
    /// The lookup is the caller's because only the conductor has the
    /// publications (`aeron_driver_conductor_find_network_publication_by_tag`,
    /// `aeron_driver_conductor.h:764-778`).
    fn read_session_id(
        &mut self,
        uri: &ChannelUri<'_>,
        find_by_tag: impl Fn(i64) -> Option<TaggedPublication>,
    ) -> Result<(), PublicationParamsError> {
        let Some(value) = uri.value(key::SESSION_ID) else {
            return Ok(());
        };

        if let Some(tag) = value.strip_prefix("tag:") {
            // `strtoll(&session_id_str[4], &end_ptr, 0)`, and the reference
            // fails the whole parameter when `errno` is set or any character is
            // left over (`:166-178`) — the same base-zero reader every other
            // integer parameter goes through.
            let not_a_number = || {
                PublicationParamsError::Uri(UriError::NotANumber {
                    key: key::SESSION_ID.to_owned(),
                    value: value.to_owned(),
                })
            };

            let Some(number) = parse_base_zero(tag) else {
                return Err(not_a_number());
            };

            let tag = i64::try_from(number).map_err(|_| {
                PublicationParamsError::Uri(UriError::OutOfRange {
                    key: key::SESSION_ID.to_owned(),
                    value: value.to_owned(),
                })
            })?;

            let Some(publication) = find_by_tag(tag) else {
                return Err(PublicationParamsError::UnknownSessionIdTag { tag });
            };

            self.session_id = Some(publication.session_id);
            self.mtu_length = publication.mtu_length;
            self.term_length = publication.term_length;

            return Ok(());
        }

        self.session_id = uri.i32(key::SESSION_ID)?;

        Ok(())
    }

    /// `tags=a,b`: the second half is the entity tag
    /// (`aeron_driver_uri.c:256-273`).
    fn read_entity_tag(&mut self, uri: &ChannelUri<'_>) -> Result<(), PublicationParamsError> {
        let Some(tags) = uri.value(key::TAGS) else {
            return Ok(());
        };

        let Some((_, entity_tag)) = tags.split_once(',') else {
            // A channel tag alone says nothing about a publication.
            return Ok(());
        };

        if entity_tag.is_empty() {
            return Ok(());
        }

        self.entity_tag =
            entity_tag
                .parse::<i64>()
                .map_err(|_| PublicationParamsError::EntityTag {
                    value: entity_tag.to_owned(),
                })?;

        Ok(())
    }
}

/// `aeron_logbuffer_check_term_length`: a power of two in range.
pub(crate) fn check_term_length(term_length: u64) -> Result<i32, PublicationParamsError> {
    let in_range = (descriptor::TERM_MIN_LENGTH as u64..=descriptor::TERM_MAX_LENGTH as u64)
        .contains(&term_length);

    if !in_range || !term_length.is_power_of_two() {
        return Err(PublicationParamsError::TermLength { value: term_length });
    }

    #[allow(clippy::cast_possible_truncation)] // the range check is the bound
    Ok(term_length as i32)
}

/// `aeron_driver_context_validate_mtu_length`: above the frame header, at most
/// a UDP payload, and on a frame boundary.
pub(crate) fn check_mtu(mtu_length: u64) -> Result<i32, PublicationParamsError> {
    if mtu_length < descriptor::FRAME_ALIGNMENT as u64 + 1
        || mtu_length > MAX_UDP_PAYLOAD_LENGTH
        || mtu_length % descriptor::FRAME_ALIGNMENT as u64 != 0
    {
        return Err(PublicationParamsError::Mtu { value: mtu_length });
    }

    #[allow(clippy::cast_possible_truncation)] // bounded by MAX_UDP_PAYLOAD_LENGTH
    Ok(mtu_length as i32)
}

/// `aeron_producer_window_length` (`aeron-driver/src/main/c/aeron_driver_context.h:461-470`):
/// half the term, or the configured window when that is smaller and not zero.
///
/// A window *larger* than half a term is ignored rather than clamped, which is
/// the reference's `!= 0 && < window_length` condition.
pub fn producer_window_length(configured: i32, term_length: i32) -> i32 {
    let half = term_length / 2;

    if configured != 0 && configured < half {
        configured
    } else {
        half
    }
}

/// The `init-term-id` / `term-id` / `term-offset` triple
/// (`aeron_driver_uri.c:305-395`).
fn read_position(
    uri: &ChannelUri<'_>,
    term_length: i32,
) -> Result<Option<StartingPosition>, PublicationParamsError> {
    let initial_term_id = uri.i32(key::INITIAL_TERM_ID)?;
    let term_id = uri.i32(key::TERM_ID)?;
    let term_offset = uri.value(key::TERM_OFFSET);

    let named = usize::from(initial_term_id.is_some())
        + usize::from(term_id.is_some())
        + usize::from(term_offset.is_some());

    if named == 0 {
        return Ok(None);
    }
    if named < 3 {
        return Err(PublicationParamsError::Position(PositionError::Incomplete));
    }

    let (Some(initial_term_id), Some(term_id), Some(term_offset)) =
        (initial_term_id, term_id, term_offset)
    else {
        return Err(PublicationParamsError::Position(PositionError::Incomplete));
    };

    let term_offset: u64 = term_offset.parse().map_err(|_| {
        PublicationParamsError::Uri(UriError::NotANumber {
            key: key::TERM_OFFSET.to_owned(),
            value: term_offset.to_owned(),
        })
    })?;

    // A wrapping subtraction, which is what `aeron_sub_wrap_i32` is: the
    // difference's low 32 bits, negative when the term ids are the wrong way
    // round (`aeron-client/src/main/c/util/aeron_math.h:31-38`).
    if term_id.wrapping_sub(initial_term_id) < 0 {
        return Err(PublicationParamsError::Position(
            PositionError::TermIdsTooFarApart {
                initial_term_id,
                term_id,
            },
        ));
    }

    #[allow(clippy::cast_sign_loss)] // term lengths are positive
    if term_offset > term_length as u64 {
        return Err(PublicationParamsError::Position(
            PositionError::TermOffsetOutOfRange {
                term_offset,
                term_length,
            },
        ));
    }

    if term_offset % descriptor::FRAME_ALIGNMENT as u64 != 0 {
        return Err(PublicationParamsError::Position(
            PositionError::TermOffsetMisaligned { term_offset },
        ));
    }

    #[allow(clippy::cast_possible_wrap)] // bounded by a term length
    Ok(Some(StartingPosition {
        initial_term_id,
        term_id,
        term_offset: term_offset as i64,
    }))
}

/// `response-correlation-id`, including `prototype`
/// (`aeron_driver_uri.c:206-235`).
fn read_response_correlation_id(uri: &ChannelUri<'_>) -> Result<i64, PublicationParamsError> {
    let Some(value) = uri.value(key::RESPONSE_CORRELATION_ID) else {
        return Ok(-1);
    };

    if value == "prototype" {
        return Ok(PROTOTYPE_CORRELATION_ID);
    }

    let correlation_id: i64 = value.parse().map_err(|_| {
        PublicationParamsError::Uri(UriError::NotANumber {
            key: key::RESPONSE_CORRELATION_ID.to_owned(),
            value: value.to_owned(),
        })
    })?;

    if correlation_id < -1 {
        return Err(PublicationParamsError::ResponseCorrelationId {
            value: correlation_id,
        });
    }

    Ok(correlation_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::channel_uri::ChannelUri;
    use deepmsg_core::logbuffer::descriptor;

    /// A configuration with this build's defaults and a term length small
    /// enough for a test to talk about.
    fn config() -> DriverConfig {
        DriverConfig::default()
    }

    fn resolve(uri: &str) -> Result<PublicationParams, PublicationParamsError> {
        resolve_with_tag(uri, |_| None)
    }

    /// The same, for a URI whose `session-id=tag:N` names something — the
    /// lookup is the conductor's, so a test hands it what a conductor would
    /// find (`crate::network_publications::NetworkPublications::find_by_tag`).
    fn resolve_with_tag(
        uri: &str,
        find_by_tag: impl Fn(i64) -> Option<TaggedPublication>,
    ) -> Result<PublicationParams, PublicationParamsError> {
        let parsed = ChannelUri::parse(uri.as_bytes()).expect("the URI parses");
        PublicationParams::resolve(&parsed, &config(), find_by_tag)
    }

    fn resolve_ok(uri: &str) -> PublicationParams {
        resolve(uri).unwrap_or_else(|error| panic!("{uri}: {error}"))
    }

    /// `SubscriptionParams::resolve` for a channel, which the publication
    /// helper above does not cover.
    fn resolve_subscription(uri: &str) -> Result<SubscriptionParams, PublicationParamsError> {
        let parsed = ChannelUri::parse(uri.as_bytes()).expect("a URI");
        SubscriptionParams::resolve(&parsed, &config())
    }

    #[test]
    fn a_channels_nak_delay_is_read_or_left_to_the_driver() {
        let named = resolve_subscription("aeron:udp?endpoint=127.0.0.1:40123|nak-delay=2ms")
            .expect("a subscription");

        assert_eq!(
            Some(2_000_000),
            named.nak_delay_ns,
            "the duration the channel named, in nanoseconds"
        );

        let silent =
            resolve_subscription("aeron:udp?endpoint=127.0.0.1:40123").expect("a subscription");

        assert_eq!(
            None, silent.nak_delay_ns,
            "which leaves the receiver the driver's own delays"
        );
    }

    #[test]
    fn a_nak_delay_that_is_not_a_duration_is_refused_rather_than_ignored() {
        // The reference fails the *image* with `EINVAL` and leaves the
        // subscription standing (`aeron_publication_image.c:113-117`); this
        // refuses the subscription, which is the same answer a client can act
        // on, delivered earlier.
        let error = resolve_subscription("aeron:udp?endpoint=127.0.0.1:40123|nak-delay=soon")
            .expect_err("a duration that is not one");

        assert!(
            matches!(
                error,
                PublicationParamsError::Uri(UriError::NotANumber { .. })
            ),
            "{error}"
        );
    }

    #[test]
    fn a_congestion_control_this_driver_does_not_carry_is_refused() {
        // Three arms in the reference: nothing or `static` is the static window,
        // `cubic` is cubic, and anything else leaves its supplier's `result` at
        // `-1` with **no error set** (`aeron_congestion_control.c:165-205`) —
        // a client told nothing at all. This build carries the static window and
        // refuses the rest by name.
        let named_static = resolve_subscription("aeron:udp?endpoint=127.0.0.1:40123|cc=static")
            .expect("a subscription");
        assert_eq!(CongestionControl::Static, named_static.congestion_control);

        let silent =
            resolve_subscription("aeron:udp?endpoint=127.0.0.1:40123").expect("a subscription");
        assert_eq!(
            CongestionControl::Static,
            silent.congestion_control,
            "naming nothing is the reference's own default"
        );

        for uri in [
            "aeron:udp?endpoint=127.0.0.1:40123|cc=cubic",
            "aeron:udp?endpoint=127.0.0.1:40123|cc=nonsense",
        ] {
            let error = resolve_subscription(uri).expect_err("a strategy this build lacks");
            assert!(
                matches!(error, PublicationParamsError::CongestionControl { .. }),
                "{uri}: {error}"
            );
        }
    }

    #[test]
    fn a_bare_channel_gets_every_default() {
        let params = resolve_ok("aeron:ipc");

        assert_eq!(
            64 * 1024 * 1024,
            params.term_length,
            "aeron.ipc.term.buffer.length"
        );
        assert_eq!(1408, params.mtu_length, "aeron.ipc.mtu.length");
        assert_eq!(params.term_length / 2, params.publication_window_length);
        assert_eq!(5_000_000_000, params.linger_timeout_ns);
        assert_eq!(5_000_000_000, params.untethered_window_limit_timeout_ns);
        assert_eq!(
            5_000_000_000, params.untethered_linger_timeout_ns,
            "unset (-1) means the window limit timeout"
        );
        assert_eq!(10_000_000_000, params.untethered_resting_timeout_ns);
        assert!(
            params.is_sparse,
            "aeron.term.buffer.sparse.file defaults true"
        );
        assert!(params.signal_eos, "eos defaults true");
        assert!(!params.spies_simulate_connection);
        assert_eq!(-1, params.entity_tag);
        assert_eq!(-1, params.response_correlation_id);
        assert_eq!(None, params.session_id);
        assert_eq!(
            config().max_resend,
            params.max_resend,
            "a channel that names none gets the driver's `aeron.max.resend`, not zero: \
             the reference reads `context->max_resend` where the channel named none \
             (`aeron_network_publication.c:144`), and zero would be a publication \
             that never retransmits"
        );
        assert_eq!(None, params.starting_position);
    }

    #[test]
    fn a_network_channel_is_sized_like_a_network_channel() {
        // The reference chooses four defaults by *transport*, not once for both
        // (`aeron_driver_uri.c:230`, `:232`, `:295`): a UDP channel that names
        // no term, MTU or window gets the network settings. Reading them from
        // the IPC ones is a 64 MiB term where the reference has 16 MiB — which
        // is not an invisible number: the term length goes out in the SETUP
        // frame and is the length of the log file the receiver maps.
        let config = config();
        let params = resolve_ok("aeron:udp?endpoint=localhost:40456");

        assert_eq!(config.term_buffer_length, params.term_length);
        assert_eq!(config.mtu_length, params.mtu_length);
        assert_eq!(
            producer_window_length(config.publication_window_length, params.term_length),
            params.publication_window_length
        );

        // And the IPC side is unchanged: it is the one that was right.
        let ipc = resolve_ok("aeron:ipc");
        assert_eq!(config.ipc_term_buffer_length, ipc.term_length);
        assert_eq!(config.ipc_mtu_length, ipc.mtu_length);
    }

    #[test]
    fn the_parameters_a_uri_names_replace_the_defaults() {
        let params = resolve_ok(
            "aeron:ipc?session-id=1001|term-length=64k|mtu=1408|pub-wnd=16384|linger=1s|\
             max-resend=3|sparse=false|eos=false|ssc=true|tags=channel,7|untethered-resting-timeout=1s",
        );

        assert_eq!(Some(1001), params.session_id);
        assert_eq!(64 * 1024, params.term_length);
        assert_eq!(1408, params.mtu_length);
        assert_eq!(16384, params.publication_window_length);
        assert_eq!(1_000_000_000, params.linger_timeout_ns);
        assert_eq!(3, params.max_resend);
        assert!(!params.is_sparse);
        assert!(!params.signal_eos);
        assert!(params.spies_simulate_connection);
        assert_eq!(7, params.entity_tag);
        assert_eq!(1_000_000_000, params.untethered_resting_timeout_ns);
    }

    #[test]
    fn a_subscriptions_group_is_what_the_uri_says_and_otherwise_the_drivers_own() {
        use crate::config::InferableBoolean::{ForceFalse, ForceTrue, Infer};

        assert_eq!(
            Infer,
            resolve_subscription("aeron:udp?endpoint=127.0.0.1:40123")
                .expect("a subscription")
                .group
                .clone(),
            "a channel that names nothing gets the driver's consideration, \
             which is `infer` by default"
        );

        for (named, expected) in [
            ("group=true", ForceTrue),
            ("group=infer", Infer),
            ("group=false", ForceFalse),
            ("group=nonsense", ForceFalse),
        ] {
            let uri = format!("aeron:udp?endpoint=127.0.0.1:40123|{named}");
            assert_eq!(
                expected,
                resolve_subscription(&uri).expect("a subscription").group,
                "{named}"
            );
        }

        // And the consideration moves the default with it, which is the whole
        // reason it is a setting.
        let parsed = ChannelUri::parse(b"aeron:udp?endpoint=127.0.0.1:40123").expect("a URI");
        let mut config = config();
        config.receiver_group_consideration = ForceTrue;
        assert_eq!(
            ForceTrue,
            SubscriptionParams::resolve(&parsed, &config)
                .expect("a subscription")
                .group
        );
    }

    #[test]
    fn a_window_follows_the_term_length_it_is_computed_from() {
        // The default window is half the term, so a URI that shortens the term
        // moves the window with it — and `pub-wnd` is then checked against that
        // term rather than against the driver's default.
        let params = resolve_ok("aeron:ipc?term-length=64k");
        assert_eq!(32 * 1024, params.publication_window_length);

        // At the boundary: the MTU is the floor and half the term is the
        // ceiling.
        assert_eq!(
            1408,
            resolve_ok("aeron:ipc?term-length=64k|pub-wnd=1408").publication_window_length
        );
        assert_eq!(
            32 * 1024,
            resolve_ok("aeron:ipc?term-length=64k|pub-wnd=32k").publication_window_length
        );

        assert!(matches!(
            resolve("aeron:ipc?term-length=64k|pub-wnd=1407"),
            Err(PublicationParamsError::PublicationWindow { .. })
        ));
        assert!(matches!(
            resolve("aeron:ipc?term-length=64k|pub-wnd=64k"),
            Err(PublicationParamsError::PublicationWindow { .. })
        ));
    }

    #[test]
    fn a_term_length_must_be_a_power_of_two_in_range() {
        for bad in ["65536", "1048576", "1073741824"] {
            assert!(
                resolve(&format!("aeron:ipc?term-length={bad}")).is_ok(),
                "{bad}"
            );
        }

        // 65536k would be 64 MiB and fine — it is the *value* that has to be a
        // power of two, not the spelling — so the rejections are a non-power-of
        // -two either side of the range and one past its end.
        for bad in ["65535", "100000", "100m", "2147483648"] {
            assert!(
                matches!(
                    resolve(&format!("aeron:ipc?term-length={bad}")),
                    Err(PublicationParamsError::TermLength { .. })
                ),
                "{bad} should not be a term length"
            );
        }
    }

    #[test]
    fn an_mtu_must_be_on_a_frame_boundary_and_within_a_udp_payload() {
        assert_eq!(Ok(64), check_mtu(64));
        assert_eq!(Ok(65504), check_mtu(65504));

        for bad in [0, 32, 33, 65505, 1410] {
            assert!(
                matches!(check_mtu(bad), Err(PublicationParamsError::Mtu { .. })),
                "{bad} should not be an mtu"
            );
        }
    }

    #[test]
    fn the_position_triple_comes_as_a_set() {
        let params = resolve_ok("aeron:ipc?init-term-id=7|term-id=9|term-offset=64");
        let position = params.starting_position.expect("a position");
        assert_eq!(7, position.initial_term_id);
        assert_eq!(9, position.term_id);
        assert_eq!(64, position.term_offset);
        assert_eq!(7, params.initial_term_id, "the term id the log starts at");

        // Not a set.
        for partial in [
            "aeron:ipc?init-term-id=7",
            "aeron:ipc?term-id=9",
            "aeron:ipc?term-offset=64",
            "aeron:ipc?init-term-id=7|term-id=9",
        ] {
            assert!(
                matches!(
                    resolve(partial),
                    Err(PublicationParamsError::Position(PositionError::Incomplete))
                ),
                "{partial} is not a position"
            );
        }

        // A term id before the initial one, and an offset off the frame grid.
        assert!(matches!(
            resolve("aeron:ipc?init-term-id=9|term-id=7|term-offset=64"),
            Err(PublicationParamsError::Position(
                PositionError::TermIdsTooFarApart { .. }
            ))
        ));
        assert!(matches!(
            resolve("aeron:ipc?init-term-id=7|term-id=9|term-offset=33"),
            Err(PublicationParamsError::Position(
                PositionError::TermOffsetMisaligned { .. }
            ))
        ));
    }

    #[test]
    fn a_position_is_taken_from_the_uri_and_the_initial_term_id_is_not() {
        // Without a position the initial term id is random — two publications
        // of the same channel from two drivers are different streams — so the
        // only thing a test can say about it is that it is not the default.
        let without = resolve_ok("aeron:ipc");
        assert!(
            (0..8).any(|_| resolve_ok("aeron:ipc").initial_term_id != without.initial_term_id),
            "an initial term id that never differs is not random"
        );
    }

    #[test]
    fn the_remaining_readers_refuse_what_the_reference_refuses() {
        assert!(matches!(
            resolve("aeron:ipc?max-resend=0"),
            Err(PublicationParamsError::MaxResend { value: 0 })
        ));
        assert!(matches!(
            resolve("aeron:ipc?max-resend=257"),
            Err(PublicationParamsError::MaxResend { value: 257 })
        ));
        assert!(matches!(
            resolve("aeron:ipc?response-correlation-id=-2"),
            Err(PublicationParamsError::ResponseCorrelationId { value: -2 })
        ));
        assert_eq!(
            Ok(PROTOTYPE_CORRELATION_ID),
            read_response_correlation_id(
                &ChannelUri::parse(b"aeron:ipc?response-correlation-id=prototype").expect("parses")
            )
        );
        assert!(matches!(
            resolve("aeron:ipc?tags=channel,seven"),
            Err(PublicationParamsError::EntityTag { .. })
        ));
        assert!(matches!(
            resolve("aeron:ipc?session-id=tag:42"),
            Err(PublicationParamsError::UnknownSessionIdTag { tag: 42 })
        ));

        // A channel tag with no entity tag, and a trailing comma, are both
        // "no entity tag" rather than an error.
        assert_eq!(-1, resolve_ok("aeron:ipc?tags=channel").entity_tag);
        assert_eq!(-1, resolve_ok("aeron:ipc?tags=channel,").entity_tag);
    }

    /// `session-id=tag:N` takes the named publication's session **and its two
    /// lengths** (`aeron_driver_uri.c:188-190`), so a channel that names a tag
    /// comes out the same shape as the publication it names — which is what
    /// `shouldMergeStreamsFromMultiplePublicationsWithSameParams` leans on:
    /// two channels that disagree about mtu or term are two publications, not
    /// one shared stream.
    #[test]
    fn a_tagged_session_takes_the_publications_session_and_lengths() {
        let named = TaggedPublication {
            session_id: 7_777,
            mtu_length: 1_408,
            term_length: 1 << 20,
        };

        let params = resolve_with_tag(
            "aeron:udp?endpoint=127.0.0.1:40123|session-id=tag:5",
            |tag| (5 == tag).then_some(named),
        )
        .expect("the tag names a publication");

        assert_eq!(Some(7_777), params.session_id);
        assert_eq!(1_408, params.mtu_length);
        assert_eq!(1 << 20, params.term_length);

        // **Taken**, not named: the URI still says nothing about the lengths,
        // and that is what a publication being shared has to agree about
        // (`aeron_confirm_publication_match`, `aeron_driver_conductor.c:1126-1136`).
        assert!(!params.mtu_length_named);
        assert!(!params.term_length_named);

        // The tag is read before `mtu=` and `term-length=`
        // (`aeron_driver_uri.c:277-300`), so a URI that names them overrides
        // what the tag gave.
        let explicit = resolve_with_tag(
            "aeron:udp?endpoint=127.0.0.1:40123|session-id=tag:5|mtu=2048",
            |_| Some(named),
        )
        .expect("the tag names a publication");

        assert_eq!(2_048, explicit.mtu_length);
        assert!(explicit.mtu_length_named);
    }

    /// The two ways a tag can fail to name anything: no publication carries
    /// it, and the tag is not a number at all.
    #[test]
    fn a_tag_that_names_no_publication_is_refused() {
        let error = resolve("aeron:udp?endpoint=127.0.0.1:40123|session-id=tag:42")
            .expect_err("no publication carries the tag");

        assert_eq!(
            "session-id=tag:42 must reference a network publication",
            error.to_string(),
            "the reference's words (`aeron_driver_uri.c:184-187`)"
        );

        assert!(matches!(
            resolve("aeron:ipc?session-id=tag:seven"),
            Err(PublicationParamsError::Uri(_))
        ));

        // Read in base zero, as the reference's `strtoll(..., 0)` reads it, so
        // a tag written as hex is that tag (`aeron_driver_uri.c:166-178`).
        let params = resolve_with_tag("aeron:ipc?session-id=tag:0x10", |tag| {
            (16 == tag).then_some(TaggedPublication {
                session_id: 4,
                mtu_length: 1_408,
                term_length: 1 << 20,
            })
        })
        .expect("0x10 is sixteen");

        assert_eq!(Some(4), params.session_id);
    }

    #[test]
    fn the_producer_window_helper_ignores_a_window_larger_than_half_a_term() {
        assert_eq!(
            32 * 1024,
            producer_window_length(0, 64 * 1024),
            "zero means half"
        );
        assert_eq!(4096, producer_window_length(4096, 64 * 1024));
        assert_eq!(
            32 * 1024,
            producer_window_length(64 * 1024, 64 * 1024),
            "larger than half is ignored, not clamped"
        );
        assert_eq!(
            descriptor::TERM_MIN_LENGTH / 2,
            producer_window_length(0, descriptor::TERM_MIN_LENGTH)
        );
    }
}
