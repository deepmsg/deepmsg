//! Sender-side flow-control strategies (M10).
//!
//! A publication asks its strategy one question — how far may the producer
//! write? — and the strategy answers from what the receivers have told it. The
//! reference keeps the sender limit (`snd-lmt`) as a *single* value the
//! strategy returns (`aeron_network_publication_on_status_message`,
//! `aeron-driver/src/main/c/aeron_network_publication.c:779-840`), which is why
//! [the rule the stub recorded][`Strategy::on_sm`] holds here too: this is the
//! only place `snd-lmt` moves.
//!
//! # What this build carries
//!
//! `max`, which is the unicast default: a status message says how far its
//! receiver has read and how much room it has, and the sender limit becomes the
//! far edge of that window — never less than it already was
//! (`aeron_flow_control.c:108-127`). A multi-destination channel is given the
//! same `max` under that channel's own retransmit multiple; `rrwm:` is the one
//! option `fc=` parses, and [`strategy_for_channel`] is where the choice
//! between the two suppliers is made. The `min` strategy's admission gate,
//! `tagged`'s per-tag limits and the multicast variants are **not** served:
//! multicast is refused outright (`docs/compat.md`), and a `fc=` naming `min`
//! or `tagged` comes back as a strategy this build does not have rather than as
//! a malformed channel.
//!
//! # Names, and where they are read
//!
//! `fc=` names the strategy, and the reference resolves it through a symbol
//! table (`aeron_flow_control_strategy_supplier_load`,
//! `aeron_flow_control.c:74-79`) whose names are `max`, `min` and `tagged`
//! (`aeron_flow_control.h:24-26`). Options follow the name after a comma, and
//! are parsed by [`max_options`].

/// What a strategy answers with, every time it is asked.
///
/// Not a `Result`: a strategy that cannot advance the limit answers with the
/// limit it had, which is a state rather than a failure (ADR-0003).
pub type SenderLimit = i64;

/// The options `fc=max` accepts
/// (`aeron_flow_control_parse_max_options`, `aeron_flow_control.c:196-266`).
///
/// One option so far — how many receiver windows a retransmission may cover —
/// and it is parsed here so that a malformed option is refused at the channel
/// rather than ignored until the day it matters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MaxOptions {
    /// `rrwm:` — the retransmit receiver window multiple.
    pub rrwm: Option<usize>,
}

/// The options `fc=min` and `fc=tagged` accept
/// (`aeron_flow_control_parse_tagged_options`, `aeron_flow_control.c:487-676`).
///
/// The same comma-separated fields [`MaxOptions`] has, plus three: `g:` names
/// the group tag a status message has to carry to count, `g:<tag>/<size>` also
/// names how many receivers have to be present before the limit moves at all,
/// and `t:` how long a receiver may go quiet before it is dropped.
///
/// A field that is *not* one of these is refused rather than skipped
/// (`:658-670`), which is what makes a typo fail at the channel instead of at
/// the day it matters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TaggedOptions {
    /// `g:` — the group tag (`aeron_min_flow_control.c:353`). Unnamed leaves
    /// the strategy on the context's own tag.
    pub group_tag: Option<i64>,
    /// `g:<tag>/<size>` — the least number of matching receivers there have to
    /// be (`:246-249`). **Not** set by a `g:` without a slash: the reference
    /// only reads a size after one (`aeron_flow_control.c:582-610`).
    pub group_min_size: Option<i32>,
    /// `t:` — the receiver timeout (`:572-573`).
    pub timeout_ns: Option<i64>,
    /// `rrwm:` — the option `max` has too, and the same default.
    pub rrwm: Option<usize>,
}

/// Why a flow-control specification was refused
/// (`aeron_flow_control.c:243-262`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FlowControlError {
    /// A strategy this build does not have.
    UnknownStrategy(String),
    /// `fc=` with nothing before the first comma
    /// (`aeron_flow_control.c:425-431`).
    NoStrategyName,
    /// An option the strategy does not recognise.
    UnrecognisedOption(String),
    /// `rrwm:` with something that is not a positive number.
    InvalidOption(String),
    /// `g:` with digits followed by something that is neither the end of the
    /// field nor a slash (`aeron_flow_control.c:570-580`).
    InvalidGroup(String),
    /// A `g:<tag>/<size>` whose size is not a count (`:592-600`).
    InvalidGroupCount(String),
    /// `t:` with something that is not a duration (`:612-632`).
    InvalidTimeout(String),
    /// A numeric field longer than the reference's own buffer
    /// (`AERON_FLOW_CONTROL_NUMBER_BUFFER_LEN`, `:485`).
    NumberFieldTooLong(String),
}

impl std::fmt::Display for FlowControlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownStrategy(name) => {
                write!(f, "unknown flow control strategy: {name}")
            }
            Self::NoStrategyName => {
                write!(f, "No flow control strategy name specified")
            }
            Self::UnrecognisedOption(option) => write!(
                f,
                "Flow control options - unrecognised option, field: {option}"
            ),
            Self::InvalidOption(option) => write!(
                f,
                "Flow control options - invalid flow control retransmit receiver window \
                 multiple, field: {option}"
            ),
            Self::InvalidGroup(option) => {
                write!(f, "Flow control options - invalid group, field: {option}")
            }
            Self::InvalidGroupCount(option) => {
                write!(
                    f,
                    "Flow control options - Group count invalid, field: {option}"
                )
            }
            Self::InvalidTimeout(option) => {
                write!(f, "Flow control options - invalid timeout, field: {option}")
            }
            Self::NumberFieldTooLong(option) => write!(
                f,
                "Flow control options - number field too long, field: {option}"
            ),
        }
    }
}

impl std::error::Error for FlowControlError {}

/// A status message, as a strategy sees it
/// (`aeron_status_message_header_t`, plus the group tag that may follow it).
///
/// A strategy that keeps its own receivers keys them by `receiver_id` and dates
/// them by `now_ns`, so it needs more than the window the `max` strategy reads
/// — and the reference hands over the whole frame for exactly that reason.
#[derive(Clone, Copy, Debug)]
pub struct StatusMessage {
    /// Where the receiver has read to, already computed from the frame's term
    /// id and offset against the publication's initial term id — which is the
    /// caller's job (`aeron_logbuffer_compute_position`,
    /// `aeron_flow_control.c:120-124`).
    pub consumption_position: i64,
    /// How much room it has (`receiver_window`).
    pub receiver_window: i32,
    /// Who is reporting (`receiver_id`) — what a strategy that keeps receivers
    /// keys them by.
    pub receiver_id: i64,
    /// Which session and stream it is reading, which the reference passes on to
    /// the log hooks it calls when a receiver is added or dropped
    /// (`aeron_min_flow_control.c:228-239`, `:126-137`).
    pub session_id: i32,
    pub stream_id: i32,
    /// `AERON_STATUS_MESSAGE_HEADER_EOS_FLAG`: the receiver is leaving rather
    /// than reporting (`:175`).
    pub eos_flagged: bool,
    /// The group tag this message carried, if it carried one — the fixed
    /// eight bytes the reference reads by frame length
    /// (`aeron_udp_protocol.c:26-43`).
    pub group_tag: Option<i64>,
}

/// What a sender asks of its strategy
/// (`aeron_flow_control_strategy_t`, `aeron_flow_control.h:46-100`).
pub trait Strategy {
    /// A status message arrived: it carries the receiver's consumption
    /// position and its window, and the answer is the new sender limit.
    ///
    /// `now_ns` is the clock the caller works from, and a strategy that dates
    /// its receivers reads it here (`aeron_min_flow_control.c:159-166`).
    fn on_sm(
        &mut self,
        status_message: &StatusMessage,
        snd_lmt: SenderLimit,
        now_ns: i64,
    ) -> SenderLimit;

    /// Nothing arrived this pass (`aeron_max_flow_control_strategy_on_idle`,
    /// `:87-95`): the limit is unchanged, and asking is how a strategy gets to
    /// change its mind.
    fn on_idle(
        &mut self,
        now_ns: i64,
        snd_lmt: SenderLimit,
        snd_pos: i64,
        is_end_of_stream: bool,
    ) -> SenderLimit;

    /// How long a retransmission may be
    /// (`aeron_max_flow_control_strategy_max_retransmission_length`,
    /// `:156-166`).
    ///
    /// The receiver's window times the multiple, or what is left of the term,
    /// whichever is smaller — and never more than the NAK asked for.
    fn max_retransmission_length(
        &self,
        term_offset: usize,
        resend_length: usize,
        term_buffer_length: usize,
        initial_window_length: usize,
    ) -> usize;

    /// A `SETUP` frame that a status message asked for is about to go out
    /// (`aeron_network_publication.c:414-421`).
    ///
    /// The reference answers a status message with a `SETUP` only when one has
    /// been elicited, and it tells the strategy at that moment — which is how a
    /// strategy that gates on receivers gets to see the sender limit the setup
    /// was sent under (`aeron_min_flow_control.c:283-303`).
    fn on_setup(&mut self, now_ns: i64, snd_lmt: SenderLimit);

    /// A reader refused the stream (`aeron_network_publication.c:869`).
    ///
    /// What a `max` strategy has no use for: it keeps no receiver of its own.
    /// The strategies that gate on receivers mark this one as gone, so that
    /// the next pass drops it (`aeron_min_flow_control.c:305-324`).
    fn on_error(&mut self, receiver_id: i64);

    /// A status message asked for a `SETUP` frame
    /// (`aeron_network_publication_trigger_send_setup_frame`,
    /// `aeron_network_publication.h:243-267`).
    ///
    /// `group_tag` is what the asking status message carried, if it carried
    /// one — which is the only thing a tagged strategy reads it for
    /// (`aeron_min_flow_control.c:394-422`).
    fn on_trigger_send_setup(&mut self, group_tag: Option<i64>);

    /// Whether the receivers this strategy has seen are enough for the
    /// publication to call itself connected
    /// (`aeron_network_publication.c:760`).
    ///
    /// `true` for everything but a strategy that waits for a group, which is
    /// what the reference's own default answers
    /// (`aeron_flow_control_strategy_has_required_receivers_default`,
    /// `aeron_flow_control.c:76-79`).
    fn has_required_receivers(&self) -> bool;
}

/// The strategy a publication sends under — what `fc=` chooses
/// (`aeron_flow_control_strategy_t`, `aeron_flow_control.h:46-100`).
///
/// One value rather than a `Box<dyn Strategy>`: a strategy holds its own
/// receiver table, so it outlives any one call and cannot be built per use, and
/// the publication asks it on every pass — an allocation and a virtual call on
/// that path is what ADR-0003 rules out. The set is closed, so the enum is too.
#[derive(Debug)]
pub enum FlowControl {
    /// `fc=max` — and the default for a channel that names nothing, unicast or
    /// multicast (`aeron_driver_context.c:201`).
    Max(MaxStrategy),
}

impl Default for FlowControl {
    fn default() -> Self {
        Self::Max(MaxStrategy::default())
    }
}

impl Strategy for FlowControl {
    fn on_sm(
        &mut self,
        status_message: &StatusMessage,
        snd_lmt: SenderLimit,
        now_ns: i64,
    ) -> SenderLimit {
        match self {
            Self::Max(strategy) => strategy.on_sm(status_message, snd_lmt, now_ns),
        }
    }

    fn on_idle(
        &mut self,
        now_ns: i64,
        snd_lmt: SenderLimit,
        snd_pos: i64,
        is_end_of_stream: bool,
    ) -> SenderLimit {
        match self {
            Self::Max(strategy) => strategy.on_idle(now_ns, snd_lmt, snd_pos, is_end_of_stream),
        }
    }

    fn max_retransmission_length(
        &self,
        term_offset: usize,
        resend_length: usize,
        term_buffer_length: usize,
        initial_window_length: usize,
    ) -> usize {
        match self {
            Self::Max(strategy) => strategy.max_retransmission_length(
                term_offset,
                resend_length,
                term_buffer_length,
                initial_window_length,
            ),
        }
    }

    fn on_setup(&mut self, now_ns: i64, snd_lmt: SenderLimit) {
        match self {
            Self::Max(strategy) => strategy.on_setup(now_ns, snd_lmt),
        }
    }

    fn on_error(&mut self, receiver_id: i64) {
        match self {
            Self::Max(strategy) => strategy.on_error(receiver_id),
        }
    }

    fn on_trigger_send_setup(&mut self, group_tag: Option<i64>) {
        match self {
            Self::Max(strategy) => strategy.on_trigger_send_setup(group_tag),
        }
    }

    fn has_required_receivers(&self) -> bool {
        match self {
            Self::Max(strategy) => strategy.has_required_receivers(),
        }
    }
}

/// The `max` strategy: the sender may write to the far edge of every receiver's
/// window, and never moves backwards.
///
/// One implementation serves both the unicast and the multicast flavour in the
/// reference (`aeron_unicast_flow_control_strategy_state_t` and
/// `aeron_max_flow_control_strategy_state_t` are the same struct with the same
/// functions, `aeron_flow_control.c:30-40`); the multicast admission gate lives
/// in the delivery layer, not here.
#[derive(Clone, Copy, Debug)]
pub struct MaxStrategy {
    /// How many receiver windows a retransmission may cover
    /// (`retransmit_receiver_window_multiple`).
    pub retransmit_receiver_window_multiple: usize,
}

impl Default for MaxStrategy {
    /// The driver's own default multiple, **not** `usize::default()`: a
    /// strategy whose multiple is zero is one that answers every NAK with a
    /// zero-length retransmission, which is a driver that reports a loss it
    /// never retransmits. The derived default would be exactly that.
    fn default() -> Self {
        Self {
            retransmit_receiver_window_multiple: UNICAST_RRWM_DEFAULT,
        }
    }
}

/// The default retransmit receiver window multiple
/// (`AERON_UNICAST_FLOW_CONTROL_RETRANSMIT_RECEIVER_WINDOW_MULTIPLE`,
/// `aeron-driver/src/main/c/aeron_flow_control.h:31`, which the context starts
/// from at `aeron_driver_context.c:505`).
///
/// Sixteen receiver windows: a retransmission may cover far more than the
/// window itself, because the receiver asks for what it is missing and the
/// answer is bounded by the term rather than by the window most of the time.
pub const UNICAST_RRWM_DEFAULT: usize = 16;

/// The default retransmit receiver window multiple for a **multicast or
/// multi-destination** endpoint
/// (`AERON_MULTICAST_FLOW_CONTROL_RETRANSMIT_RECEIVER_WINDOW_MULTIPLE`,
/// `aeron-driver/src/main/c/aeron_flow_control.h:30`, which the context starts
/// from at `aeron_driver_context.c:504`).
///
/// Four receiver windows against unicast's sixteen. A multicast retransmission
/// goes to every receiver on the group, so it is held to what the receiver that
/// asked could hold rather than to what the term allows.
pub const MULTICAST_RRWM_DEFAULT: usize = 4;

/// The strategy an endpoint's channel asks for
/// (`aeron_default_multicast_flow_control_strategy_supplier`,
/// `aeron_flow_control.c:400-475`).
///
/// The selector is entered for **every** publication and takes the *endpoint's*
/// channel (`aeron_driver_conductor.c:4492-4496`), not the publication's. What
/// it branches on is whether that channel is multi-destination or multicast.
///
/// A **unicast** channel never looks at `fc=`: the unicast supplier is chosen
/// outright and the parameter is ignored
/// (`aeron_unicast_flow_control_strategy_supplier`, `:326-365`, which reads no
/// options at all). So a unicast channel naming `fc=min` is not refused for it —
/// it is served as though the parameter were not there. This build used to parse
/// `fc=` on every channel and answer `GENERIC_ERROR` for anything but `max`,
/// which is a refusal the reference does not make.
///
/// A **multi-destination** channel reads it, and the name before the first comma
/// picks the supplier: `max` is the only one this build has, so `min` and
/// `tagged` come back as strategies it does not have rather than as malformed
/// channels. No `fc=` at all falls to the context's multicast supplier, which is
/// `max` (`aeron_driver_context.c:201`).
///
/// # Errors
///
/// [`FlowControlError::NoStrategyName`] for an `fc=` with nothing before its
/// first comma, [`FlowControlError::UnknownStrategy`] for a name this build
/// cannot serve, and the option errors of [`MaxStrategy::from_options`].
pub fn strategy_for_channel(
    is_multi_destination: bool,
    fc: Option<&str>,
    unicast_rrwm: usize,
    multicast_rrwm: usize,
) -> Result<MaxStrategy, FlowControlError> {
    if !is_multi_destination {
        return Ok(MaxStrategy {
            retransmit_receiver_window_multiple: unicast_rrwm,
        });
    }

    let Some(options) = fc else {
        return Ok(MaxStrategy {
            retransmit_receiver_window_multiple: multicast_rrwm,
        });
    };

    match options.split(',').next().unwrap_or("") {
        "max" => MaxStrategy::from_options(multicast_rrwm, Some(options)),
        "" => Err(FlowControlError::NoStrategyName),
        name => Err(FlowControlError::UnknownStrategy(name.to_owned())),
    }
}

impl MaxStrategy {
    /// The strategy the reference builds from a `fc=max` channel: the driver's
    /// configured multiple unless the channel named one
    /// (`aeron_unicast_flow_control_strategy_supplier`,
    /// `aeron_flow_control.c:350-366`).
    pub fn from_options(
        configured_rrwm: usize,
        options: Option<&str>,
    ) -> Result<Self, FlowControlError> {
        let parsed = max_options(options)?;

        Ok(Self {
            retransmit_receiver_window_multiple: parsed.rrwm.unwrap_or(configured_rrwm),
        })
    }
}

impl Strategy for MaxStrategy {
    fn on_sm(
        &mut self,
        status_message: &StatusMessage,
        snd_lmt: SenderLimit,
        _now_ns: i64,
    ) -> SenderLimit {
        // `:108-127`: the window edge, and the limit never goes backwards.
        let window_edge = status_message
            .consumption_position
            .saturating_add(i64::from(status_message.receiver_window));

        snd_lmt.max(window_edge)
    }

    fn on_idle(
        &mut self,
        _now_ns: i64,
        snd_lmt: SenderLimit,
        _snd_pos: i64,
        _is_end_of_stream: bool,
    ) -> SenderLimit {
        // `:87-95`: there is nothing to learn from silence.
        snd_lmt
    }

    fn max_retransmission_length(
        &self,
        term_offset: usize,
        resend_length: usize,
        term_buffer_length: usize,
        initial_window_length: usize,
    ) -> usize {
        // `:134-146`: whichever is smaller — the rest of the term, or the
        // receiver's window times the multiple — and never more than what the
        // NAK asked for.
        let length_to_end_of_term = term_buffer_length.saturating_sub(term_offset);
        let receiver_window = receiver_window_length(initial_window_length, term_buffer_length);
        let estimated = receiver_window.saturating_mul(self.retransmit_receiver_window_multiple);

        resend_length.min(length_to_end_of_term.min(estimated))
    }

    /// `aeron_max_flow_control_strategy_on_setup` returns the limit it was
    /// given (`aeron_flow_control.c:91-101`), and the supplier ignores the
    /// answer (`aeron_network_publication.c:414-421`): a strategy with no
    /// receiver table has nothing to record about a setup.
    fn on_setup(&mut self, _now_ns: i64, _snd_lmt: SenderLimit) {}

    /// `aeron_max_flow_control_strategy_on_error` is empty
    /// (`aeron_flow_control.c:125-132`): a strategy that keeps no receivers has
    /// none to mark as gone.
    fn on_error(&mut self, _receiver_id: i64) {}

    /// And so is `aeron_max_flow_control_strategy_on_trigger_send_setup`
    /// (`:182-189`).
    fn on_trigger_send_setup(&mut self, _group_tag: Option<i64>) {}

    /// The default every supplier that does not install its own gets
    /// (`aeron_flow_control.c:76-79`).
    fn has_required_receivers(&self) -> bool {
        true
    }
}

/// The `min` strategy: the sender may write only as far as the **slowest**
/// receiver has read plus its window (`aeron_min_flow_control.c:159-258`).
///
/// Where `max` answers each status message on its own, this one keeps the
/// receivers that have reported and answers from all of them, which is what
/// makes it a *group* strategy: a sender under it waits for the reader that is
/// furthest behind, and — with a `group_min_size` — for there to be readers at
/// all.
#[derive(Debug)]
pub struct MinStrategy {
    /// The receivers that have reported (`receivers`), in the order they were
    /// first seen.
    receivers: Vec<MinReceiver>,
    /// How long a receiver may go quiet before it is dropped
    /// (`receiver_timeout_ns`).
    receiver_timeout_ns: i64,
    /// How many receivers have to have reported before the limit moves at all
    /// (`group_min_size`). Zero, the default, means the first one is enough.
    group_min_size: i32,
    /// The tag a status message has to carry to count (`group_tag`), or
    /// `None` for the strategy that counts every message: the reference's two
    /// names are one supplier with an `is_group_tag_aware` flag
    /// (`aeron_min_flow_control.c:515-634`), and the flag is exactly whether
    /// there is a tag to match against.
    group_tag: Option<i64>,
    /// Whether the receivers seen so far satisfy `group_min_size`, kept so the
    /// publication can ask without walking the table (`has_required_receivers`).
    has_required_receivers: bool,
    /// When the last elicited setup went out, and the limit it carried, which
    /// is what the sender may write until the first receiver answers
    /// (`last_setup_snd_lmt`).
    time_of_last_setup_ns: i64,
    last_setup_snd_lmt: SenderLimit,
    /// A status message with this strategy's tag asked for a setup since the
    /// last one was sent (`has_tagged_status_message_triggered_setup`): a setup
    /// sent for another reason does not open the gate.
    has_matching_status_message_triggered_setup: bool,
    /// How many receiver windows a retransmission may cover — the same option
    /// and the same default `max` has (`aeron_min_flow_control.c:598`).
    retransmit_receiver_window_multiple: usize,
}

/// One receiver of a [`MinStrategy`]
/// (`aeron_min_flow_control_strategy_receiver_t`, `aeron_min_flow_control.c:34-46`).
///
/// The reference also keeps the session, stream and registration id of each
/// receiver, for the log hooks it calls when one arrives or goes
/// (`aeron_min_flow_control.c:217-218`, `:228-239`) and for the receiver
/// counter's label (`:491-501`). This build has the default null hooks and no
/// counter yet, so they arrive with the counter.
#[derive(Clone, Copy, Debug)]
struct MinReceiver {
    receiver_id: i64,
    /// The furthest this receiver has said it has read. It only ever moves
    /// forwards, however the messages arrive (`:188`).
    last_position: i64,
    /// That position plus the window it offered, which is how far the sender
    /// may write for this receiver's sake.
    last_position_plus_window: i64,
    time_of_last_status_message_ns: i64,
    eos_flagged: bool,
}

impl MinStrategy {
    /// A strategy with no receivers yet, which is how one starts
    /// (`aeron_min_flow_control_strategy_supplier_init`, `:515-602`).
    pub fn new(
        receiver_timeout_ns: i64,
        group_min_size: i32,
        group_tag: Option<i64>,
        retransmit_receiver_window_multiple: usize,
    ) -> Self {
        Self {
            receivers: Vec::new(),
            receiver_timeout_ns,
            group_min_size,
            group_tag,
            has_required_receivers: false,
            time_of_last_setup_ns: 0,
            last_setup_snd_lmt: -1,
            has_matching_status_message_triggered_setup: false,
            retransmit_receiver_window_multiple,
        }
    }

    /// How many receivers have reported.
    pub fn receiver_count(&self) -> usize {
        self.receivers.len()
    }

    /// Whether a group tag counts for this strategy
    /// (`aeron_udp_protocol_group_tag`'s answer, `aeron_min_flow_control.c:353`).
    ///
    /// The plain strategy matches everything — including a message that carries
    /// no tag at all — and the tagged one matches only its own tag.
    fn tag_matches(&self, group_tag: Option<i64>) -> bool {
        self.group_tag.is_none_or(|tag| group_tag == Some(tag))
    }

    /// Whether the receivers satisfy `group_min_size`
    /// (`aeron_min_flow_control_strategy_has_required_receivers`, `:472-481`).
    fn group_is_satisfied(&self) -> bool {
        self.receivers.len() >= usize::try_from(self.group_min_size.max(0)).unwrap_or(0)
    }

    /// What the sender may write while a setup it sent is still unanswered
    /// (`aeron_min_flow_control_strategy_last_setup_snd_lmt`, `:79-96`).
    ///
    /// `INT64_MAX` — no limit at all — either because no setup has gone out, or
    /// because the one that did has aged past the receiver timeout and is
    /// forgotten: a receiver that never answered cannot hold the sender back
    /// for ever.
    fn last_setup_snd_lmt(&mut self, now_ns: i64) -> SenderLimit {
        if -1 != self.last_setup_snd_lmt {
            if self
                .time_of_last_setup_ns
                .saturating_add(self.receiver_timeout_ns)
                .saturating_sub(now_ns)
                < 0
            {
                self.last_setup_snd_lmt = -1;
            } else {
                return self.last_setup_snd_lmt;
            }
        }

        SenderLimit::MAX
    }

    /// The body both the plain and the tagged status message share
    /// (`aeron_min_flow_control_strategy_process_sm`, `:159-258`).
    ///
    /// `matches_tag` is the one thing that differs between them: the plain
    /// strategy matches every message, the tagged one only those carrying its
    /// tag (`:353`).
    fn process_status_message(
        &mut self,
        status_message: &StatusMessage,
        snd_lmt: SenderLimit,
        now_ns: i64,
        matches_tag: bool,
    ) -> SenderLimit {
        let position = status_message.consumption_position;
        let window_length = i64::from(status_message.receiver_window);
        let position_plus_window = position.saturating_add(window_length);

        let mut is_existing = false;
        let mut min_position = self.last_setup_snd_lmt(now_ns);

        for receiver in &mut self.receivers {
            if matches_tag && status_message.receiver_id == receiver.receiver_id {
                receiver.eos_flagged = status_message.eos_flagged;
                receiver.last_position = position.max(receiver.last_position);
                receiver.last_position_plus_window = position_plus_window;
                receiver.time_of_last_status_message_ns = now_ns;
                is_existing = true;
            }

            min_position = min_position.min(receiver.last_position_plus_window);
        }

        // `:198-202`: a receiver nobody has heard of joins only if it is not
        // leaving, if it matches the tag, and if it is not already behind the
        // slowest one by more than a window — a newcomer that far back cannot
        // be waited for without stalling the stream for everyone else.
        let is_admissible = !is_existing
            && !status_message.eos_flagged
            && matches_tag
            && (self.receivers.is_empty()
                || position_plus_window >= min_position.saturating_sub(window_length));

        if is_admissible {
            self.receivers.push(MinReceiver {
                receiver_id: status_message.receiver_id,
                last_position: position,
                last_position_plus_window: position_plus_window,
                time_of_last_status_message_ns: now_ns,
                eos_flagged: false,
            });

            min_position = min_position.min(position_plus_window);
            self.has_required_receivers = self.group_is_satisfied();

            // `:226`: a new receiver is a new reason for the sender to be let
            // forward, so whatever the last setup allowed is forgotten.
            self.last_setup_snd_lmt = -1;
        }

        // `:246-257`, and the three answers are different on purpose: too few
        // receivers means the **limit does not move** (rather than moving to
        // this receiver's window), and the empty table — which is not the same
        // as too few, because a group of none has no minimum — is the one case
        // where a single message is allowed to carry the limit forwards.
        if !self.group_is_satisfied() {
            snd_lmt
        } else if self.receivers.is_empty() {
            snd_lmt.max(position_plus_window)
        } else {
            snd_lmt.max(min_position)
        }
    }
}

impl Strategy for MinStrategy {
    fn on_sm(
        &mut self,
        status_message: &StatusMessage,
        snd_lmt: SenderLimit,
        now_ns: i64,
    ) -> SenderLimit {
        // `aeron_min_flow_control_strategy_on_sm`, `:260-281` (every message
        // matches) and `aeron_tagged_flow_control_strategy_on_sm`, `:326-357`
        // (only one carrying the tag does).
        let matches_tag = self.tag_matches(status_message.group_tag);

        self.process_status_message(status_message, snd_lmt, now_ns, matches_tag)
    }

    /// Drop the receivers that have gone quiet or left, and answer from the
    /// slowest of the rest (`:98-157`).
    ///
    /// This is where a group strategy notices that its group is gone: nothing
    /// arrives to say so, the messages simply stop.
    fn on_idle(
        &mut self,
        now_ns: i64,
        snd_lmt: SenderLimit,
        _snd_pos: i64,
        _is_end_of_stream: bool,
    ) -> SenderLimit {
        let mut min_limit_position = self.last_setup_snd_lmt(now_ns);
        let timeout = self.receiver_timeout_ns;

        // The reference removes these while walking the table backwards and
        // calls its `receiver_removed` log hook for each (`:126-137`); the hook
        // is one the driver context leaves null and no component here sets
        // (`aeron_driver_context.c:1239-1240`), so there is nothing to call.
        self.receivers.retain(|receiver| {
            let has_gone_quiet = receiver
                .time_of_last_status_message_ns
                .saturating_add(timeout)
                .saturating_sub(now_ns)
                < 0;

            !(has_gone_quiet || receiver.eos_flagged)
        });

        for receiver in &self.receivers {
            min_limit_position = min_limit_position.min(receiver.last_position_plus_window);
        }

        self.has_required_receivers = self.group_is_satisfied();

        if !self.group_is_satisfied() || self.receivers.is_empty() {
            snd_lmt
        } else {
            min_limit_position
        }
    }

    /// The same arithmetic `max` does, over the same multiple
    /// (`aeron_min_flow_control_strategy_max_retransmission_length`, `:424-438`).
    fn max_retransmission_length(
        &self,
        term_offset: usize,
        resend_length: usize,
        term_buffer_length: usize,
        initial_window_length: usize,
    ) -> usize {
        MaxStrategy {
            retransmit_receiver_window_multiple: self.retransmit_receiver_window_multiple,
        }
        .max_retransmission_length(
            term_offset,
            resend_length,
            term_buffer_length,
            initial_window_length,
        )
    }

    /// A setup is going out (`aeron_min_flow_control_strategy_on_setup`,
    /// `:283-303`) — and this is the one moment the strategy is told, so it is
    /// the one moment it can say what the sender may write while the receivers
    /// think about answering.
    ///
    /// Two conditions, and both matter: a status message with the matching tag
    /// must have asked for the setup (a setup sent because the setup timer
    /// expired is not an answer to anyone), and there must be receivers to
    /// answer it (`:294`). Either way the flag is cleared — the next setup has
    /// to be asked for again.
    fn on_setup(&mut self, now_ns: i64, snd_lmt: SenderLimit) {
        if self.has_matching_status_message_triggered_setup && !self.receivers.is_empty() {
            self.time_of_last_setup_ns = now_ns;
            self.last_setup_snd_lmt = snd_lmt;
        }

        self.has_matching_status_message_triggered_setup = false;
    }

    /// The reader that refused the stream is marked as leaving, and the next
    /// pass drops it (`aeron_min_flow_control_strategy_on_error`, `:305-324`).
    ///
    /// Marked and not removed: this is the network thread's call, and the
    /// receiver table is the strategy's own (`:321` sets `eos_flagged` and
    /// stops).
    fn on_error(&mut self, receiver_id: i64) {
        for receiver in &mut self.receivers {
            if receiver_id == receiver.receiver_id {
                receiver.eos_flagged = true;
            }
        }
    }

    /// A status message asked for a setup
    /// (`aeron_min_flow_control_strategy_process_on_trigger_send_setup`,
    /// `:381-392`).
    ///
    /// The flag is only ever *set* here — and only by the first asking message
    /// since the last setup went out — and only ever cleared by `on_setup`.
    fn on_trigger_send_setup(&mut self, group_tag: Option<i64>) {
        if !self.has_matching_status_message_triggered_setup {
            self.has_matching_status_message_triggered_setup = self.tag_matches(group_tag);
        }
    }

    fn has_required_receivers(&self) -> bool {
        self.has_required_receivers
    }
}

/// The receiver window a given initial window length settles at
/// (`aeron_receiver_window_length`,
/// `aeron-client/src/main/c/util/aeron_netutil.c` — the driver's own helper,
/// used by `aeron_flow_control_calculate_retransmission_length`, `:138-146`).
///
/// A window is at most half a term, because a receiver needs the other half to
/// keep reading while the publisher writes.
pub const fn receiver_window_length(
    initial_window_length: usize,
    term_buffer_length: usize,
) -> usize {
    let half_term = term_buffer_length / 2;

    if initial_window_length > half_term {
        half_term
    } else {
        initial_window_length
    }
}

/// Parse the options that follow a strategy name
/// (`aeron_flow_control_parse_max_options`, `:196-266`).
///
/// The shape is comma-separated fields, the first of which is the strategy name
/// itself — `fc=max,rrwm:3` — and anything unrecognised is refused rather than
/// skipped.
///
/// # Errors
///
/// [`FlowControlError::UnrecognisedOption`] for a field that is not one of the
/// known ones, [`FlowControlError::InvalidOption`] for an `rrwm:` that is not a
/// positive number.
pub fn max_options(options: Option<&str>) -> Result<MaxOptions, FlowControlError> {
    let Some(options) = options else {
        return Ok(MaxOptions::default());
    };

    if options.is_empty() {
        return Ok(MaxOptions::default());
    }

    let mut parsed = MaxOptions::default();

    for field in fields(options) {
        if field == "max" {
            continue;
        }

        if let Some(value) = field.strip_prefix("rrwm:") {
            parsed.rrwm = Some(retransmit_window_multiple(field, value)?);

            continue;
        }

        return Err(FlowControlError::UnrecognisedOption(field.to_owned()));
    }

    Ok(parsed)
}

/// Parse the options that follow a strategy name for the strategies that read
/// a group tag (`aeron_flow_control_parse_tagged_options`, `:487-676`).
///
/// The first field is the strategy name and is not judged here — the selector
/// is what judges it (`:529-533`) — and everything after it is one of `g:`,
/// `t:`, `rrwm:` or a refusal.
///
/// # Errors
///
/// [`FlowControlError::UnrecognisedOption`] for a field that is none of those,
/// [`FlowControlError::InvalidGroup`] / [`FlowControlError::InvalidGroupCount`]
/// for a `g:` the reference refuses, [`FlowControlError::InvalidTimeout`] for a
/// `t:` that is not a duration, [`FlowControlError::NumberFieldTooLong`] for a
/// numeric field past the reference's buffer, and
/// [`FlowControlError::InvalidOption`] for an `rrwm:` that is not a positive
/// number.
pub fn tagged_options(options: Option<&str>) -> Result<TaggedOptions, FlowControlError> {
    let Some(options) = options else {
        return Ok(TaggedOptions::default());
    };

    if options.is_empty() {
        return Ok(TaggedOptions::default());
    }

    let mut parsed = TaggedOptions::default();

    for (index, field) in fields(options).enumerate() {
        // `:529-533`: whatever is before the first comma is the strategy name.
        if 0 == index {
            continue;
        }

        // `:534`: a group or timeout field is one of more than two characters
        // that starts `g:` or `t:`, so `g:` on its own falls through to the
        // unrecognised case below.
        if field.len() > 2 && (field.starts_with("g:") || field.starts_with("t:")) {
            // `:541-554`: the field is copied into a fixed buffer before it is
            // read, and one that does not fit is refused rather than truncated.
            if field.len() - 2 >= NUMBER_BUFFER_LENGTH {
                return Err(FlowControlError::NumberFieldTooLong(field.to_owned()));
            }

            let value = &field[2..];

            if field.starts_with("g:") {
                let (tag, min_size) = group_options(field, value)?;

                parsed.group_tag = tag.or(parsed.group_tag);
                parsed.group_min_size = min_size.or(parsed.group_min_size);
            } else {
                parsed.timeout_ns = Some(
                    crate::channel_uri::parse_duration_value(value)
                        .ok_or_else(|| FlowControlError::InvalidTimeout(field.to_owned()))?,
                );
            }

            continue;
        }

        if let Some(value) = field.strip_prefix("rrwm:") {
            parsed.rrwm = Some(retransmit_window_multiple(field, value)?);

            continue;
        }

        return Err(FlowControlError::UnrecognisedOption(field.to_owned()));
    }

    Ok(parsed)
}

/// `rrwm:` — how many receiver windows a retransmission may cover
/// (`aeron_flow_control.c:238-255`).
fn retransmit_window_multiple(field: &str, value: &str) -> Result<usize, FlowControlError> {
    // A positive number or nothing: `strtol` with an errno check, which reads
    // what it can and refuses the rest.
    let number = match scan_number(value) {
        Scanned::Value(number, _) if number > 0 => number,
        _ => return Err(FlowControlError::InvalidOption(field.to_owned())),
    };

    #[allow(clippy::cast_sign_loss)] // checked positive
    {
        Ok(number as usize)
    }
}

/// The tag and the size of a `g:` field (`aeron_flow_control.c:558-610`).
///
/// The two halves are independent, and the reference reads them that way: a
/// field with no digits at all says nothing and is **not** an error — which is
/// what allows `g:/5`, a size with no tag, to be a way of naming only a size.
fn group_options(field: &str, value: &str) -> Result<(Option<i64>, Option<i32>), FlowControlError> {
    let invalid_group = || FlowControlError::InvalidGroup(field.to_owned());
    let scan = scan_number(value);

    let tag = match &scan {
        // Digits all the way to the end of the field, or up to a slash: the
        // tag is the number (`:565-568`).
        Scanned::Value(number, rest) if rest.is_empty() || rest.starts_with('/') => Some(*number),
        // A number too large for the type. With a slash after it the reference
        // reads the size and drops the tag — `errno` is set, so its first
        // branch is not taken, and its second needs a field with *no* slash.
        Scanned::TooLarge(rest) if rest.starts_with('/') => None,
        // No digits at all: nothing was consumed, and neither branch of the
        // reference's tag parse is entered.
        Scanned::Nothing(_) => None,
        _ => return Err(invalid_group()),
    };

    let size = match scan.tail().strip_prefix('/') {
        Some(size) => Some(group_min_size(field, size)?),
        None => None,
    };

    Ok((tag, size))
}

/// `g:<tag>/<size>` — the size, which the reference takes from `strtol` and
/// bounds to `0..=INT32_MAX` (`aeron_flow_control.c:582-600`).
fn group_min_size(field: &str, value: &str) -> Result<i32, FlowControlError> {
    let invalid = || FlowControlError::InvalidGroupCount(field.to_owned());

    match scan_number(value) {
        Scanned::Value(number, rest)
            if rest.is_empty() && (0..=i64::from(i32::MAX)).contains(&number) =>
        {
            i32::try_from(number).map_err(|_| invalid())
        }
        _ => Err(invalid()),
    }
}

/// The comma-separated fields of an `fc=` value, in the order the reference
/// walks them (`aeron_flow_control.c:506-528`).
///
/// Not `str::split(',')`: the reference advances past a comma and stops when
/// nothing is left after it, so a trailing comma does not produce a trailing
/// empty field — `fc=max,` is a one-field list, while `fc=max,,` is a two-field
/// one whose second field is empty and therefore refused.
fn fields(options: &str) -> impl Iterator<Item = &str> {
    let mut rest = options;

    std::iter::from_fn(move || {
        if rest.is_empty() {
            return None;
        }

        let (field, tail) = match rest.split_once(',') {
            Some((field, tail)) => (field, tail),
            None => (rest, ""),
        };

        rest = tail;

        Some(field)
    })
}

/// `AERON_FLOW_CONTROL_NUMBER_BUFFER_LEN` (`aeron_flow_control.c:485`): an
/// option's numeric field is copied into a buffer this long, so one that does
/// not fit is refused before it is read.
const NUMBER_BUFFER_LENGTH: usize = 64;

/// What the reference's `strtoll` found at the front of a field — the number,
/// where it stopped, and whether it found one at all
/// (`aeron_flow_control.c:558-600`, which reads every number this way).
#[derive(Clone, Copy, Debug)]
enum Scanned<'a> {
    /// No digits: `strtoll` consumed nothing, and set no error either. The
    /// reference tells this apart from a bad number (`:565` vs `:570`), which
    /// is why `g:abc` is accepted and `g:12abc` is not.
    ///
    /// The whole field is carried, because a `strtoll` that converts nothing
    /// leaves its end pointer at the start — so a field that *begins* with a
    /// slash still has one to find (`g:/5`).
    Nothing(&'a str),
    /// A number too large for the type, which `strtoll` reports through `errno`
    /// (`:570`). The tail is carried so that a slash after it is still seen.
    TooLarge(&'a str),
    /// The number, and the tail after it.
    Value(i64, &'a str),
}

impl<'a> Scanned<'a> {
    /// What follows the number — which for a scan that found none is the whole
    /// field, since that is where `strtoll` left its end pointer.
    fn tail(&self) -> &'a str {
        match self {
            Self::Nothing(text) | Self::TooLarge(text) | Self::Value(_, text) => text,
        }
    }
}

/// `strtoll` as the option parsers use it: leading whitespace and one sign, as
/// many digits as there are, and nothing else (`aeron_flow_control.c:558-600`).
fn scan_number(text: &str) -> Scanned<'_> {
    let number = text.trim_start_matches(|character: char| character.is_ascii_whitespace());

    let (sign, unsigned) = match number.strip_prefix(['+', '-']) {
        Some(rest) => (&number[..1], rest),
        None => ("", number),
    };

    let digits = unsigned.len()
        - unsigned
            .trim_start_matches(|character: char| character.is_ascii_digit())
            .len();

    if 0 == digits {
        return Scanned::Nothing(text);
    }

    let (digits, rest) = number.split_at(sign.len() + digits);

    match digits.parse::<i64>() {
        Ok(value) => Scanned::Value(value, rest),
        Err(_) => Scanned::TooLarge(rest),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A status message with everything but the fields under test left at
    /// nothing.
    pub(crate) fn reporting(consumption_position: i64, receiver_window: i32) -> StatusMessage {
        StatusMessage {
            consumption_position,
            receiver_window,
            receiver_id: 1,
            session_id: 1,
            stream_id: 1,
            eos_flagged: false,
            group_tag: None,
        }
    }

    #[test]
    fn the_sender_limit_is_the_far_edge_of_the_window_and_never_backs_up() {
        let mut strategy = MaxStrategy::default();

        assert_eq!(1_600, strategy.on_sm(&reporting(1_000, 600), 0, 0));
        // A window that moved backwards does not move the limit with it.
        assert_eq!(1_600, strategy.on_sm(&reporting(900, 300), 1_600, 0));
        // And a window that moved forwards does.
        assert_eq!(2_000, strategy.on_sm(&reporting(1_500, 500), 1_600, 0));
    }

    #[test]
    fn an_idle_pass_leaves_the_limit_alone() {
        let mut strategy = MaxStrategy::default();

        assert_eq!(42, strategy.on_idle(1_000, 42, 10, false));
    }

    #[test]
    fn a_retransmission_is_bounded_by_the_term_the_window_and_the_nak() {
        // A term of 64 KiB, a window of 8 KiB, a multiple of two: a
        // retransmission covers the window or the rest of the term, whichever
        // is smaller.
        let strategy = MaxStrategy {
            retransmit_receiver_window_multiple: 2,
        };

        assert_eq!(
            16 * 1024,
            strategy.max_retransmission_length(0, 64 * 1024, 64 * 1024, 8 * 1024),
            "two windows, which is less than the term"
        );
        assert_eq!(
            4 * 1024,
            strategy.max_retransmission_length(0, 4 * 1024, 64 * 1024, 8 * 1024),
            "but never more than the NAK asked for"
        );
        assert_eq!(
            1_024,
            strategy.max_retransmission_length(63 * 1024, 64 * 1024, 64 * 1024, 8 * 1024),
            "and never past the end of the term"
        );
    }

    #[test]
    fn a_window_is_at_most_half_a_term() {
        assert_eq!(8 * 1024, receiver_window_length(8 * 1024, 64 * 1024));
        assert_eq!(32 * 1024, receiver_window_length(64 * 1024, 64 * 1024));
        assert_eq!(32 * 1024, receiver_window_length(1024 * 1024, 64 * 1024));
    }

    /// A status message from `receiver_id`, reading from `position` with
    /// `window` of room.
    fn from(receiver_id: i64, position: i64, window: i32) -> StatusMessage {
        StatusMessage {
            consumption_position: position,
            receiver_window: window,
            receiver_id,
            session_id: 1,
            stream_id: 1,
            eos_flagged: false,
            group_tag: None,
        }
    }

    /// A `min` strategy with the default timeout and no group gate, so that
    /// one receiver is a group.
    fn min() -> MinStrategy {
        MinStrategy::new(5_000_000_000, 0, None, MULTICAST_RRWM_DEFAULT)
    }

    #[test]
    fn the_limit_is_the_slowest_receivers_window_edge() {
        let mut strategy = min();

        assert_eq!(1_500, strategy.on_sm(&from(1, 1_000, 500), 0, 0));
        assert_eq!(
            1_500,
            strategy.on_sm(&from(2, 1_300, 500), 0, 0),
            "the sender waits for the slowest reader, not for this one"
        );
        assert_eq!(2, strategy.receiver_count());

        assert_eq!(
            1_800,
            strategy.on_sm(&from(1, 1_400, 400), 0, 0),
            "receiver 1 has moved up to 1_800, which is now the lower of the two"
        );
    }

    #[test]
    fn the_limit_never_moves_backwards() {
        let mut strategy = min();

        assert_eq!(1_500, strategy.on_sm(&from(1, 1_000, 500), 0, 0));
        assert_eq!(
            1_500,
            strategy.on_sm(&from(1, 100, 100), 1_500, 0),
            "a receiver that moved backwards does not take the limit with it"
        );
    }

    #[test]
    fn an_existing_receivers_window_replaces_the_one_it_had() {
        let mut strategy = min();

        strategy.on_sm(&from(1, 1_000, 500), 0, 0);
        // `:189`: `position + window_length`, not the larger of the two — a
        // receiver that shrinks its window is taken at its word.
        assert_eq!(1_200, strategy.on_sm(&from(1, 1_000, 200), 0, 0));
    }

    #[test]
    fn a_receiver_that_has_gone_quiet_is_dropped_and_the_limit_follows_the_rest() {
        let mut strategy = MinStrategy::new(1_000, 0, None, MULTICAST_RRWM_DEFAULT);

        strategy.on_sm(&from(1, 1_000, 500), 0, 0);
        strategy.on_sm(&from(2, 2_000, 500), 0, 500);
        assert_eq!(2, strategy.receiver_count());

        // Receiver 1 was last heard from at 0 and receiver 2 at 500, so a
        // timeout of 1_000 that is now at 1_001 drops the first and not the
        // second: the limit becomes what is left.
        assert_eq!(2_500, strategy.on_idle(1_001, 1_500, 0, false));
        assert_eq!(1, strategy.receiver_count());
    }

    #[test]
    fn with_no_receivers_left_the_limit_does_not_move() {
        let mut strategy = MinStrategy::new(1_000, 0, None, MULTICAST_RRWM_DEFAULT);

        strategy.on_sm(&from(1, 1_000, 500), 0, 0);
        assert_eq!(1_500, strategy.on_sm(&from(1, 1_000, 500), 0, 0));

        assert_eq!(
            1_500,
            strategy.on_idle(1_001, 1_500, 0, false),
            "the table is empty, so there is nothing to hold the limit back"
        );
        assert_eq!(0, strategy.receiver_count());
        assert!(
            strategy.has_required_receivers(),
            "a group of none is satisfied by nobody (`0 >= 0`) — what keeps the \
             publication unconnected is having no receiver at all"
        );
    }

    #[test]
    fn too_few_receivers_means_the_limit_does_not_move_at_all() {
        let mut strategy = MinStrategy::new(5_000_000_000, 2, None, MULTICAST_RRWM_DEFAULT);

        assert_eq!(
            500,
            strategy.on_sm(&from(1, 1_000, 500), 500, 0),
            "one receiver is not a group of two: not the window edge, the limit it was given"
        );
        assert!(!strategy.has_required_receivers());
        assert_eq!(1, strategy.receiver_count(), "but it is remembered");

        assert_eq!(1_500, strategy.on_sm(&from(2, 1_000, 500), 500, 0));
        assert!(strategy.has_required_receivers());
    }

    #[test]
    fn a_receiver_that_is_leaving_does_not_join() {
        let mut strategy = min();
        let leaving = StatusMessage {
            eos_flagged: true,
            ..from(1, 1_000, 500)
        };

        assert_eq!(0, strategy.receiver_count());
        assert_eq!(
            1_500,
            strategy.on_sm(&leaving, 0, 0),
            "an empty table is the one case a message carries the limit forwards"
        );
        assert_eq!(
            0,
            strategy.receiver_count(),
            "and it still does not become a receiver"
        );
    }

    #[test]
    fn a_receiver_further_behind_than_a_window_does_not_join() {
        let mut strategy = min();

        strategy.on_sm(&from(1, 10_000, 500), 0, 0);

        // `:198-202`: `position_plus_window >= min_position - window_length`.
        strategy.on_sm(&from(2, 8_500, 500), 0, 0);
        assert_eq!(
            1,
            strategy.receiver_count(),
            "9_000 is more than a window below 10_500"
        );

        strategy.on_sm(&from(3, 9_500, 500), 0, 0);
        assert_eq!(
            2,
            strategy.receiver_count(),
            "10_000 is exactly a window below it, which is close enough"
        );
    }

    #[test]
    fn a_leaving_receiver_is_dropped_by_the_next_pass() {
        let mut strategy = min();

        strategy.on_sm(&from(1, 1_000, 500), 0, 0);
        strategy.on_sm(&from(2, 2_000, 500), 0, 0);
        assert_eq!(2, strategy.receiver_count());

        strategy.on_sm(
            &StatusMessage {
                eos_flagged: true,
                ..from(1, 1_000, 500)
            },
            0,
            0,
        );
        assert_eq!(
            2,
            strategy.receiver_count(),
            "an end-of-stream message marks the receiver rather than removing it"
        );

        strategy.on_idle(0, 0, 0, false);
        assert_eq!(1, strategy.receiver_count());
        assert_eq!(2_500, strategy.on_idle(0, 2_500, 0, false));
    }

    /// A strategy that answers only to messages carrying `tag`
    /// (`fc=tagged,g:<tag>`), with a group of one so that the empty-table
    /// branch cannot answer for it.
    fn tagged(tag: i64) -> MinStrategy {
        MinStrategy::new(5_000_000_000, 1, Some(tag), MULTICAST_RRWM_DEFAULT)
    }

    #[test]
    fn a_tagged_strategy_counts_only_messages_carrying_its_tag() {
        let mut strategy = tagged(7);

        assert_eq!(
            999,
            strategy.on_sm(&from(1, 1_000, 500), 999, 0),
            "no tag at all is not its tag"
        );
        assert_eq!(
            999,
            strategy.on_sm(
                &StatusMessage {
                    group_tag: Some(9),
                    ..from(1, 1_000, 500)
                },
                999,
                0
            ),
            "nor is someone else's"
        );
        assert_eq!(0, strategy.receiver_count());

        assert_eq!(
            1_500,
            strategy.on_sm(
                &StatusMessage {
                    group_tag: Some(7),
                    ..from(1, 1_000, 500)
                },
                999,
                0
            ),
            "its own tag is, and a group of one is satisfied"
        );
        assert_eq!(1, strategy.receiver_count());
        assert!(strategy.has_required_receivers());
    }

    #[test]
    fn the_plain_strategy_counts_a_message_that_carries_no_tag() {
        let mut strategy = MinStrategy::new(5_000_000_000, 1, None, MULTICAST_RRWM_DEFAULT);

        assert_eq!(1_500, strategy.on_sm(&from(1, 1_000, 500), 0, 0));
        assert_eq!(
            1_500,
            strategy.on_sm(
                &StatusMessage {
                    group_tag: Some(9),
                    ..from(2, 2_000, 500)
                },
                0,
                0
            ),
            "a tag it never asked for is not a reason to ignore anyone — and the \
             limit is still the slowest receiver's"
        );
        assert_eq!(2, strategy.receiver_count());
    }

    /// `on_setup` is the strategy's one chance to say what the sender may
    /// write while the receivers it just asked are thinking about answering,
    /// and it takes two conditions to use it (`aeron_min_flow_control.c:283-303`).
    #[test]
    fn only_a_setup_a_matching_message_asked_for_records_its_limit() {
        let mut strategy = tagged(7);
        let reporting = |position: i64, window: i32| StatusMessage {
            group_tag: Some(7),
            ..from(1, position, window)
        };

        strategy.on_sm(&reporting(1_000, 500), 0, 0);

        // The setup timer expired rather than anyone asking: nothing recorded.
        strategy.on_setup(10, 1_200);
        assert_eq!(3_500, strategy.on_sm(&reporting(3_000, 500), 0, 10));

        // Asked for, but by a message with someone else's tag.
        strategy.on_trigger_send_setup(Some(9));
        strategy.on_setup(20, 1_200);
        assert_eq!(3_500, strategy.on_sm(&reporting(3_000, 500), 0, 20));

        // Asked for by its own tag: now the limit holds.
        strategy.on_trigger_send_setup(Some(7));
        strategy.on_setup(30, 1_200);
        assert_eq!(
            1_200,
            strategy.on_sm(&reporting(3_000, 500), 0, 30),
            "the sender stays where the setup left it until the receivers answer"
        );
    }

    /// And the record only lasts as long as a receiver may: a setup nobody
    /// answers cannot hold the sender for ever (`:83-96`).
    #[test]
    fn the_limit_a_setup_was_sent_under_is_forgotten_on_the_receiver_timeout() {
        let mut strategy = MinStrategy::new(1_000, 1, Some(7), MULTICAST_RRWM_DEFAULT);

        strategy.on_sm(
            &StatusMessage {
                group_tag: Some(7),
                ..from(1, 1_000, 500)
            },
            0,
            0,
        );
        strategy.on_trigger_send_setup(Some(7));
        strategy.on_setup(10, 1_200);

        assert_eq!(
            1_200,
            strategy.on_sm(
                &StatusMessage {
                    group_tag: Some(7),
                    ..from(1, 3_000, 500)
                },
                0,
                10
            )
        );
        assert_eq!(
            3_500,
            strategy.on_sm(
                &StatusMessage {
                    group_tag: Some(7),
                    ..from(1, 3_000, 500)
                },
                0,
                1_011
            ),
            "ten nanoseconds past the setup's own one thousand"
        );
    }

    #[test]
    fn a_receiver_that_refused_the_stream_is_dropped_by_the_next_pass() {
        let mut strategy = min();

        strategy.on_sm(&from(1, 1_000, 500), 0, 0);
        strategy.on_sm(&from(2, 2_000, 500), 0, 0);
        assert_eq!(1_500, strategy.on_sm(&from(1, 1_000, 500), 0, 0));

        strategy.on_error(1);
        assert_eq!(2, strategy.receiver_count(), "marked, not removed");

        assert_eq!(
            2_500,
            strategy.on_idle(0, 1_500, 0, false),
            "and the limit now follows the receiver that is left"
        );
        assert_eq!(1, strategy.receiver_count());
    }

    #[test]
    fn the_options_are_the_ones_the_reference_parses() {
        assert_eq!(Ok(MaxOptions::default()), max_options(Some("max")));
        assert_eq!(
            Ok(MaxOptions { rrwm: Some(3) }),
            max_options(Some("max,rrwm:3"))
        );
        assert_eq!(
            Ok(MaxOptions { rrwm: Some(3) }),
            max_options(Some("rrwm:3"))
        );
        assert_eq!(Ok(MaxOptions::default()), max_options(None));

        assert_eq!(
            Err(FlowControlError::UnrecognisedOption("nonsense".to_owned())),
            max_options(Some("max,nonsense"))
        );
        assert_eq!(
            Err(FlowControlError::InvalidOption("rrwm:0".to_owned())),
            max_options(Some("rrwm:0"))
        );
        assert_eq!(
            Err(FlowControlError::InvalidOption("rrwm:x".to_owned())),
            max_options(Some("rrwm:x"))
        );
    }

    #[test]
    fn the_configured_multiple_is_what_a_channel_that_names_none_gets() {
        assert_eq!(16, UNICAST_RRWM_DEFAULT, "the context's own default");
        assert_eq!(
            UNICAST_RRWM_DEFAULT,
            MaxStrategy::from_options(UNICAST_RRWM_DEFAULT, Some("max"))
                .expect("a strategy")
                .retransmit_receiver_window_multiple
        );
        assert_eq!(
            7,
            MaxStrategy::from_options(UNICAST_RRWM_DEFAULT, Some("max,rrwm:7"))
                .expect("a strategy")
                .retransmit_receiver_window_multiple
        );
    }

    /// A unicast channel never reads `fc=`. The reference picks the unicast
    /// supplier outright and hands it no options at all
    /// (`aeron_unicast_flow_control_strategy_supplier`, `aeron_flow_control.c:326-365`),
    /// so a parameter naming a strategy this driver does not have is not a
    /// refusal — it is a parameter that was never consulted.
    ///
    /// This build used to parse `fc=` on every channel and answer
    /// `GENERIC_ERROR` for anything but `max`, which the reference does not do.
    #[test]
    fn a_unicast_channel_is_served_as_though_fc_were_not_there() {
        let rrwm = |fc| {
            strategy_for_channel(false, fc, UNICAST_RRWM_DEFAULT, MULTICAST_RRWM_DEFAULT)
                .expect("a unicast channel cannot be refused for its `fc=`")
                .retransmit_receiver_window_multiple
        };

        assert_eq!(UNICAST_RRWM_DEFAULT, rrwm(None));
        assert_eq!(UNICAST_RRWM_DEFAULT, rrwm(Some("max")));
        // Not `7`: the option is not read either.
        assert_eq!(UNICAST_RRWM_DEFAULT, rrwm(Some("max,rrwm:7")));
        // And not an error, which is the whole point.
        assert_eq!(UNICAST_RRWM_DEFAULT, rrwm(Some("min")));
        assert_eq!(UNICAST_RRWM_DEFAULT, rrwm(Some("tagged")));
        assert_eq!(UNICAST_RRWM_DEFAULT, rrwm(Some("nonsense")));
    }

    /// A multi-destination channel does read it, and no `fc=` at all falls to
    /// the context's multicast supplier, which is `max`
    /// (`aeron_driver_context.c:201`).
    #[test]
    fn a_multi_destination_channel_reads_fc_and_defaults_to_four() {
        let rrwm = |fc| {
            strategy_for_channel(true, fc, UNICAST_RRWM_DEFAULT, MULTICAST_RRWM_DEFAULT)
                .expect("a strategy")
                .retransmit_receiver_window_multiple
        };

        assert_eq!(4, MULTICAST_RRWM_DEFAULT, "the context's own default");
        assert_eq!(MULTICAST_RRWM_DEFAULT, rrwm(None));
        assert_eq!(MULTICAST_RRWM_DEFAULT, rrwm(Some("max")));
        assert_eq!(
            7,
            rrwm(Some("max,rrwm:7")),
            "the option is read here, unlike the unicast branch"
        );
    }

    /// The two strategies this build does not have come back as such, and an
    /// `fc=` with nothing before its comma is the reference's own distinct
    /// refusal (`aeron_flow_control.c:425-431`).
    #[test]
    fn a_multi_destination_channel_naming_a_strategy_this_build_lacks_is_refused() {
        let refusal = |fc| {
            strategy_for_channel(true, fc, UNICAST_RRWM_DEFAULT, MULTICAST_RRWM_DEFAULT)
                .expect_err("this build cannot serve that strategy")
        };

        assert_eq!(
            FlowControlError::UnknownStrategy("min".to_owned()),
            refusal(Some("min"))
        );
        assert_eq!(
            FlowControlError::UnknownStrategy("tagged".to_owned()),
            refusal(Some("tagged"))
        );
        assert_eq!(
            FlowControlError::UnknownStrategy("cubic".to_owned()),
            refusal(Some("cubic"))
        );
        assert_eq!(FlowControlError::NoStrategyName, refusal(Some("")));
        assert_eq!(
            FlowControlError::NoStrategyName,
            refusal(Some(",rrwm:7")),
            "a comma straight away is the same as nothing at all"
        );
    }

    /// The reference walks the fields by eating a comma and stopping when
    /// nothing is left after it (`aeron_flow_control.c:506-528`), so a trailing
    /// comma leaves no field behind — while two commas in a row do.
    #[test]
    fn a_trailing_comma_does_not_make_a_trailing_field() {
        assert_eq!(Ok(MaxOptions::default()), max_options(Some("max,")));
        assert_eq!(
            Err(FlowControlError::UnrecognisedOption(String::new())),
            max_options(Some("max,,")),
            "a field between two commas is a field, and an empty one is refused"
        );
        assert_eq!(
            Ok(TaggedOptions::default()),
            tagged_options(Some("tagged,"))
        );
    }

    #[test]
    fn the_tagged_options_are_the_ones_the_reference_parses() {
        assert_eq!(Ok(TaggedOptions::default()), tagged_options(None));
        assert_eq!(Ok(TaggedOptions::default()), tagged_options(Some("")));
        assert_eq!(Ok(TaggedOptions::default()), tagged_options(Some("min")));
        assert_eq!(Ok(TaggedOptions::default()), tagged_options(Some("tagged")));

        assert_eq!(
            Ok(TaggedOptions {
                group_tag: Some(123),
                ..Default::default()
            }),
            tagged_options(Some("tagged,g:123")),
            "a `g:` with no slash names a tag and no size"
        );
        assert_eq!(
            Ok(TaggedOptions {
                group_tag: Some(123),
                group_min_size: Some(1),
                ..Default::default()
            }),
            tagged_options(Some("tagged,g:123/1"))
        );
        assert_eq!(
            Ok(TaggedOptions {
                timeout_ns: Some(1_000_000_000),
                ..Default::default()
            }),
            tagged_options(Some("tagged,t:1s"))
        );
        assert_eq!(
            Ok(TaggedOptions {
                group_tag: Some(123),
                group_min_size: Some(1),
                timeout_ns: Some(2_000_000_000),
                rrwm: Some(7),
            }),
            tagged_options(Some("tagged,g:123/1,t:2s,rrwm:7")),
            "the name is not the only field that is read"
        );
        assert_eq!(
            Ok(TaggedOptions {
                group_min_size: Some(5),
                ..Default::default()
            }),
            tagged_options(Some("tagged,g:/5")),
            "a size with no tag, which is what the empty value is allowed for"
        );
    }

    /// Two answers the reference tells apart: a field with **no digits** is
    /// ignored, and one with digits followed by something that is neither the
    /// end nor a slash is refused (`aeron_flow_control.c:565-580`).
    #[test]
    fn a_group_field_with_no_digits_is_ignored_where_a_bad_number_is_refused() {
        assert_eq!(
            Ok(TaggedOptions::default()),
            tagged_options(Some("tagged,g:abc")),
            "`strtoll` consumed nothing, so neither of the tag's branches is entered"
        );
        assert_eq!(
            Err(FlowControlError::InvalidGroup("g:12abc".to_owned())),
            tagged_options(Some("tagged,g:12abc"))
        );
        assert_eq!(
            Err(FlowControlError::InvalidGroupCount("g:12/".to_owned())),
            tagged_options(Some("tagged,g:12/"))
        );
        assert_eq!(
            Err(FlowControlError::InvalidGroupCount("g:123/-1".to_owned())),
            tagged_options(Some("tagged,g:123/-1")),
            "a count is not a number: `0 <= group_min_size <= INT32_MAX`"
        );
        assert_eq!(
            Err(FlowControlError::InvalidGroupCount(
                "g:123/2147483648".to_owned()
            )),
            tagged_options(Some("tagged,g:123/2147483648")),
            "and it stops at INT32_MAX"
        );
    }

    /// A tag too large for the type is dropped, but a slash after it is still
    /// read: the reference's tag branch needs `errno` clear, while its
    /// "invalid group" branch needs the *absence* of a slash
    /// (`aeron_flow_control.c:570-580`).
    #[test]
    fn a_tag_too_large_for_the_type_is_dropped_and_its_size_is_still_read() {
        assert_eq!(
            Ok(TaggedOptions {
                group_min_size: Some(5),
                ..Default::default()
            }),
            tagged_options(Some("tagged,g:99999999999999999999/5"))
        );
        assert_eq!(
            Err(FlowControlError::InvalidGroup(
                "g:99999999999999999999".to_owned()
            )),
            tagged_options(Some("tagged,g:99999999999999999999")),
            "with no slash there is nothing to read, and it is a bad number"
        );
    }

    #[test]
    fn the_other_fields_are_refused_the_way_the_reference_refuses_them() {
        assert_eq!(
            Err(FlowControlError::UnrecognisedOption("nonsense".to_owned())),
            tagged_options(Some("tagged,nonsense"))
        );
        assert_eq!(
            Err(FlowControlError::UnrecognisedOption("g:".to_owned())),
            tagged_options(Some("tagged,g:")),
            "two characters is not a group field, it is an option nobody knows"
        );
        assert_eq!(
            Err(FlowControlError::InvalidTimeout("t:1x".to_owned())),
            tagged_options(Some("tagged,t:1x"))
        );
        assert_eq!(
            Err(FlowControlError::InvalidOption("rrwm:0".to_owned())),
            tagged_options(Some("tagged,rrwm:0"))
        );

        let long = format!("tagged,t:{}", "9".repeat(64));
        assert_eq!(
            Err(FlowControlError::NumberFieldTooLong(format!(
                "t:{}",
                "9".repeat(64)
            ))),
            tagged_options(Some(&long)),
            "the field has to fit the reference's own 64-byte buffer"
        );
    }
}
