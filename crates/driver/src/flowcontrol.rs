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

/// What a sender asks of its strategy
/// (`aeron_flow_control_strategy_t`, `aeron_flow_control.h:46-100`).
pub trait Strategy {
    /// A status message arrived: it carries the receiver's consumption
    /// position and its window, and the answer is the new sender limit.
    ///
    /// `consumption_position` is the receiver's position, already computed from
    /// the frame's term id and offset against the publication's initial term id
    /// — which is the caller's job in both implementations
    /// (`aeron_logbuffer_compute_position`, `aeron_flow_control.c:120-124`).
    fn on_sm(
        &mut self,
        consumption_position: i64,
        receiver_window: i32,
        snd_lmt: SenderLimit,
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
        consumption_position: i64,
        receiver_window: i32,
        snd_lmt: SenderLimit,
    ) -> SenderLimit {
        // `:108-127`: the window edge, and the limit never goes backwards.
        let window_edge = consumption_position.saturating_add(i64::from(receiver_window));

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

    #[test]
    fn the_sender_limit_is_the_far_edge_of_the_window_and_never_backs_up() {
        let mut strategy = MaxStrategy::default();

        assert_eq!(1_600, strategy.on_sm(1_000, 600, 0));
        // A window that moved backwards does not move the limit with it.
        assert_eq!(1_600, strategy.on_sm(900, 300, 1_600));
        // And a window that moved forwards does.
        assert_eq!(2_000, strategy.on_sm(1_500, 500, 1_600));
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
