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

    for field in options.split(',') {
        if field == "max" {
            continue;
        }

        if let Some(value) = field.strip_prefix("rrwm:") {
            // `strtol` with an errno check: a positive number or nothing.
            let number = value
                .parse::<i64>()
                .ok()
                .filter(|number| *number > 0)
                .ok_or_else(|| FlowControlError::InvalidOption(field.to_owned()))?;

            #[allow(clippy::cast_sign_loss)] // checked positive
            {
                parsed.rrwm = Some(number as usize);
            }

            continue;
        }

        return Err(FlowControlError::UnrecognisedOption(field.to_owned()));
    }

    Ok(parsed)
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
}
