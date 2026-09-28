//! Serving the retransmissions a NAK asks for, on a delay.
//!
//! Mirrors `aeron-driver/src/main/c/aeron_retransmit_handler.c`. A NAK says
//! "term *t*, from offset *o*, *n* bytes"; the handler decides whether the
//! answer is now, in `delay_ns`, or never — and it is the *only* thing that
//! decides, because a sender that answered every NAK immediately would answer
//! the same lost packet twice while the first retransmission was still in
//! flight.
//!
//! # The three states
//!
//! An action starts `Delayed` with an expiry `delay_timeout_ns` away, becomes
//! `Lingering` when it is served (its own expiry now `linger_timeout_ns` away,
//! during which a NAK inside the same range is *not* served again), and
//! `Inactive` when the linger expires — which frees the slot
//! (`aeron_retransmit_handler_process_timeouts`, `:158-198`).
//!
//! # Unicast has one slot, multicast has many
//!
//! `max_retransmits` is `max_resend` under group semantics and **one**
//! otherwise (`:38`): a unicast publication answers one NAK at a time and
//! reuses that slot for a NAK that does not overlap the one being served
//! (`:222-240`). That is what makes a peer that keeps asking for the same
//! range unable to make the sender send it over and over.

/// Where an action is (`aeron_retransmit_action_state_t`,
/// `aeron-driver/src/main/c/aeron_retransmit_handler.h:24-29`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActionState {
    /// Asked for, waiting for its delay to expire.
    Delayed,
    /// Served; ignoring NAKs inside its range until its linger expires.
    Lingering,
    /// Free.
    Inactive,
}

/// One retransmission the handler owes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Action {
    expiry_ns: i64,
    term_id: i32,
    term_offset: i32,
    length: usize,
    state: ActionState,
}

/// The default `max.resend` (`AERON_RETRANSMIT_HANDLER_MAX_RESEND`, `:23`).
pub const MAX_RESEND_DEFAULT: usize = 16;

/// The last offset a NAK may name and still describe a frame
/// (`aeron_retransmit_handler_is_invalid`, `:62-64`): a term's last data header,
/// past which no frame can begin.
const LAST_FRAME_OFFSET: i32 = 40;

/// What the handler asks the caller to do.
///
/// Returned rather than called back so that the module owns no mutable
/// reference to the publication: the caller does the sending, and the handler
/// is told whether it worked.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Resend {
    /// The term.
    pub term_id: i32,
    /// Where in it.
    pub term_offset: i32,
    /// How much of it.
    pub length: usize,
}

/// The counters a handler bumps, named rather than addressed so that this
/// module keeps no opinion about counters (`:66-84` bumps
/// `invalid_packets` and `:263` the retransmit overflow).
pub trait Faults {
    /// A NAK whose offset or length could not describe anything in a term.
    fn invalid_packet(&mut self);

    /// A group-semantics publication with every slot taken.
    fn retransmit_overflow(&mut self);
}

/// What [`crate::retransmit_handler::RetransmitHandler::on_nak`] decided.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NakOutcome {
    /// Send this, now.
    Send(Resend),
    /// It will be sent when its delay expires.
    Scheduled,
    /// A NAK inside the range of one already being served, or one the
    /// publication has already moved past: nothing to do.
    Ignored,
    /// The NAK did not describe a term offset or length that can exist.
    Invalid,
    /// No slot: the caller counts a retransmit overflow and drops it.
    Overflow,
}

/// The retransmissions a publication owes.
#[derive(Debug)]
pub struct RetransmitHandler {
    actions: Vec<Action>,
    delay_timeout_ns: i64,
    linger_timeout_ns: i64,
    has_group_semantics: bool,
}

impl RetransmitHandler {
    /// A handler for a publication
    /// (`aeron_retransmit_handler_init`, `:28-53`).
    ///
    /// `max_resend` is the channel's `max-resend`, and it is the *group*
    /// semantics' slot count: a unicast publication has one slot whatever it
    /// says (`:38`).
    pub fn new(
        delay_timeout_ns: i64,
        linger_timeout_ns: i64,
        has_group_semantics: bool,
        max_resend: usize,
    ) -> Self {
        let slots = if has_group_semantics {
            max_resend.max(1)
        } else {
            1
        };

        Self {
            actions: vec![
                Action {
                    expiry_ns: 0,
                    term_id: 0,
                    term_offset: 0,
                    length: 0,
                    state: ActionState::Inactive,
                };
                slots
            ],
            delay_timeout_ns,
            linger_timeout_ns,
            has_group_semantics,
        }
    }

    /// How many retransmissions are in flight, for the "is there anything to
    /// do" check the timeout pass makes first (`:166`).
    pub fn active_count(&self) -> usize {
        self.actions
            .iter()
            .filter(|action| action.state != ActionState::Inactive)
            .count()
    }

    /// Whether this handler treats a term as belonging to a group
    /// (`has_group_semantics`, which the setup frame's `GROUP` flag carries).
    pub const fn has_group_semantics(&self) -> bool {
        self.has_group_semantics
    }

    /// A NAK arrived (`aeron_retransmit_handler_on_nak`, `:86-140`).
    ///
    /// `max_retransmission_length` is the flow-control strategy's answer, asked
    /// with `(term_offset, length, term_length, mtu)` — the handler does not
    /// know the strategy, only that the NAK is not the whole of what may be
    /// sent.
    #[allow(clippy::too_many_arguments)] // the NAK's fields and the term's
    pub fn on_nak(
        &mut self,
        faults: &mut impl Faults,
        term_id: i32,
        term_offset: i32,
        length: i32,
        term_length: usize,
        max_retransmission_length: impl FnOnce(usize, usize) -> usize,
        now_ns: i64,
    ) -> NakOutcome {
        // `:66-84`: an offset past the last frame a term can hold, a negative
        // offset or a negative length is a NAK that cannot be served.
        if term_offset < 0 || length < 0 || term_offset > term_length as i32 - LAST_FRAME_OFFSET {
            faults.invalid_packet();
            return NakOutcome::Invalid;
        }

        if length == 0 {
            return NakOutcome::Ignored;
        }

        #[allow(clippy::cast_sign_loss)] // checked non-negative above
        let requested = length as usize;
        let retransmit_length =
            max_retransmission_length(term_offset.unsigned_abs() as usize, requested);

        let slot = match self.claim_slot(term_id, term_offset) {
            Ok(Some(slot)) => slot,
            Ok(None) => return NakOutcome::Ignored,
            Err(()) => {
                faults.retransmit_overflow();
                return NakOutcome::Overflow;
            }
        };

        let resend = Resend {
            term_id,
            term_offset,
            length: retransmit_length,
        };

        self.actions[slot] = Action {
            expiry_ns: 0,
            term_id,
            term_offset,
            length: retransmit_length,
            state: if self.delay_timeout_ns == 0 {
                ActionState::Lingering
            } else {
                ActionState::Delayed
            },
        };
        self.actions[slot].expiry_ns = now_ns
            + if self.delay_timeout_ns == 0 {
                self.linger_timeout_ns
            } else {
                self.delay_timeout_ns
            };

        if self.delay_timeout_ns == 0 {
            NakOutcome::Send(resend)
        } else {
            NakOutcome::Scheduled
        }
    }

    /// Serve what is due and retire what has lingered long enough
    /// (`aeron_retransmit_handler_process_timeouts`, `:158-198`).
    ///
    /// Returns how many actions changed state, which is the pass's work count.
    /// A `Delayed` action past its expiry is served; a `Lingering` one past its
    /// expiry is dropped. The two expiries are the whole state machine.
    pub fn process_timeouts(
        &mut self,
        now_ns: i64,
        mut resend: impl FnMut(Resend) -> bool,
    ) -> usize {
        let mut work = 0;

        for slot in 0..self.actions.len() {
            let action = self.actions[slot];

            match action.state {
                ActionState::Delayed if now_ns > action.expiry_ns => {
                    let _ = resend(Resend {
                        term_id: action.term_id,
                        term_offset: action.term_offset,
                        length: action.length,
                    });

                    self.actions[slot].state = ActionState::Lingering;
                    self.actions[slot].expiry_ns = now_ns + self.linger_timeout_ns;
                    work += 1;
                }
                ActionState::Lingering if now_ns > action.expiry_ns => {
                    self.actions[slot].state = ActionState::Inactive;
                    work += 1;
                }
                _ => {}
            }
        }

        work
    }

    /// Forget everything: the reference's `aeron_network_publication_on_nak`
    /// path resets nothing, but a publication that is being revoked has no
    /// retransmissions to serve.
    pub fn reset(&mut self) {
        for action in &mut self.actions {
            action.state = ActionState::Inactive;
        }
    }

    /// Find the slot a NAK may use, or say why not
    /// (`aeron_retransmit_handler_scan_for_available_retransmit`, `:200-259`).
    ///
    /// `Ok(None)` is the reference's `*actionp = NULL` — a NAK inside a range
    /// already being served; `Err(())` is the group-semantics overflow.
    fn claim_slot(&mut self, term_id: i32, term_offset: i32) -> Result<Option<usize>, ()> {
        let mut available: Option<usize> = None;

        for slot in 0..self.actions.len() {
            let action = self.actions[slot];

            match action.state {
                ActionState::Inactive => {
                    if available.is_none() {
                        available = Some(slot);
                    }
                }
                ActionState::Delayed | ActionState::Lingering => {
                    // Already being served, or about to be: serving it twice
                    // is the thing this whole module exists to avoid.
                    if action.term_id == term_id
                        && action.term_offset <= term_offset
                        && term_offset < action.term_offset + action.length as i32
                    {
                        return Ok(None);
                    }

                    if !self.has_group_semantics {
                        // Unicast: the NAK does not overlap, so the one slot
                        // is reused rather than left occupied.
                        available = Some(slot);
                    }
                }
            }
        }

        if self.has_group_semantics && available.is_none() {
            // `:245-248`: a group may not overwrite an action another member is
            // still owed, so no slot at all is the overflow counter.
            return Err(());
        }

        Ok(available)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct Counters {
        invalid: usize,
        overflow: usize,
    }

    impl Faults for Counters {
        fn invalid_packet(&mut self) {
            self.invalid += 1;
        }

        fn retransmit_overflow(&mut self) {
            self.overflow += 1;
        }
    }

    fn handler() -> RetransmitHandler {
        // A unicast publication: one slot, a delay before serving.
        RetransmitHandler::new(1_000, 5_000, false, MAX_RESEND_DEFAULT)
    }

    #[test]
    fn a_nak_is_served_after_its_delay_and_not_before() {
        let mut handler = handler();
        let mut counters = Counters::default();

        let outcome = handler.on_nak(&mut counters, 7, 100, 64, 1024, |_, length| length, 10_000);

        assert_eq!(NakOutcome::Scheduled, outcome);
        assert_eq!(1, handler.active_count());

        let mut served = Vec::new();
        assert_eq!(
            0,
            handler.process_timeouts(10_500, |resend| {
                served.push(resend);
                true
            }),
            "nothing yet: the delay has not expired"
        );
        assert!(served.is_empty());

        assert_eq!(
            1,
            handler.process_timeouts(11_001, |resend| {
                served.push(resend);
                true
            })
        );
        assert_eq!(
            vec![Resend {
                term_id: 7,
                term_offset: 100,
                length: 64
            }],
            served
        );

        // And it lingers: the same NAK is ignored until the linger expires.
        assert_eq!(
            NakOutcome::Ignored,
            handler.on_nak(&mut counters, 7, 120, 64, 1024, |_, length| length, 11_100)
        );
        assert_eq!(1, handler.process_timeouts(16_002, |_| true), "retired");
        assert_eq!(0, handler.active_count());
    }

    #[test]
    fn a_zero_delay_serves_the_nak_at_once_and_lingers() {
        let mut handler = RetransmitHandler::new(0, 5_000, false, MAX_RESEND_DEFAULT);
        let mut counters = Counters::default();

        assert_eq!(
            NakOutcome::Send(Resend {
                term_id: 7,
                term_offset: 100,
                length: 64
            }),
            handler.on_nak(&mut counters, 7, 100, 64, 1024, |_, length| length, 1_000)
        );

        // The action is lingering, so an overlapping NAK is still ignored.
        assert_eq!(
            NakOutcome::Ignored,
            handler.on_nak(&mut counters, 7, 101, 8, 1024, |_, length| length, 1_100)
        );
    }

    #[test]
    fn a_unicast_publication_reuses_its_one_slot_for_a_nak_that_does_not_overlap() {
        let mut handler = handler();
        let mut counters = Counters::default();

        handler.on_nak(&mut counters, 7, 100, 64, 1024, |_, length| length, 1_000);
        let outcome = handler.on_nak(&mut counters, 7, 500, 64, 1024, |_, length| length, 1_100);

        assert_eq!(
            NakOutcome::Scheduled,
            outcome,
            "one slot, reused: unicast is a conversation with one peer"
        );
        assert_eq!(1, handler.active_count());

        // And a NAK for another term is served too.
        assert_eq!(
            NakOutcome::Scheduled,
            handler.on_nak(&mut counters, 8, 0, 32, 1024, |_, length| length, 1_200)
        );
        assert_eq!(0, counters.overflow);
    }

    #[test]
    fn a_group_publication_has_max_resend_slots_and_counts_the_overflow() {
        let mut handler = RetransmitHandler::new(1_000, 5_000, true, 2);
        let mut counters = Counters::default();

        assert_eq!(
            NakOutcome::Scheduled,
            handler.on_nak(&mut counters, 7, 0, 64, 1024, |_, length| length, 1_000)
        );
        assert_eq!(
            NakOutcome::Scheduled,
            handler.on_nak(&mut counters, 7, 500, 64, 1024, |_, length| length, 1_000)
        );

        // Both slots are busy, and neither range covers this NAK.
        assert_eq!(
            NakOutcome::Overflow,
            handler.on_nak(&mut counters, 7, 900, 64, 1024, |_, length| length, 1_000)
        );
        assert_eq!(1, counters.overflow);
    }

    #[test]
    fn a_nak_that_could_not_describe_a_frame_is_counted_and_dropped() {
        let mut handler = handler();
        let mut counters = Counters::default();

        for (term_offset, length) in [(-1, 64), (100, -1), (1024, 64)] {
            assert_eq!(
                NakOutcome::Invalid,
                handler.on_nak(
                    &mut counters,
                    7,
                    term_offset,
                    length,
                    1024,
                    |_, length| length,
                    1_000
                ),
                "offset {term_offset}, length {length}"
            );
        }

        assert_eq!(3, counters.invalid);
        assert_eq!(0, handler.active_count());
    }

    #[test]
    fn the_retransmission_length_comes_from_the_flow_control_strategy() {
        let mut handler = handler();
        let mut counters = Counters::default();

        // The strategy answers with half of what the NAK asked for.
        let outcome = handler.on_nak(
            &mut counters,
            7,
            0,
            1_000,
            1024,
            |_, length| length / 2,
            1_000,
        );

        assert_eq!(
            NakOutcome::Send(Resend {
                term_id: 7,
                term_offset: 0,
                length: 500
            }),
            {
                // A zero delay is the only way to see the length here, so ask
                // again with one.
                let mut immediate = RetransmitHandler::new(0, 5_000, false, MAX_RESEND_DEFAULT);
                immediate.on_nak(
                    &mut counters,
                    7,
                    0,
                    1_000,
                    1024,
                    |_, length| length / 2,
                    1_000,
                )
            }
        );
        assert_eq!(
            NakOutcome::Scheduled,
            outcome,
            "the delayed handler schedules what the immediate one sends"
        );
    }
}
