//! Generators that drop what an endpoint sends, on purpose.
//!
//! The reference ships a family of them behind one vtable: a randomised one
//! that drops a fraction of what it is shown
//! (`media/aeron_random_loss_generator.c:33-62`), a fixed one that drops one
//! term-and-offset region exactly once per frame
//! (`media/aeron_fixed_loss_generator.c:33-95`), and a multi-gap one. What
//! they are for is testing loss recovery: loopback does not drop, so a test
//! that waits for a real network to lose something waits forever, and the gap,
//! the NAK, the retransmission and the de-duplication never become
//! deterministic.
//!
//! # Where a generator hangs
//!
//! On an **endpoint**, in two slots — `data_loss_generator` and
//! `control_loss_generator` (`media/aeron_send_channel_endpoint.h:73-74`,
//! `media/aeron_receive_channel_endpoint.h:87-88`). The reference fills them
//! from a supplier on the driver context, which the endpoint's own create
//! calls (`media/aeron_send_channel_endpoint.c:237-240`,
//! `media/aeron_receive_channel_endpoint.c:148-151`), so a driver that wants
//! loss is configured once and every endpoint made afterwards has it.
//!
//! This build has the **send endpoint's data slot**, which is where the
//! reference consults its own generator too
//! (`media/aeron_send_channel_endpoint.c:391-403`). The control slots — the
//! incoming NAKs, status messages and errors a *send* endpoint drops — and the
//! receive side arrive with the features that need them.
//!
//! # What a generator is shown is a datagram, not a frame
//!
//! Worth stating plainly, because it is the one thing about this seam that
//! surprises: the send path hands over **one datagram at a time**, and a
//! datagram carries as many frames as fit in the MTU. A caller that publishes
//! sixteen small messages produces one datagram, not sixteen. So "one in every
//! n" counts datagrams here, and the reference's own send-side slot has the
//! same coarseness — it looks at `iov[0]` and answers for the whole call
//! (`:391-403`). Per-frame decisions live on the *receive* side, where each
//! call carries exactly one frame (`media/aeron_receive_channel_endpoint.c:576-589`).
//!
//! # This is a test seam, not a feature
//!
//! Nothing installs one unless the driver was configured for it
//! (`debug.send.data.loss.drop.every`), and it is deliberately as dumb as it
//! can be: every n-th datagram is withheld and the rest are sent unchanged.
//! The shape a *deployment* would need — a rate, a seed, a packet filter — is
//! what the reference's other generators are.
//!
//! # One deliberate difference from the reference
//!
//! The reference reports a withheld call as *nothing sent*, so its sender does
//! not advance `snd-pos` and the same frames go out on a later pass: the wire
//! never shows a gap, and nothing is ever retransmitted. That simulates a pass
//! that did not happen, not a datagram that was lost on its way. Here a
//! withheld datagram counts as handed over, so the gap is real and the
//! recovery path is the one under test. `docs/compat.md` records this.

use std::net::SocketAddr;

/// What an endpoint asks of the generator it holds
/// (`aeron_loss_generator_t`, `media/aeron_loss_generator.h:44-51`).
///
/// The reference's vtable has two entry points: a *simple* one, named
/// `should_drop_frame`, which is given the address, the buffer and its length,
/// and a *detailed* one, which also gets the frame's stream, session, term and
/// offset so that a generator can name one region of one term.
///
/// **Which one is asked depends on the frame type, not on the generator**
/// (`media/aeron_receive_channel_endpoint.c`): a **`DATA`** frame goes to the
/// *detailed* question (`:576-585`), and a **`SETUP`** or **`RTTM`** frame —
/// neither of which carries a term offset a caller could name — goes to the
/// simple one (`:618-620`, `:646-648`). Both slots may be null, and a null slot
/// means *no drop*: a generator that answers only the detailed question never
/// drops a `SETUP`, and one that answers only the simple question never drops
/// data. That is not an accident of the reference's vtable — it is what makes
/// `aeron_setup_at_position_loss_generator` able to drop exactly the handshakes
/// it names and nothing else — so here the two are separate methods and **both
/// default to `false`**.
///
/// The send endpoint's data path asks the simple question
/// (`media/aeron_send_channel_endpoint.c:391-403`), which is what this build's
/// [`EveryNthDatagram`] answers.
///
/// The reference's third entry point, `close`, has no counterpart here: a
/// generator is owned by the endpoint that holds it, so `Drop` is the close.
pub trait LossGenerator: Send {
    /// Whether what it is shown should be withheld — the question a `SETUP` or
    /// `RTTM` frame is asked, and the one the send path asks.
    ///
    /// On the send path that is **one whole datagram**: `buffer` is the datagram
    /// and `length` its length, and a `true` answer keeps every frame inside it
    /// off the wire. `address` is where it was going, which the reference passes
    /// as well.
    fn should_drop(&mut self, address: SocketAddr, buffer: &[u8], length: usize) -> bool;

    /// Whether this **`DATA` frame** should be withheld, with the frame's own
    /// fields passed in so that a generator can name one stream, one term, or
    /// one offset in it (`aeron_loss_generator_should_drop_frame_detailed_func_t`,
    /// `media/aeron_loss_generator.h:44-51`).
    ///
    /// The default answers `false`, which is the reference's null slot.
    // Seven of them because the reference's entry point carries seven
    // (`media/aeron_loss_generator.h:44-51`), and a rule that could not name a
    // term offset would not be the rule this exists for.
    #[allow(clippy::too_many_arguments)]
    fn should_drop_detailed(
        &mut self,
        address: SocketAddr,
        buffer: &[u8],
        stream_id: i32,
        session_id: i32,
        term_id: i32,
        term_offset: i32,
        length: usize,
    ) -> bool {
        let _ = (
            address,
            buffer,
            stream_id,
            session_id,
            term_id,
            term_offset,
            length,
        );

        false
    }
}

/// Withholds every `drop_every`-th datagram it is shown.
///
/// The counting is over calls, so which datagrams are lost does not depend on
/// how a sender happened to batch them: one withheld call is one missing
/// datagram, whatever it was carrying.
pub struct EveryNthDatagram {
    drop_every: u64,
    /// How many datagrams have been shown to this generator.
    seen: u64,
    /// How many of them it withheld.
    dropped: u64,
}

impl EveryNthDatagram {
    /// Withhold one datagram in every `drop_every`.
    ///
    /// A `drop_every` below two withholds nothing rather than everything: a
    /// rate of one is a generator that empties the wire, which no caller
    /// means, and the configuration that would ask for it is refused before
    /// it reaches here.
    pub const fn new(drop_every: u64) -> Self {
        Self {
            drop_every,
            seen: 0,
            dropped: 0,
        }
    }

    /// How many datagrams this generator withheld.
    pub const fn dropped(&self) -> u64 {
        self.dropped
    }

    /// How many it was shown, withheld ones included.
    pub const fn attempted(&self) -> u64 {
        self.seen
    }
}

impl std::fmt::Debug for EveryNthDatagram {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EveryNthDatagram")
            .field("drop_every", &self.drop_every)
            .field("seen", &self.seen)
            .field("dropped", &self.dropped)
            .finish()
    }
}

impl LossGenerator for EveryNthDatagram {
    fn should_drop(&mut self, _address: SocketAddr, _buffer: &[u8], _length: usize) -> bool {
        if self.drop_every < 2 {
            return false;
        }

        self.seen += 1;

        if self.seen % self.drop_every == 0 {
            self.dropped += 1;
            return true;
        }

        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn loopback() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 40456)
    }

    /// Answer with what a generator would withhold, in order.
    fn drop_pattern(generator: &mut dyn LossGenerator, datagrams: usize) -> Vec<bool> {
        (0..datagrams)
            .map(|index| {
                let payload = [u8::try_from(index).expect("small")];
                generator.should_drop(loopback(), &payload, payload.len())
            })
            .collect()
    }

    #[test]
    fn every_nth_datagram_is_withheld_and_the_rest_are_kept() {
        let mut generator = EveryNthDatagram::new(3);

        assert_eq!(
            vec![false, false, true, false, false, true, false, false, true],
            drop_pattern(&mut generator, 9)
        );
        assert_eq!(3, generator.dropped());
        assert_eq!(9, generator.attempted());
    }

    #[test]
    fn a_withheld_datagram_is_whole_however_many_frames_it_carries() {
        let mut generator = EveryNthDatagram::new(2);

        // The datagram's contents are the generator's to read and its length
        // is the datagram's — one answer covers everything inside it, which is
        // the granularity the send path offers.
        let datagram = [0u8; 1408];
        assert!(!generator.should_drop(loopback(), &datagram, datagram.len()));
        assert!(generator.should_drop(loopback(), &datagram, datagram.len()));
        assert_eq!(1, generator.dropped());
    }

    #[test]
    fn a_generator_that_drops_nothing_withholds_nothing() {
        for drop_every in [0, 1] {
            let mut generator = EveryNthDatagram::new(drop_every);

            assert_eq!(
                vec![false, false, false, false],
                drop_pattern(&mut generator, 4),
                "a rate of {drop_every} is read as no loss at all"
            );
            assert_eq!(0, generator.dropped());
        }
    }
}
