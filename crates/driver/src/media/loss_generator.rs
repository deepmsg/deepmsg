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
use std::time::{Duration, Instant};

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

/// What the driver's settings ask a **send** endpoint to withhold.
///
/// The reference's send-side slot is filled by a supplier on its driver context
/// (`media/aeron_send_channel_endpoint.c:237-240`), which a test builds in the
/// same process; a driver here is a process of its own, so what it drops comes
/// from its settings. The two rules are the two the tests need: a rate
/// ([`EveryNthDatagram`]) and a **bounded blackout that starts after the
/// session is up** ([`BlackoutAfterDataDatagrams`]). Which one a configuration
/// means, and that both at once are refused, is `config.rs`'s
/// `send_data_loss` — this type only says how a chosen rule makes a generator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendDataLoss {
    /// Withhold one datagram in every this many, from the endpoint's first.
    EveryNthDatagram(u64),
    /// Withhold every datagram for `hold`, starting with the one after
    /// `pass_first` datagrams that carry a `DATA` frame.
    Blackout { pass_first: u64, hold: Duration },
}

impl SendDataLoss {
    /// A fresh generator for one endpoint.
    ///
    /// One per endpoint, as the reference's supplier gives one per endpoint:
    /// each counts its own datagrams, so a driver whose ingress publication
    /// moves to a new leader counts again from zero on the new endpoint.
    #[must_use]
    pub fn generator(&self) -> Box<dyn LossGenerator> {
        match *self {
            Self::EveryNthDatagram(drop_every) => Box::new(EveryNthDatagram::new(drop_every)),
            Self::Blackout { pass_first, hold } => {
                Box::new(BlackoutAfterDataDatagrams::new(pass_first, hold))
            }
        }
    }
}

/// Withhold **every** datagram for a while, starting once the endpoint has sent
/// enough of them to be past a connection.
///
/// The reference's `PortLossGenerator.startDropping(port, durationNs)`
/// (`aeron-test-support/src/main/java/io/aeron/test/driver/PortLossGenerator.java:32-37`)
/// is switched on from the test's own thread after the service has seen five
/// messages; a driver configured before it starts has no such moment, so the
/// moment is made **deterministic** instead: the `pass_first`-th datagram that
/// carries a `DATA` frame is the trigger, and the blackout opens with the one
/// after it.
///
/// # Why only datagrams carrying `DATA` count
///
/// A datagram can be a `SETUP`, a heartbeat or another control frame, and a
/// publication sends those **before** it has anything to say: every member's
/// ingress publication is added during a cluster connect, so a rule that
/// counted every datagram would spend its budget on handshakes that happened
/// at connect time and open its window long before the first message — or,
/// for a publication that never carries one, open it at all. Counting `DATA`
/// is what makes "after N messages" the thing the setting says.
///
/// A `DATA` frame that is only its header is a heartbeat — `EOS` and `REVOKE`
/// ride it — and is not counted for the same reason the receive-side
/// [`Blackout`](super::receive_loss::Blackout) passes heartbeats.
///
/// # The window is a duration, not a frame count
///
/// Withholding everything stops the frames arriving, so a budget of frames
/// would stall with the budget unspent; a duration ends whether the wire is
/// still busy or not. That is the same conclusion the receive side reached,
/// and its note states the measurement.
#[derive(Debug)]
pub struct BlackoutAfterDataDatagrams {
    /// How many `DATA`-carrying datagrams go through before the blackout.
    pass_first: u64,
    /// How long it withholds once it opens.
    hold: Duration,
    /// How many `DATA`-carrying datagrams it has been shown.
    seen: u64,
    /// When it opened — `None` until the threshold is crossed.
    opened_at: Option<Instant>,
    /// How many datagrams it withheld.
    dropped: u64,
}

impl BlackoutAfterDataDatagrams {
    /// Pass `pass_first` datagrams that carry a `DATA` frame, then withhold
    /// every datagram for `hold`.
    #[must_use]
    pub const fn new(pass_first: u64, hold: Duration) -> Self {
        Self {
            pass_first,
            hold,
            seen: 0,
            opened_at: None,
            dropped: 0,
        }
    }

    /// How many datagrams this generator withheld.
    #[must_use]
    pub const fn dropped(&self) -> u64 {
        self.dropped
    }

    /// How many `DATA`-carrying datagrams it was shown, the one that opened
    /// the blackout included.
    #[must_use]
    pub const fn seen(&self) -> u64 {
        self.seen
    }

    /// Whether the blackout is still in force.
    fn holding(&self) -> bool {
        self.opened_at
            .is_some_and(|opened| opened.elapsed() < self.hold)
    }
}

impl LossGenerator for BlackoutAfterDataDatagrams {
    fn should_drop(&mut self, _address: SocketAddr, buffer: &[u8], _length: usize) -> bool {
        if self.opened_at.is_some() {
            if self.holding() {
                self.dropped += 1;
                return true;
            }

            return false;
        }

        if !datagram_carries_data(buffer) {
            return false;
        }

        self.seen += 1;

        if self.seen <= self.pass_first {
            return false;
        }

        self.opened_at = Some(Instant::now());
        self.dropped += 1;
        true
    }
}

/// Whether this datagram holds a `DATA` frame with a payload.
///
/// The frames in a datagram are contiguous from its first byte, so this walks
/// them: `DATA` is the answer, a frame that is only its header is a heartbeat
/// and is passed over, and anything that does not parse ends the walk with no.
fn datagram_carries_data(buffer: &[u8]) -> bool {
    let mut offset = 0;

    while offset + crate::protocol::HEADER_LENGTH <= buffer.len() {
        let Some(header) = crate::protocol::FrameHeader::read(&buffer[offset..]) else {
            return false;
        };

        if header.frame_type == crate::protocol::frame_type::DATA
            && header.frame_length as usize > crate::protocol::DataFrame::LENGTH
        {
            return true;
        }

        let length = header.frame_length;

        if length < crate::protocol::HEADER_LENGTH as i32 {
            return false;
        }

        offset += length as usize;
    }

    false
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

    /// A datagram holding one `DATA` frame with `payload` bytes.
    ///
    /// Written through the protocol's own writer, for the reason the receive
    /// side's helper gives: a hand-rolled frame tests the byte the test got
    /// wrong.
    fn data_datagram(payload: usize) -> Vec<u8> {
        use crate::protocol::DataFrame;

        let mut frame = vec![0u8; DataFrame::LENGTH + payload];

        DataFrame {
            term_offset: 0,
            session_id: 7,
            stream_id: 101,
            term_id: 0,
            reserved_value: 0,
        }
        .write(&mut frame)
        .expect("the frame fits");

        let length = i32::try_from(frame.len())
            .expect("a small frame")
            .to_le_bytes();
        frame[0..4].copy_from_slice(&length);

        frame
    }

    /// A datagram holding one `SETUP` and nothing else.
    fn setup_datagram() -> Vec<u8> {
        use crate::protocol::SetupFrame;

        let mut frame = vec![0u8; SetupFrame::LENGTH];

        SetupFrame {
            term_offset: 0,
            session_id: 7,
            stream_id: 101,
            initial_term_id: 0,
            active_term_id: 0,
            term_length: 64 * 1024,
            mtu: 1408,
            ttl: 0,
        }
        .write(&mut frame)
        .expect("the frame fits");

        frame
    }

    #[test]
    fn a_blackout_passes_its_budget_then_withholds_for_a_while_and_ends_by_itself() {
        let mut generator = BlackoutAfterDataDatagrams::new(3, Duration::from_millis(10));
        let datagram = data_datagram(32);

        for index in 0..3 {
            assert!(
                !generator.should_drop(loopback(), &datagram, datagram.len()),
                "datagram {index} is inside the budget"
            );
        }

        assert!(
            generator.should_drop(loopback(), &datagram, datagram.len()),
            "the one after the budget opens the blackout"
        );
        assert!(
            generator.should_drop(loopback(), &datagram, datagram.len()),
            "and everything inside the window goes with it"
        );

        std::thread::sleep(Duration::from_millis(20));

        assert!(
            !generator.should_drop(loopback(), &datagram, datagram.len()),
            "a window that ends lets everything through again"
        );
        assert_eq!(2, generator.dropped());
        assert_eq!(
            4,
            generator.seen(),
            "the budget and the datagram that opened the blackout, and nothing after it"
        );
    }

    #[test]
    fn a_setup_does_not_spend_the_blackouts_budget() {
        let mut generator = BlackoutAfterDataDatagrams::new(1, Duration::from_secs(1));
        let setup = setup_datagram();
        let data = data_datagram(32);

        assert!(
            !generator.should_drop(loopback(), &setup, setup.len()),
            "a handshake passes and is not counted"
        );
        assert!(!generator.should_drop(loopback(), &data, data.len()));
        assert!(
            generator.should_drop(loopback(), &data, data.len()),
            "the second data datagram opens the blackout, not the SETUP"
        );
    }

    #[test]
    fn a_data_frame_that_is_only_its_header_does_not_count() {
        let mut generator = BlackoutAfterDataDatagrams::new(0, Duration::from_secs(1));
        let heartbeat = data_datagram(0);
        let data = data_datagram(32);

        assert!(
            !generator.should_drop(loopback(), &heartbeat, heartbeat.len()),
            "an EOS/REVOKE heartbeat is a DATA frame with no payload"
        );
        assert!(
            generator.should_drop(loopback(), &data, data.len()),
            "a budget of zero opens on the first datagram that carries a message"
        );
    }

    #[test]
    fn a_data_frame_after_a_control_frame_in_one_datagram_is_found() {
        let mut datagram = setup_datagram();
        datagram.extend_from_slice(&data_datagram(32));

        assert!(datagram_carries_data(&datagram));
        assert!(!datagram_carries_data(&setup_datagram()));
    }

    #[test]
    fn each_endpoint_gets_its_own_generator_and_its_own_count() {
        let loss = SendDataLoss::Blackout {
            pass_first: 1,
            hold: Duration::from_secs(1),
        };
        let mut first = loss.generator();
        let mut second = loss.generator();
        let data = data_datagram(32);

        assert!(!first.should_drop(loopback(), &data, data.len()));
        assert!(
            !second.should_drop(loopback(), &data, data.len()),
            "the second endpoint's budget is its own"
        );
        assert!(first.should_drop(loopback(), &data, data.len()));
        assert!(second.should_drop(loopback(), &data, data.len()));
    }
}
