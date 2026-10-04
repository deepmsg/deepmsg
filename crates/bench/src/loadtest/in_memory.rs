//! A transceiver that talks to itself through a ring of longs.
//!
//! Mirrors `benchmarks-api/.../InMemoryMessageTransceiver.java`. It exists to
//! exercise the rig rather than a driver: nothing goes through a socket, a log
//! buffer or a driver, so what its tests measure is the rig's own arithmetic.
//!
//! # What is copied, and what is not
//!
//! The reference reaches into its `long[]` with `UnsafeApi` and reads and writes
//! it with acquire/release semantics, because it is written to be driven from
//! two threads. This indexes the same array safely and does not ask for
//! ordering. Neither changes what the ring *does* — a run drives it from one
//! thread, and the reference's own rig does too — and the alternative is
//! `unsafe` outside the modules `docs/adr/0002` allows it in.
//!
//! What is kept exactly is the *geometry*, because it is what gives the ring its
//! depth. A message takes eight slots: a timestamp, a checksum, and six the
//! reference never writes but that its check for room looks through. That is
//! `BitUtil.CACHE_LINE_LENGTH / SIZE_OF_LONG - 1` of padding, and it is why the
//! ring holds 512 messages rather than 2048 — a difference a run at half a
//! million messages a second would feel.

use crate::loadtest::config::Configuration;
use crate::loadtest::recorder::Recorder;
use crate::loadtest::transceiver::{Clock, MessageTransceiver, TransceiverError};

/// The ring's size in slots, which is `InMemoryMessageTransceiver.SIZE`.
pub const RING_SLOTS: usize = 4096;

/// How many slots one message occupies. See the module documentation.
const MESSAGE_SLOTS: usize = 8;

/// `SIZE - 1`, which is how the reference folds an ever-growing index back into
/// the array.
const RING_MASK: usize = RING_SLOTS - 1;

/// A transceiver that publishes into a ring and reads back out of it.
///
/// Single-threaded, and single-*position*: `send` writes forward from where the
/// last batch ended and `receive` reads forward from where the last one was
/// taken. Sending faster than receiving fills the ring and `send` starts
/// returning zero, which is the backpressure the rig's partial-batch handling
/// exists for.
#[derive(Clone, Debug)]
pub struct InMemoryTransceiver {
    messages: Vec<i64>,
    send_index: usize,
    receive_index: usize,
}

impl Default for InMemoryTransceiver {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryTransceiver {
    /// An empty ring, with both cursors at its start.
    #[must_use]
    pub fn new() -> Self {
        Self {
            messages: vec![0; RING_SLOTS],
            send_index: 0,
            receive_index: 0,
        }
    }

    /// The slot an index names, folded into the ring.
    ///
    /// An index is not bounded by the ring: it counts messages for the whole
    /// run, and only the *slot* it lands in wraps. Because every message takes
    /// eight slots and the ring is a multiple of eight, no message straddles the
    /// fold.
    fn slot(&self, index: usize) -> i64 {
        self.messages[index & RING_MASK]
    }

    fn set_slot(&mut self, index: usize, value: i64) {
        self.messages[index & RING_MASK] = value;
    }
}

/// Where the `n`th message of a batch starts, relative to the batch's first.
///
/// `InMemoryMessageTransceiver.messageIndexOffset`.
fn message_offset(n: usize) -> usize {
    (n - 1) * MESSAGE_SLOTS
}

impl<C: Clock> MessageTransceiver<C> for InMemoryTransceiver {
    fn init(&mut self, _configuration: &Configuration) -> Result<(), TransceiverError> {
        self.messages.fill(0);
        self.send_index = 0;
        self.receive_index = 0;

        Ok(())
    }

    fn destroy(&mut self) -> Result<(), TransceiverError> {
        self.messages.fill(0);

        Ok(())
    }

    fn send(
        &mut self,
        number_of_messages: usize,
        _message_length: usize,
        timestamp: i64,
        checksum: i64,
        _recorder: &mut Recorder<C>,
    ) -> usize {
        // Nothing sends an empty batch, and the reference's arithmetic would
        // index before the start of the ring if one did.
        if number_of_messages == 0 {
            return 0;
        }

        let index = self.send_index;

        // Room for the whole batch, asked of the one slot that says so: the
        // checksum of its last message, which the receiver clears as it passes.
        // A batch is never split — a partial write would leave the receiver
        // reading a message whose checksum is not there yet.
        if self.slot(index + 1 + message_offset(number_of_messages)) != 0 {
            return 0;
        }

        // Backwards, so that the last message's checksum — the one the check
        // above looks at — is the last thing written.
        for i in (2..=number_of_messages).rev() {
            self.set_slot(index + message_offset(i), timestamp);
            self.set_slot(index + 1 + message_offset(i), checksum);
        }
        self.set_slot(index, timestamp);
        self.set_slot(index + 1, checksum);

        self.send_index += message_offset(number_of_messages + 1);

        number_of_messages
    }

    fn receive(&mut self, recorder: &mut Recorder<C>) {
        let checksum_offset = self.receive_index + 1;
        let checksum = self.slot(checksum_offset);

        if checksum == 0 {
            return;
        }

        let timestamp_offset = self.receive_index;
        let timestamp = self.slot(timestamp_offset);

        // Cleared before the message is handed on, so that the slot means
        // "consumed" from the moment the receiver may send again.
        self.set_slot(timestamp_offset, 0);
        self.set_slot(checksum_offset, 0);

        recorder.on_message_received(timestamp, checksum);

        self.receive_index += MESSAGE_SLOTS;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::loadtest::recorder::{Recorder, checksum};
    use crate::loadtest::result;

    /// A clock that always reads the same, which is all these tests need.
    #[derive(Clone, Copy, Debug, Default)]
    struct FixedClock(i64);

    impl Clock for FixedClock {
        fn nano_time(&self) -> i64 {
            self.0
        }
    }

    /// A recorder whose clock reads `now`, so a round trip of a known-timestamped
    /// message is `now - timestamp`.
    fn recorder_at(now: i64) -> Recorder<FixedClock> {
        Recorder::new(result::histogram(), checksum(), FixedClock(now))
    }

    #[test]
    fn a_single_message_is_sent() {
        let mut transceiver = InMemoryTransceiver::new();
        let mut recorder = recorder_at(1500);

        assert_eq!(transceiver.send(1, 16, 123, i64::MIN, &mut recorder), 1);
    }

    #[test]
    fn a_batch_is_sent_whole() {
        let mut transceiver = InMemoryTransceiver::new();
        let mut recorder = recorder_at(1500);

        assert_eq!(transceiver.send(4, 64, 800, -222, &mut recorder), 4);
    }

    /// The ring's depth is 512 messages, and a batch of `RING_SLOTS` is far more
    /// than that: the reference's own test sends it and expects the ring to be
    /// full afterwards.
    #[test]
    fn sending_more_than_the_ring_holds_leaves_no_room() {
        let mut transceiver = InMemoryTransceiver::new();
        let mut recorder = recorder_at(1500);

        transceiver.send(RING_SLOTS, 8, 777, 100, &mut recorder);

        assert_eq!(transceiver.send(1, 100, 555, 21, &mut recorder), 0);
    }

    #[test]
    fn receiving_with_nothing_written_does_nothing() {
        let mut transceiver = InMemoryTransceiver::new();
        let mut recorder = recorder_at(1500);

        transceiver.receive(&mut recorder);

        assert_eq!(recorder.received_messages(), 0);
        assert_eq!(recorder.histogram().len(), 0);
    }

    #[test]
    fn receiving_after_everything_is_consumed_does_nothing_more() {
        let mut transceiver = InMemoryTransceiver::new();
        let mut recorder = recorder_at(1500);

        transceiver.send(5, 128, 1111, checksum(), &mut recorder);

        for _ in 0..5 {
            transceiver.receive(&mut recorder);
        }
        transceiver.receive(&mut recorder);

        assert_eq!(recorder.received_messages(), 5);
        // Every round trip is 1500 - 1111.
        assert_eq!(recorder.histogram().len(), 5);
        assert_eq!(recorder.histogram().max(), 389);
        assert_eq!(recorder.histogram().min(), 389);
    }

    /// In through one end and out of the other, in the order they went in.
    ///
    /// The histogram is a multiset — `iter_recorded` hands back values in
    /// ascending order, not in the order they arrived — so arrival order is not
    /// visible in it after the fact. Asking after *every single* receive is: the
    /// count has to grow by one, and the newest value has to be the one just
    /// sent. The reference's own test can compare a hundred thousand timestamps
    /// one by one only because its recorder is a mock that sees each value as it
    /// goes past.
    ///
    /// Kept small on purpose, too: at these values the histogram's buckets are
    /// one nanosecond wide, so a reading is its own value and not a bucket
    /// shared with its neighbours.
    #[test]
    fn messages_come_back_in_the_order_they_went_in() {
        let mut transceiver = InMemoryTransceiver::new();
        let mut recorder = recorder_at(1500);

        for timestamp in 1..=10_i64 {
            assert_eq!(
                transceiver.send(1, 24, timestamp, checksum(), &mut recorder),
                1
            );
            transceiver.receive(&mut recorder);

            assert_eq!(
                recorder.received_messages(),
                timestamp,
                "the {timestamp}th message is the one that should have arrived"
            );
            assert_eq!(
                recorder
                    .histogram()
                    .count_at(u64::try_from(1500 - timestamp).expect("positive")),
                1
            );
        }

        assert_eq!(recorder.histogram().len(), 10);
    }

    /// More messages than the ring holds, with the receiver keeping up: nothing
    /// is lost and nothing arrives twice.
    #[test]
    fn messages_survive_going_round_the_ring() {
        const COUNT: i64 = 20_000;
        const NOW: i64 = 100_000;

        let mut transceiver = InMemoryTransceiver::new();
        let mut recorder = recorder_at(NOW);

        for timestamp in 1..=COUNT {
            while transceiver.send(1, 24, timestamp, checksum(), &mut recorder) == 0 {
                transceiver.receive(&mut recorder);
            }
        }

        while recorder.received_messages() < COUNT {
            transceiver.receive(&mut recorder);
        }

        assert_eq!(recorder.received_messages(), COUNT);
        assert_eq!(
            recorder.histogram().len(),
            u64::try_from(COUNT).expect("positive")
        );

        // Both ends of the run arrived, and nothing arrived twice. Counts are
        // per bucket rather than per value at this size, so the ends are asked
        // about with `> 0`; what says nothing was lost is the total.
        for timestamp in [1, COUNT] {
            assert!(
                recorder
                    .histogram()
                    .count_at(u64::try_from(NOW - timestamp).expect("positive"))
                    > 0,
                "the message sent at {timestamp} never came back"
            );
        }
    }
}
