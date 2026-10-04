//! Putting a batch of messages into a publication.
//!
//! Mirrors `benchmarks-aeron/src/main/java/io/aeron/benchmarks/aeron/MessageSender.java`.
//! Two ways of getting a message into a log, and the difference is what a run is
//! often measuring:
//!
//! - **Claim** — take space in the log, write the message *where it will lie*,
//!   publish it. The message is a timestamp, a receiver index and a checksum;
//!   the bytes between them are whatever the term already held and are never
//!   written at all.
//! - **Offer** — assemble the message in a buffer this sender keeps and offer
//!   that. Same bytes, one more copy, and no claim to publish.
//!
//! Which one a run uses is `io.aeron.benchmarks.aeron.use.try.claim`, and the
//! reference's default is to claim.

use deepmsg_core::buffer::ReadWrite;
use deepmsg_core::logbuffer::append::Appended;
use deepmsg_core::logbuffer::frame::Frame;

use crate::loadtest::config::IdleStrategy;
use crate::loadtest::transport::util::{
    MIN_MESSAGE_LENGTH, RECEIVER_INDEX_OFFSET, SEND_ATTEMPTS, TIMESTAMP_OFFSET,
    check_publication_result,
};

/// Where a sender puts one message.
///
/// The reference's senders hold an `ExclusivePublication` and call `tryClaim` or
/// `offer` on it. This is those two operations without the publication, so that
/// the retry and give-up behaviour below — which is what a bench run's back
/// pressure actually consists of — can be driven by something that says
/// `BACK_PRESSURED` on demand.
pub trait Outbound {
    /// Take `length` bytes in the log, hand the frame to `write`, and publish it.
    ///
    /// Returns what the publication said. Anything but [`Appended::Ok`] means
    /// nothing was published.
    fn claim<W: FnMut(&Frame<'_, ReadWrite>)>(&mut self, length: usize, write: W) -> Appended;

    /// Send `payload` as one message.
    fn offer(&mut self, payload: &[u8]) -> Appended;
}

/// Which of the reference's two senders this is
/// (`MessageSender.create`, `:67-80`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// `MessageSender.TryClaim`, the reference's default.
    Claim,
    /// `MessageSender.Offer`.
    Offer,
}

/// Where a message is written into a payload the caller has a slice of.
///
/// The three fields the reference's `preparePayload` writes
/// (`MessageSender.java:51-65`), for the offer path, which assembles the message
/// in a buffer of its own. [`write_into_frame`] is the same three fields for the
/// claim path, which writes them where they will lie; the two are kept from
/// drifting by a test that puts both through the same reader.
///
/// # Errors
///
/// `None` when `payload` is too short to hold a message — the reference's
/// `validateMessageLength` refuses one before a run starts, and this refuses one
/// that got here anyway.
pub fn prepare_payload(
    payload: &mut [u8],
    timestamp: i64,
    checksum: i64,
    receiver_index: i32,
) -> Option<()> {
    let message_length = payload.len();
    if message_length < MIN_MESSAGE_LENGTH {
        return None;
    }

    payload[TIMESTAMP_OFFSET..TIMESTAMP_OFFSET + 8].copy_from_slice(&timestamp.to_le_bytes());
    payload[RECEIVER_INDEX_OFFSET..RECEIVER_INDEX_OFFSET + 4]
        .copy_from_slice(&receiver_index.to_le_bytes());
    payload[message_length - 8..].copy_from_slice(&checksum.to_le_bytes());

    Some(())
}

/// The same three fields, written into a frame that has been claimed.
fn write_into_frame(
    frame: &Frame<'_, ReadWrite>,
    message_length: usize,
    timestamp: i64,
    checksum: i64,
    receiver_index: i32,
) -> Option<()> {
    frame.store_i64_in_payload(TIMESTAMP_OFFSET, timestamp)?;
    frame.store_i32_in_payload(RECEIVER_INDEX_OFFSET, receiver_index)?;
    frame.store_i64_in_payload(message_length - 8, checksum)
}

/// Which receiver the next message goes to.
///
/// `BitUtil.next(receiverIndex, numReceivers)`, which is how one message of a
/// batch reaches exactly one of a fan-out's receivers: the far end answers only
/// the messages whose index names it, so a batch of nine to three receivers
/// earns three replies and not nine.
#[derive(Clone, Copy, Debug)]
struct ReceiverIndex {
    index: i32,
    count: i32,
}

impl ReceiverIndex {
    fn new(count: i32) -> Self {
        Self {
            index: 0,
            count: count.max(1),
        }
    }

    /// The index to write, and then the one after it.
    ///
    /// Written before it advances, and once per message rather than once per
    /// attempt: a message that is retried goes to the receiver it was always
    /// going to.
    fn next(&mut self) -> i32 {
        let written = self.index;
        self.index = if written + 1 == self.count {
            0
        } else {
            written + 1
        };

        written
    }
}

/// Sends a run's messages, in batches, the way the reference's do.
#[derive(Debug)]
pub struct MessageSender {
    kind: Kind,
    receiver_index: ReceiverIndex,
    idle: IdleStrategy,
    /// The offer path's message, kept between messages so that assembling one
    /// does not allocate.
    scratch: Vec<u8>,
}

impl MessageSender {
    /// The sender a run's settings ask for.
    #[must_use]
    pub fn new(kind: Kind, idle: IdleStrategy, receiver_count: i32) -> Self {
        Self {
            kind,
            receiver_index: ReceiverIndex::new(receiver_count),
            idle,
            scratch: Vec::new(),
        }
    }

    /// Which sender this is.
    #[must_use]
    pub fn kind(&self) -> Kind {
        self.kind
    }

    /// Send up to `number_of_messages`, and say how many actually went.
    ///
    /// Less than asked for is not a failure: the rig sends the remainder as its
    /// next batch, which is the whole reason this returns a count. A publication
    /// that is back-pressured gets [`SEND_ATTEMPTS`] attempts per message; one
    /// that says something else ends the run, as the reference's throw does.
    ///
    /// # Panics
    ///
    /// When the publication is not merely slow — not connected, closed, out of
    /// position — with the reference's own message.
    pub fn send(
        &mut self,
        outbound: &mut impl Outbound,
        number_of_messages: usize,
        message_length: usize,
        timestamp: i64,
        checksum: i64,
    ) -> usize {
        let mut sent = 0;

        for _ in 0..number_of_messages {
            let receiver_index = self.receiver_index.next();
            let mut attempts = SEND_ATTEMPTS;

            loop {
                let outcome = match self.kind {
                    Kind::Claim => outbound.claim(message_length, |frame| {
                        let _ = write_into_frame(
                            frame,
                            message_length,
                            timestamp,
                            checksum,
                            receiver_index,
                        );
                    }),
                    Kind::Offer => self.offer(
                        outbound,
                        message_length,
                        timestamp,
                        checksum,
                        receiver_index,
                    ),
                };

                match outcome {
                    Appended::Ok { .. } => break,
                    other => match check_publication_result(other, &mut self.idle) {
                        // An administrative action: the log rotated under us.
                        // Try again, and do not spend an attempt on it.
                        Ok(true) => {}
                        Ok(false) => {
                            attempts -= 1;
                            if attempts == 0 {
                                return sent;
                            }
                        }
                        Err(error) => panic!("{error}"),
                    },
                }
            }

            sent += 1;
        }

        sent
    }

    /// Assemble one message in the buffer this sender keeps and offer it.
    fn offer(
        &mut self,
        outbound: &mut impl Outbound,
        message_length: usize,
        timestamp: i64,
        checksum: i64,
        receiver_index: i32,
    ) -> Appended {
        self.scratch.resize(message_length, 0);

        assert!(
            prepare_payload(&mut self.scratch, timestamp, checksum, receiver_index).is_some(),
            "a message of {message_length} bytes cannot hold a timestamp, a receiver index and a checksum"
        );

        outbound.offer(&self.scratch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    use deepmsg_core::buffer::AtomicBuffer;
    use deepmsg_core::logbuffer::frame::DATA_HEADER_LENGTH;

    /// A publication that says whatever the test tells it to, and keeps what it
    /// was given.
    #[derive(Debug)]
    struct ScriptedOutbound {
        outcomes: VecDeque<Appended>,
        last: Appended,
        /// Every payload that was actually published, in order.
        published: Vec<Vec<u8>>,
        /// The term a claim hands out.
        term: Vec<u8>,
    }

    impl ScriptedOutbound {
        fn new(outcomes: impl IntoIterator<Item = Appended>) -> Self {
            Self {
                outcomes: outcomes.into_iter().collect(),
                last: ok(),
                published: Vec::new(),
                term: vec![0; 4096],
            }
        }

        /// The next thing the publication says — its last answer once the script
        /// runs out, as a stubbed mock does.
        fn next_outcome(&mut self) -> Appended {
            match self.outcomes.pop_front() {
                Some(outcome) => {
                    self.last = outcome;
                    outcome
                }
                None => self.last,
            }
        }
    }

    impl Outbound for ScriptedOutbound {
        fn claim<W: FnMut(&Frame<'_, ReadWrite>)>(
            &mut self,
            length: usize,
            mut write: W,
        ) -> Appended {
            let outcome = self.next_outcome();

            if matches!(outcome, Appended::Ok { .. }) {
                {
                    let buffer = AtomicBuffer::from_slice_mut(&mut self.term).expect("a term");
                    write(&Frame::new(&buffer, 0));
                }

                self.published
                    .push(self.term[DATA_HEADER_LENGTH..DATA_HEADER_LENGTH + length].to_vec());
            }

            outcome
        }

        fn offer(&mut self, payload: &[u8]) -> Appended {
            let outcome = self.next_outcome();

            if matches!(outcome, Appended::Ok { .. }) {
                self.published.push(payload.to_vec());
            }

            outcome
        }
    }

    /// The publication's "yes": a position, which nothing here reads.
    fn ok() -> Appended {
        Appended::Ok {
            position: deepmsg_core::logbuffer::position::Position::from_term_count(0, 0, 0),
            term_offset: 0,
        }
    }

    fn receiver_index_of(payload: &[u8]) -> i32 {
        i32::from_le_bytes(
            payload[RECEIVER_INDEX_OFFSET..RECEIVER_INDEX_OFFSET + 4]
                .try_into()
                .expect("four bytes"),
        )
    }

    fn timestamp_of(payload: &[u8]) -> i64 {
        i64::from_le_bytes(
            payload[TIMESTAMP_OFFSET..TIMESTAMP_OFFSET + 8]
                .try_into()
                .expect("eight bytes"),
        )
    }

    fn checksum_of(payload: &[u8]) -> i64 {
        i64::from_le_bytes(
            payload[payload.len() - 8..]
                .try_into()
                .expect("eight bytes"),
        )
    }

    /// The reference's `shouldSendMultipleMessagesUsingOffer`, whose answer of
    /// three is what its retry arithmetic comes to: five messages, one
    /// administrative action that costs nothing, and three back-pressures on the
    /// fourth message that end the batch.
    #[test]
    fn an_offer_sender_gives_up_after_three_back_pressures() {
        let mut outbound = ScriptedOutbound::new([
            ok(),
            ok(),
            Appended::MidRotation,
            ok(),
            Appended::BackPressured,
            Appended::BackPressured,
            Appended::BackPressured,
        ]);
        let mut sender = MessageSender::new(Kind::Offer, IdleStrategy::NoOp, 3);

        let sent = sender.send(&mut outbound, 5, 288, 42, 0x0263_4823_3434);

        assert_eq!(sent, 3);
        assert_eq!(outbound.published.len(), 3);
        for (attempt, payload) in outbound.published.iter().enumerate() {
            assert_eq!(payload.len(), 288);
            assert_eq!(timestamp_of(payload), 42, "attempt {attempt}");
            assert_eq!(checksum_of(payload), 0x0263_4823_3434, "attempt {attempt}");
        }
        // The index advances once per *message*, not once per attempt, so the
        // administrative action on the third message does not move it.
        assert_eq!(receiver_index_of(&outbound.published[0]), 0);
        assert_eq!(receiver_index_of(&outbound.published[1]), 1);
        assert_eq!(receiver_index_of(&outbound.published[2]), 2);
    }

    /// The reference's `shouldSendMultipleMessagesUsingTryClaim`: nine messages,
    /// four sent, and the receiver index reaching the fifth receiver before the
    /// batch is cut short.
    #[test]
    fn a_claim_sender_gives_up_after_three_back_pressures() {
        let mut outbound = ScriptedOutbound::new([
            ok(),
            ok(),
            ok(),
            Appended::MidRotation,
            ok(),
            Appended::BackPressured,
            Appended::BackPressured,
            Appended::BackPressured,
        ]);
        let mut sender = MessageSender::new(Kind::Claim, IdleStrategy::NoOp, 5);

        let sent = sender.send(&mut outbound, 9, 1024, i64::MAX - 120_000_000, 42);

        assert_eq!(sent, 4);
        assert_eq!(outbound.published.len(), 4);

        let indices: Vec<i32> = outbound
            .published
            .iter()
            .map(|payload| receiver_index_of(payload))
            .collect();
        assert_eq!(indices, vec![0, 1, 2, 3]);
    }

    /// The two paths write the same message: one assembles it and copies it in,
    /// the other writes the three fields where the message will lie. Same bytes,
    /// or `use.try.claim` would be measuring two different things.
    #[test]
    fn both_paths_write_the_same_message() {
        const MESSAGE_LENGTH: usize = 288;

        let mut assembled = vec![0_u8; MESSAGE_LENGTH];
        prepare_payload(&mut assembled, 42, -7, 2).expect("long enough");

        let mut term = vec![0_u8; 4096];
        {
            let buffer = AtomicBuffer::from_slice_mut(&mut term).expect("a term");
            write_into_frame(&Frame::new(&buffer, 0), MESSAGE_LENGTH, 42, -7, 2)
                .expect("inside the term");
        }
        let claimed = &term[DATA_HEADER_LENGTH..DATA_HEADER_LENGTH + MESSAGE_LENGTH];

        assert_eq!(assembled.as_slice(), claimed);
        assert_eq!(timestamp_of(claimed), 42);
        assert_eq!(receiver_index_of(claimed), 2);
        assert_eq!(checksum_of(claimed), -7);
    }

    /// A message with nowhere to put its checksum is refused rather than written
    /// past the end of itself.
    #[test]
    fn a_message_too_short_for_its_fields_is_refused() {
        let mut payload = vec![0_u8; MIN_MESSAGE_LENGTH - 1];

        assert_eq!(prepare_payload(&mut payload, 1, 2, 3), None);
    }

    #[test]
    fn the_receiver_index_goes_round_and_starts_again() {
        let mut index = ReceiverIndex::new(3);

        assert_eq!(
            [
                index.next(),
                index.next(),
                index.next(),
                index.next(),
                index.next()
            ],
            [0, 1, 2, 0, 1]
        );
    }

    /// A publication that is not connected is not slow, it is broken, and the
    /// run ends with the reference's own words.
    #[test]
    #[should_panic(expected = "Publication error: Not connected")]
    fn a_publication_that_is_not_connected_ends_the_run() {
        let mut outbound = ScriptedOutbound::new([Appended::NotConnected]);
        let mut sender = MessageSender::new(Kind::Claim, IdleStrategy::NoOp, 1);

        sender.send(&mut outbound, 1, 32, 0, 0);
    }
}
