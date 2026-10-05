//! Whole messages out of the fragments that carried them.
//!
//! Mirrors `aeron-client/src/main/c/aeron_fragment_assembler.c` and the
//! `aeron_buffer_builder_t` it assembles into
//! (`aeron-client/src/main/c/aeron_fragment_assembler.h:26-140`). (M05)
//!
//! # The rule
//!
//! A message that fits one frame carries `BEGIN|END` and is passed straight
//! through. Anything else arrives as a run of frames: the first carries
//! `BEGIN`, the last carries `END`, and each one's term offset is exactly where
//! the previous one ended — which is the whole of the continuity test
//! (`:170`, `:251`). A fragment that does not line up is a fragment whose
//! predecessors were overwritten or never read, and the message it belonged to
//! is abandoned.
//!
//! The key is the **session id**, not the stream: two publications can carry
//! one stream at the same time, and their fragments must never be assembled
//! into one message. That keying is the **Java** assembler's rule — one builder
//! per session id, its `builderBySessionIdMap`
//! (`aeron-client/src/main/java/io/aeron/FragmentAssembler.java:46`) — not the
//! C file this module otherwise mirrors: the C assembler keeps a single
//! builder, and one session's `BEGIN` resets another's run
//! (`aeron-client/src/main/c/aeron_fragment_assembler.c:152-188`). The
//! stronger rule is deliberate, and the difference is recorded in
//! `docs/compat.md`. A builder is kept per session and reused, so the second
//! message on a session assembles into the same buffer as the first.
//!
//! # Why the payload is copied
//!
//! [`Message::payload`] points into the assembler's own buffer, so every
//! message is copied once. The reference passes a pointer into the term for an
//! unfragmented message and copies only the fragmented ones; this copies both,
//! because a `&[u8]` over the term is a reference into memory that a producer —
//! possibly *this* process's own, in every test that publishes and subscribes
//! at once — is still writing. `deepmsg-core`'s buffer API hands out no such
//! slice on purpose, and this is the same rule one layer up. The copy goes into
//! a buffer that is reused, so nothing is allocated after the first message of
//! a session.

use std::collections::HashMap;

use deepmsg_core::logbuffer::descriptor;
use deepmsg_core::logbuffer::frame::{DATA_HEADER_LENGTH, FLAG_BEGIN, FLAG_END, FLAG_UNFRAGMENTED};

use crate::image::Fragment;

/// The header a whole message carries, rewritten for the assembled whole.
///
/// The reference copies the first fragment's header and then edits two fields
/// of it (`aeron_buffer_builder_complete_header`,
/// `aeron_fragment_assembler.h:113-127`); this is the same information in a
/// struct that no longer pretends to be a frame in a term.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MessageHeader {
    /// The publication's session.
    pub session_id: i32,
    /// The stream.
    pub stream_id: i32,
    /// Where the first fragment began in its term.
    pub term_offset: i32,
    /// The flags as the message ends: the first fragment's, with the **last**
    /// fragment's OR-ed in, which is what gives an assembled message its `END`
    /// bit (`aeron_fragment_assembler.h:124`).
    pub flags: u8,
    /// Where the message began in the stream — the first fragment's position.
    pub position: i64,
    /// The frame length of the message **as assembled**: the header plus the
    /// whole payload, whatever it took in fragments.
    pub frame_length: i32,
    /// What one unfragmented frame carrying this payload would have measured
    /// (`aeron_logbuffer_compute_fragmented_length`, called at
    /// `aeron_fragment_assembler.h:120-121`).
    pub fragmented_frame_length: i32,
}

/// One whole message.
///
/// `Copy` so that a handler takes it by value: a reference to a type with a
/// lifetime of its own makes every handler bound higher-ranked, and the
/// compiler says so — "implementation of `FnMut` is not general enough" —
/// rather than guessing what the caller meant.
#[derive(Clone, Copy, Debug)]
pub struct Message<'a> {
    /// What the message is, and where it began.
    pub header: MessageHeader,
    /// The payload, whole. Borrowed from the assembler, so it lives only as
    /// long as the handler call it arrives in.
    pub payload: &'a [u8],
}

/// What one session's fragments are being assembled into.
#[derive(Debug, Default)]
struct Builder {
    buffer: Vec<u8>,
    /// Where the next fragment of the message in progress has to begin, or
    /// `None` when no message is in progress.
    next_term_offset: Option<i32>,
    /// The first fragment's header, kept until the last one arrives.
    header: Option<MessageHeader>,
}

/// What one session's fragments are being assembled into.
impl Builder {
    /// Forget the message in progress, keeping the buffer.
    fn reset(&mut self) {
        self.buffer.clear();
        self.next_term_offset = None;
        self.header = None;
    }
}

/// Reassembles the fragments of one or more sessions into whole messages.
///
/// One assembler per subscription is the arrangement the reference's samples
/// use (`basic_subscriber.c` creates one per subscription), and it is the right
/// one here too: the builders are keyed by session, and two subscriptions that
/// read different streams through one assembler would still be correct — but
/// their messages would share the buffer, and the buffer is what a handler is
/// handed.
#[derive(Debug, Default)]
pub struct FragmentAssembler {
    builders: HashMap<i32, Builder>,
    /// The buffer an unfragmented message is copied into, so that every
    /// handler sees the same kind of `&[u8]`.
    passthrough: Vec<u8>,
    /// Messages delivered whole since this assembler was created.
    delivered: u64,
    /// Messages that were abandoned: a fragment arrived that did not continue
    /// the one before it, or that continued nothing at all. The reference drops
    /// these silently; counting them is this build's addition (ADR-0003: a
    /// skipped input is a counted one), and the count is what tells a caller
    /// that a message it never sees is a message that was lost rather than one
    /// that was never sent.
    abandoned: u64,
}

impl FragmentAssembler {
    /// No fragments seen yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// How many whole messages this assembler has delivered.
    pub const fn delivered(&self) -> u64 {
        self.delivered
    }

    /// How many messages were abandoned mid-assembly.
    pub const fn abandoned(&self) -> u64 {
        self.abandoned
    }

    /// Feed one fragment; call `handler` for every whole message it completes.
    ///
    /// The handler may be called at most once per fragment, and the message it
    /// is given borrows this assembler — so it cannot be kept, which is what
    /// keeps the buffer underneath it honest.
    pub fn push<F>(&mut self, fragment: &Fragment<'_>, handler: &mut F)
    where
        F: FnMut(Message<'_>),
    {
        let (Some(session_id), Some(stream_id), Some(term_offset)) = (
            fragment.session_id(),
            fragment.stream_id(),
            fragment.term_offset(),
        ) else {
            // A fragment whose header cannot be read is not a fragment this
            // build can assemble; it is counted and dropped rather than
            // guessed at.
            self.abandoned += 1;
            return;
        };

        let Some(flags) = fragment.flags() else {
            self.abandoned += 1;
            return;
        };

        if flags & FLAG_UNFRAGMENTED == FLAG_UNFRAGMENTED {
            self.deliver_unfragmented(fragment, session_id, stream_id, term_offset, flags, handler);
            return;
        }

        let Some(next_term_offset) = fragment.next_term_offset() else {
            self.abandoned += 1;
            return;
        };

        if flags & FLAG_BEGIN == FLAG_BEGIN {
            self.begin(
                fragment,
                session_id,
                stream_id,
                term_offset,
                next_term_offset,
                flags,
            );
            return;
        }

        // A middle or last fragment: it continues a message only if it begins
        // exactly where the previous one ended.
        let Some(builder) = self.builders.get_mut(&session_id) else {
            // Nothing was being assembled for this session: this is the tail of
            // a message whose beginning was never seen.
            self.abandoned += 1;
            return;
        };

        if builder.next_term_offset != Some(term_offset) {
            // The gap. The message in progress can never be completed — a
            // fragment it needed is gone — so it is abandoned here rather than
            // delivered short.
            builder.reset();
            self.abandoned += 1;
            return;
        }

        let Some(header) = builder.header.as_mut() else {
            builder.reset();
            self.abandoned += 1;
            return;
        };

        header.flags |= flags;

        if !append(fragment, &mut builder.buffer) {
            builder.reset();
            self.abandoned += 1;
            return;
        }

        if flags & FLAG_END == FLAG_END {
            let Some(header) = builder.header.take() else {
                builder.reset();
                self.abandoned += 1;
                return;
            };

            let header = complete(header, builder.buffer.len(), fragment);

            // The borrow of the builder's buffer ends with this call, which is
            // what lets the reset below happen — and the handler is what the
            // borrow is for, so nothing else may touch the buffer while it
            // holds it.
            handler(Message {
                header,
                payload: &builder.buffer,
            });

            builder.reset();
        } else {
            builder.next_term_offset = fragment.next_term_offset();
        }
    }

    /// A message that arrived in one frame.
    ///
    /// Copied into the passthrough buffer rather than handed out as a slice of
    /// the term — see the module note. The session's builder is deliberately
    /// **not** touched: a fragmented message may be in flight for the same
    /// session, and the reference passes the whole one through without
    /// disturbing it (`aeron_fragment_assembler.c:158-161`).
    fn deliver_unfragmented<F>(
        &mut self,
        fragment: &Fragment<'_>,
        session_id: i32,
        stream_id: i32,
        term_offset: i32,
        flags: u8,
        handler: &mut F,
    ) where
        F: FnMut(Message<'_>),
    {
        self.passthrough.clear();
        if !append(fragment, &mut self.passthrough) {
            self.abandoned += 1;
            return;
        }

        let header = MessageHeader {
            session_id,
            stream_id,
            term_offset,
            flags,
            position: fragment.position(),
            frame_length: frame_length_for(self.passthrough.len()),
            fragmented_frame_length: fragmented_frame_length(
                self.passthrough.len(),
                self.passthrough.len(),
            ),
        };

        self.delivered += 1;
        handler(Message {
            header,
            payload: &self.passthrough,
        });
    }

    /// Start a message.
    fn begin(
        &mut self,
        fragment: &Fragment<'_>,
        session_id: i32,
        stream_id: i32,
        term_offset: i32,
        next_term_offset: i32,
        flags: u8,
    ) {
        let builder = self.builders.entry(session_id).or_default();

        // A `BEGIN` while a message is in progress replaces it: the previous
        // one lost a fragment, and this one is the message the stream is
        // actually on (`aeron_fragment_assembler.c:175-181`).
        if builder.next_term_offset.is_some() {
            self.abandoned += 1;
        }

        builder.reset();
        builder.buffer.clear();

        if !append(fragment, &mut builder.buffer) {
            builder.reset();
            self.abandoned += 1;
            return;
        }

        builder.header = Some(MessageHeader {
            session_id,
            stream_id,
            term_offset,
            flags,
            position: fragment.position(),
            frame_length: frame_length_for(builder.buffer.len()),
            fragmented_frame_length: fragmented_frame_length(
                builder.buffer.len(),
                fragment.payload_length(),
            ),
        });
        builder.next_term_offset = Some(next_term_offset);
    }
}

/// Append one fragment's payload to the buffer an assembler owns.
fn append(fragment: &Fragment<'_>, buffer: &mut Vec<u8>) -> bool {
    let length = fragment.payload_length();
    let prior_length = buffer.len();

    buffer.resize(prior_length + length, 0);

    match fragment.copy_payload(&mut buffer[prior_length..]) {
        Some(()) => true,
        None => {
            buffer.truncate(prior_length);
            false
        }
    }
}

/// The frame length of a message whose payload is this long.
fn frame_length_for(payload_length: usize) -> i32 {
    #[allow(clippy::cast_possible_truncation)] // bounded by a term's length
    let frame_length = payload_length + DATA_HEADER_LENGTH;

    #[allow(clippy::cast_possible_truncation)] // bounded by a term's length
    let frame_length = frame_length as i32;

    frame_length
}

/// What one frame would have measured carrying the whole payload
/// (`aeron_logbuffer_compute_fragmented_length`, `descriptor.rs`).
fn fragmented_frame_length(payload_length: usize, max_payload_length: usize) -> i32 {
    let length = if max_payload_length == 0 {
        // A message that fits no frame at all: the reference divides by the
        // maximum payload length and would divide by zero here.
        payload_length + DATA_HEADER_LENGTH
    } else {
        descriptor::compute_fragmented_length(payload_length, max_payload_length)
    };

    #[allow(clippy::cast_possible_truncation)] // a message length, far below i32::MAX
    let length = length as i32;

    length
}

/// Rewrite the first fragment's header for the assembled whole
/// (`aeron_buffer_builder_complete_header`, `aeron_fragment_assembler.h:113-127`).
fn complete(
    mut header: MessageHeader,
    payload_length: usize,
    last: &Fragment<'_>,
) -> MessageHeader {
    let first_frame_length = header.frame_length;
    let max_payload_length =
        usize::try_from(first_frame_length - DATA_HEADER_LENGTH as i32).unwrap_or(0);

    header.frame_length = frame_length_for(payload_length);
    header.fragmented_frame_length = fragmented_frame_length(payload_length, max_payload_length);
    header.flags |= last.flags().unwrap_or(0);

    header
}

/// What a controlled handler tells the poll to do with the message it just saw.
///
/// The reference has four of these and names them the same way
/// (`ControlledFragmentHandler.java:30-53`; C spells them
/// `AERON_ACTION_ABORT`…`AERON_ACTION_CONTINUE`, `aeronc.h:1719-1737`).
///
/// **The numbers are not part of the contract.** The two reference
/// implementations agree on the order and disagree on where it starts — Java
/// numbers them 0…3, C 1…4 — so a Rust enum that carried a discriminant would
/// be picking a side that does not exist. What is a contract is the *meaning*
/// of each, which the three predicates below state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action {
    /// Do not consume this message, and stop. The reader's position stays
    /// where it was, so the same message arrives again on the next poll.
    Abort,
    /// Consume this message, then stop. The position moves past it.
    Break,
    /// Consume this message, publish the position **here** rather than at the
    /// end, and carry on. Publishing mid-poll is the whole of what `Commit`
    /// means: flow control sees the reader has got this far.
    Commit,
    /// Consume this message and carry on; the position is published once, when
    /// the poll ends.
    Continue,
}

impl Action {
    /// Whether the message is consumed.
    ///
    /// The C loop is where this is visible: `ABORT` walks the offset back by
    /// the frame it had already stepped over and gives the fragment count back
    /// (`aeron_image.c`, the `AERON_ACTION_ABORT` arm). Nothing else does.
    pub const fn consumes(self) -> bool {
        !matches!(self, Self::Abort)
    }

    /// Whether the poll stops after this message.
    pub const fn stops(self) -> bool {
        matches!(self, Self::Abort | Self::Break)
    }

    /// Whether the position is published at this message instead of at the end.
    ///
    /// Exactly one action asks for it, and that is what makes it worth a
    /// predicate: `ABORT` and `BREAK` also end the poll early, and a reader that
    /// conflated "stopped" with "published here" would move the position over
    /// an aborted message.
    pub const fn publishes_now(self) -> bool {
        matches!(self, Self::Commit)
    }
}

/// What a controlled poll calls: look at a message, say what to do with it.
///
/// `on_message` rather than the reference's `onFragment`, and the name is the
/// difference: this build's controlled face delivers **whole messages** and not
/// the fragments that carried them. The reason is in `docs/compat.md` — a
/// handler here is handed a slice that belongs to this process, so the
/// reference's zero-copy forwarding of term memory is not something this build
/// can offer. What it offers instead is the *decision*, which is what every
/// consumer of this face actually uses: the archive's eight pollers all decode
/// the message in place and none of them forwards it.
pub trait ControlledHandler {
    /// Say what the poll should do with this message.
    fn on_message(&mut self, message: Message<'_>) -> Action;
}

/// A closure is a handler, so the common case needs no type of its own.
///
/// The reference gets this from an interface a lambda satisfies; here it is a
/// blanket implementation, which is the same thing said in Rust.
impl<F> ControlledHandler for F
where
    F: FnMut(Message<'_>) -> Action,
{
    fn on_message(&mut self, message: Message<'_>) -> Action {
        self(message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use deepmsg_core::buffer::AtomicBuffer;
    use deepmsg_core::logbuffer::append::{Appended, Appender};
    use deepmsg_core::logbuffer::frame;
    use deepmsg_core::logbuffer::position;

    use crate::image::Fragment;

    /// The plan's table, as the three predicates. Four actions and four
    /// readings — an enum with two rows alike would be an enum with a spare
    /// variant, and the predicates exist to be the thing a poll switches on.
    #[test]
    fn the_four_actions_are_four_readings_of_one_poll() {
        use Action::{Abort, Break, Commit, Continue};

        let table = [
            // action, consumes, stops, publishes_now
            (Abort, false, true, false),
            (Break, true, true, false),
            (Commit, true, false, true),
            (Continue, true, false, false),
        ];

        let mut readings = Vec::new();
        for (action, consumes, stops, publishes_now) in table {
            assert_eq!(consumes, action.consumes(), "{action:?} consumes");
            assert_eq!(stops, action.stops(), "{action:?} stops");
            assert_eq!(
                publishes_now,
                action.publishes_now(),
                "{action:?} publishes now"
            );
            readings.push((consumes, stops, publishes_now));
        }

        let mut unique = readings.clone();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(
            readings.len(),
            unique.len(),
            "two actions read the same: {readings:?}"
        );
    }

    /// `Commit` is the only one that asks for the position early, and it does
    /// not stop. Keeping those two apart is the whole point of three
    /// predicates rather than one: `Abort` and `Break` also end the poll, and a
    /// reader that took "stopped" for "published here" would move the position
    /// over a message it refused.
    #[test]
    fn committing_early_is_not_the_same_as_stopping_early() {
        assert!(Action::Commit.publishes_now());
        assert!(!Action::Commit.stops());

        assert!(Action::Abort.stops());
        assert!(!Action::Abort.publishes_now());
        assert!(Action::Break.stops());
        assert!(!Action::Break.publishes_now());
    }

    /// The blanket implementation, which is what lets a caller hand a closure
    /// where the reference hands a lambda.
    #[test]
    fn a_closure_is_a_controlled_handler() {
        fn deliver<H: ControlledHandler>(handler: &mut H, message: Message<'_>) -> Action {
            handler.on_message(message)
        }

        let seen = std::cell::Cell::new(0);
        let mut closure = |message: Message<'_>| {
            seen.set(seen.get() + message.payload.len());
            Action::Break
        };

        let message = Message {
            header: MessageHeader {
                session_id: 7,
                stream_id: 1,
                term_offset: 0,
                flags: 0,
                position: 0,
                frame_length: 0,
                fragmented_frame_length: 0,
            },
            payload: b"hello",
        };

        assert_eq!(Action::Break, deliver(&mut closure, message));
        assert_eq!(5, seen.get());
    }

    /// The smallest legal term length, so the fixture stays small.
    const TERM_LENGTH: i32 = 64 * 1024;
    const INITIAL_TERM_ID: i32 = 17;

    #[repr(align(64))]
    struct Buffer<const N: usize>([u8; N]);

    /// A term with frames written into it, and where each of them begins.
    ///
    /// The frames are written by the **same appender a publication writes
    /// with**, so the assembler is tested against real frames — with the real
    /// flag bits, the real alignment and the real fragmentation — rather than
    /// against a description of them.
    struct Log {
        metadata: Buffer<{ descriptor::METADATA_STRUCT_LENGTH }>,
        term: Box<Buffer<{ TERM_LENGTH as usize }>>,
        frames: Vec<usize>,
    }

    impl Log {
        fn new() -> Self {
            let mut log = Self {
                metadata: Buffer([0u8; descriptor::METADATA_STRUCT_LENGTH]),
                // Boxed: 64 KiB is a legal term length and a large stack frame.
                term: Box::new(Buffer([0u8; TERM_LENGTH as usize])),
                frames: Vec::new(),
            };

            {
                let meta = AtomicBuffer::from_slice_mut(&mut log.metadata.0).expect("aligned");
                meta.store_i32_relaxed(descriptor::TERM_LENGTH_OFFSET, TERM_LENGTH)
                    .expect("in range");
                meta.store_i32_relaxed(descriptor::INITIAL_TERM_ID_OFFSET, INITIAL_TERM_ID)
                    .expect("in range");
                meta.store_i32_relaxed(
                    descriptor::MTU_LENGTH_OFFSET,
                    descriptor::MTU_LENGTH_DEFAULT,
                )
                .expect("in range");
                meta.store_i32_relaxed(descriptor::IS_CONNECTED_OFFSET, 1)
                    .expect("in range");
            }

            // The tails, which the driver writes when it creates the log: from
            // nothing but a term length and an initial term id, an appender has
            // no way to know which term its term 0 is, and it says so with
            // `MidRotation` rather than writing into a term it cannot place.
            {
                let metadata = AtomicBuffer::from_slice_mut(&mut log.metadata.0).expect("aligned");
                let term = AtomicBuffer::from_slice_mut(&mut log.term.0).expect("aligned");
                let appender = Appender::new(metadata, term).expect("a usable log");
                assert!(appender.initialise_tails(INITIAL_TERM_ID));
            }

            log
        }

        /// Append one message and remember where its frames begin.
        fn write(&mut self, session_id: i32, stream_id: i32, payload: &[u8]) {
            let metadata = AtomicBuffer::from_slice_mut(&mut self.metadata.0).expect("aligned");
            let term = AtomicBuffer::from_slice_mut(&mut self.term.0).expect("aligned");
            let appender = Appender::new(metadata, term).expect("a usable log");
            let appended = appender.append(session_id, stream_id, i64::MAX, payload);

            assert!(matches!(appended, Appended::Ok { .. }), "{appended:?}");

            self.collect_frames();
        }

        /// Walk the term, noting where each data frame begins.
        ///
        /// From the beginning every time rather than from where it left off:
        /// the offsets are what the tests index by, and a list with yesterday's
        /// frames still in it would index them by the wrong ones.
        fn collect_frames(&mut self) {
            let view = AtomicBuffer::from_slice(&self.term.0).expect("aligned");
            self.frames.clear();
            let mut offset = 0usize;

            while offset + frame::DATA_HEADER_LENGTH <= TERM_LENGTH as usize {
                let candidate = frame::Frame::new(&view, offset);
                let Some(length) = candidate.frame_length() else {
                    break;
                };
                if length <= 0 {
                    break;
                }

                if !candidate.is_padding() {
                    self.frames.push(offset);
                }

                offset += usize::try_from(position::align_up(length, descriptor::FRAME_ALIGNMENT))
                    .expect("a frame fits a term");
            }
        }

        /// Feed the frame at `index` to an assembler, as a subscriber would.
        ///
        /// The buffer view is built inside this call because a `Fragment`
        /// borrows the buffer it reads through, and the buffer borrows the
        /// term: neither may outlive the frame it describes.
        fn push_into<F>(&self, index: usize, assembler: &mut FragmentAssembler, handler: &mut F)
        where
            F: FnMut(Message<'_>),
        {
            self.push_offset_into(self.frames[index], assembler, handler);
        }

        /// The same, for a frame at an offset no message wrote — which is what
        /// a frame that arrived out of order looks like.
        fn push_offset_into<F>(
            &self,
            offset: usize,
            assembler: &mut FragmentAssembler,
            handler: &mut F,
        ) where
            F: FnMut(Message<'_>),
        {
            let view = AtomicBuffer::from_slice(&self.term.0).expect("aligned");
            let fragment = Fragment::new(
                frame::Frame::new(&view, offset),
                i64::from(i32::try_from(offset).expect("a term fits an i32")),
            );

            assembler.push(&fragment, handler);
        }
    }

    #[test]
    fn a_whole_message_is_passed_through() {
        let mut log = Log::new();
        log.write(7, 1, b"hello");

        let mut assembler = FragmentAssembler::new();
        let mut seen = Vec::new();
        let mut handler = |message: Message<'_>| {
            seen.push((message.header.session_id, message.payload.to_vec()));
        };

        log.push_into(0, &mut assembler, &mut handler);

        assert_eq!(vec![(7, b"hello".to_vec())], seen);
        assert_eq!(0, assembler.abandoned());
    }

    #[test]
    fn a_message_in_two_fragments_arrives_whole() {
        let mut log = Log::new();
        // 24 bytes of payload per frame: 1376 is what one frame holds, so this
        // is three frames — the appender's own fragmentation, not a test's.
        let payload = vec![b'a'; 1376 + 64];
        log.write(7, 1, &payload);
        assert_eq!(2, log.frames.len(), "one frame's worth and a remainder");

        let mut assembler = FragmentAssembler::new();
        let mut seen = Vec::new();
        {
            // The closure holds the borrow of `seen` for as long as it lives,
            // which is why each push that a test wants to look at afterwards
            // sits in a block of its own.
            let mut handler = |message: Message<'_>| seen.push(message.payload.to_vec());
            log.push_into(0, &mut assembler, &mut handler);
        }
        assert!(seen.is_empty(), "half a message is no message");

        {
            let mut handler = |message: Message<'_>| seen.push(message.payload.to_vec());
            log.push_into(1, &mut assembler, &mut handler);
        }

        assert_eq!(vec![payload], seen);
        assert_eq!(0, assembler.abandoned());
    }

    #[test]
    fn a_message_in_many_fragments_keeps_its_first_header_and_its_last_flags() {
        let mut log = Log::new();
        let payload = vec![b'a'; 1376 * 3 + 10];
        log.write(7, 1, &payload);
        assert_eq!(4, log.frames.len());

        let mut assembler = FragmentAssembler::new();
        let mut headers = Vec::new();
        let mut delivered = Vec::new();
        let mut handler = |message: Message<'_>| {
            headers.push(message.header);
            delivered.push(message.payload.to_vec());
        };

        for index in 0..log.frames.len() {
            log.push_into(index, &mut assembler, &mut handler);
        }

        assert_eq!(vec![payload], delivered);
        let header = headers[0];
        assert_eq!(7, header.session_id);
        assert_eq!(1, header.stream_id);
        assert_eq!(0, header.term_offset, "the first fragment's");
        assert_eq!(
            frame::FLAG_BEGIN | frame::FLAG_END,
            header.flags,
            "the last fragment's flags are OR-ed into the first's"
        );
        assert_eq!(
            frame::DATA_HEADER_LENGTH as i32 + 1376 * 3 + 10,
            header.frame_length,
            "the assembled length, not the first fragment's"
        );
    }

    #[test]
    fn a_gap_abandons_the_message_instead_of_delivering_it_short() {
        let mut log = Log::new();
        // Two fragmented messages: the first was overwritten by the second,
        // which is what a publisher produces when its consumer falls further
        // behind than a term can hold.
        let overwritten = vec![b'a'; 1376 + 64];
        let overwriting = vec![b'b'; 1376 + 64];
        log.write(7, 1, &overwritten);
        log.write(7, 1, &overwriting);
        assert_eq!(4, log.frames.len());

        let mut assembler = FragmentAssembler::new();
        let mut seen = 0;

        {
            let mut handler = |_: Message<'_>| seen += 1;

            // The first message's beginning, then the second message's *last*
            // frame: a frame at an offset that does not continue the message in
            // progress, which is exactly what a hole in a stream looks like.
            log.push_into(0, &mut assembler, &mut handler);
            log.push_into(3, &mut assembler, &mut handler);
        }

        assert_eq!(0, seen, "a message with a hole in it is not a message");
        assert_eq!(1, assembler.abandoned());

        // And the session still works: the gap reset the builder rather than
        // poisoning it, so the message that overwrote the first one arrives
        // whole when its own frames do.
        {
            let mut handler = |_: Message<'_>| seen += 1;
            log.push_into(2, &mut assembler, &mut handler);
            log.push_into(3, &mut assembler, &mut handler);
        }

        assert_eq!(1, seen);
        assert_eq!(1, assembler.abandoned());
    }

    #[test]
    fn a_tail_with_no_head_is_counted_not_assembled() {
        let mut log = Log::new();
        let payload = vec![b'a'; 1376 + 64];
        log.write(7, 1, &payload);

        let mut assembler = FragmentAssembler::new();
        let mut seen = 0;

        {
            let mut handler = |_: Message<'_>| seen += 1;
            // Only the second frame: its beginning was never read.
            log.push_into(1, &mut assembler, &mut handler);
        }

        assert_eq!(0, seen);
        assert_eq!(1, assembler.abandoned());
    }

    #[test]
    fn two_sessions_are_assembled_apart() {
        let mut log = Log::new();
        let seven = vec![b'7'; 1376 + 64];
        let nine = vec![b'9'; 1376 + 64];

        log.write(7, 1, &seven);
        log.write(9, 1, &nine);

        // Four frames: 7's pair, then 9's.
        assert_eq!(4, log.frames.len());

        let mut assembler = FragmentAssembler::new();
        let mut seen = Vec::new();
        let mut handler = |message: Message<'_>| seen.push(message.header.session_id);

        // Interleaved, which is what two publications on one stream look like
        // to a subscriber.
        for index in [0, 2, 1, 3] {
            log.push_into(index, &mut assembler, &mut handler);
        }

        assert_eq!(vec![7, 9], seen, "each session's fragments are its own");
        assert_eq!(0, assembler.abandoned());
    }

    #[test]
    fn a_whole_message_does_not_disturb_a_message_in_progress() {
        let mut log = Log::new();
        let big = vec![b'a'; 1376 + 64];
        log.write(7, 1, &big);
        log.write(7, 1, b"small");

        let mut assembler = FragmentAssembler::new();
        let mut seen = Vec::new();
        let mut handler = |message: Message<'_>| seen.push(message.payload.to_vec());

        // The first frame of the big message, then a whole one on the same
        // session, then the big message's last frame: the reference passes the
        // whole one through without touching the builder
        // (`aeron_fragment_assembler.c:158-161`).
        for index in [0, 2, 1] {
            log.push_into(index, &mut assembler, &mut handler);
        }

        assert_eq!(vec![b"small".to_vec(), big], seen);
        assert_eq!(0, assembler.abandoned());
    }
}
