//! criterion micro-benchmarks: the paths underneath `offer` and `poll`.
//!
//! In this process, in memory, with no driver and no socket. What that buys is
//! a number that moves only when the code under it moves: a regression in the
//! append path is visible here and nowhere else, because at the harness's scale
//! it is lost in scheduling noise.
//!
//! Three groups, one per layer:
//!
//! - **`logbuffer_append`** — `Appender::append`, which is the whole client-side
//!   cost of `offer`: read the tail, claim room, write the frame, publish it.
//!   The term is allowed to rotate rather than being reset, so the rotation's
//!   share is in the number, which is where it belongs — a publisher that never
//!   rotated would not be one.
//! - **`term_scan`** — `scan_for_availability`, which is what the *sender* runs
//!   to decide what fits one datagram (`aeron_network_publication.c:715`, and
//!   the same call again on the retransmit path at `:896`). The budget is the
//!   default MTU, exactly as the driver passes it.
//! - **`buffer`** — the accessors everything else is built from. They are one
//!   instruction each, which is the point: if one of them ever stops being one
//!   instruction, this is where it shows.
//!
//! Every routine asserts the outcome it expects. A benchmark whose subject
//! quietly stopped working — an append that starts returning `BackPressured`
//! and measures nothing — reports a beautiful number for an empty operation,
//! and that is the failure mode worth spending an `assert!` on. The assertion
//! is outside the operation's own cost and its payload is only formatted when
//! it fails.

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use deepmsg_core::buffer::AtomicBuffer;
use deepmsg_core::logbuffer::append::{Appended, Appender};
use deepmsg_core::logbuffer::descriptor;
use deepmsg_core::logbuffer::scan::{Availability, scan_for_availability};

/// The smallest legal term length, so a fixture is 64 KiB and not 16 MiB.
const TERM_LENGTH: i32 = 64 * 1024;

/// A term id that is not zero, so a term id that leaked into a frame is visible.
const INITIAL_TERM_ID: i32 = 17;

/// The session and stream a synthetic publication records in its frames.
const SESSION_ID: i32 = 7;
const STREAM_ID: i32 = 1002;

/// The position limit a publisher with no subscriber to respect would use.
///
/// `i64::MAX` rather than a counter's value: what is being measured is the
/// append path itself, and a limit that closed would turn every later iteration
/// into a measurement of the `position >= limit` branch.
const UNLIMITED: i64 = i64::MAX;

/// A byte region an [`AtomicBuffer`] may be built over.
///
/// Aligned because the buffer refuses an 8-byte-unaligned base — and because
/// benchmarking deliberately misaligned accessors would measure the wrong
/// thing: the reference's own log buffers are page-aligned.
#[repr(align(64))]
struct Aligned<const N: usize>([u8; N]);

/// A publication's log buffer in memory: the metadata block and the term it
/// describes.
///
/// The same two regions the driver maps for a real publication, with the three
/// fields `Appender::new` reads — term length, initial term id, MTU — written by
/// hand. Nothing here touches a file: what is under test is the append path,
/// not the mapping.
struct Log {
    metadata: Box<Aligned<{ descriptor::METADATA_STRUCT_LENGTH }>>,
    term: Box<Aligned<{ TERM_LENGTH as usize }>>,
}

impl Log {
    /// A log with the fields an appender needs and its tails initialised.
    fn primed() -> Self {
        let mut log = Self {
            metadata: Box::new(Aligned([0_u8; descriptor::METADATA_STRUCT_LENGTH])),
            // Boxed on purpose: 64 KiB on the stack is a legal term length and
            // a large stack frame.
            term: Box::new(Aligned([0_u8; TERM_LENGTH as usize])),
        };

        {
            let metadata =
                AtomicBuffer::from_slice_mut(&mut log.metadata.0).expect("8-byte aligned");
            metadata
                .store_i32_relaxed(descriptor::TERM_LENGTH_OFFSET, TERM_LENGTH)
                .expect("in range");
            metadata
                .store_i32_relaxed(descriptor::INITIAL_TERM_ID_OFFSET, INITIAL_TERM_ID)
                .expect("in range");
            metadata
                .store_i32_relaxed(
                    descriptor::MTU_LENGTH_OFFSET,
                    descriptor::MTU_LENGTH_DEFAULT,
                )
                .expect("in range");
        }

        // The tails are initialised through an appender that is then dropped,
        // because the method lives there and the borrow it takes is the
        // appender's, not this struct's.
        assert!(
            log.appender().initialise_tails(INITIAL_TERM_ID),
            "the tails are what an append reads first"
        );

        log
    }

    /// An appender over this log.
    ///
    /// Takes `&mut self` because a writable [`AtomicBuffer`] is built from an
    /// exclusive borrow, and holds it for as long as the appender lives.
    fn appender(&mut self) -> Appender<'_> {
        Appender::new(
            AtomicBuffer::from_slice_mut(&mut self.metadata.0).expect("8-byte aligned"),
            AtomicBuffer::from_slice_mut(&mut self.term.0).expect("8-byte aligned"),
        )
        .expect("a log with a term length and an MTU")
    }

    /// A read-only view of the term, for a scanner.
    fn term_view(&self) -> AtomicBuffer<'_, deepmsg_core::buffer::ReadOnly> {
        AtomicBuffer::from_slice(&self.term.0).expect("8-byte aligned")
    }

    /// Fill one term with frames of `payload_length` bytes each.
    ///
    /// Exactly one term, not a hair more: an append that would rotate leaves
    /// the term with a padding frame at its end, and a scanner's cost with and
    /// without one are different questions.
    fn fill_one_term(&mut self, payload_length: usize) {
        let appender = self.appender();
        let payload = vec![0xA5_u8; payload_length];
        let frame_length = (payload_length + deepmsg_core::logbuffer::frame::DATA_HEADER_LENGTH)
            .next_multiple_of(descriptor::FRAME_ALIGNMENT as usize);
        let frames = TERM_LENGTH as usize / frame_length;

        for index in 0..frames {
            let outcome = appender.append(SESSION_ID, STREAM_ID, UNLIMITED, &payload);
            assert!(
                matches!(outcome, Appended::Ok { .. }),
                "frame {index} of {frames} was refused: {outcome:?}"
            );
        }
    }
}

/// Append until the log takes it, the way a producer does.
///
/// `EndOfLog` and `MidRotation` are the reference's `ADMIN_ACTION`
/// (`aeron-client/src/main/c/aeron_publication.c:493`): the term rotated under
/// this call and the caller tries again — the reference's own samples spin on
/// exactly that (`cping.c:105-112`). A benchmark that treated one as a failure
/// would be benchmarking a caller nobody writes, and, worse, one that has to
/// special-case the term boundary the code under test is supposed to handle.
///
/// # Panics
///
/// On any outcome a producer cannot retry out of: the point of the assertion is
/// that a bench whose subject quietly stopped appending must not report a
/// beautiful number for an empty operation.
fn append_retrying(appender: &Appender<'_>, payload: &[u8]) {
    loop {
        match appender.append(SESSION_ID, STREAM_ID, UNLIMITED, payload) {
            Appended::Ok { .. } => return,
            Appended::EndOfLog | Appended::MidRotation => {}
            other => panic!("the append path stopped appending: {other:?}"),
        }
    }
}

/// `Appender::append`, by payload size.
fn logbuffer_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("logbuffer_append");

    for payload_length in [32_usize, 1024] {
        // Bytes per second as well as appends: which of the two is flat says
        // whether the cost is per message or per byte.
        group.throughput(Throughput::Bytes(payload_length as u64));

        let mut log = Log::primed();
        let payload = vec![0xA5_u8; payload_length];

        group.bench_function(BenchmarkId::from_parameter(payload_length), |b| {
            let appender = log.appender();

            b.iter(|| {
                // The payload is a message the appender writes a frame around;
                // `black_box` keeps the compiler from deciding the whole loop is
                // dead because nothing reads the term.
                append_retrying(&appender, black_box(&payload));
            });
        });
    }

    group.finish();
}

/// `scan_for_availability`, at the two budgets the driver uses.
fn term_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("term_scan");

    let mut log = Log::primed();
    log.fill_one_term(32);
    let term = log.term_view();

    // One datagram's worth, which is how the sender decides what to put in the
    // frame it is about to send (`network_publication.rs:715`).
    group.bench_function("one_datagram", |b| {
        b.iter(|| {
            let availability = scan_for_availability(
                black_box(&term),
                0,
                TERM_LENGTH,
                descriptor::MTU_LENGTH_DEFAULT,
            );
            assert!(
                matches!(availability, Availability::Ready { .. }),
                "a full term should fill a datagram: {availability:?}"
            );
        });
    });

    // The whole term, which is the upper bound of the same scan and the shape a
    // reader's pass over a term has.
    group.bench_function("whole_term", |b| {
        b.iter(|| {
            let availability = scan_for_availability(black_box(&term), 0, TERM_LENGTH, TERM_LENGTH);
            assert!(
                matches!(availability, Availability::Ready { .. }),
                "a full term should scan to its end: {availability:?}"
            );
        });
    });

    group.finish();
}

/// The buffer accessors the layers above are made of.
fn buffer(c: &mut Criterion) {
    let mut region = Aligned([0_u8; 4096]);
    let view = AtomicBuffer::from_slice_mut(&mut region.0).expect("8-byte aligned");
    let payload_32 = [0xA5_u8; 32];
    let payload_1k = [0xA5_u8; 1024];

    // Each routine gets its own offset. Sharing one would couple them: the
    // store leaves a pattern behind, and a compare-and-exchange that expected
    // zero would then fail on the second routine's first iteration — which is
    // how this was found.
    const STORE: usize = 0;
    const SWAP: usize = 64;
    const COPY: usize = 128;

    c.bench_function("buffer/store_i64", |b| {
        b.iter(|| {
            view.store_i64_relaxed(STORE, black_box(0x0123_4567_89AB_CDEF_u64 as i64))
                .expect("in range");
        });
    });

    c.bench_function("buffer/load_i64", |b| {
        b.iter(|| {
            black_box(view.load_i64_acquire(STORE).expect("in range"));
        });
    });

    // The claim's compare-and-exchange: what decides whether a producer gets
    // the room it asked for, so it is on the append path's critical line.
    c.bench_function("buffer/compare_exchange_i64", |b| {
        b.iter(|| {
            let swapped = view
                .compare_exchange_i64(SWAP, black_box(0), black_box(1))
                .expect("in range");
            assert!(swapped, "the value should have been what was expected");

            // Put it back, so the next iteration finds what this one did.
            view.store_i64_relaxed(SWAP, 0).expect("in range");
        });
    });

    c.bench_function("buffer/copy_in_32", |b| {
        b.iter(|| {
            view.copy_in(COPY, black_box(&payload_32))
                .expect("in range");
        });
    });

    c.bench_function("buffer/copy_in_1k", |b| {
        b.iter(|| {
            view.copy_in(COPY, black_box(&payload_1k))
                .expect("in range");
        });
    });

    c.bench_function("buffer/copy_out_1k", |b| {
        let mut into = [0_u8; 1024];

        b.iter(|| {
            view.copy_out(COPY, black_box(&mut into)).expect("in range");
        });
    });
}

criterion_group!(benches, logbuffer_append, term_scan, buffer);
criterion_main!(benches);
