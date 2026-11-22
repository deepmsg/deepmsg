#![no_main]

//! Target 02: the term scanner and the fragment assembler.
//!
//! A term buffer is shared memory another process writes; a frame header is
//! therefore untrusted. Two walks are exercised:
//!
//! 1. `scan_for_availability` with the offset, remaining length and maximum
//!    all taken from the input — an `i32` extreme is a legitimate case.
//! 2. `Scanner` over the same bytes, feeding the assembler **only** the frames
//!    the scanner accepted (`Step::Data`), which is what `Image::poll` does.
//!    The assembler's payload copy is bounded by the frame the scanner
//!    validated, so a hostile `frame_length` cannot make it allocate.
//!
//! The assembler is reached through the `fuzz-hooks` feature's
//! `Fragment::new_for_fuzz`; the feature is off in every workspace build.

use deepmsg_client::fragment_assembler::{Action, ControlledHandler, FragmentAssembler, Message};
use deepmsg_client::image::Fragment;
use deepmsg_core::logbuffer::frame::Frame;
use deepmsg_core::logbuffer::scan::{Scanner, Step, scan_for_availability};
use deepmsg_fuzz::with_read_only;
use libfuzzer_sys::fuzz_target;

#[derive(Default)]
struct Counter {
    delivered: usize,
    bytes: usize,
}

impl ControlledHandler for Counter {
    fn on_message(&mut self, message: Message<'_>) -> Action {
        self.delivered += 1;
        self.bytes += message.payload.len();
        Action::Continue
    }
}

/// An `i32` from up to four bytes at `at`, zero-padded.
fn le_i32_at(data: &[u8], at: usize) -> i32 {
    let mut bytes = [0_u8; 4];
    for (index, byte) in bytes.iter_mut().enumerate() {
        if let Some(value) = data.get(at + index) {
            *byte = *value;
        }
    }
    i32::from_le_bytes(bytes)
}

fuzz_target!(|data: &[u8]| {
    with_read_only(data, |buffer, len| {
        if len == 0 {
            return;
        }

        let offset = usize::from(data[0]).min(len);
        let _ = scan_for_availability(&buffer, offset, le_i32_at(data, 1), le_i32_at(data, 5));

        // One forward walk. Frames the scanner refused end it, which is what
        // production does: the cursor resumes there on the next poll.
        let term_length = buffer.len();
        let mut scanner = Scanner::new(&buffer, term_length);
        let mut assembler = FragmentAssembler::new();
        let mut counter = Counter::default();

        for _ in 0..64 {
            match scanner.advance() {
                Step::Data { offset, .. } => {
                    let frame = Frame::new(&buffer, offset);
                    let fragment =
                        Fragment::new_for_fuzz(frame, i64::try_from(offset).unwrap_or(i64::MAX), 0);
                    let _ = assembler.push_controlled(&fragment, &mut counter);
                }
                Step::Padding { .. } => {}
                Step::NotReady { .. } | Step::Malformed { .. } | Step::End => break,
            }
        }

        // Every copied payload byte came from a frame inside the term, and a
        // frame is visited once: the assembler never holds more than the term.
        assert!(
            counter.bytes <= term_length,
            "assembled {} bytes from a {term_length}-byte term",
            counter.bytes
        );
    });
});
