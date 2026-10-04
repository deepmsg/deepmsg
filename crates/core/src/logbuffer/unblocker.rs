//! Unblocking a log at a **position**: which term it falls in, and the rotation
//! that may have to follow.
//!
//! Mirrors `aeron-driver/src/main/c/concurrent/aeron_logbuffer_unblocker.c:19-66`
//! and Java's `io.aeron.logbuffer.LogBufferUnblocker`
//! (`aeron-client/src/main/java/io/aeron/logbuffer/LogBufferUnblocker.java:40-78`),
//! which are the same thirty lines under two different module roofs — C files it
//! in the driver, Java in the client. It needs nothing that the metadata block
//! and the term buffers do not already give it, so it lives here beside
//! [`super::repair::Unblocker`], the term-level function it wraps.
//!
//! What it adds to that function is the **position arithmetic** and the two
//! rotations:
//!
//! * the log is already one term ahead of where the blocked position sits, and
//!   the blocked position is the first byte of its term — the term is full and
//!   nobody rotated it, so rotate it and report success without even looking;
//! * the term-level unblocker covered a run that reaches the **end** of its
//!   term, so the log has to turn before anything can be read past it.

use crate::buffer::{AtomicBuffer, ReadWrite};

use super::descriptor;
use super::logfile::LogFile;
use super::position::{self, RawTail};
use super::repair::{UnblockStatus, Unblocker};

/// Try to unblock the log at `blocked_position`.
///
/// Returns whether anything was done. A publication calls this when it has been
/// blocked past its unblock timeout, and again on every time event while it
/// drains — see `aeron_logbuffer_unblocker.c`'s two callers in
/// `aeron_network_publication.c:1019` and `aeron_ipc_publication.c:579`.
///
/// `blocked_position` is a **position** in the stream, not an offset in a term:
/// which term it falls in, and therefore which partition's block of memory is
/// examined, is the first thing worked out here.
pub fn unblock(log: &LogFile, blocked_position: i64, term_length: i32) -> bool {
    let Some(metadata) = log.metadata() else {
        return false;
    };

    let Some(bits_to_shift) = position::bits_to_shift(term_length) else {
        return false;
    };

    let blocked_term_count = (blocked_position >> bits_to_shift) as i32;
    let blocked_index = position::index_by_term_count(blocked_term_count);
    let blocked_offset = blocked_position as i32 & (term_length - 1);

    let Some(active_term_count) = metadata.load_i32_acquire(descriptor::ACTIVE_TERM_COUNT_OFFSET)
    else {
        return false;
    };

    // Whoever was reading is already at the start of the **active** term while
    // the blocked position is the first byte of the term before it. That term
    // is full and the rotation it is owed never happened, so the rotation is
    // the whole of the unblocking — there is nothing to examine
    // (`aeron_logbuffer_unblocker.c:38-49`, `LogBufferUnblocker.java:51-56`).
    //
    // The rotation's own answer is deliberately discarded, as the reference
    // discards it: losing the compare-exchange means somebody else rotated,
    // which is the outcome this wanted anyway.
    if active_term_count == blocked_term_count.wrapping_sub(1) && blocked_offset == 0 {
        let Some(raw_tail) = raw_tail(&metadata, active_term_count) else {
            return false;
        };

        descriptor::rotate_log(&metadata, active_term_count, raw_tail.term_id());

        return true;
    }

    // The term the blocked position is in, and how far its producer got — the
    // forward scan stops at that tail, because past it nothing is expected yet.
    let Some(raw_tail) = raw_tail(&metadata, blocked_term_count) else {
        return false;
    };

    let term_id = raw_tail.term_id();
    let tail_offset = raw_tail.term_offset(term_length);

    let Some(term_buffer) = log.term(blocked_index) else {
        return false;
    };

    let metadata_view = metadata.as_read_only();

    match Unblocker::new(&term_buffer, &metadata_view).unblock(
        term_length,
        blocked_offset,
        tail_offset,
        term_id,
    ) {
        UnblockStatus::UnblockedToEnd => {
            descriptor::rotate_log(&metadata, blocked_term_count, term_id);

            true
        }
        UnblockStatus::Unblocked => true,
        UnblockStatus::NoAction => false,
    }
}

/// The raw tail of the partition that `term_count` falls in, read **acquire**
/// (`aeron_logbuffer_descriptor.h:94-102`: the count is what says which counter
/// is current, so it is read first and this follows from it).
fn raw_tail(metadata: &AtomicBuffer<'_, ReadWrite>, term_count: i32) -> Option<RawTail> {
    let index = position::index_by_term_count(term_count);
    let offset =
        descriptor::TERM_TAIL_COUNTERS_OFFSET + index * descriptor::TERM_TAIL_COUNTER_STRIDE;

    metadata.load_i64_acquire(offset).map(RawTail::from_raw)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The smallest legal term, as the reference's own tests use
    /// (`aeron_logbuffer_unblocker_test.cpp:25`).
    const TERM_LENGTH: i32 = descriptor::TERM_MIN_LENGTH;
    const PAGE_SIZE: usize = 4096;
    const TERM_ID: i32 = 1;
    const HEADER_LENGTH: i32 = super::super::frame::DATA_HEADER_LENGTH as i32;

    struct TempDir(std::path::PathBuf);

    impl TempDir {
        fn new() -> Self {
            static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
            let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let path =
                std::env::temp_dir().join(format!("deepmsg-unblocker-{}-{n}", std::process::id()));
            std::fs::create_dir_all(&path).expect("create the directory");
            Self(path)
        }

        fn path(&self) -> &std::path::Path {
            &self.0
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// A log in the state the reference's fixture builds: term `TERM_ID` active
    /// at offset zero, the other two partitions holding the terms three and two
    /// rotations back (`aeron_logbuffer_unblocker_test.cpp:154-175`).
    fn log() -> (TempDir, LogFile) {
        let dir = TempDir::new();
        let log = LogFile::create(
            &dir.path().join("test.logbuffer"),
            TERM_LENGTH,
            PAGE_SIZE,
            false,
        )
        .expect("a log buffer");

        assert!(log.initialise_tails(TERM_ID, None), "the tails go in");

        // And the default-header template, which is what a padding frame is
        // built from. A real log buffer always has one — the driver writes it at
        // publication creation (`aeron_ipc_publication.c:107-142`) — so the
        // fixture has one too, as every other test in this crate does. (The
        // reference's C test leaves the block zeroed and gets away with it,
        // because a zero template length is a `memcpy` of zero bytes there; that
        // is a case `repair.rs` tests on its own rather than one this fixture
        // has to stand for.)
        assert!(
            super::super::descriptor::fill_default_header(&metadata(&log), 11, 22, TERM_ID)
                .is_some(),
            "the padding template goes in"
        );

        (dir, log)
    }

    fn metadata(log: &LogFile) -> AtomicBuffer<'_, ReadWrite> {
        log.metadata().expect("the block is mapped")
    }

    /// Overwrite one partition's tail, as the reference's tests do directly.
    fn set_tail(log: &LogFile, index: usize, raw: i64) {
        metadata(log)
            .store_i64_release(
                descriptor::TERM_TAIL_COUNTERS_OFFSET
                    + index * descriptor::TERM_TAIL_COUNTER_STRIDE,
                raw,
            )
            .expect("in range");
    }

    fn active_term_count(log: &LogFile) -> i32 {
        metadata(log)
            .load_i32_acquire(descriptor::ACTIVE_TERM_COUNT_OFFSET)
            .expect("in range")
    }

    /// The active partition's tail.
    fn active_tail(log: &LogFile) -> RawTail {
        raw_tail(&metadata(log), active_term_count(log)).expect("in range")
    }

    /// Write a frame length into a term, leaving the rest of the header zero.
    fn set_length(log: &LogFile, term_index: usize, offset: usize, length: i32) {
        log.term(term_index)
            .expect("a term")
            .store_i32_release(offset, length)
            .expect("in range");
    }

    /// The blocked position of an offset in the first term. The first term's id
    /// **is** the initial term id, so the position is the offset; only the
    /// rotation case names a position beyond it.
    fn position_in_first_term(offset: i32) -> i64 {
        i64::from(offset)
    }

    #[test]
    fn a_complete_message_is_left_alone() {
        // `shouldNotUnblockWhenPositionHasCompleteMessage`.
        let (_dir, log) = log();
        let blocked_offset = HEADER_LENGTH * 4;
        set_length(&log, 0, blocked_offset as usize, HEADER_LENGTH);

        assert!(
            !unblock(&log, position_in_first_term(blocked_offset), TERM_LENGTH),
            "a frame is there and readable"
        );
        assert_eq!(
            TERM_ID,
            active_tail(&log).term_id(),
            "and the log did not turn"
        );
    }

    #[test]
    fn a_stalled_claim_in_the_current_term_is_padded() {
        // `shouldUnblockWhenPositionHasNonCommittedMessageAndTailWithinTerm`.
        let (_dir, log) = log();
        let blocked_offset = HEADER_LENGTH * 4;
        let message_length = HEADER_LENGTH * 4;
        set_length(&log, 0, blocked_offset as usize, -message_length);

        assert!(
            unblock(&log, position_in_first_term(blocked_offset), TERM_LENGTH),
            "a claim in flight is exactly what this is for"
        );
        assert_eq!(
            TERM_ID,
            active_tail(&log).term_id(),
            "the run did not reach the end of the term, so nothing rotated"
        );
        assert_eq!(
            Some(message_length),
            super::super::frame::Frame::new(&log.term(0).expect("a term"), blocked_offset as usize)
                .frame_length(),
            "and what it leaves behind is padding of that length"
        );
    }

    #[test]
    fn a_run_to_the_end_of_the_term_rotates() {
        // `shouldUnblockWhenPositionHasNonCommittedMessageAndTailAtEndOfTerm`.
        let (_dir, log) = log();
        let message_length = HEADER_LENGTH * 4;
        let blocked_offset = TERM_LENGTH - message_length;
        set_tail(&log, 0, (i64::from(TERM_ID) << 32) | i64::from(TERM_LENGTH));

        assert!(
            unblock(&log, position_in_first_term(blocked_offset), TERM_LENGTH),
            "the run reaches the end of the term"
        );
        assert_eq!(1, active_term_count(&log), "so the log turned");
        assert_eq!(
            TERM_ID + 1,
            active_tail(&log).term_id(),
            "onto the next term, at its start"
        );
    }

    #[test]
    fn a_full_term_that_was_never_rotated_is_rotated() {
        // `shouldUnblockWhenPositionHasCommittedMessageAndTailAtEndOfTermButNotRotated`.
        // The readers are at the start of a term whose predecessor is full:
        // nothing is examined, the rotation is the whole of it.
        let (_dir, log) = log();
        set_tail(&log, 0, (i64::from(TERM_ID) << 32) | i64::from(TERM_LENGTH));

        assert!(
            unblock(&log, i64::from(TERM_LENGTH), TERM_LENGTH),
            "the position is the first byte of the term after a full one"
        );
        assert_eq!(1, active_term_count(&log));
        assert_eq!(TERM_ID + 1, active_tail(&log).term_id());
    }

    #[test]
    fn a_tail_past_the_end_of_the_term_still_rotates() {
        // `shouldUnblockWhenPositionHasNonCommittedMessageAndTailPastEndOfTerm`:
        // the raw offset saturates at the term length
        // (`RawTail::term_offset`), so this is the end-of-term case again.
        let (_dir, log) = log();
        let message_length = HEADER_LENGTH * 4;
        let blocked_offset = TERM_LENGTH - message_length;
        set_tail(
            &log,
            0,
            (i64::from(TERM_ID) << 32) | i64::from(TERM_LENGTH + HEADER_LENGTH),
        );

        assert!(unblock(
            &log,
            position_in_first_term(blocked_offset),
            TERM_LENGTH
        ));
        assert_eq!(1, active_term_count(&log));
        assert_eq!(TERM_ID + 1, active_tail(&log).term_id());
    }
}
