//! Creating a log buffer file — the driver's half.
//!
//! A client only ever maps one: `deepmsg-client`'s `log_buffer` module documents
//! the geometry from that side, and says why a client that created the file
//! first would make the *driver's* creation fail. This is the other side — the
//! driver (or rather its native resource agent) creates the file with
//! `O_CREAT | O_EXCL`, lays out three terms and a metadata block, and hands the
//! mapping to the publication (`aeron-client/src/main/c/util/aeron_fileutil.c:1260-1307`).
//!
//! ```text
//! offset 0                   : term 0                   (term_length bytes)
//! offset term_length         : term 1
//! offset 2 * term_length     : term 2
//! offset 3 * term_length     : metadata                 (4096 bytes)
//! ```
//!
//! so `length = ALIGN(3 * term_length + 4096, page_size)`
//! (`concurrent/aeron_logbuffer_descriptor.h:107-110`).
//!
//! # The metadata is written after the file exists, not before
//!
//! `create` leaves the metadata block zeroed and returns a handle to it; the
//! caller fills it (`descriptor::initialise`) and then writes the term tails.
//! That order is the reference's — `aeron_logbuffer_metadata_init` does not
//! touch the tails (`:235-239`), and the publication writes them itself
//! (`aeron_ipc_publication.c:75-103`) — and it is what lets a publication start
//! at a non-zero position without a second template.

use std::io;
use std::path::Path;

use crate::buffer::{AtomicBuffer, ReadWrite};
use crate::pal::MappedFile;

use super::descriptor;
use super::position::{self, RawTail};

/// A log buffer file, mapped: either one this process created
/// ([`LogFile::create`]) or a second view of one that already exists
/// ([`LogFile::open`]).
pub struct LogFile {
    file: MappedFile,
    /// Where it is, for the removal and for reports. `MappedFile` does not
    /// carry a name — it is a mapping, and the caller is what knows which file
    /// it came from.
    path: std::path::PathBuf,
    term_length: i32,
    metadata_offset: usize,
}

impl LogFile {
    /// Create `path` with room for three terms and a metadata block.
    ///
    /// The file is created exclusively — a log buffer that already exists
    /// belongs to a publication, and two publications must not share one by
    /// accident. `sparse` picks how the length is made real: a sparse file
    /// declares its length and leaves the pages to the filesystem, which is
    /// the reference's default (`term.buffer.sparse.file`); a dense one
    /// allocates and touches its whole length up front, so a producer never
    /// pays a page fault on the hot path. The reference's `aeron_raw_log_map`
    /// makes the same choice from its `use_sparse_files` parameter
    /// (`aeron-client/src/main/c/util/aeron_fileutil.c:1269-1292`).
    ///
    /// # Errors
    ///
    /// [`io::Error`] if the term length is not one the layout allows, or if the
    /// file cannot be created, allocated or mapped. A file this call created
    /// and then abandoned is removed before returning.
    pub fn create(
        path: &Path,
        term_length: i32,
        page_size: usize,
        sparse: bool,
    ) -> io::Result<Self> {
        let Some(length) = Self::log_length(term_length, page_size) else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "the term length or page size is not one a log buffer may have",
            ));
        };

        let file = if sparse {
            MappedFile::create_sparse(path, length)?
        } else {
            MappedFile::create(path, length)?
        };

        Ok(Self {
            file,
            path: path.to_owned(),
            term_length,
            metadata_offset: length - descriptor::METADATA_LENGTH,
        })
    }

    /// Map a log buffer that already exists, as a second view of it.
    ///
    /// The same file, mapped again. Two mappings of one file are two addresses
    /// over the **same pages** — the page cache is what they share — so this
    /// does not copy anything and does not put the threads that use the two
    /// views on different cache lines. What it buys is that the two threads
    /// doing different jobs over one log buffer can each hold a mapping of
    /// their own, which is the shape the reference has: its conductor cleans a
    /// publication's terms and its sender reads them to build datagrams
    /// (`aeron_network_publication.c:947-1010` against `:515-560`), over one
    /// `aeron_log_buffer_t` they both reach through the same publication.
    ///
    /// Read-write rather than read-only, because cleaning is a write: a caller
    /// that only wants the metadata could pass nothing else, but there is no
    /// such caller yet and a read-only map would fail at the first `zero`.
    ///
    /// # Errors
    ///
    /// [`io::Error`] if the file cannot be opened or mapped, or is shorter than
    /// the layout `term_length` and `page_size` describe. The length is checked
    /// rather than assumed because a second view can be opened over a file this
    /// process did not create — a shorter one would map, and then every offset
    /// past its end would silently read as absent (`MappedFile::region` bounds
    /// its windows).
    pub fn open(path: &Path, term_length: i32, page_size: usize) -> io::Result<Self> {
        let Some(length) = Self::log_length(term_length, page_size) else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "the term length or page size is not one a log buffer may have",
            ));
        };

        let file = MappedFile::open_readwrite(path)?;
        if file.len() < length {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "the log buffer is shorter than its term length and page size describe",
            ));
        }

        Ok(Self {
            file,
            path: path.to_owned(),
            term_length,
            metadata_offset: length - descriptor::METADATA_LENGTH,
        })
    }

    /// The page-aligned length a log buffer of `term_length` and `page_size`
    /// occupies, or `None` for a pair the layout does not allow.
    ///
    /// The same arithmetic [`LogFile::create`] makes a file of, factored out
    /// for the caller that must know the length *before* it decides to create:
    /// the reference's storage check compares this against the usable space
    /// first (`aeron_driver_context.c:1354-1375`, asking
    /// `aeron_logbuffer_compute_log_length` for the same number).
    ///
    /// The reference's own compute is a bare alignment
    /// (`aeron_logbuffer_descriptor.h:107-110`) and trusts its caller for the
    /// page size, because the driver validates it once at start-up — a range
    /// and a power of two (`aeron_driver.c:468-485`). This is the single
    /// validation point on this side, so the power-of-two test lives here: a
    /// non-power-of-two page size would not merely be unusual, the mask below
    /// would round to an arbitrary length.
    pub fn log_length(term_length: i32, page_size: usize) -> Option<usize> {
        if position::bits_to_shift(term_length).is_none()
            || page_size < descriptor::PAGE_MIN_SIZE
            || !page_size.is_power_of_two()
        {
            return None;
        }

        let unaligned = (term_length as usize)
            .saturating_mul(descriptor::PARTITION_COUNT)
            .saturating_add(descriptor::METADATA_LENGTH);

        Some(unaligned.saturating_add(page_size - 1) & !(page_size - 1))
    }

    /// The file's length, as created.
    pub const fn length(&self) -> usize {
        self.file.len()
    }

    /// Where the file is.
    ///
    /// A log buffer's name is part of its contract — it is what the driver
    /// sends a subscriber in `ON_AVAILABLE_IMAGE` and what the reference derives
    /// from the publication's registration id — so callers need to read it back
    /// rather than remember it.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The term buffer at `index`, or `None` past the third.
    pub fn term(&self, index: usize) -> Option<AtomicBuffer<'_, ReadWrite>> {
        if index >= descriptor::PARTITION_COUNT {
            return None;
        }

        let offset = index * self.term_length as usize;
        self.file.region_mut(offset, self.term_length as usize)
    }

    /// The metadata block: the last page of the file.
    pub fn metadata(&self) -> Option<AtomicBuffer<'_, ReadWrite>> {
        self.file
            .region_mut(self.metadata_offset, descriptor::METADATA_LENGTH)
    }

    /// Write the three term tails and the active term count.
    ///
    /// With `start` at `None` the log begins at the first term of
    /// `initial_term_id`, which is every publication that does not name a
    /// position. With `Some((term_id, term_offset))` it begins *there* — the
    /// other two partitions still hold the terms three and two rotations back,
    /// counted **forward from the active one**, which is what
    /// `aeron_ipc_publication.c:75-103` writes and what `Appender::rotate`
    /// looks for when the log turns.
    ///
    /// Returns whether the block could be written.
    pub fn initialise_tails(&self, initial_term_id: i32, start: Option<(i32, i32)>) -> bool {
        let Some(metadata) = self.metadata() else {
            return false;
        };

        let store = |index: usize, tail: RawTail| {
            let offset = descriptor::TERM_TAIL_COUNTERS_OFFSET
                + index * descriptor::TERM_TAIL_COUNTER_STRIDE;
            metadata.store_i64_release(offset, tail.raw()).is_some()
        };

        match start {
            None => {
                // Written here rather than through `Appender::initialise_tails`
                // because the tails come *before* the metadata block in the
                // reference's creation order (`aeron_ipc_publication.c:75-103`
                // then `:107-142`), and an appender needs the metadata it has
                // not been given yet.
                if !store(0, RawTail::new(initial_term_id, 0)) {
                    return false;
                }

                for index in 1..descriptor::PARTITION_COUNT {
                    let expected = initial_term_id
                        .wrapping_add(index as i32)
                        .wrapping_sub(descriptor::PARTITION_COUNT as i32);

                    if !store(index, RawTail::new(expected, 0)) {
                        return false;
                    }
                }

                metadata
                    .store_i32_relaxed(descriptor::ACTIVE_TERM_COUNT_OFFSET, 0)
                    .is_some()
            }
            Some((term_id, term_offset)) => {
                let term_count = position::term_count(term_id, initial_term_id);
                let mut index = position::index_by_term_count(term_count);

                if !store(index, RawTail::new(term_id, term_offset)) {
                    return false;
                }

                for step in 1..descriptor::PARTITION_COUNT {
                    index = (index + 1) % descriptor::PARTITION_COUNT;
                    let expected = term_id
                        .wrapping_add(step as i32)
                        .wrapping_sub(descriptor::PARTITION_COUNT as i32);

                    if !store(index, RawTail::new(expected, 0)) {
                        return false;
                    }
                }

                metadata
                    .store_i32_relaxed(descriptor::ACTIVE_TERM_COUNT_OFFSET, term_count)
                    .is_some()
            }
        }
    }

    /// Unmap the file and remove it, in that order.
    ///
    /// The order is the reference's (`aeron_driver_conductor_delete_log_buffer`)
    /// and it is not a formality: unlinking a file this process still has
    /// mapped leaves the pages alive but nameless, so a subscriber that maps it
    /// after the delete gets nothing while a reader that mapped it before the
    /// delete keeps working.
    ///
    /// # Errors
    ///
    /// [`io::Error`] if the file cannot be removed. The unmapping happens
    /// either way.
    pub fn remove(self) -> io::Result<()> {
        let path = self.path;
        drop(self.file);

        std::fs::remove_file(path)
    }
}

impl std::fmt::Debug for LogFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogFile")
            .field("path", &self.path)
            .field("length", &self.length())
            .field("term_length", &self.term_length)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The smallest legal term, so a test file is 200 KiB rather than 192 MiB.
    const TERM_LENGTH: i32 = descriptor::TERM_MIN_LENGTH;
    const PAGE_SIZE: usize = 4096;

    struct TempDir(std::path::PathBuf);

    impl TempDir {
        fn new() -> Self {
            static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
            let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let path =
                std::env::temp_dir().join(format!("deepmsg-logfile-{}-{n}", std::process::id()));
            std::fs::create_dir_all(&path).expect("create the directory");
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn created() -> (TempDir, LogFile) {
        let dir = TempDir::new();
        let log = LogFile::create(
            &dir.path().join("test.logbuffer"),
            TERM_LENGTH,
            PAGE_SIZE,
            false,
        )
        .expect("a log buffer");

        (dir, log)
    }

    #[test]
    fn a_second_view_of_a_log_buffer_reaches_the_same_pages() {
        // What `open` is for: two mappings over one file. A write through the
        // first has to be visible through the second, or two threads holding
        // one each would be working on different memory and the cleaning one of
        // them does would not reach the one that sends.
        let (dir, first) = created();
        let path = dir.path().join("test.logbuffer");

        first
            .term(0)
            .expect("term 0")
            .store_i32_release(0, 0x2A)
            .expect("in range");

        let second = LogFile::open(&path, TERM_LENGTH, PAGE_SIZE).expect("a second view");

        assert_eq!(
            Some(0x2A),
            second.term(0).expect("term 0").load_i32_acquire(0),
            "the second view reads what the first wrote"
        );
    }

    #[test]
    fn a_file_shorter_than_the_layout_describes_is_refused() {
        // A second view can be opened over a file this process did not create,
        // and a short one would map happily — every offset past its end would
        // then read as absent rather than as an error, which is the kind of
        // failure that shows up as a driver that silently sends nothing.
        let dir = TempDir::new();
        let short = dir.path().join("short.logbuffer");
        std::fs::write(&short, b"not a log buffer").expect("a small file");

        assert!(LogFile::open(&short, TERM_LENGTH, PAGE_SIZE).is_err());
    }

    #[test]
    fn a_page_size_that_is_not_a_power_of_two_is_refused() {
        // The mask the length is rounded with only means anything for a power
        // of two, and the reference's driver validates the same thing at
        // start-up (`aeron_driver.c:468-485`). Refused here means the file is
        // never made.
        let dir = TempDir::new();
        let odd = dir.path().join("odd.logbuffer");

        assert!(LogFile::log_length(TERM_LENGTH, PAGE_SIZE + 2).is_none());
        assert!(LogFile::create(&odd, TERM_LENGTH, PAGE_SIZE + 2, false).is_err());
        assert!(!odd.exists());
    }

    #[test]
    fn the_length_is_three_terms_and_a_metadata_block_rounded_to_the_page() {
        let (dir, log) = created();

        let expected = 3 * TERM_LENGTH as usize + descriptor::METADATA_LENGTH;
        assert_eq!(
            expected,
            log.length(),
            "already page-aligned at 64 KiB terms"
        );
        assert_eq!(
            expected,
            std::fs::metadata(dir.path().join("test.logbuffer"))
                .expect("the file is there")
                .len() as usize
        );
    }

    #[test]
    fn the_metadata_block_is_the_last_page_and_the_terms_are_in_front_of_it() {
        let (_dir, log) = created();

        for index in 0..descriptor::PARTITION_COUNT {
            let term = log.term(index).expect("a term");
            assert_eq!(TERM_LENGTH as usize, term.len());
        }
        assert!(
            log.term(descriptor::PARTITION_COUNT).is_none(),
            "three, not four"
        );

        let metadata = log.metadata().expect("the metadata block");
        assert_eq!(descriptor::METADATA_LENGTH, metadata.len());
    }

    #[test]
    fn a_fresh_log_starts_without_a_position() {
        let (_dir, log) = created();
        assert!(log.initialise_tails(17, None));

        let metadata = log.metadata().expect("the metadata block");
        let tail = |index: usize| {
            let offset = descriptor::TERM_TAIL_COUNTERS_OFFSET
                + index * descriptor::TERM_TAIL_COUNTER_STRIDE;
            RawTail::from_raw(metadata.load_i64_acquire(offset).expect("in range"))
        };

        assert_eq!(
            RawTail::new(17, 0),
            tail(0),
            "the first term is the initial one"
        );
        assert_eq!(RawTail::new(15, 0), tail(1), "the one two rotations back");
        assert_eq!(RawTail::new(16, 0), tail(2), "the one three back");
        assert_eq!(
            Some(0),
            metadata.load_i32_acquire(descriptor::ACTIVE_TERM_COUNT_OFFSET),
            "and the log is at its first term"
        );
    }

    #[test]
    fn a_log_that_starts_at_a_position_puts_the_other_two_terms_behind_it() {
        // What a publication resuming at a position writes, and what matters is
        // that the two partitions *after* the active one hold the terms the next
        // rotations will look for.
        let (_dir, log) = created();
        let initial_term_id = 17;
        let start_term_id = 19;

        assert!(log.initialise_tails(initial_term_id, Some((start_term_id, 64))));

        let metadata = log.metadata().expect("the metadata block");
        let tail = |index: usize| {
            let offset = descriptor::TERM_TAIL_COUNTERS_OFFSET
                + index * descriptor::TERM_TAIL_COUNTER_STRIDE;
            RawTail::from_raw(metadata.load_i64_acquire(offset).expect("in range"))
        };

        let active =
            position::index_by_term_count(position::term_count(start_term_id, initial_term_id));
        assert_eq!(RawTail::new(19, 64), tail(active), "the active partition");
        assert_eq!(
            RawTail::new(17, 0),
            tail((active + 1) % descriptor::PARTITION_COUNT),
            "then the term two rotations back"
        );
        assert_eq!(
            RawTail::new(18, 0),
            tail((active + 2) % descriptor::PARTITION_COUNT),
            "then the one three back"
        );
        assert_eq!(
            Some(2),
            metadata.load_i32_acquire(descriptor::ACTIVE_TERM_COUNT_OFFSET)
        );
    }

    #[test]
    fn a_second_create_of_the_same_path_is_refused() {
        // The exclusivity is what keeps two publications from sharing one log by
        // accident: the second one must fail and be given a different file.
        let dir = TempDir::new();
        let path = dir.path().join("taken.logbuffer");
        let first = LogFile::create(&path, TERM_LENGTH, PAGE_SIZE, false).expect("the first");
        let second = LogFile::create(&path, TERM_LENGTH, PAGE_SIZE, false);

        assert!(second.is_err(), "the file already exists");
        drop(first);
    }

    #[test]
    fn removing_unmaps_and_deletes() {
        let dir = TempDir::new();
        let path = dir.path().join("gone.logbuffer");
        let log = LogFile::create(&path, TERM_LENGTH, PAGE_SIZE, false).expect("a log buffer");

        log.remove().expect("removed");

        assert!(!path.exists(), "the file is gone");
    }
}
