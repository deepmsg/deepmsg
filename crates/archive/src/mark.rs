//! A mark file: the mapped file a process stamps to say it is alive.
//!
//! Mirrors `org.agrona.MarkFile`. Two things live in it, and neither is the
//! caller's own data: a **version**, which is also the signal that the file has
//! been initialised at all, and an **activity timestamp**, which is the proof of
//! life. Everything else in the file belongs to whoever owns it — for the
//! archive that is an SBE header carrying its channels, its archive id and its
//! error log ([`crate::mark_file`]).
//!
//! # Why the two offsets are parameters
//!
//! Agrona's mark file is generic over where its two fields live, because two
//! products put them in different places: the archive's SBE header
//! (`schemas/aeron-archive-mark-codecs.xml`) has `version` at one offset and
//! `activityTimestamp` at another, and the cluster's
//! (`aeron-cluster-mark-codecs.xml`) is a different schema again. Both are
//! passed in here rather than compiled in, which is the same shape the
//! reference has and the reason it has it.
//!
//! # What a mark file is not
//!
//! It is not CnC: no rings, no counters, no driver. The only reader that
//! matters is another process asking "is that thing still alive, and what did
//! it say it was" — `ArchiveTool pid` is one, and the archive's own startup is
//! another.

use std::fmt;
use std::io;
use std::path::{Path, PathBuf};

use deepmsg_core::buffer::{AtomicBuffer, ReadOnly, ReadWrite};
use deepmsg_core::pal::MappedFile;

/// `MarkFile.ACTIVATION_IN_PROGRESS_TIMESTAMP`.
///
/// What a file's timestamp holds while it is being written and before it can be
/// trusted. Not a value this module writes — the archive writes it during its
/// own initialisation — but named here because a reader has to know it.
pub const ACTIVATION_IN_PROGRESS_TIMESTAMP: i64 = i64::MAX;

/// `Aeron.NULL_VALUE`, which is what a timestamp holds once a process has
/// **deliberately** stopped rather than merely gone quiet.
///
/// The distinction is the whole point of the value: a stale timestamp means the
/// process died, this one means it finished. Both read as not active, and
/// `ArchiveTool` and the reference agree on that.
pub const NULL_VALUE: i64 = -1;

/// Why a mark file could not be opened.
#[derive(Debug)]
pub enum MarkFileError {
    /// The file could not be created, mapped or read.
    Io(io::Error),
    /// The file is shorter than the two fields this caller asked for, which is
    /// a file written by something else.
    TooShort {
        /// The file that was too short.
        path: PathBuf,
        /// How long it actually is.
        length: usize,
        /// How long it would have to be to hold what was asked for.
        needed: usize,
    },
}

impl fmt::Display for MarkFileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "{error}"),
            Self::TooShort {
                path,
                length,
                needed,
            } => write!(
                f,
                "mark file ({}) is {length} bytes, too short to hold the fields at {needed}",
                path.display()
            ),
        }
    }
}

impl std::error::Error for MarkFileError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::TooShort { .. } => None,
        }
    }
}

/// A mapped, initialised mark file.
///
/// The mapping is held and regions are handed out per call, the way
/// [`deepmsg_cnc::CncFile`] does it: a buffer that borrowed this struct's own
/// mapping would make the type self-referential, and every caller here wants a
/// window rather than a whole file.
pub struct MarkFile {
    mapping: MappedFile,
    path: PathBuf,
    version_offset: usize,
    timestamp_offset: usize,
}

impl MarkFile {
    /// Create a file of `length` bytes and map it, replacing one that is there.
    ///
    /// The bytes are zero, and a zero version means "created but not
    /// initialised" to every reader — including [`MarkFile::is_active`]'s
    /// reference, which waits for the field to become non-zero before it will
    /// look at anything else.
    ///
    /// # Errors
    ///
    /// [`MarkFileError::TooShort`] when `length` cannot hold the two fields, and
    /// [`MarkFileError::Io`] for the file system.
    pub fn create(
        path: &Path,
        length: usize,
        version_offset: usize,
        timestamp_offset: usize,
    ) -> Result<Self, MarkFileError> {
        need(length, version_offset, timestamp_offset, path)?;

        let mapping = MappedFile::create(path, length).map_err(MarkFileError::Io)?;

        Self::over(mapping, path, version_offset, timestamp_offset)
    }

    /// Map a mark file that is already there.
    ///
    /// # Errors
    ///
    /// [`MarkFileError::Io`] when there is no such file, and
    /// [`MarkFileError::TooShort`] when it is shorter than the fields named.
    pub fn open(
        path: &Path,
        version_offset: usize,
        timestamp_offset: usize,
    ) -> Result<Self, MarkFileError> {
        let mapping = MappedFile::open_readwrite(path).map_err(MarkFileError::Io)?;

        Self::over(mapping, path, version_offset, timestamp_offset)
    }

    /// Map the file if it is there and make one if it is not, which is what a
    /// process starting up wants: a second start finds the first one's file.
    ///
    /// # Errors
    ///
    /// As [`MarkFile::create`] and [`MarkFile::open`].
    pub fn open_or_create(
        path: &Path,
        length: usize,
        version_offset: usize,
        timestamp_offset: usize,
    ) -> Result<Self, MarkFileError> {
        if path.exists() {
            Self::open(path, version_offset, timestamp_offset)
        } else {
            Self::create(path, length, version_offset, timestamp_offset)
        }
    }

    fn over(
        mapping: MappedFile,
        path: &Path,
        version_offset: usize,
        timestamp_offset: usize,
    ) -> Result<Self, MarkFileError> {
        need(mapping.len(), version_offset, timestamp_offset, path)?;

        Ok(Self {
            mapping,
            path: path.to_path_buf(),
            version_offset,
            timestamp_offset,
        })
    }

    /// The file this is.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// How long the file is.
    pub fn length(&self) -> usize {
        self.mapping.len()
    }

    /// The version field, with the acquire a reader of another process wants.
    ///
    /// **Zero means the file has been created and not initialised**: the
    /// reference waits for this field to become non-zero before it trusts
    /// anything else in the file, and throws if it never does.
    pub fn version(&self) -> Option<i32> {
        self.values()?.load_i32_acquire(self.version_offset)
    }

    /// The activity timestamp, with the same acquire.
    pub fn timestamp(&self) -> Option<i64> {
        self.values()?.load_i64_acquire(self.timestamp_offset)
    }

    /// Whether the process that owns this file is still alive, by the rule the
    /// reference's `MarkFile.isActive` applies after its wait: **the timestamp
    /// is no older than `timeout_ms`**.
    ///
    /// The wait is not here. The reference's `isActive` first spins — up to
    /// `timeout_ms`, sleeping a millisecond a turn — until the version field is
    /// non-zero, and throws `mark file is created but not initialised` if it
    /// never is. That is a **startup** convenience for a process waiting on
    /// another to publish, and a caller that wants it can ask
    /// [`MarkFile::version`] in its own loop; a predicate that blocks for
    /// seconds would be a surprising thing to call `is_active`.
    ///
    /// The version check is not here either, and for the same reason: the
    /// reference hands the version it read to a callback the **caller**
    /// supplies (`validateVersion` for the archive), so that a mark file from
    /// another major version is refused by the product that owns the format
    /// rather than by the generic layer.
    pub fn is_active(&self, now_ms: i64, timeout_ms: i64) -> bool {
        match self.timestamp() {
            Some(timestamp) => now_ms - timestamp <= timeout_ms,
            None => false,
        }
    }

    /// Publish this file as ready, which is what makes the version field
    /// non-zero and readable by everybody else.
    ///
    /// The reference's `signalReady(int)`, and the same store it makes: Agrona
    /// writes the field with `putIntRelease` (`org.agrona.MarkFile`, the body
    /// of `signalReady(int)`), which is this build's `store_i32_release`. The
    /// two orderings being the same one is worth saying rather than assuming —
    /// the same class's `timestampOrdered` is a *weaker* store, so the pair is
    /// not uniform by accident.
    pub fn signal_ready(&self, version: i32) -> Option<()> {
        self.values()?
            .store_i32_release(self.version_offset, version)
    }

    /// Say the process has stopped **on purpose**: the timestamp becomes
    /// [`NULL_VALUE`], which reads as "not active, and not by accident".
    ///
    /// The reference's `signalTerminated`, which is `signalReady(NULL_VALUE)` —
    /// the version field is left alone, because the file still describes what
    /// it described.
    pub fn signal_terminated(&self, version: i32) -> Option<()> {
        self.update_activity_timestamp(NULL_VALUE)?;

        self.signal_ready(version)
    }

    /// A proof of life.
    ///
    /// `now_ms` rather than a clock read here: this crate takes the clock as an
    /// argument wherever one is needed, so that a test can drive time and no
    /// library call reads the wall clock behind a caller's back.
    pub fn update_activity_timestamp(&self, now_ms: i64) -> Option<()> {
        self.values()?
            .store_i64_release(self.timestamp_offset, now_ms)
    }

    /// A window onto the file, for whatever the owner keeps in it.
    ///
    /// `None` when the window reaches past the file, which a caller asking for
    /// its own header's worth of bytes never does.
    pub fn region(&self, offset: usize, length: usize) -> Option<AtomicBuffer<'_, ReadOnly>> {
        self.mapping.region(offset, length)
    }

    /// The same window, writable.
    pub fn region_mut(&self, offset: usize, length: usize) -> Option<AtomicBuffer<'_, ReadWrite>> {
        self.mapping.region_mut(offset, length)
    }

    /// Push what has been written out to disk.
    ///
    /// The reference calls this once, after `signalReady`, and the reason is
    /// the one thing a mark file cannot do lazily: the process that reads it is
    /// usually a **different** one, and a reader that arrives before the
    /// mapping is flushed sees a file that says nothing.
    pub fn sync(&self) -> io::Result<()> {
        self.mapping.sync()
    }

    fn values(&self) -> Option<AtomicBuffer<'_, ReadWrite>> {
        self.mapping.region_mut(0, self.mapping.len())
    }
}

impl fmt::Debug for MarkFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MarkFile")
            .field("path", &self.path)
            .field("length", &self.mapping.len())
            .field("version_offset", &self.version_offset)
            .field("timestamp_offset", &self.timestamp_offset)
            .finish()
    }
}

/// `MarkFile.ensureMarkFileLink`: tell a directory where its mark file went.
///
/// A mark file may live outside the directory it describes
/// (`aeron.archive.mark.file.dir`), and then the directory holds a small text
/// file naming the one it belongs to — because everything that reads an archive
/// starts with the directory and has to find the mark from there
/// (`ArchiveTool.resolveMarkFileDir` reads this file's **whole contents**,
/// trims them, and takes the result as a path).
///
/// Two rules, both the reference's:
///
/// * the two directories are the **same**, by canonical path: any link is
///   **removed**. A stale link pointing at the archive itself would make a
///   reader resolve a directory that is already the answer.
/// * they differ: the mark file's directory is written out in US-ASCII,
///   replacing what was there.
///
/// # Errors
///
/// [`io::Error`] for the canonicalisation, the removal or the write — the
/// reference throws `RuntimeException` for each, and prints what it was doing.
pub fn ensure_mark_file_link(
    directory: &Path,
    mark_file: &Path,
    link_filename: &str,
) -> io::Result<()> {
    let directory = directory.canonicalize()?;
    let link = directory.join(link_filename);

    // The mark file's *directory* is what a reader follows the link to, and
    // what the comparison below is between.
    let mark_file_directory = mark_file
        .parent()
        .ok_or_else(|| io::Error::other("the mark file has no directory"))?
        .canonicalize()?;

    if directory == mark_file_directory {
        if link.exists() {
            std::fs::remove_file(&link)?;
        }

        return Ok(());
    }

    std::fs::write(&link, mark_file_directory.to_string_lossy().as_bytes())
}

/// Whether `length` can hold a field at each offset.
fn need(
    length: usize,
    version_offset: usize,
    timestamp_offset: usize,
    path: &Path,
) -> Result<(), MarkFileError> {
    let needed = version_offset.max(timestamp_offset.saturating_add(8));

    if length < needed {
        return Err(MarkFileError::TooShort {
            path: path.to_path_buf(),
            length,
            needed,
        });
    }

    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::*;

    /// Hand-rolled, like the ones in `deepmsg_cnc::create` and
    /// `deepmsg_core::pal`: the workspace has no test dependencies and this is
    /// all one would be used for.
    pub(crate) struct TempDir(PathBuf);

    impl TempDir {
        pub(crate) fn new() -> Self {
            static COUNTER: AtomicU32 = AtomicU32::new(0);
            let n = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path =
                std::env::temp_dir().join(format!("deepmsg-mark-{}-{n}", std::process::id()));
            std::fs::create_dir_all(&path).expect("create temp dir");

            Self(path)
        }

        pub(crate) fn file(&self, name: &str) -> PathBuf {
            self.0.join(name)
        }

        pub(crate) fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// The two offsets the archive's own header puts its fields at, which is
    /// what `openExistingMarkFile` passes: the SBE message header is eight
    /// bytes, `version` is the block's first field and `activityTimestamp` is
    /// at block offset 8 (`schemas/aeron-archive-mark-codecs.xml:33-35`).
    ///
    /// They are a caller's numbers rather than this module's — that is the
    /// point of their being arguments — but there is no reason for a test to
    /// stand in with a different layout than the one caller there is.
    const VERSION_OFFSET: usize = 8;
    const TIMESTAMP_OFFSET: usize = 16;
    const LENGTH: usize = 64 * 1024;
    const VERSION: i32 = 0x0003_0001;

    /// An epoch clock's reading, because the rule is arithmetic on one: the
    /// reference's `EpochClock.time()` is milliseconds since 1970, and a
    /// timestamp of zero is not "now" under it.
    const NOW: i64 = 1_700_000_000_000;

    fn created(dir: &TempDir) -> (MarkFile, PathBuf) {
        let path = dir.file("archive-mark.dat");
        let mark =
            MarkFile::create(&path, LENGTH, VERSION_OFFSET, TIMESTAMP_OFFSET).expect("create");

        (mark, path)
    }

    #[test]
    fn a_created_file_is_not_initialised_until_it_is_signalled() {
        let dir = TempDir::new();
        let (mark, path) = created(&dir);

        assert_eq!(Some(0), mark.version(), "a zero version is the signal");
        assert_eq!(
            Some(0),
            mark.timestamp(),
            "and a mark file that has never been written is all zeros"
        );
        assert!(
            !mark.is_active(NOW, 5_000),
            "which is not alive: the age rule is arithmetic on an **epoch** clock, \
             so a zero timestamp is fifty-odd years old rather than recent"
        );

        assert_eq!(Some(()), mark.signal_ready(VERSION));
        assert_eq!(Some(VERSION), mark.version());

        // And another process sees what was written, which is the whole point
        // of the file: this one opened it separately rather than reading its
        // own mapping.
        let opened = MarkFile::open(&path, VERSION_OFFSET, TIMESTAMP_OFFSET).expect("reopen");
        assert_eq!(Some(VERSION), opened.version());
    }

    /// The rule the reference applies once its wait is over: the timestamp is
    /// no older than the timeout. A timestamp of `NULL_VALUE` is a process that
    /// stopped **on purpose**, and reads as dead by the same arithmetic — which
    /// is why the two are not told apart here, and are by nothing that matters.
    #[test]
    fn liveness_is_the_age_of_the_timestamp() {
        let dir = TempDir::new();
        let (mark, _path) = created(&dir);

        mark.signal_ready(VERSION);
        mark.update_activity_timestamp(NOW);

        assert!(mark.is_active(NOW, 1_000), "written this instant");
        assert!(
            mark.is_active(NOW + 1_000, 1_000),
            "and the timeout is inclusive"
        );
        assert!(
            !mark.is_active(NOW + 1_001, 1_000),
            "a millisecond past it is not"
        );

        // A timestamp **after** the clock reads as alive, because the rule is a
        // subtraction and nothing else: `now - timestamp` is negative, and a
        // negative age is inside any timeout. Worth pinning rather than
        // "fixing": it is what the reference does, and it is what a mark file
        // written by a host whose clock runs ahead of the reader's produces —
        // while the tidier rule (an absolute difference) would be a divergence
        // nobody asked for.
        assert!(
            mark.is_active(NOW - 60_000, 1_000),
            "a timestamp from the future passes the same test"
        );

        mark.signal_terminated(VERSION);
        assert_eq!(Some(NULL_VALUE), mark.timestamp());
        assert_eq!(
            Some(VERSION),
            mark.version(),
            "the file still describes what it described"
        );
        assert!(
            !mark.is_active(NOW, 1_000),
            "a deliberate stop is not alive: NULL_VALUE read as an age is a whole epoch"
        );
    }

    /// `open_or_create` is what a process starting up wants: the second start
    /// finds the first one's file rather than replacing it.
    #[test]
    fn opening_finds_what_creating_made() {
        let dir = TempDir::new();
        let path = dir.file("archive-mark.dat");

        let (mark, _) = created(&dir);
        mark.signal_ready(VERSION);
        mark.update_activity_timestamp(NOW);

        let again = MarkFile::open_or_create(&path, LENGTH, VERSION_OFFSET, TIMESTAMP_OFFSET)
            .expect("open");
        assert_eq!(Some(VERSION), again.version());
        assert_eq!(Some(NOW), again.timestamp(), "not replaced by a fresh file");

        let fresh = dir.file("nothing-here-yet.dat");
        let made = MarkFile::open_or_create(&fresh, LENGTH, VERSION_OFFSET, TIMESTAMP_OFFSET)
            .expect("make");
        assert_eq!(LENGTH, made.length());
        assert_eq!(Some(0), made.version());
    }

    /// A file too short to hold what the caller asked for is refused rather
    /// than read past — the reference's `validateOffsets`.
    #[test]
    fn a_file_too_short_for_the_fields_is_refused() {
        let dir = TempDir::new();
        let path = dir.file("short.dat");

        let error = MarkFile::create(&path, TIMESTAMP_OFFSET, VERSION_OFFSET, TIMESTAMP_OFFSET)
            .expect_err("the timestamp has no room");
        assert!(matches!(error, MarkFileError::TooShort { .. }), "{error:?}");

        let error = MarkFile::open(&path, VERSION_OFFSET, TIMESTAMP_OFFSET)
            .expect_err("the file was never written");
        assert!(matches!(error, MarkFileError::Io(_)), "{error:?}");
    }

    /// The two rules of the link, both from Agrona's `ensureMarkFileLink`: the
    /// same directory means **no link** — and a stale one is removed, because a
    /// reader that followed it would resolve the directory it already has —
    /// while a different one writes the mark file's directory out as text.
    #[test]
    fn the_link_names_the_mark_files_directory_only_when_it_is_elsewhere() {
        let dir = TempDir::new();
        let elsewhere = dir.file("elsewhere");
        std::fs::create_dir_all(&elsewhere).expect("the other directory");

        let link = dir.file("archive-mark.lnk");
        let here = dir.file("archive-mark.dat");
        MarkFile::create(&here, LENGTH, VERSION_OFFSET, TIMESTAMP_OFFSET).expect("create");

        // Same directory: nothing is written.
        ensure_mark_file_link(&dir.0, &here, "archive-mark.lnk").expect("no link needed");
        assert!(!link.exists(), "a link to the same directory says nothing");

        // A stale link from a previous, separated run goes.
        std::fs::write(&link, "/somewhere/else").expect("a stale link");
        ensure_mark_file_link(&dir.0, &here, "archive-mark.lnk").expect("remove the stale link");
        assert!(!link.exists(), "and it is removed rather than left");

        // Separated: the mark file's directory, as text.
        let over_there = elsewhere.join("archive-mark.dat");
        MarkFile::create(&over_there, LENGTH, VERSION_OFFSET, TIMESTAMP_OFFSET).expect("create");

        ensure_mark_file_link(&dir.0, &over_there, "archive-mark.lnk").expect("write the link");

        let written = std::fs::read_to_string(&link).expect("the link");
        assert_eq!(
            elsewhere.canonicalize().expect("canonical"),
            PathBuf::from(written.trim()),
            "what `ArchiveTool.resolveMarkFileDir` reads: the whole file, trimmed"
        );

        // And going back to the same directory removes it again.
        ensure_mark_file_link(&dir.0, &here, "archive-mark.lnk").expect("remove");
        assert!(!link.exists());
    }
}
