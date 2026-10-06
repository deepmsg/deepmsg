//! The archive's mark file: `archive-mark.dat`.
//!
//! Two layers in one file, which is the reference's arrangement and worth
//! keeping straight. [`crate::mark`] is the generic half — a version field that
//! doubles as "this file has been initialised", an activity timestamp, and the
//! rule that reads them. This is the archive's half: an SBE header
//! (`schemas/aeron-archive-mark-codecs.xml`, schema 100) that says **which
//! archive** the file belongs to, and the error log that follows it.
//!
//! The two meet in one place, and it is the easy thing to get wrong: the
//! version and the timestamp live **inside** the SBE block, so the generic half
//! is told where they are rather than owning them. [`VERSION_OFFSET`] and
//! [`ACTIVITY_TIMESTAMP_OFFSET`] are that answer, and
//! `the_offsets_are_the_generated_codecs_own` holds them to the generated
//! codec's.
//!
//! Mirrors `io.aeron.archive.ArchiveMarkFile`
//! (`aeron-archive/src/main/java/io/aeron/archive/ArchiveMarkFile.java`), whose
//! layout facts are: an 8 KiB header, the error buffer behind it, a semantic
//! version whose **major** decides acceptance, and a length that is the page
//! size rounded up over both.

use std::fmt;
use std::io;
use std::path::{Path, PathBuf};

use deepmsg_codec::archive_mark::mark_file_header_codec::{
    MarkFileHeaderDecoder, MarkFileHeaderEncoder, SBE_BLOCK_LENGTH,
};
use deepmsg_codec::archive_mark::message_header_codec::{
    ENCODED_LENGTH as MESSAGE_HEADER_LENGTH, MessageHeaderDecoder,
};
use deepmsg_codec::archive_mark::{ReadBuf, WriteBuf};
use deepmsg_core::buffer::{AtomicBuffer, ReadOnly, ReadWrite};

use crate::mark::{self, MarkFile, MarkFileError};

/// `ArchiveMarkFile.FILENAME` (`:82`).
pub const FILENAME: &str = "archive-mark.dat";

/// `ArchiveMarkFile.LINK_FILENAME` (`:87`).
pub const LINK_FILENAME: &str = "archive-mark.lnk";

/// `ArchiveMarkFile.HEADER_LENGTH` (`:77`): everything the archive has to say
/// about itself must fit here, and the error buffer starts at this offset.
pub const HEADER_LENGTH: usize = 8 * 1024;

/// The semantic version this build writes (`ArchiveMarkFile.java:55-72`).
pub const MAJOR_VERSION: i32 = 3;
/// See [`MAJOR_VERSION`].
pub const MINOR_VERSION: i32 = 1;
/// See [`MAJOR_VERSION`].
pub const PATCH_VERSION: i32 = 0;

/// `SemanticVersion.compose` (`org.agrona.SemanticVersion`): major in the third
/// byte, then minor, then patch — so a major can be read without knowing the
/// rest, which is all [`version_is_acceptable`] does.
pub const SEMANTIC_VERSION: i32 = (MAJOR_VERSION << 16) | (MINOR_VERSION << 8) | PATCH_VERSION;

/// How often a running archive stamps its mark file
/// (`Archive.Configuration.MARK_FILE_UPDATE_INTERVAL_MS`, `Archive.java:668`).
pub const MARK_FILE_UPDATE_INTERVAL_MS: i64 = 1_000;

/// How long a mark file's timestamp may go unstamped before the archive that
/// owns it is taken to be gone: ten intervals (`Archive.java:673`).
pub const LIVENESS_TIMEOUT_MS: i64 = 10 * MARK_FILE_UPDATE_INTERVAL_MS;

/// `Archive.Configuration.ERROR_BUFFER_LENGTH_DEFAULT` (`Archive.java:633`).
pub const ERROR_BUFFER_LENGTH_DEFAULT: usize = 1024 * 1024;

/// Where the version field sits in the file: the SBE message header is eight
/// bytes and `version` is the block's first field — `mark_file_header_codec`'s
/// "encodedOffset: 0" for `version`, read off the generated accessor.
pub const VERSION_OFFSET: usize = 8;

/// Where the activity timestamp sits: the block's field at offset 8, so eight
/// bytes further in than the version.
pub const ACTIVITY_TIMESTAMP_OFFSET: usize = 16;

/// The archive's own SBE header: which archive this is, on what channels, and
/// how big its error log is.
///
/// The fields `ArchiveMarkFile.encode` sets (`:491-505`) are all here, because
/// a reader decodes the same header the reference's `ArchiveTool` prints. The
/// two the archive *does not* choose are not: `pid` is stamped by whoever
/// creates the file (`:196`) and `version` by [`ArchiveMarkFile::signal_ready`],
/// which is the whole of what the generic half owns.
#[derive(Clone, Debug)]
pub struct Header<'a> {
    /// When the archive process started, in epoch milliseconds.
    pub start_timestamp: i64,
    /// The control channel a client connects to, or `None` when
    /// `aeron.archive.control.channel.enabled` is false.
    pub control_channel: Option<&'a str>,
    /// The local control channel.
    pub local_control_channel: &'a str,
    /// The recording-events channel, or `None` when events are off.
    pub events_channel: Option<&'a str>,
    /// The Aeron directory this archive records from.
    pub aeron_directory: &'a str,
    /// The control stream id.
    pub control_stream_id: i32,
    /// The local control stream id.
    pub local_control_stream_id: i32,
    /// The recording-events stream id.
    pub events_stream_id: i32,
    /// The archive's id, which a client names to reach it.
    pub archive_id: i64,
}

/// Why an archive mark file could not be opened or created.
#[derive(Debug)]
pub enum ArchiveMarkError {
    /// The file itself.
    Mark(MarkFileError),
    /// The version it holds is not this build's: the reference's
    /// `validateVersion` compares the **major** and nothing else
    /// (`ArchiveMarkFile.java:507-517`).
    Version {
        /// The file's path.
        path: PathBuf,
        /// The major it holds.
        found: i32,
        /// The major this build writes.
        expected: i32,
    },
    /// The archive has more to say about itself than the header holds
    /// (`:458-462`).
    HeaderTooLong {
        /// The length the fields need.
        needed: usize,
        /// The room there is.
        capacity: usize,
    },
    /// The file has been created and never published: its version field is
    /// still zero, which is the state the reference waits out at startup and
    /// then refuses with `mark file is created but not initialised`
    /// (`org.agrona.MarkFile.isActive`'s loop).
    ///
    /// Its own variant rather than a version mismatch, because the two say
    /// different things: a major of zero is not an old format, it is a file
    /// nobody has finished writing.
    Uninitialised {
        /// The file's path.
        path: PathBuf,
    },
    /// The file is not a mark file this build can read: no SBE message header
    /// where one has to be.
    NotAMarkFile {
        /// The file's path.
        path: PathBuf,
    },
}

impl fmt::Display for ArchiveMarkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Mark(error) => write!(f, "{error}"),
            Self::Version {
                path,
                found,
                expected,
            } => write!(
                f,
                "mark file ({}) major version {found} does not match software: {expected}",
                path.display()
            ),
            Self::HeaderTooLong { needed, capacity } => write!(
                f,
                "ArchiveMarkFile headerLength={needed} > headerLengthCapacity={capacity}"
            ),
            Self::Uninitialised { path } => write!(
                f,
                "mark file ({}) is created but not initialised",
                path.display()
            ),
            Self::NotAMarkFile { path } => write!(
                f,
                "mark file ({}) has no SBE message header where one must be",
                path.display()
            ),
        }
    }
}

impl std::error::Error for ArchiveMarkError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Mark(error) => Some(error),
            Self::Version { .. }
            | Self::HeaderTooLong { .. }
            | Self::Uninitialised { .. }
            | Self::NotAMarkFile { .. } => None,
        }
    }
}

impl From<MarkFileError> for ArchiveMarkError {
    fn from(error: MarkFileError) -> Self {
        Self::Mark(error)
    }
}

/// An archive mark file, mapped.
pub struct ArchiveMarkFile {
    mark: MarkFile,
    error_buffer_length: usize,
}

impl ArchiveMarkFile {
    /// Create the mark file for an archive in `directory`, replacing one that is
    /// there.
    ///
    /// The length is the reference's `alignedTotalFileLength` (`:450-471`): the
    /// header and the error buffer together, rounded up to the page size —
    /// because a mapping is whole pages whatever the file asks for, and the
    /// error buffer has to be what the reader expects to find behind the
    /// header.
    ///
    /// The pid is this process's, stamped once, here: `ArchiveMarkFile`'s
    /// creating constructor does the same (`:196`) and its opening one does
    /// not, so a reader that finds a stale file learns who wrote it rather than
    /// who looked at it last.
    ///
    /// # Errors
    ///
    /// [`ArchiveMarkError::HeaderTooLong`] when what the archive has to say
    /// does not fit the 8 KiB a header is, and the file system's own errors.
    pub fn create(
        directory: &Path,
        header: &Header<'_>,
        error_buffer_length: usize,
        page_size: usize,
        pid: i64,
    ) -> Result<Self, ArchiveMarkError> {
        let path = directory.join(FILENAME);
        let length = total_length(error_buffer_length, page_size);

        // Counted before anything is written, which is what the reference does
        // with the same numbers in `alignedTotalFileLength` (`:450-462`): a
        // header that does not fit is a refusal, not a truncated file, and a
        // refusal only works if it happens before the writing.
        let needed = header_length(header, error_buffer_length);

        if needed > HEADER_LENGTH {
            return Err(ArchiveMarkError::HeaderTooLong {
                needed,
                capacity: HEADER_LENGTH,
            });
        }

        let mut bytes = vec![0_u8; needed];
        let encoded = encode(&mut bytes, header, error_buffer_length, pid)?;

        let mark = MarkFile::create(&path, length, VERSION_OFFSET, ACTIVITY_TIMESTAMP_OFFSET)?;
        write(&mark, &bytes[..encoded])?;

        mark::ensure_mark_file_link(directory, &path, LINK_FILENAME).map_err(MarkFileError::Io)?;

        Ok(Self {
            mark,
            error_buffer_length,
        })
    }

    /// Open the mark file `directory` describes, following the link if the
    /// archive wrote one.
    ///
    /// The version is checked here rather than in [`crate::mark`], because it is
    /// this product's format that decides what a major means — the reference
    /// hands the generic file a `versionCheck` callback for the same reason
    /// (`:543-552`).
    ///
    /// # Errors
    ///
    /// [`ArchiveMarkError::Version`] for a file written by another major,
    /// [`ArchiveMarkError::NotAMarkFile`] for one with no message header at
    /// offset 0, and the file system's own errors.
    pub fn open(directory: &Path) -> Result<Self, ArchiveMarkError> {
        let path = mark_file_path(directory);
        let mark = MarkFile::open(&path, VERSION_OFFSET, ACTIVITY_TIMESTAMP_OFFSET)?;

        let bytes = header_bytes(&mark)?;
        let (version, error_buffer_length) = fields(&bytes)?;

        if 0 == version {
            return Err(ArchiveMarkError::Uninitialised { path });
        }

        if !version_is_acceptable(version) {
            return Err(ArchiveMarkError::Version {
                path,
                found: major_of(version),
                expected: MAJOR_VERSION,
            });
        }

        Ok(Self {
            mark,
            error_buffer_length,
        })
    }

    /// The file this is.
    pub fn path(&self) -> &Path {
        self.mark.path()
    }

    /// How long the file is.
    pub fn length(&self) -> usize {
        self.mark.length()
    }

    /// `ArchiveTool pid` (`ArchiveTool.java:504-509`): the process that
    /// **created** the file, which is the archive it belongs to.
    pub fn pid(&self) -> Option<i64> {
        self.field(|header| header.pid())
    }

    /// The semantic version the file holds, which is zero until
    /// [`ArchiveMarkFile::signal_ready`] has run.
    pub fn version(&self) -> Option<i32> {
        self.field(|header| header.version())
    }

    /// The archive id a client has to name, or `None` for a file written before
    /// the field existed (SBE version 2).
    pub fn archive_id(&self) -> Option<i64> {
        self.field(|header| header.archive_id()).flatten()
    }

    /// When the archive process started, in epoch milliseconds.
    pub fn start_timestamp(&self) -> Option<i64> {
        self.field(|header| header.start_timestamp())
    }

    /// The activity timestamp, which is the proof of life.
    pub fn activity_timestamp(&self) -> Option<i64> {
        self.field(|header| header.activity_timestamp())
    }

    /// The four strings in the header, in the order the format puts them.
    pub fn channels(&self) -> Option<Channels> {
        let bytes = header_bytes(&self.mark).ok()?;
        let mut header = decoder(&bytes).ok()?;

        // Read in order, each length prefix sitting after the previous string's
        // bytes: the codec's `*_decoder()` calls advance the decoder's limit, so
        // asking for `events_channel` on its own would read whatever
        // `control_channel` left behind and call it a channel name. Each slice
        // is copied out before the next call, because the next one is a
        // mutation of the same decoder.
        let at = header.control_channel_decoder();
        let control = text(header.control_channel_slice(at));

        let at = header.local_control_channel_decoder();
        let local_control = text(header.local_control_channel_slice(at));

        let at = header.events_channel_decoder();
        let events = text(header.events_channel_slice(at));

        let at = header.aeron_directory_decoder();
        let aeron_directory = text(header.aeron_directory_slice(at));

        Some(Channels {
            control,
            local_control,
            events,
            aeron_directory,
        })
    }

    /// Whether the archive that owns this file is still alive, by this
    /// product's timeout ([`LIVENESS_TIMEOUT_MS`]).
    ///
    /// The rule itself is [`crate::mark`]'s; what this adds is the number, and
    /// the fact that a file whose version field is still zero is one nobody has
    /// published yet — which the reference waits out at startup and throws
    /// over, and which a caller here can see with [`ArchiveMarkFile::version`].
    pub fn is_active(&self, now_ms: i64) -> bool {
        self.mark.is_active(now_ms, LIVENESS_TIMEOUT_MS)
    }

    /// The error log, which begins at [`HEADER_LENGTH`] and runs for the length
    /// the header names (`:263-266`).
    ///
    /// The format inside it is the Aeron distinct error log, which this build
    /// already writes and which the reference's own `ErrorStat` already reads
    /// (`deepmsg_cnc::error_log`): nothing here re-implements it, this is the
    /// window onto it.
    pub fn error_buffer(&self) -> Option<AtomicBuffer<'_, ReadWrite>> {
        self.mark
            .region_mut(HEADER_LENGTH, self.error_buffer_length)
    }

    /// How long that window is.
    pub const fn error_buffer_length(&self) -> usize {
        self.error_buffer_length
    }

    /// Publish this file as ready, at the version this build writes.
    ///
    /// The reference's `signalReady()`: the timestamp and then the version,
    /// then a flush — the reader is usually a **different process**, and one
    /// that arrives before the mapping is flushed reads a file that says
    /// nothing (`:331-339`).
    pub fn signal_ready(&self, now_ms: i64) -> io::Result<()> {
        self.mark
            .update_activity_timestamp(now_ms)
            .and_then(|()| self.mark.signal_ready(SEMANTIC_VERSION))
            .ok_or_else(|| io::Error::other("the mark file has no room for its own header"))?;

        self.mark.sync()
    }

    /// A proof of life (`:354-359`).
    pub fn update_activity_timestamp(&self, now_ms: i64) -> Option<()> {
        self.mark.update_activity_timestamp(now_ms)
    }

    /// Say the archive stopped **on purpose** (`:344-347`): a timestamp of
    /// `NULL_VALUE` rather than a stale one, so a reader can tell a clean
    /// shutdown from a crash.
    pub fn signal_terminated(&self) -> Option<()> {
        self.mark.signal_terminated(SEMANTIC_VERSION)
    }

    /// A window onto the whole file.
    pub fn region(&self) -> Option<AtomicBuffer<'_, ReadOnly>> {
        self.mark.region(0, self.mark.length())
    }

    /// Read one field out of the SBE header.
    ///
    /// The header is read and decoded per call rather than kept, which is what
    /// [`deepmsg_cnc::CncFile`]'s accessors do for the same reason: a decoded
    /// value that borrowed this struct's own mapping would make the type
    /// self-referential, and a header is 8 KiB.
    fn field<T>(&self, read: impl FnOnce(&mut MarkFileHeaderDecoder<'_>) -> T) -> Option<T> {
        let bytes = header_bytes(&self.mark).ok()?;
        let mut header = decoder(&bytes).ok()?;

        Some(read(&mut header))
    }
}

impl fmt::Debug for ArchiveMarkFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ArchiveMarkFile")
            .field("path", &self.mark.path())
            .field("error_buffer_length", &self.error_buffer_length)
            .finish()
    }
}

/// The directory a mark file is in, following the link if the archive wrote one
/// (`ArchiveTool.resolveMarkFileDir`, `ArchiveTool.java:1150-1178`).
///
/// The link's **whole contents**, trimmed, are the path. A link that cannot be
/// read falls back to the directory itself, which is the reference's `else`
/// branch — a link that is *there* and unreadable is the reference's
/// `RuntimeException`, and the difference matters: the two answers are
/// different archives, so guessing would open the wrong one.
pub fn mark_file_directory(directory: &Path) -> PathBuf {
    let link = directory.join(LINK_FILENAME);

    std::fs::read_to_string(&link).map_or_else(
        |_| directory.to_path_buf(),
        |contents| PathBuf::from(contents.trim()),
    )
}

/// The mark file's own path, link and all.
pub fn mark_file_path(directory: &Path) -> PathBuf {
    mark_file_directory(directory).join(FILENAME)
}

/// The major of a semantic version, as `SemanticVersion.major` computes it
/// (`(version >> 16) & 0xFF`, read from `org.agrona.SemanticVersion`).
pub const fn major_of(version: i32) -> i32 {
    (version >> 16) & 0xFF
}

/// Whether a version is one this build accepts: the reference's
/// `validateVersion`, which compares the major and nothing else
/// (`:507-517`) — a minor ahead of ours is a file this build can still read.
pub const fn version_is_acceptable(version: i32) -> bool {
    major_of(version) == MAJOR_VERSION
}

/// The four strings, in the order the format puts them.
///
/// A struct rather than four accessors because they are a **sequence**: the
/// second one's length prefix starts where the first one's bytes end, so
/// reading one alone is not a smaller question than reading them all.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Channels {
    /// The control channel, empty when the archive serves none.
    pub control: String,
    /// The local control channel.
    pub local_control: String,
    /// The recording-events channel, empty when events are off.
    pub events: String,
    /// The Aeron directory the archive records from.
    pub aeron_directory: String,
}

/// What `alignedTotalFileLength` counts before it encodes anything
/// (`:450-462`): the message header, the block, four length prefixes and the
/// four strings.
fn header_length(header: &Header<'_>, error_buffer_length: usize) -> usize {
    let _ = error_buffer_length;

    MESSAGE_HEADER_LENGTH
        + SBE_BLOCK_LENGTH as usize
        + 4 * 4
        + header.control_channel.unwrap_or_default().len()
        + header.local_control_channel.len()
        + header.events_channel.unwrap_or_default().len()
        + header.aeron_directory.len()
}

/// A header string, as text: the format says US-ASCII, and a byte outside it is
/// the writer's problem rather than a reason to fail a read.
fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

/// `align(HEADER_LENGTH + error_buffer_length, page_size)` (`:450-471`).
fn total_length(error_buffer_length: usize, page_size: usize) -> usize {
    deepmsg_cnc::layout::align_up(HEADER_LENGTH + error_buffer_length, page_size)
}

/// Encode the SBE header into `bytes` and say how much of it was used.
///
/// The encoder writes through a `&mut [u8]` rather than into the mapping, which
/// is the generated codec's shape: the bytes are built here and copied in, so a
/// header is never half-written into a file another process is reading.
fn encode(
    bytes: &mut [u8],
    header: &Header<'_>,
    error_buffer_length: usize,
    pid: i64,
) -> Result<usize, ArchiveMarkError> {
    // The body is wrapped past the message header and the message header is
    // written at 0 over it, which is how the generated encoders compose — the
    // same shape `tests/integration/sbe_golden.rs` uses to re-encode a golden.
    let encoder =
        MarkFileHeaderEncoder::default().wrap(WriteBuf::new(bytes), MESSAGE_HEADER_LENGTH);
    let mut message_header = encoder.header(0);
    let mut encoder = message_header
        .parent()
        .map_err(|_| ArchiveMarkError::NotAMarkFile {
            path: PathBuf::new(),
        })?;

    // `version` and `activityTimestamp` are deliberately **not** set here.
    // They are the generic half's two fields — the ones `signal_ready` writes —
    // and the reference's `encode` sets neither of them (`:491-505`): a mark
    // file is created with a zero version, which is what "created but not
    // initialised" means to every reader of it.
    encoder
        .start_timestamp(header.start_timestamp)
        .pid(pid)
        .control_stream_id(header.control_stream_id)
        .local_control_stream_id(header.local_control_stream_id)
        .events_stream_id(header.events_stream_id)
        .header_length(as_i32(HEADER_LENGTH))
        .error_buffer_length(as_i32(error_buffer_length))
        .archive_id(header.archive_id)
        .control_channel(header.control_channel.unwrap_or_default().as_bytes())
        .local_control_channel(header.local_control_channel.as_bytes())
        .events_channel(header.events_channel.unwrap_or_default().as_bytes())
        .aeron_directory(header.aeron_directory.as_bytes());

    Ok(MESSAGE_HEADER_LENGTH + encoder.encoded_length())
}

/// Decode the header in `bytes`.
fn decoder(bytes: &[u8]) -> Result<MarkFileHeaderDecoder<'_>, ArchiveMarkError> {
    let message_header = MessageHeaderDecoder::default().wrap(ReadBuf::new(bytes), 0);

    Ok(MarkFileHeaderDecoder::default().header(message_header, 0))
}

/// The two fields this module needs before it can hand out any others: the
/// version, and how long the error buffer is.
///
/// `error_buffer_length` arrived in SBE version 1 and is absent in a version 0
/// file, and the reference reads that absence as **zero**: a file old enough to
/// have no error buffer has none (`:263-266`, whose `headerLength() > 0` test
/// is the same statement). The generated decoder already applies the file's own
/// acting version, so a version 0 file decodes with the field missing rather
/// than misread.
fn fields(bytes: &[u8]) -> Result<(i32, usize), ArchiveMarkError> {
    let header = decoder(bytes)?;
    let error_buffer_length = header
        .error_buffer_length()
        .and_then(|length| usize::try_from(length).ok())
        .unwrap_or(0);

    Ok((header.version(), error_buffer_length))
}

/// The header's bytes, copied out of the mapping.
fn header_bytes(mark: &MarkFile) -> Result<Vec<u8>, MarkFileError> {
    let mut bytes = vec![0_u8; HEADER_LENGTH];

    mark.region(0, HEADER_LENGTH)
        .and_then(|region| region.copy_out(0, &mut bytes))
        .ok_or(MarkFileError::TooShort {
            path: mark.path().to_path_buf(),
            length: mark.length(),
            needed: HEADER_LENGTH,
        })?;

    Ok(bytes)
}

fn write(mark: &MarkFile, bytes: &[u8]) -> Result<(), MarkFileError> {
    mark.region_mut(0, bytes.len())
        .and_then(|region| region.copy_in(0, bytes))
        .ok_or(MarkFileError::TooShort {
            path: mark.path().to_path_buf(),
            length: mark.length(),
            needed: bytes.len(),
        })
}

fn as_i32(value: usize) -> i32 {
    i32::try_from(value).unwrap_or(i32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mark::tests::TempDir;

    /// An epoch clock's reading, as in `crate::mark`'s tests: the liveness rule
    /// is arithmetic on one.
    const NOW: i64 = 1_700_000_000_000;
    const PID: i64 = 4242;
    const PAGE: usize = 4096;
    const ARCHIVE_ID: i64 = 7;

    fn header() -> Header<'static> {
        Header {
            start_timestamp: NOW - 5_000,
            control_channel: Some("aeron:udp?endpoint=localhost:8010"),
            local_control_channel: "aeron:ipc",
            events_channel: Some("aeron:udp?endpoint=localhost:8011"),
            aeron_directory: "/dev/shm/aeron-gavin",
            control_stream_id: 10,
            local_control_stream_id: 20,
            events_stream_id: 30,
            archive_id: ARCHIVE_ID,
        }
    }

    fn created(dir: &TempDir) -> ArchiveMarkFile {
        ArchiveMarkFile::create(
            dir.path(),
            &header(),
            ERROR_BUFFER_LENGTH_DEFAULT,
            PAGE,
            PID,
        )
        .expect("create")
    }

    /// The file the reference would have written: the length is the header and
    /// the error buffer rounded up to a page, and everything the archive has to
    /// say about itself is where a reader looks for it.
    #[test]
    fn what_is_created_is_what_a_reader_finds() {
        let dir = TempDir::new();
        let mark = created(&dir);

        assert_eq!(
            deepmsg_cnc::layout::align_up(HEADER_LENGTH + ERROR_BUFFER_LENGTH_DEFAULT, PAGE),
            mark.length(),
            "the error buffer is inside the file, and the file is whole pages"
        );

        assert_eq!(Some(PID), mark.pid(), "stamped by whoever created it");
        assert_eq!(Some(ARCHIVE_ID), mark.archive_id());
        assert_eq!(Some(NOW - 5_000), mark.start_timestamp());
        let channels = mark.channels().expect("the four strings");
        assert_eq!("aeron:udp?endpoint=localhost:8010", channels.control);
        assert_eq!("aeron:ipc", channels.local_control);
        assert_eq!("aeron:udp?endpoint=localhost:8011", channels.events);
        assert_eq!("/dev/shm/aeron-gavin", channels.aeron_directory);

        assert_eq!(Some(0), mark.version(), "nothing has been published yet");
        assert!(
            !mark.is_active(NOW),
            "and a file nobody has stamped is not alive"
        );

        // The error buffer is where the header says it is, and it is as long as
        // it says: an error log written at the header's end has to land in it.
        let buffer = mark.error_buffer().expect("the error buffer");
        assert_eq!(ERROR_BUFFER_LENGTH_DEFAULT, mark.error_buffer_length());
        assert!(buffer.len() >= ERROR_BUFFER_LENGTH_DEFAULT);
    }

    /// Ready means ready: the version, the timestamp, and a reader in another
    /// process sees both.
    #[test]
    fn signalling_ready_is_what_makes_it_alive() {
        let dir = TempDir::new();
        let mark = created(&dir);

        mark.signal_ready(NOW).expect("signalled");

        assert_eq!(Some(SEMANTIC_VERSION), mark.version());
        assert_eq!(
            3,
            major_of(mark.version().expect("a version")),
            "which is major 3"
        );
        assert_eq!(Some(NOW), mark.activity_timestamp());
        assert!(mark.is_active(NOW));
        assert!(
            mark.is_active(NOW + LIVENESS_TIMEOUT_MS),
            "and it is alive for the whole timeout"
        );
        assert!(!mark.is_active(NOW + LIVENESS_TIMEOUT_MS + 1));

        // A stop that was meant reads as not alive, and leaves the version —
        // the file still describes what it described.
        mark.signal_terminated().expect("terminated");
        assert_eq!(Some(crate::mark::NULL_VALUE), mark.activity_timestamp());
        assert!(!mark.is_active(NOW));
        assert_eq!(Some(SEMANTIC_VERSION), mark.version());
    }

    /// A file that has been created and never published is **not** an old
    /// format: its version field is zero, which is what "nobody has finished
    /// writing this" looks like, and the reference has its own words and its
    /// own wait for it (`org.agrona.MarkFile.isActive`).
    #[test]
    fn a_file_nobody_published_is_not_a_version_mismatch() {
        let dir = TempDir::new();
        let mark = created(&dir);

        assert_eq!(Some(0), mark.version(), "created, and not published");

        let error = ArchiveMarkFile::open(dir.path()).expect_err("not published yet");
        assert!(
            matches!(error, ArchiveMarkError::Uninitialised { .. }),
            "{error:?}"
        );
        assert!(
            error.to_string().contains("created but not initialised"),
            "the reference's own words: {error}"
        );

        // And once it is published, it opens.
        mark.signal_ready(NOW).expect("signalled");
        assert!(ArchiveMarkFile::open(dir.path()).is_ok());
    }

    /// The error buffer's length is a **field**, not a constant: what the
    /// archive was configured with is what a reader finds behind the header,
    /// and the file is sized for it.
    #[test]
    fn the_error_buffer_is_as_long_as_the_header_says() {
        let dir = TempDir::new();
        let length = ERROR_BUFFER_LENGTH_DEFAULT + PAGE;

        let mark =
            ArchiveMarkFile::create(dir.path(), &header(), length, PAGE, PID).expect("create");
        mark.signal_ready(NOW).expect("signalled");

        assert_eq!(length, mark.error_buffer_length());
        assert_eq!(
            deepmsg_cnc::layout::align_up(HEADER_LENGTH + length, PAGE),
            mark.length(),
            "and the file is whole pages over both"
        );

        // Opened again, because a field that only round-trips in memory is not
        // a field: this one is read back out of the mapping.
        let opened = ArchiveMarkFile::open(dir.path()).expect("open");
        assert_eq!(length, opened.error_buffer_length());
        assert!(
            opened.error_buffer().expect("the window").len() >= length,
            "the window is at least as long as the field says"
        );
    }

    /// The join between the two layers, which is the one thing here that could
    /// silently be wrong: the generic half is told where the version and the
    /// timestamp are, and the answer has to be where the **generated codec**
    /// puts them.
    ///
    /// A schema that moved either field would move the codec's own offsets and
    /// leave these two constants where they were — a file half of this build
    /// could read and the other half could not, which is the kind of failure
    /// that only shows up as a process that is never seen as alive.
    #[test]
    fn the_offsets_are_the_generated_codecs_own() {
        let mut bytes = vec![0_u8; HEADER_LENGTH];
        let length =
            encode(&mut bytes, &header(), ERROR_BUFFER_LENGTH_DEFAULT, PID).expect("encode");

        const VERSION: i32 = 0x0003_0102;
        const TIMESTAMP: i64 = 1_234_567_890;

        bytes[VERSION_OFFSET..VERSION_OFFSET + 4].copy_from_slice(&VERSION.to_le_bytes());
        bytes[ACTIVITY_TIMESTAMP_OFFSET..ACTIVITY_TIMESTAMP_OFFSET + 8]
            .copy_from_slice(&TIMESTAMP.to_le_bytes());

        let decoded = decoder(&bytes[..length]).expect("decode");

        assert_eq!(
            VERSION,
            decoded.version(),
            "what sits at VERSION_OFFSET is the field the codec calls `version`"
        );
        assert_eq!(
            TIMESTAMP,
            decoded.activity_timestamp(),
            "and what sits at ACTIVITY_TIMESTAMP_OFFSET is `activityTimestamp`"
        );
    }

    /// A file from another major is refused, and refused by **major**: the
    /// reference's `validateVersion` compares that field and nothing else, so a
    /// minor ahead of ours is still a file this build may read.
    #[test]
    fn another_major_is_refused_and_a_minor_is_not() {
        let dir = TempDir::new();
        {
            let mark = created(&dir);
            mark.signal_ready(NOW).expect("signalled");
        }

        // The version field is the **generic** layer's to write — that is the
        // whole of what it owns — so a file from another build is one whose
        // field was written by another build, which is what this is.
        let version_of_another_build = |version: i32| {
            let generic = MarkFile::open(
                &dir.file(FILENAME),
                VERSION_OFFSET,
                ACTIVITY_TIMESTAMP_OFFSET,
            )
            .expect("open");
            generic.signal_ready(version).expect("write the version");
        };

        // A minor ahead: still ours — same major, later minor.
        version_of_another_build((MAJOR_VERSION << 16) | (99 << 8));
        assert!(
            ArchiveMarkFile::open(dir.path()).is_ok(),
            "a minor ahead of ours is a file this build reads"
        );

        // Another major: not ours, and the message says which.
        version_of_another_build((2 << 16) | (1 << 8));
        let error = ArchiveMarkFile::open(dir.path()).expect_err("major 2 is not ours");

        assert!(
            matches!(
                error,
                ArchiveMarkError::Version {
                    found: 2,
                    expected: 3,
                    ..
                }
            ),
            "{error:?}"
        );
        assert!(
            error.to_string().starts_with("mark file ("),
            "the reference's own wording: {error}"
        );
    }

    /// `headerLength > headerLengthCapacity` is a refusal, not a truncation
    /// (`ArchiveMarkFile.java:458-462`): an archive whose channels do not fit
    /// the header has to say so rather than write half of itself.
    #[test]
    fn an_archive_too_big_for_its_header_is_refused() {
        let dir = TempDir::new();
        let long = "x".repeat(HEADER_LENGTH);

        let error = ArchiveMarkFile::create(
            dir.path(),
            &Header {
                aeron_directory: &long,
                ..header()
            },
            ERROR_BUFFER_LENGTH_DEFAULT,
            PAGE,
            PID,
        )
        .expect_err("four strings cannot exceed the header");

        assert!(
            matches!(error, ArchiveMarkError::HeaderTooLong { .. }),
            "{error:?}"
        );
    }

    /// The link, from the reader's side: a directory whose mark file is
    /// elsewhere follows the link, and one that has none is its own answer.
    #[test]
    fn the_mark_file_is_found_through_the_link() {
        let dir = TempDir::new();
        let elsewhere = dir.file("elsewhere");
        std::fs::create_dir_all(&elsewhere).expect("the other directory");

        assert_eq!(
            dir.file(FILENAME),
            mark_file_path(dir.path()),
            "with no link, the directory is the answer"
        );

        ArchiveMarkFile::create(
            &elsewhere,
            &header(),
            ERROR_BUFFER_LENGTH_DEFAULT,
            PAGE,
            PID,
        )
        .expect("create elsewhere");
        mark::ensure_mark_file_link(dir.path(), &elsewhere.join(FILENAME), LINK_FILENAME)
            .expect("link");

        assert_eq!(
            elsewhere.canonicalize().expect("canonical").join(FILENAME),
            mark_file_path(dir.path()),
            "and with one, the link's contents are"
        );
    }
}
