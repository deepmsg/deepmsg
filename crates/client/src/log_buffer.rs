//! Mapping a log buffer file and reading the geometry it describes.
//!
//! Mirrors `aeron-client/src/main/c/util/aeron_fileutil.c:1309-1356` — the
//! `aeron_raw_log_map_existing` path, which is the only way a client ever gets
//! a log buffer. **A client never creates one**: the driver's native resource
//! agent does, with `O_CREAT | O_EXCL`, and a client that created it first
//! would make the driver's own creation fail.
//!
//! # The metadata is the *last* page, not the first
//!
//! This is the fact most likely to be got wrong, because it is the opposite of
//! the CnC file, where the metadata block is at the front. A log buffer file is
//!
//! ```text
//! offset 0             : term 0          (term_length bytes)
//! offset term_length   : term 1
//! offset 2*term_length : term 2
//! offset 3*term_length : metadata       (4096 bytes)
//! ```
//!
//! so the file is `ALIGN(3 * term_length + 4096, page_size)` bytes and the
//! metadata begins at `length - 4096`. Both of the reference's C map functions
//! say so (`aeron_raw_log_map` and `aeron_raw_log_map_existing`, each computing
//! `addr + (length - AERON_LOGBUFFER_META_DATA_LENGTH)`), and Java says it
//! independently by numbering the metadata section `PARTITION_COUNT`
//! (`LogBufferDescriptor.java:53` — `LOG_META_DATA_SECTION_INDEX`), i.e. the
//! section *after* the three terms.
//!
//! # The geometry comes from the file, not the message
//!
//! Neither `ON_PUBLICATION_READY` nor `ON_AVAILABLE_IMAGE` carries a term
//! length. Both carry a path and a counter id; everything else is read from the
//! mapped metadata, which is why this module validates it the way
//! `aeron_raw_log_map_existing` does before handing anything back.

use std::io;
use std::path::Path;

use deepmsg_core::logbuffer::{descriptor, position};
use deepmsg_core::pal::MappedFile;

/// What the metadata block says about the log's shape.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LogGeometry {
    /// How long each of the three terms is.
    pub term_length: i32,
    /// The absolute term id the stream started at. Randomised per publication,
    /// so it is never derivable from anything else.
    pub initial_term_id: i32,
    /// `log2(term_length)`.
    pub bits_to_shift: u32,
    /// Where the metadata block begins — `file_length - 4096`.
    pub metadata_offset: usize,
}

/// A log buffer file the driver has already created.
pub struct LogBuffer {
    file: MappedFile,
    geometry: LogGeometry,
}

impl LogBuffer {
    /// Map `path` and read its geometry.
    ///
    /// `writable` produces the producer's view; a subscriber maps the same file
    /// read-only.
    ///
    /// # Errors
    ///
    /// [`io::ErrorKind::NotFound`] if the driver has not created the file yet —
    /// which for a publication is impossible after `ON_PUBLICATION_READY`,
    /// because the driver only sends it once the mapping succeeded
    /// (`aeron_driver_conductor.c:4056-4068`). `InvalidData` if the metadata
    /// does not describe a usable log.
    pub fn open(path: &Path, writable: bool) -> io::Result<Self> {
        let file = if writable {
            MappedFile::open_readwrite(path)?
        } else {
            MappedFile::open_readonly(path)?
        };

        let metadata_offset = file
            .len()
            .checked_sub(descriptor::METADATA_LENGTH)
            .ok_or_else(|| invalid("the file is shorter than one metadata block"))?;

        let metadata = file
            .region(metadata_offset, descriptor::METADATA_LENGTH)
            .ok_or_else(|| invalid("the metadata block is not addressable"))?;

        let term_length = metadata
            .load_i32(descriptor::TERM_LENGTH_OFFSET)
            .ok_or_else(|| invalid("the term length is not readable"))?;
        let bits_to_shift = position::bits_to_shift(term_length)
            .ok_or_else(|| invalid("the term length is not a power of two in range"))?;

        let initial_term_id = metadata
            .load_i32(descriptor::INITIAL_TERM_ID_OFFSET)
            .ok_or_else(|| invalid("the initial term id is not readable"))?;

        // The three terms must actually fit, or every offset computed from the
        // geometry would run past the mapping.
        let terms_length = (term_length as usize)
            .checked_mul(descriptor::PARTITION_COUNT)
            .ok_or_else(|| invalid("the term length overflows"))?;
        if terms_length > metadata_offset {
            return Err(invalid("the terms do not fit before the metadata block"));
        }

        Ok(Self {
            file,
            geometry: LogGeometry {
                term_length,
                initial_term_id,
                bits_to_shift,
                metadata_offset,
            },
        })
    }

    /// The log's shape.
    pub const fn geometry(&self) -> LogGeometry {
        self.geometry
    }

    /// The mapped file.
    pub const fn file(&self) -> &MappedFile {
        &self.file
    }

    /// The metadata block.
    pub fn metadata(&self) -> Option<deepmsg_core::buffer::AtomicBuffer<'_>> {
        self.file
            .region(self.geometry.metadata_offset, descriptor::METADATA_LENGTH)
    }

    /// Whether the driver says a subscriber is attached
    /// (`LogBufferDescriptor.isConnected`, the metadata byte
    /// `aeron_logbuffer_descriptor_t` carries at
    /// [`descriptor::IS_CONNECTED_OFFSET`]).
    ///
    /// Read here rather than through an append view because the byte is the
    /// driver's and **both** kinds of publication have it: an exclusive one has
    /// no append view and a connected byte all the same.
    ///
    /// [`None`] when the metadata cannot be read, which a log this type opened
    /// never is.
    pub fn is_connected(&self) -> Option<bool> {
        self.metadata()
            .and_then(|meta| meta.load_i32(descriptor::IS_CONNECTED_OFFSET))
            .map(|value| value != 0)
    }

    /// One of the three terms.
    pub fn term(&self, partition: usize) -> Option<deepmsg_core::buffer::AtomicBuffer<'_>> {
        if partition >= descriptor::PARTITION_COUNT {
            return None;
        }

        self.file.region(
            partition * self.geometry.term_length as usize,
            self.geometry.term_length as usize,
        )
    }

    /// The same, with write access, on a writable mapping.
    pub fn term_mut(
        &self,
        partition: usize,
    ) -> Option<deepmsg_core::buffer::AtomicBuffer<'_, deepmsg_core::buffer::ReadWrite>> {
        if partition >= descriptor::PARTITION_COUNT {
            return None;
        }

        self.file.region_mut(
            partition * self.geometry.term_length as usize,
            self.geometry.term_length as usize,
        )
    }
}

impl std::fmt::Debug for LogBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogBuffer")
            .field("geometry", &self.geometry)
            .field("file_length", &self.file.len())
            .finish()
    }
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.to_owned())
}
