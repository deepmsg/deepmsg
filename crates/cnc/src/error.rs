//! What can go wrong while reading a CnC file.
//!
//! Every variant is a statement about the *file*, never about a race: a
//! mismatch means the bytes are not a CnC file this build understands, which
//! is a different thing from a driver that has not finished starting. That
//! second case is not an error at all — it is
//! [`deepmsg_core::version::CncVersionCompatibility::NotReady`], a retryable
//! state.

/// Which region of the file a problem concerns.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Region {
    /// The 128-byte metadata block itself.
    Metadata,
    /// The MPSC command ring the client writes and the driver reads.
    ToDriver,
    /// The broadcast region the driver writes and clients read.
    ToClients,
    /// Counter metadata records.
    CountersMetadata,
    /// Counter value records.
    CountersValues,
    /// The distinct error log.
    ErrorLog,
}

impl Region {
    /// A stable name, for messages.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Metadata => "metadata",
            Self::ToDriver => "to-driver",
            Self::ToClients => "to-clients",
            Self::CountersMetadata => "counters-metadata",
            Self::CountersValues => "counters-values",
            Self::ErrorLog => "error-log",
        }
    }
}

impl std::fmt::Display for Region {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

/// Why a CnC file could not be read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CncError {
    /// The file is not longer than the metadata region. The reference uses a
    /// strict `>`: a file of exactly 128 bytes has no room for any region.
    FileTooShort {
        /// The file's actual length.
        length: usize,
    },
    /// A region length is zero or negative.
    ///
    /// These fields are `int32_t` and a corrupted or stale block can carry a
    /// negative one. The C reader survives it by casting to `size_t`, which
    /// wraps to a huge value and makes the length check fail "by accident";
    /// here it is rejected explicitly, because in Rust the same cast would
    /// wrap and the sum would overflow.
    RegionLengthNotPositive {
        /// The region whose length is bad.
        region: Region,
        /// The offending value, as it appeared.
        length: i32,
    },
    /// The regions, laid end to end, do not fit inside the file.
    RegionsExceedFile {
        /// Bytes the metadata claims are needed.
        required: usize,
        /// Bytes actually present.
        file_length: usize,
    },
    /// A region starts on an address that its accessors cannot use. Every
    /// field the reader touches is at least 4 bytes wide and the widest is 8,
    /// so each region base must be 8-byte aligned.
    RegionUnaligned {
        /// The region that starts off-grid.
        region: Region,
        /// Where it starts.
        offset: usize,
    },
    /// The two counter regions disagree about their size.
    ///
    /// The reference fixes `metadata = 4 * values`
    /// (`aeron-client/src/main/c/concurrent/aeron_counters_manager.h:25-29`)
    /// and validates it with a `>=` comparison
    /// (`aeron_counters_manager.h:100-101`); a reader that recomputed the
    /// ratio instead of reading it would silently break if it ever changed,
    /// so the check is kept here as a consistency assertion rather than as
    /// arithmetic.
    CountersBuffersInconsistent {
        /// `counter_metadata_buffer_length`, as read.
        metadata_length: i32,
        /// `counter_values_buffer_length`, as read.
        values_length: i32,
    },
}

impl std::fmt::Display for CncError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FileTooShort { length } => {
                write!(f, "CnC file is {length} bytes, too short to hold metadata")
            }
            Self::RegionLengthNotPositive { region, length } => {
                write!(f, "{region} region length is {length}, must be positive")
            }
            Self::RegionsExceedFile {
                required,
                file_length,
            } => write!(
                f,
                "regions need {required} bytes but the file is {file_length}"
            ),
            Self::RegionUnaligned { region, offset } => {
                write!(f, "{region} region starts at {offset}, not 8-byte aligned")
            }
            Self::CountersBuffersInconsistent {
                metadata_length,
                values_length,
            } => write!(
                f,
                "counters metadata is {metadata_length} bytes for {values_length} of values, \
                 expected at least four times as many"
            ),
        }
    }
}

impl std::error::Error for CncError {}
