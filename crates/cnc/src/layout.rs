//! Every offset and length in the CnC file, as constants.
//!
//! The shared-memory layout has no specification — the bytes *are* the
//! contract — so each constant here carries the reference line it was read
//! from, and the relationships between them are asserted at compile time
//! rather than described. A `const _: () = assert!(...)` is the cheapest
//! possible enforcement of "every byte-level claim cites a reference source":
//! the claim becomes executable and a drift becomes a build failure.
//!
//! All values are from Aeron 1.53.2 (commit `664f58e705`).

/// Cache line size, and a **byte contract rather than an optimisation
/// target**.
///
/// `aeron-client/src/main/c/util/aeron_bitutil.h:33` hard-codes `64u`; it is
/// never detected at runtime, because the value decides where fields land in
/// the file. Detecting it is exactly the defect that shipped in `aeron-rs` on
/// Apple Silicon, where a 128-byte detection silently computed a wrong layout.
pub const CACHE_LINE_LENGTH: usize = 64;

/// Size of the metadata *region* at the head of the file: two cache lines.
///
/// `aeron-client/src/main/c/aeron_cnc_file_descriptor.h:57`
/// (`AERON_CNC_VERSION_AND_META_DATA_LENGTH`), mirrored by
/// `aeron-client/src/main/java/io/aeron/CncFileDescriptor.java:162`
/// (`META_DATA_LENGTH`).
///
/// This is **not** `sizeof(aeron_cnc_metadata_t)`, which is 52. The struct is
/// 52 bytes inside a 128-byte region and the regions after it begin at 128, so
/// copying 52 bytes out of the file and calling it the header is a mistake
/// that produces plausible numbers.
pub const VERSION_AND_METADATA_LENGTH: usize = 2 * CACHE_LINE_LENGTH;

const _: () = assert!(VERSION_AND_METADATA_LENGTH == 128);

// ---------------------------------------------------------------------------
// Metadata block — `aeron_cnc_file_descriptor.h:28-43`, offsets per
// `CncFileDescriptor.java:141-153`. `#pragma pack(4)`, so the int64 fields sit
// at 24/32/40 even unpacked.
// ---------------------------------------------------------------------------

/// Offset of `cnc_version` (`int32`, volatile). Zero means "not published
/// yet": the driver fills every other field first and then stores this one
/// with a release (`aeron_driver_context.h:456`, called from
/// `aeron_driver.c:972` before any agent thread starts).
pub const CNC_VERSION_OFFSET: usize = 0;

/// Offset of `to_driver_buffer_length` (`int32`) — the MPSC command ring.
pub const TO_DRIVER_BUFFER_LENGTH_OFFSET: usize = 4;

/// Offset of `to_clients_buffer_length` (`int32`) — the broadcast region.
pub const TO_CLIENTS_BUFFER_LENGTH_OFFSET: usize = 8;

/// Offset of `counter_metadata_buffer_length` (`int32`).
pub const COUNTER_METADATA_BUFFER_LENGTH_OFFSET: usize = 12;

/// Offset of `counter_values_buffer_length` (`int32`).
pub const COUNTER_VALUES_BUFFER_LENGTH_OFFSET: usize = 16;

/// Offset of `error_log_buffer_length` (`int32`).
pub const ERROR_LOG_BUFFER_LENGTH_OFFSET: usize = 20;

/// Offset of `client_liveness_timeout` (`int64`), in **nanoseconds**.
pub const CLIENT_LIVENESS_TIMEOUT_OFFSET: usize = 24;

/// Offset of `start_timestamp` (`int64`), epoch **milliseconds**.
pub const START_TIMESTAMP_OFFSET: usize = 32;

/// Offset of `pid` (`int64`) — the driver process id.
pub const PID_OFFSET: usize = 40;

/// Offset of `file_page_size` (`int32`). May be zero on a writer older than
/// 1.48; a reader needs no fallback for it, because region offsets are plain
/// arithmetic and the total is validated against the real file length.
pub const FILE_PAGE_SIZE_OFFSET: usize = 48;

/// `sizeof(aeron_cnc_metadata_t)` — the last field ends here, inside the
/// 128-byte region.
pub const METADATA_STRUCT_LENGTH: usize = 52;

const _: () = assert!(FILE_PAGE_SIZE_OFFSET + 4 == METADATA_STRUCT_LENGTH);
const _: () = assert!(METADATA_STRUCT_LENGTH < VERSION_AND_METADATA_LENGTH);

// ---------------------------------------------------------------------------
// Ring trailers. Both rings put their descriptor at the *end* of their region,
// after the data area, so `capacity = region_length - TRAILER_LENGTH`.
// ---------------------------------------------------------------------------

/// Length of the to-driver MPSC ring trailer:
/// `aeron-client/src/main/c/concurrent/aeron_rb.h:50`
/// (`AERON_RB_TRAILER_LENGTH = sizeof(aeron_rb_descriptor_t)`).
pub const MPSC_RB_TRAILER_LENGTH: usize = 768;

/// Offset of `consumer_heartbeat` within the MPSC trailer — the field the
/// reference reads to decide whether a driver is alive
/// (`aeron-client/src/main/c/aeron_cnc.c:131-141`).
///
/// Each of the trailer's five `int64` fields is padded out to its own pair of
/// cache lines, so they sit at 128, 256, 384, 512 and 640.
pub const MPSC_CONSUMER_HEARTBEAT_OFFSET: usize = 640;

/// Offset of `tail_position` within the MPSC trailer: the byte counter a
/// **producer** advances by compare-and-exchange. Unbounded — it is masked into
/// an index at each use, never stored masked.
pub const MPSC_TAIL_POSITION_OFFSET: usize = 128;

/// Offset of `head_cache_position`: a cache of `head_position` that producers
/// publish to each other, and the field a producer writes besides the tail.
///
/// A stale value here is harmless by construction: `head_position` only ever
/// grows, so a producer can publish an over-conservative head, and the next one
/// that finds space tight re-reads the authoritative field.
pub const MPSC_HEAD_CACHE_POSITION_OFFSET: usize = 256;

/// Offset of `head_position`: the **consumer's** field, advanced only after it
/// has zeroed the bytes it consumed. A producer reads it and never writes it.
pub const MPSC_HEAD_POSITION_OFFSET: usize = 384;

/// Offset of `correlation_counter`: shared by every producer of the ring,
/// including producers in other processes, and advanced by fetch-and-add.
pub const MPSC_CORRELATION_COUNTER_OFFSET: usize = 512;

/// The smallest legal ring capacity, and the value at which
/// `max_message_length` collapses to zero:
/// `AERON_MPSC_RB_MIN_CAPACITY = AERON_RB_RECORD_HEADER_LENGTH`
/// (`aeron-client/src/main/c/concurrent/aeron_mpsc_rb.h:22`).
pub const MPSC_MIN_CAPACITY: usize = RECORD_HEADER_LENGTH;

/// `length` within a ring record header (`int32`, volatile).
pub const RECORD_LENGTH_OFFSET: usize = 0;

/// `msg_type_id` within a ring record header (`int32`).
pub const RECORD_MSG_TYPE_ID_OFFSET: usize = 4;

/// The record header's alignment and the stride records advance by:
/// `AERON_RB_ALIGNMENT` (`aeron-client/src/main/c/concurrent/aeron_rb.h:52`).
///
/// Note this is **not** the header struct's alignment, which `#pragma pack(4)`
/// makes 4 — it is the granularity the writer rounds a record length up to.
pub const RECORD_ALIGNMENT: usize = 8;

// Twelve cache lines: a two-line leading pad, then five fields each padded
// out to a two-line block (5 * 2 + 2). Writing it as `6 * CACHE_LINE_LENGTH`
// is the arithmetic error this assertion exists to catch.
const _: () = assert!(MPSC_RB_TRAILER_LENGTH == 12 * CACHE_LINE_LENGTH);
const _: () = assert!(MPSC_CONSUMER_HEARTBEAT_OFFSET == 10 * CACHE_LINE_LENGTH);
const _: () = assert!(MPSC_CONSUMER_HEARTBEAT_OFFSET + 8 <= MPSC_RB_TRAILER_LENGTH);

/// Length of the to-clients broadcast trailer:
/// `aeron-client/src/main/c/concurrent/aeron_broadcast_descriptor.h:49`.
///
/// Unlike the MPSC trailer there is no leading pad: the three counters are
/// back to back with no cache-line separation.
pub const BROADCAST_TRAILER_LENGTH: usize = 128;

/// Offset of `tail_intent_counter` within the broadcast trailer. Written
/// *before* the record body, so it orders nothing — it exists for lap
/// detection only.
pub const BROADCAST_TAIL_INTENT_COUNTER_OFFSET: usize = 0;

/// Offset of `tail_counter`: published after the record body, and the signal
/// that a record is complete.
pub const BROADCAST_TAIL_COUNTER_OFFSET: usize = 8;

/// Offset of `latest_counter`: the start position of the most recently written
/// record, i.e. one record behind `tail_counter`. A fresh reader starts here
/// (`aeron_broadcast_receiver.c:47-52`); starting at `tail_counter` would
/// replay the whole ring.
pub const BROADCAST_LATEST_COUNTER_OFFSET: usize = 16;

const _: () = assert!(BROADCAST_TRAILER_LENGTH == 2 * CACHE_LINE_LENGTH);
const _: () = assert!(BROADCAST_LATEST_COUNTER_OFFSET + 8 <= BROADCAST_TRAILER_LENGTH);

/// Header of a ring record, in either ring:
/// `aeron-client/src/main/c/concurrent/aeron_rb.h:42-48`.
///
/// `sizeof` is 8 and the struct is `#pragma pack(4)`, so its *alignment* is 4;
/// what makes record starts 8-aligned is `AERON_RB_ALIGNMENT`
/// (`aeron_rb.h:52`), which is how the writer advances the index. Saying
/// "alignment 8" about the struct would contradict the header.
pub const RECORD_HEADER_LENGTH: usize = 8;

/// `msg_type_id` value marking a padding record written to fill a ring wrap.
pub const PADDING_MSG_TYPE_ID: i32 = -1;

// ---------------------------------------------------------------------------
// Counters — `aeron-client/src/main/c/aeronc.h:849-870`.
// ---------------------------------------------------------------------------

/// Stride of the counters **metadata** region, and the stride of enumeration.
pub const COUNTER_METADATA_LENGTH: usize = 512;

/// Stride of the counters **values** region.
pub const COUNTER_VALUE_LENGTH: usize = 128;

/// `counter_value` within a value record (`int64`, volatile).
pub const COUNTER_VALUE_OFFSET: usize = 0;

/// `registration_id` within a value record (`int64`, volatile).
pub const COUNTER_REGISTRATION_ID_OFFSET: usize = 8;

/// `owner_id` within a value record (`int64`).
pub const COUNTER_OWNER_ID_OFFSET: usize = 16;

/// `reference_id` within a value record (`int64`).
pub const COUNTER_REFERENCE_ID_OFFSET: usize = 24;

/// Value written into the to-driver ring's consumer heartbeat by a driver
/// shutting down cleanly (`aeron-driver/src/main/c/aeron_driver_conductor.c:3493`,
/// `AERON_NULL_VALUE` at `aeron-client/src/main/c/aeronc.h:30`). A heartbeat
/// of `-1` therefore means "was alive, stopped on purpose", which is a
/// different thing from "never started" (zero) or "died" (a stale timestamp).
pub const NULL_VALUE: i64 = -1;

/// `state` within a metadata record (`int32`, volatile).
pub const COUNTER_STATE_OFFSET: usize = 0;

/// `type_id` within a metadata record (`int32`).
pub const COUNTER_TYPE_ID_OFFSET: usize = 4;

/// `key` within a metadata record; always read at its full length, never
/// trimmed (`aeron-client/src/main/c/concurrent/aeron_counters_manager.c:310`).
pub const COUNTER_KEY_OFFSET: usize = 16;

/// Length of the `key` field.
pub const COUNTER_KEY_LENGTH: usize = 112;

/// `label_length` within a metadata record (`int32`, volatile).
pub const COUNTER_LABEL_LENGTH_OFFSET: usize = 128;

/// `label` within a metadata record; free-form text, **not** NUL-terminated.
pub const COUNTER_LABEL_OFFSET: usize = 132;

/// Maximum length of the `label` field.
pub const COUNTER_LABEL_LENGTH_MAX: usize = 380;

/// Record state meaning "never allocated, and nothing after it is either".
pub const COUNTER_STATE_UNUSED: i32 = 0;
/// Record state meaning "live and readable".
pub const COUNTER_STATE_ALLOCATED: i32 = 1;
/// Record state meaning "returned to the pool"; its `key` is zeroed
/// non-atomically, so the key of a reclaimed record must not be read.
pub const COUNTER_STATE_RECLAIMED: i32 = -1;

const _: () = assert!(COUNTER_KEY_OFFSET + COUNTER_KEY_LENGTH == COUNTER_LABEL_LENGTH_OFFSET);
const _: () = assert!(COUNTER_LABEL_OFFSET + COUNTER_LABEL_LENGTH_MAX == COUNTER_METADATA_LENGTH);
const _: () = assert!(COUNTER_METADATA_LENGTH == 4 * COUNTER_VALUE_LENGTH);

// ---------------------------------------------------------------------------
// Error log — `aeron-client/src/main/c/concurrent/aeron_distinct_error_log.h:27-40`.
// ---------------------------------------------------------------------------

/// Header length of one error-log entry.
pub const ERROR_LOG_HEADER_LENGTH: usize = 24;

/// `length` within an entry (`int32`, volatile): the **total** entry length,
/// header included. Zero means end of log.
pub const ERROR_LOG_LENGTH_OFFSET: usize = 0;

/// `observation_count` within an entry (`int32`, volatile).
pub const ERROR_LOG_OBSERVATION_COUNT_OFFSET: usize = 4;

/// `last_observation_timestamp` within an entry (`int64`, volatile).
pub const ERROR_LOG_LAST_TIMESTAMP_OFFSET: usize = 8;

/// `first_observation_timestamp` within an entry (`int64`).
pub const ERROR_LOG_FIRST_TIMESTAMP_OFFSET: usize = 16;

/// Entries advance on this boundary
/// (`aeron_distinct_error_log.h:42`, `AERON_ERROR_LOG_RECORD_ALIGNMENT`).
pub const ERROR_LOG_RECORD_ALIGNMENT: usize = 8;

const _: () = assert!(ERROR_LOG_LAST_TIMESTAMP_OFFSET + 8 == ERROR_LOG_FIRST_TIMESTAMP_OFFSET);
const _: () = assert!(ERROR_LOG_FIRST_TIMESTAMP_OFFSET + 8 == ERROR_LOG_HEADER_LENGTH);

/// Round `value` up to the next multiple of `alignment`, which must be a power
/// of two. Mirrors `AERON_ALIGN` (`aeron-client/src/main/c/util/aeron_bitutil.h:35`).
pub const fn align_up(value: usize, alignment: usize) -> usize {
    (value + (alignment - 1)) & !(alignment - 1)
}

const _: () = assert!(align_up(0, 8) == 0);
const _: () = assert!(align_up(1, 8) == 8);
const _: () = assert!(align_up(8, 8) == 8);
const _: () = assert!(align_up(9, 8) == 16);
