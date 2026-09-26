//! The log buffer metadata block: every field's offset, and the sizes the
//! arithmetic elsewhere depends on.
//!
//! # Why there is no struct here
//!
//! The reference declares this block as a `#pragma pack(4)` struct whose
//! `sizeof` is **508**, and pins every field with an `offsetof` static assert
//! (`aeron-client/src/main/c/concurrent/aeron_logbuffer_descriptor.c:22-179`).
//! A Rust `#[repr(C)]` struct would not reproduce it: `untethered_linger_timeout_ns`
//! sits at offset **500**, which is only 4-byte aligned, and `repr(C)` would
//! insert four bytes of padding and place it at 504 — changing every offset
//! after it and making `size_of` 512.
//!
//! So the block is addressed by explicit offsets over a byte view, which is
//! what [`crate::buffer`] already provides. The offsets below are the
//! authoritative table, and the assertions at the bottom of this file are the
//! ones the reference pins.
//!
//! # Two lengths, not one
//!
//! [`METADATA_LENGTH`] is **4096**; [`METADATA_STRUCT_LENGTH`] is **508**. Both
//! are pinned in the reference (`:90` and `:177-179`). Anything that sizes a
//! buffer from the second is wrong — the block is a full page with the rest
//! reserved and assumed zero.

// ---------------------------------------------------------------------------
// Sizes and ranges — `aeron_logbuffer_descriptor.h:27-38`.
// ---------------------------------------------------------------------------

/// How many terms a log buffer has.
pub const PARTITION_COUNT: usize = 3;

/// The metadata block's length: one page, not the struct's size.
pub const METADATA_LENGTH: usize = PAGE_MIN_SIZE;

/// `sizeof` the reference's metadata struct.
///
/// Exported because the *pair* with [`METADATA_LENGTH`] is the trap: a reader
/// who uses this one to size a buffer gets a block that does not line up with
/// the term area.
pub const METADATA_STRUCT_LENGTH: usize = 508;

/// The smallest page size, and the metadata block's length.
pub const PAGE_MIN_SIZE: usize = 4 * 1024;

/// The largest page size.
pub const PAGE_MAX_SIZE: usize = 1024 * 1024 * 1024;

/// The smallest term length.
pub const TERM_MIN_LENGTH: i32 = 64 * 1024;

/// The largest term length. Also the bound that keeps `term_id` inside an
/// `i32` once positions are shifted (see [`Position::MAX`]-style limits).
pub const TERM_MAX_LENGTH: i32 = 1024 * 1024 * 1024;

/// Frame alignment. Frames are 32-byte aligned, not 8 and not the cache line.
pub const FRAME_ALIGNMENT: i32 = 32;

/// The cap on a single message, independent of the term length.
pub const MAX_MESSAGE_LENGTH: i32 = 16 * 1024 * 1024;

/// The default MTU, and the default for the IPC channel too.
pub const MTU_LENGTH_DEFAULT: i32 = 1408;

/// `AERON_LOGBUFFER_PADDING_SIZE` — the cache line, used only to derive the
/// pads below.
pub const PADDING_SIZE: usize = 64;

// ---------------------------------------------------------------------------
// Field offsets — `aeron_logbuffer_descriptor.c:22-179`, cross-checked against
// `LogBufferDescriptor.java:395-440`. Every one agrees between C and Java.
// ---------------------------------------------------------------------------

/// `term_tail_counters[3]`: one packed raw tail per partition, stride 8.
///
/// There is no block-level tail. This array plus [`ACTIVE_TERM_COUNT_OFFSET`] is
/// the whole of the tail state, and a *consumer* reads neither.
pub const TERM_TAIL_COUNTERS_OFFSET: usize = 0;

/// Stride between the entries of [`TERM_TAIL_COUNTERS_OFFSET`].
pub const TERM_TAIL_COUNTER_STRIDE: usize = 8;

/// `active_term_count`: the producer's term count, and the only thing that says
/// which partition is current. The index is derived, not stored.
pub const ACTIVE_TERM_COUNT_OFFSET: usize = 24;

/// `end_of_stream_position`. [`END_OF_STREAM_OPEN`] means "still open".
pub const END_OF_STREAM_POSITION_OFFSET: usize = 128;

/// `is_connected`: 1 when the driver considers this log connected.
pub const IS_CONNECTED_OFFSET: usize = 136;

/// `active_transport_count`.
pub const ACTIVE_TRANSPORT_COUNT_OFFSET: usize = 140;

/// `correlation_id` — the registration id of the command that created this log.
/// A **counter id**, not a position.
pub const CORRELATION_ID_OFFSET: usize = 256;

/// `initial_term_id`. Randomised at creation so a stream is not accidentally
/// reused, which is exactly why a raw tail and a position are not the same
/// number.
pub const INITIAL_TERM_ID_OFFSET: usize = 264;

/// `default_frame_header_length`: how many bytes of [`DEFAULT_FRAME_HEADER_OFFSET`]
/// are valid. 32 in practice.
///
/// The C implementation honours this field when copying; the Java one hardcodes
/// 32. Honouring it is the more faithful reading of the C struct.
pub const DEFAULT_FRAME_HEADER_LENGTH_OFFSET: usize = 268;

/// `mtu_length`.
pub const MTU_LENGTH_OFFSET: usize = 272;

/// `term_length`.
pub const TERM_LENGTH_OFFSET: usize = 276;

/// `page_size`.
pub const PAGE_SIZE_OFFSET: usize = 280;

/// `publication_window_length`.
pub const PUBLICATION_WINDOW_LENGTH_OFFSET: usize = 284;

/// `receiver_window_length` — zero for a publication.
pub const RECEIVER_WINDOW_LENGTH_OFFSET: usize = 288;

/// `socket_sndbuf_length`.
pub const SOCKET_SNDBUF_LENGTH_OFFSET: usize = 292;

/// `os_default_socket_sndbuf_length` — an observation of the OS, not a setting.
pub const OS_DEFAULT_SOCKET_SNDBUF_LENGTH_OFFSET: usize = 296;

/// `os_max_socket_sndbuf_length` — as above.
pub const OS_MAX_SOCKET_SNDBUF_LENGTH_OFFSET: usize = 300;

/// `socket_rcvbuf_length`.
pub const SOCKET_RCVBUF_LENGTH_OFFSET: usize = 304;

/// `os_default_socket_rcvbuf_length`.
pub const OS_DEFAULT_SOCKET_RCVBUF_LENGTH_OFFSET: usize = 308;

/// `os_max_socket_rcvbuf_length`.
pub const OS_MAX_SOCKET_RCVBUF_LENGTH_OFFSET: usize = 312;

/// `max_resend`.
pub const MAX_RESEND_OFFSET: usize = 316;

/// `default_header`: a 128-byte template frame header the receiver copies when
/// it needs to write a frame without a source.
pub const DEFAULT_FRAME_HEADER_OFFSET: usize = 320;

/// The length of [`DEFAULT_FRAME_HEADER_OFFSET`], two cache lines.
pub const DEFAULT_FRAME_HEADER_MAX_LENGTH: usize = 128;

/// `entity_tag`.
pub const ENTITY_TAG_OFFSET: usize = 448;

/// `response_correlation_id` — another counter id.
pub const RESPONSE_CORRELATION_ID_OFFSET: usize = 456;

/// `linger_timeout_ns`.
pub const LINGER_TIMEOUT_NS_OFFSET: usize = 464;

/// `untethered_window_limit_timeout_ns`.
pub const UNTETHERED_WINDOW_LIMIT_TIMEOUT_NS_OFFSET: usize = 472;

/// `untethered_resting_timeout_ns`.
pub const UNTETHERED_RESTING_TIMEOUT_NS_OFFSET: usize = 480;

/// `group` — a multicast group tag.
pub const GROUP_OFFSET: usize = 488;

/// `is_response`.
pub const IS_RESPONSE_OFFSET: usize = 489;

/// `rejoin`.
pub const REJOIN_OFFSET: usize = 490;

/// `reliable`.
pub const RELIABLE_OFFSET: usize = 491;

/// `sparse`.
pub const SPARSE_OFFSET: usize = 492;

/// `signal_eos`.
pub const SIGNAL_EOS_OFFSET: usize = 493;

/// `spies_simulate_connection`.
pub const SPIES_SIMULATE_CONNECTION_OFFSET: usize = 494;

/// `tether`.
pub const TETHER_OFFSET: usize = 495;

/// `is_publication_revoked` — volatile in C, plain in Java. Treat it as
/// volatile: the driver writes it at runtime to revoke a live publication.
pub const IS_PUBLICATION_REVOKED_OFFSET: usize = 496;

/// `type`: concurrent publication, exclusive publication, or image.
pub const TYPE_OFFSET: usize = 497;

/// Two reserved bytes between the flags block and the untethered linger
/// timeout. Java calls them "space available"; leave them zero.
pub const PAD3_OFFSET: usize = 498;

/// `untethered_linger_timeout_ns`.
///
/// **Offset 500, which is only 4-byte aligned.** This field is the reason the
/// module doc says there is no struct here.
pub const UNTETHERED_LINGER_TIMEOUT_NS_OFFSET: usize = 500;

/// A log's role, from [`TYPE_OFFSET`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogType {
    /// A concurrent publication: many producers, tails claimed by CAS or
    /// fetch-and-add.
    ConcurrentPublication,
    /// An exclusive publication: one producer, which stores the tail directly.
    ExclusivePublication,
    /// An image: the receive side.
    PublicationImage,
    /// A value the reference does not define.
    Unknown(u8),
}

impl LogType {
    /// The byte at [`TYPE_OFFSET`].
    pub const fn from_byte(value: u8) -> Self {
        match value {
            0 => Self::ConcurrentPublication,
            1 => Self::ExclusivePublication,
            2 => Self::PublicationImage,
            other => Self::Unknown(other),
        }
    }
}

/// `end_of_stream_position` when the stream is still open.
///
/// The same value as `i64::MAX`, but the *name* is the point: a reader that
/// does arithmetic on it without checking produces nonsense.
pub const END_OF_STREAM_OPEN: i64 = i64::MAX;

// ---------------------------------------------------------------------------
// The assertions the reference pins. A drift here is a layout change, and it
// should fail the build rather than a test on someone else's machine.
// ---------------------------------------------------------------------------

const _: () =
    assert!(TERM_TAIL_COUNTERS_OFFSET + (PARTITION_COUNT * TERM_TAIL_COUNTER_STRIDE) == 24);
const _: () = assert!(ACTIVE_TERM_COUNT_OFFSET + 4 <= 128);
const _: () = assert!(END_OF_STREAM_POSITION_OFFSET == 2 * PADDING_SIZE);
const _: () = assert!(CORRELATION_ID_OFFSET == 4 * PADDING_SIZE);
const _: () = assert!(DEFAULT_FRAME_HEADER_OFFSET == 5 * PADDING_SIZE);
const _: () = assert!(DEFAULT_FRAME_HEADER_OFFSET + DEFAULT_FRAME_HEADER_MAX_LENGTH == 448);
const _: () =
    assert!(ENTITY_TAG_OFFSET == DEFAULT_FRAME_HEADER_OFFSET + DEFAULT_FRAME_HEADER_MAX_LENGTH);
const _: () = assert!(TYPE_OFFSET + 1 == PAD3_OFFSET);
const _: () = assert!(UNTETHERED_LINGER_TIMEOUT_NS_OFFSET == METADATA_STRUCT_LENGTH - 8);
const _: () = assert!(METADATA_STRUCT_LENGTH < METADATA_LENGTH);
const _: () = assert!(METADATA_LENGTH == PAGE_MIN_SIZE);
const _: () = assert!(DEFAULT_FRAME_HEADER_MAX_LENGTH == 2 * PADDING_SIZE);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_two_lengths_are_distinct_and_both_correct() {
        // The trap this module exists to make visible.
        assert_eq!(4096, METADATA_LENGTH);
        assert_eq!(508, METADATA_STRUCT_LENGTH);
        assert_ne!(METADATA_LENGTH, METADATA_STRUCT_LENGTH);
    }

    #[test]
    fn the_fields_the_reference_places_deliberately_are_where_it_puts_them() {
        // Each of these is an `offsetof` static assert in the reference; a
        // failure here means the layout moved, not that this test is strict.
        for (name, actual, expected) in [
            ("term_tail_counters", TERM_TAIL_COUNTERS_OFFSET, 0),
            ("active_term_count", ACTIVE_TERM_COUNT_OFFSET, 24),
            ("end_of_stream_position", END_OF_STREAM_POSITION_OFFSET, 128),
            ("is_connected", IS_CONNECTED_OFFSET, 136),
            ("active_transport_count", ACTIVE_TRANSPORT_COUNT_OFFSET, 140),
            ("correlation_id", CORRELATION_ID_OFFSET, 256),
            ("initial_term_id", INITIAL_TERM_ID_OFFSET, 264),
            (
                "default_frame_header_length",
                DEFAULT_FRAME_HEADER_LENGTH_OFFSET,
                268,
            ),
            ("mtu_length", MTU_LENGTH_OFFSET, 272),
            ("term_length", TERM_LENGTH_OFFSET, 276),
            ("page_size", PAGE_SIZE_OFFSET, 280),
            ("default_header", DEFAULT_FRAME_HEADER_OFFSET, 320),
            ("entity_tag", ENTITY_TAG_OFFSET, 448),
            (
                "response_correlation_id",
                RESPONSE_CORRELATION_ID_OFFSET,
                456,
            ),
            ("linger_timeout_ns", LINGER_TIMEOUT_NS_OFFSET, 464),
            ("group", GROUP_OFFSET, 488),
            ("tether", TETHER_OFFSET, 495),
            ("is_publication_revoked", IS_PUBLICATION_REVOKED_OFFSET, 496),
            ("type", TYPE_OFFSET, 497),
            (
                "untethered_linger_timeout_ns",
                UNTETHERED_LINGER_TIMEOUT_NS_OFFSET,
                500,
            ),
        ] {
            assert_eq!(expected, actual, "{name} moved");
        }
    }

    #[test]
    fn the_unaligned_field_is_unaligned() {
        // If this ever becomes 8-byte aligned the module doc's argument goes
        // away — and so does the reason not to use a struct. Worth knowing.
        assert_eq!(
            4,
            UNTETHERED_LINGER_TIMEOUT_NS_OFFSET % 8,
            "this field is only 4-byte aligned, which a Rust repr(C) struct \
             would silently change"
        );
    }

    #[test]
    fn log_types_round_trip() {
        assert_eq!(LogType::ConcurrentPublication, LogType::from_byte(0));
        assert_eq!(LogType::ExclusivePublication, LogType::from_byte(1));
        assert_eq!(LogType::PublicationImage, LogType::from_byte(2));
        assert_eq!(LogType::Unknown(9), LogType::from_byte(9));
    }
}
