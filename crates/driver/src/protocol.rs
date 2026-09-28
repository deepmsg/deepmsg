//! The UDP wire protocol: the frame headers a driver and a client put on a
//! datagram.
//!
//! Mirrors `aeron-client/src/main/c/protocol/aeron_udp_protocol.h`, and the
//! layout written here is **Aeron 1.53.2's** — the version this driver is
//! byte-compatible with. A later reference that moves a field moves the wire,
//! and this module moves with it.
//!
//! Nothing here is SBE. The reference declares `#pragma pack(4)` structs
//! (`aeron_udp_protocol.h:25-26`) and writes their members straight into a
//! buffer that `sendmsg` transmits, so on this wire the protocol *is* the
//! memory layout of those structs: there is no schema to generate code from and
//! no field ordering left to discover. SBE starts where the cluster and archive
//! protocols start; the UDP data plane is structs all the way down.
//!
//! # The frames, and why they are the size they are
//!
//! | frame | type | fixed bytes | reference |
//! |---|---|---|---|
//! | header | — | 8 | `aeron_udp_protocol.h:27-34` |
//! | DATA | `0x01` | 32 | `:56-65` |
//! | NAK | `0x02` | 28 | `:67-76` |
//! | SM | `0x03` | 36, or 44 with a group tag | `:78-88`, `:90-94` |
//! | ERR | `0x04` | 40, then the text | `:96-106` |
//! | SETUP | `0x05` | 40 | `:42-54` |
//! | RTTM | `0x06` | 40 | `:108-117` |
//! | RSP_SETUP | `0x0B` | 20 | `:158-165` |
//!
//! The sizes follow from the fields and the packing, and were confirmed against
//! a C compiler with the same `#pragma pack(4)` rather than by adding up in
//! this comment. That they are round numbers is a consequence — each struct
//! packs into 8-byte groups — and not a rule to design against.
//!
//! Two of them are easy to get wrong from a casual reading, and both are wrong
//! in a direction that only shows up on the wire:
//!
//! **ERR is 40 bytes, not 32.** The group tag is a field of the *fixed* header
//! (`:102`) and is written unconditionally
//! (`aeron_receive_channel_endpoint.c:489`); the `AERON_ERROR_HAS_GROUP_TAG_FLAG`
//! bit in `flags` (`:227`) only says whether the receiver should *believe* the
//! value it finds there (`aeron_network_publication.c:880`). The message text
//! therefore begins at offset 40, not 32
//! (`aeron_receive_channel_endpoint.c:491`, `aeron_network_publication.c:867`).
//!
//! **RTTM is 40 bytes, and `reception_delta` is an `int64_t`.** It is a
//! nanosecond interval — `now - echo_timestamp - reception_delta`
//! (`aeron_publication_image.c:854`) — and travels as 64 bits on both sides
//! (`aeron_receive_channel_endpoint.c:400`, `:414`). Reading it as 32 bits
//! would misplace `receiver_id` and silently corrupt the round-trip estimate.
//!
//! # Little-endian, and only little-endian
//!
//! Every field is little-endian on the wire. The reference gets there by not
//! trying: it assigns struct members into a stack buffer and hands the buffer
//! to `sendmsg` (`aeron_receive_channel_endpoint.c:407-418`), so what travels
//! is whatever the host's byte order made of those members. Every platform
//! Aeron is built for is little-endian, which is what makes that work; a
//! big-endian host would need swapping on both sides, and the reference has the
//! same constraint — it contains no byte-swapping code at all. This module
//! fixes the order with `to_le_bytes`/`from_le_bytes`, so the bytes do not
//! depend on the host that produced them.
//!
//! # Two things that are not fixed-length
//!
//! An ERR frame carries text, and an SM frame may carry an 8-byte group tag.
//! Both ride *after* the fixed header, and in both cases the fixed header is
//! what says whether they are there: `frame_length` of 36 versus 44 for the SM
//! (`aeron_udp_protocol.c:33`), and `error_length + 40` for the ERR
//! (`aeron_send_channel_endpoint.c:666-667`). See [`ErrorFrame::text`] and
//! [`StatusMessageFrame::group_tag`].
//!
//! # What is not here
//!
//! RES, ATS and EXT payloads. `aeron_resolution_header_t` and its IPv4/IPv6
//! forms (`:124-149`) describe a *list* of variable-length entries with a
//! packed(1) layout, and the resolver that walks it is not written yet; only
//! their contribution to [`is_frame_valid`] — the 22-byte floor a RES packet
//! must clear (`:255`) — appears below.
//!
//! # No unsafe, no allocation
//!
//! Read and write are `from_le_bytes`/`to_le_bytes` at explicit offsets, so no
//! `#[repr(C)]` cast of a `&[u8]` and no `transmute` is needed, and nothing on
//! either path allocates: a reader returns a `Copy` struct or a borrow of the
//! buffer it was handed, and a writer only stores into the caller's slice.

/// `AERON_FRAME_ALIGNMENT` (`aeron_udp_protocol.h:188`): every frame in a term
/// buffer starts on a 32-byte boundary, and padding frames are what keep that
/// true when a message will not fit at the end of a term.
pub const FRAME_ALIGNMENT: usize = 32;

/// `AERON_FRAME_HEADER_LENGTH` (`aeron_udp_protocol.h:186`) — the size of
/// `aeron_frame_header_t` (`:27-34`): 4 + 1 + 1 + 2, with nothing for
/// `#pragma pack(4)` to squeeze, since the members already fit the alignment.
pub const HEADER_LENGTH: usize = 8;

/// `AERON_FRAME_HEADER_VERSION` (`aeron_udp_protocol.h:170`). The only version
/// the protocol has ever had, and the only one [`is_frame_valid`] accepts
/// (`:233`).
pub const VERSION: i8 = 0;

/// `AERON_FRAME_MAX_MESSAGE_LENGTH` (`aeron_udp_protocol.h:211`) — the ceiling
/// [`compute_max_message_length`] clamps a term-derived limit to. The reference
/// spells it unsigned because it also flows into `size_t` arithmetic; here it
/// is the `i32` both this constant and that function are used as.
pub const MAX_MESSAGE_LENGTH: i32 = 16 * 1024 * 1024;

/// `AERON_ERROR_MAX_TEXT_LENGTH` (`aeron_udp_protocol.h:225`) — the most text
/// an ERR frame may carry, which is exactly what an arriving `error_length` is
/// validated against (`aeron_send_channel_endpoint.c:666`).
pub const MAX_ERROR_TEXT_LENGTH: i32 = 1023;

/// The frame type in a header's last two bytes (`aeron_udp_protocol.h:172-184`).
///
/// The type is an `i16`, not a `u8`, and [`EXT`](frame_type::EXT) is where that
/// matters: the driver agent's extended frames are type `-1`, so a reader that
/// treats the field as unsigned sees `0xFFFF` and must not confuse it with a
/// type that happens to have the high bit set.
pub mod frame_type {
    /// `AERON_HDR_TYPE_PAD` (`:172`): a data-plane frame with no payload,
    /// written to fill out the end of a term.
    pub const PAD: i16 = 0x00;
    /// `AERON_HDR_TYPE_DATA` (`:173`): a message, or a fragment of one.
    pub const DATA: i16 = 0x01;
    /// `AERON_HDR_TYPE_NAK` (`:174`): a gap report.
    pub const NAK: i16 = 0x02;
    /// `AERON_HDR_TYPE_SM` (`:175`): a status message — the receiver's
    /// position, its window, and its liveness.
    pub const SM: i16 = 0x03;
    /// `AERON_HDR_TYPE_ERR` (`:176`): a receiver refusing a publication, or a
    /// sender telling its peer to go away.
    pub const ERR: i16 = 0x04;
    /// `AERON_HDR_TYPE_SETUP` (`:177`): the first frame of a session.
    pub const SETUP: i16 = 0x05;
    /// `AERON_HDR_TYPE_RTTM` (`:178`): a measurement, not a control frame.
    pub const RTTM: i16 = 0x06;
    /// `AERON_HDR_TYPE_RES` (`:179`): name-resolution gossip, whose payload is
    /// not modelled here.
    pub const RES: i16 = 0x07;
    /// `AERON_HDR_TYPE_ATS_DATA` (`:180`): the ATS-encrypted form of DATA.
    pub const ATS_DATA: i16 = 0x08;
    /// `AERON_HDR_TYPE_ATS_SETUP` (`:181`): the ATS-encrypted form of SETUP.
    pub const ATS_SETUP: i16 = 0x09;
    /// `AERON_HDR_TYPE_ATS_SM` (`:182`): the ATS-encrypted form of SM.
    pub const ATS_SM: i16 = 0x0A;
    /// `AERON_HDR_TYPE_RSP_SETUP` (`:183`): a receiver's answer to a SETUP
    /// that asked for a response.
    pub const RSP_SETUP: i16 = 0x0B;
    /// `AERON_HDR_TYPE_EXT` (`:184`): an extended frame, seen only by the
    /// driver agent's logging, never on the wire.
    pub const EXT: i16 = -1;
}

/// The flags byte, per frame type (`aeron_udp_protocol.h:190-205`, `:227`).
///
/// One byte is shared by seven different meanings, so the names carry their
/// frame wherever a bare root would be ambiguous. The DATA four are the
/// exception and keep the bare names, because that is where the byte is
/// actually *read* on the hot path and because
/// [`UNFRAGMENTED`](header_flags::UNFRAGMENTED) is that pair.
///
/// **The values collide across frame types, deliberately and unavoidably.**
/// DATA's `EOS` is `0x20` and SM's is `0x40`; SETUP's `SEND_RESPONSE` and SM's
/// `SEND_SETUP` are both `0x80`. A header's flags byte means nothing until its
/// type is known, which is why [`FrameHeader::is_data`] exists and why a
/// reader must branch on `frame_type` before looking at `flags`.
pub mod header_flags {
    /// `AERON_DATA_HEADER_BEGIN_FLAG` (`:190`): the first fragment of a
    /// message.
    pub const BEGIN: u8 = 0x80;
    /// `AERON_DATA_HEADER_END_FLAG` (`:191`): the last fragment of a message.
    pub const END: u8 = 0x40;
    /// `AERON_DATA_HEADER_EOS_FLAG` (`:192`): the publisher is closing.
    pub const EOS: u8 = 0x20;
    /// `AERON_DATA_HEADER_REVOKED_FLAG` (`:193`): a fragment the receiver
    /// should discard rather than assemble.
    pub const REVOKED: u8 = 0x10;
    /// `AERON_DATA_HEADER_UNFRAGMENTED` (`:195`) — BEGIN and END at once, the
    /// ordinary case for a message that fits in one frame.
    pub const UNFRAGMENTED: u8 = BEGIN | END;

    /// `AERON_STATUS_MESSAGE_HEADER_SEND_SETUP_FLAG` (`:199`): the receiver
    /// has not been set up yet and is answering with a setup request.
    pub const SM_SEND_SETUP: u8 = 0x80;
    /// `AERON_STATUS_MESSAGE_HEADER_EOS_FLAG` (`:200`): the receiver saw the
    /// publisher's end of stream.
    pub const SM_EOS: u8 = 0x40;

    /// `AERON_SETUP_HEADER_SEND_RESPONSE_FLAG` (`:202`): the publisher wants a
    /// response setup frame back.
    pub const SETUP_SEND_RESPONSE: u8 = 0x80;
    /// `AERON_SETUP_HEADER_GROUP_FLAG` (`:203`): the publication has group
    /// semantics, so its frames must be treated as a group.
    pub const SETUP_GROUP: u8 = 0x40;

    /// `AERON_RTTM_HEADER_REPLY_FLAG` (`:205`): this measurement is the answer
    /// to one that arrived, and must not be answered again
    /// (`aeron_network_publication.c:893`).
    pub const RTTM_REPLY: u8 = 0x80;

    /// `AERON_ERROR_HAS_GROUP_TAG_FLAG` (`:227`): the ERR frame's group tag
    /// field holds a value this endpoint's peer sent, rather than padding.
    pub const ERR_HAS_GROUP_TAG: u8 = 0x08;
}

/// [RES-frame](frame_type::RES) IPv4 resolution header, `sizeof` of
/// `aeron_resolution_header_ipv4_t` (`aeron_udp_protocol.h:135-141`) under its
/// own `#pragma pack(1)`: 1 + 1 + 2 + 4 + 4 + 2.
///
/// Private: the only thing this module owes RES is the floor
/// [`is_frame_valid`] checks (`:255`).
const RESOLUTION_HEADER_IPV4_LENGTH: usize = 14;

/// `sizeof(aeron_response_setup_header_t)` (`aeron_udp_protocol.h:158-165`),
/// used only as [`is_frame_valid`]'s floor for the type (`:258`).
const RESPONSE_SETUP_HEADER_LENGTH: usize = 20;

/// Read `buffer` as a slot holding 4 little-endian bytes.
fn read_i32(buffer: &[u8], offset: usize) -> Option<i32> {
    Some(i32::from_le_bytes(
        buffer.get(offset..offset + 4)?.try_into().ok()?,
    ))
}

/// Read `buffer` as a slot holding 2 little-endian bytes.
fn read_i16(buffer: &[u8], offset: usize) -> Option<i16> {
    Some(i16::from_le_bytes(
        buffer.get(offset..offset + 2)?.try_into().ok()?,
    ))
}

/// Read `buffer` as a slot holding 8 little-endian bytes.
fn read_i64(buffer: &[u8], offset: usize) -> Option<i64> {
    Some(i64::from_le_bytes(
        buffer.get(offset..offset + 8)?.try_into().ok()?,
    ))
}

/// Store `value` into `buffer` as 4 little-endian bytes.
fn write_i32(buffer: &mut [u8], offset: usize, value: i32) -> Option<()> {
    buffer
        .get_mut(offset..offset + 4)?
        .copy_from_slice(&value.to_le_bytes());
    Some(())
}

/// Store `value` into `buffer` as 2 little-endian bytes.
fn write_i16(buffer: &mut [u8], offset: usize, value: i16) -> Option<()> {
    buffer
        .get_mut(offset..offset + 2)?
        .copy_from_slice(&value.to_le_bytes());
    Some(())
}

/// Store `value` into `buffer` as 8 little-endian bytes.
fn write_i64(buffer: &mut [u8], offset: usize, value: i64) -> Option<()> {
    buffer
        .get_mut(offset..offset + 8)?
        .copy_from_slice(&value.to_le_bytes());
    Some(())
}

/// The header of `buffer`, if the frame it describes is of type `expected`.
///
/// This is the check every frame's `read` opens with: the reference dispatches
/// on `frame_header->type` first and interprets the rest of the bytes only
/// afterwards (`aeron_receive_channel_endpoint.c:533-545`), so reading the
/// wrong frame's fields is not something a caller may do by accident here.
fn header_of(buffer: &[u8], expected: i16) -> Option<FrameHeader> {
    let header = FrameHeader::read(buffer)?;
    if header.frame_type == expected {
        Some(header)
    } else {
        None
    }
}

/// Build the fixed header of a frame whose length is its own size.
///
/// The constructors in the reference all do exactly this —
/// `frame_length = sizeof(struct)`, `version = AERON_FRAME_HEADER_VERSION`,
/// `type = AERON_HDR_TYPE_*`, `flags = the caller's or 0` — whether they are
/// sending a NAK (`aeron_receive_channel_endpoint.c:354-357`), an SM
/// (`:313-316`), a setup (`aeron_network_publication.c:396-399`), a response
/// setup (`aeron_receive_channel_endpoint.c:445-447`) or a RTTM (`:407-410`).
fn fixed_header(frame_type: i16, length: usize, flags: u8) -> FrameHeader {
    FrameHeader {
        frame_length: length as i32,
        version: VERSION,
        flags,
        frame_type,
    }
}

/// The 8-byte header every UDP frame begins with
/// (`aeron_udp_protocol.h:27-34`).
///
/// Every frame struct below has one of these as its first field, and on the
/// wire the four members sit at offsets 0, 4, 5 and 6: `frame_length` is
/// 4 bytes, `version` and `flags` one each, and `type` two more, which under
/// `#pragma pack(4)` leaves no gap and no tail. `type` is spelled `frame_type`
/// here because `type` is a Rust keyword; on the wire it is the same two bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct FrameHeader {
    /// `(aeron_udp_protocol.h:29)` — the whole frame, header included, in
    /// bytes. **Negative means uncommitted**: a publication writes the length
    /// as a negative and flips it after the payload lands
    /// (`aeron_publication.c:132`), so a negative value here is a frame that
    /// was read out of a term buffer mid-commit, never one that came off a
    /// socket. [`is_frame_valid`] refuses it (`:232`).
    pub frame_length: i32,
    /// `(aeron_udp_protocol.h:30)` — always [`VERSION`]; a receiver drops
    /// anything else (`:233`).
    pub version: i8,
    /// `(aeron_udp_protocol.h:31)` — bits whose meaning depends on
    /// `frame_type`; see [`header_flags`].
    pub flags: u8,
    /// `(aeron_udp_protocol.h:32)` — one of [`frame_type`].
    pub frame_type: i16,
}

impl FrameHeader {
    /// Read the header at the front of `buffer`.
    ///
    /// # Returns
    ///
    /// `None` when fewer than [`HEADER_LENGTH`] bytes are available. Nothing
    /// else is inspected: whether the frame is *well-formed* is
    /// [`is_frame_valid`]'s question, and whether it is the type a caller wants
    /// is [`DataFrame::read`]'s and its siblings'.
    pub fn read(buffer: &[u8]) -> Option<Self> {
        if buffer.len() < HEADER_LENGTH {
            return None;
        }

        Some(Self {
            frame_length: read_i32(buffer, 0)?,
            version: *buffer.get(4)? as i8,
            flags: *buffer.get(5)?,
            frame_type: read_i16(buffer, 6)?,
        })
    }

    /// Write the header to the front of `buffer`, little-endian.
    ///
    /// # Errors
    ///
    /// `None`, with nothing written, if `buffer` is shorter than
    /// [`HEADER_LENGTH`].
    pub fn write(&self, buffer: &mut [u8]) -> Option<()> {
        if buffer.len() < HEADER_LENGTH {
            return None;
        }

        write_i32(buffer, 0, self.frame_length)?;
        *buffer.get_mut(4)? = self.version as u8;
        *buffer.get_mut(5)? = self.flags;
        write_i16(buffer, 6, self.frame_type)
    }

    /// Whether this header belongs to a data-plane frame: DATA **or** PAD.
    ///
    /// The reference pairs the two wherever the distinction from control
    /// frames matters — one branch of `aeron_is_frame_valid`
    /// (`aeron_udp_protocol.h:235`), one `case` pair in the receiver's dispatch
    /// (`aeron_receive_channel_endpoint.c:535-536`), one loop over frames in
    /// the publication image (`aeron_publication_image.c:687`). A padding frame
    /// carries no payload but is still a frame of the term buffer, and code
    /// that reads "not NAK/SM/SETUP, therefore data" gets PAD wrong in a way
    /// nothing notices until a term boundary.
    pub fn is_data(&self) -> bool {
        self.frame_type == frame_type::DATA || self.frame_type == frame_type::PAD
    }
}

/// `aeron_is_frame_valid` (`aeron_udp_protocol.h:229-264`): does a received
/// datagram of `packet_length` bytes hold a frame of the type its header
/// claims?
///
/// Three checks come first and apply to every type (`:231-233`): the packet is
/// at least a header long, `frame_length` is not negative, and the version is
/// [`VERSION`].
///
/// Then one floor per type, and one asymmetry that is easy to read past:
/// **data frames are never checked against `frame_length`.** For DATA and PAD
/// the reference tests `packet_length >= AERON_DATA_HEADER_LENGTH` and
/// `AERON_IS_ALIGNED(packet_length, AERON_FRAME_ALIGNMENT)` (`:237`, the
/// alignment macro at `aeron_bitutil.h:37`) — it is the *packet* that must be a
/// 32-byte multiple, and the header's own length is not consulted at all. Every
/// other type is checked against both its floor and `frame_length <=
/// packet_length`.
///
/// SETUP is deliberately exempt from that second check: a SETUP from an ATS
/// peer describes more than this struct does, and the reference says so in
/// place (`:251`). The types the switch does not list — the ATS data frames,
/// [`frame_type::EXT`] and anything a newer reference invents — fall to
/// `default: return false` (`:259-260`).
///
/// A caller that wants the reference's *per-destination* rules — the ones that
/// also validate `error_length` (`aeron_send_channel_endpoint.c:666-667`) or
/// the log-buffer frame length a data frame must have
/// (`aeron_publication_image.c:732`) — still has to ask those questions
/// separately; this function is the shared envelope and nothing more.
pub fn is_frame_valid(header: &FrameHeader, packet_length: usize) -> bool {
    if packet_length < HEADER_LENGTH || header.frame_length < 0 || header.version != VERSION {
        return false;
    }

    if header.is_data() {
        return packet_length >= DataFrame::LENGTH && packet_length % FRAME_ALIGNMENT == 0;
    }

    let frame_length = header.frame_length as usize;
    match header.frame_type {
        frame_type::NAK => packet_length >= NakFrame::LENGTH && frame_length <= packet_length,
        frame_type::SM | frame_type::ATS_SM => {
            packet_length >= StatusMessageFrame::LENGTH && frame_length <= packet_length
        }
        frame_type::ERR => packet_length >= ErrorFrame::LENGTH && frame_length <= packet_length,
        frame_type::SETUP => packet_length >= SetupFrame::LENGTH,
        frame_type::RTTM => packet_length >= RttmFrame::LENGTH && frame_length <= packet_length,
        frame_type::RES => {
            packet_length >= HEADER_LENGTH + RESOLUTION_HEADER_IPV4_LENGTH
                && frame_length <= packet_length
        }
        frame_type::RSP_SETUP => {
            packet_length >= RESPONSE_SETUP_HEADER_LENGTH && frame_length <= packet_length
        }
        _ => false,
    }
}

/// `aeron_compute_max_message_length` (`aeron_udp_protocol.h:272-276`): the
/// most a publication may offer in one message, given its term length.
///
/// One eighth of a term, clamped to [`MAX_MESSAGE_LENGTH`]. The eighth is what
/// keeps a fragmented message's retransmit window inside a term, and the clamp
/// is what keeps a large term from licensing a datagram no network will carry
/// — a 1 GiB term would otherwise allow 128 MiB. The reference's version takes
/// and returns `size_t`, where a negative argument becomes a huge unsigned
/// value and clamps to 16 MiB; this one keeps the sign, so a negative term
/// length returns a negative number rather than a clamp. Callers pass a term
/// length, which is always positive.
///
/// The client applies it to a publication's `max_message_length`
/// (`aeron-client/src/main/c/aeron_publication.c:70`) and the driver to its
/// window below a sender position (`aeron_network_publication.c:655`), so a
/// sender and a receiver that disagree about it is a flow-control bug rather
/// than a protocol error.
pub fn compute_max_message_length(term_length: i32) -> i32 {
    let max_length_for_term = term_length >> 3;
    if max_length_for_term < MAX_MESSAGE_LENGTH {
        max_length_for_term
    } else {
        MAX_MESSAGE_LENGTH
    }
}

/// `aeron_data_header_t` (`aeron_udp_protocol.h:56-65`): the header of a frame
/// in a term buffer, and the only frame that carries a term's payload.
///
/// [`DataFrame::LENGTH`] is 32 because the struct is four 8-byte groups, and
/// the reference asserts that rather than deriving it: `aeron_data_header_as_longs_t`
/// (`:36-40`) exists so the header can be zeroed and copied four words at a
/// time, and the client's translation unit static-asserts the two sizes agree
/// (`aeron_udp_protocol.c:22-24`). Any field added here changes a number the
/// reference has compiled into a hard failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DataFrame {
    /// `(aeron_udp_protocol.h:59)` — where the frame starts within its term,
    /// always a multiple of [`FRAME_ALIGNMENT`]. It is what makes a frame
    /// addressable in a gap report: a NAK names a term offset, not a position.
    pub term_offset: i32,
    /// `(aeron_udp_protocol.h:60)` — the session the frame belongs to.
    pub session_id: i32,
    /// `(aeron_udp_protocol.h:61)` — the stream within that session.
    pub stream_id: i32,
    /// `(aeron_udp_protocol.h:62)` — the term buffer generation the offset is
    /// relative to.
    pub term_id: i32,
    /// `(aeron_udp_protocol.h:63)` — zero in the ordinary case
    /// (`AERON_DATA_HEADER_DEFAULT_RESERVED_VALUE`, `:197`). A publication with
    /// a reserved-value fill writes something else, and a receiver that does
    /// not recognise the value is expected to discard the frame.
    pub reserved_value: i64,
}

impl DataFrame {
    /// `sizeof(aeron_data_header_t)` — 32, header included
    /// (`aeron_udp_protocol.h:56-65`).
    pub const LENGTH: usize = 32;

    /// Read a DATA frame from `buffer`.
    ///
    /// # Returns
    ///
    /// `None` if the buffer is shorter than [`DataFrame::LENGTH`] or the header
    /// is not DATA. `frame_length` is not checked against the length of the
    /// buffer: the reference validates frames at the point of use, not here
    /// (`aeron_publication_image.c:732`).
    pub fn read(buffer: &[u8]) -> Option<Self> {
        header_of(buffer, frame_type::DATA)?;

        Some(Self {
            term_offset: read_i32(buffer, 8)?,
            session_id: read_i32(buffer, 12)?,
            stream_id: read_i32(buffer, 16)?,
            term_id: read_i32(buffer, 20)?,
            reserved_value: read_i64(buffer, 24)?,
        })
    }

    /// Write a DATA frame to `buffer`, with `flags` zeroed.
    ///
    /// A DATA frame with no flags set is not one any receiver expects to act
    /// on, so a caller sending a real message wants [`write_with_flags`] and
    /// at least [`header_flags::BEGIN`] and [`header_flags::END`]; this form
    /// exists for the frame's fixed part on its own.
    ///
    /// [`write_with_flags`]: DataFrame::write_with_flags
    ///
    /// # Errors
    ///
    /// `None`, with nothing written, if `buffer` is shorter than
    /// [`DataFrame::LENGTH`].
    pub fn write(&self, buffer: &mut [u8]) -> Option<()> {
        self.write_with_flags(buffer, 0)
    }

    /// Write a DATA frame to `buffer` with `flags` in the header.
    ///
    /// The flags are the sender's: `BEGIN|END` for a message that fits in one
    /// frame (`aeron_publication.c:136`), `BEGIN` or `END` alone for the ends
    /// of a fragmented one, and `REVOKED` for a fragment that has become part
    /// of a padding frame instead.
    ///
    /// # Errors
    ///
    /// `None`, with nothing written, if `buffer` is shorter than
    /// [`DataFrame::LENGTH`].
    pub fn write_with_flags(&self, buffer: &mut [u8], flags: u8) -> Option<()> {
        if buffer.len() < Self::LENGTH {
            return None;
        }

        fixed_header(frame_type::DATA, Self::LENGTH, flags).write(buffer)?;
        write_i32(buffer, 8, self.term_offset)?;
        write_i32(buffer, 12, self.session_id)?;
        write_i32(buffer, 16, self.stream_id)?;
        write_i32(buffer, 20, self.term_id)?;
        write_i64(buffer, 24, self.reserved_value)
    }
}

/// `aeron_setup_header_t` (`aeron_udp_protocol.h:42-54`): the frame that opens
/// a session, and the only one that names the term length and MTU the sender
/// intends to use.
///
/// [`SetupFrame::LENGTH`] is 40 — the header plus eight `i32`s. The last of
/// them, `ttl`, is the multicast TTL from the channel URI
/// (`aeron_network_publication.c:401`); on a unicast channel it is carried and
/// ignored.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SetupFrame {
    /// `(aeron_udp_protocol.h:45)`.
    pub term_offset: i32,
    /// `(aeron_udp_protocol.h:46)`.
    pub session_id: i32,
    /// `(aeron_udp_protocol.h:47)`.
    pub stream_id: i32,
    /// `(aeron_udp_protocol.h:48)` — the generation a receiver starts counting
    /// term ids from.
    pub initial_term_id: i32,
    /// `(aeron_udp_protocol.h:49)` — the generation the publisher is writing
    /// now.
    pub active_term_id: i32,
    /// `(aeron_udp_protocol.h:50)` — the term buffer length in bytes, from
    /// which both ends derive [`compute_max_message_length`].
    pub term_length: i32,
    /// `(aeron_udp_protocol.h:51)` — the datagram size the publisher will not
    /// exceed.
    pub mtu: i32,
    /// `(aeron_udp_protocol.h:52)` — the multicast TTL.
    pub ttl: i32,
}

impl SetupFrame {
    /// `sizeof(aeron_setup_header_t)` — 40, header included
    /// (`aeron_udp_protocol.h:42-54`).
    pub const LENGTH: usize = 40;

    /// Read a SETUP frame from `buffer`.
    ///
    /// # Returns
    ///
    /// `None` if the buffer is shorter than [`SetupFrame::LENGTH`] or the
    /// header is not SETUP.
    pub fn read(buffer: &[u8]) -> Option<Self> {
        header_of(buffer, frame_type::SETUP)?;

        Some(Self {
            term_offset: read_i32(buffer, 8)?,
            session_id: read_i32(buffer, 12)?,
            stream_id: read_i32(buffer, 16)?,
            initial_term_id: read_i32(buffer, 20)?,
            active_term_id: read_i32(buffer, 24)?,
            term_length: read_i32(buffer, 28)?,
            mtu: read_i32(buffer, 32)?,
            ttl: read_i32(buffer, 36)?,
        })
    }

    /// Write a SETUP frame to `buffer`, with `flags` zeroed.
    ///
    /// # Errors
    ///
    /// `None`, with nothing written, if `buffer` is shorter than
    /// [`SetupFrame::LENGTH`].
    pub fn write(&self, buffer: &mut [u8]) -> Option<()> {
        self.write_with_flags(buffer, 0)
    }

    /// Write a SETUP frame to `buffer` with `flags` in the header.
    ///
    /// The two flags the reference sets here are
    /// [`header_flags::SETUP_SEND_RESPONSE`], when the publication wants a
    /// response correlation id answered, and [`header_flags::SETUP_GROUP`],
    /// when it has group semantics; it computes them as separate bits and ORs
    /// them (`aeron_network_publication.c:392-400`), so both may be set at
    /// once.
    ///
    /// # Errors
    ///
    /// `None`, with nothing written, if `buffer` is shorter than
    /// [`SetupFrame::LENGTH`].
    pub fn write_with_flags(&self, buffer: &mut [u8], flags: u8) -> Option<()> {
        if buffer.len() < Self::LENGTH {
            return None;
        }

        fixed_header(frame_type::SETUP, Self::LENGTH, flags).write(buffer)?;
        write_i32(buffer, 8, self.term_offset)?;
        write_i32(buffer, 12, self.session_id)?;
        write_i32(buffer, 16, self.stream_id)?;
        write_i32(buffer, 20, self.initial_term_id)?;
        write_i32(buffer, 24, self.active_term_id)?;
        write_i32(buffer, 28, self.term_length)?;
        write_i32(buffer, 32, self.mtu)?;
        write_i32(buffer, 36, self.ttl)
    }
}

/// `aeron_nak_header_t` (`aeron_udp_protocol.h:67-76`): a receiver reporting a
/// gap, and the frame the retransmit path runs on.
///
/// [`NakFrame::LENGTH`] is 28 — the header plus five `i32`s — so it is *not* a
/// multiple of [`FRAME_ALIGNMENT`]. That is fine and deliberate: alignment is a
/// property of the term buffer, and a NAK is only ever a datagram. The
/// reference sends NAKs with `flags` set to zero
/// (`aeron_receive_channel_endpoint.c:356`); the type defines no flag bits, so
/// there is no `write_with_flags` here.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NakFrame {
    /// `(aeron_udp_protocol.h:70)`.
    pub session_id: i32,
    /// `(aeron_udp_protocol.h:71)`.
    pub stream_id: i32,
    /// `(aeron_udp_protocol.h:72)` — the term the gap is in.
    pub term_id: i32,
    /// `(aeron_udp_protocol.h:73)` — the first byte of the gap.
    pub term_offset: i32,
    /// `(aeron_udp_protocol.h:74)` — how many bytes are missing. Unbounded in
    /// the struct, and in practice cut to what one datagram of retransmits can
    /// answer.
    pub length: i32,
}

impl NakFrame {
    /// `sizeof(aeron_nak_header_t)` — 28, header included
    /// (`aeron_udp_protocol.h:67-76`).
    pub const LENGTH: usize = 28;

    /// Read a NAK frame from `buffer`.
    ///
    /// # Returns
    ///
    /// `None` if the buffer is shorter than [`NakFrame::LENGTH`] or the header
    /// is not NAK.
    pub fn read(buffer: &[u8]) -> Option<Self> {
        header_of(buffer, frame_type::NAK)?;

        Some(Self {
            session_id: read_i32(buffer, 8)?,
            stream_id: read_i32(buffer, 12)?,
            term_id: read_i32(buffer, 16)?,
            term_offset: read_i32(buffer, 20)?,
            length: read_i32(buffer, 24)?,
        })
    }

    /// Write a NAK frame to `buffer`, with `flags` zeroed as the reference
    /// sends it (`aeron_receive_channel_endpoint.c:356`).
    ///
    /// # Errors
    ///
    /// `None`, with nothing written, if `buffer` is shorter than
    /// [`NakFrame::LENGTH`].
    pub fn write(&self, buffer: &mut [u8]) -> Option<()> {
        if buffer.len() < Self::LENGTH {
            return None;
        }

        fixed_header(frame_type::NAK, Self::LENGTH, 0).write(buffer)?;
        write_i32(buffer, 8, self.session_id)?;
        write_i32(buffer, 12, self.stream_id)?;
        write_i32(buffer, 16, self.term_id)?;
        write_i32(buffer, 20, self.term_offset)?;
        write_i32(buffer, 24, self.length)
    }
}

/// `aeron_status_message_header_t` (`aeron_udp_protocol.h:78-88`): the frame
/// that carries a receiver's position, its window and its liveness.
///
/// [`StatusMessageFrame::LENGTH`] is 36, and the `i64` at the end is why the
/// struct is 36 rather than the 28 a reader counting five `i32`s would expect:
/// `receiver_id` is 8 bytes at offset 28.
///
/// The frame may be **44** bytes instead. When the endpoint has a group tag to
/// gossip, the reference appends `aeron_status_message_optional_header_t`
/// (`:90-94`) and sets `frame_length` to 36 + 8
/// (`aeron_receive_channel_endpoint.c:309-316`); a reader tells the two forms
/// apart by `frame_length` alone. See [`StatusMessageFrame::group_tag`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StatusMessageFrame {
    /// `(aeron_udp_protocol.h:81)`.
    pub session_id: i32,
    /// `(aeron_udp_protocol.h:82)`.
    pub stream_id: i32,
    /// `(aeron_udp_protocol.h:83)` — the term the receiver has consumed up to.
    pub consumption_term_id: i32,
    /// `(aeron_udp_protocol.h:84)` — how far into that term.
    pub consumption_term_offset: i32,
    /// `(aeron_udp_protocol.h:85)` — the receiver's flow-control window.
    pub receiver_window: i32,
    /// `(aeron_udp_protocol.h:86)` — which receiver sent this, so a publisher
    /// with several destinations can keep their positions apart.
    pub receiver_id: i64,
}

impl StatusMessageFrame {
    /// `sizeof(aeron_status_message_header_t)` — 36, header included
    /// (`aeron_udp_protocol.h:78-88`).
    pub const LENGTH: usize = 36;

    /// `sizeof(aeron_status_message_optional_header_t)` — the 8 bytes of group
    /// tag that may follow the fixed header (`aeron_udp_protocol.h:90-94`).
    pub const OPTIONAL_GROUP_TAG_LENGTH: usize = 8;

    /// Read an SM frame from `buffer`.
    ///
    /// # Returns
    ///
    /// `None` if the buffer is shorter than [`StatusMessageFrame::LENGTH`] or
    /// the header is not SM. This succeeds for the 44-byte group-tagged form as
    /// well: the fixed header is the same 36 bytes either way, and the extra
    /// eight are [`group_tag`](StatusMessageFrame::group_tag)'s business.
    pub fn read(buffer: &[u8]) -> Option<Self> {
        header_of(buffer, frame_type::SM)?;

        Some(Self {
            session_id: read_i32(buffer, 8)?,
            stream_id: read_i32(buffer, 12)?,
            consumption_term_id: read_i32(buffer, 16)?,
            consumption_term_offset: read_i32(buffer, 20)?,
            receiver_window: read_i32(buffer, 24)?,
            receiver_id: read_i64(buffer, 28)?,
        })
    }

    /// Write an SM frame to `buffer`, with `flags` zeroed.
    ///
    /// This writes the 36-byte form. A group tag is not part of this struct, so
    /// a caller that needs one writes it into `buffer[36..44]` and fixes up
    /// `frame_length`; the reference's own function takes the flag byte as a
    /// parameter and no struct owns the tag.
    ///
    /// # Errors
    ///
    /// `None`, with nothing written, if `buffer` is shorter than
    /// [`StatusMessageFrame::LENGTH`].
    pub fn write(&self, buffer: &mut [u8]) -> Option<()> {
        self.write_with_flags(buffer, 0)
    }

    /// Write an SM frame to `buffer` with `flags` in the header.
    ///
    /// The two flags the reference uses are
    /// [`header_flags::SM_SEND_SETUP`], for a receiver answering a publisher it
    /// has no image for, and [`header_flags::SM_EOS`], once it has seen the
    /// publisher's end of stream.
    ///
    /// # Errors
    ///
    /// `None`, with nothing written, if `buffer` is shorter than
    /// [`StatusMessageFrame::LENGTH`].
    pub fn write_with_flags(&self, buffer: &mut [u8], flags: u8) -> Option<()> {
        if buffer.len() < Self::LENGTH {
            return None;
        }

        fixed_header(frame_type::SM, Self::LENGTH, flags).write(buffer)?;
        write_i32(buffer, 8, self.session_id)?;
        write_i32(buffer, 12, self.stream_id)?;
        write_i32(buffer, 16, self.consumption_term_id)?;
        write_i32(buffer, 20, self.consumption_term_offset)?;
        write_i32(buffer, 24, self.receiver_window)?;
        write_i64(buffer, 28, self.receiver_id)
    }

    /// The group tag this SM carries, if it carries one — the mirror of
    /// `aeron_udp_protocol_group_tag` (`aeron_udp_protocol.c:26-43`).
    ///
    /// The tag is not a field of the fixed header, because it is not always
    /// there: the reference decides by `frame_length` alone, comparing it to
    /// `sizeof(status_message_header) + sizeof(optional_header)`
    /// (`aeron_udp_protocol.c:31-33`), and appends the 8 bytes only when the
    /// endpoint has a tag to send. So the answer to "is there a group tag"
    /// lives in the frame header, and this is the only place that reads it.
    ///
    /// `buffer` is the same datagram `read` was given; `None` means no tag, and
    /// a frame whose `frame_length` claims one that the buffer does not hold
    /// also answers `None` rather than reporting a truncated value.
    pub fn group_tag(&self, buffer: &[u8]) -> Option<i64> {
        let header = FrameHeader::read(buffer)?;
        if header.frame_length != (Self::LENGTH + Self::OPTIONAL_GROUP_TAG_LENGTH) as i32 {
            return None;
        }

        read_i64(buffer, Self::LENGTH)
    }
}

/// `aeron_error_header_t` (`aeron_udp_protocol.h:96-106`): a receiver refusing
/// a publication, or a driver telling a peer's receiver to go away.
///
/// [`ErrorFrame::LENGTH`] is 40 and the text follows it. Both halves of that
/// sentence are load-bearing: the group tag is a fixed field even when the
/// frame does not carry the flag that makes it meaningful
/// (`aeron_receive_channel_endpoint.c:489`), and the text begins at offset 40
/// (`aeron_receive_channel_endpoint.c:491`). See [`ErrorFrame::text`] for that
/// second half, and [`MAX_ERROR_TEXT_LENGTH`] for how long it may be.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ErrorFrame {
    /// `(aeron_udp_protocol.h:99)`.
    pub session_id: i32,
    /// `(aeron_udp_protocol.h:100)`.
    pub stream_id: i32,
    /// `(aeron_udp_protocol.h:101)` — who is refusing; it is also the liveness
    /// signal that lets a publisher stop waiting on a receiver that has gone
    /// (`aeron_network_publication.c:871`).
    pub receiver_id: i64,
    /// `(aeron_udp_protocol.h:102)` — meaningful only when `flags` carries
    /// [`header_flags::ERR_HAS_GROUP_TAG`]; a reader that sees the bit clear
    /// must ignore what it finds here (`aeron_network_publication.c:880`).
    pub group_tag: i64,
    /// `(aeron_udp_protocol.h:103)` — the error code, or a positive errno the
    /// receiver's platform produced.
    pub error_code: i32,
    /// `(aeron_udp_protocol.h:104)` — how many bytes of text follow the fixed
    /// header, at most [`MAX_ERROR_TEXT_LENGTH`] (`:225`).
    pub error_length: i32,
}

impl ErrorFrame {
    /// `sizeof(aeron_error_header_t)` — 40, header included, before any text
    /// (`aeron_udp_protocol.h:96-106`).
    pub const LENGTH: usize = 40;

    /// Read an ERR frame's fixed header from `buffer`.
    ///
    /// # Returns
    ///
    /// `None` if the buffer is shorter than [`ErrorFrame::LENGTH`] or the
    /// header is not ERR. The text is not part of the returned struct; see
    /// [`text`](ErrorFrame::text).
    pub fn read(buffer: &[u8]) -> Option<Self> {
        header_of(buffer, frame_type::ERR)?;

        Some(Self {
            session_id: read_i32(buffer, 8)?,
            stream_id: read_i32(buffer, 12)?,
            receiver_id: read_i64(buffer, 16)?,
            group_tag: read_i64(buffer, 24)?,
            error_code: read_i32(buffer, 32)?,
            error_length: read_i32(buffer, 36)?,
        })
    }

    /// Write an ERR frame's fixed header to `buffer`, with `flags` zeroed.
    ///
    /// The frame's `frame_length` is written as
    /// `LENGTH + error_length` — the whole frame including the text, which is
    /// what the reference stores (`aeron_receive_channel_endpoint.c:480-481`)
    /// and what a receiver's own validity check tests
    /// (`aeron_send_channel_endpoint.c:666-667`). The text itself is *not*
    /// written: this stores 40 bytes, and the caller writes the message
    /// `error_length` names into `buffer[`[`ErrorFrame::LENGTH`]`..]`
    /// afterwards. A length that claims text the caller does not write is the
    /// caller's to avoid; a length that omits text the caller *does* write
    /// would not be, so the field is honoured here rather than left at 40.
    ///
    /// # Errors
    ///
    /// `None`, with nothing written, if `buffer` is shorter than
    /// [`ErrorFrame::LENGTH`].
    pub fn write(&self, buffer: &mut [u8]) -> Option<()> {
        self.write_with_flags(buffer, 0)
    }

    /// Write an ERR frame's fixed header to `buffer` with `flags` in the
    /// header, text and all not included.
    ///
    /// The only flag the reference sets on an ERR frame is
    /// [`header_flags::ERR_HAS_GROUP_TAG`], and only when the endpoint has a
    /// group tag (`aeron_receive_channel_endpoint.c:483`).
    ///
    /// # Errors
    ///
    /// `None`, with nothing written, if `buffer` is shorter than
    /// [`ErrorFrame::LENGTH`].
    pub fn write_with_flags(&self, buffer: &mut [u8], flags: u8) -> Option<()> {
        if buffer.len() < Self::LENGTH {
            return None;
        }

        let frame_length = Self::LENGTH + self.error_length.max(0) as usize;
        fixed_header(frame_type::ERR, frame_length, flags).write(buffer)?;
        write_i32(buffer, 8, self.session_id)?;
        write_i32(buffer, 12, self.stream_id)?;
        write_i64(buffer, 16, self.receiver_id)?;
        write_i64(buffer, 24, self.group_tag)?;
        write_i32(buffer, 32, self.error_code)?;
        write_i32(buffer, 36, self.error_length)
    }

    /// The message text that follows the fixed header: the bytes at
    /// `buffer[LENGTH..LENGTH + error_length]`, validated against the buffer
    /// that was read.
    ///
    /// The text is not a field of the struct because it is not a field of the
    /// struct in the reference either — it is `error + 1`, the byte after the
    /// 40-byte header, whose length is `error_length`
    /// (`aeron_network_publication.c:867`,
    /// `aeron_receive_channel_endpoint.c:491`), and the reference's own
    /// validity check is exactly the arithmetic this method performs
    /// (`error_length` non-negative, within its own maximum, and within the
    /// frame: `aeron_send_channel_endpoint.c:666-667`).
    ///
    /// The returned slice borrows the caller's buffer, so it lives no longer
    /// than the datagram does.
    ///
    /// # Returns
    ///
    /// `None` if `error_length` is negative, if it exceeds
    /// [`MAX_ERROR_TEXT_LENGTH`], or if the buffer does not hold that many
    /// bytes after the fixed header — a truncated datagram reports "no text"
    /// rather than a prefix of one.
    pub fn text<'a>(&self, buffer: &'a [u8]) -> Option<&'a [u8]> {
        if self.error_length < 0 || self.error_length > MAX_ERROR_TEXT_LENGTH {
            return None;
        }

        buffer
            .get(Self::LENGTH..Self::LENGTH + self.error_length as usize)
            .filter(|text| text.len() == self.error_length as usize)
    }
}

/// `aeron_rttm_header_t` (`aeron_udp_protocol.h:108-117`): a measurement of how
/// long a datagram took to arrive, echoed so the sender can size its timers.
///
/// [`RttmFrame::LENGTH`] is 40 — the header, two `i32`s and three `i64`s.
/// `reception_delta` is the `i64` that a reader sizing this frame by its other
/// fields tends to miss.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RttmFrame {
    /// `(aeron_udp_protocol.h:111)`.
    pub session_id: i32,
    /// `(aeron_udp_protocol.h:112)`.
    pub stream_id: i32,
    /// `(aeron_udp_protocol.h:113)` — the timestamp the original frame carried.
    pub echo_timestamp: i64,
    /// `(aeron_udp_protocol.h:114)` — nanoseconds spent inside the receiver,
    /// subtracted alongside the echo when the round trip is computed
    /// (`aeron_publication_image.c:854`). The reply the reference sends zeroes
    /// it (`aeron_network_publication.c:907`). It is an `i64` on the wire, and
    /// the send path passes it as one (`aeron_receive_channel_endpoint.c:400`).
    pub reception_delta: i64,
    /// `(aeron_udp_protocol.h:115)` — which receiver measured.
    pub receiver_id: i64,
}

impl RttmFrame {
    /// `sizeof(aeron_rttm_header_t)` — 40, header included
    /// (`aeron_udp_protocol.h:108-117`).
    pub const LENGTH: usize = 40;

    /// Read an RTTM frame from `buffer`.
    ///
    /// # Returns
    ///
    /// `None` if the buffer is shorter than [`RttmFrame::LENGTH`] or the header
    /// is not RTTM.
    pub fn read(buffer: &[u8]) -> Option<Self> {
        header_of(buffer, frame_type::RTTM)?;

        Some(Self {
            session_id: read_i32(buffer, 8)?,
            stream_id: read_i32(buffer, 12)?,
            echo_timestamp: read_i64(buffer, 16)?,
            reception_delta: read_i64(buffer, 24)?,
            receiver_id: read_i64(buffer, 32)?,
        })
    }

    /// Write an RTTM frame to `buffer`, with `flags` zeroed.
    ///
    /// # Errors
    ///
    /// `None`, with nothing written, if `buffer` is shorter than
    /// [`RttmFrame::LENGTH`].
    pub fn write(&self, buffer: &mut [u8]) -> Option<()> {
        self.write_with_flags(buffer, 0)
    }

    /// Write an RTTM frame to `buffer` with `flags` in the header.
    ///
    /// The one flag here is [`header_flags::RTTM_REPLY`], which a receiver sets
    /// on the answer it echoes back so that the answer is not answered in turn
    /// (`aeron_receive_channel_endpoint.c:409`,
    /// `aeron_network_publication.c:893`).
    ///
    /// # Errors
    ///
    /// `None`, with nothing written, if `buffer` is shorter than
    /// [`RttmFrame::LENGTH`].
    pub fn write_with_flags(&self, buffer: &mut [u8], flags: u8) -> Option<()> {
        if buffer.len() < Self::LENGTH {
            return None;
        }

        fixed_header(frame_type::RTTM, Self::LENGTH, flags).write(buffer)?;
        write_i32(buffer, 8, self.session_id)?;
        write_i32(buffer, 12, self.stream_id)?;
        write_i64(buffer, 16, self.echo_timestamp)?;
        write_i64(buffer, 24, self.reception_delta)?;
        write_i64(buffer, 32, self.receiver_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The values an `i32` field is round-tripped through: zero, both signs,
    /// the alignment boundary, and both ends of the range.
    const I32_VALUES: [i32; 10] = [
        0,
        1,
        -1,
        32,
        -32,
        0x1234_5678,
        0x89AB_CDEF_u32 as i32,
        i32::MIN,
        i32::MIN + 1,
        i32::MAX,
    ];

    /// The same for `i64` fields, where sign handling also has a 4-byte-gap
    /// mistake to make.
    const I64_VALUES: [i64; 9] = [
        0,
        1,
        -1,
        i64::MIN,
        i64::MIN + 1,
        i64::MAX,
        i64::MAX - 1,
        0x0102_0304_0506_0708,
        -0x0102_0304_0506_0708,
    ];

    /// A frame header built by hand, little-endian, with the version the
    /// reference's frames always carry.
    fn header_bytes(frame_type: i16, frame_length: i32, flags: u8) -> [u8; HEADER_LENGTH] {
        let mut bytes = [0u8; HEADER_LENGTH];
        bytes[..4].copy_from_slice(&frame_length.to_le_bytes());
        bytes[4] = 0;
        bytes[5] = flags;
        bytes[6..].copy_from_slice(&frame_type.to_le_bytes());
        bytes
    }

    /// One frame type's reader, as a pointer, so a table can hold readers of
    /// six different types.
    type Reader = fn(&[u8]) -> bool;

    /// A datagram of `packet_length` bytes whose header claims `frame_length`
    /// and `frame_type`, for asking [`is_frame_valid`] about the pairing.
    fn packet(frame_type: i16, frame_length: i32, packet_length: usize) -> Vec<u8> {
        let mut bytes = vec![0u8; packet_length.max(HEADER_LENGTH)];
        bytes[..HEADER_LENGTH].copy_from_slice(&header_bytes(frame_type, frame_length, 0));
        bytes
    }

    /// [`is_frame_valid`] for a header that is itself well-formed, so that only
    /// the length question varies.
    fn validity(frame_type: i16, frame_length: i32, packet_length: usize) -> bool {
        let bytes = packet(frame_type, frame_length, packet_length);
        let header = FrameHeader::read(&bytes).expect("a full header");
        is_frame_valid(&header, packet_length)
    }

    #[test]
    fn the_lengths_are_the_reference_struct_sizes() {
        // `aeron_udp_protocol.h:186-188` and the `sizeof` of each struct
        // (`:42-54`, `:56-65`, `:67-76`, `:78-88`, `:96-106`, `:108-117`),
        // all under `#pragma pack(4)` (`:25-26`).
        assert_eq!(HEADER_LENGTH, 8);
        assert_eq!(FRAME_ALIGNMENT, 32);
        assert_eq!(DataFrame::LENGTH, 32);
        assert_eq!(SetupFrame::LENGTH, 40);
        assert_eq!(NakFrame::LENGTH, 28);
        assert_eq!(StatusMessageFrame::LENGTH, 36);
        assert_eq!(ErrorFrame::LENGTH, 40);
        assert_eq!(RttmFrame::LENGTH, 40);

        // The two that are not the obvious sum: ERR has a fixed 8-byte group
        // tag before its text, and RTTM's `reception_delta` is an i64.
        assert_eq!(ErrorFrame::LENGTH, HEADER_LENGTH + 4 + 4 + 8 + 8 + 4 + 4);
        assert_eq!(RttmFrame::LENGTH, HEADER_LENGTH + 4 + 4 + 8 + 8 + 8);
        assert_eq!(
            StatusMessageFrame::LENGTH + StatusMessageFrame::OPTIONAL_GROUP_TAG_LENGTH,
            44
        );
    }

    #[test]
    fn the_type_constants_are_the_reference_macros() {
        // `aeron_udp_protocol.h:172-184`
        assert_eq!(frame_type::PAD, 0x00);
        assert_eq!(frame_type::DATA, 0x01);
        assert_eq!(frame_type::NAK, 0x02);
        assert_eq!(frame_type::SM, 0x03);
        assert_eq!(frame_type::ERR, 0x04);
        assert_eq!(frame_type::SETUP, 0x05);
        assert_eq!(frame_type::RTTM, 0x06);
        assert_eq!(frame_type::RES, 0x07);
        assert_eq!(frame_type::ATS_DATA, 0x08);
        assert_eq!(frame_type::ATS_SETUP, 0x09);
        assert_eq!(frame_type::ATS_SM, 0x0A);
        assert_eq!(frame_type::RSP_SETUP, 0x0B);
        assert_eq!(frame_type::EXT, -1);
        assert_eq!(VERSION, 0);
    }

    #[test]
    fn the_flag_constants_are_the_reference_macros() {
        // `aeron_udp_protocol.h:190-205`, `:227`
        assert_eq!(header_flags::BEGIN, 0x80);
        assert_eq!(header_flags::END, 0x40);
        assert_eq!(header_flags::EOS, 0x20);
        assert_eq!(header_flags::REVOKED, 0x10);
        assert_eq!(
            header_flags::UNFRAGMENTED,
            header_flags::BEGIN | header_flags::END
        );
        assert_eq!(header_flags::SM_SEND_SETUP, 0x80);
        assert_eq!(header_flags::SM_EOS, 0x40);
        assert_eq!(header_flags::SETUP_SEND_RESPONSE, 0x80);
        assert_eq!(header_flags::SETUP_GROUP, 0x40);
        assert_eq!(header_flags::RTTM_REPLY, 0x80);
        assert_eq!(header_flags::ERR_HAS_GROUP_TAG, 0x08);

        // The collisions are real: the same byte means different things to
        // different frame types, and a SETUP can carry both of its bits at
        // once (`aeron_network_publication.c:392-400`).
        assert_ne!(header_flags::EOS, header_flags::SM_EOS);
        assert_eq!(
            header_flags::SETUP_SEND_RESPONSE | header_flags::SETUP_GROUP,
            0xC0
        );
        assert_ne!(
            header_flags::SETUP_SEND_RESPONSE,
            header_flags::ERR_HAS_GROUP_TAG
        );
    }

    #[test]
    fn frame_header_golden_vector() {
        // frame_length = 32, version = 0, flags = BEGIN|END, type = DATA.
        let bytes: [u8; 8] = [0x20, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x01, 0x00];

        let header = FrameHeader::read(&bytes).expect("a full header");
        assert_eq!(
            header,
            FrameHeader {
                frame_length: 32,
                version: 0,
                flags: header_flags::BEGIN | header_flags::END,
                frame_type: frame_type::DATA,
            }
        );

        let mut out = [0u8; HEADER_LENGTH];
        header.write(&mut out).expect("written");
        assert_eq!(out, bytes);
    }

    #[test]
    fn frame_header_signs_a_negative_length_and_an_extension_type() {
        // The uncommitted state of a term-buffer frame is a negative length
        // (`aeron_publication.c:132`), and EXT is the type that needs the i16.
        let bytes: [u8; 8] = [0xE0, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0xFF, 0xFF];

        let header = FrameHeader::read(&bytes).expect("a full header");
        assert_eq!(header.frame_length, -32);
        assert_eq!(header.frame_type, frame_type::EXT);
        assert!(!header.is_data());

        let mut out = [0u8; HEADER_LENGTH];
        header.write(&mut out).expect("written");
        assert_eq!(out, bytes);
    }

    #[test]
    fn data_frame_golden_vector() {
        let bytes: [u8; 32] = [
            0x20, 0x00, 0x00, 0x00, // frame_length = 32
            0x00, // version = 0
            0xC0, // flags = BEGIN | END
            0x01, 0x00, // type = DATA
            0x40, 0x01, 0x00, 0x00, // term_offset = 320
            0x78, 0x56, 0x34, 0x12, // session_id = 0x12345678
            0xEF, 0xCD, 0xAB, 0x89, // stream_id = 0x89ABCDEF (negative)
            0x00, 0x00, 0x00, 0x00, // term_id = 0
            0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, // reserved_value
        ];

        let frame = DataFrame::read(&bytes).expect("a DATA frame");
        assert_eq!(
            frame,
            DataFrame {
                term_offset: 320,
                session_id: 0x1234_5678,
                stream_id: 0x89AB_CDEF_u32 as i32,
                term_id: 0,
                reserved_value: 0x0102_0304_0506_0708,
            }
        );

        let mut out = [0u8; DataFrame::LENGTH];
        frame
            .write_with_flags(&mut out, header_flags::UNFRAGMENTED)
            .expect("written");
        assert_eq!(out, bytes);
    }

    #[test]
    fn data_frame_write_defaults_to_no_flags() {
        let frame = DataFrame {
            term_offset: 0,
            session_id: 1,
            stream_id: 2,
            term_id: 3,
            reserved_value: 4,
        };
        let mut out = [0u8; DataFrame::LENGTH];
        frame.write(&mut out).expect("written");

        let header = FrameHeader::read(&out).expect("a full header");
        assert_eq!(header.flags, 0);
        assert_eq!(header.frame_length, DataFrame::LENGTH as i32);
        assert_eq!(header.version, VERSION);
        assert_eq!(header.frame_type, frame_type::DATA);
        assert_eq!(DataFrame::read(&out), Some(frame));
    }

    #[test]
    fn setup_frame_golden_vector() {
        let bytes: [u8; 40] = [
            0x28, 0x00, 0x00, 0x00, // frame_length = 40
            0x00, // version
            0xC0, // flags = SEND_RESPONSE | GROUP
            0x05, 0x00, // type = SETUP
            0x00, 0x00, 0x00, 0x00, // term_offset = 0
            0x01, 0x00, 0x00, 0x00, // session_id = 1
            0x02, 0x00, 0x00, 0x00, // stream_id = 2
            0x03, 0x00, 0x00, 0x00, // initial_term_id = 3
            0x04, 0x00, 0x00, 0x00, // active_term_id = 4
            0x00, 0x00, 0x10, 0x00, // term_length = 1048576
            0x48, 0x04, 0x00, 0x00, // mtu = 1096
            0x10, 0x00, 0x00, 0x00, // ttl = 16
        ];

        let frame = SetupFrame::read(&bytes).expect("a SETUP frame");
        assert_eq!(
            frame,
            SetupFrame {
                term_offset: 0,
                session_id: 1,
                stream_id: 2,
                initial_term_id: 3,
                active_term_id: 4,
                term_length: 1024 * 1024,
                mtu: 1096,
                ttl: 16,
            }
        );

        let mut out = [0u8; SetupFrame::LENGTH];
        frame
            .write_with_flags(
                &mut out,
                header_flags::SETUP_SEND_RESPONSE | header_flags::SETUP_GROUP,
            )
            .expect("written");
        assert_eq!(out, bytes);
    }

    #[test]
    fn nak_frame_golden_vector() {
        let bytes: [u8; 28] = [
            0x1C, 0x00, 0x00, 0x00, // frame_length = 28
            0x00, // version
            0x00, // flags
            0x02, 0x00, // type = NAK
            0x11, 0x11, 0x11, 0x11, // session_id
            0x22, 0x22, 0x22, 0x22, // stream_id
            0x33, 0x33, 0x33, 0x33, // term_id
            0x80, 0x02, 0x00, 0x00, // term_offset = 640
            0x20, 0x00, 0x00, 0x00, // length = 32
        ];

        let frame = NakFrame::read(&bytes).expect("a NAK frame");
        assert_eq!(
            frame,
            NakFrame {
                session_id: 0x1111_1111,
                stream_id: 0x2222_2222,
                term_id: 0x3333_3333,
                term_offset: 640,
                length: 32,
            }
        );

        let mut out = [0u8; NakFrame::LENGTH];
        frame.write(&mut out).expect("written");
        assert_eq!(out, bytes);
    }

    #[test]
    fn status_message_frame_golden_vector() {
        let bytes: [u8; 36] = [
            0x24, 0x00, 0x00, 0x00, // frame_length = 36
            0x00, // version
            0x40, // flags = EOS
            0x03, 0x00, // type = SM
            0x0A, 0x00, 0x00, 0x00, // session_id = 10
            0x0B, 0x00, 0x00, 0x00, // stream_id = 11
            0x0C, 0x00, 0x00, 0x00, // consumption_term_id = 12
            0x00, 0x04, 0x00, 0x00, // consumption_term_offset = 1024
            0xFF, 0xFF, 0x3F, 0x00, // receiver_window = 4194303
            0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, 0x00, // receiver_id
        ];

        let frame = StatusMessageFrame::read(&bytes).expect("an SM frame");
        assert_eq!(
            frame,
            StatusMessageFrame {
                session_id: 10,
                stream_id: 11,
                consumption_term_id: 12,
                consumption_term_offset: 1024,
                receiver_window: 4_194_303,
                receiver_id: 0x0001_0203_0405_0607,
            }
        );
        assert_eq!(frame.group_tag(&bytes), None);

        let mut out = [0u8; StatusMessageFrame::LENGTH];
        frame
            .write_with_flags(&mut out, header_flags::SM_EOS)
            .expect("written");
        assert_eq!(out, bytes);
    }

    #[test]
    fn status_message_group_tag_is_the_optional_eight_bytes() {
        // 44 bytes: the fixed header with `frame_length` set to 36 + 8, then
        // the tag (`aeron_udp_protocol.c:31-38`).
        let mut bytes = [0u8; 44];
        bytes[..4].copy_from_slice(&44i32.to_le_bytes());
        bytes[5] = header_flags::SM_SEND_SETUP;
        bytes[6..8].copy_from_slice(&frame_type::SM.to_le_bytes());
        bytes[8..12].copy_from_slice(&10i32.to_le_bytes());
        bytes[12..16].copy_from_slice(&11i32.to_le_bytes());
        bytes[28..36].copy_from_slice(&0x0001_0203_0405_0607i64.to_le_bytes());
        bytes[36..44].copy_from_slice(&0x1112_1314_1516_1718i64.to_le_bytes());

        let frame = StatusMessageFrame::read(&bytes).expect("an SM frame");
        assert_eq!(frame.consumption_term_offset, 0);
        assert_eq!(frame.group_tag(&bytes), Some(0x1112_1314_1516_1718));

        // `frame_length` is what says whether the tag is there: the same bytes
        // with a fixed-header length claim no tag (`aeron_udp_protocol.c:33`).
        bytes[..4].copy_from_slice(&36i32.to_le_bytes());
        assert_eq!(frame.group_tag(&bytes), None);

        // A tag the datagram does not actually hold is not a tag.
        bytes[..4].copy_from_slice(&44i32.to_le_bytes());
        assert_eq!(frame.group_tag(&bytes[..40]), None);
    }

    #[test]
    fn error_frame_golden_vector_with_its_text() {
        let mut bytes = [0u8; 40 + 4];
        bytes[..4].copy_from_slice(&44i32.to_le_bytes());
        bytes[5] = header_flags::ERR_HAS_GROUP_TAG;
        bytes[6..8].copy_from_slice(&frame_type::ERR.to_le_bytes());
        bytes[8..12].copy_from_slice(&0x0102_0304i32.to_le_bytes());
        bytes[12..16].copy_from_slice(&0x0506_0708i32.to_le_bytes());
        bytes[16..24].copy_from_slice(&(-1i64).to_le_bytes());
        bytes[24..32].copy_from_slice(&0x0A0B_0C0D_0E0F_1011i64.to_le_bytes());
        bytes[32..36].copy_from_slice(&(-100i32).to_le_bytes());
        bytes[36..40].copy_from_slice(&4i32.to_le_bytes());
        bytes[40..44].copy_from_slice(b"boom");

        let frame = ErrorFrame::read(&bytes).expect("an ERR frame");
        assert_eq!(
            frame,
            ErrorFrame {
                session_id: 0x0102_0304,
                stream_id: 0x0506_0708,
                receiver_id: -1,
                group_tag: 0x0A0B_0C0D_0E0F_1011,
                error_code: -100,
                error_length: 4,
            }
        );
        // The text starts after all 40 bytes, which is the whole point of
        // `ErrorFrame::LENGTH` (`aeron_receive_channel_endpoint.c:491`).
        assert_eq!(frame.text(&bytes), Some(&b"boom"[..]));

        // `write` writes the fixed header with `frame_length` counted over the
        // text, and leaves the text to its caller
        // (`aeron_receive_channel_endpoint.c:480-491`).
        let mut out = [0u8; 40 + 4];
        frame
            .write_with_flags(&mut out, header_flags::ERR_HAS_GROUP_TAG)
            .expect("written");
        out[ErrorFrame::LENGTH..].copy_from_slice(b"boom");
        assert_eq!(out, bytes);

        let header = FrameHeader::read(&out).expect("a full header");
        assert_eq!(header.frame_length, 44);
    }

    #[test]
    fn error_text_is_bounded_by_the_buffer_and_the_structs_own_maximum() {
        let mut bytes = [0u8; 40 + 2];
        bytes[..4].copy_from_slice(&42i32.to_le_bytes());
        bytes[6..8].copy_from_slice(&frame_type::ERR.to_le_bytes());
        bytes[36..40].copy_from_slice(&2i32.to_le_bytes());
        bytes[40..42].copy_from_slice(b"hi");

        let mut frame = ErrorFrame::read(&bytes).expect("an ERR frame");
        assert_eq!(frame.text(&bytes), Some(&b"hi"[..]));

        // A length the datagram does not hold is not a truncated text.
        assert_eq!(frame.text(&bytes[..41]), None);

        // A negative length is refused before any slicing (`:666`).
        frame.error_length = -1;
        assert_eq!(frame.text(&bytes), None);

        // The reference caps the text at `AERON_ERROR_MAX_TEXT_LENGTH` too.
        frame.error_length = MAX_ERROR_TEXT_LENGTH + 1;
        assert_eq!(frame.text(&bytes), None);
    }

    #[test]
    fn rttm_frame_golden_vector() {
        let bytes: [u8; 40] = [
            0x28, 0x00, 0x00, 0x00, // frame_length = 40
            0x00, // version
            0x80, // flags = REPLY
            0x06, 0x00, // type = RTTM
            0x01, 0x00, 0x00, 0x00, // session_id = 1
            0x02, 0x00, 0x00, 0x00, // stream_id = 2
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // echo_timestamp = 0
            0x2A, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, // reception_delta
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // receiver_id = 1
        ];

        let frame = RttmFrame::read(&bytes).expect("an RTTM frame");
        assert_eq!(
            frame,
            RttmFrame {
                session_id: 1,
                stream_id: 2,
                echo_timestamp: 0,
                // 0x1_0000_002A: 8 bytes of delta whose low half is negative
                // when read alone, which is the mistake this vector is here to
                // catch.
                reception_delta: 0x1_0000_002A,
                receiver_id: 1,
            }
        );

        let mut out = [0u8; RttmFrame::LENGTH];
        frame
            .write_with_flags(&mut out, header_flags::RTTM_REPLY)
            .expect("written");
        assert_eq!(out, bytes);
    }

    #[test]
    fn a_negative_reception_delta_keeps_its_sign() {
        // The measured interval is subtracted, not compared, so a negative one
        // has to survive the wire (`aeron_publication_image.c:854`).
        let frame = RttmFrame {
            session_id: -1,
            stream_id: -2,
            echo_timestamp: i64::MIN,
            reception_delta: -16,
            receiver_id: i64::MAX,
        };

        let mut out = [0u8; RttmFrame::LENGTH];
        frame.write(&mut out).expect("written");
        assert_eq!(RttmFrame::read(&out), Some(frame));
    }

    #[test]
    fn every_reader_refuses_a_buffer_shorter_than_its_frame() {
        // Each entry is the type, the reader under test, and the length that
        // reader demands. The datagram is built to that length and then sliced:
        // a slice of 8 bytes or more carries a header of the right type, so a
        // reader that still refuses one is refusing it for its own reasons.
        let cases: [(&str, i16, Reader, usize); 6] = [
            (
                "DATA",
                frame_type::DATA,
                |bytes| DataFrame::read(bytes).is_some(),
                DataFrame::LENGTH,
            ),
            (
                "SETUP",
                frame_type::SETUP,
                |bytes| SetupFrame::read(bytes).is_some(),
                SetupFrame::LENGTH,
            ),
            (
                "NAK",
                frame_type::NAK,
                |bytes| NakFrame::read(bytes).is_some(),
                NakFrame::LENGTH,
            ),
            (
                "SM",
                frame_type::SM,
                |bytes| StatusMessageFrame::read(bytes).is_some(),
                StatusMessageFrame::LENGTH,
            ),
            (
                "ERR",
                frame_type::ERR,
                |bytes| ErrorFrame::read(bytes).is_some(),
                ErrorFrame::LENGTH,
            ),
            (
                "RTTM",
                frame_type::RTTM,
                |bytes| RttmFrame::read(bytes).is_some(),
                RttmFrame::LENGTH,
            ),
        ];

        for (name, kind, reads, length) in cases {
            let bytes = packet(kind, length as i32, length);
            assert!(reads(&bytes), "{name} read from its own length");

            for shorter in 0..length {
                assert!(
                    !reads(&bytes[..shorter]),
                    "{name} read from {shorter} bytes"
                );
            }
        }
    }

    #[test]
    fn every_writer_refuses_a_buffer_shorter_than_its_frame_and_writes_nothing() {
        let data = DataFrame {
            term_offset: 1,
            session_id: 2,
            stream_id: 3,
            term_id: 4,
            reserved_value: 5,
        };
        for shorter in 0..DataFrame::LENGTH {
            let mut out = [0xFFu8; 64];
            assert!(data.write(&mut out[..shorter]).is_none());
            assert!(out[..shorter].iter().all(|byte| *byte == 0xFF));
        }

        let setup = SetupFrame {
            term_offset: 1,
            session_id: 2,
            stream_id: 3,
            initial_term_id: 4,
            active_term_id: 5,
            term_length: 6,
            mtu: 7,
            ttl: 8,
        };
        for shorter in 0..SetupFrame::LENGTH {
            let mut out = [0xFFu8; 64];
            assert!(setup.write(&mut out[..shorter]).is_none());
            assert!(out[..shorter].iter().all(|byte| *byte == 0xFF));
        }

        let nak = NakFrame {
            session_id: 1,
            stream_id: 2,
            term_id: 3,
            term_offset: 4,
            length: 5,
        };
        for shorter in 0..NakFrame::LENGTH {
            let mut out = [0xFFu8; 64];
            assert!(nak.write(&mut out[..shorter]).is_none());
            assert!(out[..shorter].iter().all(|byte| *byte == 0xFF));
        }

        let sm = StatusMessageFrame {
            session_id: 1,
            stream_id: 2,
            consumption_term_id: 3,
            consumption_term_offset: 4,
            receiver_window: 5,
            receiver_id: 6,
        };
        for shorter in 0..StatusMessageFrame::LENGTH {
            let mut out = [0xFFu8; 64];
            assert!(sm.write(&mut out[..shorter]).is_none());
            assert!(out[..shorter].iter().all(|byte| *byte == 0xFF));
        }

        let err = ErrorFrame {
            session_id: 1,
            stream_id: 2,
            receiver_id: 3,
            group_tag: 4,
            error_code: 5,
            error_length: 0,
        };
        for shorter in 0..ErrorFrame::LENGTH {
            let mut out = [0xFFu8; 64];
            assert!(err.write(&mut out[..shorter]).is_none());
            assert!(out[..shorter].iter().all(|byte| *byte == 0xFF));
        }

        let rttm = RttmFrame {
            session_id: 1,
            stream_id: 2,
            echo_timestamp: 3,
            reception_delta: 4,
            receiver_id: 5,
        };
        for shorter in 0..RttmFrame::LENGTH {
            let mut out = [0xFFu8; 64];
            assert!(rttm.write(&mut out[..shorter]).is_none());
            assert!(out[..shorter].iter().all(|byte| *byte == 0xFF));
        }

        // And a header on its own obeys the same rule.
        let header = FrameHeader {
            frame_length: 32,
            version: VERSION,
            flags: 0,
            frame_type: frame_type::DATA,
        };
        let mut short = [0xFFu8; HEADER_LENGTH];
        for shorter in 0..HEADER_LENGTH {
            assert!(header.write(&mut short[..shorter]).is_none());
        }
    }

    #[test]
    fn a_reader_refuses_another_frame_type() {
        let data = packet(frame_type::DATA, 32, 32);
        let nak = packet(frame_type::NAK, NakFrame::LENGTH as i32, NakFrame::LENGTH);
        let pad = packet(frame_type::PAD, 32, 32);

        assert_eq!(NakFrame::read(&data), None);
        assert_eq!(DataFrame::read(&nak), None);
        assert_eq!(DataFrame::read(&pad), None, "PAD is not DATA");
        assert_eq!(StatusMessageFrame::read(&data), None);
        assert_eq!(SetupFrame::read(&data), None);
        assert_eq!(ErrorFrame::read(&data), None);
        assert_eq!(RttmFrame::read(&data), None);
    }

    #[test]
    fn padding_is_a_data_frame_and_nothing_else_is() {
        let of = |frame_type| FrameHeader {
            frame_type,
            ..Default::default()
        };

        assert!(of(frame_type::DATA).is_data());
        assert!(of(frame_type::PAD).is_data());

        for other in [
            frame_type::NAK,
            frame_type::SM,
            frame_type::ERR,
            frame_type::SETUP,
            frame_type::RTTM,
            frame_type::RES,
            frame_type::ATS_DATA,
            frame_type::ATS_SETUP,
            frame_type::ATS_SM,
            frame_type::RSP_SETUP,
            frame_type::EXT,
        ] {
            assert!(!of(other).is_data(), "type {other} is not a data frame");
        }
    }

    #[test]
    fn data_and_padding_packets_must_be_aligned_and_hold_a_data_header() {
        // `aeron_udp_protocol.h:237`: the alignment is a property of the
        // *packet*, and `frame_length` is not consulted at all.
        assert!(validity(frame_type::DATA, 32, 32));
        assert!(validity(frame_type::PAD, 32, 32));
        assert!(validity(frame_type::DATA, 32, 64));
        assert!(validity(frame_type::DATA, 64, 32));
        assert!(validity(frame_type::DATA, 1600, 1600));

        assert!(!validity(frame_type::DATA, 32, 33));
        assert!(!validity(frame_type::DATA, 32, 31));
        assert!(!validity(frame_type::PAD, 32, 33));
        assert!(!validity(frame_type::DATA, 32, 8), "no room for the frame");
        assert!(
            !validity(frame_type::DATA, -32, 32),
            "uncommitted in a term"
        );
    }

    #[test]
    fn other_frames_need_only_to_fit_their_floor_and_the_packet() {
        // `aeron_udp_protocol.h:243-258`
        assert!(validity(frame_type::NAK, 28, 28));
        assert!(!validity(frame_type::NAK, 28, 27));
        assert!(!validity(frame_type::NAK, 32, 28));

        assert!(validity(frame_type::SM, 36, 36));
        assert!(validity(frame_type::SM, 44, 44), "the group-tagged form");
        assert!(validity(frame_type::ATS_SM, 36, 36), "shares the SM case");
        assert!(!validity(frame_type::SM, 36, 35));
        assert!(!validity(frame_type::SM, 44, 36));

        assert!(validity(frame_type::ERR, 40, 40));
        assert!(validity(frame_type::ERR, 1063, 1063), "40 + 1023 of text");
        assert!(!validity(frame_type::ERR, 40, 39));
        assert!(!validity(frame_type::ERR, 44, 40));

        assert!(validity(frame_type::RTTM, 40, 40));
        assert!(!validity(frame_type::RTTM, 40, 39));
        assert!(!validity(frame_type::RTTM, 44, 40));

        // SETUP is the exception: an ATS setup carries a larger `frame_length`
        // than this struct describes, so there is no upper check (`:251`).
        assert!(validity(frame_type::SETUP, 40, 40));
        assert!(validity(frame_type::SETUP, 4000, 40));

        assert!(validity(frame_type::RES, 22, 22), "8 + 14 of resolution");
        assert!(!validity(frame_type::RES, 22, 21));
        assert!(!validity(frame_type::RES, 26, 22));

        assert!(validity(frame_type::RSP_SETUP, 20, 20));
        assert!(!validity(frame_type::RSP_SETUP, 20, 19));
        assert!(!validity(frame_type::RSP_SETUP, 24, 20));
    }

    #[test]
    fn the_envelope_is_checked_before_the_type_is() {
        // `aeron_udp_protocol.h:231-233`
        assert!(!validity(frame_type::NAK, 28, 7), "shorter than a header");
        assert!(!validity(frame_type::NAK, -1, 28), "negative frame_length");
        assert!(!validity(frame_type::DATA, -32, 32));
        assert!(!validity(frame_type::NAK, i32::MIN, 28));

        let bytes = header_bytes(frame_type::NAK, 28, 0);
        let mut wrong_version = bytes;
        wrong_version[4] = 1;
        let header = FrameHeader::read(&wrong_version).expect("a full header");
        assert_eq!(header.version, 1);
        assert!(!is_frame_valid(&header, 28));

        // Types with no case fall through to `return false` (`:259-260`).
        assert!(!validity(frame_type::ATS_DATA, 28, 28));
        assert!(!validity(frame_type::ATS_SETUP, 40, 40));
        assert!(!validity(frame_type::EXT, 28, 28));
        assert!(!validity(0x0C, 28, 28));
    }

    #[test]
    fn every_frame_round_trips_through_extreme_values() {
        for (index, &value) in I32_VALUES.iter().enumerate() {
            let next = I32_VALUES[(index + 1) % I32_VALUES.len()];
            let third = I32_VALUES[(index + 3) % I32_VALUES.len()];
            let fourth = I32_VALUES[(index + 7) % I32_VALUES.len()];
            let wide = I64_VALUES[index % I64_VALUES.len()];
            let narrower = I64_VALUES[(index + 4) % I64_VALUES.len()];

            let data = DataFrame {
                term_offset: value,
                session_id: next,
                stream_id: third,
                term_id: fourth,
                reserved_value: wide,
            };
            let mut out = [0u8; DataFrame::LENGTH];
            data.write_with_flags(&mut out, header_flags::BEGIN | header_flags::END)
                .expect("written");
            assert_eq!(DataFrame::read(&out), Some(data));
            assert_eq!(
                FrameHeader::read(&out).expect("a full header").flags,
                header_flags::UNFRAGMENTED
            );

            let setup = SetupFrame {
                term_offset: value,
                session_id: next,
                stream_id: third,
                initial_term_id: fourth,
                active_term_id: value,
                term_length: next,
                mtu: third,
                ttl: fourth,
            };
            let mut out = [0u8; SetupFrame::LENGTH];
            setup
                .write_with_flags(&mut out, header_flags::SETUP_GROUP)
                .expect("written");
            assert_eq!(SetupFrame::read(&out), Some(setup));

            let nak = NakFrame {
                session_id: value,
                stream_id: next,
                term_id: third,
                term_offset: fourth,
                length: value,
            };
            let mut out = [0u8; NakFrame::LENGTH];
            nak.write(&mut out).expect("written");
            assert_eq!(NakFrame::read(&out), Some(nak));

            let sm = StatusMessageFrame {
                session_id: value,
                stream_id: next,
                consumption_term_id: third,
                consumption_term_offset: fourth,
                receiver_window: value,
                receiver_id: narrower,
            };
            let mut out = [0u8; StatusMessageFrame::LENGTH];
            sm.write_with_flags(&mut out, header_flags::SM_SEND_SETUP | header_flags::SM_EOS)
                .expect("written");
            assert_eq!(StatusMessageFrame::read(&out), Some(sm));

            let err = ErrorFrame {
                session_id: value,
                stream_id: next,
                receiver_id: wide,
                group_tag: narrower,
                error_code: third,
                error_length: 0,
            };
            let mut out = [0u8; ErrorFrame::LENGTH];
            err.write_with_flags(&mut out, header_flags::ERR_HAS_GROUP_TAG)
                .expect("written");
            assert_eq!(ErrorFrame::read(&out), Some(err));

            let rttm = RttmFrame {
                session_id: value,
                stream_id: next,
                echo_timestamp: wide,
                reception_delta: narrower,
                receiver_id: wide,
            };
            let mut out = [0u8; RttmFrame::LENGTH];
            rttm.write_with_flags(&mut out, header_flags::RTTM_REPLY)
                .expect("written");
            assert_eq!(RttmFrame::read(&out), Some(rttm));

            let header = FrameHeader {
                frame_length: value,
                version: 0,
                flags: 0xFF,
                frame_type: third as i16,
            };
            let mut out = [0u8; HEADER_LENGTH];
            header.write(&mut out).expect("written");
            assert_eq!(FrameHeader::read(&out), Some(header));
        }
    }

    #[test]
    fn max_message_length_is_an_eighth_of_a_term_clamped_to_sixteen_mib() {
        // `aeron_udp_protocol.h:272-276`
        assert_eq!(compute_max_message_length(0), 0);
        assert_eq!(compute_max_message_length(1024 * 1024), 128 * 1024);
        assert_eq!(
            compute_max_message_length(64 * 1024 * 1024),
            8 * 1024 * 1024
        );
        assert_eq!(
            compute_max_message_length(8 * MAX_MESSAGE_LENGTH),
            MAX_MESSAGE_LENGTH,
            "exactly at the clamp"
        );
        assert_eq!(
            compute_max_message_length(1024 * 1024 * 1024),
            MAX_MESSAGE_LENGTH,
            "128 MiB of term is still clamped"
        );

        // The reference's `size_t` would turn a negative argument into a huge
        // value and clamp it; i32 keeps the sign. Pinned so the difference is a
        // decision rather than an accident.
        assert_eq!(compute_max_message_length(-8), -1);
        assert_eq!(compute_max_message_length(i32::MIN), i32::MIN >> 3);
    }
}
