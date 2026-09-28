# The wire: UDP frames

The datagrams a media driver puts on a socket, byte for byte, as Aeron
**1.53.2** defines them. The authority is
`aeron-client/src/main/c/protocol/aeron_udp_protocol.h` — hand-written C
structs under `#pragma pack(4)`, **not** SBE, and not generated: the codec in
`crates/driver/src/protocol.rs` mirrors that header field for field, with a
byte-exact golden test per frame.

Two facts decide everything below.

- **Little-endian.** The reference assigns struct members and sends the buffer;
  it never byte-swaps, so the wire is the host's order and both ends are
  assumed little-endian. A big-endian host would have to swap on both sides.
- **A datagram is a *packet*, an image's term holds *frames*, and the sender
  sends the aligned frame.** A 100-byte payload is a 132-byte frame occupying
  160 bytes in the term, and what goes on the wire is those 160 bytes — which
  is why a packet's length is a multiple of 32 and why
  `aeron_publication_image_validate_packet` can reject a packet that does not
  end on a frame boundary (`aeron-driver/src/main/c/aeron_publication_image.c:645-735`).

## The frame header

Every frame begins with eight bytes (`aeron_udp_protocol.h:27-34`):

| Offset | Field | Type |
|---|---|---|
| 0 | `frame_length` | `i32` — the whole frame, header included. **Negative means uncommitted**: a producer writes a negative length and flips it after the payload lands, so a negative value read out of a *term* is a frame being written (`aeron_publication.c:132`). A receiver refuses it. |
| 4 | `version` | `i8` — always 0. A receiver drops anything else. |
| 5 | `flags` | `u8` — bits whose meaning depends on the type. |
| 6 | `type` | `i16` — see the table below. |

## The five families this build speaks

| Frame | Type | Length | Fields after the header |
|---|---|---|---|
| **DATA** | `0x01` | 32 | `term_offset` `i32`, `session_id` `i32`, `stream_id` `i32`, `term_id` `i32`, `reserved_value` `i64` |
| **NAK** | `0x02` | 28 | `session_id`, `stream_id`, `term_id`, `term_offset`, `length` — all `i32` |
| **SM** | `0x03` | 36 | `session_id`, `stream_id`, `consumption_term_id`, `consumption_term_offset`, `receiver_window` (`i32`), `receiver_id` (`i64`) |
| **ERR** | `0x04` | 40 + text | `session_id`, `stream_id`, `receiver_id` (`i64`), `group_tag` (`i64`), `error_code` `i32`, `error_length` `i32`, then `error_length` bytes of text |
| **SETUP** | `0x05` | 40 | `term_offset`, `session_id`, `stream_id`, `initial_term_id`, `active_term_id`, `term_length`, `mtu`, `ttl` — all `i32` |
| **RTTM** | `0x06` | 40 | `session_id`, `stream_id`, `echo_timestamp` (`i64`), `reception_delta` (`i64`), `receiver_id` (`i64`) |

Three of these are worth a note each, because the obvious reading is wrong:

- **A heartbeat is a DATA frame whose `frame_length` is 0.** The packet is a
  whole data header (32 bytes) and the length says it carries nothing
  (`aeron_network_publication.c:463`). It is how a publisher keeps a stream
  alive without payload, and how it says the stream has ended.
- **ERR's `group_tag` is part of the fixed header**, written whether or not the
  `HAS_GROUP_TAG` flag says to believe it (`aeron_udp_protocol.h:102`); the
  text starts at offset 40.
- **RTTM's `reception_delta` is an `i64`** (`:114`) — an `i32` there would read
  any delta over two seconds as negative.

`RES` (`0x07`, name resolution) and `RSP_SETUP` (`0x0B`, response channels) are
defined in the reference and not served here: P1-5 carries both.

## Flags

| Meaning | Value | Frame |
|---|---|---|
| `BEGIN` | `0x80` | DATA |
| `END` | `0x40` | DATA |
| `EOS` | `0x20` | DATA (a heartbeat that says the stream is over) |
| `REVOKED` | `0x10` | DATA (the publication was revoked) |
| `SEND_SETUP` | `0x80` | SM (asks the publisher for a SETUP) |
| `EOS` | `0x40` | SM (this receiver is going away) |
| `SEND_RESPONSE` | `0x80` | SETUP |
| `GROUP` | `0x40` | SETUP (the publisher is a group member) |
| `REPLY` | `0x80` | RTTM |

One flags byte carries seven meanings, so the names are prefixed where they
collide: `EOS` is `0x20` on a DATA frame and `0x40` on an SM.

`UNFRAGMENTED` is `BEGIN | END` — a frame that is a whole message.

## Validity

`aeron_is_frame_valid` (`aeron_udp_protocol.h:229-264`) is the receiver's gate,
and it is asymmetric on purpose:

- **DATA and PAD** must be at least 32 bytes **and** a 32-byte multiple of the
  *packet* — their own `frame_length` is not what is checked, because a
  heartbeat's is zero.
- **SETUP** needs a floor but no ceiling: ATS sends larger ones (`:251`).
- Everything else needs the floor **and** `frame_length <= packet_length`.
- An unlisted type — ATS, `EXT`, anything a future version adds — is invalid.

## What a subscription's traffic looks like

The sequence the interop tests exercise, in order:

1. the subscriber binds the channel's endpoint port and waits;
2. the publisher, having no receiver, sends **SETUP** every 100 ms
   (`aeron_network_publication.c:383-437`);
3. the subscriber learns the sender's address from that datagram and answers
   with an **SM** carrying `SEND_SETUP`, to `control` when the channel named one
   and to the *source* otherwise (`aeron_data_packet_dispatcher.c:616-659`);
4. a conductor on the receiving side builds a publication image from the
   SETUP, and the subscriber's SMs carry its consumption position and window —
   which is the publisher's entire flow control
   (`aeron_network_publication.c:779-840`);
5. **DATA** flows, and a gap is asked for with a **NAK** once it has survived
   the loss detector's delay (`aeron_loss_detector.h:96-126`);
6. a **zero-length DATA heartbeat** every 100 ms of silence, and an end-of-stream
   one when the stream ends.

## Where the golden tests live

`crates/driver/src/protocol.rs` holds one hand-built byte vector per frame,
asserted in both directions, plus the round trips over extreme values and the
`is_frame_valid` cases. `crates/driver/src/network_publication.rs` and
`crates/driver/src/publication_image.rs` assert the *use*: what a sender puts
in a datagram (the aligned frame, heartbeat lengths) and what a receiver
accepts.
