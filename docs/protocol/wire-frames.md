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

## The frames this build speaks

| Frame | Type | Length | Fields after the header |
|---|---|---|---|
| **DATA** | `0x01` | 32 | `term_offset` `i32`, `session_id` `i32`, `stream_id` `i32`, `term_id` `i32`, `reserved_value` `i64` |
| **NAK** | `0x02` | 28 | `session_id`, `stream_id`, `term_id`, `term_offset`, `length` — all `i32` |
| **SM** | `0x03` | 36 | `session_id`, `stream_id`, `consumption_term_id`, `consumption_term_offset`, `receiver_window` (`i32`), `receiver_id` (`i64`) |
| **ERR** | `0x04` | 40 + text | `session_id`, `stream_id`, `receiver_id` (`i64`), `group_tag` (`i64`), `error_code` `i32`, `error_length` `i32`, then `error_length` bytes of text |
| **SETUP** | `0x05` | 40 | `term_offset`, `session_id`, `stream_id`, `initial_term_id`, `active_term_id`, `term_length`, `mtu`, `ttl` — all `i32` |
| **RTTM** | `0x06` | 40 | `session_id`, `stream_id`, `echo_timestamp` (`i64`), `reception_delta` (`i64`), `receiver_id` (`i64`) |
| **RSP_SETUP** | `0x0B` | 20 | `session_id`, `stream_id`, `response_session_id` — all `i32` |

Four of these are worth a note each, because the obvious reading is wrong:

- **A heartbeat is a DATA frame whose `frame_length` is 0.** The packet is a
  whole data header (32 bytes) and the length says it carries nothing
  (`aeron_network_publication.c:463`). It is how a publisher keeps a stream
  alive without payload, and how it says the stream has ended.
- **ERR's `group_tag` is part of the fixed header**, written whether or not the
  `HAS_GROUP_TAG` flag says to believe it (`aeron_udp_protocol.h:102`); the
  text starts at offset 40.
- **RTTM's `reception_delta` is an `i64`** (`:114`) — an `i32` there would read
  any delta over two seconds as negative.
- **RSP_SETUP carries no correlation id.** Its `session_id` and `stream_id` are
  the *sender's own*, and they are how the far end finds the publication being
  answered; the correlation id never crosses the wire, each side reads it out of
  the publication it already holds (`aeron_send_channel_endpoint.c:738-748`).
  The `response_session_id` is the session the far end's subscription should
  adopt for the stream it is about to read
  (`aeron_driver_conductor.c:7100-7105`). Its flags byte is always zero and no
  bit of it is read (`aeron_receive_channel_endpoint.c:446`), so unlike the
  others it has no `write_with_flags`; and like a NAK it is only ever a
  datagram rather than a frame in a term, which is why its 20 bytes need not be
  a multiple of 32.

`RES` (`0x07`, name resolution) is defined in the reference and is **not** served
here; RSP_SETUP (`0x0B`, response channels) now is, and
`tests/interop/response_channel.rs` reads and writes it byte for byte.

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

## What a response channel adds

A response channel is two channels with one direction each: a
`control-mode=response` subscription, and a publication that names that
subscription's registration id as its `response-correlation-id=`. Its URI is
normally `control=` with **no** `endpoint=` (the reference's own samples use
`samples_configuration.h:34`), which is what keeps the two directions off each
other's port: the receiving destination binds an ephemeral port and the sending
endpoint binds `control`.

No frame type is new beyond RSP_SETUP; what is new is the order, and it is
short:

1. a response **subscription** builds no image from the SETUP it receives. It is
   registered with the receiver as a *response* stream rather than as interest
   in one (`aeron_driver_conductor.c:5072-5092`), so a SETUP that lands on it is
   answered the way traffic for a stream nobody reads is — with an SM carrying
   `SEND_SETUP` — rather than turned into an image;
2. a publication that carries `response-correlation-id=` says so in its SETUP
   with the `SEND_RESPONSE` bit, and the image built from that SETUP records it
   (`aeron_publication_image.h:346-349`). That record is what lets a **response
   publication** be created against the image
   (`aeron_driver_conductor.c:1787-1833`), and creating one is what gives the
   image a session id to answer with: an **RSP_SETUP** back down the control
   path;
3. the RSP_SETUP stops as soon as a status message from that publication
   arrives: the first live SM is what tells the image the pair is up
   (`aeron_driver_conductor.c:7117-7131`);
4. from then on DATA flows, and only to the address that SM came from
   (`aeron_network_publication.h:243-274`).

`tests/interop/response_channel.rs` walks the whole thing with hand-built
frames, and `tests/interop/response_channel_reference.rs` runs the reference's
own `response_client` and `response_server` against this driver.

## Where the golden tests live

`crates/driver/src/protocol.rs` holds one hand-built byte vector per frame,
asserted in both directions, plus the round trips over extreme values and the
`is_frame_valid` cases. `crates/driver/src/network_publication.rs` and
`crates/driver/src/publication_image.rs` assert the *use*: what a sender puts
in a datagram (the aligned frame, heartbeat lengths) and what a receiver
accepts.
