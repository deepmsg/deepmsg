# Term buffer layout

A publication's log buffer: three terms, a page of metadata, and the frame
descriptor a producer writes into the terms. It is the second file the driver
and its clients share, after the CnC file, and like that one it has no
specification — the bytes are the contract.

Baseline: Aeron **1.53.2**, commit `664f58e705`. The authority for the
metadata struct is `aeron-client/src/main/c/concurrent/aeron_logbuffer_descriptor.h:42-88`,
for the file's length `:104-110`, and for the frame descriptor
`aeron-client/src/main/c/protocol/aeron_udp_protocol.h:23-62`. The
constants in `crates/core/src/logbuffer/descriptor.rs` assert their
relationships at compile time and their offsets in tests, and the two must
agree: a failure there means the layout moved.

## The file

```
<aeron_dir>/publications/<registration_id>.logbuffer
```

named after the **publication's** registration id
(`aeron-client/src/main/c/util/aeron_fileutil.c:1208-1214`), and

```
AERON_ALIGN((AERON_LOGBUFFER_PARTITION_COUNT * term_length) + AERON_LOGBUFFER_META_DATA_LENGTH, page_size)
```

bytes long: three terms, then a page of metadata, rounded up to the page size.

| Constant | Value | Source |
|---|---|---|
| partitions | 3 | `aeron_logbuffer_descriptor.h:27` |
| padding size | 64 | `:32` |
| metadata length | 4096 (one page) | `:90` (`AERON_PAGE_MIN_SIZE`) |
| frame alignment | 32 | `aeron_udp_protocol.h:188` (`AERON_FRAME_ALIGNMENT`) |
| default frame header | 128 (2 cache lines) | `:33` |
| page size | 4096 | `aeron-driver/src/main/c/aeron_driver_context.c:185` |
| IPC term length | 64 MiB | `aeron_driver_context.c:180` (`AERON_IPC_TERM_BUFFER_LENGTH_DEFAULT`) |
| IPC MTU | 1408 | `aeron_driver_context.c:187` (`AERON_IPC_MTU_LENGTH_DEFAULT`) |

**The metadata is the last page of the file, not the first.** A reader that
assumes the CnC file's layout — metadata at offset 0 — reads a term length of
zero. The two lengths in this file are also distinct and both correct:
`METADATA_LENGTH` is 4096 (the page) and `METADATA_STRUCT_LENGTH` is 508 (the
struct inside it); the remaining page holds the term tail counters' cache-line
padding and the default frame header's room.

## The metadata block

Offsets are absolute within the metadata page. The gaps are the reference's
own — it pads to cache-line boundaries with named arrays (`pad1`, `pad2`,
`pad3`) rather than leaving them implicit, which is why they are named here.

| Offset | Size | Field | Notes |
|---|---|---|---|
| 0 | 24 | `term_tail_counters[3]` | `(term_id << 32) \| term_offset`, one per partition |
| 24 | 4 | `active_term_count` | how many terms have carried data; picks the partition |
| 28 | 100 | `pad1` | to 128 |
| 128 | 8 | `end_of_stream_position` | `INT64_MAX` until the stream ends |
| 136 | 4 | `is_connected` | 1 while a publication has a reader |
| 140 | 4 | `active_transport_count` | network only |
| 144 | 112 | `pad2` | to 256 |
| 256 | 8 | `correlation_id` | the publication's registration id |
| 264 | 4 | `initial_term_id` | where the stream's terms start |
| 268 | 4 | `default_frame_header_length` | 32, the data header |
| 272 | 4 | `mtu_length` | the largest frame, header included |
| 276 | 4 | `term_length` | one term |
| 280 | 4 | `page_size` | the file's page |
| 284 | 4 | `publication_window_length` | how far ahead of its slowest reader a producer may run |
| 288 | 4 | `receiver_window_length` | an image's; 0 for a publication |
| 292 | 4 | `socket_sndbuf_length` | 0 for IPC — see below |
| 296 | 4 | `os_default_socket_sndbuf_length` | **the kernel's** — see below |
| 300 | 4 | `os_max_socket_sndbuf_length` | 0, deliberately |
| 304 | 4 | `socket_rcvbuf_length` | 0 for IPC |
| 308 | 4 | `os_default_socket_rcvbuf_length` | **the kernel's** |
| 312 | 4 | `os_max_socket_rcvbuf_length` | 0, deliberately |
| 316 | 4 | `max_resend` | 0 for IPC: nothing is retransmitted over shared memory |
| 320 | 128 | `default_header` | the frame header a producer copies for every frame |
| 448 | 8 | `entity_tag` | `-1` (`AERON_URI_INVALID_TAG`) unless `tags=` named one |
| 456 | 8 | `response_correlation_id` | `-1` unless the channel is a response channel |
| 464 | 8 | `linger_timeout_ns` | how long a drained publication lingers |
| 472 | 8 | `untethered_window_limit_timeout_ns` | |
| 480 | 8 | `untethered_resting_timeout_ns` | |
| 488 | 1 | `group` | |
| 489 | 1 | `is_response` | |
| 490 | 1 | `rejoin` | |
| 491 | 1 | `reliable` | |
| 492 | 1 | `sparse` | **true** by default (`aeron_driver_context.c:181`) |
| 493 | 1 | `signal_eos` | **true** by default |
| 494 | 1 | `spies_simulate_connection` | |
| 495 | 1 | `tether` | |
| 496 | 1 | `is_publication_revoked` | set by `REMOVE_PUBLICATION`'s revoke flag |
| 497 | 1 | `type` | 0 concurrent, 1 exclusive, 2 image |
| 498 | 2 | `pad3` | |
| 500 | 8 | `untethered_linger_timeout_ns` | **only 4-byte aligned** |

The last field is why this layout is a table of offsets rather than a Rust
struct: a `repr(C)` struct would place an `i64` at 504 and change the file.
`descriptor.rs`'s `the_unaligned_field_is_unaligned` test exists to say so.

## The socket buffer fields

Six fields, and they are not six zeroes:

- `socket_sndbuf_length` and `socket_rcvbuf_length` are **0** for an IPC
  publication. They describe a socket this publication does not have; a
  network publication writes the channel's configured buffers there
  (`aeron-driver/src/main/c/aeron_network_publication.c:210-214`).
- `os_default_socket_sndbuf_length` and `os_default_socket_rcvbuf_length` are
  **the machine's**, and they are in every log buffer whether it is IPC or
  not. The reference asks the kernel for them at start-up with a throwaway
  UDP socket (`aeron-client/src/main/c/util/aeron_netutil.c:883-919`) and
  passes the answer into the metadata init
  (`aeron-driver/src/main/c/aeron_ipc_publication.c:117-124`); deepmsg does
  the same in `deepmsg_driver::sys::default_socket_buffers`, so the bytes a
  reader finds are the bytes the reference would have written on that machine.
- `os_max_socket_sndbuf_length` and `os_max_socket_rcvbuf_length` are **0**,
  and that is also the reference's answer rather than a simplification: its
  context declares four buffer lengths and assigns only the two `default_*`
  ones (`aeron-driver/src/main/c/aeron_driver_context.h:408-413` declares the
  struct; `aeron_driver_context.c:1315-1316` fills the defaults and leaves the
  maxima at the zero the context was allocated with).

The consequence for testing is worth stating plainly: two of these fields
depend on the host's `net.core.{r,w}mem_default`, so a byte-for-byte
comparison against a golden file is only meaningful on one machine — or with
those two fields masked, the way `crates/driver/tests/system_counters.rs`
masks the build-identity labels.

## The frame descriptor

A data frame is a 32-byte header followed by its payload
(`aeron_udp_protocol.h:23-62`: `aeron_frame_header_t` at `:25-35`,
`aeron_data_header_t` at `:56-62`), and one function writes it: the producer's
`offer`.

| Offset | Size | Field |
|---|---|---|
| 0 | 4 | `frame_length` — 32 + payload, or **negative** while the frame is being written |
| 4 | 1 | `version` (0) |
| 5 | 1 | `flags` |
| 6 | 2 | `type` — 1 data, 8 ATS data, 0 padding |
| 8 | 4 | `term_offset` |
| 12 | 4 | `session_id` |
| 16 | 4 | `stream_id` |
| 20 | 4 | `term_id` |
| 24 | 8 | reserved |

Flags (`aeron_udp_protocol.h:190-195`):

| Bit | Name | Meaning |
|---|---|---|
| 0x80 | `BEGIN` | the first frame of a message |
| 0x40 | `END` | the last frame of a message |
| 0x20 | `EOS` | end of stream |
| 0x10 | `REVOKED` | the publication was revoked |

`BEGIN|END` together is `UNFRAGMENTED`: a message in one frame, and the only
shape a fragment handler may pass straight through without assembling.

A frame whose `frame_length` is negative has been claimed but not published: a
reader stops there and comes back, which is why the field is written twice,
each with a release store — negative in `aeron_publication_header_write`
(`aeron-client/src/main/c/aeron_publication.c:123-142`, whose other fields are
written in between) and the real length once the payload is in place
(`:245`, `:307`, `:365` for the three append paths).

A **padding** frame (`type` 0) covers the remainder of a term that is too
small for the next message, and it is written by the same function as any other
frame — the type is the only difference (`aeron_publication.c:157-159`) — so it
is indistinguishable from a normal frame to the layout: its `frame_length` is
the distance to the term's end. A reader
that has consumed it must move to the *next* term, and the writer's next frame
begins at offset 0 there.

## Position arithmetic

A position is a byte offset from the start of the stream, with the term id
packed into its high bits so that a reader can compute both without keeping
a term count:

```
position   = ((term_id - initial_term_id) << bits_to_shift) | term_offset
term_id    = initial_term_id + (position >> bits_to_shift)
term_offset = position & (term_length - 1)
```

`bits_to_shift` is `log2(term_length)` — which is why a term length has to be
a power of two, and why a log buffer whose metadata says otherwise is not a
log buffer.

The tail counter packs the same two values into one `i64` — `(term_id << 32) |
term_offset` — and there are three of them, indexed by
`(term_id - initial_term_id) mod 3`. A producer advances its term's tail with
a get-and-add of the frame's aligned length and only then publishes the frame;
the driver reads the current term's tail to find the producer's position.

A message that does not fit the remainder of a term is padded and written into
the next one — never split across terms, because a frame that spans two terms
cannot be written with one get-and-add. The fragment assembler's continuity
test is therefore always within one term: the next fragment of a message
begins at `ALIGN(term_offset + frame_length, 32)`, and a fragment that begins
anywhere else is a fragment whose predecessor is gone
(`aeron-client/src/main/c/aeron_subscription.c:587-593`).

## What each side owns

- **The driver** writes the whole metadata block when it creates the log
  buffer, and afterwards only `end_of_stream_position` (when a stream ends) and
  `is_connected` (when a reader comes or goes) plus the publication's two
  position counters, which live in the CnC file rather than here.
- **A producer** writes the default header's session and stream into frames it
  writes itself, and owns the tail counter of the term it is writing in.
- **A subscriber** writes nothing here at all: its position is a counter in the
  CnC file, and the log buffer is read-only to it.
