# The distinct error log

The layout of one entry — its 24-byte header, the 8-byte alignment, and the
scan rule a reader walks — is [`cnc-layout.md`](cnc-layout.md)'s "Error log"
section. This file is the writer's half of the same region: what the text
holds, how sightings de-duplicate, and where the log stops.

Reference: `aeron-client/src/main/c/concurrent/aeron_distinct_error_log.{c,h}`
(writer `:60-204`, reader `:216-257`), plus
`aeron-client/src/main/c/util/aeron_error.c` for the composition. Rust:
`deepmsg_cnc::error_log`.

## The text is a composition, not a message

What a recording site passes is a message; what the log holds is the two-line
composition `AERON_SET_ERR` left in the thread's error buffer (`aeron_err_set`,
`aeron_error.c:351-375`, the entry formatting at `:326-334`):

```text
(<code>) <the code's description>
[<function>, <file>:<line>] <the message>
```

- The code appears as it was set. The driver's recording sites pass **negated**
  codes (`-AERON_ERROR_CODE_*`), so an entry a driver wrote begins `(-6)` and
  not `(6)`.
- The description is chosen by the **sign** of the code (`aeron_err_set`,
  `aeron_error.c:361-373`). At or below zero it is the row in
  `aeron_error_code_str` (`aeron_error.c:195-230`) — `unknown command type id`
  for 6, `insufficient storage space` for 12 — and a code with no row is
  `unknown error code`. Above zero it is the OS's own `strerror` text, because
  a positive code is an errno rather than one of the protocol's: a receiver
  refusing a sender's MTU raises `AERON_SET_ERR(EINVAL, …)` and the entry reads
  `(22) Invalid argument`, where the table's nearest answer would be wrong in
  the one place a reader looks.
- `<line>` is the line bearing the `AERON_SET_ERR` itself — the macro name's
  line, not a closing paren. Pinned empirically against a live 1.53.2 driver:
  unknown commands report `aeron_driver_conductor.c:3219`, malformed ones
  `:3232`, and the storage warning `aeron_driver_context.c:1370`.
- The composition ends in `\n`, and a long one is cut at byte 8186:
  `AERON_ERROR_MAX_TOTAL_LENGTH` is 8192 (`aeron_error.h:26`) and the
  `"...\n"` trailer is `strcpy`d six bytes from the end (`aeron_error.c:33`,
  `:348`).

The one driver-side recording that does **not** carry the composition is a
broadcast transmit failure. The reference appends it with `AERON_APPEND_ERR`
(`aeron_driver_conductor.c:2240`), which is the other half of
`aeron_err_update_entry`: no code's own line, no reset of the code, just the
site line and the message — so the entry reads
`[aeron_driver_conductor_client_transmit, aeron_driver_conductor.c:2240]
failed to transmit message`. That divergence is recorded in `docs/compat.md`.

- A recording site may append further lines after the composition — that is
  the other half of `AERON_APPEND_ERR` (`aeron_error.c:337-349`). The
  sender-MTU refusal is one: `aeron_receiver_channel_endpoint_validate_sender_mtu_length`
  raises it and the conductor's call site appends a second site line naming the
  stream and the session (`aeron_driver_conductor.c:6509`).
- How many appended lines there are is a property of **how many layers** the
  failure crossed, because each one appends as it unwinds. A subscription whose
  port cannot be bound is the longest this driver records: five —
  `aeron_bind` sets it (`aeron_socket.c:93`), then the transport
  (`aeron_udp_channel_transport.c:151`), the destination
  (`aeron_receive_destination.c:78`) and two frames of the conductor
  (`aeron_driver_conductor.c:2110`, `:5043`) add theirs. The last of them has an
  **empty message**: the site line alone says where it was.

Rust: `deepmsg_cnc::error_log::compose_description`, with the OS's text coming
through `deepmsg_core::pal::error_string`, and `ErrorReport` for the
accumulation — the reference's per-thread buffer as a value, since this build
has no thread-local and the buffer has to travel up five layers to be finished.
The whole of it is both what the log holds and what an `ON_ERROR` carries: one
composition with two readers. The golden texts are pinned by its
unit tests, by the interop suite's comparison of both drivers' entries through
the reference's own `ErrorStat`, and — for the appended-line shape — by
`tests/interop/mtu_fault_entry.rs`, which drives both drivers through the
sender-MTU refusal and compares the entries with the session id masked.

## De-duplication

Sightings count rather than write. The writer keeps a table in its own memory
— one row per distinct entry, holding the error code, the region offset, and
the description as it was first written (`aeron_distinct_error_log.c:41-46`,
`:50-66`) — and a sighting counts against a row when the codes are equal
**and** the remembered description is a prefix of the arriving one
(`:79-92`). The comparison is `strncmp` with the *stored* length, so
"failed to X" also counts "failed to X for reason Y", and the region keeps
the shorter text.

The scan over the table runs newest-first: the reference prepends new rows
(`:150`) and searches backwards, so a description that prefixes two
remembered ones counts against the newer.

The error code lives only in that table — the region holds no code
(`aeron_distinct_error_log.h:29-37`) — and that has two consequences. Two
entries with one text under different codes are indistinguishable to a
reader; and a restart forgets the distinction entirely, because the
initialiser stores `next_offset = 0` without reading anything back (`:52`),
so the same text seen by a new process writes a new entry rather than
counting.

## Ordering, and the unrecordable boundary

For a new entry, the description, the first-seen time and a zero count land
first with no ordering of their own; the header's `length` is stored
**last**, under a release, and that store is what publishes everything before
it (`:136-156`, the publish at `:156`). The count of one and the last-seen
time arrive after the publish (`:200-201`) — a reader can legitimately
observe a published entry whose count is still zero. A repeat sighting is a
fetch-add on the count and a store of the last-seen time (`:199-202`).

An entry that would not fit — `(offset + length) > capacity` (`:127`) — is
refused whole: no truncated description, no half entry. The caller prints to
stderr instead (`:185-191`), and the errors system counter is bumped all the
same (`aeron_driver_conductor.c:1203-1215`).

Rust: `DistinctErrorLog::record` and `RecordError::Unrecordable`.

## What a reader sees

`ErrorStat` prints each entry as `***`, a summary line with the observation
count and two dates, and then the description — its first line indented one
space by the print, the rest verbatim (`aeron-samples/src/main/c/error_stat.c:57-79`).
The dates are the entry's first- and last-seen times, and are the only part
of the output that is about the run rather than the log.
