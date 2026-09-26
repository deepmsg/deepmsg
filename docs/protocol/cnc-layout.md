# CnC file layout

The `cnc.dat` file is the interface between a client and a media driver on one
host. It has no specification — the bytes are the contract — so everything
below cites the reference line it was read from, and the constants in
`crates/cnc/src/layout.rs` assert their relationships at compile time.

Baseline: Aeron **1.53.2**, commit `664f58e705`.

## Metadata region — 128 bytes

The metadata *struct* is 52 bytes; the metadata *region* is 128, and the
regions that follow begin at 128. Copying 52 bytes out of the file and calling
it the header produces numbers that look plausible and put every region at the
wrong offset.

`aeron-client/src/main/c/aeron_cnc_file_descriptor.h:28-43` (`#pragma pack(4)`),
offsets per `aeron-client/src/main/java/io/aeron/CncFileDescriptor.java:141-153`:

| Offset | Size | Name | Notes |
|---|---|---|---|
| 0 | 4 | `cnc_version` | volatile; **0 means "not published yet"** |
| 4 | 4 | `to_driver_buffer_length` | the MPSC command ring |
| 8 | 4 | `to_clients_buffer_length` | the broadcast region |
| 12 | 4 | `counter_metadata_buffer_length` | |
| 16 | 4 | `counter_values_buffer_length` | |
| 20 | 4 | `error_log_buffer_length` | |
| 24 | 8 | `client_liveness_timeout` | **nanoseconds** |
| 32 | 8 | `start_timestamp` | epoch **milliseconds** |
| 40 | 8 | `pid` | the driver process |
| 48 | 4 | `file_page_size` | may be 0 on a writer older than 1.48 |

`128 == 2 * AERON_CACHE_LINE_LENGTH` (`aeron_cnc_file_descriptor.h:57`), and the
cache line is hard-coded to 64 (`aeron-client/src/main/c/util/aeron_bitutil.h:33`).

> **64 is a byte contract, not an optimisation target.** Detecting the cache
> line size at runtime is the defect that shipped in `aeron-rs` on Apple
> Silicon, where a 128-byte detection silently computed a wrong layout.

## Version and acceptance

`cnc_version` is a packed semantic version: `(major << 16) | (minor << 8) | patch`
(`aeron-client/src/main/c/aeron_version.c:51-69`). `0.2.0` therefore reads as
**512** — not as a `(major, minor)` pair.

The reference applies two rules, and they come from different implementations:

| Rule | Source | Fatal? |
|---|---|---|
| major must match | `aeron-client/src/main/c/aeron_cnc_file_descriptor.c:103` | yes |
| the file's minor must be ≥ ours | `aeron-client/src/main/java/io/aeron/CommonContext.java:1432`, `aeron-client/src/main/c/aeron_context.c:617` | yes |

**The C reference is inconsistent with itself here.** Its *read* path checks
only the major; its *command* path (`aeron_context.c:617-624`, in
`aeron_context_request_driver_termination`) checks both, as does the Java
client. `deepmsg` applies both everywhere, which matches the C command path —
so this is not a case of being stricter than C, but of picking one of C's two
answers. `deepmsg_core::version::check_cnc_version` is the single place that
decides, and the negative-version case is a third such disagreement, recorded
in `tests/integration/cnc_terminate.rs`.

A driver newer than the client — a *higher* minor — is tolerated. Patch is
never compared.

### Why this needs synthetic tests

The reference driver always writes `0.2.0`, so the interop suite can only ever
exercise one branch of this table. The rules are unit-tested in
`crates/core/src/version.rs`, and the reason is worth keeping in mind before
"simplifying" those tests away.

## Readiness

`cnc_version != 0`, read **with an acquire**, is the single readiness gate.

The driver fills every other field first
(`aeron-driver/src/main/c/aeron_driver.c:252-260`) and only then stores the
version with a release
(`aeron-driver/src/main/c/aeron_driver_context.h:456`, called at
`aeron_driver.c:972`), before any agent thread starts. An acquire that observes
a non-zero version therefore orders every other field in the block, which is
why the reader can copy the whole 128 bytes afterwards and decode them as plain
little-endian.

A file of exactly 128 bytes is not enough: the reference requires *strictly*
more (`aeron_cnc_file_descriptor.c:70,88`).

## Regions

Five regions, contiguous, beginning immediately after the metadata region. No
padding between them. Only the **total** is aligned, to the writer's page size
(`aeron_cnc_file_descriptor.h:93-96`):

    file_length = align_up(128 + D + C + CM + CV + E, file_page_size)

| Region | Start | Length |
|---|---|---|
| to-driver (MPSC ring) | 128 | `D` |
| to-clients (broadcast) | 128 + D | `C` |
| counters metadata | 128 + D + C | `CM` |
| counters values | 128 + D + C + CM | `CV` |
| error log | 128 + D + C + CM + CV | `E` |

`CM >= 4 * CV` (`aeron-client/src/main/c/concurrent/aeron_counters_manager.h:25-29`,
validated with `>=` at `:100-101`).

The length fields are `int32` and a stale or corrupt block can carry a negative
one. The C reader survives that by casting to `size_t`, which wraps to a huge
value and makes its length check fail *by accident*; `deepmsg` rejects a
non-positive length before any arithmetic runs.

## Ring trailers

Both rings place their descriptor at the **end** of their region, after the
data area, so `capacity = region_length - TRAILER_LENGTH`.

**to-driver MPSC** — 768 bytes, twelve cache lines: a two-line leading pad,
then five `volatile int64` fields each padded out to a two-line block
(`aeron-client/src/main/c/concurrent/aeron_rb.h:24-40`).

| Offset in trailer | Field |
|---|---|
| 128 | `tail_position` |
| 256 | `head_cache_position` |
| 384 | `head_position` |
| 512 | `correlation_counter` |
| 640 | `consumer_heartbeat` |

`consumer_heartbeat` is the liveness signal: the driver is considered alive
while `now_ms - heartbeat <= driver_timeout_ms`
(`aeron-driver/src/main/c/aeron_driver_context.c:1625-1643`). There is no lock
file anywhere in the aeron-directory discipline. A driver shutting down
deliberately writes `AERON_NULL_VALUE` (`-1`) there
(`aeron-driver/src/main/c/aeron_driver_conductor.c:3493`), which is a different
signal from a stale timestamp.

**to-clients broadcast** — 128 bytes, three `volatile int64` with **no**
cache-line separation (`aeron-client/src/main/c/concurrent/aeron_broadcast_descriptor.h:22-49`):
`tail_intent_counter` @0, `tail_counter` @8, `latest_counter` @16.

`tail_intent_counter` is written *before* the record body and is therefore not
a publication barrier; it exists for lap detection only. `tail_counter` is
published after the body and is the completeness signal. `latest_counter` lags
`tail_counter` by exactly one record. A fresh reader starts at
`latest_counter` (`aeron_broadcast_receiver.c:47-52`); starting at
`tail_counter` would replay the whole ring.

**Record header**, both rings: `{ int32 length; int32 msg_type_id }`, 8 bytes.
The struct is `#pragma pack(4)`, so its *alignment* is 4; record *starts* are
8-aligned via `AERON_RB_ALIGNMENT` (`aeron-client/src/main/c/concurrent/aeron_rb.h:52`). `msg_type_id == -1` marks
a padding record; `length <= 0` ends a scan.

> The broadcast region is laid out here but has no reader in `deepmsg-cnc`. The
> reference's own `aeron_cnc_t` never constructs a broadcast receiver — that is
> the client conductor's job — so lap detection would have no consumer and no
> oracle until P0-b.

## Writing a command

A client sends a command by claiming a record in the to-driver ring and filling
it in place (`aeron-client/src/main/c/concurrent/aeron_mpsc_rb.c:143-202`). The claim
reserves `align_up(payload + 8, 8)` bytes of index space; the record is then:

| # | Store | Ordering |
|---|---|---|
| 1 | `length = -record_length` | release |
| 2 | payload | plain |
| 3 | `msg_type_id` | plain |
| 4 | `length = +record_length` | release |

**Step 4 is the publication.** A consumer acquire-loads the length and stops at
anything `<= 0`, so between 1 and 4 the record is invisible and everything after
it is correctly fenced off. The consumer never compares indices.

A record that would straddle the end of the ring is replaced by a **padding
record** — `msg_type_id = -1`, `length = capacity - tail_index` — and the
message restarts at index 0. The tail is advanced past the padding *before* the
padding header is published, so a consumer arriving in that window sees a zero
length and waits; `aeron_mpsc_rb_unblock` exists to break that stall if a
producer dies in it.

The producer writes exactly two descriptor fields: `tail_position`, by
compare-and-exchange, and `head_cache_position`. `head_position` and
`consumer_heartbeat` are the consumer's, and the producer must not zero
anything — the consumer zeroes what it consumes, and that ordering is what lets
a producer assume freshly claimed space starts zeroed.

### The command payload

`TERMINATE_DRIVER` (`aeron-client/src/main/c/command/aeron_control_protocol.h:40`, `0x0E`) carries
`{ int64 client_id; int64 correlation_id; int32 token_length; byte token[] }`
— 20 bytes plus the token (`:213-218`).

**The command type is in the record header's `msg_type_id`, not in the
payload.** The Java client puts a `commandTypeId` field inside its encoding, so
a port written from the Java side produces a payload four bytes too long that
the driver misparses.

Both correlation fields are filled from consecutive increments of the ring's
`correlation_counter`, and the driver never reads either one for this command.
They are ceremony the wire format requires — there is no reply to correlate.

## Counters

Metadata records are **512 bytes**, value records **128**, indexed identically
by counter id (`aeron-client/src/main/c/aeronc.h:849-870`).

| Metadata offset | Field |
|---|---|
| 0 | `state` — volatile; `0` unused, `1` allocated, `-1` reclaimed |
| 4 | `type_id` |
| 8 | `free_for_reuse_deadline_ms` |
| 16 | `key` (112 bytes, always read whole) |
| 128 | `label_length` (volatile) |
| 132 | `label` (380 bytes, **not** NUL-terminated) |

| Value offset | Field |
|---|---|
| 0 | `counter_value` (volatile) |
| 8 | `registration_id` (volatile) |
| 16 | `owner_id` |
| 24 | `reference_id` |

Enumeration mirrors `aeron-client/src/main/c/concurrent/aeron_counters_manager.c:284-321`:
stride 512 from index 0, report only `ALLOCATED`, **stop at the first `UNUSED`**
(counters are allocated densely from zero), and step over `RECLAIMED` without
reading its key, which reclamation zeroes non-atomically.

`max_counter_id = CV / 128 - 1` (`aeron_counters_manager.h:170`).

Two constants that sit next to each other in the header and mean different
things: `AERON_SYSTEM_COUNTER_ID_AERON_VERSION` is `34`, a counter's **slot**
(`aeron_counters.h:55`), while `AERON_COUNTER_SYSTEM_COUNTER_TYPE_ID` is `0`,
the **kind** every system counter carries (`aeron_counters.h:69`).

## Error log

Entries are `{ volatile int32 length; volatile int32 observation_count;
volatile int64 last_observation_timestamp; int64 first_observation_timestamp }`
— 24 bytes — followed by `length - 24` bytes of non-NUL-terminated text, padded
to 8 (`aeron-client/src/main/c/concurrent/aeron_distinct_error_log.h:27-42`).

Scan from offset 0, acquire-read `length`, **stop at the first zero**, advance
by `align_up(length, 8)`. An entry is appended once and thereafter only has its
counter and timestamp bumped (`aeron_distinct_error_log.c:199-202`).

## Ordering

The reference marks these fields `volatile` and wraps access in
`AERON_GET_ACQUIRE` / `AERON_SET_RELEASE`
(`aeron-client/src/main/c/concurrent/aeron_atomic64_gcc_x86_64.h:23,34`). Rust
has no `volatile`, so `deepmsg-core::buffer` names its accessors after the
*ordering* instead, and carries a field-by-field table binding each one to the
macro it mirrors. Where the reference reads a field "plainly", the Rust
equivalent is `Ordering::Relaxed` and **not** a non-atomic read: the bytes are
still being written by another process.

A reader must not:

- advance `head_position` or zero the MPSC ring — that is the driver's job as
  the ring's single consumer (`aeron-client/src/main/c/concurrent/aeron_mpsc_rb.c:242-246`);
- write any broadcast counter;
- assume a region's length is page-aligned, or that the metadata struct is as
  long as the metadata region.
