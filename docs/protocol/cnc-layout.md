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

## Who creates it

Only a media driver creates a CnC file. The reference has no client-side
creation path: `aeron_cnc_length` is a function of the *driver's* context
(`aeron-driver/src/main/c/aeron_driver_context.c:1690`), and a client only
ever maps a file that is already there (`aeron-client/src/main/c/aeron_context.c:220`
unmaps the one it was handed). A client that finds an aeron directory with no
`cnc.dat` in it is looking at a dead driver, not at a job to do.

Creation is exclusive — `open(O_RDWR|O_CREAT|O_EXCL, 0666)`
(`aeron-client/src/main/c/util/aeron_fileutil.c:967`) — so a driver pointed at
a directory another driver holds fails rather than sharing the file. What to
do about that directory is decided *before* the create, by the driver's
directory discipline (`aeron-driver/src/main/c/aeron_driver.c:136-235`): a
heartbeat inside the driver timeout means `EBUSY`, and anything else means the
directory is deleted and rebuilt.

The length is allocated rather than merely declared: the CnC file is mapped
with `fill_with_zeroes = true` (`aeron-driver.c:313`), which selects a
non-sparse file (`aeron_fileutil.c:1135`) and then touches every page
(`:1123-1132`). The fields are written plainly and the version is stored last
with a release (`aeron-driver.c:972`), then the whole file is flushed (`:973`).
deepmsg splits that into `CncFile::create` and `CncFile::publish`, because
there is work that belongs between them: the driver writes its first heartbeat
one line before the version (`:971`), and a client that passes the version
gate must not then find a heartbeat of zero. `publish` **enforces** that order
rather than documenting it: a file whose to-driver heartbeat is still zero is
refused, because the alternative is a file that every client reads as a driver
that has already gone.

Two rules bind a writer that a reader never has to know, because they are
about the *ring* rather than the region. The to-driver capacity — the region
length less its 768-byte trailer — must be a power of two, at least
`AERON_MPSC_RB_MIN_CAPACITY`, and below `INT32_MAX`
(`aeron-client/src/main/c/concurrent/aeron_rb.h:79-82`); the to-clients
capacity — the region less its 128-byte trailer — must be a power of two
(`concurrent/aeron_broadcast_descriptor.h:43`). A region length that is a
round number of mebibytes fails both, and the reference reports it only later,
from conductor init: `Invalid capacity: 2096384`
(`concurrent/aeron_mpsc_rb.c:37`).

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

**The transmitter is not the Java one.** The C writes a record's header once,
*before* the payload — `length` then `msg_type_id`, both plain stores — and
publishes with a single release store of `tail_counter`
(`concurrent/aeron_broadcast_transmitter.c:96-102`). There is no second
length-and-type-id write after the body and no per-record commit marker: a
reader that waits for one, as the Java reader does, waits forever. The only
ordering that is made explicit is the *tail intent*, which is raised with a
release and then a full fence (`:45-49`) **before** the record is touched,
because it is what a lapped reader measures itself against.

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

### Writing one

`deepmsg_cnc::CounterManager` is the write half, and the order it writes in is
the contract a reader depends on (`aeron_counters_manager.c:87-124`): `type_id`,
the reuse deadline, the key, the label, `label_length`, and then `state` with a
**release**. That release — and nothing else — is what makes a half-filled
record invisible. The value record is not touched by an allocation at all;
`registration_id`, `owner_id` and `reference_id` are written afterwards by
whoever owns the counter.

Two properties of the write side have no counterpart on the read side and are
easy to mistake for bugs:

- **The free list is in the driver's heap, not in the file.** A driver that
  restarts re-derives ids from a high-water mark and knows nothing of what was
  freed. The durable half of reclamation is `state = RECLAIMED` plus
  `free_for_reuse_deadline_ms`; nothing in the file marks a slot as "waiting".
- **Neither the key nor the label is cleared to its full width.** Only the bytes
  the length covers are written, so a recycled slot can show the previous
  tenant's text past the current label. A reader bounds itself by
  `label_length`; a writer must not "tidy" the tail.

`free` is the exception that proves the rule: it zeroes the *key*
(`:262-263`), because a reader is told never to look at a reclaimed record's
key, and leaves `type_id`, `label` and `label_length` exactly as they were.

### The position counters

Three counters are neither system counters nor a client's own: they are where
a driver and its clients keep a stream's positions, and they are the *only*
counters the IPC data plane needs.

| Type id | Name | Label | Source |
|---|---|---|---|
| 1 | `pub-lmt` | `pub-lmt: <registration> <session> <stream> <channel>` | `aeron-client/src/main/c/aeron_counters.h:71-72` |
| 4 | `sub-pos` | `sub-pos: <registration> <session> <stream> <channel> @<joining position>` | `:80-81` |
| 12 | `pub-pos` | `pub-pos (concurrent\|exclusive): <registration> <session> <stream> <channel>` | `:100-102` |

All three carry the same **112-byte key**
(`aeron-client/src/main/c/concurrent/aeron_counters_manager.h:37-47`):

| Offset | Size | Field |
|---|---|---|
| 0 | 8 | `registration_id` |
| 8 | 4 | `session_id` |
| 12 | 4 | `stream_id` |
| 16 | 4 | `channel_length` |
| 20 | 92 | channel |

Every field is written, including the channel's tail past the 92nd byte — the
reference builds the key as a designated initialiser
(`aeron-driver/src/main/c/aeron_position.c:41-42`), so what it does not name is
zero rather than whatever was on the stack. The label is the other way round:
it carries the channel **whole**, up to 380 bytes (`:35-39`), and its length
field is the label's own. A 200-byte URI therefore produces a label that shows
all of it and a key that shows the first 92 and says so — copying one length
into the other is a bug neither key nor label alone would reveal.

Which side writes which is the whole of the backpressure contract: the producer
writes `pub-pos` through the log's tail (and the driver writes it again from the
same place every duty cycle), the driver is the only writer of `pub-lmt`, and a
subscriber writes `sub-pos` — whereupon the driver recomputes `pub-lmt` from the
smallest of them (`aeron-driver/src/main/c/aeron_ipc_publication.c:278-328`).
A reader that reads without reporting eventually stops the producer, which is
the property `tests/interop/c_driver_pubsub.rs` asserts from the other side.

### The system counters

A driver allocates forty-six of them at conductor init
(`aeron-driver/src/main/c/aeron_system_counters.c:24-71`), all with type id `0`,
a four-byte little-endian **index** as the key, the index as their
`registration_id` and `NULL_VALUE` as their owner. The reference fails init if
the manager hands back any id other than the table index (`:99-107`), so they
must be the first thing a fresh file carries.

Their labels are interop-visible text, and two of them name a build:
`Errors: version=… commit=…` (id 15) and `Aeron software: …` (id 34). The
reference writes its own version text and git sha; **deepmsg writes the Aeron
version whose contracts it implements and its own identity**
(`deepmsg-<crate version>`). The divergence is deliberate — a file should not
claim to have been written by a build that does not exist — and it is why a
golden comparison of the two catalogues masks those two labels.

The reference also appends runtime context to eight labels
(`aeron_driver_conductor.c:848-951`): the resolver's name, the driver's
threading mode and the duty-cycle thresholds. deepmsg appends to three of them;
the sender, receiver and name-resolver counters keep their base labels, because
those agents are not built yet and a suffix naming the duty cycle of something
that never runs describes nothing.

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

The writer's half of the same region — what the text holds (a composition, not
a bare message), how sightings de-duplicate, and the unrecordable boundary —
is [`error-log-layout.md`](error-log-layout.md).

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

## Reading the responses

The to-clients ring is a **broadcast**, not a queue: every client reads the
whole stream, including responses addressed to other clients. That is the
opposite of the to-driver ring, which is many-to-one, and it decides how a
client has to behave.

`aeron-client/src/main/c/concurrent/aeron_broadcast_receiver.c`:

- **The cursor starts at `latest_counter`, not `tail_counter`.** `tail` is one
  *past* the newest record, so starting there would skip the only message a
  late joiner could have seen. There is no history and no way to ask for any.
- **One message per call.** Java's Agrona drains in a loop; the C does not, and
  neither does this. The caller's duty cycle is where timeouts, command
  processing and liveness checks live, and a drain loop would change how long
  each of them waits.
- **Copy first, validate after.** The check is `cursor + capacity >
  tail_intent_counter`, and it means "the writer has not yet announced a write
  past this slot". Doing it *before* the copy would guard nothing — the window
  that matters is the copy itself. The writer raises its intent before it
  destroys, so a validating load that does not see the flag proves the
  destructive write was not ordered before the copy. A failed validate
  **discards** the message; the cursor has already advanced and there is no
  retry.

A reader owns no descriptor field — unlike the MPSC consumer, which publishes
`head_position` and zeroes what it consumed. So a slow reader cannot block the
writer, and loss is discovered only after the fact. `lapped` counts *events*,
never messages: there is no way to know how many were missed.

### Matching

A response is matched to a request by the `correlation_id` the request carried,
which the driver echoes. A response matching nothing pending — another client's,
or one whose request already expired — is dropped silently, as the reference
does at every one of its handlers.

`ON_SUBSCRIPTION_READY` (`0x0F07`) is 12 bytes:
`{ int64 correlation_id; int32 channel_status_indicator_id }`. The status field
is a **counter id**, and it is `-1` for IPC and spy subscriptions because no
channel-status counter was allocated for them. Treating that as an error gets
IPC wrong.

A fresh client's *first* response is its own `ON_COUNTER_READY` (`0x0F08`), with
its **client id** in the correlation field — the driver emits it while creating
the client record, before any command's reply. Matching on the correlation id
rather than on arrival order is what makes that harmless.

### Staying alive

`aeron_driver_conductor_get_or_add_client` creates a client record on first
sight of a `client_id` — there is no registration command in this protocol —
and allocates a heartbeat counter for it: type id `11`, registration id equal
to the client id
(`aeron-driver/src/main/c/aeron_driver_conductor.c:994-998`).

The driver reaps that client once the counter's age exceeds
`aeron.client.liveness.timeout`, **10 s by default**, and reaping destroys every
subscription, publication and counter the client owned
(`aeron_driver_conductor.c:1038-1055`, `:1233-1283`). It announces the fact as
`ON_CLIENT_TIMEOUT` (`0x0F0A`).

A client writes that counter directly on every duty cycle — the reference does
the same (`aeron-client/src/main/c/aeron_client_conductor.c:1345-1387`) and does
*not* send `CLIENT_KEEPALIVE`, which the driver would ignore anyway for a client
it has not yet seen.

The counter does not exist until the first command reaches the driver, so a
client's first poll or two will not find it. That is normal and must not be
treated as an error.

### A leak in the reference, recorded rather than copied

When a command's deadline expires, the reference marks it timed out and sends
nothing (`aeron_client_conductor.c:1451-1481`). The driver keeps whatever it
created, so a subscription can exist that the client believes failed — and its
registration id, which is the correlation id, is the only handle on it. That is
why the timeout error here carries the correlation id.
