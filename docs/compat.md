# Compatibility matrix

deepmsg targets byte-level compatibility with Aeron **1.53.2** (ADR-0001).
Every row below is a testable contract; a row counts as verified only when
the interop suite (`tests/interop`, feature `interop`) or a golden byte test
covers it.

| Surface | Constant | Value | Reference source |
|---|---|---|---|
| CnC semantic version | `AERON_CNC_VERSION` | **0.2.0** | `aeron-client/src/main/c/aeron_cnc_file_descriptor.h:26` |
| Archive protocol SBE | schema id / version | 101 / 14 (semanticVersion 5.2) | `schemas/aeron-archive-codecs.xml` |
| Archive mark SBE | schema id / version | 100 / 2 (semanticVersion 5.2) | `schemas/aeron-archive-mark-codecs.xml` |
| Cluster protocol SBE | schema id / version | 111 / 17 (semanticVersion 5.4) | `schemas/aeron-cluster-codecs.xml` |
| Cluster mark SBE | schema id / version | 110 / 2 (semanticVersion 5.4) | `schemas/aeron-cluster-mark-codecs.xml` |
| Cluster node-state SBE | schema id / version | 112 / 10 | `schemas/aeron-cluster-node-state-codecs.xml` |
| Frame byte order | SBE `byteOrder` | little-endian; only the socket API converts (ports/addresses) | `schemas/*.xml` |
| Archive `recording.log` | — | hand-rolled: 48-byte entries, INVALID bit 31, 64-byte alignment, no file header | `aeron-cluster/src/main/java/io/aeron/cluster/RecordingLog.java` |

Rules:

- A CnC file is accepted when its major matches
  (`aeron-client/src/main/c/aeron_cnc_file_descriptor.c:103`) **and** its minor
  is >= ours (`aeron-client/src/main/java/io/aeron/CommonContext.java:1432`,
  `aeron-client/src/main/c/aeron_context.c:617`). The C reference disagrees
  with itself on the second rule — its read path omits it and its command path
  applies it — so `deepmsg` applies both everywhere and the choice is recorded
  in `docs/protocol/cnc-layout.md`. The rules are covered by unit tests in
  `crates/core/src/version.rs` and by
  `tests/integration/cnc_terminate.rs`; they have to be, because the reference
  driver only ever writes 0.2.0 and the interop suite can reach no other branch
  of the table.
- Wire-protocol constants (frame types, header layouts) live in
  `deepmsg-core::logbuffer` and `docs/protocol/` once migrated; this file
  tracks cross-component versions only.
- Changing anything in this table without extending the interop suite first
  is a review-blocking offence.

## Counters

Two of the forty-six system counter labels name the build that wrote the file:
`Errors: version=… commit=…` (id 15) and `Aeron software: …` (id 34)
(`aeron-driver/src/main/c/aeron_system_counters.c:41,60`). The reference writes
`AERON_VERSION_TXT` and its git sha. deepmsg writes `version=1.53.2` — the
version whose contracts it implements, and the same version its value field
packs as `79106` — and `commit=deepmsg-<crate version>`.

That is a recorded divergence rather than a copy, and it is deliberate: a git
sha is a fact about a build, the reference's own label changes from build to
build, and writing theirs would put a claim in the file that is not true. A
tool that *parses* the field (none in the reference does; `AeronStat` prints
it) would take `deepmsg-0.1.0` for a commit, which is the one honest thing it
could say.

The other labels the reference suffixes at runtime — the driver's threading
mode, the resolver's name, the duty-cycle thresholds
(`aeron_driver_conductor.c:848-951`) — are *configuration*, not contract: the
reference's own text changes with its settings. deepmsg appends `INVOKER` to
the conductor's two counters, matching the reference's word for a driver whose
agents are invoked by one thread, and leaves the sender, receiver and
name-resolver counters unsuffixed because those agents do not exist yet.

The consequence for testing: a golden comparison of the two catalogues
compares each label up to its first colon, and
`crates/driver/tests/system_counters.rs` asserts the masked halves separately
so that being *absent* cannot pass for being right.

## Driver liveness and the directory

Two drivers cannot share an aeron directory, and the question "is somebody else
living here?" is the to-driver ring's consumer heartbeat — there is no lock file
anywhere (`aeron-driver/src/main/c/aeron_driver.c:136-235`). Two rules around it
are worth recording, because deepmsg answers them the way the *contract* does
rather than the way the reference's code happens to:

- **A file whose version is still zero is not a dead driver.** The reference
  waits for the version to appear before it judges anything, up to
  `aeron.driver.timeout` (`aeron_is_driver_active_with_cnc`, `aeron_driver_context.c:1603-1612`),
  because a driver creating a 46 MB file looks exactly like a dead one. So does
  deepmsg. Past the window, a file that never got a version is taken over. The
  *wait* is measured with a monotonic clock rather than the reference's epoch
  clock: a duration measured against a clock a VM can step is a different
  duration after the step, and the failure mode of the short side is a live
  driver's directory.
- **A file this build may not *read* is not a file nobody owns.** The reference
  judges liveness by major version alone; deepmsg also applies the Java client's
  minor rule when reading a file. Where the two meet — a same-major, older-minor
  file — deepmsg refuses the directory (`BusyIncompatible`) instead of deleting
  it. The reference would find a fresh heartbeat there and refuse it too; the
  difference is only in which test it took to get there, and the cost of the
  wrong answer is a live driver's directory.

A CnC file is also *published* differently: `CncFile::publish` refuses to store
the version until the to-driver ring has a heartbeat, because the reference
writes the heartbeat first for a reason — a file that passes a client's version
gate and then fails its liveness rule is a file every client reads as a driver
that is already gone (`aeron-driver/src/main/c/aeron_driver.c:971-972`).

## The space a log buffer needs

Before a log buffer is created, the filesystem it will land on is asked
whether it has room — the reference's check, on by default
(`perform.storage.checks`, `aeron_driver_context.c:1354-1375`) — and a
refusal reaches the client as `STORAGE_SPACE`, the code the reference
composes for an `ENOSPC`, whether the check's or the kernel's
(`aeron_driver_conductor.c:2326-2341`). The setting's names and defaults
are pinned by `crates/driver/src/config.rs::the_storage_check_answers_to_the_references_names`,
and the refusal by the native resource agent's own tests.

The reference's check has a second half this build does not answer yet: at
or below `low.file.store.warning.threshold` it records a warning in the
driver's distinct error log, which this build has no counterpart of. The
setting is read and its default — ten of the reference's default term
lengths (`aeron_driver_context.c:179-183`) — is honoured by configuration
alone; the warning arrives with the error log that would record it.

## The client's view of the ring, and of a message

Four places where this build answers a question the reference answers
differently, all on the client's side. Each names the test that covers it.

- **A lap is counted, not fatal.** When the driver writes more events than the
  to-clients ring holds while a client is not reading, the events the client has
  not read are overwritten. The reference treats that as a system error and
  force-closes the client (`aeron-client/src/main/c/aeron_client_conductor.c:2728-2734`);
  deepmsg resynchronises forward, counts (`Client::laps`, `Client::discarded`)
  and passes the counts into a timeout's message, so a caller that sees a
  timeout can tell "the driver ignored me" from "my reply was overwritten".
  Covered by `crates/driver/src/conductor.rs::a_client_that_falls_a_ring_behind_counts_the_lap`
  and, for the receiver's own semantics, `crates/cnc/src/broadcast.rs::a_lap_resyncs_forwards_and_is_counted`.
- **One term per poll.** `Image::poll` fixes the partition at its entry and
  stops at that term's end, advancing into the next term on the *next* call —
  which is what `aeron_image_poll` does (`aeron-client/src/main/c/aeron_image.c:266-273`)
  and what a caller that throttles by counting fragments per poll depends on.
  Covered by `crates/driver/src/conductor.rs::a_poll_reads_one_term_at_a_time`.
- **An abandoned message is counted.** The fragment assembler drops a message
  whose fragments do not line up, as the reference does, and keeps a count of
  them (`FragmentAssembler::abandoned`) where the reference counts nothing
  (`aeron-client/src/main/c/aeron_fragment_assembler.c:170-181`). ADR-0003: a
  skipped input is a counted one. Covered by the assembler's own tests in
  `crates/client/src/fragment_assembler.rs`.
- **A delivered message is copied.** `Message::payload` points into the
  assembler's buffer, so every message is copied once; the reference hands out
  a pointer into the term for a message that arrived in one frame
  (`aeron_fragment_assembler.c:158-161`). A `&[u8]` over a term is a reference
  into memory a producer may be writing — in one process, in every test that
  publishes and subscribes at once — and `deepmsg-core`'s buffer API hands out
  no such slice for that reason. The copy is into a reused buffer, so a
  session's messages allocate nothing after the first. Covered by the same
  tests.

## Configuration names

The driver reads the reference's settings under both spellings: the property
name the reference documents, and deepmsg's prefix for the same name — so
`aeron.dir` and `deepmsg.dir` are the same setting, as a `-D` argument or in
the environment (`AERON_DIR` / `DEEPMSG_DIR`). Deepmsg's own spelling wins,
then its environment variable, then the reference's, which is the order a
one-off override wants. The suffixes are the reference's, including the three
environment names that are *not* the property name in capitals:
`aeron.to.conductor.buffer.length` is `AERON_CONDUCTOR_BUFFER_LENGTH`
(`aeronmd.h:97`), not `AERON_TO_CONDUCTOR_BUFFER_LENGTH`. A deployment's
existing configuration therefore works unchanged, which is the point.

The table is `crates/driver/src/config.rs`, and its tests pin every name; the
one divergence is recorded there too — the reference warns and clamps a value
it cannot parse, and this refuses.

`aeron.counters.free.to.reuse.timeout` follows the same rule and is one more
name whose environment variable is not the property name in capitals
(`AERON_COUNTERS_FREE_TO_REUSE_TIMEOUT`, `aeronmd.h:525`).

One setting has a bound the reference does not: `aeron.timer.interval` is
capped at one hour. The reference parses up to `INT64_MAX` and then adds the
period to a nanosecond clock (`aeron_driver_conductor.c:3379`), which a period
near `INT64_MAX` turns into a wrapped deadline — a driver that looks healthy and
spins a core. deepmsg computes its deadlines with saturating arithmetic *and*
refuses the value, because a tier period above an hour is a typo rather than a
configuration.
