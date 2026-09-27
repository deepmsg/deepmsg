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
