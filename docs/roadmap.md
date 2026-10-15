# Roadmap

Phases map to crates; acceptance is defined by the interop suite and the
reference tooling, never by prose.

| Phase | Scope (crates) | Acceptance | Reference sources |
|---|---|---|---|
| P0 | core, cnc, codec, client | deepmsg client against the reference C `aeronmd` 1.53.2: pub/sub roundtrip, counters, clean termination; CnC 0.2.0 negotiation verified | `aeron-client/src/main/c/` |
| P1 | driver | pure-Rust end-to-end (deepmsg client + deepmsg driver); reference C client against deepmsg driver; latency baseline in bench | `aeron-driver/src/main/c/` |
| P2 | archive | the reference and this build **swapped for one another**: LA-0…LA-5, below | `aeron-archive/src/main/java/`, `aeron-archive/src/main/c/` |
| P3 | cluster (placeholder) | out of scope for now; premium (Standby) deferred, seams preserved | `aeron-cluster/src/main/java/` |

P1's latency baseline is measured, not asserted: `docs/benchmarks.md` records
the three configurations (our client, the reference's own instrument on our
driver, and both on the reference's) with the machine they were taken on. A
number from a shared machine is not a gate, so the harness compiles in CI and
does not run there.

## P2's acceptance: LA-0…LA-5

The archive's criterion is not prose either. P2 is accepted when the reference and
this build can be **swapped for one another**, six ways, each with an instrument
that runs — the list is the reference's own definition of the job
(`analysis/archive/deepmsg-p2-plan-archive.md` §1), and what stands beside each one
is what this build measures today.

| | Which half is swapped | Instrument | Standing |
|---|---|---|---|
| LA-0 | the **driver**, under the reference's Java archive | the reference's Java system tests, pointed at this driver with `aeron.test.system.aeronmd.path` | P2-0c's entry check-up — a hard gate before the slice began |
| LA-1 | the **service**: the reference's C client against this archive | `analysis/archive/run-c4-acceptance.sh` — the reference's own C suite, pointed at this build | **306 cases, all passing** |
| LA-2 | the **client**: this build's archive client against the reference's Java `ArchivingMediaDriver` | `cargo test -p deepmsg-tests --features interop` | **341 passing, none failing** |
| LA-3 | the **disk**: an archive directory either build can open | the goldens in `crates/archive/tests/` (the reference's own writers made them), and `tests/interop/{mark_file,catalog}_reference.rs` (this build wrote the file, the reference's own `ArchiveTool` reads it) | both directions covered |
| LA-4 | the **configuration**: a reference deployment's properties, unchanged | the archive's own `aeron.archive.*` names | **not yet** — the names are P2-9c's, and are not all accepted |
| LA-5 | **mixed**: two archives replicating to each other | the reference's own replication cases | the reference as **source** runs (`tests/interop/replication_reference_archive.rs`) and this build both ways runs (the C suite's replication cases); the reference as **destination** is recorded as not done |

What P2 originally asked for — one stream recorded, replayed, in a catalog the
reference's `ArchiveTool` reads byte-identically — is the first milestone inside
LA-1 and LA-3 rather than a criterion beside them. The rows that do not stand yet
are named where they stand, not omitted: LA-4 is P2-9c, and the missing LA-5
pairing is a recorded gap in `analysis/archive/deepmsg-p2-plan-archive.md`.

Cross-cutting gates that apply from day one:

- `cargo clippy --workspace --all-targets -- -D warnings` stays clean.
- No `unsafe` outside the zones listed in ADR-0002.
- Schema drift vs `schemas/` must be reflected in `docs/compat.md`.
- Every byte-level claim cites a reference source (file, with `:line` where
  it matters).
