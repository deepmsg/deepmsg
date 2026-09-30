# Roadmap

Phases map to crates; acceptance is defined by the interop suite and the
reference tooling, never by prose.

| Phase | Scope (crates) | Acceptance | Reference sources |
|---|---|---|---|
| P0 | core, cnc, codec, client | deepmsg client against the reference C `aeronmd` 1.53.2: pub/sub roundtrip, counters, clean termination; CnC 0.2.0 negotiation verified | `aeron-client/src/main/c/` |
| P1 | driver | pure-Rust end-to-end (deepmsg client + deepmsg driver); reference C client against deepmsg driver; latency baseline in bench | `aeron-driver/src/main/c/` |
| P2 | archive | record one stream + replay + a catalog the reference Java `ArchiveTool` reads byte-identically | `aeron-archive/src/main/java/`, `aeron-archive/src/main/c/` |
| P3 | cluster (placeholder) | out of scope for now; premium (Standby) deferred, seams preserved | `aeron-cluster/src/main/java/` |

P1's latency baseline is measured, not asserted: `docs/benchmarks.md` records
the three configurations (our client, the reference's own instrument on our
driver, and both on the reference's) with the machine they were taken on. A
number from a shared machine is not a gate, so the harness compiles in CI and
does not run there.

Cross-cutting gates that apply from day one:

- `cargo clippy --workspace --all-targets -- -D warnings` stays clean.
- No `unsafe` outside the zones listed in ADR-0002.
- Schema drift vs `schemas/` must be reflected in `docs/compat.md`.
- Every byte-level claim cites a reference source (file, with `:line` where
  it matters).
