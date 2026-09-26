# deepmsg

A from-scratch Rust implementation of the
[Aeron](https://github.com/aeron-io/aeron) messaging system — client, media
driver and archive — built for low-latency trading workloads and byte-level
compatibility with Aeron **1.53.2**.

Status: **skeleton** (see `docs/roadmap.md`). Premium features (Cluster
Standby and friends) are explicitly out of scope for now; the seams they need
(cluster schemas, reserved counter ids, the `crates/cluster` placeholder) are
preserved so they can be added later without rework.

Principles:

1. **Compatibility is proven, not asserted.** The CnC file, the wire protocol
   and the archive on-disk formats follow the 1.53.2 reference; the interop
   suite (`tests/interop`, feature `interop`) diffs against the reference
   implementation (ADR-0001).
2. **Pure Rust, no async runtime, no FFI.** Threads with idle strategies,
   mirroring the reference agent model; hot paths stay allocation-free
   (ADR-0003).
3. **Unsafe has an address.** Raw shared-memory access lives only in
   `deepmsg-core::buffer`, `deepmsg-cnc`, and the driver's syscall shim
   (ADR-0002); every other crate forbids it outright.

## Layout

    crates/
      core/      protocol-agnostic foundation: buffers, lock-free structures,
                 log-buffer framing, counters, URI, versioning
      cnc/       the CnC file contract (client <-> driver shared memory)
      codec/     SBE codecs generated from schemas/ (ADR-0004)
      client/    publications, subscriptions, images, client conductor
      driver/    media driver: conductor, sender, receiver, media, loss,
                 flow control
      archive/   recording, replay, catalog (P2)
      cluster/   placeholder for the future cluster track (not a workspace
                 member yet)
      tools/     cnc-dump, errlog-dump, reclog-dump, deepmsg-stat
      bench/     micro-benchmarks + latency harness
    examples/    ping/pong, pub/sub, record/replay
    tests/       integration tests + interop suite (feature "interop")
    schemas/     SBE XML schemas forked from the reference tree (single
                 source of truth)
    docs/        ADRs, roadmap, compatibility matrix, protocol notes

## Building

    cargo build --workspace
    cargo test --workspace

Interop tests need the reference C driver from the Aeron 1.53.2 checkout
(`docs/reference.md` documents the expected sibling-directory layout):

    cargo test -p deepmsg-tests --features interop

## Conventions

- Domain vocabulary follows ADR-0005: Aeron's concept names are kept,
  byte-boundary names are frozen, and Rust code avoids C-era abbreviations.
  `GLOSSARY.md` maps every term to a plain-English definition.
- "The reference" means the Aeron 1.53.2 checkout expected at `../aeron`
  (C client + driver sources, Java archive + cluster).
- Source citations in code and docs use reference-repo-relative paths,
  sometimes with `:line` pinned to 1.53.2.

## License

Apache-2.0, matching the upstream license.
