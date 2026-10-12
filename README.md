# deepmsg

A from-scratch Rust implementation of the
[Aeron](https://github.com/aeron-io/aeron) messaging system — client, media
driver and archive — built for low-latency trading workloads and byte-level
compatibility with Aeron **1.53.2**.

## Where it is

- **The client and the media driver are complete, in Rust** (P0 and P1). The
  client runs against the reference C `aeronmd` 1.53.2 for real — CnC
  negotiation, liveness, payloads over `aeron:ipc` — and `deepmsg-driver` is a
  media driver: it writes the CnC file byte for byte as a real one is written,
  runs the conductor's duty cycle, and speaks UDP both ways (SETUP, DATA,
  heartbeats, loss detection and retransmission, `max` flow control, fan-out,
  response channels, spies). The acceptance is two-sided and runs against the
  reference in both directions, in the interop suite.
  What is left is the reference's own system-test suite: `.github/workflows/interop.yml`
  runs its 72 classes against this driver and is **expected to be red** — a red
  run there is a list of known gaps, class by class, not a broken build, and it
  is the work queue rather than a summary. `docs/compat.md` lists every
  divergence with the test that pins it.
- **The load test rig is complete, in Rust** (`crates/bench`). `loadtest-rig` is
  a port of the reference harness's `LoadTestRig` semantics — the same rate
  control, the same pipeline, the same `expected == sent == received` verdict,
  the same interval-log result files — and `echo-node` is the far end of the
  same measurement. Both run under `bench/`, the two batches that measure this
  build against the reference's own benchmark repository.
- **The archive is nearly complete** (P2). Recording, replay, replication and
  the catalog are implemented, with a server and a client whose command set is
  the reference's own (start/stop recording, list, replicate, truncate, purge,
  extend, segment attach/detach/delete/purge, find-last-matching, the replay
  family, positions, recording subscriptions). The on-disk formats and the SBE
  control protocol are pinned against the reference's Java archive in
  `tests/interop` and `crates/archive/tests`.
- **The cluster is not started** (P3). `crates/cluster` is a placeholder and the
  seams it will need — cluster schemas, reserved counter ids — are preserved so
  that it can be added without rework. Premium features (Cluster Standby and
  friends) are explicitly out of scope for now.

## Where it stands

Two batches under `bench/` measure this build against the reference's own
benchmark harness (`aeron-io/benchmarks`) — a rate-controlled load with the
harness's own echo client and node — rather than against a `Ping`/`Pong` pair,
on an 8-core Intel i7-9700K running Ubuntu 24.04 with gcc 13.3.0. Both are read as
`E[X | X < p99]`, two rounds with the order reversed, against a floor measured
in the same session. The record — the command that produced each table, the
harness revision, the digest of every binary, the core placement — is in
`docs/benchmarks.md`, which is the file to quote from; the numbers here are the
headline and nothing else.

The media driver, one layer swapped (this is also the evidence for three C
driver changes this work is sending upstream):

| ratio | 101K:1344 | 301K:288 | 501K:32 |
|---|---|---|---|
| C driver with the changes / the same C driver without | 0.90 / 0.89 | 0.94 / 0.95 | 0.92 / 0.92 |
| C driver with the changes / the Java driver | 0.96 / 0.96 | 0.96 / 0.97 | 0.97 / 0.97 |
| the C driver without them / the Java driver | 1.07 / 1.07 | 1.03 / 1.02 | 1.05 / 1.05 |

All nine ratios agree in sign across the reversal: the C driver's receive path
was 2.3–7.1% slower than the Java driver's, is 2.9–4.4% faster with the three
changes, and the changes are worth 5.1–10.8% against the unpatched build of the
same source.

The whole stack, this build against the reference's (`rust / java`):

| tier | round 1 | round 2 | |
|---|---|---|---|
| 25K:32 | 0.983 | 0.969 | same sign |
| 101K:1344 | 1.011 | 0.987 | disagree |
| 301K:288 | 0.981 | 0.982 | same sign |
| 501K:32 | 0.996 | 0.995 | same sign |

At 301K:288 this build's stack is 1.8% faster in both rounds, well outside that
tier's floor; at 25K:32 and 501K:32 the difference is one to two times its own
floor, which is to say it can be argued either way; at 101K:1344 the two rounds
disagree in sign, so nothing is read there. This batch swaps whole stacks and
cannot say which layer a difference came from. One quantity in it is a known
unknown and is named as such in `docs/benchmarks.md`: the Java side is the
harness's own Aeron 1.53.3, while this build targets 1.53.2.

## Principles

1. **Compatibility is proven, not asserted.** The CnC file, the wire protocol
   and the archive on-disk formats follow the 1.53.2 reference; the interop
   suite (`tests/interop`, feature `interop`) diffs against the reference
   implementation (ADR-0001).
2. **Pure Rust by default, no async runtime.** Threads with idle strategies,
   mirroring the reference agent model; hot paths stay allocation-free
   (ADR-0003). FFI is not a red line: the operating system's own interfaces are
   reachable no other way, so `libc` carries the memory-mapping seam today and
   the batching syscalls later, and a kernel-bypass transport would be FFI
   throughout. Each use is an exception that names what it buys — never the
   default, and never a substitute for a Rust implementation that would do.
3. **Unsafe has an address.** Raw memory and syscall access lives only in
   ADR-0002's five named zones — `deepmsg-core::pal`, `deepmsg-core::buffer`,
   `deepmsg-cnc`, the driver's syscall shim, and the archive shim — and each
   zone is a module a reviewer reads; every other crate forbids it outright.

## Layout

    bench/       the two run batches that measure this build against the
                 reference's benchmark harness (see bench/README.md)
    crates/
      core/      protocol-agnostic foundation: buffers, lock-free structures,
                 log-buffer framing, counters, URI, versioning
      cnc/       the CnC file contract (client <-> driver shared memory)
      codec/     the SBE codecs (ADR-0004): this crate is the façade over five
                 generated crates in the directories inside it — archive/,
                 archive-mark/, cluster/, cluster-mark/, cluster-node-state/
      client/    publications, subscriptions, images, client conductor
      driver/    media driver: conductor, sender, receiver, media, loss,
                 flow control
      archive/   recording, replay, replication, catalog, and the archive's own
                 server and client
      cluster/   placeholder for the future cluster track (not a workspace
                 member yet)
      tools/     cnc-dump, errlog-dump, reclog-dump, deepmsg-stat, and the
                 archive shim the reference's archive suite is pointed at
                 (docs/reference.md)
      bench/     the rig, the echo node, the micro-benchmarks and the latency
                 harness
    examples/    ping/pong, pub/sub, record/replay
    tests/       integration tests + interop suite (feature "interop")
    schemas/     SBE XML schemas forked from the reference tree (single
                 source of truth)
    docs/        ADRs, roadmap, compatibility matrix, protocol notes,
                 benchmarks

## Building

    cargo build --workspace
    cargo test --workspace

Interop tests need the reference C driver from the Aeron 1.53.2 checkout
(`docs/reference.md` documents the expected sibling-directory layout):

    cargo test -p deepmsg-tests --features interop

Benchmarks are snapshots rather than gates. `docs/benchmarks.md` holds the
numbers, the machine they were taken on, and what they do not say; `bench/`
holds the batches that produce them, and needs two checkouts beside this one
(`bench/README.md` names them):

    cargo bench -p deepmsg-bench --bench micro
    bench/build.sh
    bench/rust-vs-java/run-batch.sh --list
    bench/upstream-c-drivers/run-batch.sh --list

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
