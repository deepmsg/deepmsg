# `deepmsg` fuzz suite

An independent `cargo-fuzz` workspace (libFuzzer + Sanitizer). It is **not** a
member of the root workspace, so `cargo build/test --workspace` never downloads
or compiles `libfuzzer-sys`.

The design and the per-target contracts are in
`analysis/fuzz/deepmsg-fuzz-plan.md`.

## Targets

| Target | What it reads |
|---|---|
| `01_channel_uri` | the driver's `channel` strings; parse → build → parse |
| `02_logbuffer_scan` | a term's frames: `scan_for_availability`, the scanner, the fragment assembler |
| `03_cnc_rings_and_logs` | CnC header, driver ring, broadcast, counters, error log, loss report |
| `04_cnc_command_decode` | every control command decoder, plus encode→decode |
| `05_udp_protocol_frames` | UDP frames off a socket, resolution walk included |
| `06_userspace_packet` | Ethernet / IPv4 / UDP / ARP / ICMP bytes off a DPDK port |
| `07_sbe_archive_codecs` | every archive control request `dispatch` answers (34 templates) |
| `08_sbe_cluster_codecs` | every cluster template a production entry point decodes (groups included) |
| `09_archive_catalog_segment` | `archive.catalog` and a `.rec` segment (slow tier) |
| `10_archive_poller_events` | the client pollers and `RecordingEvents` dispatch |
| `11_cluster_recording_log` | `recording.log` and `node-state.dat` (slow tier) |
| `12_state_machine_rings` | structured claim/commit/abort/write sequences on the rings |
| `13_mark_files` | the archive and cluster mark files, read from a scratch path |

The slow tier (09, 11, 13) writes a fixed file under `/dev/shm` per iteration —
`$DEEPMSG_FUZZ_SCRATCH` overrides the directory. Run it with a smaller budget:

    cargo +nightly fuzz run 09_archive_catalog_segment -- -runs=2000 -max_len=65536 -timeout=5

## Prerequisites

    cargo install cargo-fuzz
    rustup toolchain install nightly          # libFuzzer needs nightly
    rustup component add miri --toolchain nightly   # only for the Miri tier

## Seeding and running

    ./corpus_seed/seed_from_fixtures.sh
    cargo +nightly fuzz run 01_channel_uri
    cargo +nightly fuzz run 01_channel_uri -- -runs=100000 -max_len=4096

Dictionaries live in `dictionaries/` (`channel_uri.dict` for target 01).

## When it finds a crash

1. Minimise: `cargo +nightly fuzz tmin <target> artifacts/<target>/crash-*`
2. Turn the minimised bytes into a normal `#[test]` in the production crate —
   the regressions fixed so far live in `crates/core/src/uri.rs`,
   `crates/core/src/logbuffer/{scan,position}.rs`,
   `crates/archive/src/{catalog.rs,server/control_adapter.rs,client/poller.rs,client/recording_events.rs}`,
   `crates/cluster/consensus/src/backup/agent.rs` and
   `crates/cnc/src/{ring,broadcast}.rs`.
3. Fix the production code; the unit test and the target must both go green.
4. Copy the bytes into `fuzz/tests/regression_artifacts.rs` so the stable gate
   keeps them even when `artifacts/` is cleaned.

## Gates

The root gates (`cargo fmt/clippy/test --workspace`) do not cover this
directory. Two tiers:

    # PR tier, stable: formatting, the frozen regressions, and that every
    # target still compiles
    cargo fmt --check && cargo test && cargo check --bins

    # nightly smoke: every target for 60 s
    ../just fuzz-smoke

`fuzz/Cargo.lock` is committed; the path dependencies are the root crates, so
there is no second copy of anything.

## Alignment

`AtomicBuffer` requires an 8-byte aligned base; libFuzzer hands over an
arbitrarily aligned slice. Targets that build one go through `src/lib.rs`'s
thread-local 64-byte scratch. Do not nest two of those helpers in one another.
