# Reference material

## Reference implementations (checked out next to this repo)

| Path | What it is | Used for |
|---|---|---|
| `../aeron` | Aeron 1.53.2 (C client + driver sources, Java archive + cluster) | source of truth for every byte-level decision |
| `../aeron-rs` | UnitedTraders pure-Rust client (CnC 0.0.16 era, compat ceiling ~1.44) | prior art; also a catalogue of pitfalls to avoid |

## The reference driver, for interop tests

The interop suite (`tests/interop`, feature `interop`) runs against a real
driver process, so it needs a built `aeronmd`:

    ../aeron/cppbuild/Release/binaries/aeronmd

`DEEPMSG_REF_AERONMD` overrides that path. The harness refuses a binary whose
`-v` output does not name Aeron 1.53.2 at `664f58e705`, which turns "the wrong
driver is on `PATH`" into a clear message rather than a confusing decode
failure deep inside a test.

Each test gets its own aeron directory under `/dev/shm`, and the driver is
started with `aeron.dir.delete.on.shutdown=true` so a clean signal removes it
again — that is what keeps a day of test runs from filling the tmpfs.

With no driver available the interop tests print a `SKIPPED` line and pass:
the feature is opt-in, and failing would make it unusable on any machine
without the checkout. The cost is real and worth stating plainly — a green run
that skipped verified nothing. What keeps that honest is
`tests/integration/cnc_fixture.rs`, which needs no checkout and does run in
CI; it decodes a committed header captured from a real driver.
