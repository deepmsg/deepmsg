# Common tasks. `just` is optional; every recipe is a plain cargo invocation.

default:
    @just --list

# Build everything.
build:
    cargo build --workspace

# Run the standalone media driver (implemented in P1).
driver:
    cargo run -p deepmsg-driver --bin deepmsg-driver

# Check formatting (CI gate).
fmt:
    cargo fmt --all -- --check

# Apply formatting.
format:
    cargo fmt --all

# Lint the workspace (CI gate).
lint:
    cargo clippy --workspace --all-targets -- -D warnings

# Run unit and integration tests (interop suite excluded).
test:
    cargo test --workspace

# Run interop tests against the reference C driver (docs/reference.md).
interop:
    cargo test -p deepmsg-tests --features interop

# Run the micro-benchmarks (criterion).
bench:
    cargo bench -p deepmsg-bench --bench micro

# Measure a live round trip through a driver. Needs the release driver:
# `cargo build --release -p deepmsg-driver`. Extra arguments go after `--`.
bench-latency *args:
    cargo bench -p deepmsg-bench --bench latency -- {{args}}

# Build the three binaries a run batch measures, at the profile they are
# measured under. See bench/README.md.
bench-build:
    ./bench/build.sh

# The C driver arms against each other and the Java one. Long; `--list` first.
bench-drivers *args:
    ./bench/upstream-c-drivers/run-batch.sh {{args}}

# Two whole stacks, this build and the reference's. See bench/README.md for why
# this one cannot say which layer a difference came from.
bench-stacks *args:
    ./bench/rust-vs-java/run-batch.sh {{args}}

# Regenerate the SBE codecs from schemas/ (ADR-0004). Needs a JDK and the
# pinned sbe-tool/agrona jars; see `gen-sbe-codecs --help`.
gen *args:
    cargo run --quiet -p deepmsg-tools --bin gen-sbe-codecs -- {{args}}

# License and dependency audit.
deny:
    cargo deny check

# The fuzz suite is a workspace of its own (`fuzz/Cargo.toml`), which is why
# the recipes above do not reach it. Design: `analysis/fuzz/deepmsg-fuzz-plan.md`.

# Fill the corpora from the committed seeds and the golden fixtures.
fuzz-seed:
    ./fuzz/corpus_seed/seed_from_fixtures.sh

# One target, nightly: `just fuzz 05_udp_protocol_frames -- -runs=100000`.
fuzz target *args:
    cd fuzz && cargo +nightly fuzz run {{target}} -- {{args}}

# The fuzz workspace's stable gate: formatting, the frozen regressions, and
# that every target still compiles.
fuzz-gate:
    cd fuzz && cargo fmt --check && cargo test && cargo check --bins

# Every target for 60 seconds. A crash fails the recipe; artifacts are under
# `fuzz/artifacts/`.
fuzz-smoke:
    ./fuzz/corpus_seed/seed_from_fixtures.sh
    cd fuzz && for target in $(cargo +nightly fuzz list); do \
        echo "== $target"; \
        cargo +nightly fuzz run "$target" "corpus/$target" \
            -- -max_total_time=60 -timeout=2 -rss_limit_mb=2048 || exit 1; \
    done
