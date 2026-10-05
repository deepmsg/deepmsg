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

# Regenerate the SBE codecs from schemas/ (ADR-0004). Needs a JDK and the
# pinned sbe-tool/agrona jars; see `gen-sbe-codecs --help`.
gen *args:
    cargo run --quiet -p deepmsg-tools --bin gen-sbe-codecs -- {{args}}

# License and dependency audit.
deny:
    cargo deny check
