#!/usr/bin/env bash
#
# The binaries a batch measures: the driver, the rig that drives it, and the
# nodes that answer — `echo-node` for the echo batches, `archive-node` for the
# archive one. Plus the archive's own two programs, which `archive-vs-java` runs
# in a process of their own (`deepmsg-archive` attaches to a driver somebody
# else runs; `deepmsg-archiving-media-driver` carries its own).
#
# Why the rig and the node are not just `cargo build --release`:
#
#   - Two of the settings cannot be given per package. Cargo refuses to parse
#     `lto` and `panic` inside `[profile.release.package.*]` — measured, not
#     read — so they come from the build invocation's environment instead.
#   - `target-cpu` is named rather than `native`. `native` would mean two
#     machines' builds of the same source differ with nothing recording which
#     was which, and the point of a batch is that it can be repeated.
#   - The rig and the node get their own target directory, so switching between
#     this build and an ordinary `cargo build --release` throws away neither
#     one's cache.
#
# The driver does not get that treatment: it is built by the workspace's own
# release profile, which is what `docs/benchmarks.md` has always reported it as.
# If a batch and the workspace's profile ever disagree about the driver, the
# digest in the fingerprint is what says so.
#
# Usage:  bench/build.sh
#
#   BENCH_TARGET_CPU   the CPU to compile for (default: skylake, the bench
#                      machine's i7-9700K). Named, never `native`.

set -eu

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo=$(dirname "$here")
cpu=${BENCH_TARGET_CPU:-skylake}

cd "$repo"

echo "== the driver and the archive (the workspace's release profile) =="
cargo build --release -p deepmsg-driver -p deepmsg-archive --bins

echo
echo "== the cluster node and its container (the cluster batch's node side) =="
echo "   the reference puts archive, consensus module and service in the node's"
echo "   JVM; these two are the same three roles on processes of their own."
cargo build --release -p deepmsg-cluster -p deepmsg-cluster-service --bins

echo
echo "== the counters reader (the archive batch explains a cell with it) =="
cargo build --release -p deepmsg-tools --bin cnc-dump

echo
echo "== the rig and the two nodes =="
echo "CARGO_TARGET_DIR=target/rig \\"
echo "CARGO_PROFILE_RELEASE_LTO=fat \\"
echo "CARGO_PROFILE_RELEASE_PANIC=abort \\"
echo "RUSTFLAGS=-C target-cpu=$cpu \\"
echo "cargo build --release -p deepmsg-bench --bin loadtest-rig --bin echo-node --bin archive-node"
echo

CARGO_TARGET_DIR=target/rig \
    CARGO_PROFILE_RELEASE_LTO=fat \
    CARGO_PROFILE_RELEASE_PANIC=abort \
    RUSTFLAGS="-C target-cpu=$cpu" \
    cargo build --release -p deepmsg-bench --bin loadtest-rig --bin echo-node --bin archive-node

echo
echo "== what a batch will use =="
ls -l target/release/deepmsg-driver target/release/deepmsg-archive \
    target/release/deepmsg-archiving-media-driver target/release/cnc-dump \
    target/release/deepmsg-cluster-node target/release/deepmsg-clustered-service-container \
    target/rig/release/loadtest-rig target/rig/release/echo-node target/rig/release/archive-node
echo
echo "== digests (these are what the fingerprint records) =="
sha256sum target/release/deepmsg-driver target/release/deepmsg-archive \
    target/release/deepmsg-archiving-media-driver target/release/cnc-dump \
    target/release/deepmsg-cluster-node target/release/deepmsg-clustered-service-container \
    target/rig/release/loadtest-rig target/rig/release/echo-node target/rig/release/archive-node
