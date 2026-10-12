#!/usr/bin/env bash
#
# Where the pieces of a batch live, and what to say when one is missing.
#
# A batch under this tree measures two or three software stacks against each
# other, and one of them is not in this repository: the reference's benchmark
# harness is a checkout of `aeron-io/benchmarks`, and the C driver it drives is
# built out of a checkout of `aeron-io/aeron`. Neither is vendored here, so every
# path that is not ours is an environment variable with a default that is a
# sibling directory — the layout these batches were developed in.
#
# Sourced, never executed. It sets `BENCH_*` paths and defines `need_*`.
#
#   DEEPMSG_DIR       this repository's checkout (default: two levels up)
#   BENCHMARKS_DIR    the reference harness (default: ../benchmarks)
#   AERON_REF_DIR     the 1.53.2 reference checkout, the frozen one
#                     (default: ../aeron). Read by the fingerprint and by the
#                     build of the *stock* C driver; never written to.
#   AERON_FORK_DIR    the upstream work area — a clone of our fork of aeron,
#                     where the commits a batch is evidence for live
#                     (default: ../aeron-deepmsg). Only `upstream-c-drivers`
#                     reads it.
#   BENCH_OUT_DIR     where results land (default: <repo>/bench/out)
#
# `BENCH_DRIVER` is the built release driver, and the rig and node are the ones
# `bench/build.sh` produces — see `bench/README.md` for why they are built
# apart from the workspace's own profile.

if [ -z "${BASH_VERSION:-}" ]; then
    echo "checkouts.sh needs bash" >&2
    exit 2
fi

BENCH_LIB="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_DIR="$(dirname "$BENCH_LIB")"
REPO_ROOT="$(dirname "$BENCH_DIR")"

DEEPMSG_DIR="${DEEPMSG_DIR:-$REPO_ROOT}"
BENCHMARKS_DIR="${BENCHMARKS_DIR:-$(dirname "$REPO_ROOT")/benchmarks}"
AERON_REF_DIR="${AERON_REF_DIR:-$(dirname "$REPO_ROOT")/aeron}"
AERON_FORK_DIR="${AERON_FORK_DIR:-$(dirname "$REPO_ROOT")/aeron-deepmsg}"
BENCH_OUT_DIR="${BENCH_OUT_DIR:-$BENCH_DIR/out}"

# The reference's own driver: the Java one is a script that runs
# `io.aeron.driver.MediaDriver`, the C one execs the `aeronmd` beside it.
BENCHMARKS_DRIVER_JAVA="$BENCHMARKS_DIR/scripts/aeron/media-driver"
BENCHMARKS_DRIVER_C="$BENCHMARKS_DIR/scripts/aeron/c-media-driver"
BENCHMARKS_ECHO_SERVER="$BENCHMARKS_DIR/scripts/aeron/echo-server"
BENCHMARKS_ECHO_CLIENT="$BENCHMARKS_DIR/scripts/aeron/echo-client"
BENCHMARKS_SCRIPTS_DIR="$BENCHMARKS_DIR/scripts/aeron"

# This build's two halves of a measurement.
BENCH_DRIVER="${BENCH_DRIVER:-$DEEPMSG_DIR/target/release/deepmsg-driver}"
BENCH_RIG="${BENCH_RIG:-$DEEPMSG_DIR/target/rig/release/loadtest-rig}"
BENCH_NODE="${BENCH_NODE:-$DEEPMSG_DIR/target/rig/release/echo-node}"

BENCH_AERON_DIR="${BENCH_AERON_DIR:-/dev/shm/aeron}"

# The cores. The driver pins its own three out of the properties file it is
# given; these two are the ones a batch has to place itself.
BENCH_NODE_CPU="${BENCH_NODE_CPU:-4}"
BENCH_RIG_CPU="${BENCH_RIG_CPU:-5}"

need_dir() {
    local what=$1 path=$2
    if [ ! -d "$path" ]; then
        echo "no $what at $path" >&2
        echo "  set the environment variable named beside it in bench/lib/checkouts.sh" >&2
        return 1
    fi
}

need_file() {
    local what=$1 path=$2 hint=${3:-}
    if [ ! -x "$path" ] && [ ! -f "$path" ]; then
        echo "no $what at $path" >&2
        [ -n "$hint" ] && echo "  $hint" >&2
        return 1
    fi
}

# Says what a batch is about to use, before it uses it. Every batch prints this
# into its own log: a table without the digests that produced it is an anecdote.
bench_checkouts_report() {
    cat <<EOF
benchmark harness   $BENCHMARKS_DIR
reference checkout  $AERON_REF_DIR
upstream work area  $AERON_FORK_DIR
this build          $DEEPMSG_DIR
results under       $BENCH_OUT_DIR
EOF
}
