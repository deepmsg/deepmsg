#!/usr/bin/env bash
#
# One batch: this build's whole stack against the reference's.
#
#   run-batch.sh [--arms rust,java] [--pairs 25K:32,...] [--runs 1]
#                [--iterations 60] [--warmup-iterations 30] [--out DIR]
#                [--list] [--quick] [--no-read]
#
# Two stacks, each of them whole:
#
#   java   the reference's harness end to end — its `media-driver` (the Java
#          driver), its `echo-client` (the Java `LoadTestRig` over
#          `EchoMessageTransceiver`) and its `echo-server` (the Java node).
#   rust   this build end to end — `deepmsg-driver`, the Rust rig
#          (`loadtest-rig`, a port of the same `LoadTestRig` semantics) and this
#          build's `echo-node`.
#
# Nothing is swapped inside either arm, and that is the point: this is the only
# batch whose reading is a whole system against a whole system, so it is also the
# only one that cannot say which layer a difference came from. The layered
# batches are the ones that can (see `bench/README.md`), and a number from here
# may not be used as if it were one of theirs.
#
# **Both sides write the same result files**, which is what makes the pair
# readable at all: the harness's `PersistedHistogram` and this build's
# `loadtest/result.rs` write the same HdrHistogram interval log, and
# `bench/tools/quantiles.sh` reads both with the harness's own decoder. The
# client's own OK/WARNING line is the same contract on both sides too
# (`expected == sent == received`).
#
# Each round visits the arms in the order given, and the second round reverses
# it, so a difference that follows the machine rather than the arm shows up as a
# sign flip. The tiers default to the four this tree's own work is read at;
# `--pairs` takes any list.
#
# The driver settings are the aligned ones (`lib/conf/benchmark.properties`):
# three throughput knobs this build has no setting for are pinned to what it
# hard-codes, and the same file is handed to the Java driver, so that what is
# being compared is two stacks and not two configurations. A cell run on the
# aligned values may not be read beside one that is not.
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
BENCH_DIR=$(dirname "$here")
# shellcheck source=../lib/checkouts.sh
. "$BENCH_DIR/lib/checkouts.sh"
# shellcheck source=../lib/fingerprint.sh
. "$BENCH_DIR/lib/fingerprint.sh"
# shellcheck source=../lib/cell.sh
. "$BENCH_DIR/lib/cell.sh"

arms="rust,java"
pairs="25K:32,101K:1344,301K:288,501K:32"
runs=1
iterations=60
warmup_iterations=30
warmup_rate=25K
read_after=1
list_only=0
out=""

while [ $# -gt 0 ]; do
    case "$1" in
        --arms) arms=$2; shift 2 ;;
        --pairs) pairs=$2; shift 2 ;;
        --runs) runs=$2; shift 2 ;;
        --iterations) iterations=$2; shift 2 ;;
        --warmup-iterations) warmup_iterations=$2; shift 2 ;;
        --out) out=$2; shift 2 ;;
        --list) list_only=1; shift ;;
        --no-read) read_after=0; shift ;;
        --quick)
            pairs="25K:32"
            runs=1; iterations=5; warmup_iterations=3
            shift ;;
        -h|--help) sed -n '3,40p' "${BASH_SOURCE[0]}"; exit 0 ;;
        *) echo "unknown option $1" >&2; exit 2 ;;
    esac
done

out=${out:-$BENCH_OUT_DIR/rust-vs-java/$(date +%Y-%m-%d)-$(hostname -s)}

IFS=',' read -ra arm_list <<<"$arms"
IFS=',' read -ra pair_list <<<"$pairs"

export BENCH_ITERATIONS="$iterations"
export BENCH_WARMUP_ITERATIONS="$warmup_iterations"
export BENCH_WARMUP_RATE="$warmup_rate"

_check() {
    need_dir "harness checkout" "$BENCHMARKS_DIR" || return 1
    need_file "harness echo client" "$BENCHMARKS_ECHO_CLIENT" || return 1
    need_file "harness echo server" "$BENCHMARKS_ECHO_SERVER" || return 1
    need_file "the driver" "$BENCH_DRIVER" "run bench/build.sh" || return 1
    local problem=0
    for arm in "${arm_list[@]}"; do
        case "$arm" in
            java) ;;
            rust)
                need_file "the rig" "$BENCH_RIG" "run bench/build.sh" || problem=1
                need_file "the node" "$BENCH_NODE" "run bench/build.sh" || problem=1
                ;;
            *) echo "unknown arm '$arm' (expected rust or java)" >&2; problem=1 ;;
        esac
    done
    [ "$problem" -eq 0 ]
}

if [ "$list_only" = 1 ]; then
    echo "=== plan ==="
    echo "harness         $BENCHMARKS_DIR"
    echo "arms            ${arm_list[*]}  (whole stacks; nothing swapped inside either)"
    echo "pairs           $pairs"
    echo "runs            $runs over $iterations s (warmup ${warmup_iterations}s at $warmup_rate)"
    echo "rounds          forward, reverse"
    echo "aligned knobs   $(grep -c . "$BENCH_DIR/lib/conf/benchmark.properties") settings from lib/conf/benchmark.properties, given to both drivers"
    echo "results         $out"
    echo
    if _check; then
        echo "prerequisites: ok"
        exit 0
    fi
    echo "prerequisites: NOT met (see above)"
    exit 2
fi

_check || {
    echo >&2
    echo "nothing was run." >&2
    exit 1
}

mkdir -p "$out"
bench_fingerprint "$out/fingerprint.txt" >/dev/null
{
    echo "arms            ${arm_list[*]}"
    echo "pairs           $pairs"
    echo "runs/iterations $runs x ${iterations}s (warmup ${warmup_iterations}s at $warmup_rate)"
    echo "rounds          forward, reverse"
    echo "cores           node=$BENCH_NODE_CPU rig=$BENCH_RIG_CPU, driver pins its own three"
    echo
    echo "what each arm is"
    echo "  java   $BENCHMARKS_DRIVER_JAVA, $BENCHMARKS_ECHO_CLIENT, $BENCHMARKS_ECHO_SERVER"
    echo "  rust   $(sha256sum "$BENCH_DRIVER" | cut -d' ' -f1)  deepmsg-driver"
    echo "         $(sha256sum "$BENCH_RIG" | cut -d' ' -f1)  loadtest-rig"
    echo "         $(sha256sum "$BENCH_NODE" | cut -d' ' -f1)  echo-node"
    echo
    echo "the driver settings both sides are given:"
    sed 's/^/  /' "$BENCH_DIR/lib/conf/benchmark.properties"
} >"$out/RUN-INFO.txt"

echo "=== deepmsg (all ours) / reference (all Java) ==="
sed 's/^/  /' "$out/RUN-INFO.txt"
echo

# --- one cell -----------------------------------------------------------------

_cell() {
    local round=$1 arm=$2 rate=$3 length=$4 run=$5
    local cell="$out/$round/$arm/${rate}_length=${length}"
    local dir="$cell/run-$run"
    local prefix="rust-vs-java_${arm}_echo"
    mkdir -p "$dir"

    wait_idle || return 1
    rm -rf "$BENCH_AERON_DIR"

    case "$arm" in
        java) driver_start_java "$dir/driver.log" "$BENCH_DIR/lib/conf/benchmark.properties" >/dev/null ;;
        rust) driver_start_deepmsg "$dir/driver.log" >/dev/null ;;
    esac
    if ! await_cnc; then
        echo "  $arm $rate: the driver never published its CnC file" >&2
        cat "$dir/driver.log" >&2
        kill_driver
        return 1
    fi

    case "$arm" in
        java) node_start_java "$dir" "$dir/node.log" >/dev/null ;;
        rust) node_start_rust "$dir" "$dir/node.log" >/dev/null ;;
    esac
    sleep 2

    case "$arm" in
        java) run_client_java "$cell" "$dir/client.log" "$rate" "$length" "$run" "$prefix" ;;
        rust) run_client_rust "$cell" "$dir/client.log" "$rate" "$length" "$run" "$prefix" ;;
    esac
    local status=$?

    driver_kill

    if [ "$status" -ne 0 ]; then
        echo "  $arm $rate: the client exited $status" >&2
        return 1
    fi
    if ! cell_validate "$cell" "$run" "$rate" "$iterations"; then
        echo "  $arm $rate run $run: INVALID" >&2
        return 1
    fi
    return 0
}

_run_round() {
    local round=$1; shift
    echo "########## $round ($*) ##########"
    rm -rf "$out/$round"
    for pair in "${pair_list[@]}"; do
        local rate=${pair%%:*} length=${pair##*:}
        for arm in "$@"; do
            local ok=0 run
            for run in $(seq 1 "$runs"); do
                printf '  %-5s %-6s run %s/%s ... ' "$arm" "$rate" "$run" "$runs"
                if _cell "$round" "$arm" "$rate" "$length" "$run"; then
                    echo "ok"
                    ok=$((ok + 1))
                else
                    echo "FAILED"
                fi
            done
            [ "$ok" -eq "$runs" ] || echo "  !! $arm $rate: $ok/$runs runs passed" >&2
        done
    done
}

_run_round round1-forward "${arm_list[@]}"
reversed=$(IFS=,; for ((i = ${#arm_list[@]} - 1; i >= 0; i--)); do printf '%s' "${arm_list[i]}"; [ "$i" -gt 0 ] && printf ','; done)
IFS=',' read -ra rev_list <<<"$reversed"
_run_round round2-reverse "${rev_list[@]}"

echo
echo "results in $out"

if [ "$read_after" = 1 ]; then
    echo
    python3 "$here/read.py" "$out" || true
fi
