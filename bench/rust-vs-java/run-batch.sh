#!/usr/bin/env bash
#
# One batch: this build's whole stack against the reference's.
#
#   run-batch.sh [--arms rust,java] [--pairs 25K:32,...] [--runs 1]
#                [--iterations 60] [--warmup-iterations 30] [--out DIR]
#                [--two-host [--peer user@host]] [--list] [--quick] [--no-read]
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
# **Two shapes, and only one of them is the reference's.** Without arguments
# both halves of a cell run on this machine over loopback — the shape this
# batch has always had, and the one its own earlier readings are in. With
# `--two-host --peer` the server half (driver and node) stays here and the
# client half (driver and client) runs on the peer, which is the shape of the
# reference's own echo benchmark (`scripts/aeron/README.md:6-11`: the client is
# the `LoadTestRig`, the server is the node, and they are advised to be
# different machines). The two shapes are not interchangeable: a number from one
# is not a number from the other.
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
# lan.sh falls back to a default peer (the WSL machine). A two-host cell has to
# be told which machine the other half is on, so remember whether one was named
# before that default exists.
peer_named=${BENCH_PEER_SSH:-}
# shellcheck source=../lib/lan.sh
. "$BENCH_DIR/lib/lan.sh"
# shellcheck source=../lib/peer.sh
. "$BENCH_DIR/lib/peer.sh"
# shellcheck source=../lib/echo-two-host.sh
. "$BENCH_DIR/lib/echo-two-host.sh"

arms="rust,java"
pairs="25K:32,101K:1344,301K:288,501K:32"
runs=1
iterations=60
warmup_iterations=30
warmup_rate=25K
read_after=1
list_only=0
mode=single
out=""
peer=""
peer_repo=${BENCH_PEER_REPO:-$DEEPMSG_DIR}
peer_benchmarks_dir=${BENCH_PEER_BENCHMARKS_DIR:-$BENCHMARKS_DIR}
peer_java_home=${BENCH_PEER_JAVA_HOME:-/usr/lib/jvm/java-21-openjdk-amd64}
peer_rig_cpu=${BENCH_PEER_RIG_CPU:-8}
peer_driver_conductor_cpu=${BENCH_PEER_DRIVER_CONDUCTOR_CPU:-2}
peer_driver_sender_cpu=${BENCH_PEER_DRIVER_SENDER_CPU:-4}
peer_driver_receiver_cpu=${BENCH_PEER_DRIVER_RECEIVER_CPU:-6}
two_host_destination_port=${BENCH_DESTINATION_PORT:-23433}
two_host_source_port=${BENCH_SOURCE_PORT:-23434}
two_host_mtu=${BENCH_MTU:-1408}

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
        --two-host) mode=two; shift ;;
        --single-host) mode=single; shift ;;
        --peer) peer=$2; mode=two; shift 2 ;;
        --peer-repo) peer_repo=$2; shift 2 ;;
        --peer-benchmarks) peer_benchmarks_dir=$2; shift 2 ;;
        --peer-java-home) peer_java_home=$2; shift 2 ;;
        --peer-rig-cpu) peer_rig_cpu=$2; shift 2 ;;
        --peer-driver-conductor-cpu) peer_driver_conductor_cpu=$2; shift 2 ;;
        --peer-driver-sender-cpu) peer_driver_sender_cpu=$2; shift 2 ;;
        --peer-driver-receiver-cpu) peer_driver_receiver_cpu=$2; shift 2 ;;
        --destination-port) two_host_destination_port=$2; shift 2 ;;
        --source-port) two_host_source_port=$2; shift 2 ;;
        --mtu) two_host_mtu=$2; shift 2 ;;
        --quick)
            pairs="25K:32"
            runs=1; iterations=5; warmup_iterations=3
            shift ;;
        -h|--help) sed -n '3,48p' "${BASH_SOURCE[0]}"; exit 0 ;;
        *) echo "unknown option $1" >&2; exit 2 ;;
    esac
done

if [ "$mode" = two ] && [ -n "$peer" ]; then
    BENCH_PEER_SSH=$peer
    BENCH_PEER_IP=${peer#*@}
fi
_peer_guard_paths

mode_suffix=""
[ "$mode" = two ] && mode_suffix="-two-host"
out=${out:-$BENCH_OUT_DIR/rust-vs-java/$(date +%Y-%m-%d)-$(hostname -s)$mode_suffix}

IFS=',' read -ra arm_list <<<"$arms"
IFS=',' read -ra pair_list <<<"$pairs"

export BENCH_ITERATIONS="$iterations"
export BENCH_WARMUP_ITERATIONS="$warmup_iterations"
export BENCH_WARMUP_RATE="$warmup_rate"

_check() {
    need_dir "harness checkout" "$BENCHMARKS_DIR" || return 1
    need_harness_built || return 1
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

    if [ "$mode" = two ]; then
        if [ -z "$peer" ] && [ -z "$peer_named" ]; then
            echo "--two-host needs --peer <user@host>" >&2
            problem=1
        else
            if ! _peer true 2>/dev/null; then
                echo "no ssh to $BENCH_PEER_SSH" >&2
                problem=1
            fi
            for arm in "${arm_list[@]}"; do
                case "$arm" in
                    java)
                        _peer "test -x $peer_benchmarks_dir/scripts/aeron/echo-client" 2>/dev/null || {
                            echo "the peer has no $peer_benchmarks_dir/scripts/aeron/echo-client" >&2
                            problem=1; }
                        _peer "test -x $peer_benchmarks_dir/scripts/aeron/media-driver" 2>/dev/null || {
                            echo "the peer has no $peer_benchmarks_dir/scripts/aeron/media-driver" >&2
                            problem=1; }
                        _peer "test -f $peer_benchmarks_dir/benchmarks-all/build/libs/benchmarks.jar" 2>/dev/null || {
                            echo "the peer's harness has not been built (no benchmarks.jar)" >&2
                            problem=1; }
                        ;;
                    rust)
                        _peer "test -x $peer_repo/target/release/deepmsg-driver" 2>/dev/null || {
                            echo "the peer has no $peer_repo/target/release/deepmsg-driver" >&2
                            echo "  run bench/build.sh on the peer" >&2
                            problem=1; }
                        _peer "test -x $peer_repo/target/rig/release/loadtest-rig" 2>/dev/null || {
                            echo "the peer has no $peer_repo/target/rig/release/loadtest-rig" >&2
                            echo "  run bench/build.sh on the peer" >&2
                            problem=1; }
                        ;;
                esac
            done
            # A cell whose two halves come out of two different harness
            # revisions is a cell about the revisions as well as the stacks.
            local local_rev peer_rev
            local_rev=$(git -C "$BENCHMARKS_DIR" rev-parse HEAD 2>/dev/null || true)
            peer_rev=$(_peer "git -C $peer_benchmarks_dir rev-parse HEAD" 2>/dev/null || true)
            if [ -n "$local_rev" ] && [ -n "$peer_rev" ] && [ "$local_rev" != "$peer_rev" ]; then
                echo "the peer's harness is not the revision this machine measures with:" >&2
                echo "  here $local_rev" >&2
                echo "  peer $peer_rev" >&2
                problem=1
            fi
            if ! lan_require; then
                problem=1
            fi
            if ! two_host_init; then
                problem=1
            fi
        fi
    fi
    [ "$problem" -eq 0 ]
}

if [ "$list_only" = 1 ]; then
    echo "=== plan ==="
    echo "mode            $([ "$mode" = two ] && echo 'two hosts: server here, client on the peer' || echo 'one host: both halves here')"
    echo "harness         $BENCHMARKS_DIR"
    echo "arms            ${arm_list[*]}  (whole stacks; nothing swapped inside either)"
    echo "pairs           $pairs"
    echo "runs            $runs over $iterations s (warmup ${warmup_iterations}s at $warmup_rate)"
    echo "rounds          forward, reverse"
    echo "aligned knobs   $(grep -c . "$BENCH_DIR/lib/conf/benchmark.properties") settings from lib/conf/benchmark.properties, given to both drivers"
    if [ "$mode" = two ]; then
        echo "peer            $BENCH_PEER_SSH"
        echo "peer client     harness $peer_benchmarks_dir; rust $peer_repo (driver and rig)"
        echo "channels        destination=server:$two_host_destination_port, source=peer:$two_host_source_port, mtu $two_host_mtu"
    fi
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
if [ "$mode" = two ]; then
    _peer_fingerprint "$out/fingerprint.txt"
fi
{
    echo "mode            $([ "$mode" = two ] && echo 'two hosts; server here, client on the peer' || echo 'one host')"
    echo "arms            ${arm_list[*]}"
    echo "pairs           $pairs"
    echo "runs/iterations $runs x ${iterations}s (warmup ${warmup_iterations}s at $warmup_rate)"
    echo "rounds          forward, reverse"
    if [ "$mode" = two ]; then
        echo "peer            $BENCH_PEER_SSH ($BENCH_PEER_IP)"
        echo "channels        destination=$_two_server_ip:$two_host_destination_port, source=$_two_client_ip:$two_host_source_port, mtu $two_host_mtu"
        echo "peer cores      driver conductor=$peer_driver_conductor_cpu sender=$peer_driver_sender_cpu receiver=$peer_driver_receiver_cpu, client=$peer_rig_cpu"
    fi
    echo "cores           node=$BENCH_NODE_CPU rig=$BENCH_RIG_CPU, driver pins its own three"
    echo
    echo "what each arm is"
    echo "  java   $BENCHMARKS_DRIVER_JAVA, $BENCHMARKS_ECHO_CLIENT, $BENCHMARKS_ECHO_SERVER"
    echo "  rust   $(sha256sum "$BENCH_DRIVER" | cut -d' ' -f1)  deepmsg-driver"
    echo "         $(sha256sum "$BENCH_RIG" | cut -d' ' -f1)  loadtest-rig"
    echo "         $(sha256sum "$BENCH_NODE" | cut -d' ' -f1)  echo-node"
    if [ "$mode" = two ]; then
        echo
        echo "the peer's binaries, by digest"
        two_host_peer_digests | sed 's/^/  /'
    fi
    echo
    echo "the driver settings both sides are given:"
    sed 's/^/  /' "$BENCH_DIR/lib/conf/benchmark.properties"
} >"$out/RUN-INFO.txt"

echo "=== deepmsg (all ours) / reference (all Java) ==="
sed 's/^/  /' "$out/RUN-INFO.txt"
echo

# --- one cell -----------------------------------------------------------------

_cell_single() {
    local cell=$1 dir=$2 arm=$3 rate=$4 length=$5 run=$6 prefix=$7
    wait_idle || return 1
    rm -rf "$BENCH_AERON_DIR"

    case "$arm" in
        java) driver_start_java "$dir/driver.log" "$BENCH_DIR/lib/conf/benchmark.properties" >/dev/null ;;
        rust) driver_start_deepmsg "$dir/driver.log" >/dev/null ;;
    esac
    if ! await_cnc; then
        echo "  $arm $rate: the driver never published its CnC file" >&2
        cat "$dir/driver.log" >&2
        driver_kill
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

_cell_two_host() {
    local cell=$1 dir=$2 arm=$3 rate=$4 length=$5 run=$6 prefix=$7
    local run_id="${cell#"$out"/}"   # round/arm/rate_length
    run_id=${run_id//\//_}_run-${run}

    _peer_load_gate "$out" || return 1
    wait_idle || return 1
    two_host_prepare_cell "$dir" || return 1

    case "$arm" in
        java|rust)
            two_host_server_start "$arm" "$dir" "$dir/channels.properties" || {
                echo "  $arm $rate: the server half did not come up" >&2
                two_host_stop "$dir"
                return 1; } ;;
        *) echo "  arm '$arm' has no two-host shape" >&2; two_host_stop "$dir"; return 1 ;;
    esac

    two_host_client_start "$arm" "$dir" "$dir/channels.properties" \
        "$run_id" "$prefix" "$rate" "$length" || {
        echo "  $arm $rate: the client half exited non-zero" >&2
        two_host_stop "$dir"
        return 1; }

    two_host_stop "$dir"

    if ! cell_validate "$cell" "$run" "$rate" "$iterations"; then
        echo "  $arm $rate run $run: INVALID" >&2
        return 1
    fi
    return 0
}

_cell() {
    local round=$1 arm=$2 rate=$3 length=$4 run=$5
    local cell="$out/$round/$arm/${rate}_length=${length}"
    local dir="$cell/run-$run"
    local prefix="rust-vs-java_${arm}_echo"
    mkdir -p "$dir"

    if [ "$mode" = two ]; then
        _cell_two_host "$cell" "$dir" "$arm" "$rate" "$length" "$run" "$prefix"
    else
        _cell_single "$cell" "$dir" "$arm" "$rate" "$length" "$run" "$prefix"
    fi
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

# The second round is the first one walked backwards. An array, not a CSV built
# inside a command substitution: a `[ … ] && printf` there returns non-zero on
# the last iteration, and under `set -e` that ends the script before the round
# it was building ever starts.
rev_list=()
for ((i = ${#arm_list[@]} - 1; i >= 0; i--)); do
    rev_list+=("${arm_list[i]}")
done
_run_round round2-reverse "${rev_list[@]}"

echo
echo "results in $out"

if [ "$read_after" = 1 ]; then
    echo
    python3 "$here/read.py" "$out" || true
fi
