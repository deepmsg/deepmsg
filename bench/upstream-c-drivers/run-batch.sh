#!/usr/bin/env bash
#
# One batch: the reference's echo benchmark, run with several C drivers and the
# Java one, to measure what a set of C driver changes is worth.
#
#   run-batch.sh [--arms java,c-control,c-patched] [--pairs 501K:32,...]
#                [--runs 3] [--iterations 60] [--warmup-iterations 30] [--mtu 1408]
#                [--out DIR] [--context LABEL] [--list] [--quick] [--no-read]
#
# The measurement is the harness's own: `scripts/aeron/remote-echo-single-host-
# benchmarks`, its default grid (501K<->32, 301K<->288, 101K<->1344), its own
# echo client and echo node, its own result files. What this adds is the arms —
# `--drivers` takes comma-separated *names* from the script's `supported_drivers`
# map, and `patches/install.sh` puts two more names there:
#
#   java        the reference's Java driver, out of the harness's own script
#   c           the C driver binary checked into the harness — a witness, from
#               some other build at some other time
#   c-control   the C driver built here from the unpatched revision
#   c-patched   the same source with this tree's three driver commits
#
# `c-control` is the one that matters: `c-patched` against it is a paired
# reading of the commits and nothing else, because the two are built from the
# same repository by the same script with the same flags. `c-patched` against
# `c` would compare them against whatever binary the harness happens to carry.
#
# **Two rounds, the second with the arm order reversed**, and the round is part
# of every table. The harness's outer loop is over drivers, so reversing the
# `--drivers` list is what makes the reverse round; a reading whose sign
# survives the reversal is the driver, and one that does not was the session.
#
# Prerequisites, all of which this script checks before it starts:
#   - the harness, at `${BENCHMARKS_DIR}` (see bench/lib/checkouts.sh)
#   - `patches/install.sh --apply` has been run
#   - the arms have been built: build-c-driver.sh <arm> <revision>
#
# Cost, so nobody starts one by accident: one round is
# `arms x pairs x runs x (iterations + warmup)` seconds — with the defaults that
# is three arms, three pairs, three runs, 90 s each, about 40 minutes; two rounds
# is about 80. `--quick` is one arm-set, one pair, one run, 8 s.
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
BENCH_DIR=$(dirname "$here")
# shellcheck source=../lib/checkouts.sh
. "$BENCH_DIR/lib/checkouts.sh"
# shellcheck source=../lib/fingerprint.sh
. "$BENCH_DIR/lib/fingerprint.sh"
# shellcheck source=../lib/cell.sh
. "$BENCH_DIR/lib/cell.sh"
# shellcheck source=../lib/lan.sh
. "$BENCH_DIR/lib/lan.sh"
# shellcheck source=../lib/peer.sh
. "$BENCH_DIR/lib/peer.sh"
# shellcheck source=../lib/echo-two-host.sh
. "$BENCH_DIR/lib/echo-two-host.sh"


arms="java,c-control,c-patched"
pairs="501K:32,301K:288,101K:1344"
runs=3
iterations=60
warmup_iterations=30
warmup_rate=25K
mtu=1408
context=cdrv
read_after=1
list_only=0
out=""
mode=single
peer=""
peer_benchmarks_dir=${BENCH_PEER_BENCHMARKS_DIR:-$BENCHMARKS_DIR}
peer_java_home=${BENCH_PEER_JAVA_HOME:-/usr/lib/jvm/java-21-openjdk-amd64}
peer_rig_cpu=${BENCH_PEER_RIG_CPU:-8}
peer_driver_conductor_cpu=${BENCH_PEER_DRIVER_CONDUCTOR_CPU:-2}
peer_driver_sender_cpu=${BENCH_PEER_DRIVER_SENDER_CPU:-4}
peer_driver_receiver_cpu=${BENCH_PEER_DRIVER_RECEIVER_CPU:-6}
two_host_destination_port=${BENCH_DESTINATION_PORT:-23433}
two_host_source_port=${BENCH_SOURCE_PORT:-23434}

while [ $# -gt 0 ]; do
    case "$1" in
        --arms) arms=$2; shift 2 ;;
        --pairs) pairs=$2; shift 2 ;;
        --runs) runs=$2; shift 2 ;;
        --iterations) iterations=$2; shift 2 ;;
        --warmup-iterations) warmup_iterations=$2; shift 2 ;;
        --mtu) mtu=$2; shift 2 ;;
        --context) context=$2; shift 2 ;;
        --out) out=$2; shift 2 ;;
        --list) list_only=1; shift ;;
        --no-read) read_after=0; shift ;;
        --two-host) mode=two; shift ;;
        --single-host) mode=single; shift ;;
        --peer) peer=$2; mode=two; shift 2 ;;
        --peer-benchmarks) peer_benchmarks_dir=$2; shift 2 ;;
        --peer-java-home) peer_java_home=$2; shift 2 ;;
        --peer-rig-cpu) peer_rig_cpu=$2; shift 2 ;;
        --peer-driver-conductor-cpu) peer_driver_conductor_cpu=$2; shift 2 ;;
        --peer-driver-sender-cpu) peer_driver_sender_cpu=$2; shift 2 ;;
        --peer-driver-receiver-cpu) peer_driver_receiver_cpu=$2; shift 2 ;;
        --destination-port) two_host_destination_port=$2; shift 2 ;;
        --source-port) two_host_source_port=$2; shift 2 ;;
        --quick)
            arms="java,c-control,c-patched"
            pairs="501K:32"
            runs=1; iterations=5; warmup_iterations=3; warmup_rate=25K
            shift ;;
        -h|--help) sed -n '3,45p' "${BASH_SOURCE[0]}"; exit 0 ;;
        *) echo "unknown option $1" >&2; exit 2 ;;
    esac
done

if [ "$mode" = two ] && [ -n "$peer" ]; then
    BENCH_PEER_SSH=$peer
    BENCH_PEER_IP=${peer#*@}
fi
_peer_guard_paths
two_host_mtu=$mtu

mode_suffix=""
[ "$mode" = two ] && mode_suffix="-two-host"
out=${out:-$BENCH_OUT_DIR/upstream-c-drivers/$(date +%Y-%m-%d)-$(hostname -s)$mode_suffix}

# --- the arms, as names the harness knows ------------------------------------

IFS=',' read -ra arm_list <<<"$arms"
IFS=',' read -ra pair_list <<<"$pairs"

rate_list=(); length_list=()
for pair in "${pair_list[@]}"; do
    rate_list+=("${pair%%:*}")
    length_list+=("${pair##*:}")
done

_arm_problem() {
    local arm=$1
    case "$arm" in
        java) return 0 ;;
        c)
            # The harness's own binaries; its `c-media-driver` is what uses them.
            [ -x "$BENCHMARKS_SCRIPTS_DIR/aeronmd" ] || echo "the harness has no aeronmd in $BENCHMARKS_SCRIPTS_DIR"
            return 0 ;;
    esac
    # The installed launcher is only what the harness's single-host script
    # calls; the two-host shape execs the arm's own `aeronmd` and needs no
    # launcher in the harness.
    if [ "$mode" != two ] && [ ! -x "$BENCHMARKS_DIR/scripts/aeron/c-media-driver-$arm" ]; then
        echo "no launcher: scripts/aeron/c-media-driver-$arm — run patches/install.sh --apply"
    elif [ ! -x "$AERON_C_ARM_DIR/$arm/aeronmd" ]; then
        echo "no binary: $AERON_C_ARM_DIR/$arm/aeronmd — run build-c-driver.sh $arm <revision>"
    fi
}

_check() {
    need_dir "harness checkout" "$BENCHMARKS_DIR" || return 1
    need_harness_built || return 1
    local problem=0
    for arm in "${arm_list[@]}"; do
        local why
        why=$(_arm_problem "$arm")
        if [ -n "$why" ]; then
            echo "arm '$arm': $why" >&2
            problem=1
        fi
    done

    if [ "$mode" = two ]; then
        if [ -z "$BENCH_PEER_SSH" ]; then
            echo "--two-host needs --peer <user@host>" >&2
            problem=1
        else
            if ! _peer true 2>/dev/null; then
                echo "no ssh to $BENCH_PEER_SSH" >&2
                problem=1
            fi
            _peer "test -x $peer_benchmarks_dir/scripts/aeron/echo-client" 2>/dev/null || {
                echo "the peer has no $peer_benchmarks_dir/scripts/aeron/echo-client" >&2
                problem=1; }
            _peer "test -x $peer_benchmarks_dir/scripts/aeron/media-driver" 2>/dev/null || {
                echo "the peer has no $peer_benchmarks_dir/scripts/aeron/media-driver" >&2
                problem=1; }
            _peer "test -x $peer_benchmarks_dir/scripts/aeron/c-media-driver" 2>/dev/null || {
                echo "the peer has no $peer_benchmarks_dir/scripts/aeron/c-media-driver" >&2
                problem=1; }
            _peer "test -f $peer_benchmarks_dir/benchmarks-all/build/libs/benchmarks.jar" 2>/dev/null || {
                echo "the peer's harness has not been built (no benchmarks.jar)" >&2
                problem=1; }
            # The witness `c` arm's binaries and the echo client are the peer's
            # checkout: two revisions would be a second variable.
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
    else
        need_file "harness single-host script" \
            "$BENCHMARKS_SCRIPTS_DIR/remote-echo-single-host-benchmarks" || problem=1
        if ! grep -q 'Local launch shim' "$BENCHMARKS_DIR/scripts/remote-benchmarks-runner" 2>/dev/null &&
           ! ssh -o BatchMode=yes -o ConnectTimeout=3 localhost true 2>/dev/null; then
            echo "the harness cannot launch locally: no local-launch shim and no passwordless ssh to localhost" >&2
            echo "  run patches/install.sh --apply, or set up 'ssh localhost'" >&2
            problem=1
        fi
    fi
    [ "$problem" -eq 0 ]
}

if [ "$list_only" = 1 ]; then
    echo "=== plan ==="
    echo "mode            $([ "$mode" = two ] && echo 'two hosts: server here, client on the peer' || echo 'one host (the harness single-host script)')"
    echo "harness         $BENCHMARKS_DIR"
    echo "arms            ${arm_list[*]}"
    echo "pairs           $pairs"
    echo "runs            $runs over $iterations s (warmup ${warmup_iterations}s at $warmup_rate)"
    echo "mtu             $mtu"
    echo "rounds          forward, reverse"
    if [ "$mode" = two ]; then
        echo "peer            $BENCH_PEER_SSH"
        echo "peer harness    $peer_benchmarks_dir"
        echo "channels        destination=server:$two_host_destination_port, source=peer:$two_host_source_port, mtu $mtu"
    fi
    echo "results         $out"
    echo
    # The same shape the run's own record uses, so that what `--list` promises and
    # what a finished batch writes down are the same four lines.
    echo "=== arms, by digest ==="
    for arm in "${arm_list[@]}"; do
        case "$arm" in
            java)
                echo "  java        the harness's Java driver"
                ;;
            c)
                echo "  c           $(sha256sum "$BENCHMARKS_SCRIPTS_DIR/aeronmd" | cut -d' ' -f1)  aeronmd"
                echo "              $(sha256sum "$BENCHMARKS_SCRIPTS_DIR/libaeron_driver.so" | cut -d' ' -f1)  libaeron_driver.so"
                echo "              the harness's own checked-in binaries"
                ;;
            *)
                echo "  $arm  $(sha256sum "$AERON_C_ARM_DIR/$arm/aeronmd" | cut -d' ' -f1)  aeronmd"
                echo "              $(sha256sum "$AERON_C_ARM_DIR/$arm/libaeron_driver.so" | cut -d' ' -f1)  libaeron_driver.so"
                rev=$(awk '/^revision /{print $2}' "$AERON_C_ARM_DIR/$arm/BUILD-INFO.txt" 2>/dev/null)
                named=$(awk '/^revision-name /{print $2}' "$AERON_C_ARM_DIR/$arm/BUILD-INFO.txt" 2>/dev/null)
                echo "              built from ${rev:-unknown} ${named:+($named)}"
                ;;
        esac
    done
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

# --- what the harness needs from its environment -----------------------------
#
# The single-host script requires all of these and reads several of them
# unconditionally while rewriting its driver command, so the DPDK placeholders
# have to be present and empty even though no DPDK path is taken. The core
# numbers are the whole isolation story on a one-socket, one-NUMA-node box: core
# 0 is left to the kernel and the network softirq, 6 and 7 to the JVMs' own
# threads. They are frozen here and published with every result.
export BENCHMARKS_PATH="$BENCHMARKS_DIR"

# The harness needs a JAVA_HOME, not just a `java`. The bench machine's is named
# first and the one behind `java` on the path is the fallback, so this does not
# have to be edited on a machine that keeps its JDK somewhere else.
if [ -z "${JAVA_HOME:-}" ]; then
    for candidate in /usr/lib/jvm/java-21-openjdk-amd64 \
        "$(dirname "$(dirname "$(readlink -f "$(command -v java 2>/dev/null)" 2>/dev/null)")")"; do
        if [ -x "$candidate/bin/java" ]; then
            export JAVA_HOME="$candidate"
            break
        fi
    done
fi
if [ -z "${JAVA_HOME:-}" ]; then
    echo "no JAVA_HOME: set it, or put a java on the path" >&2
    exit 1
fi

export DRIVER_CPU_NODE=0
export CLIENT_CPU_NODE=0
export SERVER_CPU_NODE=0
export DRIVER_CONDUCTOR_CPU_CORE=1
export DRIVER_SENDER_CPU_CORE=2
export DRIVER_RECEIVER_CPU_CORE=3
export SERVER_ECHO_CPU_CORE=4
export CLIENT_LOAD_TEST_RIG_MAIN_CPU_CORE=5
export NON_ISOLATED_CPU_CORES="6,7"

# 127.0.0.1 rather than `localhost`: this box has both an IPv6 address and a VPN
# interface, and the address family must not depend on resolver order.
export DESTINATION_CHANNEL="aeron:udp?endpoint=127.0.0.1:13000"
export SOURCE_CHANNEL="aeron:udp?endpoint=127.0.0.1:13100"

export SSH_CLIENT_USER=""
export SSH_CLIENT_KEY_FILE=""
export SSH_CLIENT_NODE=""
export AERON_DPDK_GATEWAY_IPV4_ADDRESS=""
export AERON_DPDK_LOCAL_IPV4_ADDRESS=""

# The arms are handed to the launchers through the environment, because the
# launcher the harness runs is a script this batch installed rather than one it
# wrote.
export AERON_C_ARM_DIR

# Everything else the harness reads with a default of its own.
export BURST_SIZE=1
export USE_TRY_CLAIM=true
export OUTPUT_TIME_UNIT=MICROSECONDS
export TRACK_HISTORY=false
export REPORT_PROGRESS=false
export CONNECTION_TIMEOUT=60s
export RUNS="$runs"
export ITERATIONS="$iterations"
export WARMUP_ITERATIONS="$warmup_iterations"
export WARMUP_MESSAGE_RATE="$warmup_rate"
export MESSAGE_RATE="$(IFS=,; echo "${rate_list[*]}")"
export MESSAGE_LENGTH="$(IFS=,; echo "${length_list[*]}")"

# --- the record --------------------------------------------------------------

bench_fingerprint "$out/fingerprint.txt" >/dev/null
if [ "$mode" = two ]; then
    _peer_fingerprint "$out/fingerprint.txt"
fi
{
    echo "mode            $([ "$mode" = two ] && echo 'two hosts; server here, client on the peer' || echo 'one host (the harness single-host script)')"
    echo "arms            ${arm_list[*]}"
    echo "pairs           $pairs"
    echo "runs/iterations $runs x ${iterations}s (warmup ${warmup_iterations}s at $warmup_rate), mtu $mtu"
    echo "rounds          forward, reverse"
    echo "context         $context"
    if [ "$mode" = two ]; then
        echo "peer            $BENCH_PEER_SSH ($BENCH_PEER_IP)"
        echo "peer harness    $peer_benchmarks_dir"
        echo "channels        destination=$_two_server_ip:$two_host_destination_port, source=$_two_client_ip:$two_host_source_port, mtu $mtu"
        echo "peer cores      driver conductor=$peer_driver_conductor_cpu sender=$peer_driver_sender_cpu receiver=$peer_driver_receiver_cpu, client=$peer_rig_cpu"
    fi
    if [ "$mode" = two ]; then
        echo "local cores     driver conductor=$DRIVER_CONDUCTOR_CPU_CORE sender=$DRIVER_SENDER_CPU_CORE receiver=$DRIVER_RECEIVER_CPU_CORE echo=$SERVER_ECHO_CPU_CORE"
    else
        echo "cores           conductor=$DRIVER_CONDUCTOR_CPU_CORE sender=$DRIVER_SENDER_CPU_CORE receiver=$DRIVER_RECEIVER_CPU_CORE echo=$SERVER_ECHO_CPU_CORE rig=$CLIENT_LOAD_TEST_RIG_MAIN_CPU_CORE aux=$NON_ISOLATED_CPU_CORES"
        echo "channels        dest=$DESTINATION_CHANNEL src=$SOURCE_CHANNEL"
    fi
    echo
    echo "arms, by digest"
    for arm in "${arm_list[@]}"; do
        case "$arm" in
            java) echo "  java        the harness's Java driver" ;;
            c)    echo "  c           $(sha256sum "$BENCHMARKS_SCRIPTS_DIR/aeronmd" 2>/dev/null | cut -d' ' -f1)  (the harness's checked-in binary)" ;;
            *)    echo "  $arm  $(sha256sum "$AERON_C_ARM_DIR/$arm/aeronmd" | cut -d' ' -f1)  aeronmd"
                  echo "              $(sha256sum "$AERON_C_ARM_DIR/$arm/libaeron_driver.so" | cut -d' ' -f1)  libaeron_driver.so"
                  # The arm's own revision, out of the file `build-c-driver.sh`
                  # leaves beside it. The work area's HEAD is a different thing
                  # and would name the same revision for every arm.
                  rev=$(awk '/^revision /{print $2}' "$AERON_C_ARM_DIR/$arm/BUILD-INFO.txt" 2>/dev/null)
                  named=$(awk '/^revision-name /{print $2}' "$AERON_C_ARM_DIR/$arm/BUILD-INFO.txt" 2>/dev/null)
                  echo "              built from ${rev:-unknown} ${named:+($named)}" ;;
        esac
    done
} >"$out/RUN-INFO.txt"

echo "=== deepmsg / C driver arms ==="
sed 's/^/  /' "$out/RUN-INFO.txt"
echo

# --- two rounds --------------------------------------------------------------

# `_run_round <round-name> <arm>...` — the arms in the order this round visits
# them, which is what makes the second round the first one reversed.
_run_round() {
    local name=$1; shift
    local csv
    csv=$(IFS=,; echo "$*")
    local raw="$out/$name/.downloaded"
    local log="$out/$name.log"

    echo "########## $name ($*) ##########"
    rm -rf "$out/$name" "$raw"
    mkdir -p "$raw"

    "$BENCHMARKS_SCRIPTS_DIR/remote-echo-single-host-benchmarks" \
        --drivers "$csv" \
        --mtu "$mtu" \
        --context "$context" \
        --download-dir "$raw" >"$log" 2>&1 || {
        echo "  the harness exited non-zero — see $log" >&2
        return 1
    }

    local tarball
    tarball=$(find "$raw" -maxdepth 1 -name '*.tar.gz' -printf '%T@ %p\n' 2>/dev/null | sort -rn | head -1 | cut -d' ' -f2-)
    if [ -z "$tarball" ]; then
        echo "  the harness left no archive in $raw — see $log" >&2
        return 1
    fi
    mkdir -p "$out/$name"
    tar xzf "$tarball" -C "$out/$name"
    echo "  $(find "$out/$name" -name '*.hdr' | wc -l) histograms, $(find "$out/$name" -name '*.FAIL' | wc -l) FAILED"
}

# --- the two-host shape ------------------------------------------------------
#
# The harness's own single-host script launches every process on this machine
# through its "local launch shim". The two-host shape does not use it: it starts
# the server half (driver and echo node) here and the client half (driver and
# echo client) on the peer, through the same cell machinery
# `rust-vs-java/run-batch.sh` uses (`lib/echo-two-host.sh`). The result tree is
# still the harness's: one directory per cell, named the way `read.py` parses.

_cell_two_host() {
    local round=$1 arm=$2 rate=$3 length=$4 run=$5
    local name="aeron-echo_${arm}_${context}_mtu=${mtu}_length=${length}_rate=${rate}"
    local cell="$out/$round/$name"
    local dir="$cell/run-$run"
    local prefix="$name"
    local run_id="${round}_${name}_run-${run}"
    local arm_dir=""
    case "$arm" in
        java|c|rust) ;;
        *) arm_dir="$AERON_C_ARM_DIR/$arm" ;;
    esac
    mkdir -p "$dir"

    _peer_load_gate "$out" || return 1
    wait_idle || return 1
    two_host_prepare_cell "$dir" || return 1

    two_host_server_start "$arm" "$dir" "$dir/channels.properties" "$arm_dir" || {
        echo "  $arm $rate: the server half did not come up" >&2
        two_host_stop "$dir"
        return 1; }

    two_host_client_start "$arm" "$dir" "$dir/channels.properties" \
        "$run_id" "$prefix" "$rate" "$length" "$arm_dir" || {
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

_run_round_two_host() {
    local name=$1; shift
    echo "########## $name ($*) ##########"
    rm -rf "$out/$name"
    for pair in "${pair_list[@]}"; do
        local rate=${pair%%:*} length=${pair##*:}
        for arm in "$@"; do
            local ok=0 run
            for run in $(seq 1 "$runs"); do
                printf '  %-10s %-6s run %s/%s ... ' "$arm" "$rate" "$run" "$runs"
                if _cell_two_host "$name" "$arm" "$rate" "$length" "$run"; then
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

# The second round is the first one walked backwards. Built as an array rather
# than a CSV string: a `[ … ] && printf` inside a command substitution returns
# non-zero on the last iteration, and under `set -e` that is the end of the
# script — which is exactly how the first version of this failed.
rev_list=()
for ((i = ${#arm_list[@]} - 1; i >= 0; i--)); do
    rev_list+=("${arm_list[i]}")
done

if [ "$mode" = two ]; then
    _run_round_two_host round1-forward "${arm_list[@]}" || exit 1
    _run_round_two_host round2-reverse "${rev_list[@]}" || exit 1
else
    _run_round round1-forward "${arm_list[@]}" || exit 1

    # The harness rewrites this file for every run, so it is read here rather
    # than before the first round — before it, what is on disk is the previous
    # run's settings, which is a way to publish the wrong configuration beside a
    # result.
    {
        echo
        echo "the driver properties the harness wrote for round 1:"
        sed 's/^/  /' "$BENCHMARKS_SCRIPTS_DIR/benchmark.properties"
    } >>"$out/RUN-INFO.txt"

    _run_round round2-reverse "${rev_list[@]}" || exit 1
fi

echo
echo "results in $out"

if [ "$read_after" = 1 ]; then
    echo
    python3 "$here/read.py" "$out" || true
fi
