#!/usr/bin/env bash
#
# One batch: a single-member cluster, four arms, the reference's client as the
# load in every one of them.
#
#   run-batch.sh [--arms j,j-d,r-a,r] [--pairs 25K:32,...] [--sync A:C,...]
#                [--runs 1] [--iterations 60] [--warmup-iterations 30]
#                [--port-base 61500] [--out DIR] [--list] [--quick] [--no-read]
#
# The four arms differ by exactly one layer each, and that is what each ratio is
# allowed to say:
#
#   j     the reference's Java driver + the reference's node (Java archive,
#         Java consensus module and Java echo service in one JVM)
#   j-d   **this build's driver** + the same reference node
#   r-a   this build's driver + the reference's `io.aeron.archive.Archive` in a
#         JVM of its own + this build's consensus module and container
#   r     this build's driver + this build's archive, consensus module and
#         container, each a process of its own
#
#     r-a / j-d   the consensus module and the container (one archive under both)
#     j-d / j     the driver (one node under both)
#     r-a / r     the archive (one consensus module under both)
#
# ★ **A cell's client is the reference's, always.** The instrument does not
# change between arms; a difference between two arms is a difference in the node
# half. `rust-vs-java` is the batch that compares instruments.
#
# ★ **Single member only.** LC-7's criteria are scoped to the single-member tier
# (the work package's Q6), and the three-member tier is a functional check, not a
# latency one. What the plan calls S3 runs here; S4 is a separate, smaller thing.
#
# ★ **One machine, in this slice.** The node side wants eight busy-spin threads
# (driver 3, archive 3, consensus module 1, container 1) and the client spins
# four more, so a cell that means to be read puts the client on the other
# machine; `--two-host` is not implemented yet in this batch and is refused by
# name rather than quietly run on one host.
#
# Each round visits the arms in the order given and the second round reverses
# it, so a difference that follows the machine rather than the arm flips sign.
# A cell that did not finish is `.FAIL` rather than a row: `lib/cell.sh`'s gate,
# unchanged — the client's own `WARNING:` line, no `.FAIL` histogram, and at
# least `rate x iterations` samples in it.
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
BENCH_DIR=$(dirname "$here")
# shellcheck source=../lib/checkouts.sh
. "$BENCH_DIR/lib/checkouts.sh"
# shellcheck source=../lib/fingerprint.sh
. "$BENCH_DIR/lib/fingerprint.sh"
# shellcheck source=../lib/cell.sh
. "$BENCH_DIR/lib/cell.sh"

arms="j,j-d,r-a,r"
pairs="25K:32,101K:1344,301K:288,501K:32"
syncs="0:0,0:2,2:0,2:2"
runs=1
iterations=60
warmup_iterations=30
warmup_rate=25K
read_after=1
list_only=0
out=""
port_base=${CLUSTER_PORT_BASE:-61500}

while [ $# -gt 0 ]; do
    case "$1" in
        --arms) arms=$2; shift 2 ;;
        --pairs) pairs=$2; shift 2 ;;
        --sync) syncs=$2; shift 2 ;;
        --runs) runs=$2; shift 2 ;;
        --iterations) iterations=$2; shift 2 ;;
        --warmup-iterations) warmup_iterations=$2; shift 2 ;;
        --port-base) port_base=$2; shift 2 ;;
        --out) out=$2; shift 2 ;;
        --list) list_only=1; shift ;;
        --no-read) read_after=0; shift ;;
        --two-host)
            echo "--two-host is not implemented in this batch yet." >&2
            echo "  A cell whose client runs on another machine is what a read number needs;" >&2
            echo "  this slice runs the plumbing on one host. See PREREG.md." >&2
            exit 2 ;;
        --quick)
            pairs="25K:32"
            syncs="0:0"
            runs=1; iterations=5; warmup_iterations=3
            shift ;;
        -h|--help) sed -n '3,45p' "${BASH_SOURCE[0]}"; exit 0 ;;
        *) echo "unknown option $1" >&2; exit 2 ;;
    esac
done

out=${out:-$BENCH_OUT_DIR/cluster-arms/$(date +%Y-%m-%d)-$(hostname -s)}

IFS=',' read -ra arm_list <<<"$arms"
IFS=',' read -ra pair_list <<<"$pairs"
IFS=',' read -ra sync_list <<<"$syncs"

export BENCH_ITERATIONS="$iterations"
export BENCH_WARMUP_ITERATIONS="$warmup_iterations"
export BENCH_WARMUP_RATE="$warmup_rate"

# --- the single member's addresses -------------------------------------------
#
# Every port a member owns comes out of one window, and the window is above the
# kernel's ephemeral range (32768-60999 here) for the reason the port-blocks card
# paid for: a port inside it can be taken by any process mid-run, and a cluster
# that loses one of its own endpoints fails in a way that looks like a bug in the
# cluster.
CLUSTER_HOST=${CLUSTER_HOST:-localhost}
P_INGRESS=$port_base
P_CONSENSUS=$((port_base + 1))
P_LOG=$((port_base + 2))
P_CATCHUP=$((port_base + 3))
P_ARCHIVE=$((port_base + 4))
P_ARCHIVE_REPLICATION=$((port_base + 5))
P_CLUSTER_REPLICATION=$((port_base + 6))

INGRESS="aeron:udp?endpoint=$CLUSTER_HOST:$P_INGRESS"
CONSENSUS="aeron:udp?endpoint=$CLUSTER_HOST:$P_CONSENSUS"
LOG_CHANNEL="aeron:udp?endpoint=$CLUSTER_HOST:$P_LOG"
CATCHUP="aeron:udp?endpoint=$CLUSTER_HOST:$P_CATCHUP"
ARCHIVE_CONTROL="aeron:udp?endpoint=$CLUSTER_HOST:$P_ARCHIVE"
ARCHIVE_REPLICATION="aeron:udp?endpoint=$CLUSTER_HOST:$P_ARCHIVE_REPLICATION"
CLUSTER_REPLICATION="aeron:udp?endpoint=$CLUSTER_HOST:$P_CLUSTER_REPLICATION"
# id,ingress,consensus,log,catchup,archive — the reference's order, and the sixth
# field is where the consensus module dials the archive.
MEMBERS="0,$CLUSTER_HOST:$P_INGRESS,$CLUSTER_HOST:$P_CONSENSUS,$CLUSTER_HOST:$P_LOG,$CLUSTER_HOST:$P_CATCHUP,$CLUSTER_HOST:$P_ARCHIVE"
CONTROL_CHANNEL="aeron:ipc?term-length=128k"
LOCAL_ARCHIVE_CONTROL="aeron:ipc?term-length=64k"

# The cores. The driver pins its own three out of the properties file every
# launcher hands it (1, 2, 3); the node side gets the other five, and both node
# shapes get the same five so that the arm is the only thing that differs.
NODE_CPUS=${CLUSTER_NODE_CPUS:-0,4,5,6,7}
ARCHIVE_CPUS=${CLUSTER_ARCHIVE_CPUS:-4,5,6}
CM_CPU=${CLUSTER_CM_CPU:-7}
CONTAINER_CPU=${CLUSTER_CONTAINER_CPU:-0}

# --- prerequisites ------------------------------------------------------------

_check() {
    local problem=0
    need_dir "harness checkout" "$BENCHMARKS_DIR" || return 1
    need_harness_built || problem=1
    need_file "the cluster node launcher" "$BENCHMARKS_SCRIPTS_DIR/cluster-node" || problem=1
    need_file "the cluster client launcher" "$BENCHMARKS_SCRIPTS_DIR/cluster-client" || problem=1
    need_file "the reference's Java driver" "$BENCHMARKS_DRIVER_JAVA" || problem=1
    need_file "the harness's run-java" "$BENCHMARKS_DIR/scripts/run-java" || problem=1

    local wants_ours=0
    for arm in "${arm_list[@]}"; do
        case "$arm" in
            j) ;;
            j-d|r-a|r) wants_ours=1 ;;
            *) echo "unknown arm '$arm' (expected j, j-d, r-a or r)" >&2; problem=1 ;;
        esac
    done
    if [ "$wants_ours" = 1 ]; then
        need_file "this build's driver" "$BENCH_DRIVER" "run bench/build.sh" || problem=1
        need_file "the media-driver launcher" \
            "$BENCHMARKS_SCRIPTS_DIR/deepmsg-media-driver" \
            "bench/cluster-arms/patches/install.sh --apply" || problem=1
    fi
    local wants_node=0 wants_archive=0
    for arm in "${arm_list[@]}"; do
        case "$arm" in
            r-a|r) wants_node=1 ;;
        esac
        case "$arm" in
            r) wants_archive=1 ;;
        esac
    done
    if [ "$wants_node" = 1 ]; then
        need_file "this build's consensus module" "$BENCH_CLUSTER_NODE" "run bench/build.sh" || problem=1
        need_file "this build's service container" "$BENCH_CLUSTER_CONTAINER" "run bench/build.sh" || problem=1
    fi
    if [ "$wants_archive" = 1 ]; then
        need_file "this build's archive" "$BENCH_ARCHIVE" "run bench/build.sh" || problem=1
    fi
    [ "$problem" -eq 0 ]
}

if [ "$list_only" = 1 ]; then
    echo "=== plan ==="
    echo "harness         $BENCHMARKS_DIR"
    echo "arms            ${arm_list[*]}"
    echo "pairs           $pairs"
    echo "sync            archive:cluster file sync level, one cell each — $syncs"
    echo "runs            $runs over $iterations s (warmup ${warmup_iterations}s at $warmup_rate)"
    echo "rounds          forward, reverse"
    echo "member          single: ingress $P_INGRESS, consensus $P_CONSENSUS, log $P_LOG,"
    echo "                catchup $P_CATCHUP, archive $P_ARCHIVE (+$P_ARCHIVE_REPLICATION, +$P_CLUSTER_REPLICATION)"
    echo "cores           driver pins 1,2,3; node side $NODE_CPUS"
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
    echo "sync            $syncs"
    echo "runs/iterations $runs x ${iterations}s (warmup ${warmup_iterations}s at $warmup_rate)"
    echo "rounds          forward, reverse"
    echo "member          single, host $CLUSTER_HOST"
    echo "channels        ingress $INGRESS"
    echo "                consensus $CONSENSUS"
    echo "                log $LOG_CHANNEL"
    echo "                catchup $CATCHUP"
    echo "                archive control $ARCHIVE_CONTROL"
    echo "                archive replication $ARCHIVE_REPLICATION"
    echo "                cluster replication $CLUSTER_REPLICATION"
    echo "                control (ipc) $CONTROL_CHANNEL"
    echo "members         $MEMBERS"
    echo "cores           driver 1,2,3 (from the properties file); node side $NODE_CPUS;"
    echo "                archive $ARCHIVE_CPUS, consensus module $CM_CPU, container $CONTAINER_CPU"
    echo "client          the reference's cluster-client, pinned to core $BENCH_RIG_CPU"
    echo
    echo "what each arm is"
    echo "  j     $BENCHMARKS_DRIVER_JAVA"
    echo "        $BENCHMARKS_SCRIPTS_DIR/cluster-node (Java archive + CM + echo in one JVM)"
    echo "  j-d   $(sha256sum "$BENCH_DRIVER" 2>/dev/null | cut -d' ' -f1)  deepmsg-driver, via deepmsg-media-driver"
    echo "        $BENCHMARKS_SCRIPTS_DIR/cluster-node"
    echo "  r-a   deepmsg-driver + $BENCHMARKS_DIR/scripts/run-java io.aeron.archive.Archive"
    echo "        $(sha256sum "$BENCH_CLUSTER_NODE" 2>/dev/null | cut -d' ' -f1)  deepmsg-cluster-node"
    echo "        $(sha256sum "$BENCH_CLUSTER_CONTAINER" 2>/dev/null | cut -d' ' -f1)  deepmsg-clustered-service-container"
    echo "  r     deepmsg-driver + $(sha256sum "$BENCH_ARCHIVE" 2>/dev/null | cut -d' ' -f1)  deepmsg-archive"
    echo "        $(sha256sum "$BENCH_CLUSTER_NODE" 2>/dev/null | cut -d' ' -f1)  deepmsg-cluster-node"
    echo "        $(sha256sum "$BENCH_CLUSTER_CONTAINER" 2>/dev/null | cut -d' ' -f1)  deepmsg-clustered-service-container"
} >"$out/RUN-INFO.txt"

echo "=== single-member cluster, four arms, the reference's client in all four ==="
sed 's/^/  /' "$out/RUN-INFO.txt"
echo

# --- processes ---------------------------------------------------------------
#
# Every process a cell starts is held here as a pid and taken down by pid. The
# name-based teardown `lib/cell.sh` uses for the driver is right for a driver a
# launcher exec'd and wrong for a cell that owns five processes: `pgrep -x` sees
# 15 characters of a process name, and `deepmsg-cluster-node` and
# `deepmsg-clustered-service-container` share all fifteen.

CELL_PIDS=()

_start() { # _start <log> <command...>
    local log=$1; shift
    "$@" >"$log" 2>&1 &
    local pid=$!
    CELL_PIDS+=("$pid")
    echo "$pid"
}

_stop() { # _stop <pid> — ask first, then take; a spinner that ignores TERM is a
          # leftover, and a leftover is the next cell's second variable.
    local pid=$1
    kill -TERM "$pid" 2>/dev/null || true
    for _ in $(seq 1 50); do
        kill -0 "$pid" 2>/dev/null || return 0
        sleep 0.1
    done
    kill -KILL "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
}

_stop_all() {
    local i pid
    for ((i = ${#CELL_PIDS[@]} - 1; i >= 0; i--)); do
        pid=${CELL_PIDS[i]}
        _stop "$pid"
    done
    CELL_PIDS=()
}

trap '_stop_all' EXIT

# The reference's node is one JVM with five busy-spin threads, and the
# reference's own harness pins each of them to a core by **thread name**
# (`remote-benchmarks-runner:210-235`). A plain `taskset` mask lets two of them
# land on one core, which is a different machine from the one the reference
# benchmarks itself on. A thread that never appears is a warning, not a cell
# failure: the names below are the reference's, and an arm that renamed one is
# still a cell.
_pin_thread() { # _pin_thread <pid> <name-prefix> <cpu>
    local pid=$1 name=$2 cpu=$3 tid=""
    for _ in $(seq 1 100); do
        tid=$(ps -L -o tid=,comm= -p "$pid" 2>/dev/null | awk -v n="$name" '$2 ~ n { print $1; exit }')
        [ -n "$tid" ] && break
        sleep 0.1
    done
    if [ -z "$tid" ]; then
        echo "  (no thread named '$name' in pid $pid; left where it was)" >&2
        return 0
    fi
    taskset -pc "$cpu" "$tid" >/dev/null 2>&1 || true
}

_pin_node_threads() { # _pin_node_threads <pid>
    _pin_thread "$1" archive-recorde 4
    _pin_thread "$1" archive-replaye 5
    _pin_thread "$1" archive-conduct 6
    _pin_thread "$1" consensus-modul 7
    _pin_thread "$1" echo-service 0
}

_alive() { # _alive <what> <pid>
    kill -0 "$2" 2>/dev/null || {
        echo "  $1 did not stay up" >&2
        return 1
    }
}

# --- the cell ----------------------------------------------------------------

_driver_start() { # _driver_start <arm> <log>
    local arm=$1 log=$2
    case "$arm" in
        j) "$BENCHMARKS_DRIVER_JAVA" >"$log" 2>&1 & echo $! ;;
        *) DEEPMSG_DRIVER="$BENCH_DRIVER" "$BENCHMARKS_SCRIPTS_DIR/deepmsg-media-driver" >"$log" 2>&1 & echo $! ;;
    esac
}

_node_start_java() { # _node_start_java <cell-dir> <log> <sync-archive> <sync-cluster>
    local dir=$1 log=$2 sync_archive=$3 sync_cluster=$4
    JVM_OPTS="\
-Daeron.dir=$BENCH_AERON_DIR \
-Daeron.cluster.dir=$dir/cluster \
-Daeron.cluster.id=0 \
-Daeron.cluster.member.id=0 \
-Daeron.cluster.members=$MEMBERS \
-Daeron.cluster.ingress.channel=$INGRESS \
-Daeron.cluster.consensus.channel=$CONSENSUS \
-Daeron.cluster.log.channel=$LOG_CHANNEL \
-Daeron.cluster.replication.channel=$CLUSTER_REPLICATION \
-Daeron.cluster.control.channel=$CONTROL_CHANNEL \
-Daeron.cluster.service.name=echo-service \
-Daeron.cluster.file.sync.level=$sync_cluster \
-Daeron.archive.dir=$dir/archive \
-Daeron.archive.mark.file.dir=$BENCH_AERON_DIR \
-Daeron.archive.control.channel=$ARCHIVE_CONTROL \
-Daeron.archive.replication.channel=$ARCHIVE_REPLICATION \
-Daeron.archive.local.control.channel=$LOCAL_ARCHIVE_CONTROL \
-Daeron.archive.file.sync.level=$sync_archive \
-Daeron.archive.catalog.file.sync.level=$sync_archive \
-Daeron.archive.recording.events.enabled=false \
-Daeron.archive.threading.mode=DEDICATED \
-Dio.aeron.benchmarks.aeron.cluster.service=echo \
-Dio.aeron.benchmarks.output.directory=$dir" \
        taskset -c "$NODE_CPUS" "$BENCHMARKS_SCRIPTS_DIR/cluster-node" >"$log" 2>&1 &
    echo $!
}

_archive_start_java() { # _archive_start_java <cell-dir> <log> <sync-archive>
    local dir=$1 log=$2 sync_archive=$3
    JVM_OPTS="\
-Daeron.dir=$BENCH_AERON_DIR \
-Daeron.archive.dir=$dir/archive \
-Daeron.archive.mark.file.dir=$BENCH_AERON_DIR \
-Daeron.archive.control.channel=$ARCHIVE_CONTROL \
-Daeron.archive.replication.channel=$ARCHIVE_REPLICATION \
-Daeron.archive.local.control.channel=$LOCAL_ARCHIVE_CONTROL \
-Daeron.archive.file.sync.level=$sync_archive \
-Daeron.archive.catalog.file.sync.level=$sync_archive \
-Daeron.archive.recording.events.enabled=false \
-Daeron.archive.threading.mode=DEDICATED" \
        PROCESS_FILE_NAME=archive-node taskset -c "$ARCHIVE_CPUS" \
        "$BENCHMARKS_DIR/scripts/run-java" io.aeron.archive.Archive >"$log" 2>&1 &
    echo $!
}

_archive_start_deepmsg() { # _archive_start_deepmsg <cell-dir> <log> <sync-archive>
    local dir=$1 log=$2 sync_archive=$3
    taskset -c "$ARCHIVE_CPUS" "$BENCH_ARCHIVE" \
        "-Daeron.dir=$BENCH_AERON_DIR" \
        "-Daeron.archive.dir=$dir/archive" \
        "-Daeron.archive.mark.file.dir=$BENCH_AERON_DIR" \
        "-Daeron.archive.control.channel=$ARCHIVE_CONTROL" \
        "-Daeron.archive.replication.channel=$ARCHIVE_REPLICATION" \
        "-Daeron.archive.local.control.channel=$LOCAL_ARCHIVE_CONTROL" \
        "-Daeron.archive.file.sync.level=$sync_archive" \
        "-Daeron.archive.catalog.file.sync.level=$sync_archive" \
        "-Daeron.archive.threading.mode=DEDICATED" >"$log" 2>&1 &
    echo $!
}

_consensus_start_deepmsg() { # _consensus_start_deepmsg <cell-dir> <log> <sync-cluster>
    local dir=$1 log=$2 sync_cluster=$3
    taskset -c "$CM_CPU" "$BENCH_CLUSTER_NODE" \
        "-Daeron.dir=$BENCH_AERON_DIR" \
        "-Daeron.cluster.dir=$dir/cluster" \
        "-Daeron.cluster.id=0" \
        "-Daeron.cluster.member.id=0" \
        "-Daeron.cluster.members=$MEMBERS" \
        "-Daeron.cluster.ingress.channel=$INGRESS" \
        "-Daeron.cluster.consensus.channel=$CONSENSUS" \
        "-Daeron.cluster.log.channel=$LOG_CHANNEL" \
        "-Daeron.cluster.control.channel=$CONTROL_CHANNEL" \
        "-Daeron.cluster.replication.channel=$CLUSTER_REPLICATION" \
        "-Daeron.cluster.service.count=1" \
        "-Daeron.cluster.file.sync.level=$sync_cluster" \
        "-Daeron.cluster.idle.strategy=spin" >"$log" 2>&1 &
    echo $!
}

_container_start_deepmsg() { # _container_start_deepmsg <cell-dir> <log>
    local dir=$1 log=$2
    taskset -c "$CONTAINER_CPU" "$BENCH_CLUSTER_CONTAINER" \
        "-Daeron.dir=$BENCH_AERON_DIR" \
        "-Daeron.cluster.dir=$dir/cluster" \
        "-Daeron.cluster.id=0" \
        "-Daeron.cluster.service.id=0" \
        "-Ddeepmsg.cluster.service=echo" \
        "-Daeron.cluster.control.channel=$CONTROL_CHANNEL" \
        "-Daeron.archive.local.control.channel=$LOCAL_ARCHIVE_CONTROL" \
        "-Daeron.cluster.idle.strategy=spin" >"$log" 2>&1 &
    echo $!
}

_client_run() { # _client_run <cell> <log> <rate> <length> <run> <arm>
    local cell=$1 log=$2 rate=$3 length=$4 run=$5 arm=$6
    JVM_OPTS="\
-Daeron.dir=$BENCH_AERON_DIR \
-Daeron.cluster.ingress.channel=$INGRESS \
-Daeron.cluster.ingress.endpoints=0=$CLUSTER_HOST:$P_INGRESS \
-Daeron.cluster.egress.channel=aeron:udp?endpoint=$CLUSTER_HOST:0 \
-Daeron.cluster.message.timeout=300000000000 \
-Dio.aeron.benchmarks.aeron.connection.timeout=60s \
-Dio.aeron.benchmarks.warmup.iterations=$warmup_iterations \
-Dio.aeron.benchmarks.warmup.message.rate=$warmup_rate \
-Dio.aeron.benchmarks.iterations=$iterations \
-Dio.aeron.benchmarks.message.rate=$rate \
-Dio.aeron.benchmarks.batch.size=1 \
-Dio.aeron.benchmarks.message.length=$length \
-Dio.aeron.benchmarks.output.file=cluster-$arm \
-Dio.aeron.benchmarks.output.directory=$cell/run-$run \
-Dio.aeron.benchmarks.output.time.unit=MICROSECONDS \
-Dio.aeron.benchmarks.track.history=false \
-Dio.aeron.benchmarks.report.progress=false \
-Dio.aeron.benchmarks.aeron.use.try.claim=true \
-Dio.aeron.benchmarks.aeron.cluster.service=echo" \
        PROCESS_FILE_NAME=cluster-client \
        timeout --signal=KILL "${BENCH_CLIENT_TIMEOUT:-600}" \
        taskset -c "$BENCH_RIG_CPU" "$BENCHMARKS_SCRIPTS_DIR/cluster-client" >"$log" 2>&1
}

_cell() {
    local round=$1 arm=$2 sync=$3 rate=$4 length=$5 run=$6
    local sync_archive=${sync%%:*} sync_cluster=${sync##*:}
    local cell="$out/$round/${arm}_sync=${sync_archive}:${sync_cluster}_length=${length}_rate=${rate}"
    local dir="$cell/run-$run"
    local log="$dir/client.log"
    mkdir -p "$dir"

    CELL_PIDS=()
    wait_idle || return 1
    # A run starts from nothing, and the window is what a leftover driver reads.
    rm -rf "$BENCH_AERON_DIR"
    rm -rf "$dir/cluster" "$dir/archive"

    local driver node pid
    driver=$(_driver_start "$arm" "$dir/driver.log")
    CELL_PIDS+=("$driver")
    if ! await_cnc; then
        echo "  $arm $rate sync $sync: the driver never published its CnC file" >&2
        tail -20 "$dir/driver.log" >&2
        _stop_all
        return 1
    fi

    case "$arm" in
        j|j-d)
            node=$(_node_start_java "$dir" "$dir/node.log" "$sync_archive" "$sync_cluster")
            CELL_PIDS+=("$node")
            _pin_node_threads "$node"
            ;;
        r-a)
            node=$(_archive_start_java "$dir" "$dir/archive.log" "$sync_archive")
            CELL_PIDS+=("$node")
            sleep 1
            _alive "the reference archive" "$node" || { tail -10 "$dir/archive.log" >&2; _stop_all; return 1; }
            ;;
        r)
            node=$(_archive_start_deepmsg "$dir" "$dir/archive.log" "$sync_archive")
            CELL_PIDS+=("$node")
            sleep 1
            _alive "the archive" "$node" || { tail -10 "$dir/archive.log" >&2; _stop_all; return 1; }
            ;;
    esac

    case "$arm" in
        r-a|r)
            pid=$(_consensus_start_deepmsg "$dir" "$dir/module.log" "$sync_cluster")
            CELL_PIDS+=("$pid")
            sleep 1
            _alive "the consensus module" "$pid" || { tail -10 "$dir/module.log" >&2; _stop_all; return 1; }
            pid=$(_container_start_deepmsg "$dir" "$dir/container.log")
            CELL_PIDS+=("$pid")
            sleep 1
            _alive "the container" "$pid" || { tail -10 "$dir/container.log" >&2; _stop_all; return 1; }
            ;;
    esac

    local status=0
    _client_run "$cell" "$log" "$rate" "$length" "$run" "$arm" || status=$?

    # The node side goes first and the driver last: the driver is what every
    # other process above is a client of.
    _stop_all

    if [ "$status" -ne 0 ]; then
        echo "  $arm $rate sync $sync: the client exited $status" >&2
        tail -10 "$log" >&2 || true
        return 1
    fi
    if ! cell_validate "$cell" "$run" "$rate" "$iterations"; then
        echo "  $arm $rate sync $sync run $run: INVALID" >&2
        return 1
    fi
    return 0
}

_run_round() {
    local round=$1; shift
    echo "########## $round ($*) ##########"
    rm -rf "$out/$round"
    local sync pair arm ok run rate length
    for sync in "${sync_list[@]}"; do
        for pair in "${pair_list[@]}"; do
            rate=${pair%%:*}; length=${pair##*:}
            for arm in "$@"; do
                ok=0
                for run in $(seq 1 "$runs"); do
                    printf '  %-4s %-5s sync %-4s run %s/%s ... ' "$arm" "$rate" "$sync" "$run" "$runs"
                    if _cell "$round" "$arm" "$sync" "$rate" "$length" "$run"; then
                        echo "ok"
                        ok=$((ok + 1))
                    else
                        echo "FAILED"
                        # A failed cell leaves `.FAIL` where the reader looks, so
                        # that a missing row is visible as a failed row rather
                        # than as a cell nobody ran.
                        mkdir -p "$out/$round/${arm}_sync=${sync%%:*}:${sync##*:}_length=${length}_rate=${rate}/run-${run}"
                        : >"$out/$round/${arm}_sync=${sync%%:*}:${sync##*:}_length=${length}_rate=${rate}/run-${run}/client.log.FAIL"
                    fi
                done
                [ "$ok" -eq "$runs" ] || echo "  !! $arm $rate sync $sync: $ok/$runs runs passed" >&2
            done
        done
    done
}

_run_round round1-forward "${arm_list[@]}"

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
