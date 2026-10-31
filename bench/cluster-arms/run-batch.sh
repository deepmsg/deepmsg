#!/usr/bin/env bash
#
# One batch: a single-member cluster, four arms, the reference's client as the
# load in every one of them.
#
#   run-batch.sh [--arms j,j-d,r-a,r] [--pairs 25K:32,...] [--sync A:C,...]
#                [--runs 1] [--iterations 60] [--warmup-iterations 30]
#                [--port-base 61500] [--out DIR] [--list] [--quick] [--no-read]
#                [--peer user@host] [--two-host] [--single-host]
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
# ★ **Two machines, when the cell is meant to be read.** The node side wants
# eight busy-spin threads (driver 3, archive 3, consensus module 1, container 1)
# and the client spins four more. On one 8-core machine those twelve threads
# share cores, so a one-host cell is plumbing: `--two-host --peer <user@host>`
# puts the client and its own media driver on the other machine. The client's
# media driver is the reference's (`media-driver` on the peer), in every arm —
# the instrument does not change between arms.
#
# ★ **The tiers are the client link's.** A 1GbE client leg cannot carry
# `101K:1344` (1344 bytes x 101K/s is 1.09 Gbps of payload before any header) and
# has no margin at `301K:288` (~93% of line rate with headers). The default grid
# is therefore `25K:32` and `501K:32`; the other two are registered as not
# runnable on this topology in PREREG.md, before any reading.
#
# ★ **The network is a precondition, not a variable.** `lib/lan.sh` proves a UDP
# datagram makes the round trip in both directions before the first cell, and
# the batch refuses to start on a busy peer.
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
# shellcheck source=../lib/lan.sh
. "$BENCH_DIR/lib/lan.sh"

arms="j,j-d,r-a,r"
# The two tiers a 1GbE client leg can carry; see the header and PREREG.md §3.
pairs="25K:32,501K:32"
syncs="0:0,0:2,2:0,2:2"
runs=1
iterations=60
warmup_iterations=30
warmup_rate=25K
read_after=1
list_only=0
out=""
port_base=${CLUSTER_PORT_BASE:-61500}
mode=single
peer=""
peer_max_load=${BENCH_PEER_MAX_LOAD:-}
peer_benchmarks=${BENCH_PEER_BENCHMARKS_DIR:-${BENCHMARKS_DIR}}
peer_java_home=${BENCH_PEER_JAVA_HOME:-/usr/lib/jvm/java-21-openjdk-amd64}
# The client's four busy-spin threads on the peer. These are defaults, not a
# measurement: the plan's T2 says the core numbers are read off the peer's
# `lscpu` (avoid SMT siblings) and written into RUN-INFO.
peer_driver_conductor_cpu=${BENCH_PEER_DRIVER_CONDUCTOR_CPU:-2}
peer_driver_sender_cpu=${BENCH_PEER_DRIVER_SENDER_CPU:-4}
peer_driver_receiver_cpu=${BENCH_PEER_DRIVER_RECEIVER_CPU:-6}
peer_rig_cpu=${BENCH_PEER_RIG_CPU:-8}
# Scripts and histograms live on the peer's /tmp; the driver's mmap'd buffers do
# not. A term buffer on a disk-backed file is a page fault in the measured path
# (the same rule the bench machine's own runs follow), and WSL2's /tmp is its
# ext4 image, not a tmpfs.
peer_root=${BENCH_PEER_ROOT:-/tmp/cluster-arms-client}
peer_aeron_dir=${BENCH_PEER_AERON_DIR:-/dev/shm/cluster-arms-client/aeron}
path_samples=${BENCH_PATH_SAMPLES:-1000}
path_port=${BENCH_PATH_PORT:-61411}

# Both peer paths are `rm -rf`'d before a cell. A typo in an environment
# variable must not take out a system directory; the guard is cheap.
for _peer_path in "$peer_root" "$peer_aeron_dir"; do
    case "$_peer_path" in
        /|/dev|/dev/shm|/tmp|/home|/root|/usr|/var|/etc)
            echo "refusing to use '$_peer_path' as the peer's work directory" >&2
            exit 2 ;;
    esac
done

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
        --peer) peer=$2; mode=two; shift 2 ;;
        --two-host) mode=two; shift ;;
        --single-host) mode=single; shift ;;
        --quick)
            pairs="25K:32"
            syncs="0:0"
            runs=1; iterations=5; warmup_iterations=3
            shift ;;
        -h|--help) sed -n '3,51p' "${BASH_SOURCE[0]}"; exit 0 ;;
        *) echo "unknown option $1" >&2; exit 2 ;;
    esac
done

if [ "$mode" = two ] && [ -z "$peer" ]; then
    echo "--two-host needs --peer <user@host>." >&2
    echo "  A cell whose client runs on another machine has to say which machine;" >&2
    echo "  without it this batch would quietly measure the one-host plumbing." >&2
    exit 2
fi

if [ "$mode" = two ]; then
    BENCH_PEER_SSH=$peer
    BENCH_PEER_IP=${peer#*@}
    export BENCH_PEER_SSH BENCH_PEER_IP
    _batch_ip=$(_lan_local_ip)
    _peer_ip=$BENCH_PEER_IP
else
    _batch_ip=localhost
    _peer_ip=localhost
fi

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
# The member's address. On one machine it is localhost; on two it is this
# machine's address on the direct link, because the peer has to be able to open
# the ingress and receive the egress. `_batch_ip` is the address the route to the
# peer uses, not "the first address on the box".
CLUSTER_HOST=${CLUSTER_HOST:-$_batch_ip}
# The client's egress endpoint is the *client's* address: the cluster sends the
# answers there, and on two machines that address is not this one.
CLIENT_EGRESS_HOST=$_peer_ip
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

# --- ssh to the peer ---------------------------------------------------------
#
# The same shape `archive-vs-java/run-batch.sh` paid for on 2026-10-17: `ssh
# host "cmd &"` hangs even with all three descriptors redirected, so a process on
# the peer is started under `exec setsid` by an `ssh -f`. `_peer` is for commands
# that finish; `_peer_start` is for daemons.
_peer() {
    ssh -n -o BatchMode=yes -o ConnectTimeout=5 "$BENCH_PEER_SSH" "$@"
}

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

    if [ "$mode" = two ]; then
        if ! _peer true 2>/dev/null; then
            echo "no ssh to $BENCH_PEER_SSH (the client half runs there)." >&2
            echo "  Check the address, the key and the peer's firewall; there is no" >&2
            echo "  cell without a control path to the other machine." >&2
            problem=1
        else
            local peer_path
            peer_path=$peer_benchmarks
            if ! _peer "test -d $peer_path" 2>/dev/null; then
                echo "no reference harness on the peer at $peer_path" >&2
                echo "  the client is the reference's in every arm; set" >&2
                echo "  BENCH_PEER_BENCHMARKS_DIR to that checkout on the peer." >&2
                problem=1
            else
                _peer "test -x $peer_path/scripts/aeron/cluster-client" 2>/dev/null ||
                    { echo "the peer has no $peer_path/scripts/aeron/cluster-client" >&2; problem=1; }
                _peer "test -x $peer_path/scripts/aeron/media-driver" 2>/dev/null ||
                    { echo "the peer has no $peer_path/scripts/aeron/media-driver" >&2; problem=1; }
                _peer "test -f $peer_path/benchmarks-all/build/libs/benchmarks.jar" 2>/dev/null ||
                    { echo "the peer's harness has not been built (no benchmarks.jar)" >&2; problem=1; }
                _peer "command -v python3 >/dev/null 2>&1" 2>/dev/null ||
                    { echo "the peer has no python3 (lan.sh's UDP probe and the path probe need it)" >&2; problem=1; }
                _peer "test -x $peer_java_home/bin/java" 2>/dev/null ||
                    { echo "the peer has no Java at $peer_java_home" >&2; echo "  set BENCH_PEER_JAVA_HOME" >&2; problem=1; }

                # The client is the instrument, so its reference checkout is
                # part of the fingerprint. Two revisions of it are two
                # instruments, even though neither is one of the arms.
                local here_rev there_rev
                here_rev=$(git -C "$BENCHMARKS_DIR" rev-parse HEAD 2>/dev/null || true)
                there_rev=$(_peer "git -C $peer_path rev-parse HEAD" 2>/dev/null || true)
                if [ -n "$here_rev" ] && [ -n "$there_rev" ] && [ "$here_rev" != "$there_rev" ]; then
                    echo "the peer's harness is not the revision this machine measures with:" >&2
                    echo "  here  $here_rev" >&2
                    echo "  peer  $there_rev" >&2
                    echo "  The client is the instrument in every arm; point" >&2
                    echo "  BENCH_PEER_BENCHMARKS_DIR at the same checkout." >&2
                    problem=1
                fi
            fi
        fi
        # The data path itself: same segment, ssh, and a UDP datagram in both
        # directions. The same gate every cell runs under.
        lan_require || problem=1
    fi

    [ "$problem" -eq 0 ]
}

if [ "$list_only" = 1 ]; then
    echo "=== plan ==="
    echo "harness         $BENCHMARKS_DIR"
    echo "arms            ${arm_list[*]}"
    echo "pairs           $pairs"
    if [ "$mode" = two ]; then
        echo "                not runnable on a 1GbE client leg: 101K:1344 (1.09 Gbps"
        echo "                of payload), 301K:288 (~93% of line rate with headers)"
    fi
    echo "sync            archive:cluster file sync level, one cell each — $syncs"
    echo "runs            $runs over $iterations s (warmup ${warmup_iterations}s at $warmup_rate)"
    echo "rounds          forward, reverse"
    echo "member          single, host $CLUSTER_HOST: ingress $P_INGRESS, consensus $P_CONSENSUS, log $P_LOG,"
    echo "                catchup $P_CATCHUP, archive $P_ARCHIVE (+$P_ARCHIVE_REPLICATION, +$P_CLUSTER_REPLICATION)"
    if [ "$mode" = two ]; then
        echo "where           this machine ($_batch_ip): driver + node side"
        echo "                peer $BENCH_PEER_SSH ($_peer_ip): the reference's"
        echo "                     media-driver (conductor $peer_driver_conductor_cpu, sender"
        echo "                     $peer_driver_sender_cpu, receiver $peer_driver_receiver_cpu) + cluster-client"
        echo "                     (core $peer_rig_cpu)"
        echo "peer paths      harness $peer_benchmarks; aeron dir $peer_aeron_dir"
        echo "client egress   aeron:udp?endpoint=$CLIENT_EGRESS_HOST:0"
    else
        echo "where           one machine (loopback); the client shares this machine"
    fi
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
    if [ "$mode" = two ]; then
        echo "mode            two hosts; the client half runs on the peer"
        echo "network         direct link, no gateway: $_batch_ip <-> $_peer_ip"
        echo "peer            $BENCH_PEER_SSH"
        echo "peer cpu        $(_peer "awk -F': ' '/^model name/{print \$2; exit}' /proc/cpuinfo" 2>/dev/null || true) ($(_peer nproc 2>/dev/null || true) logical)"
        echo "peer kernel     $(_peer uname -r 2>/dev/null || true)"
        echo "peer client     the reference's media-driver (conductor $peer_driver_conductor_cpu,"
        echo "                sender $peer_driver_sender_cpu, receiver $peer_driver_receiver_cpu)"
        echo "                + the reference's cluster-client (core $peer_rig_cpu)"
        echo "peer paths      harness $peer_benchmarks"
        echo "                aeron dir $peer_aeron_dir (on the peer's /dev/shm, not /tmp)"
        echo "client egress   aeron:udp?endpoint=$CLIENT_EGRESS_HOST:0"
    else
        echo "mode            one host; the client shares the node's machine"
        echo "network         loopback"
        echo "client          the reference's cluster-client, pinned to core $BENCH_RIG_CPU"
    fi
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
if [ "$mode" = two ]; then
    echo "    (two hosts: the client and its media driver are on $BENCH_PEER_SSH)"
else
    echo "    (one host: plumbing only — a reading needs --two-host --peer)"
fi
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
    # The peer's driver is a process of its own and no pid in this shell. The
    # backstop also takes down a client whose ssh session was interrupted, so
    # the next cell does not start on top of it.
    if [ "${mode:-single}" = two ]; then
        _peer_kill_all || true
    fi
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

# --- the peer ----------------------------------------------------------------
#
# The peer runs four busy-spin threads (the reference's media driver: conductor,
# sender, receiver) and the reference's client, and nothing else. That is the
# whole reason a cell can be read: the node side's eight threads keep this
# machine's cores, and the client's four keep the peer's.

# Start something on the peer and come back. Same reason as the archive batch:
# `ssh host "cmd &"` hangs; `ssh -f` plus `exec setsid` returns. The pid is
# written by the script itself, because `ssh -f` has no child to reap.
_peer_start() {
    local script=$1 log=$2 pidfile=$3
    scp -q "$script" "$BENCH_PEER_SSH:$peer_root/$(basename "$script")" || return 1
    timeout 30 ssh -f -n -o BatchMode=yes "$BENCH_PEER_SSH" \
        "exec setsid bash $peer_root/$(basename "$script") </dev/null >$log 2>&1"
    for _ in $(seq 1 50); do
        _peer "test -s $pidfile" 2>/dev/null && return 0
        sleep 0.1
    done
    echo "  the peer's $(basename "$script") never wrote $pidfile" >&2
    return 1
}

_peer_kill() {
    local pidfile=$1
    _peer "if [ -s $pidfile ]; then kill -9 \$(cat $pidfile) 2>/dev/null || true; rm -f $pidfile; fi" 2>/dev/null || true
}

# The backstop: a driver whose pidfile was missed and a client whose ssh session
# was interrupted. The bracket in each pattern is what keeps `pkill -f` from
# matching the ssh shell that carries it.
_peer_kill_all() {
    _peer_kill "$peer_root/driver.pid"
    _peer "pkill -9 -f '[i]o.aeron.driver.MediaDriver' 2>/dev/null || true; pkill -9 -f '[i]o.aeron.benchmarks.LoadTestRig' 2>/dev/null || true" 2>/dev/null || true
}

# A run starts from nothing on both machines. The peer's aeron dir is on
# /dev/shm; scripts and histograms are on /tmp. Neither is left over.
_peer_prepare() {
    _peer "rm -rf $peer_root $peer_aeron_dir" 2>/dev/null || true
    _peer "mkdir -p $peer_root $(dirname "$peer_aeron_dir")" 2>/dev/null
}

# The peer's load is a precondition, not a variable — the instrument is there.
_load_gate() {
    local load max
    load=$(_peer "cut -d' ' -f1 /proc/loadavg" 2>/dev/null || echo "")
    if [ -z "$load" ]; then
        echo "  could not read the peer's load average" >&2
        return 1
    fi
    max=$peer_max_load
    if [ -z "$max" ]; then
        max=$(( $(_peer "nproc") / 4 ))
        [ "$max" -lt 1 ] && max=1
    fi
    if awk -v l="$load" -v max="$max" 'BEGIN { exit !(l > max) }'; then
        echo "  the peer is busy: 1-minute load $load > $max." >&2
        echo "  Nothing is measured on a busy machine — the instrument is on the peer," >&2
        echo "  and a compiler on its cores is a second variable. Wait, or raise" >&2
        echo "  BENCH_PEER_MAX_LOAD deliberately." >&2
        return 1
    fi
    echo "  peer load $load (limit $max)" >>"$out/load.txt"
}

# No leftover client or driver on the peer before a cell. The process names are
# the reference's own; a batch that is the only thing running there sees none.
_peer_wait_idle() {
    for _ in $(seq 1 150); do
        if ! _peer "pgrep -f '[i]o.aeron.driver.MediaDriver' >/dev/null 2>&1 || pgrep -f '[i]o.aeron.benchmarks.LoadTestRig' >/dev/null 2>&1" 2>/dev/null; then
            return 0
        fi
        sleep 2
    done
    echo "  the peer never went idle" >&2
    return 1
}

# The client's media driver on the peer: the reference's, in every arm. It is
# given an override after the harness's two properties files, so `aeron.dir` and
# the three affinities are the peer's, not the ones in `benchmark.properties`
# (which name cores 1/2/3 and /dev/shm/aeron on the *node*).
_peer_driver_start() { # _peer_driver_start <cell-dir>
    local dir=$1
    cat >"$dir/driver-override.properties" <<EOF
aeron.dir=$peer_aeron_dir
aeron.conductor.cpu.affinity=$peer_driver_conductor_cpu
aeron.sender.cpu.affinity=$peer_driver_sender_cpu
aeron.receiver.cpu.affinity=$peer_driver_receiver_cpu
EOF
    cat >"$dir/peer-driver.sh" <<EOF
#!/usr/bin/env bash
set -euo pipefail
echo \$\$ >"$peer_root/driver.pid"
export JAVA_HOME=\${JAVA_HOME:-$peer_java_home}
export PROCESS_FILE_NAME=cluster-client-media-driver
cd "$peer_benchmarks"
exec ./scripts/aeron/media-driver "$peer_root/driver-override.properties"
EOF
    scp -q "$dir/driver-override.properties" "$BENCH_PEER_SSH:$peer_root/driver-override.properties" || return 1
    _peer_start "$dir/peer-driver.sh" "$peer_root/driver.log" "$peer_root/driver.pid" || return 1
    for _ in $(seq 1 200); do
        _peer "test -f $peer_aeron_dir/cnc.dat" 2>/dev/null && return 0
        sleep 0.1
    done
    echo "  the peer's media driver never published its CnC file" >&2
    _peer "tail -20 $peer_root/driver.log" >&2 || true
    return 1
}

# The client's own settings, in one line because `run-java` reads `JVM_OPTS`
# with `read -ra` and a newline would truncate it at the first line.
_client_jvm_opts() { # <aeron-dir> <egress-host> <out-dir> <run> <arm> <rate> <length>
    local aeron_dir=$1 egress_host=$2 out_dir=$3 run=$4 arm=$5 rate=$6 length=$7
    printf '%s' \
        "-Daeron.dir=$aeron_dir " \
        "-Daeron.cluster.ingress.channel=$INGRESS " \
        "-Daeron.cluster.ingress.endpoints=0=$CLUSTER_HOST:$P_INGRESS " \
        "-Daeron.cluster.egress.channel=aeron:udp?endpoint=$egress_host:0 " \
        "-Daeron.cluster.message.timeout=300000000000 " \
        "-Dio.aeron.benchmarks.aeron.connection.timeout=60s " \
        "-Dio.aeron.benchmarks.warmup.iterations=$warmup_iterations " \
        "-Dio.aeron.benchmarks.warmup.message.rate=$warmup_rate " \
        "-Dio.aeron.benchmarks.iterations=$iterations " \
        "-Dio.aeron.benchmarks.message.rate=$rate " \
        "-Dio.aeron.benchmarks.batch.size=1 " \
        "-Dio.aeron.benchmarks.message.length=$length " \
        "-Dio.aeron.benchmarks.output.file=cluster-$arm " \
        "-Dio.aeron.benchmarks.output.directory=$out_dir " \
        "-Dio.aeron.benchmarks.output.time.unit=MICROSECONDS " \
        "-Dio.aeron.benchmarks.track.history=false " \
        "-Dio.aeron.benchmarks.report.progress=false " \
        "-Dio.aeron.benchmarks.aeron.use.try.claim=true " \
        "-Dio.aeron.benchmarks.aeron.embedded.media.driver=false " \
        "-Dio.aeron.benchmarks.aeron.cluster.service=echo"
}

# The client half when it is on the peer. The script is generated here and run
# there; its stdout/stderr are the cell's `client.log`, so `cell_validate` reads
# the same file in both modes.
_client_run_peer() { # <cell-dir> <log> <rate> <length> <run> <arm> <rel>
    local dir=$1 log=$2 rate=$3 length=$4 run=$5 arm=$6 rel=$7
    local jvm_opts
    jvm_opts=$(_client_jvm_opts "$peer_aeron_dir" "$CLIENT_EGRESS_HOST" "$peer_root/$rel" \
        "$run" "$arm" "$rate" "$length")
    cat >"$dir/peer-client.sh" <<EOF
#!/usr/bin/env bash
set -euo pipefail
export JAVA_HOME=\${JAVA_HOME:-$peer_java_home}
export PROCESS_FILE_NAME=cluster-client
export JVM_OPTS="$jvm_opts"
cd "$peer_benchmarks"
exec timeout --signal=KILL ${BENCH_CLIENT_TIMEOUT:-600} taskset -c $peer_rig_cpu ./scripts/aeron/cluster-client
EOF
    _peer "mkdir -p $peer_root/$rel" || return 1
    scp -q "$dir/peer-client.sh" "$BENCH_PEER_SSH:$peer_root/peer-client.sh" || return 1
    timeout --signal=KILL "${BENCH_CLIENT_TIMEOUT:-600}" \
        ssh -n -o BatchMode=yes "$BENCH_PEER_SSH" "bash $peer_root/peer-client.sh" \
        >"$log" 2>&1
}

# The histogram is the reading, and it was written where the instrument ran.
_collect_peer() { # <cell-run-dir> <relative-path>
    local dir=$1 rel=$2
    scp -q "$BENCH_PEER_SSH:$peer_root/$rel/"'*.hdr*' "$dir/" 2>/dev/null || {
        echo "  no histogram came back from the peer" >&2
        return 1
    }
    scp -q "$BENCH_PEER_SSH:$peer_root/driver.log" "$dir/peer-driver.log" 2>/dev/null || true
}

# The path is part of the instrument, and its cost is reported rather than
# assumed. A 64-byte datagram goes from here to the peer and back; the probe is
# Python at both ends, so the number is an upper bound on the wire-plus-stack
# cost, and it is labelled that way in RUN-INFO.
_path_floor() {
    local n=$path_samples port=$path_port dir=$out/path-floor
    mkdir -p "$dir"
    cat >"$dir/echo.py" <<'PY'
import socket, sys

port = int(sys.argv[1])
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.bind(("0.0.0.0", port))
while True:
    data, addr = s.recvfrom(2048)
    if data == b"END":
        break
    s.sendto(data, addr)
PY
    cat >"$dir/peer-echo.sh" <<EOF
#!/usr/bin/env bash
set -euo pipefail
echo \$\$ >"$peer_root/echo.pid"
exec python3 "$peer_root/path-echo.py" $port
EOF
    _peer "mkdir -p $peer_root" || return 1
    scp -q "$dir/echo.py" "$BENCH_PEER_SSH:$peer_root/path-echo.py" || return 1
    _peer_start "$dir/peer-echo.sh" "$peer_root/echo.log" "$peer_root/echo.pid" || return 1
    sleep 0.3
    local status=0
    python3 - "$_peer_ip" "$port" "$n" <<'PY' || status=$?
import socket, sys, time

host, port, n = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.settimeout(0.5)
rtts = []
lost = 0
for i in range(n):
    payload = b"%d" % i
    t0 = time.monotonic_ns()
    s.sendto(payload, (host, port))
    try:
        data, _ = s.recvfrom(64)
    except socket.timeout:
        lost += 1
        continue
    if data != payload:
        sys.exit("unexpected reply %r" % (data,))
    rtts.append(time.monotonic_ns() - t0)
try:
    s.sendto(b"END", (host, port))
except OSError:
    pass
if not rtts:
    sys.exit("no reply from %s:%d" % (host, port))
rtts.sort()
def q(p):
    return rtts[min(len(rtts) - 1, int(p * (len(rtts) - 1)))]
print("path floor      %d/%d replies, 64 B UDP here -> peer -> here (Python at both ends):"
      " p50 %d ns, p99 %d ns, p99.9 %d ns, min %d, max %d"
      % (len(rtts), n, q(0.5), q(0.99), q(0.999), rtts[0], rtts[-1]))
PY
    _peer_kill "$peer_root/echo.pid"
    return "$status"
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
    local jvm_opts
    jvm_opts=$(_client_jvm_opts "$BENCH_AERON_DIR" "$CLIENT_EGRESS_HOST" \
        "$cell/run-$run" "$run" "$arm" "$rate" "$length")
    JVM_OPTS="$jvm_opts" \
        PROCESS_FILE_NAME=cluster-client \
        timeout --signal=KILL "${BENCH_CLIENT_TIMEOUT:-600}" \
        taskset -c "$BENCH_RIG_CPU" "$BENCHMARKS_SCRIPTS_DIR/cluster-client" >"$log" 2>&1
}

_cell() {
    local round=$1 arm=$2 sync=$3 rate=$4 length=$5 run=$6
    local sync_archive=${sync%%:*} sync_cluster=${sync##*:}
    local cell="$out/$round/${arm}_sync=${sync_archive}:${sync_cluster}_length=${length}_rate=${rate}"
    local rel="$round/$(basename "$cell")"
    local dir="$cell/run-$run"
    local log="$dir/client.log"
    mkdir -p "$dir"

    CELL_PIDS=()
    if [ "$mode" = two ]; then
        # The instrument is on the peer, so its load and its leftovers are
        # checked before anything on this machine starts.
        _load_gate || return 1
        _peer_wait_idle || return 1
        _peer_prepare || return 1
    fi
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

    # The client half's driver is on the peer and is the reference's in every
    # arm. It is started after the node side so that a node that failed to come
    # up leaves nothing on the other machine to clean up.
    if [ "$mode" = two ]; then
        _peer_driver_start "$dir" || { _stop_all; return 1; }
    fi

    local status=0
    if [ "$mode" = two ]; then
        _client_run_peer "$dir" "$log" "$rate" "$length" "$run" "$arm" "$rel" || status=$?
    else
        _client_run "$cell" "$log" "$rate" "$length" "$run" "$arm" || status=$?
    fi

    # The node side goes first and the driver last: the driver is what every
    # other process above is a client of. On two machines the peer's driver is
    # taken down by the same call.
    _stop_all

    # The histogram was written where the instrument ran; bring it home before
    # `cell_validate` looks for it. A failed client is a `.FAIL`, not a row.
    if [ "$status" -eq 0 ] && [ "$mode" = two ]; then
        _collect_peer "$dir" "$rel" || status=1
    fi

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

if [ "$mode" = two ]; then
    echo
    echo "=== path floor (this leg is part of the instrument) ==="
    _path_floor | tee -a "$out/RUN-INFO.txt"
    echo
fi

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
