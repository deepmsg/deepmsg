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
rounds_to_run=2
iterations=60
warmup_iterations=30
warmup_rate=25K
read_after=1
list_only=0
out=""
port_base=${CLUSTER_PORT_BASE:-61500}
mode=single
peer=""
# The other two members of a three-member cluster, `--member user@host` twice.
# Empty means the single-member cell this batch has always run.
member_ssh=()
peer_max_load=${BENCH_PEER_MAX_LOAD:-}
peer_benchmarks=${BENCH_PEER_BENCHMARKS_DIR:-${BENCHMARKS_DIR}}
peer_java_home=${BENCH_PEER_JAVA_HOME:-/usr/lib/jvm/java-21-openjdk-amd64}
member_java_home=${BENCH_MEMBER_JAVA_HOME:-${JAVA_HOME:-/usr/lib/jvm/java-21-openjdk-amd64}}
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
        --rounds) rounds_to_run=$2; shift 2 ;;
        --iterations) iterations=$2; shift 2 ;;
        --warmup-iterations) warmup_iterations=$2; shift 2 ;;
        --port-base) port_base=$2; shift 2 ;;
        --out) out=$2; shift 2 ;;
        --list) list_only=1; shift ;;
        --no-read) read_after=0; shift ;;
        --peer) peer=$2; mode=two; shift 2 ;;
        --member) member_ssh+=("$2"); shift 2 ;;
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

# How many members, and where. One machine unless `--member` was given twice;
# a two-member cluster is not a thing this batch runs (a quorum of two is a
# different measurement, not a smaller one).
member_count=$((1 + ${#member_ssh[@]}))
case "$member_count" in
    1|3) ;;
    *)  echo "--member takes exactly two hosts (this batch runs single-member or" >&2
        echo "three-member): ${#member_ssh[@]} given." >&2
        exit 2 ;;
esac

# Each member owns a ten-port window, and the window is above Linux's ephemeral
# range (32768-60999) for the reason the port-blocks card paid for: a port inside
# it can be taken by any process mid-run, and a cluster that loses one of its own
# endpoints fails in a way that looks like a bug in the cluster.
_member_ip() { # <index>
    if [ "$1" -eq 0 ]; then
        printf '%s' "$CLUSTER_HOST"
    else
        printf '%s' "${member_ssh[$1 - 1]#*@}"
    fi
}

declare -a M_IP M_INGRESS M_CONSENSUS M_LOG M_CATCHUP M_ARCHIVE_CONTROL M_ARCHIVE_REPLICATION M_CLUSTER_REPLICATION
for ((_i = 0; _i < member_count; _i++)); do
    M_IP[_i]=$(_member_ip "$_i")
    _base=$((port_base + _i * 10))
    M_INGRESS[_i]="aeron:udp?endpoint=${M_IP[_i]}:$((_base + 0))"
    M_CONSENSUS[_i]="aeron:udp?endpoint=${M_IP[_i]}:$((_base + 1))"
    # ★ The log is the one channel whose shape depends on the membership. A
    # single member publishes to itself, so an endpoint is the whole channel. A
    # three-member cluster publishes to the other members, and the reference's
    # rule (`ConsensusModule.java:2145`) is *udp with no endpoint* = multi-
    # destination; the publication then gets `control-mode=manual` and a control
    # endpoint of its own. An endpoint here is a one-destination publication,
    # and followers time out with "failed to join live log".
    if [ "$member_count" -gt 1 ]; then
        M_LOG[_i]="aeron:udp?term-length=64m|control-mode=manual|control=${M_IP[_i]}:$((_base + 2))|mtu=1408"
    else
        M_LOG[_i]="aeron:udp?endpoint=${M_IP[_i]}:$((_base + 2))"
    fi
    M_CATCHUP[_i]="aeron:udp?endpoint=${M_IP[_i]}:$((_base + 3))"
    M_ARCHIVE_CONTROL[_i]="aeron:udp?endpoint=${M_IP[_i]}:$((_base + 4))"
    M_ARCHIVE_REPLICATION[_i]="aeron:udp?endpoint=${M_IP[_i]}:$((_base + 5))"
    M_CLUSTER_REPLICATION[_i]="aeron:udp?endpoint=${M_IP[_i]}:$((_base + 6))"
done

# The six-field member form, `id,ingress,consensus,log,catchup,archive`; the
# sixth field is where the consensus module dials its own archive.
build_members() {
    local out_m="" i base
    for ((i = 0; i < member_count; i++)); do
        base=$((port_base + i * 10))
        out_m+="${i},${M_IP[i]}:$((base + 0)),${M_IP[i]}:$((base + 1)),${M_IP[i]}:$((base + 2)),"
        out_m+="${M_IP[i]}:$((base + 3)),${M_IP[i]}:$((base + 4))|"
    done
    printf '%s' "${out_m%|}"
}
MEMBERS=$(build_members)

# The reference client's own form of the same thing.
build_client_endpoints() {
    local out_e="" i
    for ((i = 0; i < member_count; i++)); do
        out_e+="${i}=${M_IP[i]}:$((port_base + i * 10)),"
    done
    printf '%s' "${out_e%,}"
}
CLIENT_INGRESS_ENDPOINTS=$(build_client_endpoints)

# Member 0's channels, under the names the single-member path has always used.
INGRESS=${M_INGRESS[0]}
CONSENSUS=${M_CONSENSUS[0]}
LOG_CHANNEL=${M_LOG[0]}
CATCHUP=${M_CATCHUP[0]}
ARCHIVE_CONTROL=${M_ARCHIVE_CONTROL[0]}
ARCHIVE_REPLICATION=${M_ARCHIVE_REPLICATION[0]}
CLUSTER_REPLICATION=${M_CLUSTER_REPLICATION[0]}
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

    if [ "$member_count" -gt 1 ]; then
        local i host missing
        for ((i = 1; i < member_count; i++)); do
            host=${member_ssh[i - 1]}
            if ! ssh -n -o BatchMode=yes -o ConnectTimeout=5 "$host" true 2>/dev/null; then
                echo "no ssh to $host (member $i runs there)." >&2
                problem=1
                continue
            fi
            # The member runs the node side, so it needs what this machine has:
            # the reference harness (for the arms that use its node) and this
            # build's binaries (for the arms that use ours), at the same paths.
            missing=$(ssh -n -o BatchMode=yes "$host" "
                test -x '$BENCHMARKS_SCRIPTS_DIR/cluster-node' || echo 'no cluster-node launcher'
                test -f '$BENCHMARKS_DIR/benchmarks-all/build/libs/benchmarks.jar' || echo 'harness not built'
                test -x '$BENCH_DRIVER' || echo 'no deepmsg driver'
                test -x '$BENCH_CLUSTER_NODE' || echo 'no consensus module'
                test -x '$BENCH_CLUSTER_CONTAINER' || echo 'no service container'
                test -x '$BENCH_ARCHIVE' || echo 'no archive'
                test -x '$BENCHMARKS_DRIVER_JAVA' || echo 'no reference media-driver'
                test -x '$member_java_home/bin/java' || echo 'no java at $member_java_home'
                command -v taskset >/dev/null || echo 'no taskset'
            " 2>&1)
            if [ -n "$missing" ]; then
                echo "member $i ($host) is missing:" >&2
                echo "$missing" | sed 's/^/  /' >&2
                problem=1
            fi
        done
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
    echo "members string  $MEMBERS"
    echo "client ingress  $CLIENT_INGRESS_ENDPOINTS"
    echo "sync            archive:cluster file sync level, one cell each — $syncs"
    echo "runs            $runs over $iterations s (warmup ${warmup_iterations}s at $warmup_rate)"
    echo "rounds          $(if [ "$rounds_to_run" = 2 ]; then echo 'forward, reverse'; else echo "forward only ($rounds_to_run)"; fi)"
    if [ "$member_count" -eq 1 ]; then
        echo "member          single, host ${M_IP[0]}: ingress ${M_INGRESS[0]#aeron:udp?endpoint=},"
        echo "                consensus ${M_CONSENSUS[0]#aeron:udp?endpoint=}, log ${M_LOG[0]#aeron:udp?endpoint=},"
        echo "                catchup ${M_CATCHUP[0]#aeron:udp?endpoint=}, archive ${M_ARCHIVE_CONTROL[0]#aeron:udp?endpoint=}"
    else
        echo "members         $member_count, one per host:"
        for ((_i = 0; _i < member_count; _i++)); do
            echo "                $((_i)): ${M_IP[_i]}  ingress ${M_INGRESS[_i]#aeron:udp?endpoint=}"
        done
    fi
    if [ "$mode" = two ]; then
        echo "where           this machine ($_batch_ip): driver + node side"
        echo "                peer $BENCH_PEER_SSH ($_peer_ip): the reference's"
        echo "                     media-driver (conductor $peer_driver_conductor_cpu, sender"
        echo "                     $peer_driver_sender_cpu, receiver $peer_driver_receiver_cpu) + cluster-client"
        echo "                     (core $peer_rig_cpu)"
        echo "peer paths      harness $peer_benchmarks; aeron dir $peer_aeron_dir"
        echo "client egress   aeron:udp?endpoint=$CLIENT_EGRESS_HOST:0"
    else
        if [ "$member_count" -eq 1 ]; then
            echo "where           one machine (loopback); the client shares this machine"
        else
            echo "where           $member_count machines, one member each; the client on the peer"
        fi
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
    echo "rounds          $(if [ "$rounds_to_run" = 2 ]; then echo 'forward, reverse'; else echo "forward only ($rounds_to_run)"; fi)"
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
    echo "members         $MEMBERS"
    for ((_i = 0; _i < member_count; _i++)); do
        echo "member $_i        channels:"
        echo "                  ingress             ${M_INGRESS[_i]}"
        echo "                  consensus           ${M_CONSENSUS[_i]}"
        echo "                  log                 ${M_LOG[_i]}"
        echo "                  catchup             ${M_CATCHUP[_i]}"
        echo "                  archive control     ${M_ARCHIVE_CONTROL[_i]}"
        echo "                  archive replication ${M_ARCHIVE_REPLICATION[_i]}"
        echo "                  cluster replication ${M_CLUSTER_REPLICATION[_i]}"
        echo "                  control (ipc)       $CONTROL_CHANNEL"
        echo "                  local archive ctl   $LOCAL_ARCHIVE_CONTROL"
    done
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
# --- the members -------------------------------------------------------------
#
# One member is this machine's; the rest are reached over ssh. Both run the same
# script, `member-node.sh`, from a `member.properties` the batch writes -- two
# code paths, one local and one remote, is how an arm stops being one arm.
#
# * The directories differ per member. Everything a member writes (its aeron
# dir, its cluster and archive directories, its logs) is on its own disk; the
# batch keeps a copy of the logs and the pid list.

member_root=${BENCH_MEMBER_ROOT:-/tmp/cluster-arms-member}
_cell_dir=""

_member_host() { # <i>
    if [ "$1" -eq 0 ]; then printf '' ; else printf '%s' "${member_ssh[$1 - 1]}"; fi
}

_member_dir() { # <i> <run-dir>
    if [ "$1" -eq 0 ]; then printf '%s' "$2"; else printf '%s%s/%s' "$member_root" "$1" "$(basename "$2")"; fi
}

_member_aeron_dir() { # <i>
    if [ "$1" -eq 0 ]; then printf '%s' "$BENCH_AERON_DIR"; else printf '%s%s/aeron' "$member_root" "$1"; fi
}

_member_props() { # <i> <run-dir> <arm> <sync-archive> <sync-cluster>
    local i=$1 dir=$2 arm=$3 sync_archive=$4 sync_cluster=$5
    {
        echo "arm=$arm"
        echo "dir=$(_member_dir "$i" "$dir")"
        echo "aeron_dir=$(_member_aeron_dir "$i")"
        echo "member_id=$i"
        echo "cluster_id=${BENCH_CLUSTER_ID:-0}"
        echo "members=$MEMBERS"
        echo "ingress=${M_INGRESS[i]}"
        echo "consensus=${M_CONSENSUS[i]}"
        echo "log=${M_LOG[i]}"
        echo "catchup=${M_CATCHUP[i]}"
        echo "archive_control=${M_ARCHIVE_CONTROL[i]}"
        echo "archive_replication=${M_ARCHIVE_REPLICATION[i]}"
        echo "cluster_replication=${M_CLUSTER_REPLICATION[i]}"
        echo "control=$CONTROL_CHANNEL"
        echo "local_archive_control=$LOCAL_ARCHIVE_CONTROL"
        echo "sync_archive=$sync_archive"
        echo "sync_cluster=$sync_cluster"
        echo "mtu=1408"
        echo "java_home=$member_java_home"
        echo "benchmarks_dir=$BENCHMARKS_DIR"
        echo "benchmarks_scripts_dir=$BENCHMARKS_SCRIPTS_DIR"
        echo "benchmarks_driver_java=$BENCHMARKS_DRIVER_JAVA"
        echo "bench_driver=$BENCH_DRIVER"
        echo "bench_archive=$BENCH_ARCHIVE"
        echo "bench_cluster_node=$BENCH_CLUSTER_NODE"
        echo "bench_cluster_container=$BENCH_CLUSTER_CONTAINER"
        echo "driver_conductor_cpu=1"
        echo "driver_sender_cpu=2"
        echo "driver_receiver_cpu=3"
        echo "node_cpus=$NODE_CPUS"
        echo "archive_cpus=$ARCHIVE_CPUS"
        echo "cm_cpu=$CM_CPU"
        echo "container_cpu=$CONTAINER_CPU"
        echo "archive_recorder_cpu=4"
        echo "archive_replayer_cpu=5"
        echo "archive_conductor_cpu=6"
    } >"$dir/member.properties"
}

_member_start() { # <i> <run-dir> <arm> <sync-archive> <sync-cluster>
    local i=$1 dir=$2 host mdir
    host=$(_member_host "$i")
    mdir=$(_member_dir "$i" "$dir")
    _member_props "$@"
    if [ -z "$host" ]; then
        bash "$here/member-node.sh" start "$mdir" >"$dir/member-start.log" 2>&1 || {
            tail -20 "$dir/member-start.log" >&2
            return 1
        }
        return 0
    fi
    ssh -n -o BatchMode=yes "$host" "mkdir -p '$mdir'" || return 1
    scp -q "$dir/member.properties" "$here/member-node.sh" "$host:$mdir/" || return 1
    # `ssh -f` + `exec setsid`: the member's processes have to outlive the
    # session that started them, which is the same reason the peer's client is
    # started this way.
    timeout 30 ssh -f -n -o BatchMode=yes "$host" \
        "exec setsid bash $mdir/member-node.sh start $mdir </dev/null >$mdir/start.log 2>&1" || return 1
    for _ in $(seq 1 900); do
        ssh -n -o BatchMode=yes "$host" "test -f $mdir/up || test -f $mdir/failed" 2>/dev/null || { sleep 0.1; continue; }
        ssh -n -o BatchMode=yes "$host" "test -f $mdir/up" 2>/dev/null && return 0
        echo "  member $i ($host) failed to start" >&2
        ssh -n -o BatchMode=yes "$host" "tail -20 $mdir/start.log" >&2 || true
        return 1
    done
    echo "  member $i ($host) did not come up" >&2
    ssh -n -o BatchMode=yes "$host" "tail -20 $mdir/start.log" >&2 || true
    return 1
}

_member_stop() { # <i> <run-dir>
    local i=$1 dir=$2 host mdir
    host=$(_member_host "$i")
    mdir=$(_member_dir "$i" "$dir")
    if [ -z "$host" ]; then
        [ -f "$mdir/member.properties" ] && bash "$here/member-node.sh" stop "$mdir" >/dev/null 2>&1 || true
        return 0
    fi
    ssh -n -o BatchMode=yes "$host" "bash $mdir/member-node.sh stop $mdir" >/dev/null 2>&1 || true
    # The name-based backstop, for a member whose pid list was interrupted.
    # The bracket keeps `pkill` from matching the shell that carries it.
    ssh -n -o BatchMode=yes "$host" "
        pkill -9 -f '[d]eepmsg-cluster-node'; pkill -9 -f '[d]eepmsg-archiv'
        pkill -9 -f '[i]o.aeron.benchmarks.aeron.ClusterNode'
        pkill -9 -f '[i]o.aeron.archive.Archive'; pkill -9 -x deepmsg-driver
        pkill -9 -f '[i]o.aeron.driver.MediaDriver'; true" >/dev/null 2>&1 || true
}

_member_check() { # <i> <run-dir> — did the member survive the cell?
    local i=$1 dir=$2 host mdir
    host=$(_member_host "$i")
    mdir=$(_member_dir "$i" "$dir")
    if [ -z "$host" ]; then
        bash "$here/member-node.sh" check "$mdir" 2>&1
        return $?
    fi
    ssh -n -o BatchMode=yes "$host" "bash $mdir/member-node.sh check $mdir" 2>&1
}

_member_collect() { # <i> <run-dir>
    local i=$1 dir=$2 host mdir
    host=$(_member_host "$i")
    [ -z "$host" ] && return 0
    mdir=$(_member_dir "$i" "$dir")
    mkdir -p "$dir/member$i"
    scp -q "$host:$mdir"/*.log "$dir/member$i/" 2>/dev/null || true
    scp -q "$host:$mdir/pids" "$dir/member$i/" 2>/dev/null || true
}

_stop_all() {
    local i
    if [ -n "${_cell_dir:-}" ]; then
        for ((i = 0; i < member_count; i++)); do
            _member_stop "$i" "$_cell_dir" || true
        done
        _cell_dir=""
    fi
    # The peer's driver is a process of its own and no pid in this shell. The
    # backstop also takes down a client whose ssh session was interrupted, so
    # the next cell does not start on top of it.
    if [ "${mode:-single}" = two ]; then
        _peer_kill_all || true
    fi
}

trap '_stop_all' EXIT

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
        # Half the peer's cores, not a quarter. The 1-minute average still
        # carries the previous cell's four busy-spin threads for a minute after
        # they exit, and a quarter-of-the-cores limit refuses the cell that
        # follows its own cell (measured 2026-10-31: 4.02 > 4 on a 16-CPU
        # peer). A compiler is `nproc` and up, so half still catches it.
        max=$(( $(_peer "nproc") / 2 ))
        [ "$max" -lt 2 ] && max=2
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
        "-Daeron.cluster.ingress.endpoints=$CLIENT_INGRESS_ENDPOINTS " \
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

_cell() {
    local round=$1 arm=$2 sync=$3 rate=$4 length=$5 run=$6
    local sync_archive=${sync%%:*} sync_cluster=${sync##*:}
    local cell="$out/$round/${arm}_sync=${sync_archive}:${sync_cluster}_length=${length}_rate=${rate}"
    local rel="$round/$(basename "$cell")"
    local dir="$cell/run-$run"
    local log="$dir/client.log"
    mkdir -p "$dir"

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

    # Every member starts from nothing; `member-node.sh` is what removes its
    # own aeron, cluster and archive directories.
    _cell_dir="$dir"
    local i
    for ((i = 0; i < member_count; i++)); do
        if ! _member_start "$i" "$dir" "$arm" "$sync_archive" "$sync_cluster"; then
            echo "  $arm $rate sync $sync: member $i did not come up" >&2
            _stop_all
            return 1
        fi
    done

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
    # ★ A three-member cluster runs on a quorum of two, so a member that dies
    # mid-cell leaves a cell that still sends every message and still reads
    # well -- and is not a three-member cell. The pids are what says so.
    for ((i = 0; i < member_count; i++)); do
        if ! _member_check "$i" "$dir"; then
            echo "  $arm $rate sync $sync: member $i did not survive the cell" >&2
            status=1
        fi
    done

    _stop_all
    for ((i = 1; i < member_count; i++)); do
        _member_collect "$i" "$dir" || true
    done

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
# ★ One round is a smoke, not a reading: the reverse round is what makes a
# difference that follows the machine show up as a sign flip.
if [ "$rounds_to_run" -ge 2 ]; then
    _run_round round2-reverse "${rev_list[@]}"
else
    echo "    (one round: this cannot be read as a ratio, it says the shape runs)"
fi

echo
echo "results in $out"

if [ "$read_after" = 1 ]; then
    echo
    python3 "$here/read.py" "$out" || true
fi
