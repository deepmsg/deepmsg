#!/usr/bin/env bash
#
# A three-member cluster across three machines, the client on the fourth — run
# with the reference's own harness (`scripts/aeron/remote-cluster-benchmarks`).
#
#   run-cluster-4host.sh [--quick] [--rounds 1,2] [--fsync 0,2] [--list]
#
# Every arm here is the reference's node; what changes between the two scenarios
# of a round is the **media driver** under it:
#
#   java     the reference's Java driver
#   deepmsg  this build's driver, through the harness's `deepmsg-media-driver`
#
# So this is the cluster's counterpart of the cloud plan's step 1: it says what
# this build's driver is worth under a whole reference cluster, and nothing about
# this build's consensus module. The four-arm comparison (j / j-d / r-a / r) is
# `bench/cluster-arms/run-batch.sh`, which runs three members over three hosts
# with the client on a fourth.
#
# The scenario order is reversed in the second round, so a difference that
# follows the machine rather than the driver flips sign.
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=hosts.env
. "$here/hosts.env"

quick=0
rounds="1,2"
fsyncs=""
list_only=0
while [ $# -gt 0 ]; do
    case "$1" in
        --quick) quick=1; shift ;;
        --rounds) rounds=$2; shift 2 ;;
        --fsync) fsyncs=$2; shift 2 ;;
        --list) list_only=1; shift ;;
        -h|--help) sed -n '3,25p' "${BASH_SOURCE[0]}"; exit 0 ;;
        *) echo "unknown option $1" >&2; exit 2 ;;
    esac
done

cluster_require_hosts || exit 2
[ -n "$fsyncs" ] || fsyncs=$CLUSTER_FSYNC_LEVELS

if [ "$quick" = 1 ]; then
    CLUSTER_RATES=25K
    CLUSTER_LENGTHS=32
    CLUSTER_ITERATIONS=5
    CLUSTER_WARMUP_ITERATIONS=3
    rounds=1
fi

bench_dir=$(dirname "$here")
out=${OUT_DIR:-$bench_dir/out/cluster-4host/$(date +%Y-%m-%d-%H%M)}

# --- the four hosts, in the shape the harness reads --------------------------
#
# The harness generates a script per member and ships it over ssh; every path
# below therefore has to mean the same thing on all four machines.

export SSH_NODE0_USER=root SSH_NODE1_USER=root SSH_NODE2_USER=root SSH_CLIENT_USER=root
export SSH_NODE0_KEY_FILE=$CLUSTER_SSH_KEY SSH_NODE1_KEY_FILE=$CLUSTER_SSH_KEY \
    SSH_NODE2_KEY_FILE=$CLUSTER_SSH_KEY SSH_CLIENT_KEY_FILE=$CLUSTER_SSH_KEY
export SSH_CLUSTER_NODE0=$(_cluster_host_of "$CLUSTER_NODE0_SSH")
export SSH_CLUSTER_NODE1=$(_cluster_host_of "$CLUSTER_NODE1_SSH")
export SSH_CLUSTER_NODE2=$(_cluster_host_of "$CLUSTER_NODE2_SSH")
export SSH_CLUSTER_USER0=root SSH_CLUSTER_USER1=root SSH_CLUSTER_USER2=root
export SSH_CLUSTER_KEY_FILE0=$CLUSTER_SSH_KEY SSH_CLUSTER_KEY_FILE1=$CLUSTER_SSH_KEY \
    SSH_CLUSTER_KEY_FILE2=$CLUSTER_SSH_KEY
export SSH_CLIENT_NODE=$(_cluster_host_of "$CLUSTER_CLIENT_SSH")
# The runner the helper sources also insists on a "server" identity; on this
# topology node 0 is it.
export SSH_SERVER_USER=root SSH_SERVER_KEY_FILE=$CLUSTER_SSH_KEY
export SSH_SERVER_NODE=$SSH_CLUSTER_NODE0

export CLUSTER_SIZE=3
export CLUSTER_ID
export CLUSTER_BACKUP_NODES=0
export CLUSTER_APPOINTED_LEADER_ID=0
export CLUSTER_MEMBERS
CLUSTER_MEMBERS=$(cluster_members_string)
export CLUSTER_CONSENSUS_ENDPOINTS=""
export CLIENT_INGRESS_ENDPOINTS=$(cluster_ingress_endpoints)

# The client: reference driver, its own aeron dir, egress back to itself.
export CLIENT_JAVA_HOME=$CLUSTER_JAVA_HOME
export CLIENT_BENCHMARKS_PATH=$CLUSTER_BENCHMARKS_PATH
export CLIENT_CPU_NODE=0
export CLIENT_NON_ISOLATED_CPU_CORES=$CLIENT_NON_ISOLATED_CPU_CORES
export CLIENT_DRIVER_CONDUCTOR_CPU_CORE=$CLIENT_DRIVER_CONDUCTOR_CPU
export CLIENT_DRIVER_SENDER_CPU_CORE=$CLIENT_DRIVER_SENDER_CPU
export CLIENT_DRIVER_RECEIVER_CPU_CORE=$CLIENT_DRIVER_RECEIVER_CPU
export CLIENT_LOAD_TEST_RIG_MAIN_CPU_CORE=$CLIENT_LOAD_TEST_RIG_CPU
export CLIENT_AERON_DPDK_GATEWAY_IPV4_ADDRESS=""
export CLIENT_AERON_DPDK_LOCAL_IPV4_ADDRESS=""
export CLIENT_INGRESS_CHANNEL="aeron:udp?mtu=$CLUSTER_MTU"
export CLIENT_EGRESS_CHANNEL="aeron:udp?endpoint=${SSH_CLIENT_NODE}:0|mtu=$CLUSTER_MTU"

# ★ The driver arm on the member side needs `DEEPMSG_DIR` in the harness's
# environment; the generated per-node script inherits what is exported here and
# what this file puts into the shell that runs the harness on node0. The remote
# members get it from the same `export` because the harness ships its own
# command line, not this shell's environment — so it is set in the script each
# member runs, through `NODE<n>_EXTRA_PROPERTIES`'s sibling below.
export DEEPMSG_DIR=$CLUSTER_DEEPMSG_DIR

_set_node() { # <n> <ip>
    local n=$1 ip=$2
    declare -gx "NODE${n}_JAVA_HOME=$CLUSTER_JAVA_HOME"
    declare -gx "NODE${n}_BENCHMARKS_PATH=$CLUSTER_BENCHMARKS_PATH"
    declare -gx "NODE${n}_CPU_NODE=0"
    declare -gx "NODE${n}_NON_ISOLATED_CPU_CORES=$NON_ISOLATED_CPU_CORES"
    declare -gx "NODE${n}_DRIVER_CONDUCTOR_CPU_CORE=$DRIVER_CONDUCTOR_CPU"
    declare -gx "NODE${n}_DRIVER_SENDER_CPU_CORE=$DRIVER_SENDER_CPU"
    declare -gx "NODE${n}_DRIVER_RECEIVER_CPU_CORE=$DRIVER_RECEIVER_CPU"
    declare -gx "NODE${n}_AERON_DPDK_GATEWAY_IPV4_ADDRESS="
    declare -gx "NODE${n}_AERON_DPDK_LOCAL_IPV4_ADDRESS="
    declare -gx "NODE${n}_ARCHIVE_RECORDER_CPU_CORE=$ARCHIVE_RECORDER_CPU"
    declare -gx "NODE${n}_ARCHIVE_REPLAYER_CPU_CORE=$ARCHIVE_REPLAYER_CPU"
    declare -gx "NODE${n}_ARCHIVE_CONDUCTOR_CPU_CORE=$ARCHIVE_CONDUCTOR_CPU"
    declare -gx "NODE${n}_CONSENSUS_MODULE_CPU_CORE=$CONSENSUS_MODULE_CPU"
    declare -gx "NODE${n}_CLUSTERED_SERVICE_CPU_CORE=$CLUSTERED_SERVICE_CPU"
    declare -gx "NODE${n}_CLUSTER_DIR=$CLUSTER_DATA_DIR/node${n}/cluster"
    declare -gx "NODE${n}_ARCHIVE_DIR=$CLUSTER_DATA_DIR/node${n}/archive"
    declare -gx "NODE${n}_EXTRA_PROPERTIES="
    local p_ingress p_consensus p_log p_catchup p_archive
    read -r p_ingress p_consensus p_log p_catchup p_archive <<<"$(cluster_member_ports "$n")"
    # ★ The shape is `scripts/aws-remote-testing.sh`'s, which is the reference's
    # own three-members-on-three-hosts configuration: ingress and consensus
    # carry **no endpoint** (the member's own address comes from the members
    # string), the log is a manual-control-channel publication whose `control`
    # endpoint is this member's own log port, and the replication channel is an
    # ephemeral endpoint of this host.
    declare -gx "NODE${n}_CLUSTER_INGRESS_CHANNEL=aeron:udp?mtu=$CLUSTER_MTU"
    declare -gx "NODE${n}_CLUSTER_CONSENSUS_CHANNEL=aeron:udp?term-length=64k|mtu=$CLUSTER_MTU"
    declare -gx "NODE${n}_CLUSTER_LOG_CHANNEL=aeron:udp?term-length=64m|control-mode=manual|control=${ip}:${p_log}|mtu=$CLUSTER_MTU"
    declare -gx "NODE${n}_CLUSTER_REPLICATION_CHANNEL=aeron:udp?endpoint=${ip}:0"
    declare -gx "NODE${n}_ARCHIVE_CONTROL_CHANNEL=aeron:udp?endpoint=${ip}:${p_archive}"
    declare -gx "NODE${n}_ARCHIVE_REPLICATION_CHANNEL=aeron:udp?endpoint=${ip}:0"
}

_set_node 0 "$SSH_CLUSTER_NODE0"
_set_node 1 "$SSH_CLUSTER_NODE1"
_set_node 2 "$SSH_CLUSTER_NODE2"

# --- preflight ---------------------------------------------------------------
#
# Every check here is a thing that would otherwise show up as a cluster that
# never forms, which reads exactly like a cluster bug.

_hosts_all() {
    local host
    for host in "$CLUSTER_NODE0_SSH" "$CLUSTER_NODE1_SSH" "$CLUSTER_NODE2_SSH" "$CLUSTER_CLIENT_SSH"; do
        printf '%s\n' "$host"
    done
}

_ssh_key_opt=()
[ -f "$CLUSTER_SSH_KEY" ] && _ssh_key_opt=(-i "$CLUSTER_SSH_KEY")
_on() { ssh -n "${_ssh_key_opt[@]+"${_ssh_key_opt[@]}"}" -o BatchMode=yes -o ConnectTimeout=8 "$1" "${@:2}"; }

preflight() {
    local host bad=0
    while read -r host; do
        if ! _on "$host" true 2>/dev/null; then
            echo "preflight: $host is not reachable over ssh (key-based ssh from the controller to all four, including itself; see README)" >&2
            bad=1
            continue
        fi
        local missing
        missing=$(_on "$host" "
            test -x '$CLUSTER_BENCHMARKS_PATH/scripts/aeron/cluster-node' || echo '  no cluster-node script'
            test -x '$CLUSTER_BENCHMARKS_PATH/scripts/aeron/deepmsg-media-driver' || echo '  no deepmsg-media-driver'
            test -x '$CLUSTER_JAVA_HOME/bin/java' || echo '  no java at $CLUSTER_JAVA_HOME'
            test -d '$CLUSTER_DEEPMSG_DIR/target/release' || echo '  no deepmsg build'
            pgrep -x numactl >/dev/null && echo '  a numactl is still running'
        " 2>/dev/null || true)
        if [ -n "$missing" ]; then
            printf '%s\n' "$missing" | sed "s|^|preflight: $host:|" >&2
            bad=1
        fi
        _on "$host" "pgrep -f '[i]o.aeron' >/dev/null || pgrep -x deepmsg-driver >/dev/null" 2>/dev/null && {
            echo "preflight: $host still has an aeron process" >&2
            bad=1
        }
    done < <(_hosts_all)

    # The driver binary has to be the same one under every member, or the arm is
    # not one arm.
    local digests
    digests=$(while read -r host; do
        _on "$host" "sha256sum '$CLUSTER_DEEPMSG_DIR/target/release/deepmsg-driver' 2>/dev/null | cut -d' ' -f1" || true
    done < <(_hosts_all) | sort -u | tr '\n' ' ')
    if [ "$(printf '%s' "$digests" | wc -w)" -ne 1 ]; then
        echo "preflight: the driver digests differ across the four hosts: $digests" >&2
        bad=1
    fi
    [ "$bad" -eq 0 ] || return 1
}

if [ "$list_only" = 1 ]; then
    echo "=== plan ==="
    echo "members         $(printf '%s ' "$SSH_CLUSTER_NODE0" "$SSH_CLUSTER_NODE1" "$SSH_CLUSTER_NODE2")"
    echo "client          $SSH_CLIENT_NODE"
    echo "cluster id      $CLUSTER_ID"
    echo "members string  $CLUSTER_MEMBERS"
    echo "ingress         $CLIENT_INGRESS_ENDPOINTS"
    echo "rates/lengths   $CLUSTER_RATES / $CLUSTER_LENGTHS"
    echo "iterations      $CLUSTER_ITERATIONS s + $CLUSTER_WARMUP_ITERATIONS s warmup at $CLUSTER_WARMUP_RATE"
    echo "fsync           $fsyncs"
    echo "rounds          $rounds"
    echo "results         $out"
    if preflight; then
        echo "prerequisites: ok"
        # The plan is the whole answer. Without this exit the script fell
        # through and ran the batch that --list was only asked to describe.
        exit 0
    else
        echo "prerequisites: NOT met (see above); nothing was run." >&2
        exit 2
    fi
fi

preflight || { echo "nothing was run." >&2; exit 1; }
mkdir -p "$out"
{
    echo "# cluster-4host"
    echo "started         $(date -Is)"
    echo "members         node0=$SSH_CLUSTER_NODE0 node1=$SSH_CLUSTER_NODE1 node2=$SSH_CLUSTER_NODE2"
    echo "client          $SSH_CLIENT_NODE"
    echo "cluster id      $CLUSTER_ID"
    echo "members string  $CLUSTER_MEMBERS"
    echo "rates/lengths   $CLUSTER_RATES / $CLUSTER_LENGTHS"
    echo "iterations      $CLUSTER_ITERATIONS s + $CLUSTER_WARMUP_ITERATIONS s at $CLUSTER_WARMUP_RATE"
    echo "fsync           $fsyncs"
    echo "driver digest   $(sha256sum "$CLUSTER_DEEPMSG_DIR/target/release/deepmsg-driver" 2>/dev/null | cut -d' ' -f1)"
    echo "cores           driver $DRIVER_CONDUCTOR_CPU/$DRIVER_SENDER_CPU/$DRIVER_RECEIVER_CPU, archive $ARCHIVE_RECORDER_CPU/$ARCHIVE_REPLAYER_CPU/$ARCHIVE_CONDUCTOR_CPU, cm $CONSENSUS_MODULE_CPU, service $CLUSTERED_SERVICE_CPU; client driver $CLIENT_DRIVER_CONDUCTOR_CPU/$CLIENT_DRIVER_SENDER_CPU/$CLIENT_DRIVER_RECEIVER_CPU, rig $CLIENT_LOAD_TEST_RIG_CPU"
} >"$out/RUN-INFO.txt"

run_round() { # <name> <drivers>
    local name=$1 drivers=$2
    echo "########## $name ($drivers) $(date -Is) ##########"
    ( cd "$CLUSTER_BENCHMARKS_PATH" && \
      MESSAGE_RATE="$CLUSTER_RATES" MESSAGE_LENGTH="$CLUSTER_LENGTHS" \
      ITERATIONS="$CLUSTER_ITERATIONS" WARMUP_ITERATIONS="$CLUSTER_WARMUP_ITERATIONS" \
      WARMUP_MESSAGE_RATE="$CLUSTER_WARMUP_RATE" BURST_SIZE=1 USE_TRY_CLAIM=true \
      OUTPUT_TIME_UNIT=MICROSECONDS RUNS=1 \
      ./scripts/aeron/remote-cluster-benchmarks \
          --client-drivers "$drivers" --server-drivers "$drivers" \
          --mtu "$CLUSTER_MTU" --file-sync-level "$fsyncs" \
          --context "$name" --download-dir "$out/$name" ) 2>&1 | tee -a "$out/$name.log"
}

case ",$rounds," in
    *,1,*) run_round "round1-forward" "java,deepmsg" ;;
esac
case ",$rounds," in
    *,2,*) run_round "round2-reverse" "deepmsg,java" ;;
esac

echo
echo "results in $out"
