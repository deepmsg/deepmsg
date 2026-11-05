#!/usr/bin/env bash
#
# One echo cell with its two halves on two machines.
#
# Sourced after `checkouts.sh`, `fingerprint.sh`, `cell.sh`, `lan.sh` and
# `peer.sh`. The batch decides which arm a cell is made of; this file knows the
# three shapes an echo cell has and nothing about which batch is calling.
#
# **The local machine is the server; the peer is the client.** That is the
# reference's own layout (`scripts/aeron/README.md:6-11`: the client is the
# `LoadTestRig`, the server is the node, and they are advised to be different
# machines), and it keeps the instrument — the thing that writes the histogram —
# on the machine whose load gate the batch already trusts.
#
# The channels are two settings, not a convention:
#
#   destination.channel  the client publishes here; the server subscribes here
#   source.channel       the server publishes here; the client subscribes here
#
# so both halves are handed one file that names both addresses explicitly. (The
# archive batch needs two files because its response channel is `:0` on each
# side; an echo cell with fixed ports does not.) The addresses are the ones the
# batch derived before the first cell: `_two_server_ip` is this machine's address
# on the link to the peer, `_two_client_ip` is the peer's.
#
# Reads, from the batch:
#
#   two_host_destination_port, two_host_source_port  the two ports (both hosts
#                                                   must agree; they are written
#                                                   into RUN-INFO)
#   two_host_mtu                                    the channel MTU (default 1408)
#   peer_repo                                       this build's checkout on the peer
#   peer_benchmarks_dir                             the reference harness on the peer
#   peer_java_home                                  the peer's JDK
#   peer_rig_cpu                                    the peer's client core
#   peer_driver_conductor_cpu, peer_driver_sender_cpu,
#   peer_driver_receiver_cpu                        the peer's driver's three cores
#   BENCH_CLIENT_TIMEOUT                            optional client deadline (s)
#
# `_two_*` globals hold the pids of whatever is running here; `two_host_stop`
# takes them down by pid, and the peer by the pidfiles its scripts write.

two_host_destination_port="${two_host_destination_port:-23433}"
two_host_source_port="${two_host_source_port:-23434}"
two_host_mtu="${two_host_mtu:-1408}"

peer_java_home="${peer_java_home:-/usr/lib/jvm/java-21-openjdk-amd64}"
peer_rig_cpu="${peer_rig_cpu:-8}"
peer_driver_conductor_cpu="${peer_driver_conductor_cpu:-2}"
peer_driver_sender_cpu="${peer_driver_sender_cpu:-4}"
peer_driver_receiver_cpu="${peer_driver_receiver_cpu:-6}"

_two_server_ip=""
_two_client_ip=""
_two_driver_pid=""
_two_server_pid=""
_two_java_driver_pid=""
_two_arm_dir=""

# Call once, after the arguments are parsed and `lan_require` has passed.
two_host_init() {
    _two_server_ip=$(_lan_local_ip)
    _two_client_ip=$BENCH_PEER_IP
    if [ -z "$_two_server_ip" ] || [ -z "$_two_client_ip" ]; then
        echo "two_host_init: cannot name both ends of the link" >&2
        return 1
    fi
    if [ "$_two_server_ip" = "$_two_client_ip" ]; then
        echo "two_host_init: this machine and the peer have the same address" >&2
        return 1
    fi
    return 0
}

# The settings file both halves read. Written once per cell, into the cell's
# run directory, and copied to the peer by `_two_peer_prepare`.
_two_channels_file() {
    local dir=$1
    local file="$dir/channels.properties"
    cat >"$file" <<EOF
io.aeron.benchmarks.aeron.destination.channel=aeron:udp?endpoint=$_two_server_ip:$two_host_destination_port|mtu=$two_host_mtu
io.aeron.benchmarks.aeron.source.channel=aeron:udp?endpoint=$_two_client_ip:$two_host_source_port|mtu=$two_host_mtu
EOF
    echo "$file"
}

# This machine's driver settings: the aligned file, with the CnC directory the
# batch uses. The aligned file's three core numbers are the local driver's and
# stay as they are.
_two_local_driver_conf() {
    local dir=$1
    local file="$dir/driver.properties"
    sed "s#^aeron.dir=.*#aeron.dir=$BENCH_AERON_DIR#" \
        "$BENCH_DIR/lib/conf/benchmark.properties" >"$file"
    echo "$file"
}

# Only the aeron.dir has to change for the local driver: the aligned file is
# passed after the reference's own, so it wins for the three throughput knobs,
# and this file comes last and wins for the directory.
_two_local_driver_override() {
    local dir=$1
    local file="$dir/driver-override.properties"
    printf 'aeron.dir=%s\n' "$BENCH_AERON_DIR" >"$file"
    echo "$file"
}

# The peer's driver settings: the same aligned file, with the peer's CnC
# directory and the peer's three core numbers. The peer's cores matter because
# the peer's driver is what the client half runs on, and its conductor, sender
# and receiver have to be somewhere.
_two_peer_driver_conf() {
    local dir=$1
    local file="$dir/peer-driver.properties"
    sed -e "s#^aeron.dir=.*#aeron.dir=$BENCH_PEER_AERON_DIR#" \
        -e "s#^aeron.conductor.cpu.affinity=.*#aeron.conductor.cpu.affinity=$peer_driver_conductor_cpu#" \
        -e "s#^aeron.sender.cpu.affinity=.*#aeron.sender.cpu.affinity=$peer_driver_sender_cpu#" \
        -e "s#^aeron.receiver.cpu.affinity=.*#aeron.receiver.cpu.affinity=$peer_driver_receiver_cpu#" \
        "$BENCH_DIR/lib/conf/benchmark.properties" >"$file"
    echo "$file"
}

# --- the server half, here ----------------------------------------------------

#   two_host_server_start <java|rust|c> <dir> <channels> [<arm-dir>]
two_host_server_start() {
    local kind=$1 dir=$2 channels=$3 arm_dir=${4:-}
    local local_conf
    local_conf=$(_two_local_driver_conf "$dir")

    case "$kind" in
        java)
            "$BENCHMARKS_DRIVER_JAVA" "$local_conf" >"$dir/driver.log" 2>&1 &
            _two_java_driver_pid=$!
            _two_wait_local_cnc "$dir" || return 1
            JVM_OPTS="-Daeron.dir=$BENCH_AERON_DIR \
-Dio.aeron.benchmarks.aeron.embedded.media.driver=false \
-Dio.aeron.benchmarks.output.directory=$dir" \
                taskset -c "$BENCH_NODE_CPU" "$BENCHMARKS_ECHO_SERVER" "$channels" \
                >"$dir/node.log" 2>&1 &
            _two_server_pid=$!
            ;;
        rust)
            local override
            override=$(_two_local_driver_override "$dir")
            DEEPMSG_DRIVER="$BENCH_DRIVER" \
                "$BENCH_LIB/shims/aeronmd" \
                "$BENCHMARKS_SCRIPTS_DIR/low-latency-driver.properties" \
                "$local_conf" "$override" >"$dir/driver.log" 2>&1 &
            _two_driver_pid=$!
            _two_wait_local_cnc "$dir" || return 1
            BENCH_CLIENT_PROPERTIES="$channels" \
                taskset -c "$BENCH_NODE_CPU" "$BENCH_NODE" \
                "-Daeron.dir=$BENCH_AERON_DIR" \
                "-Dio.aeron.benchmarks.output.directory=$dir" \
                "$channels" >"$dir/node.log" 2>&1 &
            _two_server_pid=$!
            ;;
        c)
            # The harness's own checked-in C binaries: its launcher appends its
            # two property files first, and ours last so that it wins.
            "$BENCHMARKS_SCRIPTS_DIR/c-media-driver" "$local_conf" >"$dir/driver.log" 2>&1 &
            _two_driver_pid=$!
            _two_wait_local_cnc "$dir" || return 1
            JVM_OPTS="-Daeron.dir=$BENCH_AERON_DIR \
-Dio.aeron.benchmarks.aeron.embedded.media.driver=false \
-Dio.aeron.benchmarks.output.directory=$dir" \
                taskset -c "$BENCH_NODE_CPU" "$BENCHMARKS_ECHO_SERVER" "$channels" \
                >"$dir/node.log" 2>&1 &
            _two_server_pid=$!
            ;;
        c-*)
            # A built arm: `aeronmd` and its libraries, no launcher beside them.
            # The arm dir's own `aeronmd` is what the harness's installed
            # launcher would exec; this is the same command without depending on
            # that launcher being installed on this machine.
            LD_LIBRARY_PATH="$arm_dir${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
                "$arm_dir/aeronmd" \
                "$BENCHMARKS_SCRIPTS_DIR/low-latency-driver.properties" \
                "$BENCHMARKS_SCRIPTS_DIR/benchmark.properties" \
                "$local_conf" >"$dir/driver.log" 2>&1 &
            _two_driver_pid=$!
            _two_wait_local_cnc "$dir" || return 1
            JVM_OPTS="-Daeron.dir=$BENCH_AERON_DIR \
-Dio.aeron.benchmarks.aeron.embedded.media.driver=false \
-Dio.aeron.benchmarks.output.directory=$dir" \
                taskset -c "$BENCH_NODE_CPU" "$BENCHMARKS_ECHO_SERVER" "$channels" \
                >"$dir/node.log" 2>&1 &
            _two_server_pid=$!
            ;;
        *)
            echo "two_host_server_start: unknown kind '$kind'" >&2
            return 1 ;;
    esac
    # The node and the client are not connected yet; the reference's own
    # single-host cell gives them the same two seconds.
    sleep 2
    return 0
}

_two_wait_local_cnc() {
    local dir=$1
    local _
    for _ in $(seq 1 200); do
        [ -f "$BENCH_AERON_DIR/cnc.dat" ] && return 0
        sleep 0.1
    done
    echo "  the driver never published its CnC file" >&2
    tail -10 "$dir/driver.log" 2>/dev/null >&2 || true
    return 1
}

# --- the client half, on the peer ---------------------------------------------

_two_peer_driver_script() {
    local kind=$1 dir=$2 arm_dir=${3:-}
    local script="$dir/peer-driver.sh"
    case "$kind" in
        java)
            cat >"$script" <<EOF
#!/usr/bin/env bash
set -euo pipefail
echo \$\$ >"$BENCH_PEER_ROOT/driver.pid"
export JAVA_HOME="$peer_java_home"
cd "$peer_benchmarks_dir"
exec ./scripts/aeron/media-driver "$BENCH_PEER_ROOT/driver.properties"
EOF
            ;;
        rust)
            cat >"$script" <<EOF
#!/usr/bin/env bash
set -euo pipefail
echo \$\$ >"$BENCH_PEER_ROOT/driver.pid"
cd "$peer_repo"
exec env DEEPMSG_DRIVER="$peer_repo/target/release/deepmsg-driver" \\
    BENCH_OUT_DIR="$BENCH_PEER_ROOT/shims" \\
    ./bench/lib/shims/aeronmd \\
    "$peer_benchmarks_dir/scripts/aeron/low-latency-driver.properties" \\
    "$BENCH_PEER_ROOT/driver.properties"
EOF
            ;;
        c)
            cat >"$script" <<EOF
#!/usr/bin/env bash
set -euo pipefail
echo \$\$ >"$BENCH_PEER_ROOT/driver.pid"
cd "$peer_benchmarks_dir"
exec ./scripts/aeron/c-media-driver "$BENCH_PEER_ROOT/driver.properties"
EOF
            ;;
        c-*)
            cat >"$script" <<EOF
#!/usr/bin/env bash
set -euo pipefail
echo \$\$ >"$BENCH_PEER_ROOT/driver.pid"
export LD_LIBRARY_PATH="$BENCH_PEER_ROOT/arms/$(basename "$arm_dir")"
exec "$BENCH_PEER_ROOT/arms/$(basename "$arm_dir")/aeronmd" \\
    "$peer_benchmarks_dir/scripts/aeron/low-latency-driver.properties" \\
    "$peer_benchmarks_dir/scripts/aeron/benchmark.properties" \\
    "$BENCH_PEER_ROOT/driver.properties"
EOF
            ;;
        *)
            echo "two_host_client_start: unknown kind '$kind'" >&2
            return 1 ;;
    esac
    echo "$script"
}

_two_peer_client_script() {
    local kind=$1 dir=$2 run=$3 prefix=$4 rate=$5 length=$6
    local script="$dir/peer-client.sh"
    case "$kind" in
        java|c|c-*)
            cat >"$script" <<EOF
#!/usr/bin/env bash
set -euo pipefail
echo \$\$ >"$BENCH_PEER_ROOT/client.pid"
export JAVA_HOME="$peer_java_home"
export JVM_OPTS="-Daeron.dir=$BENCH_PEER_AERON_DIR \\
-Dio.aeron.benchmarks.aeron.embedded.media.driver=false \\
-Dio.aeron.benchmarks.output.directory=$BENCH_PEER_ROOT/$run \\
-Dio.aeron.benchmarks.output.file=$prefix \\
-Dio.aeron.benchmarks.message.rate=$rate \\
-Dio.aeron.benchmarks.warmup.message.rate=$warmup_rate \\
-Dio.aeron.benchmarks.iterations=$iterations \\
-Dio.aeron.benchmarks.warmup.iterations=$warmup_iterations \\
-Dio.aeron.benchmarks.message.length=$length \\
-Dio.aeron.benchmarks.batch.size=1 \\
-Dio.aeron.benchmarks.aeron.use.try.claim=true \\
-Dio.aeron.benchmarks.output.time.unit=MICROSECONDS"
cd "$peer_benchmarks_dir"
exec timeout --signal=KILL "${BENCH_CLIENT_TIMEOUT:-900}" \\
    taskset -c $peer_rig_cpu ./scripts/aeron/echo-client "$BENCH_PEER_ROOT/channels.properties"
EOF
            ;;
        rust)
            cat >"$script" <<EOF
#!/usr/bin/env bash
set -euo pipefail
echo \$\$ >"$BENCH_PEER_ROOT/client.pid"
cd "$peer_repo"
exec timeout --signal=KILL "${BENCH_CLIENT_TIMEOUT:-900}" \\
    taskset -c $peer_rig_cpu target/rig/release/loadtest-rig \\
    -Dio.aeron.benchmarks.message.transceiver=echo \\
    -Daeron.dir=$BENCH_PEER_AERON_DIR \\
    -Dio.aeron.benchmarks.aeron.embedded.media.driver=false \\
    -Dio.aeron.benchmarks.output.directory=$BENCH_PEER_ROOT/$run \\
    -Dio.aeron.benchmarks.output.file=$prefix \\
    -Dio.aeron.benchmarks.message.rate=$rate \\
    -Dio.aeron.benchmarks.warmup.message.rate=$warmup_rate \\
    -Dio.aeron.benchmarks.iterations=$iterations \\
    -Dio.aeron.benchmarks.warmup.iterations=$warmup_iterations \\
    -Dio.aeron.benchmarks.message.length=$length \\
    -Dio.aeron.benchmarks.batch.size=1 \\
    -Dio.aeron.benchmarks.aeron.use.try.claim=true \\
    -Dio.aeron.benchmarks.output.time.unit=MICROSECONDS \\
    "$BENCH_PEER_ROOT/channels.properties"
EOF
            ;;
        *)
            echo "two_host_client_start: unknown kind '$kind'" >&2
            return 1 ;;
    esac
    echo "$script"
}

#   two_host_client_start <java|rust|c> <dir> <channels> <run> <prefix> <rate> <length> [<arm-dir>]
#
# Returns the client's exit status; the caller treats a non-zero as a failed
# cell. The histogram is copied back before the peer's driver is taken down.
two_host_client_start() {
    local kind=$1 dir=$2 channels=$3 run=$4 prefix=$5 rate=$6 length=$7 arm_dir=${8:-}
    local peer_conf
    peer_conf=$(_two_peer_driver_conf "$dir")

    _peer_scp "$peer_conf" "$BENCH_PEER_SSH:$BENCH_PEER_ROOT/driver.properties" || return 1
    _peer_scp "$channels" "$BENCH_PEER_SSH:$BENCH_PEER_ROOT/channels.properties" || return 1
    case "$kind" in
        c-*)
            # A built arm travels with the cell: the peer has the harness but
            # not this batch's arms, and a build on the peer would be a second
            # compiler in a paired reading.
            _peer_scp -r "$arm_dir" "$BENCH_PEER_SSH:$BENCH_PEER_ROOT/arms/" || return 1
            ;;
    esac

    local driver_script client_script
    driver_script=$(_two_peer_driver_script "$kind" "$dir" "$arm_dir") || return 1
    client_script=$(_two_peer_client_script "$kind" "$dir" "$run" "$prefix" "$rate" "$length") || return 1
    _peer_scp "$client_script" "$BENCH_PEER_SSH:$BENCH_PEER_ROOT/$(basename "$client_script")" || return 1
    _peer "mkdir -p $BENCH_PEER_ROOT/$run" || return 1

    _peer_start "$driver_script" "$BENCH_PEER_ROOT/driver.log" "$BENCH_PEER_ROOT/driver.pid" || return 1
    _peer_await_file "$BENCH_PEER_AERON_DIR/cnc.dat" || {
        echo "  the peer's driver never published its CnC file" >&2
        _peer "tail -10 $BENCH_PEER_ROOT/driver.log" >&2 || true
        return 1
    }

    local status=0
    timeout --signal=KILL "${BENCH_CLIENT_TIMEOUT:-900}" \
        ssh -n -o BatchMode=yes "$BENCH_PEER_SSH" "bash $BENCH_PEER_ROOT/$(basename "$client_script")" \
        >"$dir/client.log" 2>&1 || status=$?

    _peer_fetch "$BENCH_PEER_ROOT/$run/*.hdr*" "$dir" || true
    _peer_kill "$BENCH_PEER_ROOT/client.pid"
    _peer_kill "$BENCH_PEER_ROOT/driver.pid"
    return "$status"
}

# --- the cell's bookkeeping ---------------------------------------------------

# Everything a cell needs before its first process starts: a clean CnC on both
# machines, one channels file, and the peer's work root.
two_host_prepare_cell() {
    local dir=$1
    rm -rf "$BENCH_AERON_DIR"
    _peer_prepare || return 1
    _peer "mkdir -p $BENCH_PEER_ROOT/arms" || true
    _two_channels_file "$dir" >/dev/null
}

# Local pids by number, the peer by the pidfiles its scripts wrote, and then the
# two CnC directories. A driver that is asked down stamps its counters
# `RECLAIMED` (see `cell.sh`); both halves are killed.
two_host_stop() {
    local dir=$1 pid
    for pid in ${_two_server_pid:-} ${_two_driver_pid:-} ${_two_java_driver_pid:-}; do
        [ -n "$pid" ] && kill -9 "$pid" 2>/dev/null || true
    done
    _two_server_pid=""; _two_driver_pid=""; _two_java_driver_pid=""
    _peer_kill_all
    _peer "rm -rf $BENCH_PEER_ROOT $BENCH_PEER_AERON_DIR" 2>/dev/null || true
    rm -rf "$BENCH_AERON_DIR"
    [ -n "$dir" ] || true
}

# The peer's binaries are part of the machine, so they are part of the record.
two_host_peer_digests() {
    _peer "sha256sum $peer_repo/target/release/deepmsg-driver \
$peer_repo/target/rig/release/loadtest-rig 2>/dev/null" 2>/dev/null || true
}
