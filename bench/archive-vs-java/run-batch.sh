#!/usr/bin/env bash
#
# One batch: this build's archive against the reference's, on two machines.
#
#   run-batch.sh [--scenario s2] [--arms j,r-rust] [--fsync 0] [--rate 101K]
#                [--length 1344] [--runs 1] [--iterations 60] [--out DIR]
#                [--peer user@host] [--archive-dir DIR] [--list] [--quick]
#                [--no-read]
#
# ## Why two machines, and who is where
#
# The archive is the thing being measured, so it stays on **this** machine
# together with its driver, its cores and its disk — a comparison between two
# archives may differ by exactly that and nothing else. The other machine is the
# load end: the client that publishes (S2) or the node that answers (S1). See
# `analysis/bench/deepmsg-bench-archive-two-machines-explained.md`.
#
# The network is therefore a **precondition, not a variable**: `lib/lan.sh`
# proves a UDP datagram makes the round trip in both directions before the first
# cell, and a cell that starts on a busy peer is refused rather than measured.
#
# ## What this slice runs
#
# `s2` — the reference's live-replay shape: a client publishes to an archive
# node, the node forwards into a recording, the archive replays it back. Two arms
# run today:
#
#   j        the reference's `archive-node` and `live-replay-client`, both Java,
#            each launching its own driver
#   r-rust   this build's driver + `deepmsg-archive` + `archive-node` on this
#            machine, and this build's driver + `loadtest-rig` on the peer
#
# `r-attach` and `r-java` need `patches/external-archive.patch` (an `ArchiveNode`
# that connects to an archive it does not own) and are refused by name until it
# lands — a cell that silently ran them as `j` would be a number about the wrong
# thing.
#
# ## The tree, which `read.py` reads
#
#   <out>/round-<name>/<scenario>_<arm>_fsync=<0|2>_mtu=<M>_length=<L>_rate=<R>/
#       run-<N>/<*.hdr>  counters.txt  archive-du.txt  client.log
#
# A cell that did not finish is not a row: the batch gates on the client's own
# `WARNING:` line and on the histogram holding what the rate and the iterations
# asked for, exactly as `lib/cell.sh` does.
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

# The two halves of a cell share a directory name on their own machines: each
# host has its own `/dev/shm`, so the path is the same string on both and means
# "here". It is deliberately not `/dev/shm/aeron` — another agent may be running
# cluster tests on the peer under that name.
BENCH_AERON_DIR="${BENCH_ARCHIVE_AERON_DIR:-/dev/shm/deepmsg-bench}"
export BENCH_AERON_DIR

scenario=s2
mode=single
arms="j,r-rust"
fsync_levels="0"
rate=101K
length=1344
mtu=1408
runs=1
iterations=60
warmup_iterations=30
warmup_rate=25K
archive_dir=${ARCHIVE_DIR:-/home/gavin/archive-bench}
peer=""
peer_repo=${BENCH_PEER_REPO:-/home/gavin/data/trader/deepmsg}
peer_rig_cpu=${BENCH_PEER_RIG_CPU:-4}
peer_max_load=${BENCH_PEER_MAX_LOAD:-}
out=""
list_only=0
read_after=1
quick=0

# The archive's own cores. The driver pins its three out of
# `lib/conf/benchmark.properties` (1/2/3); these are the archive's three agents
# and the node, and they leave core 4 for the kernel.
archive_cpus=${BENCH_ARCHIVE_CPUS:-0,6,7}
node_cpu=${BENCH_NODE_CPU:-5}

# The ports a cell's four channels use. Fixed per batch and printed, because
# both hosts have to agree on them and a leftover listener from the previous
# cell is the sort of second variable a run cannot see.
destination_port=${BENCH_DESTINATION_PORT:-23333}
source_port=${BENCH_SOURCE_PORT:-23334}
control_port=${BENCH_CONTROL_PORT:-23200}

while [ $# -gt 0 ]; do
    case "$1" in
        --scenario) scenario=$2; shift 2 ;;
        --arms) arms=$2; shift 2 ;;
        --fsync) fsync_levels=$2; shift 2 ;;
        --rate) rate=$2; shift 2 ;;
        --length) length=$2; shift 2 ;;
        --runs) runs=$2; shift 2 ;;
        --iterations) iterations=$2; shift 2 ;;
        --warmup-iterations) warmup_iterations=$2; shift 2 ;;
        --peer) peer=$2; mode=two; shift 2 ;;
        --two-host) mode=two; shift ;;
        --single-host) mode=single; shift ;;
        --archive-dir) archive_dir=$2; shift 2 ;;
        --out) out=$2; shift 2 ;;
        --list) list_only=1; shift ;;
        --no-read) read_after=0; shift ;;
        --quick)
            quick=1
            arms="r-rust"
            fsync_levels="0"
            rate=25K
            runs=1; iterations=5; warmup_iterations=2
            shift ;;
        -h|--help) sed -n '3,45p' "${BASH_SOURCE[0]}"; exit 0 ;;
        *) echo "unknown option $1" >&2; exit 2 ;;
    esac
done

if [ -n "$peer" ]; then
    BENCH_PEER_SSH=$peer
    BENCH_PEER_IP=${peer#*@}
fi
out=${out:-$BENCH_OUT_DIR/archive-vs-java/$(date +%Y-%m-%d)-$(hostname -s)}

IFS=',' read -ra arm_list <<<"$arms"
IFS=',' read -ra fsync_list <<<"$fsync_levels"

export BENCH_ITERATIONS="$iterations"
export BENCH_WARMUP_ITERATIONS="$warmup_iterations"
export BENCH_WARMUP_RATE="$warmup_rate"

if [ "$mode" = two ]; then
    _batch_ip=$(_lan_local_ip)
    _peer_ip=$BENCH_PEER_IP
else
    _batch_ip=localhost
    _peer_ip=localhost
fi
# The client half's driver. On one machine it cannot share the server's: two
# drivers in one aeron directory is two drivers writing one CnC file. On two
# machines each host has its own `/dev/shm`, so the same name means "here".
_client_aeron_dir="$BENCH_AERON_DIR-client"
_peer_root=/tmp/archive-batch
_local_driver_properties=""
_archive_channels=""
_client_channels=""

# --- ssh to the peer ---------------------------------------------------------

_peer() {
    ssh -n -o BatchMode=yes -o ConnectTimeout=5 "$BENCH_PEER_SSH" "$@"
}

# Start something on the peer and come back. ★ `ssh host "cmd &"` **hangs** —
# sshd keeps the session even with all three descriptors redirected — so the
# process is started under `exec setsid` by an `ssh -f`, which backgrounds the
# ssh client after authentication and returns in a fifth of a second. Measured
# both ways on 2026-10-17.
_peer_start() {
    local script=$1 log=$2 pidfile=$3; shift 3
    scp -q "$script" "$BENCH_PEER_SSH:$_peer_root/$(basename "$script")"
    timeout 30 ssh -f -n -o BatchMode=yes "$BENCH_PEER_SSH" \
        "exec setsid bash $_peer_root/$(basename "$script") </dev/null >$log 2>&1"
    # The pid is written by the script itself, because `ssh -f` has no child to
    # reap and a kill by name would reach whatever else is named alike.
    for _ in $(seq 1 50); do
        _peer "test -s $pidfile" && return 0
        sleep 0.1
    done
    echo "  the peer's $(basename "$script") never wrote $pidfile" >&2
    return 1
}

_peer_kill() {
    local pidfile=$1
    _peer "if [ -s $pidfile ]; then kill -9 \$(cat $pidfile) 2>/dev/null || true; rm -f $pidfile; fi" || true
}

# --- the cell's settings files ----------------------------------------------

# The two halves of a cell are given the same three channels and **different
# response channels**, and the difference is the point: a response channel is
# the one the *client* subscribes on, so each host names its own address with
# port nought and the driver hands it an ephemeral port — the reference's own
# two-machine script does exactly this (`remote-live-replay-benchmarks`: the
# server is given `${DESTINATION_IP}:0`, the client `${SOURCE_IP}:0`), and its
# archive tests use the same shape (`TestContexts.LOCALHOST_CONTROL_RESPONSE_CHANNEL`).
# One shared file would put the peer's address on this machine's subscription and
# fail to bind — measured, first run, `Cannot assign requested address`.
_write_cell_files() {
    local dir=$1
    _archive_channels="$dir/channels-archive.properties"
    _client_channels="$dir/channels-client.properties"
    local head
    head="io.aeron.benchmarks.aeron.destination.channel=aeron:udp?endpoint=$_batch_ip:$destination_port|mtu=$mtu
io.aeron.benchmarks.aeron.source.channel=aeron:udp?endpoint=$_peer_ip:$source_port|mtu=$mtu
io.aeron.benchmarks.aeron.record.channel=aeron:ipc
aeron.archive.control.channel=aeron:udp?endpoint=$_batch_ip:$control_port"
    printf '%s\naeron.archive.control.response.channel=aeron:udp?endpoint=%s:0\n' \
        "$head" "$_batch_ip" >"$_archive_channels"
    printf '%s\naeron.archive.control.response.channel=aeron:udp?endpoint=%s:0\n' \
        "$head" "$_peer_ip" >"$_client_channels"

    # The reference's `media-driver` already passes its own two files; this one
    # comes last and wins, and it is how the cell says where the driver lives.
    _java_driver_override="$dir/driver-override.properties"
    printf 'aeron.dir=%s\n' "$BENCH_AERON_DIR" >"$_java_driver_override"

    _local_driver_properties="$dir/driver.properties"
    sed "s#^aeron.dir=.*#aeron.dir=$BENCH_AERON_DIR#" \
        "$BENCH_DIR/lib/conf/benchmark.properties" >"$_local_driver_properties"
    _client_driver_properties="$dir/client-driver.properties"
    sed "s#^aeron.dir=.*#aeron.dir=$_client_aeron_dir#" \
        "$BENCH_DIR/lib/conf/benchmark.properties" >"$_client_driver_properties"
}

# `lib/conf/archive.properties` is a file the reference's own scripts take as a
# positional argument; this build's archive binary reads `-D` only, so the batch
# is where the two shapes meet.
_archive_defines() {
    local fsync=$1 file line
    file="$BENCH_DIR/lib/conf/archive.properties"
    while IFS= read -r line || [ -n "$line" ]; do
        line=${line%$'\r'}
        [ -z "${line// }" ] && continue
        case "$line" in \#*) continue ;; esac
        printf -- '-D%s\n' "$line"
    done <"$file"
    printf -- '-Daeron.dir=%s\n' "$BENCH_AERON_DIR"
    printf -- '-Daeron.archive.dir=%s\n' "$archive_dir"
    printf -- '-Daeron.archive.mark.file.dir=%s\n' "$BENCH_AERON_DIR"
    printf -- '-Daeron.archive.file.sync.level=%s\n' "$fsync"
    printf -- '-Daeron.archive.catalog.file.sync.level=%s\n' "$fsync"
    # The reference's own live-replay script sets this explicitly, and so does
    # every cell here: the events are the *recording* scenario's pacemaker, and
    # a replay cell that published them would be paying for a channel nobody
    # reads.
    printf -- '-Daeron.archive.recording.events.enabled=false\n'
    printf -- '-Daeron.archive.control.channel=%s\n' "aeron:udp?endpoint=$_batch_ip:$control_port"
    printf -- '-Daeron.archive.control.response.channel=%s\n' "aeron:udp?endpoint=$_batch_ip:0"
    # A UDP control channel obliges a replication channel — the reference refuses
    # to start without one (`Archive.java:1237`) once the control channel is not
    # IPC, and its own two-machine script passes `endpoint=${DESTINATION_IP}:0`.
    printf -- '-Daeron.archive.replication.channel=%s\n' "aeron:udp?endpoint=$_batch_ip:0"
}

# --- readiness ---------------------------------------------------------------

_await_file() {
    local path=$1 path_kind=$2
    case "$path_kind" in
        local) for _ in $(seq 1 200); do [ -e "$path" ] && return 0; sleep 0.1; done ;;
        peer) for _ in $(seq 1 200); do _peer "test -e $path" && return 0; sleep 0.1; done ;;
    esac
    return 1
}

# The archive says so itself, once, when it is serving.
_await_archive() {
    local log=$1
    for _ in $(seq 1 300); do
        grep -q 'archive running' "$log" 2>/dev/null && return 0
        sleep 0.1
    done
    return 1
}

# A recording exists when the archive says one does. Read from the driver's
# counters, which is the same thing the reference's `ArchiveNode` waits on
# (`AeronUtil.awaitRecordingStart`) and works for both arms because both archives
# allocate the same label.
_await_recording() {
    local path=$1 seconds=${2:-30}
    for _ in $(seq 1 $((seconds * 5))); do
        if "$BENCH_CNC_DUMP" --counters "$path" 2>/dev/null |
            awk 'NF >= 5 && $1 ~ /^[0-9]+$/ && $2 ~ /^[0-9]+$/ {
                    label = ""; for (i = 5; i <= NF; i++) label = label (i > 5 ? " " : "") $i;
                    if (label ~ /^Archive Recording Sessions/) { found = ($3 + 0 > 0); exit } }
                 END { exit !found }'; then
            return 0
        fi
        sleep 0.2
    done
    return 1
}

# --- the peer's load is a precondition, not a variable -----------------------

_load_gate() {
    local load max
    load=$(_peer "cut -d' ' -f1 /proc/loadavg" 2>/dev/null || echo "")
    if [ -z "$load" ]; then
        echo "  could not read the peer's load average" >&2
        return 1
    fi
    # A quarter of the peer's cores, unless the batch says otherwise. A machine
    # with a developer's agent on it sits around one; a build or a test run is
    # `nproc` and up. The point is to catch the second, and not to refuse a cell
    # because an editor is open.
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

# --- starting the two halves -------------------------------------------------

_start_java_server() {
    local dir=$1
    # ★ The reference's node does **not** launch a driver: `embeddedMediaDriver()`
    # is `Boolean.getBoolean`, whose default is false
    # (`AeronUtil.java:252`), so `ArchiveNode` takes
    # `launchArchiveWithStandaloneDriver` and waits for a driver somebody else
    # started. Measured: without one it dies on "CnC file not created".
    "$BENCHMARKS_DRIVER_JAVA" "$_java_driver_override" \
        >"$dir/java-driver.log" 2>&1 &
    _java_driver_pid=$!
    _await_file "$BENCH_AERON_DIR/cnc.dat" local || {
        echo "  the Java driver never published its CnC file" >&2
        tail -5 "$dir/java-driver.log" >&2
        return 1
    }
    JVM_OPTS="-Daeron.dir=$BENCH_AERON_DIR \
-Dio.aeron.benchmarks.aeron.destination.channel=aeron:udp?endpoint=$_batch_ip:$destination_port|mtu=$mtu \
-Dio.aeron.benchmarks.aeron.source.channel=aeron:udp?endpoint=$_peer_ip:$source_port|mtu=$mtu \
-Dio.aeron.benchmarks.aeron.record.channel=aeron:ipc \
-Daeron.archive.control.channel=aeron:udp?endpoint=$_batch_ip:$control_port \
-Daeron.archive.control.response.channel=aeron:udp?endpoint=$_batch_ip:0 \
-Daeron.archive.replication.channel=aeron:udp?endpoint=$_batch_ip:0 \
-Daeron.archive.dir=$archive_dir \
-Daeron.archive.file.sync.level=$_fsync \
-Daeron.archive.catalog.file.sync.level=$_fsync \
-Dio.aeron.benchmarks.output.directory=$dir" \
        "$BENCHMARKS_SCRIPTS_DIR/archive-node" >"$dir/archive-node.log" 2>&1 &
    _server_pid=$!
}

_start_rust_server() {
    local dir=$1
    local -a defines=()
    while IFS= read -r line; do defines+=("$line"); done < <(_archive_defines "$_fsync")

    DEEPMSG_DRIVER="$BENCH_DRIVER" BENCH_OUT_DIR="$dir/shims" \
        "$BENCH_LIB/shims/aeronmd" "$_local_driver_properties" \
        </dev/null >"$dir/driver.log" 2>&1 &
    _driver_pid=$!
    _await_file "$BENCH_AERON_DIR/cnc.dat" local || {
        echo "  the driver never published its CnC file" >&2
        tail -5 "$dir/driver.log" >&2
        return 1
    }

    taskset -c "$archive_cpus" "$BENCH_ARCHIVE" "${defines[@]}" \
        </dev/null >"$dir/archive.log" 2>&1 &
    _archive_pid=$!
    _await_archive "$dir/archive.log" || {
        echo "  the archive never said it was running" >&2
        tail -10 "$dir/archive.log" >&2
        return 1
    }

    taskset -c "$node_cpu" "$BENCH_ARCHIVE_NODE" \
        "-Daeron.dir=$BENCH_AERON_DIR" \
        "-Dio.aeron.benchmarks.output.directory=$dir" \
        "$_archive_channels" </dev/null >"$dir/node.log" 2>&1 &
    _node_pid=$!
}

_start_peer_client_driver() {
    local dir=$1
    cat >"$dir/peer-driver.sh" <<EOF
#!/usr/bin/env bash
set -euo pipefail
echo \$\$ >"$_peer_root/driver.pid"
cd "$peer_repo"
exec env DEEPMSG_DRIVER="$peer_repo/target/release/deepmsg-driver" \\
    BENCH_OUT_DIR="$_peer_root/shims" \\
    ./bench/lib/shims/aeronmd "$_peer_root/driver.properties"
EOF
    scp -q "$_local_driver_properties" "$BENCH_PEER_SSH:$_peer_root/driver.properties"
    _peer_start "$dir/peer-driver.sh" "$_peer_root/driver.log" "$_peer_root/driver.pid" || return 1
    _await_file "$BENCH_AERON_DIR/cnc.dat" peer || {
        echo "  the peer's driver never published its CnC file" >&2
        _peer "tail -5 $_peer_root/driver.log" >&2 || true
        return 1
    }
}

_run_rust_client() {
    local dir=$1 run=$2 prefix=$3
    cat >"$dir/peer-client.sh" <<EOF
#!/usr/bin/env bash
set -euo pipefail
cd "$peer_repo"
exec timeout --signal=KILL "${BENCH_CLIENT_TIMEOUT:-900}" taskset -c $peer_rig_cpu target/rig/release/loadtest-rig \\
  -Dio.aeron.benchmarks.message.transceiver=live-replay \\
  -Daeron.dir=$BENCH_AERON_DIR \\
  -Dio.aeron.benchmarks.aeron.embedded.media.driver=false \\
  -Dio.aeron.benchmarks.output.directory=$_peer_root/$run \\
  -Dio.aeron.benchmarks.output.file=$prefix \\
  -Dio.aeron.benchmarks.message.rate=$rate \\
  -Dio.aeron.benchmarks.warmup.message.rate=$warmup_rate \\
  -Dio.aeron.benchmarks.iterations=$iterations \\
  -Dio.aeron.benchmarks.warmup.iterations=$warmup_iterations \\
  -Dio.aeron.benchmarks.message.length=$length \\
  -Dio.aeron.benchmarks.batch.size=1 \\
  -Dio.aeron.benchmarks.aeron.use.try.claim=true \\
  -Dio.aeron.benchmarks.output.time.unit=MICROSECONDS \\
  "$_peer_root/channels.properties"
EOF
    scp -q "$_client_channels" "$BENCH_PEER_SSH:$_peer_root/channels.properties"
    scp -q "$dir/peer-client.sh" "$BENCH_PEER_SSH:$_peer_root/peer-client.sh"
    _peer "mkdir -p $_peer_root/$run"
    timeout --signal=KILL "${BENCH_CLIENT_TIMEOUT:-900}" \
        ssh -n -o BatchMode=yes "$BENCH_PEER_SSH" "bash $_peer_root/peer-client.sh" \
        >"$dir/client.log" 2>&1
}

_run_java_client() {
    local dir=$1 run=$2 prefix=$3
    cat >"$dir/peer-client.sh" <<EOF
#!/usr/bin/env bash
set -euo pipefail
export JAVA_HOME=\${JAVA_HOME:-/usr/lib/jvm/java-21-openjdk-amd64}
export JVM_OPTS="-Daeron.dir=$BENCH_AERON_DIR \
-Dio.aeron.benchmarks.aeron.destination.channel=aeron:udp?endpoint=$_batch_ip:$destination_port|mtu=$mtu \
-Dio.aeron.benchmarks.aeron.source.channel=aeron:udp?endpoint=$_peer_ip:$source_port|mtu=$mtu \
-Dio.aeron.benchmarks.aeron.record.channel=aeron:ipc \
-Daeron.archive.control.channel=aeron:udp?endpoint=$_batch_ip:$control_port \
-Daeron.archive.control.response.channel=aeron:udp?endpoint=$_peer_ip:0 \
-Dio.aeron.benchmarks.output.directory=$_peer_root/$run \
-Dio.aeron.benchmarks.output.file=$prefix \
-Dio.aeron.benchmarks.message.rate=$rate \
-Dio.aeron.benchmarks.warmup.message.rate=$warmup_rate \
-Dio.aeron.benchmarks.iterations=$iterations \
-Dio.aeron.benchmarks.warmup.iterations=$warmup_iterations \
-Dio.aeron.benchmarks.message.length=$length \
-Dio.aeron.benchmarks.batch.size=1 \
-Dio.aeron.benchmarks.aeron.use.try.claim=true \
-Dio.aeron.benchmarks.output.time.unit=MICROSECONDS"
cd "$BENCHMARKS_DIR"
exec timeout --signal=KILL "${BENCH_CLIENT_TIMEOUT:-900}" \\
    taskset -c $peer_rig_cpu ./scripts/aeron/live-replay-client
EOF
    scp -q "$dir/peer-client.sh" "$BENCH_PEER_SSH:$_peer_root/peer-client.sh"
    _peer "mkdir -p $_peer_root/$run"
    _start_peer_java_driver "$dir" || return 1
    timeout --signal=KILL "${BENCH_CLIENT_TIMEOUT:-900}" \
        ssh -n -o BatchMode=yes "$BENCH_PEER_SSH" "bash $_peer_root/peer-client.sh" \
        >"$dir/client.log" 2>&1
}

# The client half of the Java arm needs a driver too, for the same reason: the
# reference's transceiver launches one only when told to, and its own two-machine
# script starts one on each machine.
_start_peer_java_driver() {
    local dir=$1
    cat >"$dir/peer-jdriver.sh" <<EOF
#!/usr/bin/env bash
set -euo pipefail
echo \$\$ >"$_peer_root/jdriver.pid"
export JAVA_HOME=\${JAVA_HOME:-/usr/lib/jvm/java-21-openjdk-amd64}
cd "$BENCHMARKS_DIR"
exec ./scripts/aeron/media-driver "$_peer_root/driver-override.properties"
EOF
    scp -q "$_java_driver_override" "$BENCH_PEER_SSH:$_peer_root/driver-override.properties"
    scp -q "$dir/peer-jdriver.sh" "$BENCH_PEER_SSH:$_peer_root/peer-jdriver.sh"
    _peer_start "$dir/peer-jdriver.sh" "$_peer_root/jdriver.log" "$_peer_root/jdriver.pid" || return 1
    _await_file "$BENCH_AERON_DIR/cnc.dat" peer || {
        echo "  the peer's Java driver never published its CnC file" >&2
        _peer "tail -5 $_peer_root/jdriver.log" >&2 || true
        return 1
    }
}

# --- the client half, when it is on this machine -----------------------------

# Its own driver, in its own aeron directory: two drivers cannot share one CnC
# file, and the point of the single-machine shape is that the client's half is
# exactly the two-machine client's half with a shorter wire.
_start_local_client_driver() {
    local dir=$1
    DEEPMSG_DRIVER="$BENCH_DRIVER" BENCH_OUT_DIR="$dir/shims" \
        "$BENCH_LIB/shims/aeronmd" "$_client_driver_properties" \
        </dev/null >"$dir/client-driver.log" 2>&1 &
    _client_driver_pid=$!
    _await_file "$_client_aeron_dir/cnc.dat" local || {
        echo "  the client's driver never published its CnC file" >&2
        tail -5 "$dir/client-driver.log" >&2
        return 1
    }
}

_run_rust_client_local() {
    local dir=$1 prefix=$2
    timeout --signal=KILL "${BENCH_CLIENT_TIMEOUT:-900}" \
        taskset -c "$BENCH_RIG_CPU" "$BENCH_RIG" \
        -Dio.aeron.benchmarks.message.transceiver=live-replay \
        "-Daeron.dir=$_client_aeron_dir" \
        -Dio.aeron.benchmarks.aeron.embedded.media.driver=false \
        "-Dio.aeron.benchmarks.output.directory=$dir" \
        "-Dio.aeron.benchmarks.output.file=$prefix" \
        "-Dio.aeron.benchmarks.message.rate=$rate" \
        "-Dio.aeron.benchmarks.warmup.message.rate=$warmup_rate" \
        "-Dio.aeron.benchmarks.iterations=$iterations" \
        "-Dio.aeron.benchmarks.warmup.iterations=$warmup_iterations" \
        "-Dio.aeron.benchmarks.message.length=$length" \
        -Dio.aeron.benchmarks.batch.size=1 \
        -Dio.aeron.benchmarks.aeron.use.try.claim=true \
        -Dio.aeron.benchmarks.output.time.unit=MICROSECONDS \
        "$_client_channels" >"$dir/client.log" 2>&1
}

_run_java_client_local() {
    local dir=$1 prefix=$2
    # `embedded.media.driver=true` here and nowhere else: on one machine the
    # reference's own single-host script lets the client carry its driver, and
    # this is that shape (`remote-live-replay-benchmarks` starts a driver on each
    # machine only because there are two of them).
    JVM_OPTS="-Daeron.dir=$_client_aeron_dir \
-Dio.aeron.benchmarks.aeron.embedded.media.driver=true \
-Dio.aeron.benchmarks.output.directory=$dir \
-Dio.aeron.benchmarks.output.file=$prefix \
-Dio.aeron.benchmarks.message.rate=$rate \
-Dio.aeron.benchmarks.warmup.message.rate=$warmup_rate \
-Dio.aeron.benchmarks.iterations=$iterations \
-Dio.aeron.benchmarks.warmup.iterations=$warmup_iterations \
-Dio.aeron.benchmarks.message.length=$length \
-Dio.aeron.benchmarks.batch.size=1 \
-Dio.aeron.benchmarks.aeron.use.try.claim=true \
-Dio.aeron.benchmarks.output.time.unit=MICROSECONDS" \
        timeout --signal=KILL "${BENCH_CLIENT_TIMEOUT:-900}" \
        taskset -c "$BENCH_RIG_CPU" "$BENCHMARKS_SCRIPTS_DIR/live-replay-client" "$_client_channels" \
        >"$dir/client.log" 2>&1
}

# --- collecting --------------------------------------------------------------

_collect() {
    local dir=$1 run=$2
    # The histogram is the reading, and it was written where the instrument ran:
    # here on one machine, on the peer when there are two.
    if [ "$mode" = two ]; then
        scp -q "$BENCH_PEER_SSH:$_peer_root/$run/"'*.hdr*' "$dir/" 2>/dev/null || {
            echo "  no histogram came back from the peer" >&2
            return 1
        }
    elif ! compgen -G "$dir/*.hdr*" >/dev/null; then
        echo "  no histogram was written in $dir" >&2
        return 1
    fi
    # The counters are read from the archive's driver while the archive is still
    # alive: a counter a recording allocated is returned when its session ends,
    # and a run that reads them after the kill reads empty slots (B0′'s lesson).
    # ★ The labels carry ` - archiveId=<id>` on **both** archives
    # (`ArchiveCounters.ARCHIVE_ID_LABEL_SUFFIX`), and it names the archive
    # instance rather than the counter, so it is normalised away here: what a
    # column is, is the name before it, and that is what `read.py` looks up.
    "$BENCH_CNC_DUMP" --counters "$BENCH_AERON_DIR" 2>/dev/null |
        awk 'NF >= 5 && $1 ~ /^[0-9]+$/ && $2 ~ /^[0-9]+$/ {
                label = ""; for (i = 5; i <= NF; i++) label = label (i > 5 ? " " : "") $i;
                sub(/ - archiveId=.*$/, "", label);
                print label "\t" $3 }' >"$dir/counters.txt" || true
    du -sb "$archive_dir" 2>/dev/null | awk '{ print $1 }' >"$dir/archive-du.txt" || true
}

# --- stopping ----------------------------------------------------------------

_stop_all() {
    local pid
    for pid in ${_node_pid:-} ${_archive_pid:-} ${_driver_pid:-} ${_server_pid:-} ${_java_driver_pid:-} ${_client_driver_pid:-}; do
        [ -n "$pid" ] && kill -9 "$pid" 2>/dev/null || true
    done
    if [ "$mode" = two ]; then
        _peer_kill "$_peer_root/driver.pid"
        _peer_kill "$_peer_root/jdriver.pid"
    fi
    _node_pid=""; _archive_pid=""; _driver_pid=""; _server_pid=""; _java_driver_pid=""; _client_driver_pid=""
}
trap _stop_all EXIT

# --- one cell ----------------------------------------------------------------

_cell() {
    local round=$1 arm=$2 fsync=$3 run=$4
    local name="${scenario}_${arm}_fsync=${fsync}_mtu=${mtu}_length=${length}_rate=${rate}"
    local cell="$out/$round/$name"
    local dir="$cell/run-$run"
    local prefix="$name"
    local peer_run=""
    mkdir -p "$dir"

    _fsync=$fsync
    _write_cell_files "$dir" "$fsync"

    echo "  $arm fsync=$fsync run $run"
    if [ "$mode" = two ]; then
        _load_gate || return 1
    fi
    wait_idle || return 1
    rm -rf "$BENCH_AERON_DIR" "$_client_aeron_dir"
    if [ "$mode" = two ]; then
        _peer "rm -rf $BENCH_AERON_DIR $_peer_root" || true
        _peer "mkdir -p $_peer_root" || true
    fi

    case "$arm" in
        j)
            _start_java_server "$dir" || { _stop_all; return 1; }
            _await_recording "$BENCH_AERON_DIR" 30 || {
                echo "  the reference's archive never started a recording" >&2
                _stop_all
                return 1
            }
            if [ "$mode" = two ]; then
                _run_java_client "$dir" "$round/$name/run-$run" "$prefix"
            else
                _run_java_client_local "$dir" "$prefix"
            fi || {
                echo "  the client exited non-zero" >&2
                _stop_all
                return 1
            } ;;
        r-rust)
            _start_rust_server "$dir" || { _stop_all; return 1; }
            _await_recording "$BENCH_AERON_DIR" 30 || {
                echo "  no recording appeared" >&2
                _stop_all
                return 1
            }
            if [ "$mode" = two ]; then
                _start_peer_client_driver "$dir" || { _stop_all; return 1; }
                _run_rust_client "$dir" "$round/$name/run-$run" "$prefix"
            else
                _start_local_client_driver "$dir" || { _stop_all; return 1; }
                _run_rust_client_local "$dir" "$prefix"
            fi || {
                echo "  the client exited non-zero" >&2
                _stop_all
                return 1
            } ;;
        *)
            echo "  arm '$arm' needs patches/external-archive.patch, which is not here yet." >&2
            echo "  It is refused by name rather than run as 'j': a cell that quietly" >&2
            echo "  ran the wrong archive is a number about the wrong thing." >&2
            return 1 ;;
    esac

    _collect "$dir" "$round/$name/run-$run" || { _stop_all; return 1; }
    _stop_all
    rm -rf "$BENCH_AERON_DIR"

    # `cell_validate` reads `<cell>/run-<N>/client.log`, which is where the ssh
    # above wrote it.
    if ! cell_validate "$cell" "$run" "$rate" "$iterations"; then
        echo "  $arm fsync=$fsync run $run: INVALID" >&2
        return 1
    fi
    return 0
}

# --- prerequisites -----------------------------------------------------------

_check() {
    local problem=0
    need_dir "harness checkout" "$BENCHMARKS_DIR" || problem=1
    need_harness_built || problem=1
    need_file "the driver" "$BENCH_DRIVER" "run bench/build.sh" || problem=1
    need_file "the archive" "$BENCH_ARCHIVE" "run bench/build.sh" || problem=1
    need_file "the archive node" "$BENCH_ARCHIVE_NODE" "run bench/build.sh" || problem=1
    need_file "the counters reader" "$BENCH_CNC_DUMP" "run bench/build.sh" || problem=1

    for arm in "${arm_list[@]}"; do
        case "$arm" in
            j) need_file "the harness's archive node" "$BENCHMARKS_SCRIPTS_DIR/archive-node" || problem=1
               need_file "the harness's live-replay client" "$BENCHMARKS_SCRIPTS_DIR/live-replay-client" || problem=1 ;;
            r-rust) ;;
            *) echo "arm '$arm' needs patches/external-archive.patch; it is not part of this slice" >&2; problem=1 ;;
        esac
    done

    if [ "$mode" = two ]; then
        if ! _peer "test -d $peer_repo"; then
            echo "no this build's checkout on the peer at $peer_repo" >&2
            echo "  set BENCH_PEER_REPO" >&2
            problem=1
        else
            for f in target/release/deepmsg-driver target/rig/release/loadtest-rig; do
                _peer "test -x $peer_repo/$f" || {
                    echo "the peer has no $peer_repo/$f; run bench/build.sh there" >&2
                    problem=1
                }
            done
        fi
        # `lan_require` itself: the same gate every cell runs under.
        if ! lan_require; then
            problem=1
        fi
    fi
    return "$problem"
}

if [ "$list_only" = 1 ]; then
    echo "=== plan ==="
    if [ "$mode" = two ]; then
        echo "scenario        $scenario (live-replay: node + archive here, client on the peer)"
    else
        echo "scenario        $scenario (live-replay: node + archive and client, all here)"
    fi
    echo "arms            ${arm_list[*]}"
    echo "fsync           $fsync_levels (file and catalog levels, both archives)"
    echo "rate/length     $rate / $length, mtu $mtu"
    echo "runs            $runs over ${iterations}s (warmup ${warmup_iterations}s at $warmup_rate)"
    echo "rounds          forward, reverse"
    echo
    if [ "$mode" = two ]; then
        echo "where           this machine (archive): driver + archive + archive node"
        echo "                peer $BENCH_PEER_SSH ($_peer_ip): driver + rig"
    else
        echo "where           one machine: server (driver + archive + node) and client (driver + rig)"
    fi
    echo "channels        destination $_batch_ip:$destination_port  source $_peer_ip:$source_port"
    echo "                control     $_batch_ip:$control_port  response <each side>:0 (ephemeral)"
    if [ "$mode" = two ]; then
        echo "aeron dir       $BENCH_AERON_DIR (both hosts, their own /dev/shm)"
    else
        echo "aeron dirs      $BENCH_AERON_DIR (server), $_client_aeron_dir (client)"
    fi
    echo "archive dir     $archive_dir"
    if [ "$mode" = two ]; then
        echo "cores           archive $archive_cpus, node $node_cpu; driver 1/2/3; peer rig $peer_rig_cpu"
    else
        echo "cores           archive $archive_cpus, node $node_cpu; server driver 1/2/3; client rig $BENCH_RIG_CPU"
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

_check || { echo >&2; echo "nothing was run." >&2; exit 1; }

mkdir -p "$out"
bench_fingerprint "$out/fingerprint.txt" >/dev/null
{
    echo "scenario        $scenario"
    echo "arms            ${arm_list[*]}"
    echo "fsync           $fsync_levels"
    echo "rate/length     $rate / $length, mtu $mtu"
    echo "runs            $runs x ${iterations}s (warmup ${warmup_iterations}s at $warmup_rate)"
    echo "rounds          forward, reverse"
    if [ "$mode" = two ]; then
        echo "network         two hosts, same segment: $_batch_ip <-> $_peer_ip"
        echo "this machine    archive: driver + archive + archive node; cores $archive_cpus / $node_cpu"
        echo "peer            $BENCH_PEER_SSH: driver + rig; cores $peer_rig_cpu"
    else
        echo "network         one machine (loopback); no peer"
        echo "server          driver + archive + archive node; cores $archive_cpus / $node_cpu"
        echo "client          driver + rig; core $BENCH_RIG_CPU"
    fi
    echo "channels        destination $_batch_ip:$destination_port  source $_peer_ip:$source_port"
    echo "                control     $_batch_ip:$control_port  response <each host>:0 (ephemeral)"
    echo "aeron dir       $BENCH_AERON_DIR (each host its own)"
    echo "archive dir     $archive_dir"
    echo
    echo "what each arm is"
    echo "  j        $BENCHMARKS_SCRIPTS_DIR/archive-node + live-replay-client (Java, both ends)"
    echo "  r-rust   $(sha256sum "$BENCH_DRIVER" | cut -d' ' -f1)  deepmsg-driver"
    echo "           $(sha256sum "$BENCH_ARCHIVE" | cut -d' ' -f1)  deepmsg-archive"
    echo "           $(sha256sum "$BENCH_ARCHIVE_NODE" | cut -d' ' -f1)  archive-node"
    if [ "$mode" = two ]; then
        echo "           peer: loadtest-rig (built there)"
    else
        echo "           $(sha256sum "$BENCH_RIG" | cut -d' ' -f1)  loadtest-rig (here)"
    fi
    echo
    echo "the archive settings every cell in this batch is given:"
    sed 's/^/  /' "$BENCH_DIR/lib/conf/archive.properties"
} >"$out/RUN-INFO.txt"

echo "=== archive, on two machines ==="
sed 's/^/  /' "$out/RUN-INFO.txt"
echo

_run_round() {
    local round=$1; shift
    echo "########## $round ($*) ##########"
    rm -rf "$out/$round"
    for fsync in "${fsync_list[@]}"; do
        for arm in "$@"; do
            local ok=0 run
            for run in $(seq 1 "$runs"); do
                if _cell "$round" "$arm" "$fsync" "$run"; then
                    ok=$((ok + 1))
                else
                    echo "  FAILED" >&2
                fi
            done
            [ "$ok" -eq "$runs" ] || echo "  !! $arm fsync=$fsync: $ok/$runs passed" >&2
        done
    done
}

_run_round round-1-forward "${arm_list[@]}"

rev_list=()
for ((i = ${#arm_list[@]} - 1; i >= 0; i--)); do
    rev_list+=("${arm_list[i]}")
done
_run_round round-2-reverse "${rev_list[@]}"

echo
echo "results in $out"

if [ "$read_after" = 1 ]; then
    echo
    python3 "$here/read.py" "$out" || true
fi
