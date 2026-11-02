#!/usr/bin/env bash
#
# One member's node side, on the machine that member lives on.
#
#   member-node.sh start <dir>
#   member-node.sh stop  <dir>
#
# `<dir>` is a cell run directory the batch prepared: it holds `member.properties`
# (written by `run-batch.sh`, one `key=value` a line) and receives the logs and
# the pid list. The same file is what a *remote* member gets: the batch writes it,
# scps the whole directory, and runs this script there under `ssh -f` + `setsid`,
# so that the member's processes outlive the ssh session that started them.
#
# Why one script for every member instead of the batch's own start functions: the
# four arms have to mean the same thing on all three members, and two code paths
# -- one local, one remote -- is exactly how an arm stops being one arm. The batch
# now runs this file locally for member 0 as well.
#
# The properties file carries everything that differs between members and cells:
#
#   arm, member_id, aeron_dir, dir, members, the seven channels, the two sync
#   levels, and the cores. This build's binaries come from `lib/checkouts.sh`,
#   the same way every other batch script finds them.
set -euo pipefail

# ★ Nothing here reads this build's `lib/checkouts.sh`: this file is scp'd to a
# member that has the binaries and the reference harness, not this repository.
# Every path it needs comes in the properties file.

action=${1:?usage: member-node.sh start|stop <dir>}
dir=${2:?usage: member-node.sh start|stop <dir>}
props=$dir/member.properties

_need() {
    local key=$1
    local value=${!key:-}
    if [ -z "$value" ]; then
        echo "member-node.sh: '$key' is missing from $props" >&2
        exit 1
    fi
}

_load() {
    [ -f "$props" ] || { echo "member-node.sh: no $props" >&2; exit 1; }
    local key value
    while IFS='=' read -r key value; do
        case "$key" in
            ''|'#'*) continue ;;
        esac
        printf -v "$key" '%s' "$value"
    done <"$props"
    _need arm; _need dir; _need aeron_dir; _need member_id; _need members
    _need ingress; _need consensus; _need log; _need catchup
    _need archive_control; _need archive_replication; _need cluster_replication
    _need control; _need local_archive_control
    _need sync_archive; _need sync_cluster; _need mtu; _need cluster_id; _need java_home
    _need benchmarks_dir; _need benchmarks_scripts_dir; _need benchmarks_driver_java
    _need bench_driver; _need bench_archive; _need bench_cluster_node; _need bench_cluster_container
    _need driver_conductor_cpu; _need driver_sender_cpu; _need driver_receiver_cpu
    _need node_cpus; _need archive_cpus; _need cm_cpu; _need container_cpu
    _need archive_recorder_cpu; _need archive_replayer_cpu; _need archive_conductor_cpu
}

PIDS=()
CELL_PIDS_FILE=$dir/pids

_stop_one() {
    local pid=$1
    kill -TERM "$pid" 2>/dev/null || true
    for _ in $(seq 1 50); do
        kill -0 "$pid" 2>/dev/null || return 0
        sleep 0.1
    done
    kill -KILL "$pid" 2>/dev/null || true
}

# The reference pins its node's five busy-spin threads to cores **by thread
# name** (`remote-benchmarks-runner:210-235`); a plain mask lets two of them land
# on one core, which is a different machine from the one the reference benchmarks
# itself on. A thread that never appears is a warning, not a cell failure.
_pin_thread() { # <pid> <name-prefix> <cpu>
    local pid=$1 name=$2 cpu=$3 tid=""
    for _ in $(seq 1 100); do
        kill -0 "$pid" 2>/dev/null || break
        tid=$(ps -L -o tid=,comm= -p "$pid" 2>/dev/null | awk -v n="$name" '$2 ~ n { print $1; exit }' || true)
        [ -n "$tid" ] && break
        sleep 0.1
    done
    [ -n "$tid" ] || { echo "  (no thread named '$name' in pid $pid)" >&2; return 0; }
    taskset -pc "$cpu" "$tid" >/dev/null 2>&1 || true
}

_await_cnc() { # <aeron-dir>
    local aeron_dir=$1
    for _ in $(seq 1 200); do
        [ -f "$aeron_dir/cnc.dat" ] && return 0
        sleep 0.1
    done
    echo "member-node.sh: no CnC file under $aeron_dir" >&2
    return 1
}

_driver_properties() { # <file>
    cat >"$1" <<EOF
aeron.dir=$aeron_dir
aeron.conductor.cpu.affinity=$driver_conductor_cpu
aeron.sender.cpu.affinity=$driver_sender_cpu
aeron.receiver.cpu.affinity=$driver_receiver_cpu
aeron.mtu.length=$mtu
aeron.ipc.mtu.length=$mtu
EOF
}

_cluster_common_defines() {
    printf '%s\n' \
        "-Daeron.dir=$aeron_dir" \
        "-Daeron.cluster.dir=$dir/cluster" \
        "-Daeron.cluster.id=$cluster_id" \
        "-Daeron.cluster.member.id=$member_id" \
        "-Daeron.cluster.members=$members" \
        "-Daeron.cluster.ingress.channel=$ingress" \
        "-Daeron.cluster.consensus.channel=$consensus" \
        "-Daeron.cluster.log.channel=$log" \
        "-Daeron.cluster.catchup.channel=$catchup" \
        "-Daeron.cluster.replication.channel=$cluster_replication"
}

_archive_common_defines() {
    # ★ The `j` and `j-d` arms do not start an archive of their own: the
    # reference's `cluster-node` script starts one inside the node's JVM and
    # hands it `scripts/aeron/low-latency-archive.properties`. Without the same
    # values here the `r-a`/`r` arms would run their archive on the class
    # defaults -- `BackoffIdleStrategy` for all three agents and a 100 ms cycle
    # threshold -- and a difference between the arms would be a difference in
    # configuration. Measured 2026-11-02: `r-a/j-d` read 1.87/2.12 before this.
    #
    # The values below are that file, one for one, except two: the archive's
    # `file.sync.level` is the cell's variable (passing the file would pin it to
    # 0 and silently override `--sync 0:2`, `2:0`, `2:2`), and
    # `dir.delete.on.start` is already covered by the member directory being
    # removed before every cell.
    printf '%s\n' \
        "-Daeron.dir=$aeron_dir" \
        "-Daeron.archive.dir=$dir/archive" \
        "-Daeron.archive.mark.file.dir=$aeron_dir" \
        "-Daeron.archive.control.channel=$archive_control" \
        "-Daeron.archive.replication.channel=$archive_replication" \
        "-Daeron.archive.local.control.channel=$local_archive_control" \
        "-Daeron.archive.file.sync.level=$sync_archive" \
        "-Daeron.archive.catalog.file.sync.level=$sync_archive" \
        "-Daeron.archive.recording.events.enabled=false" \
        "-Daeron.archive.threading.mode=DEDICATED" \
        "-Daeron.archive.control.term.buffer.sparse=true" \
        "-Daeron.archive.file.io.max.length=1m" \
        "-Daeron.archive.idle.strategy=spin" \
        "-Daeron.archive.recorder.idle.strategy=noop" \
        "-Daeron.archive.replayer.idle.strategy=noop" \
        "-Daeron.archive.conductor.cycle.threshold=1ms" \
        "-Daeron.archive.recorder.cycle.threshold=1ms" \
        "-Daeron.archive.replayer.cycle.threshold=1ms"
}

start_node_java() { # j, j-d
    local logfile=$dir/node.log
    local -a defines=()
    while IFS= read -r line; do defines+=("$line"); done < <(_cluster_common_defines)
    JVM_OPTS="${defines[*]} \
-Daeron.cluster.idle.strategy=noop \
-Daeron.cluster.control.channel=$control \
-Daeron.cluster.service.name=echo-service \
-Daeron.cluster.file.sync.level=$sync_cluster \
-Daeron.archive.dir=$dir/archive \
-Daeron.archive.mark.file.dir=$aeron_dir \
-Daeron.archive.control.channel=$archive_control \
-Daeron.archive.replication.channel=$archive_replication \
-Daeron.archive.local.control.channel=$local_archive_control \
-Daeron.archive.file.sync.level=$sync_archive \
-Daeron.archive.catalog.file.sync.level=$sync_archive \
-Daeron.archive.recording.events.enabled=false \
-Daeron.archive.threading.mode=DEDICATED \
-Dio.aeron.benchmarks.aeron.cluster.service=echo \
-Dio.aeron.benchmarks.output.directory=$dir" \
        taskset -c "$node_cpus" "$benchmarks_scripts_dir/cluster-node" >"$logfile" 2>&1 &
    local pid=$!
    PIDS+=("$pid")
    echo "$pid" >>"$CELL_PIDS_FILE"
    _pin_thread "$pid" archive-recorde "$archive_recorder_cpu"
    _pin_thread "$pid" archive-replaye "$archive_replayer_cpu"
    _pin_thread "$pid" archive-conduct "$archive_conductor_cpu"
    _pin_thread "$pid" consensus-modul "$cm_cpu"
    _pin_thread "$pid" echo-service "$container_cpu"
}

start_archive_java() {
    local logfile=$dir/archive.log
    local -a defines=()
    while IFS= read -r line; do defines+=("$line"); done < <(_archive_common_defines)
    JVM_OPTS="${defines[*]}" \
        PROCESS_FILE_NAME=archive-node taskset -c "$archive_cpus" \
        "$benchmarks_dir/scripts/run-java" io.aeron.archive.Archive >"$logfile" 2>&1 &
    local pid=$!
    PIDS+=("$pid")
    echo "$pid" >>"$CELL_PIDS_FILE"
}

start_archive_deepmsg() {
    local logfile=$dir/archive.log
    local -a defines=()
    while IFS= read -r line; do defines+=("$line"); done < <(_archive_common_defines)
    taskset -c "$archive_cpus" "$bench_archive" "${defines[@]}" >"$logfile" 2>&1 &
    local pid=$!
    PIDS+=("$pid")
    echo "$pid" >>"$CELL_PIDS_FILE"
}

start_consensus_deepmsg() {
    local logfile=$dir/module.log
    local -a defines=()
    while IFS= read -r line; do defines+=("$line"); done < <(_cluster_common_defines)
    taskset -c "$cm_cpu" "$bench_cluster_node" \
        "${defines[@]}" \
        "-Daeron.cluster.control.channel=$control" \
        "-Daeron.cluster.service.count=1" \
        "-Daeron.cluster.file.sync.level=$sync_cluster" \
        "-Daeron.cluster.idle.strategy=spin" >"$logfile" 2>&1 &
    local pid=$!
    PIDS+=("$pid")
    echo "$pid" >>"$CELL_PIDS_FILE"
}

start_container_deepmsg() {
    local logfile=$dir/container.log
    taskset -c "$container_cpu" "$bench_cluster_container" \
        "-Daeron.dir=$aeron_dir" \
        "-Daeron.cluster.dir=$dir/cluster" \
        "-Daeron.cluster.id=$cluster_id" \
        "-Daeron.cluster.service.id=0" \
        "-Ddeepmsg.cluster.service=echo" \
        "-Daeron.cluster.control.channel=$control" \
        "-Daeron.archive.local.control.channel=$local_archive_control" \
        "-Daeron.cluster.idle.strategy=spin" >"$logfile" 2>&1 &
    local pid=$!
    PIDS+=("$pid")
    echo "$pid" >>"$CELL_PIDS_FILE"
}

_alive() { # <what> <pid>
    kill -0 "$2" 2>/dev/null || {
        echo "  $1 did not stay up" >&2
        return 1
    }
}

action_start() {
    _load
    # ★ `run-java` execs `${JAVA_HOME}/bin/java`; an empty one is `/bin/java`,
    # which exists on the older machines only because a system JDK was
    # installed. A member's JDK is this build's own copy, so the path comes in
    # with the properties.
    export JAVA_HOME=$java_home
    rm -f "$dir/up" "$dir/failed"
    trap ': >"$dir/failed"' ERR
    rm -rf "$aeron_dir" "$dir/cluster" "$dir/archive"
    mkdir -p "$aeron_dir" "$dir"
    : >"$CELL_PIDS_FILE"
    _driver_properties "$dir/driver.properties"

    case "$arm" in
        j)   "$benchmarks_driver_java" "$dir/driver.properties" >"$dir/driver.log" 2>&1 &
             PIDS+=("$!"); echo "$!" >>"$CELL_PIDS_FILE" ;;
        *)   DEEPMSG_DRIVER="$bench_driver" \
                 "$benchmarks_scripts_dir/deepmsg-media-driver" "$dir/driver.properties" \
                 >"$dir/driver.log" 2>&1 &
             PIDS+=("$!"); echo "$!" >>"$CELL_PIDS_FILE" ;;
    esac
    _await_cnc "$aeron_dir" || { tail -20 "$dir/driver.log" >&2 || true; return 1; }

    case "$arm" in
        j|j-d) start_node_java ;;
        r-a)   start_archive_java; sleep 1 ;;
        r)     start_archive_deepmsg; sleep 1 ;;
    esac

    case "$arm" in
        r-a|r)
            start_consensus_deepmsg
            sleep 1
            start_container_deepmsg
            ;;
    esac

    for pid in "${PIDS[@]}"; do
        _alive "process $pid on member $member_id" "$pid" || {
            for p in "${PIDS[@]}"; do _stop_one "$p"; done
            return 1
        }
    done
    # The batch watches for this file: a remote start runs under `ssh -f`, so
    # its exit status is not something the caller can see.
    : >"$dir/up"
    echo "member $member_id ($arm) up: ${PIDS[*]}"
}

action_check() {
    _load
    [ -f "$CELL_PIDS_FILE" ] || { echo "member ${member_id:-?}: no pid list"; return 1; }
    local dead=0 pid
    while read -r pid; do
        [ -n "$pid" ] || continue
        if ! kill -0 "$pid" 2>/dev/null; then
            echo "member $member_id: pid $pid is gone"
            dead=1
        fi
    done <"$CELL_PIDS_FILE"
    return "$dead"
}

action_stop() {
    local pid
    if [ -f "$CELL_PIDS_FILE" ]; then
        while read -r pid; do
            [ -n "$pid" ] && _stop_one "$pid"
        done <"$CELL_PIDS_FILE"
        rm -f "$CELL_PIDS_FILE"
    fi
    echo "member ${member_id:-?} stopped"
}

case "$action" in
    start) action_start ;;
    stop)  action_stop ;;
    check) action_check ;;
    *)     echo "member-node.sh: unknown action '$action'" >&2; exit 2 ;;
esac
