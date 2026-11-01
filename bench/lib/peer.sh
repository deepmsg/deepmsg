#!/usr/bin/env bash
#
# The other machine of a two-host cell: how to reach it, how to start something
# on it, and how to take it down again.
#
# Sourced by a batch after `checkouts.sh` and `fingerprint.sh`. Two batches
# already carry their own copy of this logic (`archive-vs-java/run-batch.sh` and
# `cluster-arms/run-batch.sh`); they are left as they are, because they work and
# a refactor of a working batch is a second variable in a session that is
# already running. This file is for the ones written from here on: the echo
# cell in `rust-vs-java/` and the C-driver arms in `upstream-c-drivers/`.
#
# Reads:
#
#   BENCH_PEER_SSH        how to reach the other machine, as `ssh` takes it
#   BENCH_PEER_IP         the address to dial (default: the host part of the ssh
#                         target; a name is not an address a channel can carry)
#   BENCH_PEER_ROOT       where scripts and histograms live on the peer
#                         (default: /tmp/deepmsg-bench-peer)
#   BENCH_PEER_AERON_DIR  the peer's CnC directory (default: /dev/shm/deepmsg-bench)
#   BENCH_PEER_MAX_LOAD   the load gate's own limit (default: nproc / 4)
#
# Everything under `BENCH_PEER_ROOT` and `BENCH_PEER_AERON_DIR` is removed before
# a cell, so both are guarded against a typo in an environment variable.

BENCH_PEER_SSH="${BENCH_PEER_SSH:-}"
BENCH_PEER_IP="${BENCH_PEER_IP:-${BENCH_PEER_SSH#*@}}"
BENCH_PEER_ROOT="${BENCH_PEER_ROOT:-/tmp/deepmsg-bench-peer}"
BENCH_PEER_AERON_DIR="${BENCH_PEER_AERON_DIR:-/dev/shm/deepmsg-bench}"

_peer_guard_paths() {
    local path
    for path in "$BENCH_PEER_ROOT" "$BENCH_PEER_AERON_DIR"; do
        case "$path" in
            /|/dev|/dev/shm|/tmp|/home|/root|/usr|/var|/etc)
                echo "refusing to use '$path' as the peer's work directory" >&2
                exit 2 ;;
            "")
                echo "the peer's work directory is empty" >&2
                exit 2 ;;
        esac
    done
}

# One command on the peer that finishes. `-n` so that a command which reads
# stdin does not eat the batch's own input.
_peer() {
    ssh -n -o BatchMode=yes -o ConnectTimeout=5 "$BENCH_PEER_SSH" "$@"
}

_peer_scp() {
    scp -q -o BatchMode=yes "$@"
}

# Start a daemon on the peer and come back.
#
# `ssh host "cmd &"` **hangs**: sshd keeps the session open even with all three
# descriptors redirected. `ssh -f` backgrounds the client after authentication
# and returns; `exec setsid` detaches the daemon from the session's process
# group. The pid is written by the script itself, because `ssh -f` has no child
# to reap and a kill by name would reach whatever else is named alike.
#
#   _peer_start <local-script> <remote-log> <remote-pidfile>
_peer_start() {
    local script=$1 log=$2 pidfile=$3
    _peer_scp "$script" "$BENCH_PEER_SSH:$BENCH_PEER_ROOT/$(basename "$script")" || return 1
    timeout 30 ssh -f -n -o BatchMode=yes "$BENCH_PEER_SSH" \
        "exec setsid bash $BENCH_PEER_ROOT/$(basename "$script") </dev/null >$log 2>&1"
    local _
    for _ in $(seq 1 100); do
        _peer "test -s $pidfile" 2>/dev/null && return 0
        sleep 0.1
    done
    echo "  the peer's $(basename "$script") never wrote $pidfile" >&2
    return 1
}

_peer_kill() {
    local pidfile=$1
    _peer "if [ -s $pidfile ]; then kill -9 \$(cat $pidfile) 2>/dev/null || true; rm -f $pidfile; fi" \
        2>/dev/null || true
}

# The backstop for a pidfile that was missed and an ssh session that was
# interrupted. The bracket in each pattern keeps `pkill -f` from matching the
# ssh shell that carries it.
_peer_kill_all() {
    _peer_kill "$BENCH_PEER_ROOT/driver.pid"
    _peer_kill "$BENCH_PEER_ROOT/client.pid"
    _peer "pkill -9 -f '[i]o.aeron.driver.MediaDriver' 2>/dev/null || true; \
pkill -9 -f '[i]o.aeron.benchmarks.LoadTestRig' 2>/dev/null || true; \
pkill -9 -x aeronmd 2>/dev/null || true; \
pkill -9 -x loadtest-rig 2>/dev/null || true; \
pkill -9 -x echo-node 2>/dev/null || true" 2>/dev/null || true
}

# A run starts from nothing on both machines: a leftover CnC directory is a
# leftover window, and a leftover histogram is a cell that can be read twice.
_peer_prepare() {
    _peer_guard_paths
    _peer "rm -rf $BENCH_PEER_ROOT $BENCH_PEER_AERON_DIR" 2>/dev/null || true
    _peer "mkdir -p $BENCH_PEER_ROOT" 2>/dev/null || return 1
}

_peer_await_file() {
    local path=$1
    local _
    for _ in $(seq 1 200); do
        _peer "test -e $path" 2>/dev/null && return 0
        sleep 0.1
    done
    return 1
}

# The peer's load is a precondition, not a variable — the instrument is there.
# A machine with a developer's agent on it sits around one; a build or a test
# run is `nproc` and up. The point is to catch the second, not to refuse a cell
# because an editor is open.
#
#   _peer_load_gate [<out-dir>]
_peer_load_gate() {
    local out=${1:-}
    local load max
    load=$(_peer "cut -d' ' -f1 /proc/loadavg" 2>/dev/null || echo "")
    if [ -z "$load" ]; then
        echo "  could not read the peer's load average" >&2
        return 1
    fi
    max=${BENCH_PEER_MAX_LOAD:-}
    if [ -z "$max" ]; then
        max=$(( $(_peer "nproc") / 4 ))
        [ "$max" -lt 1 ] && max=1
    fi
    if awk -v l="$load" -v max="$max" 'BEGIN { exit !(l > max) }'; then
        echo "  the peer is busy: 1-minute load $load > $max." >&2
        echo "  Nothing is measured on a busy machine — the instrument is on the" >&2
        echo "  peer, and a compiler on its cores is a second variable. Wait, or" >&2
        echo "  raise BENCH_PEER_MAX_LOAD deliberately." >&2
        return 1
    fi
    if [ -n "$out" ]; then
        echo "  peer load $load (limit $max)" >>"$out/load.txt"
    fi
}

# What the peer was, in the same shape as the local fingerprint: a number
# without it is an anecdote, and that goes for both halves of a cell.
_peer_fingerprint() {
    local file=$1
    {
        echo
        echo "# the peer ($BENCH_PEER_SSH)"
        _peer 'printf "peer host       %s\n" "$(hostname)"' 2>/dev/null || true
        _peer 'printf "peer kernel     %s\n" "$(uname -sr)"' 2>/dev/null || true
        _peer 'printf "peer cpu        %s\n" "$(awk -F": " "/^model name/{print \$2; exit}" /proc/cpuinfo)"' 2>/dev/null || true
        _peer 'printf "peer cores      %s logical\n" "$(nproc)"' 2>/dev/null || true
        _peer 'printf "peer numa nodes %s\n" "$(ls -d /sys/devices/system/node/node* 2>/dev/null | wc -l)"' 2>/dev/null || true
        _peer 'printf "peer memory     %s GiB\n" "$(awk "/MemTotal/{printf \"%d\", \$2/1024/1024}" /proc/meminfo)"' 2>/dev/null || true
        _peer 'printf "peer aeron dir  %s\n" "'"$BENCH_PEER_AERON_DIR"'"' 2>/dev/null || true
    } >>"$file"
}

# Copy one remote glob back to a local directory. The remote side expands it;
# the caller names it without quotes around the glob.
_peer_fetch() {
    local remote_glob=$1 local_dir=$2
    _peer_scp "$BENCH_PEER_SSH:$remote_glob" "$local_dir/" 2>/dev/null
}
