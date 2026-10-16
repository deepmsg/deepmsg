#!/usr/bin/env bash
#
# One cell of a batch: a driver, a far end, a client, and the check that the
# three of them agreed.
#
# Sourced, after `checkouts.sh` and `fingerprint.sh`. Every function here is
# parameterised by what is being swapped — the caller decides which driver, node
# and client a cell is made of, and this file does not know one batch from the
# other.
#
# The four rules a cell keeps, all of them learned the hard way:
#
# - **A run starts from nothing.** `/dev/shm/aeron` is removed before the driver
#   starts: a leftover directory is a leftover window.
# - **The driver is taken down, not asked down, at the end.** SIGTERM runs a
#   shutdown that stamps every system counter `RECLAIMED`, and a reader that
#   prints only `ALLOCATED` then finds `Bytes sent` and the cycle-time counters
#   nowhere. SIGKILL leaves the file as the last live write left it, which is
#   what the reference's own runner does too
#   (`scripts/aeron/remote-echo-single-host-benchmarks:364`). It costs nothing:
#   the next cell removes the directory anyway.
# - **A cell that did not finish is not a reading.** `cell_validate` gates on
#   the client's own `WARNING:` line, on the histogram being present rather than
#   `.FAIL`-suffixed, and on the histogram holding at least the messages the
#   rate and the iterations asked for.
# - **Nothing is measured on a busy machine.** `wait_idle` is called before a
#   cell, because two of these processes are full-core spinners and a stray one
#   from the previous cell is a second variable.

# --- processes ---------------------------------------------------------------

# The driver's process name, whichever build it is: the C arms and the shim both
# end up exec'ing something called `aeronmd`, and the reference's own helper
# waits on that name (`scripts/aeron/remote-benchmarks-helper:41`).
_bench_driver_running() {
    pgrep -x aeronmd >/dev/null 2>&1 || pgrep -f 'io.aeron.driver.MediaDriver' >/dev/null 2>&1
}

driver_stop() {
    pkill -x aeronmd 2>/dev/null || true
    pkill -f 'io.aeron.driver.MediaDriver' 2>/dev/null || true
    for _ in $(seq 1 50); do
        _bench_driver_running || return 0
        sleep 0.1
    done
    return 0
}

driver_kill() {
    pkill -9 -x aeronmd 2>/dev/null || true
    pkill -9 -f 'io.aeron.driver.MediaDriver' 2>/dev/null || true
    for _ in $(seq 1 50); do
        _bench_driver_running || return 0
        sleep 0.1
    done
    return 0
}

# Nothing left over, and nothing else eating the machine.
#
# **The two clients are matched by name, not by command line**, and that is a
# measured difference rather than a preference. `pgrep -f 'loadtest-rig|echo-node'`
# also matches any shell whose command line merely *mentions* those words — and
# the shell a batch is launched from is alive for as long as the batch is, so a
# launcher that names the clients (a leftover check, a `grep`, a `for` loop over
# the three names) makes every cell wait out the whole thirty-minute loop and
# then report "the machine never went idle". Measured on the D6 A/B: one run sat
# in its first cell for 89 minutes for exactly this reason, and a second never
# started at all. Both clients are binaries started directly (`taskset -c N
# "$BENCH_RIG"`, which execs), so their process names are exactly these two.
# The Java driver below is still matched by command line, because the reference
# starts it through a script; a command line that mentions *that* pattern will
# likewise hold a cell off, so avoid writing one while a batch runs.
wait_idle() {
    for _ in $(seq 1 900); do
        if ! pgrep -x aeronmd >/dev/null 2>&1 &&
           ! pgrep -f 'io.aeron.driver.MediaDriver' >/dev/null 2>&1 &&
           ! pgrep -x loadtest-rig >/dev/null 2>&1 &&
           ! pgrep -x echo-node >/dev/null 2>&1 &&
           ! ps -eo pcpu= | awk '$1 > 20 { found = 1 } END { exit !found }'; then
            return 0
        fi
        sleep 2
    done
    echo "  the machine never went idle" >&2
    return 1
}

await_cnc() {
    for _ in $(seq 1 200); do
        [ -f "$BENCH_AERON_DIR/cnc.dat" ] && return 0
        sleep 0.1
    done
    return 1
}

# --- drivers -----------------------------------------------------------------

# The reference's Java driver, out of the harness's own script.
driver_start_java() {
    local log=$1; shift
    "$BENCHMARKS_DRIVER_JAVA" "$@" >"$log" 2>&1 &
    _bench_driver_started $!
}

# One of the C arms: a directory holding `aeronmd` and the shared libraries it
# needs, and `c-media-driver` beside them. The harness's own launcher ends with
# `exec "${DIR}/aeronmd" ...`, so an arm is a directory with that shape — see
# `bench/upstream-c-drivers/build-c-driver.sh`.
driver_start_c() {
    local log=$1 arm=$2; shift 2
    LD_LIBRARY_PATH="$arm${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
        "$arm/c-media-driver" "$@" >"$log" 2>&1 &
    _bench_driver_started $!
}

# This build's driver, through the shim that turns the reference's positional
# properties files into `-D` arguments.
# `BENCH_DRIVER_TASKSET` is an optional CPU list for the driver's *process*, and
# it exists for the same reason the rig and the node are placed with `taskset`:
# a driver that pins its own agents out of a properties file still has threads
# of its own outside that list — a media driver has the EAL's lcore among them —
# and a batch that reserves two cores per driver has to be able to say so.
# `BENCH_DRIVER_NETNS` is an optional network namespace to start the driver in,
# and it exists because **one host cannot send to itself over a wire**: if both
# ends of a cable are addresses of this machine's kernel, the kernel takes its
# local shortcut and the cable carries nothing — measured, on the D6 A/B's first
# cell, as 58 packets on the wire against 1.5 million messages offered. A batch
# that means to measure a cable puts one end in a namespace of its own, where the
# only way to the other end is the wire.
driver_start_deepmsg() {
    local log=$1; shift
    DEEPMSG_DRIVER="$BENCH_DRIVER" \
        ${BENCH_DRIVER_NETNS:+ip netns exec "$BENCH_DRIVER_NETNS"} \
        ${BENCH_DRIVER_TASKSET:+taskset -c "$BENCH_DRIVER_TASKSET"} \
        "$BENCH_LIB/shims/aeronmd" \
        "$BENCHMARKS_SCRIPTS_DIR/low-latency-driver.properties" \
        "$BENCH_DIR/lib/conf/benchmark.properties" "$@" >"$log" 2>&1 &
    _bench_driver_started $!
}

# A driver is started to be killed at the end of the cell, and bash announces a
# killed job on stderr — one `Killed` line per cell, in the middle of the
# table's progress lines. Disowning it takes it out of the job table, so the
# kill is silent. Nothing waits on these pids: the cell kills by name.
_bench_driver_started() {
    local pid=$1
    disown "$pid" 2>/dev/null || true
    echo "$pid"
}

# --- far ends ----------------------------------------------------------------

# The reference's node. It reads `aeron.dir` and its channel settings the way
# the harness's script gives them; nothing here adds to that.
node_start_java() {
    local cell=$1 log=$2
    JVM_OPTS="-Daeron.dir=$BENCH_AERON_DIR \
-Dio.aeron.benchmarks.aeron.embedded.media.driver=false \
-Dio.aeron.benchmarks.output.directory=$cell" \
        taskset -c "$BENCH_NODE_CPU" "$BENCHMARKS_ECHO_SERVER" >"$log" 2>&1 &
    echo $!
}

# This build's node, out of the same tree as the rig.
#
# `BENCH_CLIENT_PROPERTIES` is a file of extra settings for the far end and the
# rig, read after the `-D` arguments beside it — a later file wins over an
# earlier one, and a `-D` wins over all of them, which is the order the rig's own
# argument parsing states (`crates/bench/src/bin/loadtest-rig.rs`). It exists for
# a batch whose two ends are not on one machine's loopback: the channels a run
# measures are settings (`io.aeron.benchmarks.aeron.{destination,source}.channel`,
# defaulting to `localhost`), and a run over a cable has to name the addresses on
# it without every such batch editing this file.
node_start_rust() {
    local cell=$1 log=$2
    taskset -c "$BENCH_NODE_CPU" "$BENCH_NODE" \
        "-Daeron.dir=$BENCH_AERON_DIR" \
        "-Dio.aeron.benchmarks.output.directory=$cell" \
        ${BENCH_CLIENT_PROPERTIES:+"$BENCH_CLIENT_PROPERTIES"} >"$log" 2>&1 &
    echo $!
}

# --- clients -----------------------------------------------------------------

_bench_client_common() {
    local cell=$1 run=$2 prefix=$3
    printf -- '-Daeron.dir=%s -Dio.aeron.benchmarks.aeron.embedded.media.driver=false -Dio.aeron.benchmarks.output.directory=%s -Dio.aeron.benchmarks.output.file=%s' \
        "$BENCH_AERON_DIR" "$cell/run-$run" "$prefix"
}

run_client_java() {
    local cell=$1 log=$2 rate=$3 length=$4 run=$5 prefix=$6
    # shellcheck disable=SC2046
    JVM_OPTS="$(_bench_client_common "$cell" "$run" "$prefix") \
-Dio.aeron.benchmarks.message.transceiver=io.aeron.benchmarks.aeron.EchoMessageTransceiver \
-Dio.aeron.benchmarks.message.rate=$rate \
-Dio.aeron.benchmarks.warmup.message.rate=${BENCH_WARMUP_RATE:-25K} \
-Dio.aeron.benchmarks.iterations=${BENCH_ITERATIONS:-60} \
-Dio.aeron.benchmarks.warmup.iterations=${BENCH_WARMUP_ITERATIONS:-30} \
-Dio.aeron.benchmarks.message.length=$length \
-Dio.aeron.benchmarks.batch.size=1 \
-Dio.aeron.benchmarks.aeron.use.try.claim=true \
-Dio.aeron.benchmarks.output.time.unit=MICROSECONDS" \
        taskset -c "$BENCH_RIG_CPU" "$BENCHMARKS_ECHO_CLIENT" >"$log" 2>&1
}

# The rig takes the same properties as `-D` arguments, which is what makes the
# two clients' numbers comparable at all.
#
# `BENCH_CLIENT_TIMEOUT` is an optional deadline in seconds, and it lives here
# rather than around the call because **`timeout` cannot run a shell function** —
# `timeout 600 run_client_rust …` fails with `failed to run command`: 127, which a
# batch reports as the rig exiting and never as a deadline. Measured, on the D6
# A/B's first cell after a deadline was put in front of the call. The deadline
# wraps the client itself, and `taskset` execs, so the signal reaches the rig.
# A batch that bounds nothing — a rig that cannot reach its rate does not stop on
# its own, and the kernel arm of that A/B ran nine minutes on a cell the media arm
# finishes in seventy seconds — sets it; `0` is `timeout`'s own "no deadline".
run_client_rust() {
    local cell=$1 log=$2 rate=$3 length=$4 run=$5 prefix=$6
    timeout --signal=KILL "${BENCH_CLIENT_TIMEOUT:-0}" \
        taskset -c "$BENCH_RIG_CPU" "$BENCH_RIG" \
        -Dio.aeron.benchmarks.message.transceiver=echo \
        "-Daeron.dir=$BENCH_AERON_DIR" \
        -Dio.aeron.benchmarks.aeron.embedded.media.driver=false \
        "-Dio.aeron.benchmarks.output.directory=$cell/run-$run" \
        "-Dio.aeron.benchmarks.output.file=$prefix" \
        "-Dio.aeron.benchmarks.message.rate=$rate" \
        "-Dio.aeron.benchmarks.warmup.message.rate=${BENCH_WARMUP_RATE:-25K}" \
        "-Dio.aeron.benchmarks.iterations=${BENCH_ITERATIONS:-60}" \
        "-Dio.aeron.benchmarks.warmup.iterations=${BENCH_WARMUP_ITERATIONS:-30}" \
        "-Dio.aeron.benchmarks.message.length=$length" \
        -Dio.aeron.benchmarks.batch.size=1 \
        -Dio.aeron.benchmarks.aeron.use.try.claim=true \
        -Dio.aeron.benchmarks.output.time.unit=MICROSECONDS \
        ${BENCH_CLIENT_PROPERTIES:+"$BENCH_CLIENT_PROPERTIES"} >"$log" 2>&1
}

# --- the check ---------------------------------------------------------------

# A rate as the harness writes it — `25K`, `501K`, `1M` — as a count. The suffix
# is part of the property's syntax on both sides, and arithmetic on the string
# itself is a shell error, not a slow failure.
_rate_as_number() {
    case "$1" in
        *[Kk]) echo $(( ${1%[Kk]} * 1000 )) ;;
        *[Mm]) echo $(( ${1%[Mm]} * 1000000 )) ;;
        *) echo "$1" ;;
    esac
}

# `cell_validate <cell-dir> <run> <rate> <iterations>` — 0 when the cell counts,
# 1 with a reason on stderr when it does not.
#
# The rate is the one the client was asked for, not the one it achieved: the
# harness sends `iterations` seconds' worth and the count it reports is the
# identity that says so.
cell_validate() {
    local cell=$1 run=$2 rate=$3 iterations=$4
    local dir="$cell/run-$run" log="$cell/run-$run/client.log"
    local expected=$(( $(_rate_as_number "$rate") * iterations ))

    if [ ! -f "$log" ]; then
        echo "no client log at $log" >&2
        return 1
    fi
    if grep -q 'WARNING:' "$log"; then
        echo "the client printed WARNING: $(grep -m1 'WARNING:' "$log")" >&2
        return 1
    fi

    local hdrs failed
    hdrs=$(find "$dir" -maxdepth 1 -name '*.hdr' 2>/dev/null | wc -l)
    failed=$(find "$dir" -maxdepth 1 -name '*.FAIL' 2>/dev/null | wc -l)
    if [ "$failed" -gt 0 ]; then
        echo "$failed histogram(s) were written with the .FAIL suffix" >&2
        return 1
    fi
    if [ "$hdrs" -ne 1 ]; then
        echo "expected one histogram in $dir, found $hdrs" >&2
        return 1
    fi

    local hdr count
    hdr=$(find "$dir" -maxdepth 1 -name '*.hdr')
    count=$("$BENCH_DIR/tools/quantiles.sh" "$hdr" 2>/dev/null | awk 'NR == 2 { print $2 }')
    if [ -z "$count" ]; then
        echo "no count could be read out of $(basename "$hdr")" >&2
        return 1
    fi
    if [ "$count" -lt "$expected" ]; then
        echo "$(basename "$hdr") holds $count samples, fewer than the $expected asked for" >&2
        return 1
    fi
    return 0
}
