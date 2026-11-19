#!/usr/bin/env bash
#
# E1: **this tree's DPDK media against deepmsg-cpp's DPDK media**, one cable.
#
# Both arms run this tree's `loadtest-rig` and `echo-node`; the only thing that
# differs is which driver binary owns the two ports:
#
#     arm R   this tree's `deepmsg-driver`            (Rust media)
#     arm C   deepmsg-cpp's `deepmsg-media-driver`    (C++ media)
#
# Two drivers per cell, one per side, because a media takes one port per driver
# and the two ends of a cable cannot share one. Both ports are bound to vfio and
# owned by the drivers, so — unlike the D6 K/M A/B — there is no namespace and no
# kernel interface to set up: the clients are CnC clients of their side's driver,
# and the cable is the only path between the two ends.
#
#     ./run-batch.sh --list              the plan, the prerequisites, nothing run
#     ./run-batch.sh --quick             one cell per arm, five iterations, ~3 minutes
#     ./run-batch.sh [--out DIR]         four rounds R C C R, four tiers each
#
# The C++ arm is given, not built: `E1_CPP_DRIVER` names `deepmsg-cpp`'s
# `deepmsg-media-driver`, and it has to be a **Release** build with
# `AXC_WITH_DPDK=ON` (the `dpdk` preset is Debug, and the preflight refuses a
# binary that carries debug info). `E1_RUST_DIR` / `E1_RUST_BIN` name this
# tree and its release directory when the defaults do not fit.
#
# The reading rules are the bench batches' own (`bench/README.md`): one session,
# arms alternating, the floor measured as each arm against itself, p50 with the
# tails, never the mean.
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
BENCH_DIR=$(dirname "$here")
# shellcheck source=../lib/checkouts.sh
. "$BENCH_DIR/lib/checkouts.sh"
# shellcheck source=../lib/fingerprint.sh
. "$BENCH_DIR/lib/fingerprint.sh"

ROOT=${E1_RUST_DIR:-$(cd "$here/../.." && pwd)}
RUST_BIN=${E1_RUST_BIN:-$ROOT/target/release}
CPP_DRIVER=${E1_CPP_DRIVER:-}

RIG_PCI=${E1_RIG_PCI:-0000:03:00.0}
NODE_PCI=${E1_NODE_PCI:-0000:03:00.1}
RIG_ADDRESS=${E1_RIG_ADDRESS:-10.10.0.1}
NODE_ADDRESS=${E1_NODE_ADDRESS:-10.10.0.2}
PREFIX=${E1_PREFIX:-24}
DESTINATION_PORT=${E1_DESTINATION_PORT:-13333}
SOURCE_PORT=${E1_SOURCE_PORT:-13334}

RIG_DIR=/dev/shm/e1-rig
NODE_DIR=/dev/shm/e1-node

# The D6 core map, the same six of the machine's eight on both arms. The driver
# processes are given their two cores through `taskset` so the EAL's lcore stays
# inside them.
RIG_DRIVER_CORES=${E1_RIG_DRIVER_CORES:-0,1}
NODE_DRIVER_CORES=${E1_NODE_DRIVER_CORES:-2,3}
RIG_DRIVER_CPU=${E1_RIG_DRIVER_CPU:-1}
NODE_DRIVER_CPU=${E1_NODE_DRIVER_CPU:-3}
RIG_CPU=${E1_RIG_CPU:-4}
NODE_CPU=${E1_NODE_CPU:-5}

RATES=${E1_RATES:-25K:1344,101K:1344,301K:288,501K:32}
ITERATIONS=${E1_ITERATIONS:-60}
WARMUP=${E1_WARMUP:-30}
WARMUP_RATE=${E1_WARMUP_RATE:-25K}
ROUNDS=${E1_ROUNDS:-"1-R:R 2-C:C 3-C:C 4-R:R"}
LINK_WAIT=${E1_LINK_WAIT:-25}

OUT=${E1_OUT:-$BENCH_OUT_DIR/rust-vs-cpp-dpdk-e1/$(date +%Y-%m-%d)-$(hostname -s)}
# Set by `driver_start_arm` when `E1_PERF=1`: perf attaches to the two driver
# processes, not to the rig and the node.
RIG_DRIVER_PID=""
NODE_DRIVER_PID=""

CNC_TOOL=${E1_CNC_TOOL:-$RUST_BIN/cnc-dump}
DEVBIND=${E1_DEVBIND:-/usr/bin/dpdk-devbind.py}

mode=full
while [ $# -gt 0 ]; do
    case "$1" in
        --out) OUT=${2:?--out needs a directory}; shift 2 ;;
        --list) mode=list; shift ;;
        --quick) mode=quick; shift ;;
        --help | -h) sed -n '2,29p' "$0"; exit 0 ;;
        *) echo "unknown option '$1'" >&2; exit 2 ;;
    esac
done

if [ "$mode" = quick ]; then
    RATES="25K:1344"
    ITERATIONS=5
    WARMUP=2
    ROUNDS="1-R:R 2-C:C"
fi

# --- fingerprints -------------------------------------------------------------

cc_version=$(cc --version | head -1)
rustc_version=$(rustc --version)
dpdk_version=$(pkg-config --modversion libdpdk 2>/dev/null || echo "none")
driver_digest() { sha256sum "$1" | cut -d' ' -f1; }

# The machine and the checkouts are `bench_fingerprint`'s (the same file every
# batch writes); what this batch was — the arms, the grid, the cores, the ports
# and the binaries — is `RUN-INFO.txt`, which is the file a reader needs beside
# the table.
write_fingerprint() {
    mkdir -p "$OUT"
    bench_fingerprint "$OUT/fingerprint.txt" >/dev/null
    {
        echo "label          : E1 Rust DPDK media vs C++ DPDK media"
        echo "collected      : $(date -Iseconds)"
        echo "dpdk           : $dpdk_version"
        echo "rig            : $RIG_PCI ($RIG_ADDRESS/$PREFIX)"
        echo "node           : $NODE_PCI ($NODE_ADDRESS/$PREFIX)"
        echo "cores          : driver-rig=$RIG_DRIVER_CORES (pin $RIG_DRIVER_CPU) driver-node=$NODE_DRIVER_CORES (pin $NODE_DRIVER_CPU) rig=$RIG_CPU node=$NODE_CPU"
        echo "rates          : $RATES"
        echo "rounds         : $ROUNDS"
        echo "iterations     : $ITERATIONS (+$WARMUP warmup at $WARMUP_RATE)"
        echo "link wait      : ${LINK_WAIT}s after the drivers answer (not in the window)"
        echo "binaries:"
        for name in loadtest-rig echo-node cnc-dump; do
            echo "  $(driver_digest "$RUST_BIN/$name")  $name"
        done
        echo "  $(driver_digest "$ROOT/target/release/deepmsg-driver")  deepmsg-driver (R arm)"
        echo "  $(driver_digest "$CPP_DRIVER")  deepmsg-media-driver (C arm)"
        echo "rust driver rev: $(git -C "$ROOT" log --oneline -1 2>/dev/null || echo unknown)"
        cpp_root=$(git -C "$(dirname "$CPP_DRIVER")" rev-parse --show-toplevel 2>/dev/null || true)
        echo "cpp driver rev : $(git -C "${cpp_root:-.}" log --oneline -1 2>/dev/null || echo unknown)"
    } >"$OUT/RUN-INFO.txt"
}

# --- preflight ----------------------------------------------------------------

preflight() {
    local status=0
    for binary in "$RUST_BIN/loadtest-rig" "$RUST_BIN/echo-node" "$CNC_TOOL" "$ROOT/target/release/deepmsg-driver"; do
        [ -x "$binary" ] || { echo "  missing: $binary"; status=1; }
    done
    if [ -n "$CPP_DRIVER" ] && [ ! -x "$CPP_DRIVER" ]; then
        echo "  missing: $CPP_DRIVER"
        status=1
    fi
    [ -x "$DEVBIND" ] || { echo "  missing: $DEVBIND"; status=1; }
    if [ -z "$CPP_DRIVER" ]; then
        echo "  E1_CPP_DRIVER is not set (deepmsg-cpp's Release+DPDK deepmsg-media-driver)"
        status=1
    fi
    # A Debug arm against a Release arm measures the build type. The first E1
    # session did exactly that (the `dpdk` preset is Debug, `-g` without -O):
    # the C arm read 34-38 us flat and the table said nothing about the media.
    # `file` is the guard because the difference is what the binary carries.
    if file "$CPP_DRIVER" | grep -q "with debug_info"; then
        echo "  the C arm is a Debug build: $CPP_DRIVER"
        status=1
    fi
    [ -n "$dpdk_version" ] && [ "$dpdk_version" != none ] || { echo "  pkg-config libdpdk not found"; status=1; }
    local huge
    huge=$(awk '/HugePages_Free/ {print $2}' /proc/meminfo)
    [ "${huge:-0}" -ge 512 ] || { echo "  huge pages free: ${huge:-0} (< 512)"; status=1; }
    for pci in "$RIG_PCI" "$NODE_PCI"; do
        [ -d "/sys/bus/pci/devices/$pci" ] || { echo "  no $pci"; status=1; }
    done
    local stray
    stray=$(pgrep -ax "deepmsg-driver|deepmsg-media-d|echo-node|loadtest-rig" 2>/dev/null || true)
    [ -z "$stray" ] || { echo "  stray processes: $stray"; status=1; }
    return "$status"
}

list_plan() {
    echo "E1 plan (nothing run):"
    echo "  output: $OUT"
    echo "  rounds: $ROUNDS"
    echo "  rates : $RATES   iterations=$ITERATIONS warmup=$WARMUP@$WARMUP_RATE"
    local round
    for round in $ROUNDS; do
        local tag=${round%%:*} arm=${round##*:}
        local rate
        for pair in ${RATES//,/ }; do
            rate=${pair%%:*}
            echo "    $tag arm=$arm $rate"
        done
    done
}

# --- the ports ----------------------------------------------------------------

bind_ports_vfio() {
    echo "== bind $RIG_PCI and $NODE_PCI to vfio-pci"
    # The module has to be loaded first: `dpdk-devbind.py` refuses a driver it
    # cannot see, and a cold boot has neither `vfio` nor `vfio-pci` in.
    sudo modprobe vfio 2>/dev/null || true
    sudo modprobe vfio-pci 2>/dev/null || true
    sudo "$DEVBIND" --bind=vfio-pci "$RIG_PCI" "$NODE_PCI" >"$OUT/bind-vfio.log" 2>&1 \
        || { echo "  bind failed:"; cat "$OUT/bind-vfio.log"; return 1; }
}

restore_ports() {
    echo "== restore $RIG_PCI and $NODE_PCI to ixgbe"
    sudo "$DEVBIND" --bind=ixgbe "$RIG_PCI" "$NODE_PCI" >"$OUT/bind-ixgbe.log" 2>&1 || true
    sudo rm -rf "$RIG_DIR" "$NODE_DIR" /dev/hugepages/rtemap_* 2>/dev/null || true
}

# --- one cell -----------------------------------------------------------------

media_env() { # $1 = side
    if [ "$1" = rig ]; then
        export AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_MEDIA=dpdk
        export DEEPMSG_DPDK_PORT="$RIG_PCI"
        export DEEPMSG_DPDK_IPV4_ADDRESS="$RIG_ADDRESS/$PREFIX"
        export DEEPMSG_DPDK_EAL_ARGS="-a $RIG_PCI"
    else
        export AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_MEDIA=dpdk
        export DEEPMSG_DPDK_PORT="$NODE_PCI"
        export DEEPMSG_DPDK_IPV4_ADDRESS="$NODE_ADDRESS/$PREFIX"
        export DEEPMSG_DPDK_EAL_ARGS="-a $NODE_PCI"
    fi
}

unset_media_env() {
    unset AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_MEDIA DEEPMSG_DPDK_PORT \
        DEEPMSG_DPDK_IPV4_ADDRESS DEEPMSG_DPDK_EAL_ARGS 2>/dev/null || true
}

driver_start_arm() { # $1 = arm, $2 = side, $3 = log, $4 = settings file (record only)
    local arm=$1 side=$2 log=$3 settings=$4
    local dir cpu cores binary
    if [ "$side" = rig ]; then
        dir=$RIG_DIR; cpu=$RIG_DRIVER_CPU; cores=$RIG_DRIVER_CORES
    else
        dir=$NODE_DIR; cpu=$NODE_DRIVER_CPU; cores=$NODE_DRIVER_CORES
    fi
    if [ "$arm" = R ]; then binary=$ROOT/target/release/deepmsg-driver; else binary=$CPP_DRIVER; fi
    # The settings go in as `-D` for both arms: the Rust driver takes that form
    # (the harness shim exists to turn the reference's properties files into it)
    # and the C++ driver takes both. The file beside the log is the record.
    local args=(
        "-Daeron.dir=$dir"
        -Daeron.threading.mode=SHARED
        "-Daeron.conductor.cpu.affinity=$cpu"
        -Daeron.shared.idle.strategy=spin
        -Daeron.sender.idle.strategy=spin
        -Daeron.receiver.idle.strategy=spin
        -Daeron.mtu.length=1408
        -Daeron.rcv.initial.window.length=2m
        -Daeron.socket.so_sndbuf=2m
        -Daeron.socket.so_rcvbuf=2m
    )
    printf '%s\n' "${args[@]}" >"$settings"
    media_env "$side"
    sudo -E "$(command -v taskset)" -c "$cores" "$binary" "${args[@]}" >"$log" 2>&1 &
    unset_media_env
    # The real process is the binary (taskset execs); the newest one of this
    # arm's comm is the side just started.
    sleep 0.3
    if [ "$arm" = R ]; then
        pid=$(pgrep -n -x deepmsg-driver 2>/dev/null || true)
    else
        pid=$(pgrep -n -x deepmsg-media-d 2>/dev/null || true)
    fi
    if [ "$side" = rig ]; then RIG_DRIVER_PID=$pid; else NODE_DRIVER_PID=$pid; fi
}

await_cnc() { # $1 = directory
    local dir=$1
    for _ in $(seq 1 300); do
        [ -f "$dir/cnc.dat" ] && return 0
        sleep 0.1
    done
    return 1
}

keep_counters() { # $1 = directory, $2 = file
    sudo "$CNC_TOOL" --counters "$1" >"$2" 2>"$2.err" || { echo "  counters unreadable: $2.err" >&2; return 1; }
    rm -f "$2.err"
}

kill_side() {
    # By `comm`, not by path: a `-f` pattern naming the driver also matches the
    # shell whose command line carries `E1_CPP_DRIVER=<path>` — measured here as
    # the batch's own wrapper being SIGKILLed by its first cell's cleanup. And
    # the name is 15 characters, so the C++ arm's comm is `deepmsg-media-d`.
    sudo pkill -9 -x deepmsg-driver 2>/dev/null || true
    sudo pkill -9 -x deepmsg-media-d 2>/dev/null || true
    sudo pkill -9 -x echo-node 2>/dev/null || true
    sudo pkill -9 -x loadtest-rig 2>/dev/null || true
    sudo pkill -9 -x perf 2>/dev/null || true
    sleep 0.5
    sudo rm -rf "$RIG_DIR" "$NODE_DIR" 2>/dev/null || true
}

idle_now() {
    local busy
    busy=$(ps -eo pcpu=,comm= --sort=-pcpu | awk '$1 > 20 { printf "%s(%s) ", $2, $1 }' | head -c 120)
    [ -z "$busy" ] || { echo "  busy: $busy" >&2; return 1; }
    return 0
}

run_cell() { # $1 = round tag, $2 = arm, $3 = rate:length
    local tag=$1 arm=$2 pair=$3
    local rate=${pair%%:*} length=${pair##*:}
    local cell="$OUT/$tag-$arm/${rate}_length=${length}"
    mkdir -p "$cell/run-1"
    kill_side
    idle_now || return 1

    echo "== $tag arm=$arm $pair"
    driver_start_arm "$arm" rig "$cell/driver-rig.log" "$cell/driver-rig.settings"
    driver_start_arm "$arm" node "$cell/driver-node.log" "$cell/driver-node.settings"

    await_cnc "$RIG_DIR" || { echo "  the rig-side driver never published its CnC"; tail -10 "$cell/driver-rig.log"; return 1; }
    await_cnc "$NODE_DIR" || { echo "  the node-side driver never published its CnC"; tail -10 "$cell/driver-node.log"; return 1; }
    # The drivers are root; the rig and the node are this user and have to mmap
    # a CnC they can write to (the client keepalive). D6 learned this the hard
    # way: a client that cannot open the CnC only says so as a timeout.
    sudo chown -R "$(id -u):$(id -g)" "$RIG_DIR" "$NODE_DIR" 2>/dev/null || true
    sleep "$LINK_WAIT"   # a media port's link takes ~20 s; not in the window

    for log in "$cell/driver-rig.log" "$cell/driver-node.log"; do
        grep -q "dpdk media on" "$log" || { echo "  no media line in $log"; tail -10 "$log"; return 1; }
    done

    # The drivers are root (D6's rule: one user for both sides), so the clients
    # are too — the publication's log buffer under the CnC is created by the
    # driver, and a client that cannot write it only says so as a mapping error
    # (measured here: the C arm's node died with `Permission denied` on
    # `publications/2.logbuffer` before this).
    if [ "${E1_PERF:-0}" = "1" ]; then
        # The two drivers, not the clients: cycles per frame and the symbols
        # the time is in, on the side that owns the media.
        # Shorter than the cell: the first attempt used +20 s, so `kill_side`
        # SIGKILLed `perf stat` before it wrote its report (an empty file, and
        # the evidence came from `perf record` instead).
        local window=$((WARMUP + ITERATIONS - 5))
        sudo perf stat -p "$RIG_DRIVER_PID" -p "$NODE_DRIVER_PID" \
            -e cycles,instructions,cache-references,cache-misses -o "$cell/perf-stat.txt" \
            -- sleep "$window" 2>>"$cell/perf-stat.err" &
        sudo perf record -F 199 -p "$RIG_DRIVER_PID" -o "$cell/perf.data" \
            -- sleep "$((window - 5))" 2>>"$cell/perf-record.err" &
        sleep 1
    fi
    sudo -E taskset -c "$NODE_CPU" "$RUST_BIN/echo-node" \
        "-Daeron.dir=$NODE_DIR" \
        "-Dio.aeron.benchmarks.output.directory=$cell" \
        "-Dio.aeron.benchmarks.aeron.destination.channel=aeron:udp?endpoint=$NODE_ADDRESS:$DESTINATION_PORT|mtu=1408" \
        "-Dio.aeron.benchmarks.aeron.source.channel=aeron:udp?endpoint=$RIG_ADDRESS:$SOURCE_PORT|mtu=1408" \
        >"$cell/node.log" 2>&1 &
    local node_pid=$!
    sleep 1
    kill -0 "$node_pid" 2>/dev/null || { echo "  the node died"; tail -10 "$cell/node.log"; return 1; }

    sudo -E timeout --signal=KILL 240 taskset -c "$RIG_CPU" "$RUST_BIN/loadtest-rig" \
        -Dio.aeron.benchmarks.message.transceiver=echo \
        "-Daeron.dir=$RIG_DIR" \
        -Dio.aeron.benchmarks.aeron.embedded.media.driver=false \
        "-Dio.aeron.benchmarks.output.directory=$cell/run-1" \
        -Dio.aeron.benchmarks.output.file=med \
        "-Dio.aeron.benchmarks.aeron.destination.channel=aeron:udp?endpoint=$NODE_ADDRESS:$DESTINATION_PORT|mtu=1408" \
        "-Dio.aeron.benchmarks.aeron.source.channel=aeron:udp?endpoint=$RIG_ADDRESS:$SOURCE_PORT|mtu=1408" \
        "-Dio.aeron.benchmarks.message.rate=$rate" \
        "-Dio.aeron.benchmarks.warmup.message.rate=$WARMUP_RATE" \
        "-Dio.aeron.benchmarks.iterations=$ITERATIONS" \
        "-Dio.aeron.benchmarks.warmup.iterations=$WARMUP" \
        "-Dio.aeron.benchmarks.message.length=$length" \
        -Dio.aeron.benchmarks.batch.size=1 \
        -Dio.aeron.benchmarks.aeron.use.try.claim=true \
        -Dio.aeron.benchmarks.output.time.unit=MICROSECONDS \
        >"$cell/run-1/client.log" 2>&1 || true

    keep_counters "$RIG_DIR" "$cell/counters-rig.txt" || true
    keep_counters "$NODE_DIR" "$cell/counters-node.txt" || true
    kill_side
    validate_cell "$cell" "$rate" "$length"
}

validate_cell() { # $1 = cell, $2 = rate, $3 = length
    local cell=$1 rate=$2 length=$3 log="$1/run-1/client.log"
    local ok=0
    [ -f "$log" ] || { echo "  no client log"; return 1; }
    grep -q 'WARNING:' "$log" && { echo "  WARNING: $(grep -m1 'WARNING:' "$log")"; ok=1; }
    local hdrs fails
    hdrs=$(find "$cell/run-1" -maxdepth 1 -name '*.hdr' 2>/dev/null | wc -l)
    fails=$(find "$cell/run-1" -maxdepth 1 -name '*.FAIL' 2>/dev/null | wc -l)
    [ "$hdrs" -eq 1 ] && [ "$fails" -eq 0 ] || { echo "  histograms: $hdrs .hdr / $fails .FAIL"; ok=1; }
    # The identity the harness itself checks: rate x iterations messages.
    local number
    case "$rate" in *K) number=$(( ${rate%K} * 1000 )) ;; *M) number=$(( ${rate%M} * 1000000 )) ;; *) number=$rate ;; esac
    local expected=$(( number * ITERATIONS ))
    local total
    total=$(grep -o 'Total count  = *[0-9]*' "$log" | tail -1 | grep -o '[0-9]*$' || true)
    [ "${total:-0}" = "$expected" ] || { echo "  count ${total:-?} != $expected"; ok=1; }
    # The media's drop counters: 77 nobufs, 84 missed, 88 queue drop.
    local drops
    drops=$(awk '$1 ~ /^[0-9]+$/ && ($2 == 77 || $2 == 84 || $2 == 88) { s += $3 } END { print s + 0 }' \
        "$cell/counters-rig.txt" "$cell/counters-node.txt" 2>/dev/null || echo "?")
    [ "$drops" = "0" ] || { echo "  media drop counters (77+84+88) = $drops"; ok=1; }
    [ "$ok" -eq 0 ] && echo "  ok: n=$total, no drops"
    return "$ok"
}

# --- the session --------------------------------------------------------------

mkdir -p "$OUT"
write_fingerprint
if [ "$mode" = list ]; then
    list_plan
    preflight || { echo "preflight: NOT ready"; exit 1; }
    echo "preflight: ready"
    exit 0
fi
preflight || { echo "preflight failed; nothing run" >&2; exit 1; }

status=0
trap 'restore_ports' EXIT
bind_ports_vfio || { echo "the ports would not bind to vfio" >&2; exit 1; }

for round in $ROUNDS; do
    tag=${round%%:*}; arm=${round##*:}
    for pair in ${RATES//,/ }; do
        if ! run_cell "$tag" "$arm" "$pair"; then status=1; fi
    done
done

echo "== session done ($(date -Iseconds)); output: $OUT"
exit "$status"
