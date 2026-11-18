#!/usr/bin/env bash
#
# P0: **does this build's Rust client drive the C++ driver?**
#
# The E1 batch (Rust DPDK media vs C++ DPDK media) puts one driver under the
# Rust rig and the Rust echo node in both arms, so the first question is not
# performance but whether that is possible at all: the rig and the node speak the
# 1.53.2 client protocol, the C++ driver is the reference C driver at 1.54.0.
#
# One C++ `deepmsg-media-driver` (the kernel path: no DPDK, no vfio, no root),
# one `aeron.dir`, both clients on loopback. The verdict is the harness's own:
# no `WARNING:` line, one `.hdr` and not a `.FAIL`, and the rig's own OK/PASSED
# line. The counters are read with this tree's `cnc-dump` afterwards, which is
# also the check that the two CnC readers agree.
#
#     ./p0-smoke.sh
#
#   RUST_BIN     the release directory holding loadtest-rig / echo-node / cnc-dump
#   CPP_DRIVER   the C++ deepmsg-media-driver binary
#   OUT          where the logs and the cell go
set -euo pipefail

RUST_BIN=${RUST_BIN:-/home/gavin/data/trader/deepmsg/target/release}
CPP_DRIVER=${CPP_DRIVER:-/home/gavin/data/trader/pr68-wt/build/debug/apps/deepmsg-media-driver/deepmsg-media-driver}
OUT=${OUT:-/home/gavin/data/trader/analysis/dpdk/p0-out}
AERON_DIR=${AERON_DIR:-/dev/shm/aeron-p0}
RATE=${P0_RATE:-25K}
LENGTH=${P0_LENGTH:-32}
ITERATIONS=${P0_ITERATIONS:-5}
WARMUP=${P0_WARMUP:-2}
RIG_CPU=${P0_RIG_CPU:-4}
NODE_CPU=${P0_NODE_CPU:-5}

for binary in "$RUST_BIN/loadtest-rig" "$RUST_BIN/echo-node" "$RUST_BIN/cnc-dump" "$CPP_DRIVER"; do
    [ -x "$binary" ] || { echo "missing binary: $binary" >&2; exit 2; }
done

rm -rf "$OUT" "$AERON_DIR"
mkdir -p "$OUT/cell/run-1"

driver_pid=""
node_pid=""
cleanup() {
    [ -n "$node_pid" ] && kill -9 "$node_pid" 2>/dev/null || true
    [ -n "$driver_pid" ] && kill -9 "$driver_pid" 2>/dev/null || true
    sleep 0.3
    pkill -9 -x echo-node 2>/dev/null || true
    pkill -9 -x loadtest-rig 2>/dev/null || true
    pkill -9 -x deepmsg-media-driver 2>/dev/null || true
}
trap cleanup EXIT

echo "== 1. the C++ driver (kernel path), aeron.dir=$AERON_DIR"
"$CPP_DRIVER" \
    "-Daeron.dir=$AERON_DIR" \
    -Daeron.threading.mode=SHARED \
    -Daeron.shared.idle.strategy=spin \
    "-Daeron.socket.so_sndbuf=2m" "-Daeron.socket.so_rcvbuf=2m" \
    -Daeron.mtu.length=1408 \
    -Daeron.rcv.initial.window.length=2m \
    >"$OUT/driver.log" 2>&1 &
driver_pid=$!

for _ in $(seq 1 200); do
    [ -f "$AERON_DIR/cnc.dat" ] && break
    kill -0 "$driver_pid" 2>/dev/null || { echo "the driver died: " >&2; cat "$OUT/driver.log" >&2; exit 1; }
    sleep 0.1
done
[ -f "$AERON_DIR/cnc.dat" ] || { echo "no CnC file after 20 s" >&2; cat "$OUT/driver.log" >&2; exit 1; }
echo "   driver pid=$driver_pid, CnC present"

echo "== 2. the Rust echo node"
taskset -c "$NODE_CPU" "$RUST_BIN/echo-node" \
    "-Daeron.dir=$AERON_DIR" \
    "-Dio.aeron.benchmarks.output.directory=$OUT/cell" \
    >"$OUT/node.log" 2>&1 &
node_pid=$!
sleep 1
kill -0 "$node_pid" 2>/dev/null || { echo "the node died: " >&2; cat "$OUT/node.log" >&2; exit 1; }
echo "   node pid=$node_pid"

echo "== 3. the Rust rig: ${RATE}:${LENGTH}, ${ITERATIONS} iterations (+${WARMUP} warmup)"
timeout --signal=KILL 120 taskset -c "$RIG_CPU" "$RUST_BIN/loadtest-rig" \
    -Dio.aeron.benchmarks.message.transceiver=echo \
    "-Daeron.dir=$AERON_DIR" \
    -Dio.aeron.benchmarks.aeron.embedded.media.driver=false \
    "-Dio.aeron.benchmarks.output.directory=$OUT/cell/run-1" \
    -Dio.aeron.benchmarks.output.file=p0 \
    "-Dio.aeron.benchmarks.message.rate=$RATE" \
    "-Dio.aeron.benchmarks.warmup.message.rate=$RATE" \
    "-Dio.aeron.benchmarks.iterations=$ITERATIONS" \
    "-Dio.aeron.benchmarks.warmup.iterations=$WARMUP" \
    "-Dio.aeron.benchmarks.message.length=$LENGTH" \
    -Dio.aeron.benchmarks.batch.size=1 \
    -Dio.aeron.benchmarks.aeron.use.try.claim=true \
    -Dio.aeron.benchmarks.output.time.unit=MICROSECONDS \
    >"$OUT/rig.log" 2>&1
rig_rc=$?

echo "== 4. verdict"
status=0
if [ "$rig_rc" -ne 0 ]; then
    echo "  FAIL the rig exited $rig_rc"
    status=1
fi
if grep -q 'WARNING:' "$OUT/rig.log"; then
    echo "  FAIL the rig printed: $(grep -m1 'WARNING:' "$OUT/rig.log")"
    status=1
fi
hdrs=$(find "$OUT/cell/run-1" -maxdepth 1 -name '*.hdr' 2>/dev/null | wc -l)
fails=$(find "$OUT/cell/run-1" -maxdepth 1 -name '*.FAIL' 2>/dev/null | wc -l)
if [ "$hdrs" -ne 1 ] || [ "$fails" -ne 0 ]; then
    echo "  FAIL histograms: $hdrs .hdr, $fails .FAIL"
    status=1
fi
if ! "$RUST_BIN/cnc-dump" --counters "$AERON_DIR" >"$OUT/counters.txt" 2>"$OUT/counters.err"; then
    echo "  FAIL the counters could not be read: $(cat "$OUT/counters.err")"
    status=1
else
    echo "  counters: $(wc -l <"$OUT/counters.txt") lines"
fi
echo "--- rig.log tail:"
tail -12 "$OUT/rig.log" | sed 's/^/  /'
echo "--- driver.log tail:"
tail -6 "$OUT/driver.log" | sed 's/^/  /'
echo "--- node.log tail:"
tail -4 "$OUT/node.log" | sed 's/^/  /'
if [ "$status" -eq 0 ]; then
    echo "P0 PASSED: the Rust client drives the C++ driver."
else
    echo "P0 FAILED."
fi
exit "$status"
