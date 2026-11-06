#!/usr/bin/env bash
# Drill: swap the log backend through vmauth and back, without touching the
# applications.
#
# The L2 stack is held open (E2E_HOLD), a second VictoriaLogs is started, the
# vmauth auth config's VictoriaLogs route is pointed at it and vmauth is
# restarted. Known lines go to the old backend before, the new backend after,
# and the old one again after the rollback.
set -euo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO=$(cd "$HERE/../../../.." && pwd)
# shellcheck source=versions.env
. "$HERE/versions.env"

CACHE=${DEEPMSG_OBS_TOOLS:-$REPO/target/observability-tools}
BIN=$CACHE/bin
TOKEN=${E2E_OBS_TOKEN:-e2e-token}
VL2_PORT=${E2E_VL2_PORT:-9528}

ROOT=${DEEPMSG_OBS_E2E_DIR:-$REPO/target/observability-e2e}
WORK=$ROOT/swap-$(date +%Y%m%d-%H%M%S)-$$
KEEP=${E2E_KEEP:-0}
STATUS=started
STACK_PID=""
VL2_PID=""
VMAUTH_CURRENT=""
STACK_WORK=""

fail() { echo "swap-backend: $*" >&2; exit 1; }

dump() {
  [[ -d ${WORK:-} ]] || return 0
  echo "swap-backend: evidence in $WORK" >&2
  for file in "$WORK/stack.log" "$WORK/vl2.log" "$WORK/vmauth-after.log" "$WORK/vmauth-back.log"; do
    [[ -f $file ]] && { echo "--- $file (last 40) ---" >&2; tail -n 40 "$file" >&2; }
  done
}

cleanup() {
  local code=$?
  if [[ -n $VMAUTH_CURRENT ]]; then
    kill "$VMAUTH_CURRENT" 2>/dev/null || true
    wait "$VMAUTH_CURRENT" 2>/dev/null || true
  fi
  if [[ -n $VL2_PID ]]; then
    kill "$VL2_PID" 2>/dev/null || true
    wait "$VL2_PID" 2>/dev/null || true
  fi
  if [[ -n $STACK_PID ]]; then
    kill -TERM "$STACK_PID" 2>/dev/null || true
    wait "$STACK_PID" 2>/dev/null || true
  fi
  if [[ $code -eq 0 && $STATUS == ok && $KEEP != 1 ]]; then
    [[ -n $STACK_WORK && -d $STACK_WORK ]] && rm -rf "$STACK_WORK"
    [[ -d $WORK ]] && rm -rf "$WORK"
  else
    dump
  fi
  exit "$code"
}
trap cleanup EXIT

need() { command -v "$1" >/dev/null || fail "missing $1"; }
for tool in curl ss jq; do need "$tool"; done

if ss -ltnH | awk '{print $4}' | grep -qE "[:.]$VL2_PORT\$"; then
  fail "port $VL2_PORT is already listening"
fi
mkdir -p "$WORK"

wait_http() {
  local url=$1 timeout=${2:-60}
  local deadline=$((SECONDS + timeout))
  while ((SECONDS < deadline)); do
    curl -sf -o /dev/null "$url" && return 0
    sleep 0.2
  done
  fail "nothing answered $url within ${timeout}s"
}
wait_any() { # any HTTP answer is enough for vmauth (401 included)
  local url=$1 timeout=${2:-60}
  local deadline=$((SECONDS + timeout))
  while ((SECONDS < deadline)); do
    curl -s -o /dev/null "$url" && return 0
    sleep 0.2
  done
  fail "nothing answered $url within ${timeout}s"
}

# The stack, held.
READY=$WORK/stack-ready
echo "swap-backend: starting the L2 stack in hold mode"
E2E_HOLD=$READY E2E_KEEP=1 E2E_OBS_TOKEN=$TOKEN \
  "$REPO/deploy/observability/e2e/run.sh" >"$WORK/stack.log" 2>&1 &
STACK_PID=$!
deadline=$((SECONDS + 900))
while [[ ! -f $READY ]]; do
  kill -0 "$STACK_PID" 2>/dev/null || fail "the stack E2E exited before it held"
  ((SECONDS < deadline)) || fail "the stack did not hold within 900 s"
  sleep 2
done
STACK_WORK=$(sed -n 's/^WORK=//p' "$READY")
VL_PORT=$(sed -n 's/^VL_PORT=//p' "$READY")
VMAUTH_PORT=$(sed -n 's/^VMAUTH_PORT=//p' "$READY")
VMAUTH_PID=$(sed -n 's/^VMAUTH_PID=//p' "$READY")
[[ -n $STACK_WORK && -n $VL_PORT && -n $VMAUTH_PORT && -n $VMAUTH_PID ]] ||
  fail "the stack's ready file is incomplete"
echo "swap-backend: the stack is up (work $STACK_WORK)"

# The second backend.
"$BIN/victoria-logs" -storageDataPath "$WORK/vl2" -httpListenAddr "127.0.0.1:$VL2_PORT" \
  -retentionPeriod=30d >"$WORK/vl2.log" 2>&1 &
VL2_PID=$!
wait_http "http://127.0.0.1:$VL2_PORT/health" 60
echo "swap-backend: the second backend is up on $VL2_PORT"

line() {
  printf '{"ts":"%s","level":"info","component":"tool","node_id":"e2e","event":"backpressure.enter","msg":"%s"}\n' \
    "$(date -u +%Y-%m-%dT%H:%M:%S.000000000Z)" "$1" >>"$STACK_WORK/logs/swap-e2e.json"
}
rows() { # port prefix
  curl -s "http://127.0.0.1:$1/select/logsql/query" \
    --data-urlencode 'query=event:"backpressure.enter"' --data-urlencode 'limit=100' |
    jq -r 'select(.event == "backpressure.enter") | ._msg' 2>/dev/null | grep -c "^$2" || true
}
wait_rows() { # port prefix expected timeout
  local port=$1 prefix=$2 expected=$3 timeout=${4:-60}
  local deadline=$((SECONDS + timeout)) count
  while ((SECONDS < deadline)); do
    count=$(rows "$port" "$prefix")
    [[ $count -ge $expected ]] && return 0
    sleep 1
  done
  fail "'$prefix' did not reach the backend on $port (saw $count)"
}

# 1) The old backend takes a known line.
line swap-before
wait_rows "$VL_PORT" swap-before 1 60
echo "swap-backend: the old backend has swap-before"

# 2) Swap vmauth's VictoriaLogs route to the second backend.
cp "$STACK_WORK/vmauth.yaml" "$WORK/vmauth-before.yaml"
sed "s|http://127.0.0.1:$VL_PORT|http://127.0.0.1:$VL2_PORT|" \
  "$WORK/vmauth-before.yaml" >"$WORK/vmauth-after.yaml"
grep -q "127.0.0.1:$VL2_PORT" "$WORK/vmauth-after.yaml" ||
  fail "the swapped config does not name the second backend"
kill "$VMAUTH_PID" 2>/dev/null || true
for _ in $(seq 1 20); do
  kill -0 "$VMAUTH_PID" 2>/dev/null || break
  sleep 0.2
done
"$BIN/vmauth" -auth.config "$WORK/vmauth-after.yaml" \
  -httpListenAddr "127.0.0.1:$VMAUTH_PORT" >"$WORK/vmauth-after.log" 2>&1 &
VMAUTH_CURRENT=$!
wait_any "http://127.0.0.1:$VMAUTH_PORT/" 30

# 3) The new backend takes the next line; the old one stops.
line swap-after
wait_rows "$VL2_PORT" swap-after 1 90
[[ $(rows "$VL_PORT" swap-after) == 0 ]] ||
  fail "the old backend still received swap-after"
echo "swap-backend: the new backend has swap-after; the old one did not"

# 4) Roll back.
kill "$VMAUTH_CURRENT" 2>/dev/null || true
wait "$VMAUTH_CURRENT" 2>/dev/null || true
VMAUTH_CURRENT=""
"$BIN/vmauth" -auth.config "$WORK/vmauth-before.yaml" \
  -httpListenAddr "127.0.0.1:$VMAUTH_PORT" >"$WORK/vmauth-back.log" 2>&1 &
VMAUTH_CURRENT=$!
wait_any "http://127.0.0.1:$VMAUTH_PORT/" 30
line swap-rollback
wait_rows "$VL_PORT" swap-rollback 1 60
echo "swap-backend: the rollback put the traffic back"

echo "swap-backend: the drill passed (old -> new -> old)"
STATUS=ok
