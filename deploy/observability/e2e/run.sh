#!/usr/bin/env bash
# The L2 end-to-end stack: VictoriaLogs, VictoriaMetrics, vmauth and the agent.
#
# This commit proves the four processes start, listen and stay alive. The
# applications, the exporters and the pipeline assertions arrive in the next
# commits; a run that gets here has proved the configuration loads and the
# backends answer, nothing more.
#
# Everything lives under target/observability-e2e/<timestamp>-<pid>/. A failed
# run keeps it (logs, listeners, process table) and says where it is; a
# successful one removes it unless E2E_KEEP=1.
set -euo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO=$(cd "$HERE/../../.." && pwd)
# shellcheck source=versions.env
. "$HERE/versions.env"
BIN=${DEEPMSG_OBS_TOOLS:-$REPO/target/observability-tools}/bin

VL_PORT=${E2E_VL_PORT:-9428}
VM_PORT=${E2E_VM_PORT:-8428}
VMAUTH_PORT=${E2E_VMAUTH_PORT:-8427}
COUNTERS_PORT=${E2E_COUNTERS_PORT:-9101}
TELEMETRY_PORT=8888 # the config pins it; a second value would be a second config
TOKEN=${E2E_OBS_TOKEN:-e2e-token}

ROOT=${DEEPMSG_OBS_E2E_DIR:-$REPO/target/observability-e2e}
WORK=$ROOT/$(date +%Y%m%d-%H%M%S)-$$
KEEP=${E2E_KEEP:-0}
STATUS=started
declare -A PID=()

fail() {
  echo "e2e: $*" >&2
  if [[ -d ${WORK:-} ]]; then
    local pids
    pids=$(IFS=,; echo "${PID[*]:-0}")
    echo "e2e: --- processes ---" >&2
    ps -o pid,stat,cmd -p "$pids" >&2 || true
    echo "e2e: --- listeners ---" >&2
    ss -ltn >&2 || true
    for name in "${!PID[@]}"; do
      if [[ -f $WORK/$name.log ]]; then
        echo "e2e: --- $name.log (last 40 lines) ---" >&2
        tail -n 40 "$WORK/$name.log" >&2 || true
      fi
    done
  fi
  exit 1
}

cleanup() {
  local code=$?
  local name
  for name in "${!PID[@]}"; do
    [[ -n ${PID[$name]:-} ]] && kill "${PID[$name]}" 2>/dev/null || true
  done
  sleep 0.2
  for name in "${!PID[@]}"; do
    [[ -n ${PID[$name]:-} ]] && kill -9 "${PID[$name]}" 2>/dev/null || true
  done
  if [[ $code -eq 0 && $STATUS == ok && $KEEP != 1 ]]; then
    [[ -d $WORK ]] && rm -rf "$WORK"
  elif [[ -d $WORK ]]; then
    echo "e2e: evidence in $WORK" >&2
  fi
  exit "$code"
}
trap cleanup EXIT

need() { command -v "$1" >/dev/null || fail "missing $1"; }
for tool in curl ss awk grep tail ps python3; do need "$tool"; done

alive() {
  local pid=$1 stat
  kill -0 "$pid" 2>/dev/null || return 1
  stat=$(ps -o stat= -p "$pid" 2>/dev/null) || return 1
  [[ $stat != Z* ]]
}

port_is_free() {
  local port=$1
  ! ss -ltnH | awk '{print $4}' | grep -qE "[:.]$port\$"
}

for pair in "$VL_PORT:victoria-logs" "$VM_PORT:victoria-metrics" \
  "$VMAUTH_PORT:vmauth" "$TELEMETRY_PORT:collector telemetry" \
  "$COUNTERS_PORT:counters exporter"; do
  port=${pair%%:*}
  label=${pair#*:}
  port_is_free "$port" || fail "port $port ($label) is already listening"
done

[[ $TELEMETRY_PORT == 8888 ]] || fail "the agent config pins the telemetry port at 8888"

"$HERE/fetch-tools.sh"
python3 "$HERE/check-config.py" || fail "the agent config failed its static checks"

mkdir -p "$WORK"/{vl,vm,state,logs,evidence}

# vmauth is the only way in: the agent and the E2E both hold one token.
cat >"$WORK/vmauth.yaml" <<YAML
users:
  - bearer_token: "$TOKEN"
    url_map:
      - src_paths: ["/insert/opentelemetry/v1/logs", "/select/logsql/query"]
        url_prefix: "http://127.0.0.1:$VL_PORT"
      - src_paths: ["/api/v1/write", "/api/v1/query", "/api/v1/label"]
        url_prefix: "http://127.0.0.1:$VM_PORT"
YAML

start() {
  local name=$1
  shift
  "$@" >"$WORK/$name.log" 2>&1 &
  PID[$name]=$!
  echo "e2e: started $name (pid ${PID[$name]})"
}

start victoria-logs "$BIN/victoria-logs" \
  -storageDataPath "$WORK/vl" -httpListenAddr "127.0.0.1:$VL_PORT" \
  -retentionPeriod=30d -retention.maxDiskUsagePercent=70
start victoria-metrics "$BIN/victoria-metrics" \
  -storageDataPath "$WORK/vm" -httpListenAddr "127.0.0.1:$VM_PORT" \
  -retentionPeriod=90d
start vmauth "$BIN/vmauth" \
  -auth.config "$WORK/vmauth.yaml" -httpListenAddr "127.0.0.1:$VMAUTH_PORT"
start otelcol env \
  DEEPMSG_LOG_DIR="$WORK/logs" \
  DEEPMSG_OBS_STATE_DIR="$WORK/state" \
  DEEPMSG_COUNTERS_ENDPOINT="127.0.0.1:$COUNTERS_PORT" \
  DEEPMSG_OTELCOL_TELEMETRY_ENDPOINT="127.0.0.1:$TELEMETRY_PORT" \
  DEEPMSG_VL_LOGS_ENDPOINT="http://127.0.0.1:$VMAUTH_PORT/insert/opentelemetry/v1/logs" \
  DEEPMSG_VM_WRITE_ENDPOINT="http://127.0.0.1:$VMAUTH_PORT/api/v1/write" \
  DEEPMSG_OBS_WRITE_TOKEN="$TOKEN" \
  "$BIN/otelcol-contrib" --config "$REPO/deploy/observability/agent/otelcol.yaml"

wait_http() {
  local name=$1 url=$2 timeout=${3:-30}
  local deadline=$((SECONDS + timeout))
  while ((SECONDS < deadline)); do
    alive "${PID[$name]}" || fail "$name exited early"
    curl -sf -o /dev/null "$url" && return 0
    sleep 0.2
  done
  fail "$name did not answer $url within ${timeout}s"
}

wait_any_http() {
  local name=$1 url=$2 timeout=${3:-30}
  local deadline=$((SECONDS + timeout))
  while ((SECONDS < deadline)); do
    alive "${PID[$name]}" || fail "$name exited early"
    curl -s -o /dev/null "$url" && return 0
    sleep 0.2
  done
  fail "$name did not answer $url within ${timeout}s"
}

wait_http victoria-logs "http://127.0.0.1:$VL_PORT/health"
wait_http victoria-metrics "http://127.0.0.1:$VM_PORT/health"
wait_any_http vmauth "http://127.0.0.1:$VMAUTH_PORT/"
wait_http otelcol "http://127.0.0.1:$TELEMETRY_PORT/metrics"

for name in victoria-logs victoria-metrics vmauth otelcol; do
  alive "${PID[$name]}" || fail "$name is not alive after readiness"
done

for pair in "$VL_PORT" "$VM_PORT" "$VMAUTH_PORT" "$TELEMETRY_PORT"; do
  ss -ltnH | awk '{print $4}' | grep -qE "[:.]$pair\$" ||
    fail "nothing is listening on $pair"
done

# The collector's own metrics prove it is the process answering, not a
# leftover listener from another run.
curl -sf "http://127.0.0.1:$TELEMETRY_PORT/metrics" | grep -q '^otelcol_' ||
  fail "the collector's telemetry has no otelcol_ metrics"

echo "e2e: four processes alive; ports $VL_PORT/$VM_PORT/$VMAUTH_PORT/$TELEMETRY_PORT"
"$BIN/victoria-logs" --version | head -n 1
"$BIN/victoria-metrics" --version | head -n 1
"$BIN/vmauth" --version | head -n 1
"$BIN/otelcol-contrib" --version | head -n 1

STATUS=ok
