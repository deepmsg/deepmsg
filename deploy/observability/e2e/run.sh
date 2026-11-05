#!/usr/bin/env bash
# The L2 end-to-end stack: VictoriaLogs, VictoriaMetrics, vmauth and the agent.
#
# This commit proves the four processes start, listen and stay alive, and that
# both paths work: the log path (the driver runs with no collector, then the
# agent reads that backlog and VictoriaLogs answers), and the metrics path (the
# counters exporter's scrape reaches VictoriaMetrics, the collector's own
# metrics included).
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
  # The agent goes first, so its queue can still reach vmauth: killing the
  # backend first makes a successful run look like a send failure.
  for name in otelcol counters-exporter driver-metrics driver; do
    [[ -n ${PID[$name]:-} ]] && kill "${PID[$name]}" 2>/dev/null || true
  done
  sleep 1
  for name in vmauth victoria-logs victoria-metrics; do
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
for tool in curl ss awk grep tail ps python3 cargo jq date; do need "$tool"; done

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
# src_paths are exact paths (vmauth has no prefix or glob match here), so every
# path the pipeline and a Grafana datasource use is named.
cat >"$WORK/vmauth.yaml" <<YAML
users:
  - bearer_token: "$TOKEN"
    url_map:
      - src_paths:
          - /insert/opentelemetry/v1/logs
          - /select/logsql/query
          - /select/logsql/streams
          - /select/logsql/stream_field_names
          - /select/logsql/stream_field_values
          - /select/logsql/field_names
          - /select/logsql/field_values
        url_prefix: "http://127.0.0.1:$VL_PORT"
      - src_paths:
          - /api/v1/write
          - /api/v1/query
          - /api/v1/query_range
          - /api/v1/series
          - /api/v1/labels
          - /api/v1/label/__name__/values
          - /api/v1/metadata
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

wait_for_line() {
  local file=$1 needle=$2 timeout=${3:-10}
  local deadline=$((SECONDS + timeout))
  while ((SECONDS < deadline)); do
    grep -q "$needle" "$file" 2>/dev/null && return 0
    sleep 0.1
  done
  return 1
}

wait_http victoria-logs "http://127.0.0.1:$VL_PORT/health"
wait_http victoria-metrics "http://127.0.0.1:$VM_PORT/health"
wait_any_http vmauth "http://127.0.0.1:$VMAUTH_PORT/"

# C9: the application runs with the collector not started at all. It must not
# notice — stderr stays empty and the file keeps its lines. The same file is
# the backlog the agent reads in the next step.
cargo build --manifest-path "$REPO/Cargo.toml" -p deepmsg-driver --bin deepmsg-driver \
  >"$WORK/build.log" 2>&1 || fail "the driver did not build; see $WORK/build.log"
DRIVER_BIN=${DEEPMSG_DRIVER_BIN:-$REPO/target/debug/deepmsg-driver}
DEEPMSG_LOG_DIR="$WORK/logs" DEEPMSG_NODE_ID=e2e \
  "$DRIVER_BIN" -Daeron.dir="$WORK/aeron" >"$WORK/driver.out" 2>"$WORK/driver.err" &
PID[driver]=$!
echo "e2e: started driver (pid ${PID[driver]}, no collector)"
wait_for_line "$WORK/logs/driver-e2e.json" '"event":"driver.start"' 15 ||
  fail "the driver did not record driver.start"
kill -TERM "${PID[driver]}" 2>/dev/null || true
driver_status=0
wait "${PID[driver]}" || driver_status=$?
unset 'PID[driver]'
# The driver returns the signal number, by design: a signalled driver must not
# look like one that was asked to stop.
[[ $driver_status == 15 ]] || fail "the driver exited with $driver_status, not 15"
[[ $(stat -c%s "$WORK/driver.err") == 0 ]] || fail "the driver wrote to stderr with no collector"
grep -q '"event":"driver.start"' "$WORK/logs/driver-e2e.json" || fail "driver.start is missing"
grep -q '"event":"driver.stop"' "$WORK/logs/driver-e2e.json" || fail "driver.stop is missing"
echo "e2e: C9: the driver ran with no collector; stderr 0 bytes, start and stop on disk"

# Now the agent, reading that file from its start.
start otelcol env \
  DEEPMSG_LOG_DIR="$WORK/logs" \
  DEEPMSG_OBS_STATE_DIR="$WORK/state" \
  DEEPMSG_COUNTERS_ENDPOINT="127.0.0.1:$COUNTERS_PORT" \
  DEEPMSG_OTELCOL_TELEMETRY_ENDPOINT="127.0.0.1:$TELEMETRY_PORT" \
  DEEPMSG_VL_LOGS_ENDPOINT="http://127.0.0.1:$VMAUTH_PORT/insert/opentelemetry/v1/logs" \
  DEEPMSG_VM_WRITE_ENDPOINT="http://127.0.0.1:$VMAUTH_PORT/api/v1/write" \
  DEEPMSG_OBS_WRITE_TOKEN="$TOKEN" \
  "$BIN/otelcol-contrib" --config "$REPO/deploy/observability/agent/otelcol.yaml"
wait_http otelcol "http://127.0.0.1:$TELEMETRY_PORT/metrics"

logs_query() {
  curl -s -H "Authorization: Bearer $TOKEN" \
    --data-urlencode "query=$1" --data-urlencode "limit=20" \
    "http://127.0.0.1:$VMAUTH_PORT/select/logsql/query"
}

wait_logs() {
  local query=$1 predicate=$2 timeout=${3:-30}
  local deadline=$((SECONDS + timeout)) out
  while ((SECONDS < deadline)); do
    out=$(logs_query "$query")
    if printf '%s' "$out" | jq -s -e "$predicate" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.5
  done
  echo "e2e: last answer for $query: $out" >&2
  return 1
}

# C1: the backlog written with the collector off is in VictoriaLogs.
wait_logs 'event:"driver.start"' 'any(.[]; .event == "driver.start")' 30 ||
  fail "VictoriaLogs never saw the driver's backlog"
row=$(logs_query 'event:"driver.start"' | jq -c 'select(.event == "driver.start")' | head -n 1)
[[ -n $row ]] || fail "no driver.start row"
message=$(printf '%s' "$row" | jq -r '._msg')
[[ $message == driver.start ]] || fail "driver.start's _msg is '$message', not its event name"
[[ $(printf '%s' "$row" | jq -r '.component') == driver ]] || fail "component is not driver"
[[ $(printf '%s' "$row" | jq -r '.node_id') == e2e ]] || fail "node_id is not e2e"
[[ $(printf '%s' "$row" | jq -r '.level') == info ]] || fail "level is not info"
[[ $(printf '%s' "$row" | jq -r '.severity_text') == info ]] || fail "severity_text is not info"
echo "e2e: C1: driver.start reached VictoriaLogs; _msg is its event name"

# C6: the stream identifies the source, and level/event stay ordinary fields.
stream=$(printf '%s' "$row" | jq -r '._stream')
[[ $stream == *'component="driver"'* && $stream == *'node_id="e2e"'* ]] ||
  fail "_stream does not name the source: $stream"
[[ $stream != *level* && $stream != *event* ]] ||
  fail "level or event leaked into _stream: $stream"
echo "e2e: C6: _stream is $stream"

# C1's other half: a line that has a message gets it in _msg. This is a config
# vector, not the application: the driver's lifecycle events have no message.
timestamp=$(date -u +%Y-%m-%dT%H:%M:%S.000000000Z)
printf '{"ts":"%s","level":"info","component":"tool","node_id":"e2e","event":"backpressure.enter","msg":"a synthetic message"}\n' \
  "$timestamp" >>"$WORK/logs/tool-e2e.json"
wait_logs 'event:"backpressure.enter"' \
  'any(.[]; .event == "backpressure.enter" and ._msg == "a synthetic message")' 30 ||
  fail "a line with msg did not reach _msg"
echo "e2e: C1: a line with msg reaches _msg as text"

# C2/C8: a live driver, the counters exporter, the agent's scrape, and
# VictoriaMetrics answering. A second driver on its own directory keeps a
# heartbeat fresh; the first one is gone and its directory is not reused.
cargo build --manifest-path "$REPO/Cargo.toml" -p deepmsg-tools --bin deepmsg-counters-exporter \
  >"$WORK/build-exporter.log" 2>&1 || fail "the counters exporter did not build; see $WORK/build-exporter.log"
DEEPMSG_LOG_DIR="$WORK/logs" DEEPMSG_NODE_ID=e2e \
  "$DRIVER_BIN" -Daeron.dir="$WORK/aeron-metrics" >"$WORK/driver-metrics.out" 2>"$WORK/driver-metrics.err" &
PID[driver-metrics]=$!
echo "e2e: started driver-metrics (pid ${PID[driver-metrics]})"

EXPORTER_BIN=${DEEPMSG_COUNTERS_EXPORTER_BIN:-$REPO/target/debug/deepmsg-counters-exporter}
"$EXPORTER_BIN" --dir "$WORK/aeron-metrics" --listen "127.0.0.1:$COUNTERS_PORT" --node-id e2e \
  >"$WORK/counters-exporter.log" 2>&1 &
PID[counters-exporter]=$!
echo "e2e: started counters-exporter (pid ${PID[counters-exporter]})"

vm_query() {
  curl -s -H "Authorization: Bearer $TOKEN" --get \
    --data-urlencode "query=$1" "http://127.0.0.1:$VMAUTH_PORT/api/v1/query"
}

wait_promql() {
  local query=$1 timeout=${2:-90}
  local deadline=$((SECONDS + timeout)) out
  while ((SECONDS < deadline)); do
    out=$(vm_query "$query")
    if printf '%s' "$out" | jq -e '.data.result | length > 0' >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "e2e: last answer for $query: $out" >&2
  return 1
}

# The exporter must first see the driver; then the agent's scrape has to run
# (15 s interval), pass the batch, and be written.
exporter_up_deadline=$((SECONDS + 30))
until curl -sf "http://127.0.0.1:$COUNTERS_PORT/metrics" |
  grep -q 'deepmsg_driver_up{node_id="e2e"} 1'; do
  ((SECONDS < exporter_up_deadline)) || fail "the counters exporter never saw the driver"
  sleep 0.5
done
echo "e2e: the counters exporter reports the driver up"

wait_promql 'deepmsg_driver_up{node_id="e2e"}' 90 ||
  fail "deepmsg_driver_up did not reach VictoriaMetrics"
wait_promql 'deepmsg_driver_heartbeat_age_seconds{node_id="e2e"}' 90 ||
  fail "the heartbeat age did not reach VictoriaMetrics"
wait_promql 'deepmsg_error_total{node_id="e2e",component="driver"}' 90 ||
  fail "deepmsg_error_total did not reach VictoriaMetrics"
heartbeat=$(vm_query 'deepmsg_driver_heartbeat_age_seconds{node_id="e2e"}' |
  jq -r '.data.result[0].value[1] // empty')
[[ -n $heartbeat ]] || fail "the heartbeat series disappeared between two queries"
awk -v value="$heartbeat" 'BEGIN { exit !(value < 10) }' ||
  fail "the heartbeat age is $heartbeat, not a live driver"
echo "e2e: C2: heartbeat age $heartbeat s, error counter and up are in VictoriaMetrics"

# C8: the collector's own metrics take the same pipeline. Its scrape job runs
# on the same 15 s cycle, so wait for one more turn.
collector_metrics_deadline=$((SECONDS + 90))
until curl -s -H "Authorization: Bearer $TOKEN" \
  "http://127.0.0.1:$VMAUTH_PORT/api/v1/label/__name__/values" |
  jq -e '.data | map(select(startswith("otelcol_exporter_"))) | length > 0' >/dev/null 2>&1; do
  ((SECONDS < collector_metrics_deadline)) ||
    fail "the collector's own metrics are not in VictoriaMetrics"
  sleep 1
done
echo "e2e: C8: otelcol_exporter_* metrics are in VictoriaMetrics"

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
