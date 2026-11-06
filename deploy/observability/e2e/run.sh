#!/usr/bin/env bash
# The L2 end-to-end stack: VictoriaLogs, VictoriaMetrics, vmauth and the agent.
#
# This commit proves the four processes start, listen and stay alive, that both
# paths work (the driver's backlog reaches VictoriaLogs; the counters scrape
# reaches VictoriaMetrics), and that the log path survives the collector going
# away: lines written while it is down — a rotation it did not see included —
# all arrive after the restart, with no line replayed.
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
start_agent() {
  start otelcol env \
    DEEPMSG_LOG_DIR="$WORK/logs" \
    DEEPMSG_OBS_STATE_DIR="$WORK/state" \
    DEEPMSG_COUNTERS_ENDPOINT="127.0.0.1:$COUNTERS_PORT" \
    DEEPMSG_OTELCOL_TELEMETRY_ENDPOINT="127.0.0.1:$TELEMETRY_PORT" \
    DEEPMSG_VL_LOGS_ENDPOINT="http://127.0.0.1:$VMAUTH_PORT/insert/opentelemetry/v1/logs" \
    DEEPMSG_VM_WRITE_ENDPOINT="http://127.0.0.1:$VMAUTH_PORT/api/v1/write" \
    DEEPMSG_OBS_WRITE_TOKEN="$TOKEN" \
    DEEPMSG_NODE_ID=e2e \
    DEEPMSG_HOSTMETRICS_ROOT=/ \
    "$BIN/otelcol-contrib" --config "$REPO/deploy/observability/agent/otelcol.yaml"
}

start_agent
wait_http otelcol "http://127.0.0.1:$TELEMETRY_PORT/metrics"

logs_query() {
  curl -s -H "Authorization: Bearer $TOKEN" \
    --data-urlencode "query=$1" --data-urlencode "limit=100" \
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

# C3/C3b: the collector goes away, the application keeps writing, and the
# agent has to come back without losing a line. The rotation is the writer's
# own move — the base is renamed to `.1.json` and a new base appears — so the
# collector meets exactly the file it meets at the size limit.
kill "${PID[otelcol]}" 2>/dev/null || true
wait "${PID[otelcol]}" || fail "the collector did not stop cleanly"
unset 'PID[otelcol]'
echo "e2e: the collector is down"

# The application writes while nothing collects.
DEEPMSG_LOG_DIR="$WORK/logs" DEEPMSG_NODE_ID=e2e \
  "$DRIVER_BIN" -Daeron.dir="$WORK/aeron-c3" >"$WORK/driver-c3.out" 2>"$WORK/driver-c3.err" &
PID[driver-c3]=$!
wait_for_line "$WORK/driver-c3.out" 'media driver running' 15 ||
  fail "the driver did not start with the collector down"
kill -TERM "${PID[driver-c3]}" 2>/dev/null || true
driver_status=0
wait "${PID[driver-c3]}" || driver_status=$?
unset 'PID[driver-c3]'
[[ $driver_status == 15 ]] || fail "the down-period driver exited with $driver_status"
[[ $(stat -c%s "$WORK/driver-c3.err") == 0 ]] || fail "the down-period driver wrote to stderr"
echo "e2e: C3: the driver ran with the collector down; stderr 0 bytes"

# C3: five more lines in the file the collector was already reading.
for index in 1 2 3 4 5; do
  printf '{"ts":"%s","level":"info","component":"tool","node_id":"e2e","event":"backpressure.enter","msg":"c3-%s"}\n' \
    "$(date -u +%Y-%m-%dT%H:%M:%S.000000000Z)" "$index" >>"$WORK/logs/tool-e2e.json"
done

# C3b: the writer rotates while the collector is down. `.1.json` keeps the
# fingerprint the collector stored; the new base is a file it has never seen.
mv "$WORK/logs/tool-e2e.json" "$WORK/logs/tool-e2e.1.json"
for index in 1 2 3 4 5; do
  printf '{"ts":"%s","level":"info","component":"tool","node_id":"e2e","event":"backpressure.enter","msg":"c3b-%s"}\n' \
    "$(date -u +%Y-%m-%dT%H:%M:%S.000000000Z)" "$index" >>"$WORK/logs/tool-e2e.json"
done

start_agent
wait_http otelcol "http://127.0.0.1:$TELEMETRY_PORT/metrics"

count_rows() {
  logs_query "$1" | jq -c "select($2)" | wc -l
}

wait_marker() {
  local prefix=$1 expected=$2 timeout=${3:-60}
  local deadline=$((SECONDS + timeout)) count
  while ((SECONDS < deadline)); do
    count=$(count_rows 'event:"backpressure.enter"' \
      ".event == \"backpressure.enter\" and (._msg | startswith(\"$prefix\"))")
    [[ $count == "$expected" ]] && return 0
    sleep 1
  done
  echo "e2e: $prefix rows: $count (wanted $expected)" >&2
  return 1
}

wait_marker c3- 5 60 || fail "C3 lost a line written while the collector was down"
wait_marker c3b- 5 60 || fail "C3b lost a line written after a rotation"
echo "e2e: C3/C3b: ten lines written with the collector down all arrived"

# A rotation the collector re-read would duplicate what it already shipped;
# the same line must still be one row.
duplicates=$(count_rows 'event:"backpressure.enter"' '._msg == "a synthetic message"')
[[ $duplicates == 1 ]] || fail "the synthetic line appears $duplicates times"
starts=$(count_rows 'event:"driver.start"' '.event == "driver.start"')
[[ $starts == 3 ]] || fail "driver.start appears $starts times, not 3"
echo "e2e: no replayed lines: the synthetic message once, three driver starts"

# C4: vmauth is the only door. Both directions are checked, because a write
# that needs no token is the one that matters.
status_without_read=$(curl -s -o /dev/null -w '%{http_code}' \
  "http://127.0.0.1:$VMAUTH_PORT/api/v1/label/__name__/values")
[[ $status_without_read == 401 ]] ||
  fail "vmauth answered $status_without_read for a read with no token"
status_without_write=$(curl -s -o /dev/null -w '%{http_code}' -X POST --data-binary '' \
  "http://127.0.0.1:$VMAUTH_PORT/insert/opentelemetry/v1/logs")
[[ $status_without_write == 401 ]] ||
  fail "vmauth answered $status_without_write for a write with no token"
status_with_token=$(curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $TOKEN" \
  "http://127.0.0.1:$VMAUTH_PORT/api/v1/label/__name__/values")
[[ $status_with_token == 200 ]] ||
  fail "vmauth answered $status_with_token for a read with the token"
echo "e2e: C4: 401 without a token (read and write), 200 with it"

# An absent failure counter and a broken exporter look the same, so the sent
# counter has to prove the pipeline works before zero means anything. The
# agent restarted after the durability half, so its counter starts at zero
# again; wait for the telemetry scrape to see the first batch.
sent=0
sent_deadline=$((SECONDS + 60))
while ((SECONDS < sent_deadline)); do
  sent=$(vm_query 'otelcol_exporter_sent_log_records_total{exporter="otlphttp/victorialogs"}' |
    jq -r '.data.result[0].value[1] // "0"')
  awk -v sent="$sent" 'BEGIN { exit !(sent > 0) }' && break
  sleep 1
done
awk -v sent="$sent" 'BEGIN { exit !(sent > 0) }' ||
  fail "the log exporter reports $sent sent records"
failed=$(vm_query 'otelcol_exporter_send_failed_log_records_total{exporter="otlphttp/victorialogs"}' |
  jq -r '.data.result[0].value[1] // "0"')
awk -v failed="$failed" 'BEGIN { exit !(failed == 0) }' ||
  fail "the log exporter reports $failed failed records"
echo "e2e: C4: the log exporter sent $sent records and failed $failed"

# C5: the host metrics take the same pipeline. The names are read first: the
# OTel-to-Prometheus translation is not guessed.
has_name_prefix() {
  curl -s -H "Authorization: Bearer $TOKEN" \
    "http://127.0.0.1:$VMAUTH_PORT/api/v1/label/__name__/values" |
    jq -e --arg prefix "$1" '.data | map(select(startswith($prefix))) | length > 0' \
      >/dev/null 2>&1
}
hostmetrics_deadline=$((SECONDS + 90))
until has_name_prefix system_network_ && has_name_prefix system_filesystem_; do
  ((SECONDS < hostmetrics_deadline)) ||
    fail "hostmetrics names are not in VictoriaMetrics"
  sleep 1
done
host_names=$(curl -s -H "Authorization: Bearer $TOKEN" \
  "http://127.0.0.1:$VMAUTH_PORT/api/v1/label/__name__/values" |
  jq -r '.data[]' | grep -cE '^system_(network|filesystem|disk|cpu|memory)_' || true)
echo "e2e: C5: $host_names hostmetric names in VictoriaMetrics (network and filesystem present)"

# C7: the resource budget, at 100 lines/s for 15 s. The ticks are the agent's
# own utime+stime; RSS is a snapshot at the end of the load.
tick=$(getconf CLK_TCK)
cpu_ticks() { awk '{print $14 + $15}' "/proc/$1/stat"; }
rss_mib() { awk -v kb="$(ps -o rss= -p "$1" | tr -d ' ')" 'BEGIN { printf "%.1f", kb / 1024 }'; }

agent_pid=${PID[otelcol]}
load_file="$WORK/logs/load-e2e.json"
cpu_before=$(cpu_ticks "$agent_pid")
wall_before=$(date +%s%N)
for second in $(seq 1 15); do
  timestamp=$(date -u +%Y-%m-%dT%H:%M:%S.000000000Z)
  for index in $(seq 1 100); do
    printf '{"ts":"%s","level":"info","component":"tool","node_id":"e2e","event":"backpressure.enter","msg":"load-%s-%s"}\n' \
      "$timestamp" "$second" "$index" >>"$load_file"
  done
  sleep 1
done
cpu_after=$(cpu_ticks "$agent_pid")
wall_after=$(date +%s%N)

elapsed=$(awk -v from="$wall_before" -v to="$wall_after" 'BEGIN { printf "%.3f", (to - from) / 1000000000 }')
cpu_percent=$(awk -v before="$cpu_before" -v after="$cpu_after" \
  -v tick="$tick" -v elapsed="$elapsed" \
  'BEGIN { printf "%.2f", (after - before) / tick / elapsed * 100 }')
agent_rss=$(rss_mib "$agent_pid")
vl_rss=$(rss_mib "${PID[victoria-logs]}")
vm_rss=$(rss_mib "${PID[victoria-metrics]}")
vmauth_rss=$(rss_mib "${PID[vmauth]}")
echo "e2e: C7: agent ${cpu_percent}% of one core, ${agent_rss} MiB; VL ${vl_rss} MiB; VM ${vm_rss} MiB; vmauth ${vmauth_rss} MiB"
awk -v value="$agent_rss" 'BEGIN { exit !(value < 256) }' ||
  fail "the agent's RSS is ${agent_rss} MiB, over the 256 MiB budget"
awk -v value="$cpu_percent" 'BEGIN { exit !(value < 10) }' ||
  fail "the agent used ${cpu_percent}% of a core at 100 lines/s, over the 10% budget"
awk -v value="$vl_rss" 'BEGIN { exit !(value < 512) }' ||
  fail "VictoriaLogs RSS is ${vl_rss} MiB, over the 512 MiB budget"
awk -v value="$vm_rss" 'BEGIN { exit !(value < 512) }' ||
  fail "VictoriaMetrics RSS is ${vm_rss} MiB, over the 512 MiB budget"

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

# Hold mode: everything above passed and the stack is left running for a
# caller that layers on top of it (the Grafana E2E). The ready file names the
# work directory and the ports; SIGTERM ends the hold and runs the normal
# cleanup. Without E2E_HOLD nothing changes.
if [[ -n ${E2E_HOLD:-} ]]; then
  {
    echo "WORK=$WORK"
    echo "VL_PORT=$VL_PORT"
    echo "VM_PORT=$VM_PORT"
    echo "VMAUTH_PORT=$VMAUTH_PORT"
    echo "TELEMETRY_PORT=$TELEMETRY_PORT"
    echo "COUNTERS_PORT=$COUNTERS_PORT"
  } >"$E2E_HOLD.tmp"
  mv "$E2E_HOLD.tmp" "$E2E_HOLD"
  echo "e2e: stack held for $E2E_HOLD (pid $$); SIGTERM to stop"
  hold_stop() { exit 0; }
  trap hold_stop TERM INT
  while true; do sleep 5; done
fi
