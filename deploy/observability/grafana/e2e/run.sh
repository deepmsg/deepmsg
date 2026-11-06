#!/usr/bin/env bash
# The Grafana end-to-end: the L2 stack held open, Grafana provisioned on top of
# it, and the datasources, dashboards and alert rules asked through Grafana's
# own API.
#
# The stack is the L2 E2E in hold mode (`E2E_HOLD`), so this script never
# duplicates the launch: it waits for the ready file, reads the ports, and
# SIGTERMs the holder at the end so its cleanup stops the four processes.
set -euo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO=$(cd "$HERE/../../../.." && pwd)
# shellcheck source=versions.env
. "$HERE/versions.env"

CACHE=${DEEPMSG_OBS_TOOLS:-$REPO/target/observability-tools}
GF_HOME=$CACHE/grafana
GF_PLUGINS=$CACHE/grafana-plugins
PROVISIONING=$REPO/deploy/observability/grafana/provisioning
GRAFANA_INI=$REPO/deploy/observability/grafana/grafana.ini

GRAFANA_PORT=${E2E_GRAFANA_PORT:-3000}
ADMIN_PASSWORD=${E2E_GRAFANA_ADMIN_PASSWORD:-e2e-admin}
TOKEN=${E2E_OBS_TOKEN:-e2e-token}
WEBHOOK_PORT=${E2E_WEBHOOK_PORT:-9099}

ROOT=${DEEPMSG_OBS_E2E_DIR:-$REPO/target/observability-e2e}
WORK=$ROOT/grafana-$(date +%Y%m%d-%H%M%S)-$$
KEEP=${E2E_KEEP:-0}
STATUS=started
STACK_PID=""
GF_PID=""
STACK_WORK=""

fail() { echo "grafana-e2e: $*" >&2; exit 1; }

dump() {
  [[ -d ${WORK:-} ]] || return 0
  echo "grafana-e2e: evidence in $WORK" >&2
  for file in "$WORK/grafana.log" "$WORK/stack.log" "$WORK/stack-ready"; do
    [[ -f $file ]] && { echo "--- $file (last 60) ---" >&2; tail -n 60 "$file" >&2; }
  done
  ss -ltn >&2 || true
}

cleanup() {
  local code=$?
  if [[ -n $GF_PID ]]; then
    # SIGTERM, then a bounded wait: Grafana shuts down gracefully but the
    # script must not remove its data directory while it is still running.
    kill "$GF_PID" 2>/dev/null || true
    for _ in $(seq 1 20); do
      kill -0 "$GF_PID" 2>/dev/null || break
      sleep 0.5
    done
    kill -9 "$GF_PID" 2>/dev/null || true
    wait "$GF_PID" 2>/dev/null || true
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
for tool in curl ss python3 jq; do need "$tool"; done

if ss -ltnH | awk '{print $4}' | grep -qE "[:.]$GRAFANA_PORT\$"; then
  fail "port $GRAFANA_PORT is already listening"
fi

mkdir -p "$WORK"

"$HERE/fetch-grafana.sh"

# The L2 stack, held open for the rest of this script.
READY=$WORK/stack-ready
echo "grafana-e2e: starting the L2 stack in hold mode"
E2E_HOLD=$READY E2E_KEEP=1 E2E_OBS_TOKEN=$TOKEN \
  "$REPO/deploy/observability/e2e/run.sh" >"$WORK/stack.log" 2>&1 &
STACK_PID=$!
stack_deadline=$((SECONDS + 900))
while [[ ! -f $READY ]]; do
  kill -0 "$STACK_PID" 2>/dev/null || fail "the stack E2E exited before it held"
  ((SECONDS < stack_deadline)) || fail "the stack did not hold within 900 s"
  sleep 2
done
STACK_WORK=$(sed -n 's/^WORK=//p' "$READY")
VL_PORT=$(sed -n 's/^VL_PORT=//p' "$READY")
VM_PORT=$(sed -n 's/^VM_PORT=//p' "$READY")
VMAUTH_PORT=$(sed -n 's/^VMAUTH_PORT=//p' "$READY")
echo "grafana-e2e: the stack is up (work $STACK_WORK)"

# Grafana, with a fresh data directory so GF_SECURITY_ADMIN_PASSWORD is read.
export GF_SECURITY_ADMIN_PASSWORD=$ADMIN_PASSWORD
export GF_PATHS_DATA=$WORK/grafana-data
export GF_PATHS_LOGS=$WORK/grafana-logs
export GF_PATHS_PLUGINS=$GF_PLUGINS
export GF_PATHS_PROVISIONING=$PROVISIONING
export DEEPMSG_GRAFANA_READ_TOKEN=$TOKEN
export DEEPMSG_GRAFANA_DASHBOARDS_DIR=$PROVISIONING/dashboards
export DEEPMSG_GRAFANA_WEBHOOK_URL="http://127.0.0.1:$WEBHOOK_PORT/alerts"
mkdir -p "$GF_PATHS_DATA" "$GF_PATHS_LOGS"

"$GF_HOME/bin/grafana" server --homepath "$GF_HOME" --config "$GRAFANA_INI" \
  >"$WORK/grafana.log" 2>&1 &
GF_PID=$!
echo "grafana-e2e: started Grafana (pid $GF_PID)"

wait_http() {
  local url=$1 timeout=${2:-120}
  local deadline=$((SECONDS + timeout))
  while ((SECONDS < deadline)); do
    kill -0 "$GF_PID" 2>/dev/null || fail "Grafana exited early"
    curl -sf -o /dev/null "$url" && return 0
    sleep 0.5
  done
  fail "Grafana did not answer $url within ${timeout}s"
}
wait_http "http://127.0.0.1:$GRAFANA_PORT/api/health" 120

AUTH=(-u "admin:$ADMIN_PASSWORD")
grafana_api() { curl -sf "${AUTH[@]}" "http://127.0.0.1:$GRAFANA_PORT$1"; }

# C1: the two datasources exist, are healthy, and answer a query through
# Grafana's own query path (which is what carries the provisioned header).
datasources=$(grafana_api /api/datasources)
printf '%s' "$datasources" | jq -e 'map(select(.uid == "victoriametrics" or .uid == "victorialogs")) | length == 2' >/dev/null ||
  fail "the two datasources are not provisioned"
for uid in victoriametrics victorialogs; do
  health=$(grafana_api "/api/datasources/uid/$uid/health")
  printf '%s' "$health" | jq -e '.status == "OK"' >/dev/null ||
    fail "datasource $uid is not healthy: $health"
done
echo "grafana-e2e: C1: both datasources are healthy"

query() { # body
  curl -sf "${AUTH[@]}" -H 'Content-Type: application/json' \
    -X POST "http://127.0.0.1:$GRAFANA_PORT/api/ds/query" -d "$1"
}
prom_body=$(cat <<JSON
{"queries":[{"refId":"A","datasource":{"type":"prometheus","uid":"victoriametrics"},"expr":"deepmsg_driver_up{node_id=\"e2e\"}","instant":true,"range":false,"intervalMs":1000,"maxDataPoints":100}],"from":"now-5m","to":"now"}
JSON
)
prom_result=$(query "$prom_body")
printf '%s' "$prom_result" | jq -e '.results.A.error == null and (.results.A.frames | length > 0)' >/dev/null ||
  fail "the PromQL datasource returned no frame: $prom_result"
logs_body=$(cat <<JSON
{"queries":[{"refId":"A","datasource":{"type":"victoriametrics-logs-datasource","uid":"victorialogs"},"expr":"*","queryType":"instant","maxLines":10}],"from":"now-5m","to":"now"}
JSON
)
logs_result=$(query "$logs_body")
printf '%s' "$logs_result" | jq -e '.results.A.error == null and (.results.A.frames | length > 0)' >/dev/null ||
  fail "the LogsQL datasource returned no frame: $logs_result"
echo "grafana-e2e: C1: both datasources answer through Grafana"

# C2: the three dashboards are provisioned.
for entry in "deepmsg-cluster-health:deepmsg cluster health" \
  "deepmsg-transport:deepmsg transport" "deepmsg-archive:deepmsg archive"; do
  uid=${entry%%:*}
  title=${entry#*:}
  dashboard=$(grafana_api "/api/dashboards/uid/$uid")
  printf '%s' "$dashboard" | jq -e --arg title "$title" '.dashboard.title == $title' >/dev/null ||
    fail "dashboard $uid is not provisioned as '$title'"
done
echo "grafana-e2e: C2: the three dashboards are provisioned"
# A panel's expression, through the dashboard's datasource, has data.
panel_body=$(cat <<JSON
{"queries":[{"refId":"A","datasource":{"type":"prometheus","uid":"victoriametrics"},"expr":"deepmsg_driver_heartbeat_age_seconds{node_id=\"e2e\"}","instant":true,"range":false,"intervalMs":1000,"maxDataPoints":100}],"from":"now-5m","to":"now"}
JSON
)
panel_result=$(query "$panel_body")
printf '%s' "$panel_result" | jq -e '.results.A.error == null and (.results.A.frames | length > 0)' >/dev/null ||
  fail "the cluster-health panel has no data: $panel_result"
echo "grafana-e2e: C2: a dashboard panel query has data"

# C3: the six rules, the contact point and the policy.
rules=$(grafana_api /api/v1/provisioning/alert-rules)
printf '%s' "$rules" | jq -e 'length == 6' >/dev/null || fail "expected six alert rules: $rules"
for uid in driver-heartbeat-stale driver-metrics-absent cluster-role-jitter \
  loss-rate disk-watermark log-export-dropped; do
  printf '%s' "$rules" | jq -e --arg uid "$uid" 'map(select(.uid == $uid)) | length == 1' >/dev/null ||
    fail "alert rule $uid is not provisioned"
done
contacts=$(grafana_api /api/v1/provisioning/contact-points)
printf '%s' "$contacts" | jq -e 'map(select(.name == "deepmsg-webhook")) | length == 1' >/dev/null ||
  fail "the webhook contact point is not provisioned"
policy=$(grafana_api /api/v1/provisioning/policies)
printf '%s' "$policy" | jq -e '.receiver == "deepmsg-webhook"' >/dev/null ||
  fail "the root policy does not route to the webhook"
echo "grafana-e2e: C3: six rules, the contact point and the policy are provisioned"

echo "grafana-e2e: C1–C3 green (Grafana $("$GF_HOME/bin/grafana" --version 2>&1 | head -n 1))"
STATUS=ok
