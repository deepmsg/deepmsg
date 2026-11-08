# deepmsg observability runbook

The three dashboards, the six provisioned alerts, and the two drills. The
Grafana side is `deploy/observability/grafana/`; the stack it reads is the L2
slice in `deploy/observability/`.

Every check below goes through vmauth with the **read** token
(`DEEPMSG_GRAFANA_READ_TOKEN`); the agent's write token is not a query key.

## The alerts

| anchor | severity | expression | for |
|---|---|---|---|
| [`driver-heartbeat-stale`](#driver-heartbeat-stale) | P1 | `deepmsg_driver_heartbeat_age_seconds > 3` | 5s |
| [`driver-metrics-absent`](#driver-metrics-absent) | P1 | `absent(deepmsg_driver_heartbeat_age_seconds) or (up{job="deepmsg-counters"} == bool 0)` | 30s |
| [`cluster-role-jitter`](#cluster-role-jitter) | P1 | LogsQL `event:"role.change" \| stats by (node_id) count() as changes`, `>= 3` | 0s |
| [`loss-rate`](#loss-rate) | P2 | `rate(deepmsg_loss_total[5m]) > 0` | 5m |
| [`disk-watermark`](#disk-watermark) | P2 | `system_filesystem_utilization_ratio > 0.8` | 5m |
| [`log-export-dropped`](#log-export-dropped) | P3 | `otelcol_exporter_queue_size >= bool 10000` | 1m |
| [`commit-lag`](#commit-lag) | — | **not provisioned**: no append-position counter | — |

Both checks below are the same ones the E2E uses:

```sh
# metrics (PromQL through vmauth)
curl -s -H "Authorization: Bearer $DEEPMSG_GRAFANA_READ_TOKEN" \
  'http://127.0.0.1:8427/api/v1/query?query=deepmsg_driver_heartbeat_age_seconds'
# logs (LogsQL through vmauth)
curl -s -H "Authorization: Bearer $DEEPMSG_GRAFANA_READ_TOKEN" \
  'http://127.0.0.1:8427/select/logsql/query' \
  --data-urlencode 'query=event:"role.change"' --data-urlencode 'limit=20'
```

The three dashboards are `deepmsg-cluster-health`, `deepmsg-transport` and
`deepmsg-archive` in the `deepmsg` folder.

<a id="driver-heartbeat-stale"></a>
## Driver heartbeat stale (P1)

**What it means:** the CnC's consumer heartbeat has not moved for over three
seconds. The driver may be gone, wedged, or unable to run its conductor.

**First checks:**

```sh
systemctl status deepmsg-driver            # or the process the supervisor owns
ps -o pid,stat,etime,cmd -C deepmsg-driver
curl -s -H "Authorization: Bearer $DEEPMSG_GRAFANA_READ_TOKEN" \
  'http://127.0.0.1:8427/api/v1/query?query=deepmsg_driver_up'
```

The driver's own stderr is the place to look next; it is empty on the happy
path, so anything there is real.

**Action:** restart the driver the way the deployment starts it. The next start
reclaims a dead directory and saves the old error log beside it
(`<aeron_dir>-<datestamp>-error.log`), which the error-log exporter picks up.
Check the `driver.error` events in VictoriaLogs for the reason.

**Rollback:** none — a driver restart is the action.

<a id="driver-metrics-absent"></a>
## Driver metrics gone (P1)

**What it means:** either the counters exporter (or its scrape) is gone, or the
whole node's agent stopped shipping.

**First checks:**

```sh
systemctl status deepmsg-counters-exporter deepmsg-log-agent
curl -s -H "Authorization: Bearer $DEEPMSG_GRAFANA_READ_TOKEN" \
  'http://127.0.0.1:8427/api/v1/query?query=up{job="deepmsg-counters"}'
ss -ltn | grep 9101
```

`up == 0` names the scrape target; `absent()` means the series left the store
entirely (the agent stopped shipping or the node is gone).

**Action:** restart the exporter or the agent. The exporter is a read-only
process: a restart costs nothing and does not disturb the driver.

<a id="cluster-role-jitter"></a>
## Cluster role jitter (P1)

**What it means:** three or more `role.change` log events in five minutes. The
cluster is re-electing repeatedly.

**First checks:**

```sh
curl -s -H "Authorization: Bearer $DEEPMSG_GRAFANA_READ_TOKEN" \
  'http://127.0.0.1:8427/select/logsql/query' \
  --data-urlencode 'query=event:"role.change"' --data-urlencode 'limit=20'
# the term and the leader view
curl -s -H "Authorization: Bearer $DEEPMSG_GRAFANA_READ_TOKEN" \
  'http://127.0.0.1:8427/api/v1/query?query=deepmsg_cluster_term'
```

**Action:** look for the election's reason in the `election.complete` events and
in the members' stderr; check the network between the members and the
`aeron.image.liveness.timeout` setting. Do not restart a member while the
others are mid-election unless the cluster has no leader.

<a id="loss-rate"></a>
## Loss rate above zero (P2)

**What it means:** the driver is filling loss gaps — packets are being lost
between a sender and a receiver.

**First checks:** the transport dashboard's `rate(deepmsg_loss_total[5m])` and
NAK rate; the host's `system_network_dropped` / `system_network_errors` from
hostmetrics; the `loss-report.dat` per stream (the counters exporter reads it).

**Action:** the network path first (switch/NIC drops, MTU), then the receiver's
window (`deepmsg_receiver_window`).

<a id="disk-watermark"></a>
## Disk above eighty percent (P2)

**What it means:** a filesystem on a deepmsg host is over 80% used.

**First checks:**

```sh
df -h /var/lib/victoria-logs /var/lib/victoria-metrics /var/log/deepmsg
```

**Action:** the retention caps are the first lever
(`-retentionPeriod`, `-retention.maxDiskUsagePercent` on VictoriaLogs); then
old rotated logs and segments. Never delete the current segment or a recording
an archive is still writing.

<a id="log-export-dropped"></a>
## Log export queue at the cap (P3)

**What it means:** the collector's persistent queue is at its cap, so new log
records are being dropped. The collector (v0.164.0) exposes no send-failure
counter — `otelcol_exporter_send_failed_*` does not exist — so the queue size
is the only signal.

**First checks:** the collector's own metric in VictoriaMetrics
(`otelcol_exporter_queue_size`), then vmauth and the backends:

```sh
curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:8427/select/logsql/query
systemctl status vmauth victoria-logs
```

A `400 missing route` means vmauth's exact-path table is missing the path the
exporter or Grafana calls; `401` means the token; connection refused means the
backend.

<a id="commit-lag"></a>
## Not provisioned: cluster commit lag (P2)

The pipeline plan's rule four (`append - commit`) cannot be evaluated: the
exporter has the commit position (type 203) and **no append position**. A
dashboard panel says so; this rule is deferred until a counter exists. The
workaround is the log recording's rec-pos, which is not a counter and is not
exported.

## Drills

<a id="drill-collector-off"></a>
### Drill: the collector is off

The applications must not notice. This is exactly what the L2 E2E's C3/C3b
exercises (see `deploy/observability/README.md`); the manual form is:

```sh
systemctl stop deepmsg-log-agent
# run the application normally for a while: a driver start/stop, an error, a
# client round trip. Its stderr must stay empty.
systemctl start deepmsg-log-agent
# ask VictoriaLogs for the lines written while it was down
curl -s -H "Authorization: Bearer $DEEPMSG_GRAFANA_READ_TOKEN" \
  'http://127.0.0.1:8427/select/logsql/query' \
  --data-urlencode 'query=event:"driver.start"' --data-urlencode 'limit=10'
```

Expected: the driver's stderr is empty, its file keeps growing, and after the
restart every line is in VictoriaLogs exactly once. A rotation that happened
while the agent was down is covered: the renamed file resumes and the new base
is read from its start (measured; `start_at` only governs the first start).

<a id="drill-swap-backend"></a>
### Drill: swap the log backend

Move the log traffic to a second VictoriaLogs without touching the
applications, then move it back. The scripted form is
`deploy/observability/grafana/e2e/swap-backend.sh` (it runs the L2 stack in
hold mode and a second VictoriaLogs on 9528). The manual steps:

1. Start the second backend:
   `victoria-logs -storageDataPath=/var/lib/victoria-logs-2 -httpListenAddr=127.0.0.1:9528`
2. Copy `/etc/deepmsg/vmauth.yaml`, change the **VictoriaLogs** route's
   `url_prefix` to the new backend, and restart vmauth. The agent keeps
   pointing at vmauth, so it does not change.
3. Write a known line and verify it appears in the new backend and **not** in
   the old one.
4. Roll back: restore the file and restart vmauth; a new line must appear in
   the old backend again.

Expected: the swap costs no application restart; in-flight records are retried
from the collector's persistent queue; the old backend stops growing and keeps
its retention.
