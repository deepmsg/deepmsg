# deepmsg observability deployment (L2)

The node agent that reads what the processes write and ships it to
VictoriaLogs and VictoriaMetrics. The design is
`analysis/observability/deepmsg-l2-plan-collector-and-backends.md`; the pipeline
it belongs to is `analysis/observability/plan-2026-11-05-observability-pipeline.md`.

The applications never know this directory exists: they write local JSON files
and a Prometheus endpoint, and turning collection off degrades queries to
`grep`. That contract is `crates/log/README.md`.

## Status

**Configuration, the process harness, the log path, the metrics path,
durability, authentication and the resource budget.** The agent config, its
static checks, the launcher and the log, metrics, restart, authentication, host
metric and resource assertions are here. What remains for L2 is the systemd and
Kubernetes form (the agents and backends are started by the script, not by a
unit). The launcher proves the four processes start, listen and stay alive,
that the driver runs with no collector at all (empty stderr, lines on disk),
that its log file reaches VictoriaLogs with the expected fields and stream,
that the counters exporter's scrape (the collector's own metrics included)
reaches VictoriaMetrics, that lines written while the collector is down — a
rotation it did not see included — all arrive after the restart with no line
replayed, that vmauth refuses both reads and writes without a token, and that
the resource budget holds at 100 lines/s. Nothing under `systemd/` or `k8s/`
has been run on a machine yet, and a static review is not a run.

## Layout

```
agent/otelcol.yaml              the one config, used by systemd and by the DaemonSet
e2e/check-config.py             the properties that fail silently if wrong
e2e/versions.env                the pinned tags, asset names and SHA256
e2e/fetch-tools.sh              download + verify into target/observability-tools
e2e/run.sh                      start the four processes and prove they are alive
```

The systemd units, the k8s manifests and the backend configs are added by the
later commits.

## End-to-end

```
./deploy/observability/e2e/run.sh
```

downloads the pinned releases on first use (about 290 MB, cached under
`target/observability-tools/`), verifies each against `versions.env`, then
starts VictoriaLogs, VictoriaMetrics, vmauth and `otelcol-contrib` on loopback.
Every file and every port is under the script's control; the four ports must be
free before it starts.

The run builds `deepmsg-driver` (debug) and uses it as the application: it runs
first with **no collector** and must keep its stderr empty and both lifecycle
lines on disk, then the agent starts and ships that file to VictoriaLogs. The
assertions are the log path's contract: `driver.start` arrives, `_msg` is the
event's name (or its `msg` when it has one, checked with a synthetic line), and
`_stream` is exactly `component` and `node_id`.

Then a second driver stays up with `deepmsg-counters-exporter` beside it, and
the script asks VictoriaMetrics for `deepmsg_driver_up`, the heartbeat age (it
must be under ten seconds — a live driver), and `deepmsg_error_total`. The
collector's own metrics take the same pipeline: the telemetry level is
`normal`, because `otelcol_exporter_*` does not exist at `basic`, and the
script requires at least one of those names in VictoriaMetrics.

vmauth's `src_paths` are **exact paths** — no prefix and no glob — so the
generated auth config names every path the pipeline and a Grafana datasource
use. A missing one is a 400 `missing route`, not a 404.

The durability half stops the collector, runs the driver again (must still have
empty stderr), appends five lines, then performs the writer's own rotation —
the base is renamed to `.1.json` and a new base appears — and appends five
more. After the restart all ten are in VictoriaLogs and no earlier line is
replayed. The measurement also settles `start_at`: it governs the **first**
start only (`beginning` reads a 3-line backlog, `end` reads 0); once a state
exists, offsets govern and a rotation during downtime is read from the
beginning under either value.

The last third checks the door and the cost. vmauth must answer 401 to a read
and a write with no token and 200 with it; the log exporter's sent counter must
be non-zero before its failure counter's zero means anything. The host metric
names are read from VictoriaMetrics rather than assumed — the OTel-to-
Prometheus translation is part of what is under test. Finally the script writes
100 lines/s for 15 s and checks the budget: the agent under 256 MiB and 10% of
a core (measured: about 226 MiB, 0.4%), the two backends under 512 MiB each
(measured: VL 26 MiB, VM 101 MiB).

| override | default | meaning |
|---|---|---|
| `E2E_VL_PORT` | `9428` | VictoriaLogs |
| `E2E_VM_PORT` | `8428` | VictoriaMetrics |
| `E2E_VMAUTH_PORT` | `8427` | vmauth |
| `E2E_COUNTERS_PORT` | `9101` | the counters exporter |
| `E2E_OBS_TOKEN` | `e2e-token` | the bearer token the agent and the script use |
| `DEEPMSG_OBS_E2E_DIR` | `target/observability-e2e` | where a run's state and evidence go |
| `E2E_KEEP` | `0` | keep the run directory even on success |

A failed run keeps its directory (process logs, `ss -ltn`, process table) and
prints the path. The collector's telemetry port is fixed at `8888` because the
checked-in config names it; a second value would be a second config.

## What it reads

| Source | Where it comes from |
|---|---|
| application logs | `$DEEPMSG_LOG_DIR/*.json` (the writer keeps `<component>-<node>[.N].json`) |
| driver counters | `deepmsg-counters-exporter` on `127.0.0.1:9101/metrics` |
| host metrics | the collector's `hostmetrics` receiver (CPU, memory, disk, filesystem, network) |
| the collector itself | its own `:8888` telemetry, scraped back into the same pipeline |

## Environment

The config is entirely environment-driven so that the same file serves a
laptop, a systemd unit and a DaemonSet. All of these are required; the collector
refuses to start if one is unset.

| variable | meaning | example |
|---|---|---|
| `DEEPMSG_LOG_DIR` | the directory the applications write into | `/var/log/deepmsg` |
| `DEEPMSG_OBS_STATE_DIR` | the collector's durable state (file offsets, queues) | `/var/lib/deepmsg-otelcol` |
| `DEEPMSG_COUNTERS_ENDPOINT` | the counters exporter's host:port | `127.0.0.1:9101` |
| `DEEPMSG_OTELCOL_TELEMETRY_ENDPOINT` | the collector's own Prometheus endpoint | `127.0.0.1:8888` |
| `DEEPMSG_VL_LOGS_ENDPOINT` | VictoriaLogs' OTLP/HTTP logs URL (through vmauth in the cluster profile) | `http://127.0.0.1:8427/insert/opentelemetry/v1/logs` |
| `DEEPMSG_VM_WRITE_ENDPOINT` | VictoriaMetrics' remote-write URL (through vmauth) | `http://127.0.0.1:8427/api/v1/write` |
| `DEEPMSG_OBS_WRITE_TOKEN` | the bearer token vmauth accepts for writes | (secret) |

`DEEPMSG_NODE_ID` is not read by the agent: every line already carries the
`node_id` its writer was configured with, and rewriting it here would hide a
node that was started with the wrong one. The applications still need it.

`DEEPMSG_OBS_STATE_DIR` must not be inside `DEEPMSG_LOG_DIR`: the log writer
rotates and deletes files there, and the state is the collector's memory of
what it has read.

## Profiles

- **single** — the collector is off. The applications write local files and
  nothing reads them; `grep` and `jq` are the query language.
- **cluster** — the collector runs. Production keeps the backends on a
  dedicated log host, never on a media-driver host; the E2E runs both on one
  machine and says so in its evidence.

## Checks

```
python3 e2e/check-config.py
```

asserts the properties whose absence is silent: the stream fields, the Go time
layout, the severity source, the three `file_storage` hooks, the pipelines, the
collector's own scrape job, and that no literal credential is committed. The
same three contract lines are pinned in `tests/integration/observability_config.rs`
so they run in CI without Python or the binaries.
