# deepmsg observability deployment (L2)

The node agent that reads what the processes write and ships it to
VictoriaLogs and VictoriaMetrics. The design is
`analysis/observability/deepmsg-l2-plan-collector-and-backends.md`; the pipeline
it belongs to is `analysis/observability/plan-2026-11-05-observability-pipeline.md`.

The applications never know this directory exists: they write local JSON files
and a Prometheus endpoint, and turning collection off degrades queries to
`grep`. That contract is `crates/log/README.md`.

## Status

**Configuration only.** The agent config and its static checks are here; the
end-to-end scripts arrive in the later commits of the L2 slice. Nothing under
`systemd/` or `k8s/` has been run on a machine yet — the one form that is
exercised by the E2E is "the same executables with the same flags, started by
the script". A static review is not a run.

## Layout

```
agent/otelcol.yaml              the one config, used by systemd and by the DaemonSet
e2e/check-config.py             the properties that fail silently if wrong
```

The systemd units, the k8s manifests, the backend configs and the E2E scripts
are added by the later commits.

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
