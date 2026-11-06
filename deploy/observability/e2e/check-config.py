#!/usr/bin/env python3
"""The agent config's semantic contract.

The collector validates its own YAML at startup, but a wrong stream-field list,
a strptime layout where a Go layout belongs, or a missing `storage:` does not
fail: it silently produces the wrong stream, the wrong timestamp, or lost lines
on restart. This script reads the checked-in config and asserts the properties
that the design says must hold.

It runs with the end-to-end scripts, not in cargo CI (CI has no Python
guarantee); the three line-level pins that matter most are also in
`tests/integration/observability_config.rs`.

Run: python3 deploy/observability/e2e/check-config.py
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import yaml

HERE = Path(__file__).resolve().parent
CONFIG = HERE.parent / "agent" / "otelcol.yaml"
README = HERE.parent / "README.md"

GO_LAYOUT = "2006-01-02T15:04:05.999999999Z07:00"

failures: list[str] = []
checks = 0


def check(label: str, actual: object, expected: object) -> None:
    global checks
    checks += 1
    if actual != expected:
        failures.append(f"{label}: expected {expected!r}, found {actual!r}")


def mapping(value: object, label: str) -> dict:
    if not isinstance(value, dict):
        failures.append(f"{label}: expected a mapping, found {type(value).__name__}")
        return {}
    return value


def sequence(value: object, label: str) -> list:
    if not isinstance(value, list):
        failures.append(f"{label}: expected a list, found {type(value).__name__}")
        return []
    return value


def main() -> int:
    try:
        text = CONFIG.read_text()
        config = yaml.safe_load(text)
    except FileNotFoundError:
        print(f"missing {CONFIG}", file=sys.stderr)
        return 1
    except yaml.YAMLError as error:
        print(f"{CONFIG} is not valid YAML: {error}", file=sys.stderr)
        return 1

    if not isinstance(config, dict):
        print(f"{CONFIG} is not a mapping", file=sys.stderr)
        return 1

    receivers = mapping(config.get("receivers"), "receivers")
    exporters = mapping(config.get("exporters"), "exporters")
    extensions = mapping(config.get("extensions"), "extensions")
    service = mapping(config.get("service"), "service")

    # The three storage hooks: the extension, the reader, the two writers.
    storage = mapping(extensions.get("file_storage"), "extensions.file_storage")
    check(
        "extensions.file_storage.directory is environment-driven",
        "${env:" in str(storage.get("directory", "")),
        True,
    )
    check("extensions.file_storage.create_directory", storage.get("create_directory"), True)
    check(
        "service.extensions names file_storage",
        "file_storage" in sequence(service.get("extensions"), "service.extensions"),
        True,
    )

    filelog = mapping(receivers.get("filelog"), "receivers.filelog")
    check("filelog.storage", filelog.get("storage"), "file_storage")
    # beginning: the first start reads the backlog the applications wrote.
    # Measured: end skips the same three pre-existing lines (0 vs 3).
    check("filelog.start_at", filelog.get("start_at"), "beginning")
    include = sequence(filelog.get("include"), "filelog.include")
    check("filelog.include is one glob under the log directory", include, ["${env:DEEPMSG_LOG_DIR}/*.json"])

    operators = [
        mapping(operator, "filelog.operators[]")
        for operator in sequence(filelog.get("operators"), "filelog.operators")
    ]
    parsers = [operator for operator in operators if operator.get("type") == "json_parser"]
    check("exactly one json_parser", len(parsers), 1)
    if parsers:
        parser = parsers[0]
        check("json_parser.parse_from", parser.get("parse_from"), "body")
        timestamp = mapping(parser.get("timestamp"), "json_parser.timestamp")
        check("json_parser.timestamp.parse_from", timestamp.get("parse_from"), "attributes.ts")
        check("json_parser.timestamp.layout_type", timestamp.get("layout_type"), "gotime")
        check("json_parser.timestamp.layout", timestamp.get("layout"), GO_LAYOUT)
        severity = mapping(parser.get("severity"), "json_parser.severity")
        check("json_parser.severity.parse_from", severity.get("parse_from"), "attributes.level")
    moves = [
        operator
        for operator in operators
        if operator.get("type") == "move"
        and operator.get("from") == "attributes.msg"
        and operator.get("to") == "body"
    ]
    check("exactly one move from attributes.msg to body", len(moves), 1)
    copies = [
        operator
        for operator in operators
        if operator.get("type") == "copy"
        and operator.get("from") == "attributes.event"
        and operator.get("to") == "body"
    ]
    check("exactly one copy from attributes.event to body", len(copies), 1)
    # The order is the contract: the copy must not overwrite a message, so it
    # runs on the events that have none. Both conditions are asserted because
    # swapping them is a silent bug (every line would say its event name).
    if copies and moves:
        check("copy runs only when msg is absent", copies[0].get("if"), "attributes.msg == nil")
        check("move runs only when msg is present", moves[0].get("if"), "attributes.msg != nil")
        check(
            "copy comes before move",
            operators.index(copies[0]) < operators.index(moves[0]),
            True,
        )

    # The host metrics and the collector's own metrics.
    hostmetrics = mapping(receivers.get("hostmetrics"), "receivers.hostmetrics")
    check(
        "hostmetrics.scrapers",
        sorted(mapping(hostmetrics.get("scrapers"), "hostmetrics.scrapers").keys()),
        ["cpu", "disk", "filesystem", "memory", "network"],
    )
    prometheus = mapping(receivers.get("prometheus"), "receivers.prometheus")
    prometheus_config = mapping(prometheus.get("config"), "receivers.prometheus.config")
    scrape_configs = sequence(
        prometheus_config.get("scrape_configs"), "receivers.prometheus.config.scrape_configs"
    )
    jobs = {
        mapping(job, "scrape_configs[]").get("job_name"): mapping(job, "scrape_configs[]")
        for job in scrape_configs
    }
    for job_name, variable in (
        ("deepmsg-counters", "DEEPMSG_COUNTERS_ENDPOINT"),
        ("deepmsg-otelcol", "DEEPMSG_OTELCOL_TELEMETRY_ENDPOINT"),
    ):
        job = mapping(jobs.get(job_name), f"scrape job {job_name}")
        static = sequence(job.get("static_configs"), f"scrape job {job_name}.static_configs")
        targets = sequence(
            mapping(static[0], "static_configs[0]").get("targets") if static else None,
            f"scrape job {job_name}.targets",
        )
        check(f"scrape job {job_name} targets", targets, [f"${{env:{variable}}}"])

    # The log exporter's stream fields, and the severity source.
    victorialogs = mapping(exporters.get("otlphttp/victorialogs"), "exporters.otlphttp/victorialogs")
    headers = mapping(victorialogs.get("headers"), "exporters.otlphttp/victorialogs.headers")
    check("VL-Stream-Fields", headers.get("VL-Stream-Fields"), "component,node_id")

    for name in ("otlphttp/victorialogs", "prometheusremotewrite/victoriametrics"):
        exporter = mapping(exporters.get(name), f"exporters.{name}")
        queue = mapping(exporter.get("sending_queue"), f"exporters.{name}.sending_queue")
        check(f"{name}.sending_queue.enabled", queue.get("enabled"), True)
        check(f"{name}.sending_queue.storage", queue.get("storage"), "file_storage")
        authorization = str(mapping(exporter.get("headers"), f"exporters.{name}.headers").get("Authorization", ""))
        check(f"{name}.Authorization is a token placeholder", authorization.startswith("Bearer ${env:"), True)

    # The collector's own telemetry, and the pipelines that carry everything.
    telemetry = mapping(service.get("telemetry"), "service.telemetry")
    metrics = mapping(telemetry.get("metrics"), "service.telemetry.metrics")
    # Component-level otelcol_exporter_* metrics exist only at normal and above;
    # basic is silent about the exporter, which is the P3 alert's source.
    check("collector telemetry level", metrics.get("level"), "normal")
    readers = sequence(metrics.get("readers"), "service.telemetry.metrics.readers")
    pull = mapping(mapping(readers[0], "readers[0]").get("pull"), "readers[0].pull") if readers else {}
    exporter = mapping(pull.get("exporter"), "readers[0].pull.exporter")
    prometheus_reader = mapping(exporter.get("prometheus"), "readers[0].pull.exporter.prometheus")
    check("collector telemetry host", prometheus_reader.get("host"), "127.0.0.1")
    check("collector telemetry port", prometheus_reader.get("port"), 8888)

    pipelines = mapping(service.get("pipelines"), "service.pipelines")
    logs = mapping(pipelines.get("logs"), "service.pipelines.logs")
    check("logs.receivers", logs.get("receivers"), ["filelog"])
    check("logs.processors", logs.get("processors"), ["memory_limiter", "batch"])
    check("logs.exporters", logs.get("exporters"), ["otlphttp/victorialogs"])
    metrics_pipeline = mapping(pipelines.get("metrics"), "service.pipelines.metrics")
    check("metrics.receivers", metrics_pipeline.get("receivers"), ["prometheus", "hostmetrics"])
    check("metrics.processors", metrics_pipeline.get("processors"), ["memory_limiter", "batch"])
    check(
        "metrics.exporters",
        metrics_pipeline.get("exporters"),
        ["prometheusremotewrite/victoriametrics"],
    )
    limiter = mapping(
        mapping(config.get("processors"), "processors").get("memory_limiter"),
        "processors.memory_limiter",
    )
    check("processors.memory_limiter.limit_mib", limiter.get("limit_mib"), 256)

    # Every variable the config needs is documented where an operator looks.
    try:
        readme = README.read_text()
    except FileNotFoundError:
        failures.append(f"missing {README}")
        readme = ""
    referenced = sorted(set(re.findall(r"\$\{env:([A-Z0-9_]+)\}", text)))
    for name in referenced:
        check(f"{name} is documented in README.md", name in readme, True)

    if failures:
        print(f"{CONFIG.relative_to(CONFIG.parents[2])}: {len(failures)} check(s) failed", file=sys.stderr)
        for failure in failures:
            print(f"  - {failure}", file=sys.stderr)
        return 1

    print(f"{CONFIG.relative_to(CONFIG.parents[2])}: {checks} checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
