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

import configparser
import json
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


GRAFANA = HERE.parent / "grafana"


def grafana_checks() -> None:
    """The Grafana side of the deployment: the same silent-failure discipline."""
    ini_path = GRAFANA / "grafana.ini"
    ini = configparser.ConfigParser()
    try:
        ini.read(ini_path)
    except (FileNotFoundError, configparser.Error) as error:
        failures.append(f"cannot read {ini_path}: {error}")
        return
    check("grafana server address", ini.get("server", "http_addr", fallback=None), "127.0.0.1")
    check("grafana server port", ini.get("server", "http_port", fallback=None), "3000")
    check("grafana anonymous access", ini.get("auth.anonymous", "enabled", fallback=None), "false")
    check(
        "grafana admin password is not in the file",
        ini.has_option("security", "admin_password"),
        False,
    )
    check(
        "grafana provisioning path",
        ini.get("paths", "provisioning", fallback="").startswith("/etc/deepmsg/"),
        True,
    )
    unit = HERE.parent / "systemd" / "grafana.service"
    check("grafana unit exists", unit.is_file(), True)
    if unit.is_file():
        unit_text = unit.read_text()
        check(
            "grafana unit uses the environment file",
            "EnvironmentFile=/etc/deepmsg/observability.env" in unit_text,
            True,
        )
        check(
            "grafana unit runs the pinned binary",
            "/usr/local/bin/grafana server --homepath /usr/share/grafana" in unit_text,
            True,
        )

    datasource_dir = GRAFANA / "provisioning" / "datasources"
    files = sorted(datasource_dir.glob("*.yaml")) if datasource_dir.is_dir() else []
    check("two datasource files", len(files), 2)
    types = set()
    for path in files:
        try:
            config = yaml.safe_load(path.read_text())
        except yaml.YAMLError as error:
            failures.append(f"{path}: {error}")
            continue
        for datasource in config.get("datasources", []):
            types.add(datasource.get("type"))
            check(f"{path.name}: url is vmauth", datasource.get("url"), "http://127.0.0.1:8427")
            header = datasource.get("secureJsonData", {}).get("httpHeaderValue1", "")
            check(
                f"{path.name}: token is environment-driven",
                header.startswith("Bearer $__env{DEEPMSG_GRAFANA_READ_TOKEN}"),
                True,
            )
        check(
            f"{path.name}: no literal bearer token",
            bool(re.search(r"Bearer (?!\$__env\{)", path.read_text())),
            False,
        )
    check(
        "datasource types",
        types,
        {"prometheus", "victoriametrics-logs-datasource"},
    )

    # The dashboards and the alert rules arrive in later commits; once they are
    # here, they parse or this fails.
    for path in sorted((GRAFANA / "provisioning" / "dashboards").glob("*.json")):
        try:
            json.loads(path.read_text())
        except json.JSONDecodeError as error:
            failures.append(f"{path}: {error}")
    provider_files = sorted((GRAFANA / "provisioning" / "dashboards").glob("*.yaml"))
    check("one dashboard provider file", len(provider_files), 1)
    for path in provider_files:
        try:
            config = yaml.safe_load(path.read_text())
        except yaml.YAMLError as error:
            failures.append(f"{path}: {error}")
            continue
        providers = config.get("providers", [])
        check(f"{path.name}: one provider", len(providers), 1)
        if providers:
            provider = providers[0]
            check(f"{path.name}: provider type", provider.get("type"), "file")
            check(
                f"{path.name}: dashboard path is environment-driven",
                provider.get("options", {}).get("path"),
                "$__env{DEEPMSG_GRAFANA_DASHBOARDS_DIR}",
            )
    # The alert rules: six, the three the E2E fires among them, no dashboard
    # variable in a rule, and a contact point the root policy names.
    alerting_dir = GRAFANA / "provisioning" / "alerting"
    rules_files = sorted(alerting_dir.glob("rules.yaml"))
    check("one rules file", len(rules_files), 1)
    rules: list = []
    if rules_files:
        try:
            config = yaml.safe_load(rules_files[0].read_text())
        except yaml.YAMLError as error:
            failures.append(f"{rules_files[0]}: {error}")
            config = {}
        groups = config.get("groups", [])
        check("one alert group", len(groups), 1)
        if groups:
            rules = groups[0].get("rules", [])
        check("six alert rules", len(rules), 6)
        for rule in rules:
            uid = rule.get("uid", "")
            check(f"rule {uid}: condition", rule.get("condition"), "C")
            check(
                f"rule {uid}: severity",
                rule.get("labels", {}).get("severity") in {"P1", "P2", "P3"},
                True,
            )
            check(
                f"rule {uid}: runbook_url",
                "runbook.md#" in rule.get("annotations", {}).get("runbook_url", ""),
                True,
            )
        expressions = [
            str(entry.get("model", {}).get("expr", ""))
            for rule in rules
            for entry in rule.get("data", [])
        ]
        check(
            "no dashboard variable in the rule expressions",
            any("$node" in expression or "$NODE" in expression for expression in expressions),
            False,
        )
        uids = {rule.get("uid") for rule in rules}
        for uid in ("driver-heartbeat-stale", "driver-metrics-absent", "cluster-role-jitter"):
            check(f"rule {uid} is provisioned", uid in uids, True)

    # Every rule's runbook fragment must have an anchor, or an alert links to
    # nothing when it fires.
    runbook_path = GRAFANA / "runbook.md"
    check("runbook exists", runbook_path.is_file(), True)
    runbook = runbook_path.read_text() if runbook_path.is_file() else ""
    for rule in rules:
        url = str(rule.get("annotations", {}).get("runbook_url", ""))
        fragment = url.rsplit("#", 1)[-1] if "#" in url else ""
        check(f"runbook anchor for {rule.get('uid')}", f'id="{fragment}"' in runbook, True)
    for anchor in ("drill-collector-off", "drill-swap-backend", "commit-lag"):
        check(f"runbook anchor {anchor}", f'id="{anchor}"' in runbook, True)

    contact_files = sorted(alerting_dir.glob("contact-points.yaml"))
    check("one contact-points file", len(contact_files), 1)
    if contact_files:
        try:
            config = yaml.safe_load(contact_files[0].read_text())
        except yaml.YAMLError as error:
            failures.append(f"{contact_files[0]}: {error}")
            config = {}
        points = config.get("contactPoints", [])
        check("one contact point", len(points), 1)
        if points:
            receivers = points[0].get("receivers", [])
            check("one receiver", len(receivers), 1)
            if receivers:
                check(
                    "webhook url is environment-driven",
                    receivers[0].get("settings", {}).get("url"),
                    "$__env{DEEPMSG_GRAFANA_WEBHOOK_URL}",
                )

    policy_files = sorted(alerting_dir.glob("policies.yaml"))
    check("one policies file", len(policy_files), 1)
    if policy_files:
        try:
            config = yaml.safe_load(policy_files[0].read_text())
        except yaml.YAMLError as error:
            failures.append(f"{policy_files[0]}: {error}")
            config = {}
        policies = config.get("policies", [])
        check("one root policy", len(policies), 1)
        if policies:
            check("root policy receiver", policies[0].get("receiver"), "deepmsg-webhook")

    readme = README.read_text() if README.exists() else ""
    check(
        "DEEPMSG_GRAFANA_READ_TOKEN is documented in README.md",
        "DEEPMSG_GRAFANA_READ_TOKEN" in readme,
        True,
    )
    check(
        "DEEPMSG_GRAFANA_DASHBOARDS_DIR is documented in README.md",
        "DEEPMSG_GRAFANA_DASHBOARDS_DIR" in readme,
        True,
    )
    check(
        "DEEPMSG_GRAFANA_WEBHOOK_URL is documented in README.md",
        "DEEPMSG_GRAFANA_WEBHOOK_URL" in readme,
        True,
    )


def exporter_checks() -> None:
    """The two exporters' units, and the one variable they cannot run without.

    Neither exporter is part of the agent config: they are separate processes
    beside the driver. A missing unit is not a config error — it is a pipeline
    that silently has no `up{job="deepmsg-counters"}` and no `driver.error`,
    which is why the two files are checked here with everything else.
    """
    systemd = HERE.parent / "systemd"
    units = {
        "counters": systemd / "deepmsg-counters-exporter.service",
        "errorlog": systemd / "deepmsg-errorlog-exporter.service",
    }
    texts = {}
    for name, unit in units.items():
        check(f"{name} exporter unit exists", unit.is_file(), True)
        if not unit.is_file():
            continue
        text = unit.read_text()
        texts[name] = text
        check(
            f"{name} exporter unit uses the environment file",
            "EnvironmentFile=/etc/deepmsg/observability.env" in text,
            True,
        )
        check(
            f"{name} exporter unit runs the pinned binary",
            f"/usr/local/bin/deepmsg-{name}-exporter" in text,
            True,
        )

    # The counters exporter's listen address is what the agent scrapes; a drift
    # between the two is a scrape that answers nothing, forever. The endpoint
    # line is required first, so a missing line fails rather than skipping the
    # comparison.
    environment = systemd / "observability.env.example"
    check("environment example exists", environment.is_file(), True)
    text = environment.read_text()
    ports = re.findall(r"^DEEPMSG_COUNTERS_ENDPOINT=127\.0\.0\.1:(\d+)$", text, re.M)
    check("environment example names the counters endpoint", len(ports), 1)
    if ports:
        check(
            "counters unit listens where the agent scrapes",
            f"--listen 127.0.0.1:{ports[0]}" in texts.get("counters", ""),
            True,
        )
    check("environment example names the aeron directory", "DEEPMSG_AERON_DIR=" in text, True)

    # The error-log exporter writes the applications' own log files, so the
    # directory has to survive ProtectSystem=strict.
    check(
        "errorlog unit may write the log directory",
        "ReadWritePaths=/var/log/deepmsg" in texts.get("errorlog", ""),
        True,
    )


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
        "hostmetrics.root_path is environment-driven",
        str(hostmetrics.get("root_path", "")),
        "${env:DEEPMSG_HOSTMETRICS_ROOT}",
    )
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
    check(
        "metrics.processors",
        metrics_pipeline.get("processors"),
        ["memory_limiter", "resource/node_id", "batch"],
    )
    check(
        "metrics.exporters",
        metrics_pipeline.get("exporters"),
        ["prometheusremotewrite/victoriametrics"],
    )
    resource = mapping(
        mapping(config.get("processors"), "processors").get("resource/node_id"),
        "processors.resource/node_id",
    )
    actions = sequence(resource.get("attributes"), "processors.resource/node_id.attributes")
    check("resource/node_id has one action", len(actions), 1)
    if actions:
        action = mapping(actions[0], "processors.resource/node_id.attributes[0]")
        check("metrics node_id key", action.get("key"), "node_id")
        check("metrics node_id action", action.get("action"), "upsert")
        check(
            "metrics node_id is environment-driven",
            str(action.get("value", "")),
            "${env:DEEPMSG_NODE_ID}",
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

    grafana_checks()
    exporter_checks()

    if failures:
        print(f"{CONFIG.relative_to(CONFIG.parents[2])}: {len(failures)} check(s) failed", file=sys.stderr)
        for failure in failures:
            print(f"  - {failure}", file=sys.stderr)
        return 1

    print(f"{CONFIG.relative_to(CONFIG.parents[2])}: {checks} checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
