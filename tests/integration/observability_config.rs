//! The agent configuration's contract lines.
//!
//! `deploy/observability/e2e/check-config.py` checks the whole file
//! semantically, with PyYAML, when the end-to-end scripts run. These are the
//! three lines whose absence is *silent* — the stream fields, the Go time
//! layout and the severity source — pinned where CI already runs, so a
//! well-meaning edit to the YAML fails without needing Python or the binaries.

use std::path::{Path, PathBuf};

fn config() -> (PathBuf, String) {
    let path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../deploy/observability/agent/otelcol.yaml");
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    (path, text)
}

fn app_config() -> (PathBuf, String) {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../deploy/observability/agent/otelcol-app.yaml");
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    (path, text)
}

/// How many lines, after trimming, are exactly `needle`.
fn occurrences(text: &str, needle: &str) -> usize {
    text.lines().filter(|line| line.trim() == needle).count()
}

fn assert_once(path: &Path, text: &str, needle: &str) {
    assert_eq!(
        1,
        occurrences(text, needle),
        "{} should contain exactly one `{needle}`",
        path.display()
    );
}

#[test]
fn the_stream_fields_are_the_two_identity_fields() {
    let (path, text) = config();
    assert_once(&path, &text, "VL-Stream-Fields: \"component,node_id\"");
}

#[test]
fn the_timestamp_layout_is_a_go_layout_and_not_strptime() {
    let (path, text) = config();
    assert_once(&path, &text, "layout_type: gotime");
    assert_once(
        &path,
        &text,
        "layout: '2006-01-02T15:04:05.999999999Z07:00'",
    );
}

#[test]
fn the_level_reaches_the_severity_field() {
    let (path, text) = config();
    assert_once(&path, &text, "parse_from: attributes.level");
}

#[test]
fn the_application_overlay_adds_one_job_and_one_pipeline() {
    let (path, text) = app_config();
    assert_once(&path, &text, "- job_name: app-metrics");
    assert_once(
        &path,
        &text,
        "- targets: [\"${env:DEEPMSG_APP_METRICS_ENDPOINT}\"]",
    );
    assert_once(&path, &text, "receivers: [prometheus/app]");
    assert_once(&path, &text, "metrics/app:");
    // The identity processors are the base's; the overlay must name them, or
    // application samples arrive without a node_id and every host's series is
    // one series.
    assert_once(
        &path,
        &text,
        "processors: [memory_limiter/app, resource/node_id, transform/node_id, batch]",
    );
}

#[test]
fn the_application_agent_loads_both_configs() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../deploy/observability/systemd/deepmsg-app-agent.service");
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    assert!(
        text.contains("--config /etc/deepmsg/otelcol.yaml --config /etc/deepmsg/otelcol-app.yaml")
    );
    assert!(text.contains("EnvironmentFile=/etc/deepmsg/observability.env"));
}

#[test]
fn no_literal_token_is_committed() {
    let (path, text) = config();
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("Authorization:") {
            assert!(trimmed.contains("${env:"), "{}: {trimmed}", path.display());
        }
    }
}
