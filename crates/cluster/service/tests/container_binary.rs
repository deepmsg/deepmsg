//! The container binary's refusals, run as the process they are.
//!
//! Every case here stops **before** the agent starts: no driver, no archive, no
//! consensus module. That is the point — a deployment that misspells its
//! service name, or carries a Java class name over from the reference, must be
//! told so and exit non-zero rather than run a different business.

use std::process::Command;

const BINARY: &str = env!("CARGO_BIN_EXE_deepmsg-clustered-service-container");

fn run(args: &[&str]) -> std::process::Output {
    Command::new(BINARY)
        .args(args)
        .output()
        .expect("the container binary runs")
}

#[test]
fn help_names_the_registry_and_exits_zero() {
    let output = run(&["--help"]);

    assert!(output.status.success(), "--help must not fail");
    let stdout = String::from_utf8_lossy(&output.stdout);
    for name in ["echo", "basic-auction", "noop"] {
        assert!(
            stdout.contains(name),
            "--help does not mention `{name}`:\n{stdout}"
        );
    }
}

#[test]
fn a_missing_service_name_is_refused_and_the_names_are_listed() {
    let output = run(&["-Daeron.dir=/nonexistent"]);

    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("deepmsg.cluster.service is not set"),
        "the refusal does not say what is missing:\n{stderr}"
    );
    for name in ["echo", "basic-auction", "noop"] {
        assert!(
            stderr.contains(name),
            "the refusal does not list `{name}`:\n{stderr}"
        );
    }
}

#[test]
fn an_unknown_service_name_is_refused_not_defaulted() {
    let output = run(&[
        "-Ddeepmsg.cluster.service=auction",
        "-Daeron.dir=/nonexistent",
    ]);

    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("deepmsg.cluster.service=auction names no business"),
        "the refusal does not name the business:\n{stderr}"
    );
}

#[test]
fn a_java_class_name_is_refused_by_name() {
    let output = run(&[
        "-Daeron.cluster.service.class.name=com.example.Echo",
        "-Ddeepmsg.cluster.service=echo",
        "-Daeron.dir=/nonexistent",
    ]);

    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("names a Java class"),
        "the refusal does not say why:\n{stderr}"
    );
}

#[test]
fn an_unknown_option_is_refused() {
    let output = run(&["--nonsense"]);

    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("unknown option '--nonsense'"),
        "the refusal does not name the option:\n{stderr}"
    );
}
