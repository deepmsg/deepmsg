//! P2-C1's acceptance, first half: what an archive client is configured with.
//!
//! These are ports of the reference's own cases — `AeronCArchiveIdTest` in
//! `aeron_archive_test.cpp:609` — and they are ports in the strict sense: the
//! values, the limits and the **reason texts** are the ones that file asserts,
//! because a client that refuses for a different reason is a client whose
//! caller cannot be written against it.
//!
//! Fifteen of the sixteen cases in that fixture need no archive at all, and
//! neither do these: what a context *is* is decided before anything is
//! connected, and the reference's own cases reach for a live archive in exactly
//! one place (`shouldResolveArchiveId`, which is a later commit and needs a Java
//! archive to talk to).
//!
//! The lists below are passed to `resolve` rather than set as a process's
//! environment: an environment is one per process, and two of these tests set
//! it — one to a hundred-and-three-character name, one to `9223372036s`. The
//! reader that turns a process into that list has a smoke test of its own.

use deepmsg_archive::client::context::{
    AERON_CLIENT_NAME_ENV, AERON_DIR_ENV, CLIENT_NAME_ENV, CONTROL_CHANNEL_ENV,
    CONTROL_MTU_LENGTH_ENV, CONTROL_RESPONSE_CHANNEL_ENV, CONTROL_RESPONSE_STREAM_ID_ENV,
    CONTROL_STREAM_ID_ENV, CONTROL_TERM_BUFFER_LENGTH_ENV, CONTROL_TERM_BUFFER_SPARSE_ENV,
    MESSAGE_RETRY_ATTEMPTS_ENV, MESSAGE_TIMEOUT_ENV, MTU_LENGTH_ENV, RECORDING_EVENTS_CHANNEL_ENV,
    RECORDING_EVENTS_STREAM_ID_ENV,
};
use deepmsg_archive::client::{ArchiveContext, ClientError};

/// A list of `(name, value)` pairs, which is what the environment would be.
fn environment(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
        .collect()
}

/// A context that will pass [`ArchiveContext::conclude`]: the two channels a
/// session cannot be opened without, and nothing else.
fn conclude_able() -> ArchiveContext {
    ArchiveContext::resolve(&environment(&[
        (CONTROL_CHANNEL_ENV, "aeron:ipc"),
        (CONTROL_RESPONSE_CHANNEL_ENV, "aeron:ipc"),
    ]))
}

/// `shouldInitializeContextWithDefaultValues` (`:3030-3056`).
///
/// Every default, and the three that are not the constant a reader would guess:
/// the events stream is 30, the control MTU is 1408, and a control channel's
/// term buffer is **sparse** — the opposite of a data channel's.
#[test]
fn a_context_starts_with_the_reference_defaults() {
    let context = ArchiveContext::new();

    assert!(
        !context.aeron_dir.is_empty(),
        "a client that owns its own Aeron client has to have somewhere to look"
    );
    assert_eq!(None, context.control_request_channel);
    assert_eq!(10, context.control_request_stream_id);
    assert_eq!(None, context.control_response_channel);
    assert_eq!(20, context.control_response_stream_id);
    assert_eq!(None, context.recording_events_channel);
    assert_eq!(30, context.recording_events_stream_id);
    assert_eq!(10_000_000_000, context.message_timeout_ns);
    assert_eq!(3, context.message_retry_attempts);
    assert_eq!(64 * 1024, context.control_term_buffer_length);
    assert!(context.control_term_buffer_sparse);
    assert_eq!(1408, context.control_mtu_length);
    assert!(context.warnings.is_empty(), "and nothing to warn about");
}

/// `shouldInitializeContextWithValuesSpecifiedViaEnvironment` (`:3058-3113`).
///
/// The values are the reference's, verbatim, and two of them are worth reading
/// twice: `9223372036s` is 9223372036000000000 ns — a second under `i64::MAX`,
/// so a parser that multiplies into an `i64` without checking overflows here and
/// only here — and the stream ids are read as **signed** 32-bit, so `-4321` is a
/// stream id and `INT32_MAX` is one too.
#[test]
fn a_context_reads_the_values_it_is_given() {
    let context = ArchiveContext::resolve(&environment(&[
        ("AERON_DIR", "/dev/shm/aeron-test-dir"),
        (CONTROL_CHANNEL_ENV, "aeron:udp?endpoint=localhost:5555"),
        (CONTROL_STREAM_ID_ENV, "-4321"),
        (
            CONTROL_RESPONSE_CHANNEL_ENV,
            "aeron:udp?endpoint=localhost:0",
        ),
        (CONTROL_RESPONSE_STREAM_ID_ENV, "2009"),
        (
            RECORDING_EVENTS_CHANNEL_ENV,
            "aeron:udp?endpoint=localhost:8888|alias=events",
        ),
        (RECORDING_EVENTS_STREAM_ID_ENV, "2147483647"),
        (MESSAGE_TIMEOUT_ENV, "9223372036s"),
        (MESSAGE_RETRY_ATTEMPTS_ENV, "404"),
        (CONTROL_TERM_BUFFER_LENGTH_ENV, "128k"),
        (CONTROL_TERM_BUFFER_SPARSE_ENV, "false"),
        (CONTROL_MTU_LENGTH_ENV, "8k"),
    ]));

    assert_eq!("/dev/shm/aeron-test-dir", context.aeron_dir);
    assert_eq!(
        Some("aeron:udp?endpoint=localhost:5555".to_owned()),
        context.control_request_channel
    );
    assert_eq!(-4321, context.control_request_stream_id);
    assert_eq!(
        Some("aeron:udp?endpoint=localhost:0".to_owned()),
        context.control_response_channel
    );
    assert_eq!(2009, context.control_response_stream_id);
    assert_eq!(
        Some("aeron:udp?endpoint=localhost:8888|alias=events".to_owned()),
        context.recording_events_channel
    );
    assert_eq!(i32::MAX, context.recording_events_stream_id);

    assert_eq!(9_223_372_036_000_000_000, context.message_timeout_ns);
    assert_eq!(404, context.message_retry_attempts);

    assert_eq!(128 * 1024, context.control_term_buffer_length);
    assert!(!context.control_term_buffer_sparse);
    assert_eq!(8192, context.control_mtu_length);

    assert!(
        context.conclude().is_ok(),
        "and it is a context a session can be opened with"
    );
}

/// `shouldFailWithErrorIfControlRequestChannelIsNotDefined` (`:3115-3124`).
#[test]
fn a_context_without_a_control_channel_is_refused() {
    let context = ArchiveContext::new();

    let error = context.conclude().expect_err("there is nowhere to send");

    assert_eq!(ClientError::ControlRequestChannelRequired, error);
    assert!(
        error
            .to_string()
            .contains("control request channel is required"),
        "the reason a caller matches on: {error}"
    );
}

/// `shouldFailWithErrorIfControlResponseChannelIsNotDefined` (`:3126-3137`).
#[test]
fn a_context_without_a_response_channel_is_refused() {
    let context = ArchiveContext::resolve(&environment(&[(CONTROL_CHANNEL_ENV, "aeron:ipc")]));

    let error = context
        .conclude()
        .expect_err("there is nowhere to be answered");

    assert_eq!(ClientError::ControlResponseChannelRequired, error);
    assert!(
        error
            .to_string()
            .contains("control response channel is required"),
        "the reason a caller matches on: {error}"
    );
}

/// `shouldFailWithErrorIfRetryAttemptsIsZero` (`:3139-3153`).
///
/// An offer that is never retried is a request that is lost the first time the
/// archive is busy, and the reference refuses it rather than shipping it.
#[test]
fn a_context_that_never_retries_is_refused() {
    let mut context = conclude_able();
    context.message_retry_attempts = 0;

    let error = context
        .conclude()
        .expect_err("an offer that is never sent again");

    assert_eq!(ClientError::RetryAttemptsMustBePositive, error);
    assert!(
        error
            .to_string()
            .contains("message_retry_attempts must be > 0"),
        "the reason a caller matches on: {error}"
    );
}

/// `shouldFailWithErrorIfAeronClientFailsToConnect` (`:3155-3169`).
///
/// The name is 127 characters — `"super very long client name"` and a hundred
/// `x`s — and the limit is the width of a counter's client-name field
/// (`aeronc.h:885`).
#[test]
fn a_context_whose_client_name_is_too_wide_is_refused() {
    let too_long = format!("super very long client name{}", "x".repeat(100));
    let mut context = conclude_able();
    context.aeron_client_name = too_long;

    let error = context
        .conclude()
        .expect_err("a counter has no room for it");

    assert_eq!(ClientError::ClientNameTooLong { length: 127 }, error);
    assert!(
        error.to_string().contains("client_name length must <= 100"),
        "the reason a caller matches on: {error}"
    );
}

/// A value that will not parse is the default **and a warning**.
///
/// `aeron_config_parse_*` warns and keeps the default rather than refusing
/// (`aeron_parse_util.c:650-745`), so a typo is a client that runs with a
/// default. That is only acceptable because something says so, which is what
/// [`ArchiveContext::warnings`] is for.
#[test]
fn a_value_that_will_not_parse_is_the_default_and_a_warning() {
    let context = ArchiveContext::resolve(&environment(&[
        (MESSAGE_TIMEOUT_ENV, "ten seconds"),
        (CONTROL_TERM_BUFFER_LENGTH_ENV, "sixty four k"),
        (CONTROL_STREAM_ID_ENV, "not a number"),
    ]));

    assert_eq!(10_000_000_000, context.message_timeout_ns);
    assert_eq!(64 * 1024, context.control_term_buffer_length);
    assert_eq!(10, context.control_request_stream_id);

    assert_eq!(
        vec![
            format!("{CONTROL_STREAM_ID_ENV}=not a number"),
            format!("{CONTROL_TERM_BUFFER_LENGTH_ENV}=sixty four k"),
            format!("{MESSAGE_TIMEOUT_ENV}=ten seconds"),
        ],
        context.warnings,
        "one warning per value, in the order the reader meets them"
    );
}

/// A value that parses but is out of range is **clamped**, not refused.
///
/// `aeron_config_parse_duration_ns(.., min, max)` and `aeron_config_parse_size64`
/// clamp both ends (`aeron_parse_util.c:701-745`), and the bounds are the ones a
/// buffer has to live in: a term is at least 64 KiB, and a control MTU is at
/// least a data header.
#[test]
fn a_value_out_of_range_is_clamped() {
    let context = ArchiveContext::resolve(&environment(&[
        (MESSAGE_TIMEOUT_ENV, "1ns"),
        (CONTROL_TERM_BUFFER_LENGTH_ENV, "1k"),
        (CONTROL_MTU_LENGTH_ENV, "1k"),
    ]));

    assert_eq!(
        1_000, context.message_timeout_ns,
        "a request's deadline has a floor of a microsecond"
    );
    assert_eq!(
        deepmsg_core::logbuffer::descriptor::TERM_MIN_LENGTH,
        context.control_term_buffer_length,
        "and a term one of 64 KiB, which 1k is under"
    );
    assert_eq!(
        1024, context.control_mtu_length,
        "while a MTU of 1k is inside its own bounds and stays where it was put"
    );
    assert!(
        context.warnings.is_empty(),
        "a clamped value parses: it is not a warning"
    );
}

/// A size reader stops after the suffix (`aeron_parse_util.c:60-70`).
///
/// `aeron_parse_size64` takes the leading digits and looks at **one** character
/// for `k`/`m`/`g`; it never asks for the string to end. So `64k`, `64k of
/// nonsense` and `64kilobytes` are all 65536, and a client that refused the
/// second would refuse a value the reference accepts — which is the kind of
/// difference that only shows up when a program is configured by a script
/// somebody else wrote.
#[test]
fn a_size_reads_its_suffix_and_ignores_the_rest() {
    for value in ["64k", "64k of nonsense", "64kib"] {
        let context =
            ArchiveContext::resolve(&environment(&[(CONTROL_TERM_BUFFER_LENGTH_ENV, value)]));

        assert_eq!(64 * 1024, context.control_term_buffer_length, "{value}");
        assert!(context.warnings.is_empty(), "{value} parses");
    }
}

/// The driver's own MTU seeds the control MTU's default.
///
/// `aeron_archive_context.c:92-97` reads `AERON_MTU_LENGTH` *first* and the
/// archive's variable over it, so `AERON_MTU_LENGTH` alone changes the control
/// channel too — and the archive's variable alone wins over it.
#[test]
fn the_drivers_mtu_seeds_the_control_channels() {
    let seeded = ArchiveContext::resolve(&environment(&[(MTU_LENGTH_ENV, "8k")]));

    assert_eq!(8192, seeded.control_mtu_length, "the driver's, by default");

    let overridden = ArchiveContext::resolve(&environment(&[
        (MTU_LENGTH_ENV, "8k"),
        (CONTROL_MTU_LENGTH_ENV, "2k"),
    ]));

    assert_eq!(
        2048, overridden.control_mtu_length,
        "and the archive's wins"
    );
}

/// The dialect is the reference's, name for name.
///
/// This is the whole of what makes a Rust client replace a C one: the same
/// program, configured the same way, has to find the same archive. A test that
/// spells the names out is the only thing that keeps a rename from being a
/// silent breaking change for every caller that exports them.
#[test]
fn the_environment_variable_names_are_the_references() {
    assert_eq!("AERON_DIR", AERON_DIR_ENV);
    assert_eq!("AERON_CLIENT_NAME", AERON_CLIENT_NAME_ENV);
    assert_eq!("AERON_MTU_LENGTH", MTU_LENGTH_ENV);
    assert_eq!("AERON_ARCHIVE_CONTROL_CHANNEL", CONTROL_CHANNEL_ENV);
    assert_eq!("AERON_ARCHIVE_CONTROL_STREAM_ID", CONTROL_STREAM_ID_ENV);
    assert_eq!(
        "AERON_ARCHIVE_CONTROL_RESPONSE_CHANNEL",
        CONTROL_RESPONSE_CHANNEL_ENV
    );
    assert_eq!(
        "AERON_ARCHIVE_CONTROL_RESPONSE_STREAM_ID",
        CONTROL_RESPONSE_STREAM_ID_ENV
    );
    assert_eq!(
        "AERON_ARCHIVE_RECORDING_EVENTS_CHANNEL",
        RECORDING_EVENTS_CHANNEL_ENV
    );
    assert_eq!(
        "AERON_ARCHIVE_RECORDING_EVENTS_STREAM_ID",
        RECORDING_EVENTS_STREAM_ID_ENV
    );
    assert_eq!("AERON_ARCHIVE_MESSAGE_TIMEOUT", MESSAGE_TIMEOUT_ENV);
    assert_eq!(
        "AERON_ARCHIVE_MESSAGE_RETRY_ATTEMPTS",
        MESSAGE_RETRY_ATTEMPTS_ENV
    );
    assert_eq!(
        "AERON_ARCHIVE_CONTROL_TERM_BUFFER_LENGTH",
        CONTROL_TERM_BUFFER_LENGTH_ENV
    );
    assert_eq!(
        "AERON_ARCHIVE_CONTROL_TERM_BUFFER_SPARSE",
        CONTROL_TERM_BUFFER_SPARSE_ENV
    );
    assert_eq!("AERON_ARCHIVE_CONTROL_MTU_LENGTH", CONTROL_MTU_LENGTH_ENV);
    assert_eq!("AERON_ARCHIVE_CLIENT_NAME", CLIENT_NAME_ENV);
}

/// Reading this process's environment is the list above, built from it.
///
/// The smoke test for the one function the others deliberately do not use: it
/// has to survive whatever this process was started with, which is the property
/// that matters — a client reads an environment it did not choose.
#[test]
fn this_processs_environment_is_a_context() {
    let context = ArchiveContext::from_env();

    assert!(!context.aeron_dir.is_empty());

    if let Ok(dir) = std::env::var(AERON_DIR_ENV) {
        if !dir.is_empty() {
            assert_eq!(dir, context.aeron_dir, "AERON_DIR reaches the context");
        }
    }
}
