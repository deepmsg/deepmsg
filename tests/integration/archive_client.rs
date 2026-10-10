//! P2-C1's acceptance, first half: what an archive client is configured with.
//!
//! These are ports of the reference's own cases — `AeronCArchiveIdTest` in
//! `aeron_archive_test.cpp:609` — and they are ports in the strict sense: the
//! values, the limits and the **reason texts** are the ones that file asserts,
//! because a client that refuses for a different reason is a client whose
//! caller cannot be written against it.
//!
//! Fifteen of the sixteen cases in that fixture need no archive at all, and
//! neither do most of these: what a context *is* is decided before anything is
//! connected, and the reference's own cases reach for a live archive in exactly
//! one place (`shouldResolveArchiveId`, which is a later commit and needs a Java
//! archive to talk to).
//!
//! The three cases at the end are the exception, and they are the ones about
//! `conclude`'s *second* half: what a non-response-mode context does is ask a
//! **driver** for a session id and write the answer into two channel URIs. They
//! run against our own driver, which is the production path — the reference's
//! own versions fabricate an `aeron_t` instead, to reach a shortcut that has no
//! counterpart here.
//!
//! The lists below are passed to `resolve` rather than set as a process's
//! environment: an environment is one per process, and two of these tests set
//! it — one to a hundred-and-three-character name, one to `9223372036s`. The
//! reader that turns a process into that list has a smoke test of its own.

use std::time::Duration;

use deepmsg_client::client::Client;
use deepmsg_core::uri::ChannelUri;
use deepmsg_tests::driver::{self, OwnDriver};

use deepmsg_archive::client::archive::channel_with_session_id;
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

/// A driver and a client for the cases below, or `None` when our driver is not
/// built.
///
/// The driver comes back with the client because it has to outlive it: dropping
/// a driver kills its process (`driver.rs:574-584`), and a client whose driver
/// has gone is a client that waits for an answer that cannot come.
fn driver_and_client(test_name: &str) -> Option<(OwnDriver, Client)> {
    let Some(mut own) = OwnDriver::start(test_name) else {
        driver::announce_own_skip();
        return None;
    };

    own.await_cnc(Duration::from_secs(10))
        .expect("the driver publishes its CnC file");

    let client = Client::connect(own.aeron_dir()).expect("connect to our driver");

    Some((own, client))
}

/// `shouldApplyDefaultParametersToRequestAndResponseChannels` (`:3171-3229`).
///
/// A context that names neither a term length, an MTU nor sparse-ness gets all
/// three written into **both** channels — and then **one session id, in both**.
/// That one id in two places is the point of the case: the archive has to be
/// able to tell that the request it reads and the response it is told to send
/// belong to the same session.
///
/// The reference's own version fabricates an `aeron_t` with
/// `control_protocol_version = 0` rather than starting a driver
/// (`aeron_archive_test.cpp:3177-3193`), because below 1.0.0
/// `aeron_async_next_session_id` hands back a randomised int32 with no command
/// sent at all (`aeron_client_conductor.c:2344-2371`). There is no such
/// shortcut here, so this asks a real driver — which is what production does.
#[test]
fn default_parameters_reach_both_control_channels() {
    let Some((_own, mut client)) = driver_and_client("archive-client-default-parameters") else {
        return;
    };

    let mut context = ArchiveContext::resolve(&environment(&[
        (CONTROL_CHANNEL_ENV, "aeron:ipc"),
        (
            CONTROL_RESPONSE_CHANNEL_ENV,
            "aeron:udp?endpoint=127.0.0.1:0",
        ),
    ]));
    context.control_term_buffer_length = 256 * 1024;
    context.control_mtu_length = 2048;
    context.control_term_buffer_sparse = false;

    let channels = context
        .conclude_with(&mut client)
        .expect("a context a session can be opened with");

    let request = ChannelUri::parse(&channels.request).expect("a channel");
    assert_eq!(Some("262144"), request.get("term-length"));
    assert_eq!(Some("2048"), request.get("mtu"));
    assert_eq!(Some("false"), request.get("sparse"));
    assert_eq!("ipc", request.media());

    let response = ChannelUri::parse(&channels.response).expect("a channel");
    assert_eq!(Some("262144"), response.get("term-length"));
    assert_eq!(Some("2048"), response.get("mtu"));
    assert_eq!(Some("false"), response.get("sparse"));
    assert_eq!(Some("127.0.0.1:0"), response.get("endpoint"));
    assert_eq!("udp", response.media());

    let session_id = request.get("session-id").expect("a session id");
    assert_ne!("", session_id, "and it is not the empty one");
    assert_eq!(
        Some(session_id),
        response.get("session-id"),
        "both channels name the same session"
    );
}

/// `shouldNotApplyDefaultParameters…IfTheyAreSetExplicitly` (`:3231-3296`).
///
/// Nothing that is already written is overwritten — not the term length
/// spelled `64k`, not the MTU, not sparse-ness — and the parameters a channel
/// carries for its own reasons (`ttl`, `interface`, `alias`) survive the trip
/// through the builder untouched.
///
/// **One thing is overwritten**, and it is the case's quiet half: the request
/// channel above says `session-id=0`, and afterwards it names the minted
/// session instead. A session id is not a default; it is the session's, and
/// `put_int32` replaces it (`aeron_archive_context.c:399-400`).
#[test]
fn explicit_parameters_are_not_overwritten() {
    let Some((_own, mut client)) = driver_and_client("archive-client-explicit-parameters") else {
        return;
    };

    let mut context = ArchiveContext::resolve(&environment(&[
        (
            CONTROL_CHANNEL_ENV,
            "aeron:udp?endpoint=localhost:8080|term-length=64k|mtu=1408|sparse=true|session-id=0|ttl=3|interface=127.0.0.1",
        ),
        (
            CONTROL_RESPONSE_CHANNEL_ENV,
            "aeron:ipc?term-length=128k|mtu=4096|sparse=true|alias=response",
        ),
    ]));
    context.control_term_buffer_length = 256 * 1024;
    context.control_mtu_length = 2048;
    context.control_term_buffer_sparse = false;

    let channels = context
        .conclude_with(&mut client)
        .expect("a context a session can be opened with");

    let request = ChannelUri::parse(&channels.request).expect("a channel");
    assert_eq!(Some("64k"), request.get("term-length"), "not 262144");
    assert_eq!(Some("1408"), request.get("mtu"), "not 2048");
    assert_eq!(Some("true"), request.get("sparse"), "not false");
    assert_eq!(Some("3"), request.get("ttl"));
    assert_eq!(Some("127.0.0.1"), request.get("interface"));
    assert_eq!("udp", request.media());

    let response = ChannelUri::parse(&channels.response).expect("a channel");
    assert_eq!(Some("128k"), response.get("term-length"));
    assert_eq!(Some("4096"), response.get("mtu"));
    assert_eq!(Some("true"), response.get("sparse"));
    assert_eq!(Some("response"), response.get("alias"));
    assert_eq!("ipc", response.media());

    let session_id = request.get("session-id").expect("a session id");
    assert_ne!("", session_id);
    assert_eq!(
        Some(session_id),
        response.get("session-id"),
        "still one session, named on both"
    );

    // **Added to the reference's assertions**, because without it the case is
    // nearly vacuous: the channel was written `session-id=0` above, and
    // `non-empty` is satisfied by the `0` that was already there. What the case
    // claims is that the id is *replaced*, and this is that claim.
    //
    // It is not a hope about what the driver will say. A driver never answers
    // with an id inside its reserved range — `SessionIds::cursor` *skips* the
    // whole of it (`crates/driver/src/ipc_publications.rs:343-349`), and the
    // range is `-1..=1000` by default (`config.rs:414`, `:421`) — so nought is
    // one of the ids it cannot give, whatever it seeded its cursor with.
    assert_ne!(
        Some("0"),
        request.get("session-id"),
        "the session id that was written by hand is replaced by the session's"
    );
}

/// `shouldNotSetSessionIdOnControlRequestAndReponseChannelsIfControlModeResponseIsUsed`
/// (`:3298-3336`).
///
/// The other direction, and the reason the two cases are worth separate tests:
/// a response channel whose `control-mode` is `response` is the archive's own,
/// so the client names no session **anywhere** — and does not ask the driver
/// for one, which is why this path does not touch the client at all.
///
/// Note what is *still* written: term length, MTU and sparse-ness reach both
/// channels here as well. Only the session id is conditional.
#[test]
fn a_response_mode_context_names_no_session() {
    let Some((mut own, mut client)) = driver_and_client("archive-client-response-mode") else {
        return;
    };

    let mut context = ArchiveContext::resolve(&environment(&[
        (CONTROL_CHANNEL_ENV, "aeron:udp?endpoint=localhost:8080"),
        (
            CONTROL_RESPONSE_CHANNEL_ENV,
            "aeron:udp?control=localhost:9090|control-mode=response",
        ),
    ]));
    context.control_term_buffer_length = 256 * 1024;
    context.control_mtu_length = 2048;
    context.control_term_buffer_sparse = false;

    // Take the driver away first, because *this* is the claim worth testing and
    // nothing below needs a driver: the response-mode path never touches the
    // client, and a context that never asks cannot be told a session id by a
    // driver that is not there. A conclusion that still succeeds here is one
    // that did not ask — where `next_session_id` against a dead driver cannot
    // succeed at all. "Does not set a session id" and "does not ask for one"
    // are different claims, and only the second says the branch is really
    // skipped rather than asked-and-discarded. The cursor moves on every ask,
    // so asking has a cost even when the answer is thrown away.
    own.stop().expect("stop our driver");

    let channels = context
        .conclude_with(&mut client)
        .expect("a context a session can be opened with");

    let request = ChannelUri::parse(&channels.request).expect("a channel");
    assert_eq!(Some("localhost:8080"), request.get("endpoint"));
    assert_eq!(None, request.get("session-id"));

    let response = ChannelUri::parse(&channels.response).expect("a channel");
    assert_eq!(
        None,
        response.get("endpoint"),
        "a response channel has a control address, not an endpoint"
    );
    assert_eq!(Some("localhost:9090"), response.get("control"));
    assert_eq!(Some("response"), response.get("control-mode"));
    assert_eq!(None, response.get("session-id"));
}

/// `shouldDuplicateContext` (`:3338-3405`).
///
/// A duplicated context is **equal in every field and independent in every
/// pointer**: the copy's channels say the same thing and are their own
/// allocations, so writing to one cannot be seen through the other. The
/// reference needs a function for this because its copy is a `memcpy` with
/// three re-`strdup`s (`:229-267`); here [`Clone`] does it, and what the case
/// is really pinning is that it stays a *deep* copy — a field changed from
/// `String` to `Arc<str>` would satisfy `assert_eq!` and break this.
///
/// Four of the reference's assertions have no counterpart and are not ported:
/// `aeron`, `owns_aeron_client`, `error_handler` and `idle_strategy` are a
/// driver connection, an ownership flag and two callbacks. This context holds
/// none of them — the client is an argument to
/// [`ArchiveContext::conclude_with`], not a field — so there is nothing to
/// compare.
#[test]
fn a_context_is_copied_not_shared() {
    let mut original = ArchiveContext::resolve(&environment(&[
        (CONTROL_CHANNEL_ENV, "aeron:udp?endpoint=localhost:8080"),
        (CONTROL_STREAM_ID_ENV, "42"),
        (
            CONTROL_RESPONSE_CHANNEL_ENV,
            "aeron:udp?endpoint=localhost:0",
        ),
        (CONTROL_RESPONSE_STREAM_ID_ENV, "-5"),
        (RECORDING_EVENTS_STREAM_ID_ENV, "777"),
        (CONTROL_TERM_BUFFER_LENGTH_ENV, "256k"),
        (CONTROL_MTU_LENGTH_ENV, "2048"),
        (CONTROL_TERM_BUFFER_SPARSE_ENV, "false"),
        (MESSAGE_TIMEOUT_ENV, "1s"),
    ]));

    let copy = original.clone();

    assert_eq!(original, copy, "every field, value for value");
    assert_eq!(42, copy.control_request_stream_id);
    assert_eq!(-5, copy.control_response_stream_id);
    assert_eq!(777, copy.recording_events_stream_id);
    assert_eq!(1_000_000_000, copy.message_timeout_ns);
    assert_eq!(256 * 1024, copy.control_term_buffer_length);
    assert_eq!(2048, copy.control_mtu_length);
    assert!(!copy.control_term_buffer_sparse);
    assert_eq!(
        None, copy.recording_events_channel,
        "unset, and stays unset"
    );

    // `EXPECT_NE(m_ctx->control_request_channel, copy_ctx->control_request_channel)`
    // is a pointer comparison, and this is the same claim: the two strings are
    // equal in value and are not the same memory.
    let (original_channel, copy_channel) = (
        original
            .control_request_channel
            .as_ref()
            .expect("set above"),
        copy.control_request_channel.as_ref().expect("set above"),
    );
    assert_eq!(original_channel, copy_channel);
    assert_ne!(
        original_channel.as_ptr(),
        copy_channel.as_ptr(),
        "equal in value, not the same allocation"
    );

    // And independent: changing one leaves the other as it was.
    original.control_request_channel = Some("aeron:ipc".to_owned());
    assert_eq!(
        Some("aeron:udp?endpoint=localhost:8080".to_owned()),
        copy.control_request_channel
    );
}

/// **A channel can be given a session id**
/// (`aeron_archive_channel_with_session_id`, `:2592-2607`).
///
/// There is no reference **case** to port: the C helper has none, and the Java
/// client has no equivalent at all — its two callers there write the same thing
/// inline through a builder. So what is asserted is the reference's *code*: the
/// session is a parameter **put** on the channel, so an existing one is replaced
/// where it stands and the channel's own parameters are not disturbed.
///
/// That last part is the one place this deviates from the reference. It parses
/// into a `ChannelUriStringBuilder` and prints its fields in a fixed order
/// (`ChannelUriStringBuilder.java:2451-2512`), so a channel whose parameters
/// were written in another order comes back normalised; [`ChannelUri`] prints
/// what it read, which makes this a round trip. Both are asserted below, because
/// a caller that compares the result against what it passed in needs to know
/// which one it is getting.
#[test]
fn a_channel_can_be_given_a_session_id() {
    assert_eq!(
        "aeron:ipc?session-id=7",
        channel_with_session_id("aeron:ipc", 7).expect("a channel"),
        "a channel with no parameters gets one"
    );

    assert_eq!(
        "aeron:ipc?alias=client-api|session-id=7",
        channel_with_session_id("aeron:ipc?alias=client-api", 7).expect("a channel"),
        "and one that had parameters keeps them, in the order they were read"
    );

    assert_eq!(
        "aeron:udp?endpoint=localhost:8080|session-id=7",
        channel_with_session_id("aeron:udp?endpoint=localhost:8080|session-id=5", 7)
            .expect("a channel"),
        "a session it already had is replaced where it stands, not appended"
    );

    assert!(
        channel_with_session_id("ipc", 7).is_err(),
        "and something that is not an aeron channel is not one after this either"
    );
}
