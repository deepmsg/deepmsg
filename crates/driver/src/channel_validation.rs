//! The checks a channel must pass against the endpoint it is about to join.
//!
//! Mirrors the validation half of `aeron-driver/src/main/c/aeron_driver_conductor.c`
//! (`:464-568`) and `uri/aeron_driver_uri.c:555-600`. They live in a module of
//! their own here rather than beside the endpoint lookups that call them,
//! because they are **pure**: four numbers and two channel URIs in, an error
//! message or nothing out. The reference keeps them in the conductor because it
//! keeps everything there.
//!
//! # Why a second channel has to agree with the first
//!
//! Two channels that resolve to one endpoint share **one socket**, and a socket
//! has one `SO_SNDBUF` and one `SO_RCVBUF`. So a channel that names a buffer
//! length is not describing its own socket — it is describing the one it is
//! about to join, and a value that disagrees with what that socket already has
//! is a request that cannot be honoured. The reference refuses it rather than
//! picking one of the two, and `ChannelValidationTest` is the twenty cases that
//! say so.
//!
//! Three of the four checks have a shape worth naming, because each is a way a
//! plausible implementation is wrong:
//!
//! * a channel that named **nothing** is not compared at all (`0 != new_length`):
//!   naming nothing means "whatever this endpoint has", so it always agrees;
//! * an existing value of **zero** means the endpoint is on the OS default, and
//!   that is a different message rather than a different rule;
//! * the MTU check walks a **precedence chain** — endpoint, then channel, then
//!   context, then OS default — and names in its message which one it used.
//!
//! The fourth function is not a channel-against-endpoint check at all:
//! [`validate_sender_mtu_length`] runs on the receiver when a `SETUP` announces
//! a sender's MTU, and is here because it is the same kind of thing — numbers
//! in, the reference's sentence out — and because the two live in the same
//! corner of the driver.

use deepmsg_core::logbuffer::descriptor::FRAME_ALIGNMENT;
use deepmsg_core::logbuffer::frame::DATA_HEADER_LENGTH;

use crate::publication_params::MAX_UDP_PAYLOAD_LENGTH;

/// `aeron_driver_conductor_validate_channel_buffer_length`
/// (`aeron_driver_conductor.c:528-568`).
///
/// # Errors
///
/// The reference's message, verbatim: a client reads it.
pub fn validate_channel_buffer_length(
    param_name: &str,
    named_length: usize,
    existing_length: usize,
    channel: &[u8],
    existing_channel: &[u8],
) -> Result<(), String> {
    // Naming nothing is agreement, not a mismatch: the channel takes whatever
    // the endpoint already has.
    if named_length == 0 || named_length == existing_length {
        return Ok(());
    }

    let channel = String::from_utf8_lossy(channel);
    let existing_channel = String::from_utf8_lossy(existing_channel);

    Err(if existing_length == 0 {
        format!(
            "{param_name}={named_length} does not match existing value of OS default: \
             existingChannel={existing_channel} channel={channel}"
        )
    } else {
        format!(
            "{param_name}={named_length} does not match existing value of {existing_length}: \
             existingChannel={existing_channel} channel={channel}"
        )
    })
}

/// `aeron_driver_conductor_validate_initial_window_for_rcvbuf`
/// (`aeron_driver_conductor.c:464-526`).
///
/// # Errors
///
/// The reference's message, verbatim.
pub fn validate_initial_window_for_rcvbuf(
    initial_window_length: usize,
    endpoint_socket_rcvbuf: usize,
    os_default_socket_rcvbuf: usize,
    channel: &[u8],
    existing_channel: Option<&[u8]>,
) -> Result<(), String> {
    // Two arms with one rule: a window that does not fit the receive buffer.
    // Which buffer is being measured is the difference, and it is the whole of
    // the difference — an endpoint with a buffer of its own is measured against
    // that, and one without is measured against what the kernel would have
    // given it, because that is what the socket actually has.
    let (limit, source) = if endpoint_socket_rcvbuf != 0 {
        (endpoint_socket_rcvbuf, "")
    } else {
        (os_default_socket_rcvbuf, " (OS default)")
    };

    // No `limit == 0` guard, deliberately: the second arm is
    // `os_default < window`, and an OS default of zero — which is what a
    // kernel that could not be asked leaves behind — therefore refuses every
    // window. That looks like a bug and is the reference's behaviour; a build
    // that quietly accepted the window on such a host would be the one that
    // diverges.
    if limit >= initial_window_length {
        return Ok(());
    }

    let channel = String::from_utf8_lossy(channel);
    let existing = match existing_channel {
        Some(bytes) => format!("existingChannel={} ", String::from_utf8_lossy(bytes)),
        None => String::new(),
    };

    Err(format!(
        "Initial window greater than SO_RCVBUF for channel: rcv-wnd={initial_window_length} \
         so-rcvbuf={endpoint_socket_rcvbuf}{source} {existing}channel={channel}"
    ))
}

/// `aeron_publication_params_validate_mtu_for_sndbuf`
/// (`uri/aeron_driver_uri.c:574-600`, on `:555-568`).
///
/// A frame has to fit the buffer that carries it, and the reference takes the
/// buffer from the first of four places that has one. The name of the one it
/// used goes into the message, which is the only way a reader can tell a
/// channel's own `so-sndbuf` from the driver's.
///
/// # Errors
///
/// The reference's message, verbatim.
pub fn validate_mtu_for_sndbuf(
    mtu_length: usize,
    endpoint_socket_sndbuf: usize,
    channel_socket_sndbuf: usize,
    context_socket_sndbuf: usize,
    os_default_socket_sndbuf: usize,
) -> Result<(), String> {
    for (length, name) in [
        (endpoint_socket_sndbuf, "endpoint"),
        (channel_socket_sndbuf, "channel"),
        (context_socket_sndbuf, "context"),
        (os_default_socket_sndbuf, "os default"),
    ] {
        if length != 0 {
            return validate_mtu(length, mtu_length, name);
        }
    }

    Ok(())
}

/// `aeron_receiver_channel_endpoint_validate_sender_mtu_length`
/// (`media/aeron_receive_channel_endpoint.c:984-1044`).
///
/// The check a receiver runs on the MTU a `SETUP` announces — the one time a
/// sender's own frame size arrives on this side, and therefore the only place
/// it can be held to what the socket and the window can carry. Six rules, and
/// the fourth is the one a client sees when it has configured a sender and a
/// receiver that cannot both be right: a frame that does not fit the window is
/// a frame that can never be acknowledged in time.
///
/// # This one is *recorded*, and the other three are answered
///
/// Which is why it returns the reference's whole recorded line — code,
/// description and site — where [`validate_mtu_for_sndbuf`] and its siblings
/// return the message a client reads. Nobody answers a client here: the image
/// is being built from a `SETUP`, and what happens instead is that the driver
/// writes an entry to the distinct error log, whose first line is the
/// composition `AERON_SET_ERR` leaves (`util/aeron_error.c:351-378`). The code
/// is the errno it was handed, a positive `EINVAL`, so the description is the
/// OS's text and not the protocol table's.
///
/// The caller appends its own line, which is what `AERON_APPEND_ERR` does at
/// the reference's call site (`aeron_driver_conductor.c:6509`).
///
/// # Errors
///
/// The reference's composition, verbatim. Its shape was read off a live
/// 1.53.2 driver — `tests/interop/` runs both drivers through this fault and
/// compares — because the file names, the line numbers and the OS's wording
/// are all things a careful reading gets almost right.
pub fn validate_sender_mtu_length(
    sender_mtu_length: usize,
    window_max_length: usize,
    socket_rcvbuf: usize,
    os_default_socket_rcvbuf: usize,
) -> Result<(), String> {
    if sender_mtu_length < DATA_HEADER_LENGTH {
        return Err(refused(
            992,
            format!("mtuLength={sender_mtu_length} < DATA_HEADER_LENGTH={DATA_HEADER_LENGTH}"),
        ));
    }

    #[allow(clippy::cast_possible_truncation)] // 65504 fits every pointer width here
    let max_payload = MAX_UDP_PAYLOAD_LENGTH as usize;
    if sender_mtu_length > max_payload {
        return Err(refused(
            1002,
            format!("mtuLength={sender_mtu_length} > MAX_UDP_PAYLOAD_LENGTH={max_payload}"),
        ));
    }

    #[allow(clippy::cast_sign_loss)] // 32
    let frame_alignment = FRAME_ALIGNMENT as usize;
    if sender_mtu_length % frame_alignment != 0 {
        return Err(refused(
            1012,
            format!(
                "mtuLength={sender_mtu_length} must be a multiple of FRAME_ALIGNMENT={FRAME_ALIGNMENT}"
            ),
        ));
    }

    if sender_mtu_length > window_max_length {
        return Err(refused(
            1022,
            format!("mtuLength={sender_mtu_length} > initialWindowLength={window_max_length}"),
        ));
    }

    // `:1030-1041`: the last two, which are about the *socket* rather than
    // about the arithmetic — a window or a frame larger than the receive
    // buffer is a driver whose pipeline cannot hold its own configuration.
    validate_so_rcvbuf(
        socket_rcvbuf,
        window_max_length,
        "Max Window length",
        os_default_socket_rcvbuf,
    )?;

    // Kept although it can never fire, because the reference keeps it and a
    // reader comparing the two should not have to work out that it is dead.
    // It is: reaching this line means `sender_mtu_length <= window_max_length`
    // (the rule above), and passing the call above means `socket_rcvbuf >=
    // window_max_length` — or the same for the OS default when there is no
    // socket buffer — so `socket_rcvbuf >= sender_mtu_length` already, which is
    // the negation of what this asks. Proving it once is cheaper than
    // re-deriving it every time somebody diffs the two files.
    validate_so_rcvbuf(
        socket_rcvbuf,
        sender_mtu_length,
        "Sender MTU",
        os_default_socket_rcvbuf,
    )
}

/// The file both functions of this pair raise in — the basename, because that
/// is what the reference's `__FILE__` prints.
const RECEIVE_ENDPOINT_FILE: &str = "aeron_receive_channel_endpoint.c";

/// Compose one of `aeron_receiver_channel_endpoint_validate_sender_mtu_length`'s
/// refusals: `EINVAL` (the errno every `AERON_SET_ERR` in that function is
/// handed) at the line the macro sits on.
fn refused(line: u32, message: String) -> String {
    deepmsg_cnc::error_log::compose_description(
        libc::EINVAL,
        "aeron_receiver_channel_endpoint_validate_sender_mtu_length",
        RECEIVE_ENDPOINT_FILE,
        line,
        &message,
    )
}

/// The same, for `aeron_receive_channel_endpoint_validate_so_rcvbuf`, whose two
/// arms are two sites (`:961` with a socket buffer, `:972` without).
fn refused_so_rcvbuf(line: u32, subject: &str, value: usize, limit: usize, suffix: &str) -> String {
    refused_by(
        "aeron_receive_channel_endpoint_validate_so_rcvbuf",
        line,
        format!(
            "{subject} greater than socket SO_RCVBUF, increase 'AERON_RCV_INITIAL_WINDOW_LENGTH' \
             to match window: value={value}, SO_RCVBUF={limit}{suffix}"
        ),
    )
}

/// [`refused`], for a site in another function.
fn refused_by(function: &str, line: u32, message: String) -> String {
    deepmsg_cnc::error_log::compose_description(
        libc::EINVAL,
        function,
        RECEIVE_ENDPOINT_FILE,
        line,
        &message,
    )
}

/// `aeron_receive_channel_endpoint_validate_so_rcvbuf`
/// (`media/aeron_receive_channel_endpoint.c:953-982`).
///
/// The one message in this module that names an environment variable: the
/// reader is being told which knob to turn, and the knob's name is part of the
/// sentence. Which is also why the two arms differ by a suffix rather than by
/// a sentence — `(OS Default)` is the reference's own distinction between a
/// buffer the reader configured and one the kernel handed out.
///
/// # Errors
///
/// The reference's message, verbatim.
fn validate_so_rcvbuf(
    socket_rcvbuf: usize,
    value: usize,
    subject: &str,
    os_default_socket_rcvbuf: usize,
) -> Result<(), String> {
    let (limit, suffix, line) = if socket_rcvbuf != 0 {
        (socket_rcvbuf, "", 961)
    } else {
        (os_default_socket_rcvbuf, " (OS Default)", 972)
    };

    if limit >= value {
        return Ok(());
    }

    Err(refused_so_rcvbuf(line, subject, value, limit, suffix))
}

/// `aeron_publication_params_validate_mtu` (`uri/aeron_driver_uri.c:555-568`).
fn validate_mtu(socket_sndbuf: usize, mtu_length: usize, name: &str) -> Result<(), String> {
    if socket_sndbuf < mtu_length {
        return Err(format!(
            "MTU greater than SO_SNDBUF for {name}: mtu={mtu_length} so-sndbuf={socket_sndbuf}"
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const CHANNEL: &[u8] = b"aeron:udp?endpoint=localhost:9999|so-sndbuf=65536";
    const EXISTING: &[u8] = b"aeron:udp?endpoint=localhost:9999|so-sndbuf=131072";

    #[test]
    fn a_channel_that_named_no_buffer_length_agrees_with_any() {
        // Naming nothing means "whatever this endpoint has", so it is never a
        // mismatch — which is what makes `so-sndbuf=` optional on every channel
        // but the first.
        assert_eq!(
            Ok(()),
            validate_channel_buffer_length("so-sndbuf", 0, 131_072, CHANNEL, EXISTING)
        );
        assert_eq!(
            Ok(()),
            validate_channel_buffer_length("so-sndbuf", 131_072, 131_072, CHANNEL, EXISTING)
        );
    }

    #[test]
    fn a_channel_that_named_a_different_buffer_length_is_refused_by_name() {
        let refused =
            validate_channel_buffer_length("so-sndbuf", 65_536, 131_072, CHANNEL, EXISTING)
                .expect_err("a mismatch");

        assert_eq!(
            "so-sndbuf=65536 does not match existing value of 131072: \
             existingChannel=aeron:udp?endpoint=localhost:9999|so-sndbuf=131072 \
             channel=aeron:udp?endpoint=localhost:9999|so-sndbuf=65536",
            refused
        );
    }

    #[test]
    fn an_existing_value_of_zero_is_the_os_default_and_says_so() {
        // The endpoint is on whatever the kernel gave it, which is a different
        // sentence from a number and the same refusal.
        let refused = validate_channel_buffer_length("so-rcvbuf", 65_536, 0, CHANNEL, EXISTING)
            .expect_err("a mismatch");

        assert!(
            refused.contains("does not match existing value of OS default:"),
            "{refused}"
        );
    }

    #[test]
    fn a_window_that_fits_the_receive_buffer_is_accepted() {
        assert_eq!(
            Ok(()),
            validate_initial_window_for_rcvbuf(65_536, 131_072, 0, CHANNEL, None)
        );
        assert_eq!(
            Ok(()),
            validate_initial_window_for_rcvbuf(131_072, 131_072, 0, CHANNEL, None),
            "exactly the buffer is not greater than it"
        );
    }

    #[test]
    fn a_window_larger_than_the_receive_buffer_names_which_buffer_it_measured() {
        let named = validate_initial_window_for_rcvbuf(131_072, 65_536, 0, CHANNEL, None)
            .expect_err("too large");
        assert_eq!(
            "Initial window greater than SO_RCVBUF for channel: rcv-wnd=131072 \
             so-rcvbuf=65536 channel=aeron:udp?endpoint=localhost:9999|so-sndbuf=65536",
            named
        );

        // With no endpoint buffer of its own it is the kernel's default being
        // measured, and the message says which — the distinction a reader needs
        // to know whose number was too small.
        let by_default = validate_initial_window_for_rcvbuf(131_072, 0, 65_536, CHANNEL, None)
            .expect_err("too large");
        assert!(by_default.contains("(OS default)"), "{by_default}");

        // And an existing channel, when there is one, is named: the reader has
        // two channels to look at and has to know which is which.
        let with_existing =
            validate_initial_window_for_rcvbuf(131_072, 65_536, 0, CHANNEL, Some(EXISTING))
                .expect_err("too large");
        assert!(
            with_existing.contains("existingChannel="),
            "{with_existing}"
        );
    }

    #[test]
    fn the_mtu_is_measured_against_the_first_buffer_that_exists_and_named() {
        // Nothing anywhere: there is no buffer to measure against.
        assert_eq!(Ok(()), validate_mtu_for_sndbuf(1408, 0, 0, 0, 0));

        // Each of the four on its own, to pin the precedence *and* the name.
        assert_eq!(Ok(()), validate_mtu_for_sndbuf(1408, 4096, 0, 0, 0));
        assert_eq!(Ok(()), validate_mtu_for_sndbuf(1408, 0, 4096, 0, 0));
        assert_eq!(Ok(()), validate_mtu_for_sndbuf(1408, 0, 0, 4096, 0));
        assert_eq!(Ok(()), validate_mtu_for_sndbuf(1408, 0, 0, 0, 4096));

        // The endpoint outranks everything: an MTU that fits it passes even
        // where a lower-precedence buffer is smaller.
        assert_eq!(
            Ok(()),
            validate_mtu_for_sndbuf(1408, 4096, 1024, 1024, 1024)
        );

        // And the message names the one that was used, which is the only way a
        // reader can tell a channel's own `so-sndbuf` from the driver's.
        for (args, name) in [
            ((1408, 1024, 0, 0, 0), "endpoint"),
            ((1408, 0, 1024, 0, 0), "channel"),
            ((1408, 0, 0, 1024, 0), "context"),
            ((1408, 0, 0, 0, 1024), "os default"),
        ] {
            let refused = validate_mtu_for_sndbuf(args.0, args.1, args.2, args.3, args.4)
                .expect_err("too large");
            assert_eq!(
                format!("MTU greater than SO_SNDBUF for {name}: mtu=1408 so-sndbuf=1024"),
                refused
            );
        }
    }

    #[test]
    fn a_sender_mtu_the_receiver_can_serve_is_accepted() {
        // The ordinary case: the driver's own 1408 against its own 128k window
        // and a socket that has one.
        assert_eq!(
            Ok(()),
            validate_sender_mtu_length(1408, 131_072, 131_072, 0)
        );

        // A receiver that never set a buffer falls to the kernel's, which is
        // the arm `(OS Default)` names.
        assert_eq!(
            Ok(()),
            validate_sender_mtu_length(1408, 131_072, 0, 131_072)
        );
    }

    /// The first line of a refusal from `validate_sender_mtu_length`, which is
    /// what the reference's `AERON_SET_ERR` leaves in the thread's buffer: the
    /// code it was handed, then the OS's text for that errno, then the site
    /// and the message. Written out here rather than composed by the test, so
    /// that a change to the composition fails this too.
    fn mtu_refusal(line: u32, message: &str) -> String {
        format!(
            "(22) Invalid argument\n\
             [aeron_receiver_channel_endpoint_validate_sender_mtu_length, \
             aeron_receive_channel_endpoint.c:{line}] {message}\n"
        )
    }

    #[test]
    fn a_sender_mtu_that_does_not_fit_the_receiver_window_is_refused_by_name() {
        // `ChannelValidationTest`'s case. Spelled out in full because this one
        // is the line the interop suite compares against a live driver's own
        // entry, byte for byte — the file name, the line number and the OS's
        // wording are each something a careful reading gets almost right.
        let refused = validate_sender_mtu_length(1408, 1376, 131_072, 0).expect_err("too large");

        assert_eq!(
            "(22) Invalid argument\n\
             [aeron_receiver_channel_endpoint_validate_sender_mtu_length, \
             aeron_receive_channel_endpoint.c:1022] mtuLength=1408 > initialWindowLength=1376\n",
            refused
        );
    }

    #[test]
    fn the_four_arithmetic_rules_each_say_which_one_they_are() {
        // Below the header, above the payload ceiling, unaligned, and the
        // window — in the order the reference tries them, so a value that
        // breaks two rules is refused by the first.
        // Each rule has its own site, because each has its own `AERON_SET_ERR`
        // — and the site is the macro's *own* line, not the statement's.
        for (args, line, expected) in [
            (
                (16, 131_072, 131_072, 0),
                992,
                "mtuLength=16 < DATA_HEADER_LENGTH=32",
            ),
            (
                (65_536, 131_072, 131_072, 0),
                1002,
                "mtuLength=65536 > MAX_UDP_PAYLOAD_LENGTH=65504",
            ),
            (
                (1409, 131_072, 131_072, 0),
                1012,
                "mtuLength=1409 must be a multiple of FRAME_ALIGNMENT=32",
            ),
            (
                (1408, 1376, 131_072, 0),
                1022,
                "mtuLength=1408 > initialWindowLength=1376",
            ),
        ] {
            assert_eq!(
                mtu_refusal(line, expected),
                validate_sender_mtu_length(args.0, args.1, args.2, args.3).expect_err("refused")
            );
        }

        // The order, pinned: 16 is below the header *and* unaligned, and the
        // header is what it is refused for.
        assert!(
            validate_sender_mtu_length(16, 1376, 131_072, 0)
                .expect_err("refused")
                .contains("DATA_HEADER_LENGTH"),
            "the first rule that fires is the one that speaks"
        );
    }

    #[test]
    fn a_window_larger_than_the_receive_buffer_is_refused_by_name() {
        // The socket rule, which is about what the pipeline can hold rather
        // than about arithmetic — and which names what it measured, because a
        // reader has two buffers to look at.
        // The site is `aeron_receive_channel_endpoint_validate_so_rcvbuf`'s,
        // not the caller's: the reference raises from the helper.
        let window = validate_sender_mtu_length(1408, 131_072, 65_536, 0).expect_err("too large");
        assert_eq!(
            "(22) Invalid argument\n\
             [aeron_receive_channel_endpoint_validate_so_rcvbuf, \
             aeron_receive_channel_endpoint.c:961] Max Window length greater than socket SO_RCVBUF, \
             increase 'AERON_RCV_INITIAL_WINDOW_LENGTH' to match window: \
             value=131072, SO_RCVBUF=65536\n",
            window
        );

        // With no buffer of its own the same sentence carries the reference's
        // suffix — the reader's only clue that the number is the kernel's
        // rather than theirs — and the site moves to the other arm.
        let by_default =
            validate_sender_mtu_length(1408, 131_072, 0, 65_536).expect_err("too large");
        assert!(
            by_default.contains("aeron_receive_channel_endpoint.c:972]"),
            "{by_default}"
        );
        assert!(
            by_default.ends_with("SO_RCVBUF=65536 (OS Default)\n"),
            "{by_default}"
        );
    }

    #[test]
    fn the_sender_mtu_against_the_socket_buffer_can_never_fire() {
        // Not a rule with an awkward case: a rule with no case at all. It sits
        // after the window rule, which has already established `mtu <=
        // window`, and after the window-against-the-socket rule, which has
        // established `socket >= window`. Together those give `socket >= mtu`,
        // and the rule asks for `socket < mtu`.
        //
        // Swept rather than argued, because this is the kind of argument that
        // is easy to get wrong in the same direction twice: every combination
        // below either passes or is refused by an *earlier* rule.
        for mtu in [32, 1408, 4096] {
            for window in [0, 32, 1408, 4096, 131_072] {
                for socket in [0, 32, 1408, 4096, 131_072] {
                    if let Err(recorded) = validate_sender_mtu_length(mtu, window, socket, 131_072)
                    {
                        // Read off the *message*, which is what follows the
                        // site's `] ` — not off the start of the string, which
                        // is now the composition's `(22) Invalid argument`.
                        // Checking the prefix would have gone quietly vacuous
                        // the moment the sites arrived.
                        assert!(
                            !recorded.contains("] Sender MTU greater"),
                            "mtu={mtu} window={window} socket={socket}: {recorded}"
                        );
                    }
                }
            }
        }
    }
}
