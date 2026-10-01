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
}
