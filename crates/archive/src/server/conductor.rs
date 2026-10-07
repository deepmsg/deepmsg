//! The archive's conductor.
//!
//! The reference's `ArchiveConductor` is a `SessionWorker` — an agent whose
//! `doWork` runs the adapter's poll, then the sessions, then the pieces this
//! slice does not have (`ArchiveConductor.java:364-396`, `:125-127`). What is
//! here is the first piece of it: the channel an archive answers a client on,
//! which the conductor **derives** rather than chooses.
//!
//! # The channel is rebuilt, not taken
//!
//! A client asks to be answered on a channel of its own choosing, and the
//! archive does not use it as given. It is **stripped down to a fixed list of
//! parameters and written out again** (`strippedChannelBuilder`,
//! `ArchiveConductor.java:1880-1905`) with three parameters overridden from
//! the archive's own control settings, and — when the client asked for a
//! response channel — one more added: the correlation id of the image the
//! request arrived on (`:458-481`).
//!
//! The list is not the same as "the parameters that were there". Read the
//! probe below: `term-length`, `mtu` and `sparse` in the client's channel are
//! **dropped**, because the archive sets those itself; `nak-delay` and
//! everything unnamed are dropped too. What survives is what a channel is
//! *identified* by, which is exactly the set a second archive can be told to
//! reproduce.
//!
//! # What was checked against, rather than reasoned about
//!
//! The strip list is a list of twenty-three calls, and the quickest way to be
//! wrong about it is to reason about which ones matter. It was read off the
//! reference instead: `aeron-archive-1.53.2.jar` is built in the sibling
//! checkout, `strippedChannelBuilder` is reachable by reflection, and the
//! outputs are in the test at the bottom of this file.
//!
//! Every parameter is read into the type its field has and written back out,
//! which is what the reference does — `ttl=007` comes back as `ttl=7` and
//! `so-sndbuf=1m` comes back as `1m`, and both are in the test below. What
//! remains different is only the failure: a value that will not read at all,
//! `ttl=seven`, makes the reference's setter throw and takes the archive with
//! it, where this leaves the parameter out. Dying is not obviously the better
//! answer, but it is the reference's, and a channel is the thing that differs
//! when the two choose differently.

use deepmsg_core::uri::{ChannelUri, ChannelUriStringBuilder, UriError, parse_size};

/// `AeronArchive.Configuration.CONTROL_MODE_RESPONSE` — the `control-mode` a
/// client writes when it wants a channel of its own to be answered on
/// (`ArchiveConductor.java:469`).
pub const CONTROL_MODE_RESPONSE: &str = "response";

/// What the archive's own control settings contribute to a response channel
/// (`ArchiveConductor.java:460-474`, falling back to `ctx.control*`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResponseChannelDefaults {
    /// `control.term.buffer.length`.
    pub term_buffer_length: i32,
    /// `control.term.buffer.sparse`.
    pub term_buffer_sparse: bool,
    /// `control.mtu.length`, or `None` for the driver's own — the reference's
    /// `controlMtuLength` is an `int` and this build's is an `Option`, so an
    /// archive that has not set one leaves it out rather than writing a zero.
    pub mtu_length: Option<i32>,
}

/// `strippedChannelBuilder` (`ArchiveConductor.java:1880-1905`): a builder
/// carrying the parameters an archive keeps from a client's channel.
///
/// Every `.xxx(channelUri)` call in the reference is one line here, in the same
/// order, and the ones it makes are the whole of the list. A parameter the
/// reference does not copy is one this does not copy — the omission is the
/// behaviour, not an oversight.
pub fn stripped_channel_builder(uri: &ChannelUri) -> ChannelUriStringBuilder {
    let mut builder = ChannelUriStringBuilder::default();

    // `media(channelUri)` is the transport, which is not a parameter.
    builder.media(uri.media());

    copy_text(&mut builder, uri, "tags");
    copy_text(&mut builder, uri, "endpoint");
    copy_text(&mut builder, uri, "interface");
    copy_text(&mut builder, uri, "control");
    copy_text(&mut builder, uri, "control-mode");
    copy_text(&mut builder, uri, "gtag");
    copy_text(&mut builder, uri, "tether");
    copy_text(&mut builder, uri, "group");
    copy_text(&mut builder, uri, "rejoin");
    copy_text(&mut builder, uri, "fc");
    copy_text(&mut builder, uri, "cc");
    copy_text(&mut builder, uri, "so-rcvbuf");
    copy_text(&mut builder, uri, "so-sndbuf");
    copy_text(&mut builder, uri, "rcv-wnd");
    copy_text(&mut builder, uri, "channel-snd-ts-offset");
    copy_text(&mut builder, uri, "channel-rcv-ts-offset");
    copy_text(&mut builder, uri, "media-rcv-ts-offset");
    copy_text(&mut builder, uri, "session-id");
    copy_text(&mut builder, uri, "alias");
    copy_text(&mut builder, uri, "response-correlation-id");
    copy_text(&mut builder, uri, "response-endpoint");
    copy_text(&mut builder, uri, "ttl");

    builder
}

/// The channel a session is answered on (`ArchiveConductor.java:458-481`).
///
/// The client's channel, stripped, with the archive's own term length, sparse
/// flag and MTU written over whatever the client asked for; and, when the
/// client asked for a response channel, the correlation id of the image the
/// request arrived on. That last one is what lets a client tell an answer
/// meant for it from one meant for another subscription on the same endpoint.
///
/// # Errors
///
/// [`UriError`] if the client's channel cannot be read. The reference throws
/// out of `ChannelUri.parse` here, which takes the archive with it; this
/// answers with the reason and leaves the decision to the caller.
pub fn response_channel(
    requested: &str,
    image_correlation_id: i64,
    defaults: &ResponseChannelDefaults,
) -> Result<String, UriError> {
    let uri = ChannelUri::parse(requested)?;
    let mut builder = stripped_channel_builder(&uri);

    builder
        .term_length(defaults.term_buffer_length)
        .sparse(defaults.term_buffer_sparse);

    if let Some(mtu) = defaults.mtu_length {
        builder.mtu(mtu);
    }

    if uri.get("control-mode") == Some(CONTROL_MODE_RESPONSE) {
        builder.response_correlation_id(image_correlation_id.to_string());
    }

    Ok(builder.build())
}

/// Copy a parameter's text into the matching field, if the channel carries it.
///
/// The reference has a typed overload per parameter and re-spells the value on
/// the way through — see the module note for the one place this differs. What
/// the name-to-field mapping has to get right is only which fields exist.
fn copy_text(builder: &mut ChannelUriStringBuilder, uri: &ChannelUri, name: &str) {
    let Some(value) = uri.get(name) else {
        return;
    };

    match name {
        "tags" => {
            builder.tags(value);
        }
        "endpoint" => {
            builder.endpoint(value);
        }
        "interface" => {
            builder.network_interface(value);
        }
        "control" => {
            builder.control_endpoint(value);
        }
        "control-mode" => {
            builder.control_mode(value);
        }
        "fc" => {
            builder.flow_control(value);
        }
        "cc" => {
            builder.congestion_control(value);
        }
        "alias" => {
            builder.alias(value);
        }
        "response-correlation-id" => {
            builder.response_correlation_id(value);
        }
        "response-endpoint" => {
            builder.response_endpoint(value);
        }
        "channel-snd-ts-offset" => {
            builder.channel_send_timestamp_offset(value);
        }
        "channel-rcv-ts-offset" => {
            builder.channel_receive_timestamp_offset(value);
        }
        "media-rcv-ts-offset" => {
            builder.media_receive_timestamp_offset(value);
        }
        // The numeric ones whose field is a number rather than text: a value
        // that will not read as one is not a value this builder can carry, and
        // the reference's own setters throw where this skips.
        "ttl" => {
            if let Ok(ttl) = value.parse() {
                builder.ttl(ttl);
            }
        }
        "gtag" => {
            if let Ok(tag) = value.parse() {
                builder.group_tag(tag);
            }
        }
        "session-id" => {
            if let Ok(session_id) = value.parse() {
                builder.session_id(session_id);
            }
        }
        // Sizes, which are read and re-spelled rather than copied:
        // `so-sndbuf=2048` comes back as `2k` in both, because both write
        // through `format_size`.
        "so-rcvbuf" => {
            if let Some(size) = parse_size(value) {
                builder.socket_rcvbuf_length(size);
            }
        }
        "so-sndbuf" => {
            if let Some(size) = parse_size(value) {
                builder.socket_sndbuf_length(size);
            }
        }
        "rcv-wnd" => {
            if let Some(size) = parse_size(value) {
                builder.receiver_window_length(size);
            }
        }
        // Flags. Their fields are `bool`, so the text has to read as one; the
        // reference's `Boolean.valueOf` is false for anything that is not
        // "true", which is what the fallback here matches.
        "tether" => {
            builder.tether(value.eq_ignore_ascii_case("true"));
        }
        "group" => {
            builder.group(value.eq_ignore_ascii_case("true"));
        }
        "rejoin" => {
            builder.rejoin(value.eq_ignore_ascii_case("true"));
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// What the archive's own control settings are for these tests, and the
    /// correlation id of the image a request arrived on.
    const DEFAULTS: ResponseChannelDefaults = ResponseChannelDefaults {
        term_buffer_length: 65536,
        term_buffer_sparse: false,
        mtu_length: Some(1408),
    };
    const IMAGE_CORRELATION_ID: i64 = 7;

    fn derive(requested: &str) -> String {
        response_channel(requested, IMAGE_CORRELATION_ID, &DEFAULTS).expect("a readable channel")
    }

    /// The strip list, read off the reference's own `strippedChannelBuilder` by
    /// reflection rather than reasoned about — every expected value below is
    /// what `aeron-archive-1.53.2.jar` printed.
    ///
    /// `term-length`, `mtu` and `sparse` are the interesting ones: a client
    /// that asked for them does **not** get them back, because the archive sets
    /// those from its own control settings and the strip list does not carry
    /// them. `nak-delay` and any parameter the archive has never heard of go
    /// the same way.
    #[test]
    fn the_strip_list_is_the_references() {
        assert_eq!(
            "aeron:udp?endpoint=localhost:8010",
            stripped_channel_builder(
                &ChannelUri::parse("aeron:udp?endpoint=localhost:8010").unwrap()
            )
            .build()
        );

        assert_eq!(
            "aeron:udp?endpoint=localhost:8010",
            stripped_channel_builder(
                &ChannelUri::parse(
                    "aeron:udp?endpoint=localhost:8010|term-length=64k|mtu=1408|sparse=true"
                )
                .unwrap()
            )
            .build(),
            "the three the archive sets itself are not kept"
        );

        assert_eq!(
            "aeron:udp?tags=1,2|endpoint=localhost:1|ttl=7|tether=true",
            stripped_channel_builder(
                &ChannelUri::parse("aeron:udp?endpoint=localhost:1|tags=1,2|tether=true|ttl=007")
                    .unwrap()
            )
            .build()
        );

        assert_eq!(
            "aeron:udp?endpoint=localhost:1|so-sndbuf=1m",
            stripped_channel_builder(
                &ChannelUri::parse(
                    "aeron:udp?endpoint=localhost:1|so-sndbuf=1m|nak-delay=5ms|unknown-param=x"
                )
                .unwrap()
            )
            .build()
        );
    }

    /// A plain control channel is answered on the channel it asked for, with
    /// the archive's own three parameters written over it.
    #[test]
    fn a_control_channel_gets_the_archives_own_three_parameters() {
        assert_eq!(
            "aeron:udp?endpoint=localhost:8010|mtu=1408|term-length=64k|sparse=false",
            derive("aeron:udp?endpoint=localhost:8010")
        );
    }

    /// A response channel carries the image's correlation id, and nothing else
    /// changes: that is the whole difference between the two.
    #[test]
    fn a_response_channel_carries_the_images_correlation_id() {
        assert_eq!(
            "aeron:udp?control=localhost:9090|control-mode=response|mtu=1408|term-length=64k\
             |sparse=false|response-correlation-id=7",
            derive("aeron:udp?control=localhost:9090|control-mode=response")
        );
    }

    /// The correlation id is a property of the *image*, not of the request:
    /// two clients on the same control channel get different response
    /// channels, which is what keeps their answers apart.
    #[test]
    fn two_images_on_one_channel_get_two_response_channels() {
        let first = response_channel(
            "aeron:udp?control=localhost:9090|control-mode=response",
            7,
            &DEFAULTS,
        )
        .unwrap();
        let second = response_channel(
            "aeron:udp?control=localhost:9090|control-mode=response",
            8,
            &DEFAULTS,
        )
        .unwrap();

        assert_ne!(first, second);
    }

    #[test]
    fn ipc_is_a_channel_too() {
        assert_eq!(
            "aeron:ipc?mtu=1408|term-length=64k|sparse=false",
            derive("aeron:ipc")
        );
    }

    /// An archive that has set no MTU does not write one, rather than writing
    /// the zero its `Option` would be.
    #[test]
    fn an_unset_mtu_is_left_out() {
        let defaults = ResponseChannelDefaults {
            mtu_length: None,
            ..DEFAULTS
        };

        assert_eq!(
            "aeron:ipc?term-length=64k|sparse=false",
            response_channel("aeron:ipc", IMAGE_CORRELATION_ID, &defaults).unwrap()
        );
    }

    #[test]
    fn a_channel_that_will_not_read_is_an_error() {
        assert_eq!(
            Err(UriError::InvalidScheme),
            response_channel("udp?endpoint=localhost:1", IMAGE_CORRELATION_ID, &DEFAULTS)
        );
    }
}
