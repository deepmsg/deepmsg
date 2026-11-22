#![no_main]

//! Target 01: the channel URI parser and its round-trip invariant.
//!
//! The surface is every `channel` string that reaches the driver: a client's
//! `ADD_PUBLICATION`/`ADD_SUBSCRIPTION` command, a response-channel rewrite,
//! and the archive's own control and response channels. None of them is
//! trusted, and all of them are text.
//!
//! What is asserted:
//!
//! 1. `ChannelUri::parse` never panics on valid UTF-8 — malformed `|`, `?`,
//!    empty keys, duplicate keys, and negative or overflowing `tags=` included.
//! 2. If it parses, `build()` renders a string that parses again, with every
//!    parameter, the media and the prefix equal. Parameters are an ordered
//!    `Vec`, so order is part of the invariant (a `HashMap` would lose it).
//!
//! The parser's own guarantee is that it answers `Err`, never a panic. The
//! first version of this target found `"éaeron:ipc"`: `uri[..at - 1]` cuts the
//! two-byte `é` in half. The regression test for it is in
//! `crates/core/src/uri.rs`.

use deepmsg_core::uri::ChannelUri;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };

    let Ok(uri) = ChannelUri::parse(text) else {
        return;
    };

    let rendered = uri.build();
    let reparsed = ChannelUri::parse(&rendered).expect("build() output must parse");

    assert_eq!(uri.media(), reparsed.media(), "media changed by build()");
    assert_eq!(uri.prefix(), reparsed.prefix(), "prefix changed by build()");
    assert_eq!(
        uri.params(),
        reparsed.params(),
        "parameters changed by build()"
    );
});
