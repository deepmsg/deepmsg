//! Channel URI handling (`aeron:udp?...`, `aeron:ipc`, `aeron-spy:`).
//!
//! Reference behaviour that must carry over (M06):
//!
//! - the client core never parses URIs: parsing is a driver concern; a
//!   client-side string layer (in the spirit of the C++
//!   `ChannelUri`/`ChannelUriStringBuilder`) is provided as a convenience
//!   for higher-level clients such as archive,
//! - only the recognised keys are interpreted; unknown keys are preserved
//!   verbatim so the canonical form round-trips,
//! - the canonical channel identity uses the raw parameter text.
//!
//! Reference: `aeron-client/src/main/c/util/aeron_uri.c`,
//! `aeron-client/src/main/c/media/aeron_udp_channel.c`.
