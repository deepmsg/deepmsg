//! Term buffer framing shared by client and driver.
//!
//! Planned (P0), mirroring `aeron-client/src/main/c/concurrent/` and
//! `aeron-client/src/main/c/protocol/aeron_udp_protocol.h`:
//!
//! - `frame`      — frame descriptor layout: length, version, type, flags,
//!   body offset (`aeron_frame_header_t`, `aeron_data_header_t`, both in
//!   `protocol/aeron_udp_protocol.h`)
//! - `descriptor` — log-buffer metadata: term counts, tail counters, active
//!   term/index math (`concurrent/aeron_logbuffer_descriptor.h`)
//! - `scanner`    — fragment scanning over committed ranges
//!   (`concurrent/aeron_term_scanner.c`)
//!
//! Driver-side rebuilder / gap-filler logic lives in `deepmsg-driver`
//! (loss handling), not here.
