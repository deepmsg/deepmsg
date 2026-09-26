//! Term buffer framing shared by client and driver.
//!
//! Planned (P0), mirroring `aeron-client/src/main/c/logbuffer/`:
//!
//! - `frame`      — frame descriptor layout: length, version, type, flags,
//!   body offset (`aeron_frame_descriptor.h`, `aeron_data_header.h`)
//! - `descriptor` — log-buffer metadata: term counts, tail counters, active
//!   term/index math (`aeron_log_buffer_descriptor.h`)
//! - `scanner`    — fragment scanning over committed ranges
//!   (`aeron_term_fragment_scanner.c`)
//!
//! Driver-side rebuilder / gap-filler logic lives in `deepmsg-driver`
//! (loss handling), not here.
