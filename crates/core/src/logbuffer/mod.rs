//! Term buffer framing shared by client and driver.
//!
//! A log buffer is three rotating terms behind one metadata block, and it is
//! the reason Aeron is fast: for an IPC channel the publisher and the
//! subscriber share one of these, and the payload never passes through the
//! driver.
//!
//! Mirroring `aeron-client/src/main/c/concurrent/aeron_logbuffer_descriptor.{c,h}`
//! and `aeron-client/src/main/c/protocol/aeron_udp_protocol.h`.
//!
//! - [`descriptor`] — the metadata block: every field's offset, and the two
//!   lengths that are easy to conflate.
//! - [`position`]   — `RawTail` and `Position`, and the arithmetic between them.
//! - [`frame`]      — the frame header, the data header, and the types and
//!   flags a term can hold.
//! - [`append`]     — claiming space in a term, writing a frame, the padding
//!   frame, and rotation.
//! - [`scan`]       — walking frames as a reader: skipping padding, and
//!   stopping at a frame that is not yet ready.
//! - [`logfile`]    — creating and removing the file those three terms live in.
//! - [`repair`]     — the rebuilder, the gap scanner, the gap filler, and the
//!   unblocker.
//!
//! # The repair paths are here rather than in the driver
//!
//! An earlier version of this note said they belonged to `deepmsg-driver`. In
//! the reference they live under `aeron-client/src/main/c/concurrent/`, shared
//! by both sides, and keeping them beside the layout they manipulate means one
//! module owns the term buffer rather than two with a migration owed between
//! them.
//!
//! # What this module is not
//!
//! Publications, subscriptions and images — the things that *own* a log buffer
//! and decide when to append — are elsewhere, as is mapping the log file from
//! an `ON_AVAILABLE_IMAGE` and the flow-control counter a producer reads. This
//! module is the layout and the operations on it, and nothing that needs a
//! driver to exist.

pub mod append;
pub mod descriptor;
pub mod frame;
pub mod logfile;
pub mod position;
pub mod repair;
pub mod scan;
pub mod unblocker;
