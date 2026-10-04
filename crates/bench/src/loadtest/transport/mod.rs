//! The transport-facing half of the rig: the channel settings it is given, the
//! senders that put a batch into a publication, and — to come — the transceivers
//! that echo and the node that answers them.
//!
//! Everything here is a port of the reference's `benchmarks-aeron` module. The
//! *property names* it reads (`io.aeron.benchmarks.aeron.*`) are kept verbatim:
//! they are the interface a run is described through, and both sides of a
//! comparison are handed the same ones.

pub mod sender;
pub mod util;
