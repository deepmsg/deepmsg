//! The transport-facing half of the rig: the channel settings it is given, the
//! senders that put a batch into a publication, and the nodes and transceivers
//! that sit at either end of one: the echo, the archive node that **forwards into
//! a recording** rather than answering, and (to come) the transceivers that read
//! a replay back.
//!
//! Everything here is a port of the reference's `benchmarks-aeron` module. The
//! *property names* it reads (`io.aeron.benchmarks.aeron.*`) are kept verbatim:
//! they are the interface a run is described through, and both sides of a
//! comparison are handed the same ones.

pub mod archive_node;
pub mod echo;
pub mod node;
pub mod sender;
pub mod util;
