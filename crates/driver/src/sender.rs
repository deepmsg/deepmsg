//! Sender data-plane thread.
//!
//! P1 scope, mirroring `aeron-driver/src/main/c/aeron_sender.c` (M10):
//!
//! - flywheel duty cycle over network publications,
//! - sendmmsg batching: one frame per datagram (batch the syscall, not the
//!   messages); default 4, hard cap 16,
//! - heartbeats as zero-length DATA frames (BEGIN|END),
//! - retransmit serving through the retransmit handler.
