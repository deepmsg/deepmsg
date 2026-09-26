//! Loss detection and recovery.
//!
//! P1 scope, mirroring the reference loss subsystem (M11):
//!
//! - gap scanning over the received portion of a term,
//! - the frame_length==0 single-word protocol shared by the rebuilder,
//!   gap scanner, clean_buffer_to and gap filler,
//! - NAK generation and retransmit handling.
