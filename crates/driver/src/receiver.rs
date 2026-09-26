//! Receiver data-plane thread.
//!
//! P1 scope, mirroring `aeron-driver/src/main/c/aeron_receiver.c` (M11):
//!
//! - poll strategy threshold: linear recvmmsg path vs epoll/poll path
//!   (the reference switches at 5 concurrent transports),
//! - image creation/retirement; SM generation is split between conductor
//!   (three-condition decision) and receiver (per-duty-cycle sweep),
//! - NAK multicast suppression with exponentially distributed random delay,
//! - receive-side congestion control (receiver window carried in SM).
