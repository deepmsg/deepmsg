//! Driver conductor: the single-threaded control-plane core.
//!
//! P1 scope, mirroring
//! `aeron-driver/src/main/c/aeron_driver_conductor.c` and the agent loop
//! (M08 / M09):
//!
//! - command drain from the CnC many-to-one ring; the reference drains at
//!   most one command per cycle (the control-latency lower bound) and allows
//!   at most one in-flight command per pending client object,
//! - duty-cycle housekeeping: client liveness, timers, linger/unblock,
//!   managed-resource pool reclamation (the seven-pool harvest protocol),
//! - publisher limits: `pub_lmt` is written only from the conductor duty
//!   cycle (IPC trips vs network term-gap protection),
//! - stream matching and image lifecycle for subscriptions.
