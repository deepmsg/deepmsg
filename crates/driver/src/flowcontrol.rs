//! Sender-side flow-control strategies (M10).
//!
//! P1 scope, mirroring `aeron-driver/src/main/c/aeron_flow_control.c`:
//!
//! - `max` (unicast default), `min` (multicast, with the new-receiver
//!   admission gate and setup-catchup window freeze), `tagged`,
//! - the strategy's on-SM return value is the single write point of the
//!   sender limit (`snd_lmt`),
//! - both naming schemes supported: env long names and URI `fc=` short
//!   names.
