//! UDP transport layer and name resolution.
//!
//! P1 scope, mirroring `aeron-driver/src/main/c/media/` (M12):
//!
//! - dual-fd transports for multicast+connect (receive fd / send fd),
//! - send strategies: connected single-frame send, sendmmsg with one frame
//!   per datagram, sendmsg loop fallback,
//! - the transport-bindings seam (a vtable in the reference, reserved for
//!   kernel-bypass implementations) — modelled as a Rust trait from day one,
//! - the per-agent interceptor chain (head = last env entry),
//! - the resolver: RES-frame gossip on the duty cycle.
//!
//! A `sys` submodule will host the raw syscall shims (sendmmsg/recvmmsg)
//! and is the one driver-internal zone where `unsafe` is permitted
//! (ADR-0002).
