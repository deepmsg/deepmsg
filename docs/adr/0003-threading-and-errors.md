# ADR-0003: Threading model and error semantics

- Status: accepted (2026-09-25)

## Context

Aeron's execution model is dedicated threads with busy/idle strategies and
lock-free hand-off. The prior Rust port (aeron-rs) made three choices worth
reversing: it models backpressure/admin-action as `Result::Err`, it panics on
unknown driver events, and its default error handler panics. The reference C
client treats offer outcomes as states, tolerates unknown events, and reports
errors through a callback without killing the process.

## Decision

- No async runtime in-tree. Threads plus idle strategies, mirroring the
  reference agent model. An async adapter, if ever needed, is a separate
  crate built on top of the client.
- Offer/poll outcomes are typed enums, not `Result`: `BackPressured`,
  `AdminAction`, `NotConnected`, `MaxPositionExceeded` are states the caller
  is expected to act on, not failures.
- Unknown events arriving from the driver are skipped (and counted), never
  panicked on — this is what keeps minor CnC bumps forward-compatible.
- The default error handler logs and keeps the process alive, matching the C
  client's semantics; termination-on-error is an explicit opt-in.

## Consequences

- Hot paths stay allocation-free and runtime-free.
- Callers cannot accidentally treat flow control as failure.
- Mixed deployments with the reference driver stay viable across upgrades.
