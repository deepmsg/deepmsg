# ADR-0004: SBE code generation strategy

- Status: proposed (open; blocks the first archive/cluster codec work, not
  the P0 client)

## Context

All four layers (wire, archive/cluster control protocols, mark files,
node-state) are SBE-encoded from the five XML schemas in `schemas/`.
RealLogic ships no official Rust SBE generator. Candidates: the `sbe-rs`
crate, or a small hand-written generator over the same XML.

## Decision (proposed)

- Generated sources are checked in under `deepmsg-codec/src/generated/`.
- `build.rs` validates schema presence and emits rerun triggers; it does not
  generate. Regeneration is an explicit `just gen` step.
- Golden byte tests compare generated encoders against fixture bytes
  captured from the reference Java encoder for every message template, plus
  round-trip decode tests.

## Consequences

- Reviews can diff generated code; builds stay offline-reproducible.
- Schema drift is visible in review rather than silently absorbed.
- Picking (or writing) the generator is a bounded, isolated task.
