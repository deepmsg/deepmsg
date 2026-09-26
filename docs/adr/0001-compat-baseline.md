# ADR-0001: Compatibility baseline is Aeron 1.53.2

- Status: accepted (2026-09-25)

## Context

deepmsg is a from-scratch Rust rewrite of the open-source Aeron stack. The
rewrite is backed by a full source analysis of Aeron 1.53.2. The prior
pure-Rust port (aeron-rs) pinned CnC version 0.0.16
and is structurally capped at driver 1.44; we start fresh and do not inherit
that ceiling.

Byte compatibility buys three things for this project: an authoritative
oracle to diff against (the reference driver and the Java tools), the option
to mix components during migration, and hard, testable contracts instead of
prose.

## Decision

Target byte-level compatibility with Aeron 1.53.2 on:

- the CnC file: layout, table of contents, semantic version 0.2.0
- the UDP wire protocol: data/setup/status/NAK frames, RTTM
- SBE schemas: ids and versions as recorded in `docs/compat.md`
- archive on-disk formats: `recording.log`, catalog, segment files

"Compatible" means proven by the interop suite (feature `interop`) against
the reference implementation, not asserted.

## Consequences

- Layout constants are frozen; protocol modernisation is out of scope.
- Upstream bumps are a deliberate act: update `schemas/`, commit the diff,
  extend `docs/compat.md`, extend the interop suite.
- On-disk formats (e.g. the hand-rolled `recording.log`) are replicated even
  where a Rust redesign would be cleaner.
