# ADR-0002: Unsafe policy

- Status: accepted (2026-09-25)

## Context

The system's core value is correct shared-memory concurrency between
processes (CnC file, term buffers). That requires raw atomic access to
memory-mapped bytes, which Rust cannot express without `unsafe`. Everything
else — protocol state machines, URI handling, codecs, orchestration — is
ordinary safe code.

## Decision

`unsafe` is allowed in exactly three zones:

1. `deepmsg-core::buffer` — atomic views over shared memory,
2. `deepmsg-cnc` — CnC file layout access,
3. `deepmsg-driver` media syscall shim (`sys`, when it appears: sendmmsg /
   recvmmsg and friends) — under an explicit, documented `#[allow]`.

Every other crate declares `#![forbid(unsafe_code)]`; `deepmsg-driver`
denies it at crate root so the shim's allow stays visible in review.

Within the zones:

- `unsafe_op_in_unsafe_fn` is denied workspace-wide.
- clippy `undocumented_unsafe_blocks` is denied: every block carries a
  `// SAFETY:` comment stating the invariant (alignment, exclusivity,
  lifetime) and the memory-ordering rationale — not just "needed".
- Concurrent structures get loom tests (feature-gated) and a miri pass once
  they exist (P0).

## Consequences

- The unsafe review surface is three named modules, not the whole repo.
- Zero-unsafe crates can be refactored freely.
- New unsafe needs an ADR amendment, which is the point.
