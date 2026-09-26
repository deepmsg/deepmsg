# ADR-0002: Unsafe policy

- Status: accepted (2026-09-25)

## Context

The system's core value is correct shared-memory concurrency between
processes (CnC file, term buffers). That requires raw atomic access to
memory-mapped bytes, which Rust cannot express without `unsafe`. Everything
else — protocol state machines, URI handling, codecs, orchestration — is
ordinary safe code.

## Decision

`unsafe` is allowed in exactly four zones:

1. `deepmsg-core::pal` — the platform seam: raw `mmap` / `munmap`, and the
   other syscalls that have no safe Rust spelling as they arrive. Calls
   `libc`, which is FFI and permitted by README principle 2.
2. `deepmsg-core::buffer` — atomic views over shared memory,
3. `deepmsg-cnc` — CnC file layout access,
4. `deepmsg-driver` media syscall shim (`sys`, when it appears: sendmmsg /
   recvmmsg and friends) — under an explicit, documented `#[allow]`.

`pal` and `buffer` are separate zones because they sit on different axes:
`pal` is platform-specific address-space bookkeeping, one implementation per
OS, while `buffer` is platform-neutral typed access over a pointer and a
length. Keeping them apart is what lets `buffer` be exercised under miri over
an owned `Box<[u8]>`, and it confines the unsafe a reviewer must check against
a second platform to one small module.

Every other crate declares `#![forbid(unsafe_code)]`; `deepmsg-driver`
denies it at crate root so the shim's allow stays visible in review.

Within the zones:

- `unsafe_op_in_unsafe_fn` is denied workspace-wide.
- clippy `undocumented_unsafe_blocks` is denied: every block carries a
  `// SAFETY:` comment stating the invariant (alignment, exclusivity,
  lifetime) and the memory-ordering rationale — not just "needed".
- Concurrent structures get loom tests (feature-gated) and a miri pass once
  they exist (P0). Neither tool can reach a mapping that another process
  writes: miri does not implement `mmap`, and loom models in-process
  interleavings only. The dischargeable targets are the typed access layer
  over an owned buffer and any ring this process owns outright; a reader of
  foreign shared memory is covered by golden byte fixtures and the interop
  suite instead.

## Consequences

- The unsafe review surface is four named modules, not the whole repo.
- Zero-unsafe crates can be refactored freely.
- New unsafe needs an ADR amendment, which is the point.

## Amendment history

- **2026-09-26** — added zone 1, `deepmsg-core::pal`. The original three zones
  took the mapping as already given; P0-a needed the `mmap` call itself, and
  folding it into `buffer` to avoid an amendment is precisely what the last
  consequence above exists to prevent. Recorded alongside it: FFI through
  `libc` is permitted (README principle 2), the mapping is
  `PROT_READ | MAP_SHARED` because a reader must observe the driver's later
  writes, and the write half of the accessor set stays unwritten until
  something actually writes to a CnC file.
