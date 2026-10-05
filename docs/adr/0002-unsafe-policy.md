# ADR-0002: Unsafe policy

- Status: accepted (2026-09-25)

## Context

The system's core value is correct shared-memory concurrency between
processes (CnC file, term buffers). That requires raw atomic access to
memory-mapped bytes, which Rust cannot express without `unsafe`. Everything
else — protocol state machines, URI handling, codecs, orchestration — is
ordinary safe code.

## Decision

`unsafe` is allowed in exactly five zones:

1. `deepmsg-core::pal` — the platform seam: raw `mmap` / `munmap`, and the
   other syscalls that have no safe Rust spelling as they arrive. Calls
   `libc`, which is FFI and permitted by README principle 2.
2. `deepmsg-core::buffer` — atomic views over shared memory,
3. `deepmsg-cnc` — CnC file layout access,
4. `deepmsg-driver` syscall shim (`sys`): the process's signal seam today, and
   the media syscalls (`sendmmsg` / `recvmmsg` and friends) when the data
   plane arrives — under an explicit, documented `#[allow]`.
5. `deepmsg-tools` archive shim (`archive-shim`): the process-supervision
   signal seam — catching the `SIGTERM` a test harness sends to the pid it
   spawned, and passing it to the two processes that stand where the reference
   ran one. Under the same explicit, documented `#[allow]`.

`pal` and `buffer` are separate zones because they sit on different axes:
`pal` is platform-specific address-space bookkeeping, one implementation per
OS, while `buffer` is platform-neutral typed access over a pointer and a
length. Keeping them apart is what lets `buffer` be exercised under miri over
an owned `Box<[u8]>`, and it confines the unsafe a reviewer must check against
a second platform to one small module.

Every other crate declares `#![forbid(unsafe_code)]`; `deepmsg-driver` and the
archive shim deny it at crate root so their allows stay visible in review.

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

- The unsafe review surface is five named modules, not the whole repo.
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
- **2026-10-05** — added zone 5, `archive-shim`'s supervision. The axis named
  above — *whose boundary it is* — puts this on the same boundary as zone 4:
  signals are the kernel's interface to this process either way. It is a
  separate zone because a zone is also a *named module whose contents a
  reviewer reads*, and zone 4 is the driver's own syscalls. A driver reviewer
  should not have to check a test instrument's code that no driver ever runs,
  and the shim's supervision is a second, unrelated lifetime: it exists because
  the reference's `ArchivingMediaDriver` is one process where `hybrid` needs
  two, its teardown signals one pid and waits for it
  (`aeron-archive/src/test/c/TestArchive.h:164-171`), and neither `SIGKILL` nor
  a dead parent is a substitute — the driver deletes its directory and the
  archive flushes on an orderly shutdown. Three calls, all of them the ones
  `libc` has for exactly this: `signal` to be told, `kill` to pass it on, and
  an atomic store in the handler, which is all an async-signal-safe handler may
  do. Shelling out to `/bin/sh` for the trap was the alternative and was
  rejected: it moves the same logic somewhere with weaker guarantees and no
  compile-time check, for a module whose whole job is to not leave orphans.
- **2026-09-26** — zone 4 has its first occupant, and it is not the one the
  zone was written for: `deepmsg-driver::sys` installs a `SIGINT`/`SIGTERM`
  handler, because a driver process has to be stoppable from outside and
  `libc::signal` takes a C function pointer. The zone's *name* is broadened
  from "media syscall shim" to "syscall shim" rather than opening a fifth zone
  for one `signal(2)`: the axis that decides a zone is *whose boundary it is*
  — here, the kernel's interface to this process — and signals and `recvmmsg`
  are the same boundary. The handler does one atomic store and nothing else,
  which is the whole discipline the zone needs: an async-signal-safe handler
  may not allocate, lock, or block. Zone 3 (`deepmsg-cnc`) still contains no
  `unsafe` at all, and the point of naming it was that it did not have to.
