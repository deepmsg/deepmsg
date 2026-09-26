//! Platform abstraction seam: the one place in the workspace that talks to the
//! operating system directly.
//!
//! The point of the seam is that adding a platform is a new file plus a `cfg`
//! gate, not a hunt through the tree. Everything above it deals in safe types.
//!
//! # Supported platforms
//!
//! **Linux is the only declared target for now.** The byte contracts do not
//! vary by platform — `AERON_CACHE_LINE_LENGTH` is hard-coded to 64
//! (`aeron-client/src/main/c/util/aeron_bitutil.h:33`) and every other field is
//! little-endian arithmetic — so the cost of a platform is not the code, it is
//! the *proof*. This project's rule is that compatibility is proven rather
//! than asserted, and the oracle is the reference C driver: a platform is
//! added when it can be verified against that oracle, not when it is wanted.
//!
//! The reference shows where the work sits. Of its C client and driver
//! sources, 42 files are behind `__linux__` but only **2** behind `_WIN32` and
//! **3** behind `__APPLE__` — that is the size of the delta that would need
//! porting, before the delta that would need *proving*.
//!
//! One rule to keep in mind when a second platform does arrive: **64 is a byte
//! contract, not an optimisation target.** Detecting the cache-line size at
//! runtime is exactly the defect that shipped in `aeron-rs` on Apple Silicon,
//! where it silently computed a wrong shared-memory layout.
//!
//! Unsafe policy (ADR-0002): this is zone 1, named there alongside
//! [`crate::buffer`] — `pal` owns address-space bookkeeping, one implementation
//! per platform, while `buffer` owns platform-neutral typed access over the
//! bytes it hands out. Every `unsafe` block here carries a `// SAFETY:` comment
//! naming the invariant (alignment, exclusivity, lifetime) and the
//! memory-ordering rationale.

#[cfg(target_os = "linux")]
mod linux;

#[cfg(target_os = "linux")]
pub use linux::MappedFile;

#[cfg(not(target_os = "linux"))]
compile_error!(
    "deepmsg declares support for Linux only so far. Porting means adding a \
     sibling of `pal/linux.rs` and widening this gate; note that a platform is \
     only claimed once it can be verified against the reference driver."
);
