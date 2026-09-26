//! Publications: shared (CAS on the term tail) and exclusive (single
//! writer).
//!
//! P0 scope, mirroring `aeron_publication.c` / `aeron_exclusive_publication.c`
//! (M05): offer/try-claim, position tracking, max payload / MTU semantics,
//! padding and fragmentation at the client (the driver sender never
//! fragments).
//!
//! ADR-0003: `offer` returns a typed outcome enum; `BackPressured` and
//! `AdminAction` are states, not errors.
