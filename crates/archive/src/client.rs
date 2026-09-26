//! Archive control client (P2): async-first (M13 / M16).
//!
//! Ordering note from M16: the asynchronous client must land before any
//! merge/recovery helper built on top — replay-merge style logic has no
//! self-healing without it, while persistent subscriptions (built on the
//! async client) do.
