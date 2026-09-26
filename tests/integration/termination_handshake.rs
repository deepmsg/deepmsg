//! Termination handshake: clean client close and driver termination, plus
//! the kill -9 path (driver death -> client conductor liveness -> forced
//! close -> error callback, process survives — see ADR-0003). Lands in P0;
//! until then this target intentionally contains no tests.
