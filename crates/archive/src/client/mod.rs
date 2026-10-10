//! The archive **client** — what a program that talks to an archive is.
//!
//! `crates/archive/src/server/` is the archive; this is the other end of the
//! wire, and it is a *separate* thing and not a convenience: an archive is a
//! client too, of the archive it replicates from
//! (`ReplicationSession.java:106-107`), and the two directions have to agree on
//! a protocol neither of them owns.
//!
//! # Three layers, and the split is load-bearing
//!
//! The reference's C client is layered (`aeron-archive/src/main/c/client/`) and
//! the layers are what let one piece of code serve two callers:
//!
//! * **The non-blocking layer** — request encoders (`proxy`), the response
//!   poller (`poller`), and the connect state machine (`async_connect`).
//!   Everything here is *polled*: a caller drives it a turn at a time and is
//!   never waited on. The archive's own replication runs on this layer, in the
//!   conductor's turn (`server::replication_session`), where a blocking call is
//!   a deadlock — the archive would be waiting for a driver turn that only its
//!   own turn can drive.
//! * **The wait family** — the four loops that turn "poll until the answer for
//!   this correlation id" into a value. They block, by design, and only a caller
//!   that owns its thread may use them.
//! * **The synchronous API** — an `AeronArchive`: connect, record, replay, list,
//!   and the rest, each one a request plus one of the four waits.
//!
//! # Where it comes from
//!
//! P2-C1 ports the reference's C client, file for file: `aeron_archive_context.c`
//! (780 lines) is [`context`], `aeron_archive_proxy.c` (1087) is `proxy`,
//! `aeron_archive_control_response_poller.c` (340) is `poller`,
//! `aeron_archive_async_connect.c` (582) is `async_connect`, and
//! `aeron_archive_client.c` (2617) is the synchronous API. The C++ wrapper and
//! the *async* client (`aeron_archive_async_client.c`, the self-healing one) are
//! other slices and are not here.

pub mod context;

pub use context::{ArchiveContext, ClientError, ConcludeError, ControlChannels};
