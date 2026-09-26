//! Asking a media driver to shut down.
//!
//! A client with no registration, no heartbeat and no interest in the
//! to-clients ring maps the CnC file read-write, writes one record into the
//! to-driver command ring, and returns. There is **no reply of any kind** —
//! not on success, not on refusal, not on malformed input. The driver runs a
//! validator (which refuses by default) and, if it accepts, a hook; that is the
//! whole of its response.
//!
//! Mirrors `aeron-client/src/main/c/aeron_context.c:560-671`.
//!
//! # What a success means, and what it does not
//!
//! [`TerminationOutcome::Committed`] means exactly this: a well-formed
//! `TERMINATE_DRIVER` record was claimed and committed into the ring buffer of
//! the file at `<aeron_dir>/cnc.dat`. It does **not** mean a driver is running
//! — a stale file from a dead driver passes every precondition, because the
//! directory is only removed on shutdown when the driver was started with
//! `aeron.dir.delete.on.shutdown=true` — and it does not mean the command was
//! accepted, or that the driver has terminated yet.
//!
//! The only way to see the outcome is to watch the file: a driver that closes
//! cleanly release-stores `-1` into the to-driver ring's `consumer_heartbeat`
//! (`aeron-driver/src/main/c/aeron_driver_conductor.c:3493`), so
//! `cnc.consumer_heartbeat_ms()` becoming [`deepmsg_cnc::layout::NULL_VALUE`]
//! is the evidence. `cnc-dump` prints it.

use std::path::Path;

use deepmsg_cnc::command::{MAX_TOKEN_LENGTH, TERMINATE_DRIVER_TYPE_ID, TerminateDriver};
use deepmsg_cnc::{ClaimError, CncFile, CncOpenError};

/// Whether the command was sent.
///
/// A typed outcome rather than a `Result`, per ADR-0003: the reference carries
/// these in one `int` and the C++ wrapper then collapses them into a `bool`,
/// reporting "there is no driver here" as `false` — a bug this type exists not
/// to inherit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TerminationOutcome {
    /// The record was claimed and committed. See the module docs for what that
    /// does and does not imply.
    Committed,
    /// There is no CnC file at that path, or the one there is too short to be
    /// one. The reference reports this as a plain `0`, with no error set —
    /// it is a fact about the directory, not a failure.
    NoCncFile,
}

/// Why the command could not be sent.
#[derive(Debug)]
pub enum TerminationError {
    /// The token is longer than a termination token may be. The reference's
    /// limit is `AERON_MAX_PATH`; the ring has a second, usually larger, limit
    /// of its own.
    TokenTooLong {
        /// Length supplied.
        length: usize,
        /// The limit it exceeded.
        limit: usize,
    },
    /// The file exists but its metadata is not published yet.
    NotReady,
    /// The file is not a CnC file this build can use.
    Incompatible(deepmsg_core::version::CncVersionCompatibility),
    /// The metadata describes a layout that cannot be used to send a command.
    Malformed(deepmsg_cnc::CncError),
    /// The file could not be opened or mapped.
    Io(std::io::Error),
    /// The ring was built but its trailer could not be read. The layout and
    /// capacity checks should have made this impossible, so reaching it means
    /// one of them is wrong rather than that the driver is misbehaving.
    UnreadableRing,
    /// The ring had no room. A caller may reasonably retry; a one-shot command
    /// usually should not.
    RingFull,
    /// The message cannot fit in this ring at all, whatever its free space.
    MessageTooLong {
        /// Length supplied.
        length: usize,
        /// The ring's largest payload.
        limit: usize,
    },
}

impl std::fmt::Display for TerminationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TokenTooLong { length, limit } => {
                write!(f, "token is {length} bytes, more than the {limit} allowed")
            }
            Self::NotReady => f.write_str("CnC file exists but its metadata is unpublished"),
            Self::Incompatible(compatibility) => {
                write!(f, "CnC version is not usable: {compatibility:?}")
            }
            Self::Malformed(error) => write!(f, "CnC metadata is not usable: {error}"),
            Self::Io(error) => write!(f, "CnC file could not be opened: {error}"),
            Self::UnreadableRing => f.write_str("the to-driver ring's trailer could not be read"),
            Self::RingFull => f.write_str("the to-driver command ring is full"),
            Self::MessageTooLong { length, limit } => write!(
                f,
                "command is {length} bytes including its 20-byte header, \
                 more than the ring's {limit}"
            ),
        }
    }
}

impl std::error::Error for TerminationError {}

/// Ask the media driver owning `aeron_dir` to terminate.
///
/// `token` is handed to the driver's termination validator and is otherwise
/// meaningless: both of the driver's built-in validators ignore it, so against
/// a stock driver this call is refused whatever the token says. It is only
/// useful against a driver started with a custom validator, which is what the
/// reference's own tests do.
///
/// # Errors
///
/// [`TerminationError`] for the precondition failures the reference reports as
/// `-1`, and never for [`TerminationOutcome::NoCncFile`], which is an outcome.
pub fn request_driver_termination(
    aeron_dir: &Path,
    token: &[u8],
) -> Result<TerminationOutcome, TerminationError> {
    // First, before the token is read at all. The reference checks this before
    // touching the buffer, and two of its tests pass a length longer than the
    // buffer they supply in reliance on that ordering.
    if token.len() > MAX_TOKEN_LENGTH {
        return Err(TerminationError::TokenTooLong {
            length: token.len(),
            limit: MAX_TOKEN_LENGTH,
        });
    }

    let cnc = match CncFile::try_open_writable(aeron_dir) {
        Ok(cnc) => cnc,
        // `length <= 128` is the reference's silent no-op: the file is not a
        // CnC file, and it says so by doing nothing and setting no error. A
        // file that is *absent* is an error, and `try_open_writable` reports
        // that as `Io` rather than as this.
        Err(CncOpenError::TooShort { .. }) => return Ok(TerminationOutcome::NoCncFile),
        Err(error) => return Err(TerminationError::from(error)),
    };

    let ring = cnc
        .to_driver_ring()
        .ok_or(TerminationError::UnreadableRing)?;

    // The reference compares the *token* length here, then claims
    // `20 + token`. A token of exactly the maximum therefore passes this check
    // and fails the claim — the one case where its two bounds disagree. We
    // keep both checks and keep the two errors apart.
    if token.len() > ring.max_message_length() {
        return Err(TerminationError::TokenTooLong {
            length: token.len(),
            limit: ring.max_message_length(),
        });
    }

    let command = TerminateDriver {
        // Two consecutive values, as the reference takes them: the first fills
        // `client_id` and the second `correlation_id`. Neither is read by the
        // driver for this command.
        client_id: ring
            .next_correlation_id()
            .ok_or(TerminationError::UnreadableRing)?,
        correlation_id: ring
            .next_correlation_id()
            .ok_or(TerminationError::UnreadableRing)?,
        token,
    };

    let mut payload = vec![0u8; command.encoded_length()];
    if !command.encode_into(&mut payload) {
        // Only reachable if the token grew between the checks above and here,
        // which it cannot: this is the same slice.
        return Err(TerminationError::TokenTooLong {
            length: token.len(),
            limit: ring.max_message_length(),
        });
    }

    match ring.write(TERMINATE_DRIVER_TYPE_ID, &payload) {
        Ok(()) => Ok(TerminationOutcome::Committed),
        Err(ClaimError::Full) => Err(TerminationError::RingFull),
        Err(ClaimError::Invalid) => Err(TerminationError::MessageTooLong {
            length: payload.len(),
            limit: ring.max_message_length(),
        }),
    }
}

impl From<CncOpenError> for TerminationError {
    fn from(error: CncOpenError) -> Self {
        match error {
            CncOpenError::NotReady => Self::NotReady,
            // Carried through as given, including the `Compatible` value the
            // opener never actually produces — reporting what we were handed
            // beats inventing a translation for a case that cannot occur.
            CncOpenError::Incompatible(compatibility) => Self::Incompatible(compatibility),
            CncOpenError::Malformed(inner) => Self::Malformed(inner),
            CncOpenError::Io(io) => Self::Io(io),
            // `TooShort` never reaches here: the caller handles it as the
            // silent no-op it is. Reaching this arm would mean the file changed
            // shape between the two checks.
            CncOpenError::TooShort { length } => Self::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("CnC file is {length} bytes"),
            )),
        }
    }
}
