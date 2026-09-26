//! Command encodings a client sends to the driver.
//!
//! A command travels as one MPSC record. The record header carries the command
//! **type** as its `msg_type_id`; the payload starts with a correlated header
//! and then the command's own fields.
//!
//! The type id is deliberately *not* in the payload. The Java client puts a
//! `commandTypeId` field inside its encoding, and a port written from that
//! reading produces a payload four bytes too long which the driver misparses
//! into nonsense. The C layout is the authority here (ADR-0001).

/// The `client_id` and `correlation_id` every correlated command starts with:
/// `aeron-client/src/main/c/command/aeron_control_protocol.h:64-70`.
pub const CORRELATED_COMMAND_LENGTH: usize = 16;

/// `AERON_COMMAND_TERMINATE_DRIVER`
/// (`aeron-client/src/main/c/command/aeron_control_protocol.h:40`).
pub const TERMINATE_DRIVER_TYPE_ID: i32 = 0x0E;

/// A terminate payload before its token: 16 bytes of correlated header plus a
/// 4-byte token length (`aeron_control_protocol.h:213-218`).
pub const TERMINATE_DRIVER_HEADER_LENGTH: usize = CORRELATED_COMMAND_LENGTH + 4;

/// `AERON_MAX_PATH` (`aeron-client/src/main/c/aeron_common.h:24`) — the bound
/// the reference's client puts on a termination token.
///
/// The reference's comparison is strictly greater, so exactly this many bytes
/// is accepted. It is checked **before** the token is read at all, and two of
/// the reference's own tests pass a length longer than their buffer in reliance
/// on that ordering; ours is checked in the same place for the same reason.
pub const MAX_TOKEN_LENGTH: usize = 4096;

/// A `TERMINATE_DRIVER` command, ready to be written into a ring record.
///
/// The driver passes the token to its termination validator and does nothing
/// else with it. Its built-in validators ignore the token entirely — `deny`
/// refuses and `allow` accepts, whatever the bytes say — so a token is only
/// meaningful to a driver started with a custom validator.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TerminateDriver<'t> {
    /// Filled from the ring's correlation counter, exactly as the reference
    /// fills it — and, like the reference's, never read by the driver for this
    /// command. It is ceremony the wire format requires, not a request/reply
    /// id; there is no reply.
    pub client_id: i64,
    /// The second of two consecutive counter values, for the same reason.
    pub correlation_id: i64,
    /// Opaque bytes for the validator. Not NUL-terminated on the wire.
    pub token: &'t [u8],
}

impl<'t> TerminateDriver<'t> {
    /// How many bytes this command occupies in a record payload.
    pub const fn encoded_length(&self) -> usize {
        TERMINATE_DRIVER_HEADER_LENGTH + self.token.len()
    }

    /// Write the payload into `out`, which must be exactly
    /// [`TerminateDriver::encoded_length`] bytes.
    ///
    /// Returns false without writing anything if it is not, or if the token is
    /// longer than [`MAX_TOKEN_LENGTH`] — the driver does not bound
    /// `token_length` against the message length, so a client that lies here
    /// walks the validator off the end of the record. We do not lie.
    pub fn encode_into(&self, out: &mut [u8]) -> bool {
        if out.len() != self.encoded_length() || self.token.len() > MAX_TOKEN_LENGTH {
            return false;
        }

        let Ok(token_length) = i32::try_from(self.token.len()) else {
            return false;
        };

        out[0..8].copy_from_slice(&self.client_id.to_le_bytes());
        out[8..16].copy_from_slice(&self.correlation_id.to_le_bytes());
        out[16..20].copy_from_slice(&token_length.to_le_bytes());
        out[TERMINATE_DRIVER_HEADER_LENGTH..].copy_from_slice(self.token);

        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_a_terminate_command_without_a_type_field() {
        let mut out = [0u8; TERMINATE_DRIVER_HEADER_LENGTH + 4];
        let command = TerminateDriver {
            client_id: 1,
            correlation_id: 2,
            token: b"good",
        };

        assert_eq!(command.encoded_length(), out.len());
        assert!(command.encode_into(&mut out));

        assert_eq!(1i64.to_le_bytes(), out[0..8], "client_id");
        assert_eq!(2i64.to_le_bytes(), out[8..16], "correlation_id");
        assert_eq!(4i32.to_le_bytes(), out[16..20], "token_length");
        assert_eq!(b"good", &out[20..], "the token, not NUL-terminated");

        // The payload is 20 bytes plus the token and nothing more. A port that
        // carried a command type inside would make this 24.
        assert_eq!(
            20 + 4,
            command.encoded_length(),
            "the command type lives in the record header, not here"
        );
    }

    #[test]
    fn encodes_an_empty_token() {
        // `driver_tool` passes a null token with zero length, so this has to be
        // legal rather than an edge case.
        let mut out = [0u8; TERMINATE_DRIVER_HEADER_LENGTH];
        let command = TerminateDriver {
            client_id: 0,
            correlation_id: 0,
            token: b"",
        };

        assert!(command.encode_into(&mut out));
        assert_eq!(0i32.to_le_bytes(), out[16..20]);
    }

    #[test]
    fn refuses_a_destination_of_the_wrong_size() {
        let command = TerminateDriver {
            client_id: 1,
            correlation_id: 2,
            token: b"good",
        };

        let mut short = [0u8; 20];
        assert!(!command.encode_into(&mut short), "one byte short");

        let mut long = [0u8; 32];
        assert!(!command.encode_into(&mut long), "four bytes long");
    }

    #[test]
    fn refuses_a_token_longer_than_the_reference_allows() {
        let token = vec![0u8; MAX_TOKEN_LENGTH + 1];
        let command = TerminateDriver {
            client_id: 1,
            correlation_id: 2,
            token: &token,
        };
        let mut out = vec![0u8; command.encoded_length()];

        assert!(
            !command.encode_into(&mut out),
            "the reference's client refuses this, and ours must too"
        );

        // Exactly the limit is fine: the reference's comparison is `>`.
        let token = vec![0u8; MAX_TOKEN_LENGTH];
        let command = TerminateDriver {
            client_id: 1,
            correlation_id: 2,
            token: &token,
        };
        let mut out = vec![0u8; command.encoded_length()];
        assert!(command.encode_into(&mut out));
    }
}
