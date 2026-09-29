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

use crate::layout;

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

/// `AERON_COMMAND_ADD_SUBSCRIPTION`
/// (`aeron-client/src/main/c/command/aeron_control_protocol.h:30`).
pub const ADD_SUBSCRIPTION_TYPE_ID: i32 = 0x04;

/// The subscribe payload before its channel: the 16-byte correlated header,
/// a `registration_correlation_id`, and the stream id and channel length
/// (`aeron_control_protocol.h:91-98`).
pub const ADD_SUBSCRIPTION_HEADER_LENGTH: usize = 32;

/// A subscription request.
///
/// The channel is **not** a NUL-terminated string on the wire: it is
/// `channel_length` raw bytes immediately after the header, and the driver
/// reads exactly that many (`aeron_driver_conductor.c:2959-2964`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AddSubscription<'c> {
    /// The client this belongs to. The driver does not validate it against a
    /// registration — there is no registration — and creates a client record
    /// on first sight.
    pub client_id: i64,
    /// Becomes the subscription's registration id, and is echoed back in the
    /// ready response.
    pub correlation_id: i64,
    /// A field the driver never reads. The reference sends `-1`
    /// (`aeron-driver/src/test/c/aeron_driver_conductor_test.h:451`).
    pub registration_correlation_id: i64,
    /// The stream to subscribe to.
    pub stream_id: i32,
    /// The channel URI, e.g. `aeron:ipc`.
    pub channel: &'c str,
}

impl<'c> AddSubscription<'c> {
    /// How many bytes this command occupies in a record payload.
    pub const fn encoded_length(&self) -> usize {
        ADD_SUBSCRIPTION_HEADER_LENGTH + self.channel.len()
    }

    /// Write the payload into `out`, which must be exactly
    /// [`AddSubscription::encoded_length`] bytes.
    pub fn encode_into(&self, out: &mut [u8]) -> bool {
        if out.len() != self.encoded_length() {
            return false;
        }

        let Ok(channel_length) = i32::try_from(self.channel.len()) else {
            return false;
        };

        out[0..8].copy_from_slice(&self.client_id.to_le_bytes());
        out[8..16].copy_from_slice(&self.correlation_id.to_le_bytes());
        out[16..24].copy_from_slice(&self.registration_correlation_id.to_le_bytes());
        out[24..28].copy_from_slice(&self.stream_id.to_le_bytes());
        out[28..32].copy_from_slice(&channel_length.to_le_bytes());
        out[ADD_SUBSCRIPTION_HEADER_LENGTH..].copy_from_slice(self.channel.as_bytes());

        true
    }
}

/// `AERON_COMMAND_ADD_PUBLICATION`
/// (`aeron-client/src/main/c/command/aeron_control_protocol.h:26`).
pub const ADD_PUBLICATION_TYPE_ID: i32 = 0x01;

/// `AERON_COMMAND_REMOVE_PUBLICATION`
/// (`aeron-client/src/main/c/command/aeron_control_protocol.h:28`).
pub const REMOVE_PUBLICATION_TYPE_ID: i32 = 0x02;

/// `AERON_COMMAND_REMOVE_SUBSCRIPTION`
/// (`aeron-client/src/main/c/command/aeron_control_protocol.h:31`).
pub const REMOVE_SUBSCRIPTION_TYPE_ID: i32 = 0x05;

/// `AERON_COMMAND_ADD_EXCLUSIVE_PUBLICATION`
/// (`aeron_control_protocol.h:29`).
///
/// The same wire shape as [`ADD_PUBLICATION_TYPE_ID`] — the difference is in
/// what the driver builds from it and in the type id of the response, and it is
/// a separate command rather than a flag because a client that sends the wrong
/// one gets a log buffer it may not share.
pub const ADD_EXCLUSIVE_PUBLICATION_TYPE_ID: i32 = 0x03;

/// The remove payload: the correlated head, the publication's registration id
/// and a flags word (`aeron_remove_publication_command_t`,
/// `aeron_control_protocol.h:70-76`, 32 bytes under the header's
/// `#pragma pack(4)`).
pub const REMOVE_PUBLICATION_HEADER_LENGTH: usize = 32;

/// The publish payload before its channel.
///
/// Eight bytes shorter than a subscribe: there is no
/// `registration_correlation_id`, because a publication has no subscription to
/// register against (`aeron_control_protocol.h:91-98` for the subscribe shape,
/// `:62-67` for this one).
pub const ADD_PUBLICATION_HEADER_LENGTH: usize = 24;

/// `AERON_RESPONSE_ON_PUBLICATION_READY`
/// (`aeron-client/src/main/c/command/aeron_control_protocol.h:48`).
pub const ON_PUBLICATION_READY_TYPE_ID: i32 = 0x0F03;

/// `AERON_RESPONSE_ON_EXCLUSIVE_PUBLICATION_READY`
/// (`aeron-client/src/main/c/command/aeron_control_protocol.h:49`).
///
/// The same payload as [`ON_PUBLICATION_READY_TYPE_ID`] — the type id is how a
/// client tells whether the log buffer it just mapped is one it may share, and
/// the driver picks it from the command it is answering
/// (`aeron_driver_conductor.c:2395-2399`).
pub const ON_EXCLUSIVE_PUBLICATION_READY_TYPE_ID: i32 = 0x0F06;

/// The fixed part of `ON_PUBLICATION_READY`, before the log path.
///
/// **36 bytes**, and the path that follows it is *not* aligned and *not*
/// NUL-terminated — unlike `ON_AVAILABLE_IMAGE`, whose tail pads to four bytes
/// between two strings. The two messages look similar and are laid out
/// differently; this constant and [`IMAGE_BUFFERS_READY_LENGTH`] are the pair
/// to check against.
pub const PUBLICATION_BUFFERS_READY_LENGTH: usize = 36;

/// A publication request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AddPublication<'c> {
    /// The client this belongs to.
    pub client_id: i64,
    /// Becomes the publication's registration id and is echoed back.
    pub correlation_id: i64,
    /// The stream to publish on.
    pub stream_id: i32,
    /// The channel URI, e.g. `aeron:ipc`.
    pub channel: &'c str,
}

impl<'c> AddPublication<'c> {
    /// How many bytes this command occupies in a record payload.
    pub const fn encoded_length(&self) -> usize {
        ADD_PUBLICATION_HEADER_LENGTH + self.channel.len()
    }

    /// Write the payload into `out`, which must be exactly
    /// [`AddPublication::encoded_length`] bytes.
    pub fn encode_into(&self, out: &mut [u8]) -> bool {
        if out.len() != self.encoded_length() {
            return false;
        }

        let Ok(channel_length) = i32::try_from(self.channel.len()) else {
            return false;
        };

        out[0..8].copy_from_slice(&self.client_id.to_le_bytes());
        out[8..16].copy_from_slice(&self.correlation_id.to_le_bytes());
        out[16..20].copy_from_slice(&self.stream_id.to_le_bytes());
        out[20..24].copy_from_slice(&channel_length.to_le_bytes());
        out[ADD_PUBLICATION_HEADER_LENGTH..].copy_from_slice(self.channel.as_bytes());

        true
    }
}

/// `AERON_RESPONSE_ON_SUBSCRIPTION_READY`
/// (`aeron-client/src/main/c/command/aeron_control_protocol.h:52`).
pub const ON_SUBSCRIPTION_READY_TYPE_ID: i32 = 0x0F07;

/// The payload of `ON_SUBSCRIPTION_READY`: the request's correlation id and a
/// channel status counter id (`aeron_subscription_ready_t`,
/// `aeron_control_protocol.h:106-111`, twelve bytes under the header's
/// `#pragma pack(4)`).
///
/// `channel_status_indicator_id` is [`CHANNEL_STATUS_INDICATOR_NOT_ALLOCATED`]
/// for an IPC subscription, which is the only kind this build serves.
pub fn encode_subscription_ready(
    correlation_id: i64,
    channel_status_indicator_id: i32,
) -> [u8; 12] {
    let mut out = [0u8; 12];
    out[..8].copy_from_slice(&correlation_id.to_le_bytes());
    out[8..12].copy_from_slice(&channel_status_indicator_id.to_le_bytes());

    out
}

/// `AERON_RESPONSE_ON_ERROR` (`aeron_control_protocol.h:46`).
pub const ON_ERROR_TYPE_ID: i32 = 0x0F01;

/// `AERON_RESPONSE_ON_COUNTER_READY` (`aeron_control_protocol.h:53`).
pub const ON_COUNTER_READY_TYPE_ID: i32 = 0x0F08;

/// `AERON_RESPONSE_ON_OPERATION_SUCCEEDED`
/// (`aeron-client/src/main/c/command/aeron_control_protocol.h:48`).
///
/// The **completion signal** for a command that changes something and has no
/// reply of its own — a removal, a destination change, a session id request.
/// A real client blocks on it, matched by correlation id, which is why a driver
/// that acts on the command and stays silent leaves that client waiting for a
/// deadline it will then report as a timeout.
pub const ON_OPERATION_SUCCEEDED_TYPE_ID: i32 = 0x0F04;

/// The payload of `ON_OPERATION_SUCCEEDED`: the command's correlation id, and
/// nothing else (`aeron_operation_succeeded_t`, `aeron_control_protocol.h:117-121`).
pub const OPERATION_SUCCEEDED_LENGTH: usize = 8;

/// The fixed head of `ON_ERROR` (`aeron_error_response_t`, `:123-129`): the
/// correlation id of the command that failed, the error code, and the length of
/// the message that follows it.
pub const ERROR_RESPONSE_HEADER_LENGTH: usize = 16;

/// `AERON_ERROR_CODE_UNKNOWN_COUNTER`
/// (`aeron-client/src/main/c/aeron_client_error.h:15`).
pub const ERROR_CODE_UNKNOWN_COUNTER: i32 = 5;

/// `AERON_ERROR_CODE_INVALID_CHANNEL` (`aeron_client_error.h:13`).
///
/// What the reference reports for a channel URI it cannot parse, and for a
/// session id clash on a stream.
pub const ERROR_CODE_INVALID_CHANNEL: i32 = 1;

/// `AERON_ERROR_CODE_UNKNOWN_SUBSCRIPTION` (`aeron_client_error.h:14`).
pub const ERROR_CODE_UNKNOWN_SUBSCRIPTION: i32 = 2;

/// `AERON_ERROR_CODE_UNKNOWN_PUBLICATION` (`aeron_client_error.h:16`).
pub const ERROR_CODE_UNKNOWN_PUBLICATION: i32 = 3;

/// `AERON_ERROR_CODE_NOT_SUPPORTED` (`aeron_client_error.h:20`).
pub const ERROR_CODE_NOT_SUPPORTED: i32 = 8;

/// `AERON_ERROR_CODE_GENERIC_ERROR` (`aeron_client_error.h:21`).
///
/// What the reference sends when a command fails for a reason it has no code
/// for — an `ADD_COUNTER` whose client could not be registered or whose id
/// could not be allocated, for instance, where the C appends an error string
/// and leaves the code as it found it.
pub const ERROR_CODE_GENERIC_ERROR: i32 = 11;

/// `AERON_ERROR_CODE_STORAGE_SPACE` (`aeron_client_error.h:22`).
///
/// The code the reference composes for a log buffer the filesystem could not
/// hold — whether the kernel refused with `ENOSPC` or the driver's own
/// pre-creation check decided there was no room
/// (`aeron_driver_conductor.c:2326-2341`).
pub const ERROR_CODE_STORAGE_SPACE: i32 = 12;

/// `AERON_ERROR_CODE_UNKNOWN_COMMAND_TYPE_ID` (`aeron_client_error.h:16`).
///
/// What the reference's command adapter reports for a type id the protocol
/// does not define — not sent as an `ON_ERROR`, but recorded in the distinct
/// error log, **negated**, because the adapter passes
/// `-AERON_ERROR_CODE_UNKNOWN_COMMAND_TYPE_ID` to `AERON_SET_ERR` and the log
/// keeps whatever that left (`aeron_driver_conductor.c:3218-3221`).
pub const ERROR_CODE_UNKNOWN_COMMAND_TYPE_ID: i32 = 6;

/// `AERON_ERROR_CODE_RESOURCE_TEMPORARILY_UNAVAILABLE` (`aeron_client_error.h:20`).
///
/// The one code the driver answers a client with and then **keeps out of the
/// distinct error log**: `aeron_driver_conductor_on_error` skips its own
/// `log_explicit_error` for it, so no entry appears and the errors counter
/// stays where it was (`aeron_driver_conductor.c:2367-2370`).
pub const ERROR_CODE_RESOURCE_TEMPORARILY_UNAVAILABLE: i32 = 10;

/// `AERON_ERROR_CODE_MALFORMED_COMMAND` (`aeron_client_error.h:17`).
///
/// What the reference's command adapter reports for a command whose payload
/// is shorter than its own header — recorded negated in the distinct error
/// log, for the same reason as
/// [`ERROR_CODE_UNKNOWN_COMMAND_TYPE_ID`] (`aeron_driver_conductor.c:3231-3235`).
pub const ERROR_CODE_MALFORMED_COMMAND: i32 = 7;

/// Encode `ON_OPERATION_SUCCEEDED`.
pub fn encode_operation_succeeded(correlation_id: i64) -> [u8; OPERATION_SUCCEEDED_LENGTH] {
    correlation_id.to_le_bytes()
}

/// Encode `ON_ERROR`.
///
/// The message is free text, **not** NUL-terminated, and its length is in the
/// header (`aeron_driver_conductor.c:2244-2260`). Allocating here is fine: this
/// is an error path, and the reference keeps a kilobyte of stack for it.
pub fn encode_error(correlation_id: i64, error_code: i32, message: &[u8]) -> Vec<u8> {
    #[allow(clippy::cast_possible_truncation)] // a message this build writes, far below i32::MAX
    let mut out = vec![0u8; ERROR_RESPONSE_HEADER_LENGTH + message.len()];
    out[..8].copy_from_slice(&correlation_id.to_le_bytes());
    out[8..12].copy_from_slice(&error_code.to_le_bytes());
    out[12..16].copy_from_slice(&(message.len() as i32).to_le_bytes());
    out[ERROR_RESPONSE_HEADER_LENGTH..].copy_from_slice(message);

    out
}

/// The fixed head of `ON_PUBLICATION_READY`, and the path that follows it.
///
/// The Rust spelling of `aeron_publication_buffers_ready_t`
/// (`aeron-client/src/main/c/command/aeron_control_protocol.h:79-89`) with the
/// same field order, which is the part that matters: the file's `#pragma
/// pack(4)` makes this 36 bytes with `session_id` at offset 16 and `stream_id`
/// at 20, and a struct that swapped them would be a publication a client maps
/// with the wrong stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PublicationBuffersReady<'a> {
    /// Echoes the `correlation_id` of the request.
    pub correlation_id: i64,
    /// The publication's **own** registration id: what the log file is named
    /// after, and what every image built on it reports.
    pub registration_id: i64,
    /// The session the publication runs under.
    pub session_id: i32,
    /// The stream it publishes.
    pub stream_id: i32,
    /// The `pub-lmt` counter: the client reads it for backpressure.
    pub position_limit_counter_id: i32,
    /// The channel-status counter, or
    /// [`CHANNEL_STATUS_INDICATOR_NOT_ALLOCATED`] — which is every IPC
    /// publication, because only a network channel has one.
    pub channel_status_indicator_id: i32,
    /// The log buffer's path: raw bytes, **not** aligned and **not**
    /// NUL-terminated.
    pub log_file: &'a [u8],
}

impl PublicationBuffersReady<'_> {
    /// The response's type id, which says whether the log buffer may be shared
    /// (`aeron_driver_conductor.c:2395-2399`).
    pub const fn type_id(is_exclusive: bool) -> i32 {
        if is_exclusive {
            ON_EXCLUSIVE_PUBLICATION_READY_TYPE_ID
        } else {
            ON_PUBLICATION_READY_TYPE_ID
        }
    }

    /// The payload: the fixed head, then the path with **no** alignment and
    /// **no** terminator.
    ///
    /// This is the one place the message differs from `ON_AVAILABLE_IMAGE`'s
    /// two padded strings. The reference transmits `sizeof + path_length`
    /// bytes and nothing rounds that up (`aeron_driver_conductor.c:2418`), so
    /// a decoder that expects padding reads the first four bytes of the *next*
    /// record as part of the path.
    pub fn encode(&self) -> Vec<u8> {
        #[allow(clippy::cast_possible_truncation)] // a path this build forms, far below i32::MAX
        let mut out = vec![0u8; PUBLICATION_BUFFERS_READY_LENGTH + self.log_file.len()];

        out[0..8].copy_from_slice(&self.correlation_id.to_le_bytes());
        out[8..16].copy_from_slice(&self.registration_id.to_le_bytes());
        out[16..20].copy_from_slice(&self.session_id.to_le_bytes());
        out[20..24].copy_from_slice(&self.stream_id.to_le_bytes());
        out[24..28].copy_from_slice(&self.position_limit_counter_id.to_le_bytes());
        out[28..32].copy_from_slice(&self.channel_status_indicator_id.to_le_bytes());
        out[32..36].copy_from_slice(&(self.log_file.len() as i32).to_le_bytes());
        out[PUBLICATION_BUFFERS_READY_LENGTH..].copy_from_slice(self.log_file);

        out
    }
}

/// `AERON_RESPONSE_ON_CLIENT_TIMEOUT` (`aeron_control_protocol.h:55`).
pub const ON_CLIENT_TIMEOUT_TYPE_ID: i32 = 0x0F0A;

/// `AERON_RESPONSE_ON_AVAILABLE_IMAGE` (`aeron_control_protocol.h:47`).
pub const ON_AVAILABLE_IMAGE_TYPE_ID: i32 = 0x0F02;

/// `AERON_RESPONSE_ON_UNAVAILABLE_IMAGE` (`aeron_control_protocol.h:50`).
pub const ON_UNAVAILABLE_IMAGE_TYPE_ID: i32 = 0x0F05;

/// The fixed part of `ON_AVAILABLE_IMAGE`: the 28 bytes before the log path.
///
/// **28, not 32.** The struct is inside a `#pragma pack(4)`, so its two
/// `int64_t`s are 4-byte aligned and `subscriber_registration_id` lands at
/// offset 16 rather than 24 (`aeron_control_protocol.h:107-115`, confirmed by
/// `ImageBuffersReadyFlyweight.java:60-65`). Reading it as a naturally-aligned
/// struct puts every field after `stream_id` four bytes out.
pub const IMAGE_BUFFERS_READY_LENGTH: usize = 28;

/// The fixed part of `ON_UNAVAILABLE_IMAGE`.
pub const IMAGE_MESSAGE_LENGTH: usize = 24;

/// `AERON_COUNTER_SUBSCRIPTION_POSITION_TYPE_ID`
/// (`aeron-client/src/main/c/aeron_counters.h:81`).
///
/// The counter a subscriber advances to say how far it has read. The driver
/// reads it back to compute the publisher's limit
/// (`aeron-driver/src/main/c/aeron_ipc_publication.c:289-317`), so a client
/// that never advances it eventually stops the publisher for everyone.
pub const SUBSCRIPTION_POSITION_TYPE_ID: i32 = 4;

/// `AERON_CHANNEL_STATUS_INDICATOR_NOT_ALLOCATED`
/// (`aeron-driver/src/main/c/aeron_driver_common.h:25`).
///
/// What an IPC or spy subscription reports, because no channel-status counter
/// was allocated for it. It is **not** an error, and a reader that treats a
/// negative counter id as one gets IPC wrong.
pub const CHANNEL_STATUS_INDICATOR_NOT_ALLOCATED: i32 = -1;

/// A decoded driver→client response.
///
/// The to-clients ring is a single broadcast every client reads in full, so a
/// client sees responses to other clients' commands and must match on the
/// correlation id. A response that matches nothing is dropped silently, which
/// is what the reference does at every one of its handlers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Response<'a> {
    /// A subscription was created. `channel_status_indicator_id` is a *counter
    /// id*, and is [`CHANNEL_STATUS_INDICATOR_NOT_ALLOCATED`] for channels that
    /// have none.
    SubscriptionReady {
        /// Echoes the `correlation_id` of the request.
        correlation_id: i64,
        /// The channel status counter, or `-1`.
        channel_status_indicator_id: i32,
    },
    /// A counter was allocated. A fresh client's *first* event is this one, for
    /// its own heartbeat counter, with `correlation_id == client_id`.
    CounterReady {
        /// Echoes the `correlation_id` of the request — or the client id, when
        /// the driver allocated the counter itself.
        correlation_id: i64,
        /// The counter's id, for `CountersReader`.
        counter_id: i32,
    },
    /// A counter was removed — or taken with the client that owned it, which
    /// the driver announces the same way. The payload is the same twelve bytes
    /// [`Response::CounterReady`] carries.
    CounterUnavailable {
        /// The registration id the counter was allocated under — the client
        /// id, when the driver allocated the counter itself.
        correlation_id: i64,
        /// The id the slot had, which is back in the pool now.
        counter_id: i32,
    },
    /// A command whose work is done, when the done thing has no handle to
    /// hand back: the driver's acknowledgement of a removal
    /// (`aeron_operation_succeeded_t`, eight bytes under the header).
    OperationSucceeded {
        /// Echoes the `correlation_id` of the request.
        correlation_id: i64,
    },
    /// The driver gave up on a client and destroyed everything it owned.
    ClientTimeout {
        /// Which client.
        client_id: i64,
    },
    /// A command failed. The message is not NUL-terminated.
    Error {
        /// The `correlation_id` of the command that failed, or zero.
        offending_command_correlation_id: i64,
        /// The reference's error code.
        error_code: i32,
        /// A human-readable description, borrowed from the payload.
        message: &'a [u8],
    },
    /// A publication was created.
    ///
    /// Carries the publication's **own** registration id separately from the
    /// correlation id that matched this request: the first names the log file
    /// and keys every image built on it, the second is only how this reply
    /// found its way back.
    PublicationReady {
        /// Echoes the `correlation_id` of the request.
        correlation_id: i64,
        /// The publication's registration id — what the log file is named
        /// after, and what an image reports as its own `correlation_id`.
        registration_id: i64,
        /// The publication's session id.
        session_id: i32,
        /// The publication's stream id.
        stream_id: i32,
        /// The counter holding how far this publication may write. The driver
        /// is the only writer.
        position_limit_counter_id: i32,
        /// The channel-status counter, or `-1` where none exists.
        channel_status_indicator_id: i32,
        /// The log buffer's path: raw bytes, **not** aligned and **not**
        /// NUL-terminated.
        log_file: &'a [u8],
    },
    /// An image became available: a publication this subscription matches now
    /// exists, and its log buffer can be mapped.
    ///
    /// Carries **two** ids and they are not interchangeable:
    /// [`Self::AvailableImage::publication_registration_id`] identifies the log
    /// buffer, and
    /// [`Self::AvailableImage::subscriber_registration_id`] is *your* subscription.
    /// Keying on the wrong one silently pairs an image with the wrong
    /// subscription.
    AvailableImage {
        /// The **publication's** registration id — the log file is named after
        /// it.
        publication_registration_id: i64,
        /// The publication's session id.
        session_id: i32,
        /// The publication's stream id.
        stream_id: i32,
        /// **This subscription's** registration id.
        subscriber_registration_id: i64,
        /// The counter a reader advances to report how far it has read.
        subscriber_position_id: i32,
        /// The log buffer's path, as raw bytes: length-prefixed, four-byte
        /// aligned, and **not** NUL-terminated.
        log_file: &'a [u8],
        /// Where the data came from. `aeron:ipc` for an IPC channel.
        source_identity: &'a [u8],
    },
    /// An image went away.
    UnavailableImage {
        /// The publication that is gone.
        publication_registration_id: i64,
        /// The subscription it was attached to.
        subscription_registration_id: i64,
        /// The stream id.
        stream_id: i32,
        /// The subscription's channel, raw bytes, not NUL-terminated.
        channel: &'a [u8],
    },
    /// A response this build does not model. Counted by the caller, never fatal
    /// — ADR-0003's rule, and a deliberate divergence from the reference, which
    /// reports it to the error handler and whose default handler exits.
    Other {
        /// The `msg_type_id` that arrived.
        type_id: i32,
    },
}

/// `AERON_RESPONSE_ON_UNAVAILABLE_COUNTER` (`aeron_control_protocol.h:54`).
///
/// The *removal* message. There is no in-band "this counter is gone" value: a
/// driver that frees a counter announces it with this type and the same
/// twelve-byte payload `ON_COUNTER_READY` carries
/// (`aeron_driver_conductor.c:2489-2500`). A client that reads a `-1` value as
/// a removal is reading something the protocol does not say, which is why this
/// constant exists here before a client uses it.
pub const ON_UNAVAILABLE_COUNTER_TYPE_ID: i32 = 0x0F09;

/// The payload of `ON_COUNTER_READY` and `ON_UNAVAILABLE_COUNTER`
/// (`aeron_counter_update_t`, `aeron_control_protocol.h:185-190`): an `int64`
/// correlation id and an `int32` counter id, packed to **12** not 16.
pub const COUNTER_UPDATE_LENGTH: usize = 12;

/// The payload of `ON_CLIENT_TIMEOUT` (`aeron_client_timeout_t`,
/// `aeron_control_protocol.h:203-207`): the client id, and nothing else.
pub const CLIENT_TIMEOUT_LENGTH: usize = 8;

/// Encode a counter announcement, for either of the two types that carry one.
pub fn encode_counter_update(correlation_id: i64, counter_id: i32) -> [u8; COUNTER_UPDATE_LENGTH] {
    let mut out = [0u8; COUNTER_UPDATE_LENGTH];
    out[..8].copy_from_slice(&correlation_id.to_le_bytes());
    out[8..].copy_from_slice(&counter_id.to_le_bytes());
    out
}

/// Encode `ON_CLIENT_TIMEOUT`.
///
/// The client id goes in the one field the message has — the receiver reads it
/// as the correlation id, because that is what the struct's first member is
/// (`aeron_driver_conductor.c:2542-2554`).
pub fn encode_client_timeout(client_id: i64) -> [u8; CLIENT_TIMEOUT_LENGTH] {
    client_id.to_le_bytes()
}

/// The head every client command that names a client starts with
/// (`aeron_correlated_command_t`, `aeron_control_protocol.h:64-69`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Correlated {
    /// Which client sent it.
    pub client_id: i64,
    /// What it wants matched back to it.
    pub correlation_id: i64,
}

/// Decode the head of a correlated command.
pub fn decode_correlated(payload: &[u8]) -> Option<Correlated> {
    Some(Correlated {
        client_id: le_i64(payload, 0)?,
        correlation_id: le_i64(payload, 8)?,
    })
}

/// `AERON_COMMAND_ADD_COUNTER` (`aeron_control_protocol.h:35`).
pub const ADD_COUNTER_TYPE_ID: i32 = 0x09;

/// `ADD_COUNTER` (`0x09`), decoded.
///
/// The wire form is `aeron_counter_command_t` — a correlated head and an
/// `int32 type_id` — followed by a key and a label, each with its own length
/// and the key padded to four bytes
/// (`aeron_client_conductor.c:2035-2052` writes exactly this shape).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AddCounter<'a> {
    /// Who is asking, and what to answer with.
    pub correlated: Correlated,
    /// The counter's type, which is what a reader uses to interpret the key.
    pub type_id: i32,
    /// The key: free-form bytes whose meaning belongs to `type_id`.
    pub key: &'a [u8],
    /// The label: free-form text, **not** NUL-terminated.
    pub label: &'a [u8],
}

impl AddCounter<'_> {
    /// How many bytes this command occupies in a record payload: the lengths
    /// it declares are the same ones [`decode_add_counter`] reads back, so the
    /// key is padded to four and the label is not.
    pub const fn encoded_length(&self) -> usize {
        ADD_COUNTER_KEY_OFFSET + layout::align_up(self.key.len(), 4) + 4 + self.label.len()
    }

    /// Write the payload into `out`, which must be exactly
    /// [`AddCounter::encoded_length`] bytes — the writing half of the shape
    /// the reference's own conductor writes
    /// (`aeron_client_conductor.c:2035-2052`).
    pub fn encode_into(&self, out: &mut [u8]) -> bool {
        if out.len() != self.encoded_length() {
            return false;
        }

        let Ok(key_length) = i32::try_from(self.key.len()) else {
            return false;
        };
        let Ok(label_length) = i32::try_from(self.label.len()) else {
            return false;
        };

        let key_end = ADD_COUNTER_KEY_OFFSET + self.key.len();
        out[0..8].copy_from_slice(&self.correlated.client_id.to_le_bytes());
        out[8..16].copy_from_slice(&self.correlated.correlation_id.to_le_bytes());
        out[16..20].copy_from_slice(&self.type_id.to_le_bytes());
        out[20..24].copy_from_slice(&key_length.to_le_bytes());
        // The padding is zero on the wire, and the decoder skips it by the
        // aligned length rather than reading it.
        out[ADD_COUNTER_KEY_OFFSET..key_end].copy_from_slice(self.key);
        let label_length_offset = ADD_COUNTER_KEY_OFFSET + layout::align_up(self.key.len(), 4);
        out[label_length_offset..label_length_offset + 4]
            .copy_from_slice(&label_length.to_le_bytes());
        out[label_length_offset + 4..].copy_from_slice(self.label);

        true
    }
}

/// Where `ADD_COUNTER`'s key begins: the correlated head, the `int32` type id
/// (`aeron_control_protocol.h:178-183`) and the `int32` key length.
const ADD_COUNTER_KEY_OFFSET: usize = CORRELATED_COMMAND_LENGTH + 4 + 4;

/// Decode `ADD_COUNTER`.
///
/// Lengths are read as *signed* and refused when negative, and every slice is
/// taken with `get` rather than by slicing: these bytes come from another
/// process, and a length field is the one place a hostile or buggy client can
/// make arithmetic overflow. The reference checks only the fixed head
/// (`aeron_driver_conductor.c:3096-3108`) and trusts the rest.
pub fn decode_add_counter(payload: &[u8]) -> Option<AddCounter<'_>> {
    let correlated = decode_correlated(payload)?;
    let type_id = le_i32(payload, CORRELATED_COMMAND_LENGTH)?;

    let key_length = usize::try_from(le_i32(payload, CORRELATED_COMMAND_LENGTH + 4)?).ok()?;
    let key_start = ADD_COUNTER_KEY_OFFSET;
    let key = payload.get(key_start..key_start.checked_add(key_length)?)?;

    let label_length_offset = key_start.checked_add(layout::align_up(key_length, 4))?;
    let label_length = usize::try_from(le_i32(payload, label_length_offset)?).ok()?;
    let label_start = label_length_offset.checked_add(4)?;
    let label = payload.get(label_start..label_start.checked_add(label_length)?)?;

    Some(AddCounter {
        correlated,
        type_id,
        key,
        label,
    })
}

/// `AERON_COMMAND_REMOVE_COUNTER` (`aeron_control_protocol.h:36`).
pub const REMOVE_COUNTER_TYPE_ID: i32 = 0x0A;

/// `REMOVE_COUNTER` (`0x0A`), decoded.
///
/// It names the counter by the **client's registration id**, not by its
/// counter id (`aeron_remove_counter_command_t`,
/// `aeron_control_protocol.h:131-136`): the driver looks the link up in the
/// asking client's own list and is what stops one client removing another's.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RemoveCounter {
    /// Who is asking, and what to answer with.
    pub correlated: Correlated,
    /// The registration id the counter was allocated under.
    pub registration_id: i64,
}

impl RemoveCounter {
    /// How many bytes this command occupies in a record payload.
    pub const fn encoded_length(&self) -> usize {
        CORRELATED_COMMAND_LENGTH + 8
    }

    /// Write the payload into `out`, which must be exactly
    /// [`RemoveCounter::encoded_length`] bytes.
    pub fn encode_into(&self, out: &mut [u8]) -> bool {
        if out.len() != self.encoded_length() {
            return false;
        }

        out[0..8].copy_from_slice(&self.correlated.client_id.to_le_bytes());
        out[8..16].copy_from_slice(&self.correlated.correlation_id.to_le_bytes());
        out[16..24].copy_from_slice(&self.registration_id.to_le_bytes());

        true
    }
}

/// Decode `REMOVE_COUNTER`.
pub fn decode_remove_counter(payload: &[u8]) -> Option<RemoveCounter> {
    Some(RemoveCounter {
        correlated: decode_correlated(payload)?,
        registration_id: le_i64(payload, CORRELATED_COMMAND_LENGTH)?,
    })
}

/// `ADD_PUBLICATION` / `ADD_EXCLUSIVE_PUBLICATION` as they arrive: the
/// receiving side of [`AddPublication`].
///
/// The two commands share this shape and differ only in what the driver builds
/// from it, so one decoder serves both and the caller carries the flag.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AddPublicationCommand<'a> {
    /// Who is asking. The driver does not check it against a registration —
    /// there is none — and creates a client record on first sight
    /// (`aeron_driver_conductor.c:3733-3739`).
    pub client_id: i64,
    /// Becomes the publication's registration id, names its log file, and is
    /// echoed back as the reply's `registration_id` — a *different* field from
    /// the correlation id the reply is matched by.
    pub correlation_id: i64,
    /// The stream to publish on.
    pub stream_id: i32,
    /// The channel URI as raw bytes, **not** NUL-terminated: the driver reads
    /// exactly `channel_length` of them and parses those
    /// (`aeron_driver_conductor.c:3964-3965`).
    pub channel: &'a [u8],
}

/// Decode `ADD_PUBLICATION` or `ADD_EXCLUSIVE_PUBLICATION`.
///
/// A channel length that runs past the payload is refused rather than clamped:
/// the URI is what the driver writes into counters other processes read, and
/// taking a short one silently would produce a publication nobody can match.
pub fn decode_add_publication(payload: &[u8]) -> Option<AddPublicationCommand<'_>> {
    let correlated = decode_correlated(payload)?;
    let stream_id = le_i32(payload, CORRELATED_COMMAND_LENGTH)?;
    let channel_length = usize::try_from(le_i32(payload, CORRELATED_COMMAND_LENGTH + 4)?).ok()?;
    let channel = payload.get(
        ADD_PUBLICATION_HEADER_LENGTH..ADD_PUBLICATION_HEADER_LENGTH.checked_add(channel_length)?,
    )?;

    Some(AddPublicationCommand {
        client_id: correlated.client_id,
        correlation_id: correlated.correlation_id,
        stream_id,
        channel,
    })
}

/// `ADD_SUBSCRIPTION` as it arrives: the receiving side of
/// [`AddSubscription`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AddSubscriptionCommand<'a> {
    /// Who is asking. Registered on first sight, like a publication's client.
    pub client_id: i64,
    /// Becomes the subscription's registration id, and is what every image's
    /// `subscriber_registration_id` quotes.
    pub correlation_id: i64,
    /// The field the driver never reads (`aeron_driver_conductor.c:4757-4760`
    /// stores nothing from it).
    pub registration_correlation_id: i64,
    /// The stream to read.
    pub stream_id: i32,
    /// The channel URI as raw bytes, **not** NUL-terminated.
    pub channel: &'a [u8],
}

/// Decode `ADD_SUBSCRIPTION`.
///
/// Eight bytes longer than [`decode_add_publication`]: the extra
/// `registration_correlation_id` sits between the correlated head and the
/// stream id (`aeron_control_protocol.h:91-98`).
pub fn decode_add_subscription(payload: &[u8]) -> Option<AddSubscriptionCommand<'_>> {
    let correlated = decode_correlated(payload)?;
    let registration_correlation_id = le_i64(payload, CORRELATED_COMMAND_LENGTH)?;
    let stream_id = le_i32(payload, CORRELATED_COMMAND_LENGTH + 8)?;
    let channel_length = usize::try_from(le_i32(payload, CORRELATED_COMMAND_LENGTH + 12)?).ok()?;
    let channel = payload.get(
        ADD_SUBSCRIPTION_HEADER_LENGTH
            ..ADD_SUBSCRIPTION_HEADER_LENGTH.checked_add(channel_length)?,
    )?;

    Some(AddSubscriptionCommand {
        client_id: correlated.client_id,
        correlation_id: correlated.correlation_id,
        registration_correlation_id,
        stream_id,
        channel,
    })
}

/// `REMOVE_PUBLICATION` as it arrives, with its flags word
/// (`aeron_remove_publication_command_t`, `aeron_control_protocol.h:70-76`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RemovePublication {
    /// Who is asking. Unlike a subscription removal, this one is checked
    /// against the client's own link list
    /// (`aeron_driver_conductor.c:4707-4709`).
    pub correlated: Correlated,
    /// The client's correlation id for the `ADD_PUBLICATION` that made it.
    pub registration_id: i64,
    /// [`REMOVE_PUBLICATION_FLAG_REVOKE`], or zero.
    pub flags: i64,
}

/// `AERON_COMMAND_REMOVE_PUBLICATION_FLAG_REVOKE`
/// (`aeron_control_protocol.h:60`): revoke the publication instead of just
/// letting go of it, so its readers are told the stream is done.
pub const REMOVE_PUBLICATION_FLAG_REVOKE: i64 = 0x1;

/// Decode `REMOVE_PUBLICATION`.
///
/// # The 24-byte form
///
/// The command grew a `flags` word, and the reference accepts the older shape
/// — a payload that ends where the flags would begin — by treating the flags as
/// zero (`aeron_driver_conductor.c:2920-2948`). A decoder that insisted on 32
/// bytes would refuse a removal from a client built before the flags existed,
/// and that client's publication would never go away.
pub fn decode_remove_publication(payload: &[u8]) -> Option<RemovePublication> {
    let correlated = decode_correlated(payload)?;
    let registration_id = le_i64(payload, CORRELATED_COMMAND_LENGTH)?;

    let flags = if payload.len() < REMOVE_PUBLICATION_HEADER_LENGTH {
        0
    } else {
        le_i64(payload, CORRELATED_COMMAND_LENGTH + 8)?
    };

    Some(RemovePublication {
        correlated,
        registration_id,
        flags,
    })
}

/// `REMOVE_SUBSCRIPTION` as it arrives: the correlated head and the
/// subscription's registration id (`aeron_remove_subscription_command_t`,
/// `aeron_control_protocol.h:146-151`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RemoveSubscription {
    /// Who is asking. The reference does **not** check it: a subscription is
    /// found by its registration id alone (`aeron_driver_conductor.c:5203`).
    pub correlated: Correlated,
    /// The client's correlation id for the `ADD_SUBSCRIPTION`.
    pub registration_id: i64,
}

/// Decode `REMOVE_SUBSCRIPTION`.
pub fn decode_remove_subscription(payload: &[u8]) -> Option<RemoveSubscription> {
    Some(RemoveSubscription {
        correlated: decode_correlated(payload)?,
        registration_id: le_i64(payload, CORRELATED_COMMAND_LENGTH)?,
    })
}

/// `AERON_COMMAND_ADD_DESTINATION`
/// (`aeron-client/src/main/c/command/aeron_control_protocol.h:32`).
pub const ADD_DESTINATION_TYPE_ID: i32 = 0x07;

/// `AERON_COMMAND_REMOVE_DESTINATION`
/// (`aeron-client/src/main/c/command/aeron_control_protocol.h:33`).
pub const REMOVE_DESTINATION_TYPE_ID: i32 = 0x08;

/// `AERON_COMMAND_ADD_RCV_DESTINATION`
/// (`aeron-client/src/main/c/command/aeron_control_protocol.h:37`).
pub const ADD_RECEIVE_DESTINATION_TYPE_ID: i32 = 0x0C;

/// `AERON_COMMAND_REMOVE_RCV_DESTINATION`
/// (`aeron-client/src/main/c/command/aeron_control_protocol.h:38`).
pub const REMOVE_RECEIVE_DESTINATION_TYPE_ID: i32 = 0x0D;

/// `AERON_COMMAND_REMOVE_DESTINATION_BY_ID`
/// (`aeron-client/src/main/c/command/aeron_control_protocol.h:42`).
pub const REMOVE_DESTINATION_BY_ID_TYPE_ID: i32 = 0x11;

/// A destination payload before its channel: the 16-byte correlated header, the
/// `registration_id` of the endpoint the destination belongs to, and the channel
/// length (`aeron_control_protocol.h:162-168`).
///
/// The reference declares this record under `#pragma pack(4)`
/// (`aeron_control_protocol.h:62-63`), and the pragma is load-bearing rather
/// than cosmetic. Measured against that header on this machine, the struct is 28
/// bytes with `registration_id` at 16 and `channel_length` at 24, so the URI
/// begins at 28. The same fields declared without the pragma are 32: the
/// trailing `int32_t` is padded out to the struct's eight-byte alignment, and
/// the URI would begin four bytes late — a driver reading at 28 would take four
/// bytes of the URI's own tail as its first character
/// (`aeron_driver_conductor.c:3012-3013` bounds the record by this same 28, and
/// the reference's own test helper copies a channel to
/// `sizeof(aeron_destination_command_t)`,
/// `aeron-driver/src/test/c/aeron_driver_conductor_test.h:536`).
pub const DESTINATION_COMMAND_HEADER_LENGTH: usize = 28;

/// `aeron_destination_by_id_command_t` — 32 bytes, and **no channel**
/// (`aeron_control_protocol.h:170-176`).
///
/// A pair of registration ids names the destination instead of its URI: the
/// resource it was added to, and the destination itself. That is what lets a
/// client remove a destination it has already been told about without repeating
/// the channel — and the record is fixed-length for exactly that reason, so
/// there is no `channel_length` to sanity-check against.
pub const DESTINATION_BY_ID_COMMAND_LENGTH: usize = 32;

/// A destination command as a client writes it: `ADD_DESTINATION`,
/// `REMOVE_DESTINATION`, `ADD_RCV_DESTINATION` or `REMOVE_RCV_DESTINATION`.
///
/// One record serves all four because they ask one question — *this endpoint,
/// this URI* — and differ only in which way a destination tracker is moved.
/// Which of the four a record is, is carried by the ring record's `msg_type_id`
/// and by nothing in the payload (see the module note), so one encoder writes
/// all four and the caller supplies the type.
///
/// The channel is **not** NUL-terminated: it is `channel_length` raw bytes
/// immediately after the header, exactly as a subscription's is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DestinationCommand<'c> {
    /// Who is asking. The driver registers it on first sight, as it does a
    /// publication's or a subscription's client.
    pub client_id: i64,
    /// What the answer is matched to.
    pub correlation_id: i64,
    /// The publication or subscription the destination belongs to. Which of the
    /// two it is comes from the command's type id, not from this field.
    pub registration_id: i64,
    /// The destination's channel URI, e.g. `aeron:udp?endpoint=localhost:1234`.
    pub channel: &'c str,
}

impl<'c> DestinationCommand<'c> {
    /// How many bytes this command occupies in a record payload.
    pub const fn encoded_length(&self) -> usize {
        DESTINATION_COMMAND_HEADER_LENGTH + self.channel.len()
    }

    /// Write the payload into `out`, which must be exactly
    /// [`DestinationCommand::encoded_length`] bytes.
    pub fn encode_into(&self, out: &mut [u8]) -> bool {
        if out.len() != self.encoded_length() {
            return false;
        }

        let Ok(channel_length) = i32::try_from(self.channel.len()) else {
            return false;
        };

        out[0..8].copy_from_slice(&self.client_id.to_le_bytes());
        out[8..16].copy_from_slice(&self.correlation_id.to_le_bytes());
        out[16..24].copy_from_slice(&self.registration_id.to_le_bytes());
        out[24..28].copy_from_slice(&channel_length.to_le_bytes());
        out[DESTINATION_COMMAND_HEADER_LENGTH..].copy_from_slice(self.channel.as_bytes());

        true
    }
}

/// A destination command as it arrives: the receiving side of
/// [`DestinationCommand`].
///
/// The channel is borrowed as bytes rather than as a `str`, unlike the encoder's:
/// these bytes come from another process and are not known to be UTF-8 until the
/// URI parser that wants them says so.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DestinationCommandReceived<'a> {
    /// Who is asking.
    pub client_id: i64,
    /// What the answer is matched to.
    pub correlation_id: i64,
    /// The publication or subscription the destination belongs to.
    pub registration_id: i64,
    /// The destination's channel URI as raw bytes, **not** NUL-terminated.
    pub channel: &'a [u8],
}

/// Decode any of the four destination commands that carry a channel.
///
/// A channel length that runs past the payload is refused rather than clamped,
/// for the same reason a subscription's is: the URI decides which socket a
/// datagram is sent to or expected from, and a silently truncated one names a
/// destination nobody asked for.
pub fn decode_destination_command(payload: &[u8]) -> Option<DestinationCommandReceived<'_>> {
    let correlated = decode_correlated(payload)?;
    let registration_id = le_i64(payload, CORRELATED_COMMAND_LENGTH)?;
    let channel_length = usize::try_from(le_i32(payload, CORRELATED_COMMAND_LENGTH + 8)?).ok()?;
    let channel = payload.get(
        DESTINATION_COMMAND_HEADER_LENGTH
            ..DESTINATION_COMMAND_HEADER_LENGTH.checked_add(channel_length)?,
    )?;

    Some(DestinationCommandReceived {
        client_id: correlated.client_id,
        correlation_id: correlated.correlation_id,
        registration_id,
        channel,
    })
}

/// `REMOVE_DESTINATION_BY_ID` as a client writes it.
///
/// There is no channel to write: the two registration ids are the whole request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DestinationByIdCommand {
    /// Who is asking.
    pub client_id: i64,
    /// What the answer is matched to.
    pub correlation_id: i64,
    /// The publication or subscription the destination was added to.
    pub resource_registration_id: i64,
    /// The destination, as the reply to its `ADD` named it.
    pub destination_registration_id: i64,
}

impl DestinationByIdCommand {
    /// How many bytes this command occupies in a record payload. Fixed, because
    /// this record has no variable-length part.
    pub const ENCODED_LENGTH: usize = DESTINATION_BY_ID_COMMAND_LENGTH;

    /// Write the payload into `out`, which must be exactly
    /// [`DestinationByIdCommand::ENCODED_LENGTH`] bytes.
    pub fn encode_into(&self, out: &mut [u8]) -> bool {
        if out.len() != Self::ENCODED_LENGTH {
            return false;
        }

        out[0..8].copy_from_slice(&self.client_id.to_le_bytes());
        out[8..16].copy_from_slice(&self.correlation_id.to_le_bytes());
        out[16..24].copy_from_slice(&self.resource_registration_id.to_le_bytes());
        out[24..32].copy_from_slice(&self.destination_registration_id.to_le_bytes());

        true
    }
}

/// Decode `REMOVE_DESTINATION_BY_ID`.
pub fn decode_destination_by_id_command(payload: &[u8]) -> Option<DestinationByIdCommand> {
    let correlated = decode_correlated(payload)?;

    Some(DestinationByIdCommand {
        client_id: correlated.client_id,
        correlation_id: correlated.correlation_id,
        resource_registration_id: le_i64(payload, CORRELATED_COMMAND_LENGTH)?,
        destination_registration_id: le_i64(payload, CORRELATED_COMMAND_LENGTH + 8)?,
    })
}

/// Decode a response payload.
///
/// A payload too short for its own type is reported as [`Response::Other`]
/// rather than trusted: these bytes come from another process, and the
/// reference checks the length before casting in every case.
pub fn decode_response(type_id: i32, payload: &[u8]) -> Response<'_> {
    match type_id {
        ON_SUBSCRIPTION_READY_TYPE_ID => match (le_i64(payload, 0), le_i32(payload, 8)) {
            (Some(correlation_id), Some(channel_status_indicator_id)) => {
                Response::SubscriptionReady {
                    correlation_id,
                    channel_status_indicator_id,
                }
            }
            _ => Response::Other { type_id },
        },
        ON_COUNTER_READY_TYPE_ID => match (le_i64(payload, 0), le_i32(payload, 8)) {
            (Some(correlation_id), Some(counter_id)) => Response::CounterReady {
                correlation_id,
                counter_id,
            },
            _ => Response::Other { type_id },
        },
        ON_UNAVAILABLE_COUNTER_TYPE_ID => match (le_i64(payload, 0), le_i32(payload, 8)) {
            (Some(correlation_id), Some(counter_id)) => Response::CounterUnavailable {
                correlation_id,
                counter_id,
            },
            _ => Response::Other { type_id },
        },
        ON_OPERATION_SUCCEEDED_TYPE_ID => match le_i64(payload, 0) {
            Some(correlation_id) => Response::OperationSucceeded { correlation_id },
            None => Response::Other { type_id },
        },
        ON_PUBLICATION_READY_TYPE_ID | ON_EXCLUSIVE_PUBLICATION_READY_TYPE_ID => {
            decode_publication_ready(type_id, payload)
        }
        ON_AVAILABLE_IMAGE_TYPE_ID => decode_available_image(payload),
        ON_UNAVAILABLE_IMAGE_TYPE_ID => {
            let publication_registration_id = le_i64(payload, 0);
            let subscription_registration_id = le_i64(payload, 8);
            let stream_id = le_i32(payload, 16);
            let channel_length = le_i32(payload, 20);

            match (
                publication_registration_id,
                subscription_registration_id,
                stream_id,
                channel_length,
            ) {
                (
                    Some(publication_registration_id),
                    Some(subscription_registration_id),
                    Some(stream_id),
                    Some(channel_length),
                ) if channel_length >= 0 => {
                    let end = IMAGE_MESSAGE_LENGTH
                        .saturating_add(channel_length as usize)
                        .min(payload.len());
                    Response::UnavailableImage {
                        publication_registration_id,
                        subscription_registration_id,
                        stream_id,
                        channel: &payload[IMAGE_MESSAGE_LENGTH.min(payload.len())..end],
                    }
                }
                _ => Response::Other { type_id },
            }
        }
        ON_CLIENT_TIMEOUT_TYPE_ID => match le_i64(payload, 0) {
            Some(client_id) => Response::ClientTimeout { client_id },
            None => Response::Other { type_id },
        },
        ON_ERROR_TYPE_ID => {
            let offending = le_i64(payload, 0);
            let code = le_i32(payload, 8);
            let length = le_i32(payload, 12);

            match (offending, code, length) {
                (Some(offending_command_correlation_id), Some(error_code), Some(length))
                    if length >= 0 =>
                {
                    // The transport pads the record, so the declared length is
                    // the authority on where the message ends — and it may run
                    // past what arrived, which is a truncated message rather
                    // than a reason to read beyond it.
                    let end = (16usize).saturating_add(length as usize).min(payload.len());
                    Response::Error {
                        offending_command_correlation_id,
                        error_code,
                        message: &payload[16.min(payload.len())..end],
                    }
                }
                _ => Response::Other { type_id },
            }
        }
        _ => Response::Other { type_id },
    }
}

/// Decode `ON_PUBLICATION_READY` or its exclusive twin, whose path is appended
/// with no alignment.
///
/// The type id is carried through rather than assumed, so a malformed payload
/// is reported under the id it actually arrived with — the exclusive form is a
/// different response and a client counting them wants to know which one broke.
fn decode_publication_ready(type_id: i32, payload: &[u8]) -> Response<'_> {
    let (
        Some(correlation_id),
        Some(registration_id),
        Some(session_id),
        Some(stream_id),
        Some(position_limit_counter_id),
        Some(channel_status_indicator_id),
        Some(log_file_length),
    ) = (
        le_i64(payload, 0),
        le_i64(payload, 8),
        le_i32(payload, 16),
        le_i32(payload, 20),
        le_i32(payload, 24),
        le_i32(payload, 28),
        le_i32(payload, 32),
    )
    else {
        return Response::Other { type_id };
    };

    let start = PUBLICATION_BUFFERS_READY_LENGTH;
    let end = start.saturating_add(log_file_length.max(0) as usize);
    if log_file_length < 0 || end > payload.len() {
        return Response::Other { type_id };
    }

    Response::PublicationReady {
        correlation_id,
        registration_id,
        session_id,
        stream_id,
        position_limit_counter_id,
        channel_status_indicator_id,
        log_file: &payload[start..end],
    }
}

/// The fixed head of `ON_AVAILABLE_IMAGE`
/// (`aeron_image_buffers_ready_t`, `aeron_control_protocol.h:113-119`), and
/// the two strings that follow it.
///
/// # The tail is not what it looks like
///
/// The head is 28 bytes under the header's `#pragma pack(4)`. After it comes a
/// length-prefixed log file path, then **padding to a four-byte boundary**,
/// then a length-prefixed source identity — and the identity is the one string
/// that is *not* padded, because nothing follows it
/// (`on_available_image`, `aeron_driver_conductor.c:2550-2569`).
///
/// [`ImageBuffersReady::encode`] and `decode_available_image` are the two
/// halves of this and are tested against each other; a decoder that read the
/// identity where the padding ends would read four bytes of the path's tail as
/// a length.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ImageBuffersReady<'a> {
    /// The **publication's** registration id: the log file is named after it,
    /// and this is what an image reports as its own correlation id.
    pub correlation_id: i64,
    /// The session the publication runs under.
    pub session_id: i32,
    /// The stream being read.
    pub stream_id: i32,
    /// The **subscription's** registration id — the client's own correlation
    /// id for the `ADD_SUBSCRIPTION`, which is how a client with several
    /// subscriptions on one stream tells the images apart.
    pub subscriber_registration_id: i64,
    /// The `sub-pos` counter this subscription reads through, one per
    /// (subscription, publication) pair.
    pub subscriber_position_id: i32,
    /// The log buffer's path: raw bytes, **not** NUL-terminated, padded to a
    /// four-byte boundary before the identity that follows.
    pub log_file: &'a [u8],
    /// Where the stream comes from. For an IPC channel this is the constant
    /// `"aeron:ipc"` and not the channel the client wrote
    /// (`aeron_driver_conductor.c:3612-3613` passes `AERON_IPC_CHANNEL`).
    pub source_identity: &'a [u8],
}

impl ImageBuffersReady<'_> {
    /// The payload, including the padding the decoder has to skip.
    pub fn encode(&self) -> Vec<u8> {
        #[allow(clippy::cast_possible_truncation)] // lengths this build forms
        let mut out = vec![
            0u8;
            IMAGE_BUFFERS_READY_LENGTH
                + 4
                + align_up_four(self.log_file.len())
                + 4
                + self.source_identity.len()
        ];

        out[0..8].copy_from_slice(&self.correlation_id.to_le_bytes());
        out[8..12].copy_from_slice(&self.session_id.to_le_bytes());
        out[12..16].copy_from_slice(&self.stream_id.to_le_bytes());
        out[16..24].copy_from_slice(&self.subscriber_registration_id.to_le_bytes());
        out[24..28].copy_from_slice(&self.subscriber_position_id.to_le_bytes());

        let path_length = IMAGE_BUFFERS_READY_LENGTH;
        out[path_length..path_length + 4]
            .copy_from_slice(&(self.log_file.len() as i32).to_le_bytes());
        out[path_length + 4..path_length + 4 + self.log_file.len()].copy_from_slice(self.log_file);

        // The padding bytes between the path and the identity are zero here;
        // the reference's buffer is a stack array it does not clear, so those
        // four bytes are whatever was on its stack. Nothing reads them.
        let identity_length = path_length + 4 + align_up_four(self.log_file.len());
        out[identity_length..identity_length + 4]
            .copy_from_slice(&(self.source_identity.len() as i32).to_le_bytes());
        out[identity_length + 4..].copy_from_slice(self.source_identity);

        out
    }
}

/// The payload of `ON_UNAVAILABLE_IMAGE`
/// (`aeron_image_message_t`, `aeron_control_protocol.h:153-159`): which image
/// went away, and which of the client's subscriptions was reading it.
///
/// The channel that follows the fixed head is the **subscription's**, not the
/// publication's — it is the channel the client subscribed with, echoed back —
/// and it is not aligned and not NUL-terminated
/// (`on_unavailable_image`, `aeron_driver_conductor.c:2550-2569`).
pub fn encode_unavailable_image(
    correlation_id: i64,
    subscription_registration_id: i64,
    stream_id: i32,
    channel: &[u8],
) -> Vec<u8> {
    #[allow(clippy::cast_possible_truncation)] // a channel from a command, far below i32::MAX
    let mut out = vec![0u8; IMAGE_MESSAGE_LENGTH + channel.len()];

    out[0..8].copy_from_slice(&correlation_id.to_le_bytes());
    out[8..16].copy_from_slice(&subscription_registration_id.to_le_bytes());
    out[16..20].copy_from_slice(&stream_id.to_le_bytes());
    out[20..24].copy_from_slice(&(channel.len() as i32).to_le_bytes());
    out[IMAGE_MESSAGE_LENGTH..].copy_from_slice(channel);

    out
}

/// Decode `ON_AVAILABLE_IMAGE`, whose variable tail is two length-prefixed
/// strings with four-byte alignment between them.
fn decode_available_image(payload: &[u8]) -> Response<'_> {
    let publication_registration_id = le_i64(payload, 0);
    let session_id = le_i32(payload, 8);
    let stream_id = le_i32(payload, 12);
    let subscriber_registration_id = le_i64(payload, 16);
    let subscriber_position_id = le_i32(payload, 24);
    let log_file_length = le_i32(payload, IMAGE_BUFFERS_READY_LENGTH);

    let (
        Some(publication_registration_id),
        Some(session_id),
        Some(stream_id),
        Some(subscriber_registration_id),
        Some(subscriber_position_id),
        Some(log_file_length),
    ) = (
        publication_registration_id,
        session_id,
        stream_id,
        subscriber_registration_id,
        subscriber_position_id,
        log_file_length,
    )
    else {
        return Response::Other {
            type_id: ON_AVAILABLE_IMAGE_TYPE_ID,
        };
    };

    if log_file_length < 0 {
        return Response::Other {
            type_id: ON_AVAILABLE_IMAGE_TYPE_ID,
        };
    }

    let log_file_start = IMAGE_BUFFERS_READY_LENGTH + 4;
    let log_file_end = log_file_start.saturating_add(log_file_length as usize);
    if log_file_end > payload.len() {
        return Response::Other {
            type_id: ON_AVAILABLE_IMAGE_ID_FOR_MALFORMED,
        };
    }

    // The path is padded to a four-byte boundary before the next length, and
    // skipping that padding is what keeps the source identity from being read
    // as garbage.
    let identity_length_offset = align_up_four(log_file_end);
    let Some(identity_length) = le_i32(payload, identity_length_offset) else {
        return Response::Other {
            type_id: ON_AVAILABLE_IMAGE_ID_FOR_MALFORMED,
        };
    };
    if identity_length < 0 {
        return Response::Other {
            type_id: ON_AVAILABLE_IMAGE_ID_FOR_MALFORMED,
        };
    }

    let identity_start = identity_length_offset + 4;
    let identity_end = identity_start
        .saturating_add(identity_length as usize)
        .min(payload.len());

    Response::AvailableImage {
        publication_registration_id,
        session_id,
        stream_id,
        subscriber_registration_id,
        subscriber_position_id,
        log_file: &payload[log_file_start..log_file_end],
        source_identity: &payload[identity_start.min(payload.len())..identity_end],
    }
}

/// The `msg_type_id` a malformed available-image is reported under.
const ON_AVAILABLE_IMAGE_ID_FOR_MALFORMED: i32 = ON_AVAILABLE_IMAGE_TYPE_ID;

/// Round up to a four-byte boundary, the alignment the reference uses between
/// the two strings (`AERON_ALIGN(x, sizeof(int32_t))`).
const fn align_up_four(value: usize) -> usize {
    (value + 3) & !3
}

fn le_i32(payload: &[u8], offset: usize) -> Option<i32> {
    let bytes = payload.get(offset..offset + 4)?;
    Some(i32::from_le_bytes(bytes.try_into().ok()?))
}

fn le_i64(payload: &[u8], offset: usize) -> Option<i64> {
    let bytes = payload.get(offset..offset + 8)?;
    Some(i64::from_le_bytes(bytes.try_into().ok()?))
}

#[cfg(test)]
mod response_tests {
    use super::*;

    fn payload(parts: &[&[u8]]) -> Vec<u8> {
        parts.concat()
    }

    #[test]
    fn decodes_a_subscription_ready() {
        let bytes = payload(&[&7i64.to_le_bytes(), &(-1i32).to_le_bytes()]);
        assert_eq!(
            Response::SubscriptionReady {
                correlation_id: 7,
                channel_status_indicator_id: CHANNEL_STATUS_INDICATOR_NOT_ALLOCATED,
            },
            decode_response(ON_SUBSCRIPTION_READY_TYPE_ID, &bytes)
        );
    }

    #[test]
    fn decodes_the_counter_ready_that_precedes_it() {
        // A fresh client's first event is its own heartbeat counter, with the
        // *client id* in the correlation field.
        let bytes = payload(&[&42i64.to_le_bytes(), &9i32.to_le_bytes()]);
        assert_eq!(
            Response::CounterReady {
                correlation_id: 42,
                counter_id: 9
            },
            decode_response(ON_COUNTER_READY_TYPE_ID, &bytes)
        );
    }

    #[test]
    fn decodes_an_error_with_its_message() {
        let message = b"unknown subscription";
        let mut bytes = payload(&[
            &3i64.to_le_bytes(),
            &(-5i32).to_le_bytes(),
            &(message.len() as i32).to_le_bytes(),
        ]);
        bytes.extend_from_slice(message);

        match decode_response(ON_ERROR_TYPE_ID, &bytes) {
            Response::Error {
                offending_command_correlation_id,
                error_code,
                message: text,
            } => {
                assert_eq!(3, offending_command_correlation_id);
                assert_eq!(-5, error_code);
                assert_eq!(b"unknown subscription", text);
            }
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[test]
    fn an_error_claiming_more_text_than_arrived_is_truncated_not_overrun() {
        let mut bytes = payload(&[
            &0i64.to_le_bytes(),
            &(-5i32).to_le_bytes(),
            &4096i32.to_le_bytes(),
        ]);
        bytes.extend_from_slice(b"short");

        match decode_response(ON_ERROR_TYPE_ID, &bytes) {
            Response::Error { message, .. } => assert_eq!(b"short", message),
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[test]
    fn a_payload_too_short_for_its_own_type_is_not_trusted() {
        // Every one of these arrives from another process, so a short frame is
        // a thing that happens rather than a thing that cannot.
        for (type_id, bytes) in [
            (ON_SUBSCRIPTION_READY_TYPE_ID, vec![0u8; 8]),
            (ON_COUNTER_READY_TYPE_ID, vec![0u8; 4]),
            (ON_UNAVAILABLE_COUNTER_TYPE_ID, vec![0u8; 4]),
            (ON_CLIENT_TIMEOUT_TYPE_ID, vec![0u8; 7]),
            (ON_ERROR_TYPE_ID, vec![0u8; 12]),
        ] {
            assert!(
                matches!(decode_response(type_id, &bytes), Response::Other { .. }),
                "type {type_id:#x} with {} bytes should not decode",
                bytes.len()
            );
        }
    }

    #[test]
    fn decodes_an_add_counter_with_the_key_padded_to_four() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&7i64.to_le_bytes()); // client_id
        payload.extend_from_slice(&9i64.to_le_bytes()); // correlation_id
        payload.extend_from_slice(&11i32.to_le_bytes()); // type_id
        payload.extend_from_slice(&5i32.to_le_bytes()); // key_length
        payload.extend_from_slice(b"abcde"); // key, five bytes
        payload.extend_from_slice(&[0, 0, 0]); // padded to eight
        payload.extend_from_slice(&3i32.to_le_bytes()); // label_length
        payload.extend_from_slice(b"lbl");

        let decoded = decode_add_counter(&payload).expect("decodes");
        assert_eq!(
            Correlated {
                client_id: 7,
                correlation_id: 9,
            },
            decoded.correlated
        );
        assert_eq!(11, decoded.type_id);
        assert_eq!(b"abcde", decoded.key);
        assert_eq!(b"lbl", decoded.label);

        // A payload shorter than its own lengths claim is refused rather than
        // sliced out of range.
        let mut truncated = payload.clone();
        truncated.truncate(payload.len() - 1);
        assert!(decode_add_counter(&truncated).is_none());

        // And a negative length is not a length.
        let mut negative = payload.clone();
        negative[20..24].copy_from_slice(&(-1i32).to_le_bytes());
        assert!(decode_add_counter(&negative).is_none());
        assert!(
            decode_add_counter(&[0u8; 12]).is_none(),
            "no room for a head"
        );
    }

    #[test]
    fn decodes_a_remove_counter_by_registration_id() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&7i64.to_le_bytes());
        payload.extend_from_slice(&9i64.to_le_bytes());
        payload.extend_from_slice(&42i64.to_le_bytes());

        assert_eq!(
            Some(RemoveCounter {
                correlated: Correlated {
                    client_id: 7,
                    correlation_id: 9,
                },
                registration_id: 42,
            }),
            decode_remove_counter(&payload)
        );
        assert!(decode_remove_counter(&payload[..20]).is_none());
    }

    #[test]
    fn an_add_counter_encodes_the_shape_the_decoder_reads() {
        // A key that is not a multiple of four, so the padding is exercised:
        // the reference writes the key, pads to four, then the label's length
        // (`aeron_client_conductor.c:2035-2052`), and the decoder steps by the
        // aligned length.
        let command = AddCounter {
            correlated: Correlated {
                client_id: 7,
                correlation_id: 42,
            },
            type_id: 100,
            key: b"key",
            label: b"a counter",
        };

        let mut out = vec![0u8; command.encoded_length()];
        assert!(command.encode_into(&mut out));

        // The head (correlated, type, key length), the key padded to four,
        // the label's length, the label.
        assert_eq!(24 + 4 + 4 + 9, out.len());
        assert_eq!(7i64.to_le_bytes(), out[0..8]);
        assert_eq!(42i64.to_le_bytes(), out[8..16]);
        assert_eq!(100i32.to_le_bytes(), out[16..20]);
        assert_eq!(3i32.to_le_bytes(), out[20..24]);
        assert_eq!(b"key\0", &out[24..28], "the key, padded to four");
        assert_eq!(9i32.to_le_bytes(), out[28..32]);
        assert_eq!(b"a counter", &out[32..], "no NUL, exactly the length");

        assert_eq!(
            Some(command),
            decode_add_counter(&out),
            "what the client writes is what the driver reads"
        );

        // And a buffer of the wrong size is refused rather than trusted.
        assert!(!command.encode_into(&mut vec![0u8; out.len() + 1]));
    }

    #[test]
    fn a_remove_counter_encodes_the_shape_the_decoder_reads() {
        let command = RemoveCounter {
            correlated: Correlated {
                client_id: 7,
                correlation_id: 9,
            },
            registration_id: 42,
        };

        let mut out = vec![0u8; command.encoded_length()];
        assert!(command.encode_into(&mut out));

        assert_eq!(24, out.len());
        assert_eq!(7i64.to_le_bytes(), out[0..8]);
        assert_eq!(9i64.to_le_bytes(), out[8..16]);
        assert_eq!(42i64.to_le_bytes(), out[16..24]);

        assert_eq!(Some(command), decode_remove_counter(&out));
        assert!(!command.encode_into(&mut [0u8; 23]));
    }

    #[test]
    fn the_encoders_round_trip_through_the_decoder() {
        let ready = encode_counter_update(7, 42);
        assert_eq!(12, ready.len(), "packed(4), so twelve and not sixteen");
        assert_eq!(
            Response::CounterReady {
                correlation_id: 7,
                counter_id: 42,
            },
            decode_response(ON_COUNTER_READY_TYPE_ID, &ready)
        );

        // The removal type carries the same twelve bytes the ready type does,
        // and decodes to its own variant: an event for the counter watchers,
        // never a reply to a command — nothing waits on one.
        assert_eq!(
            Response::CounterUnavailable {
                correlation_id: 7,
                counter_id: 42,
            },
            decode_response(ON_UNAVAILABLE_COUNTER_TYPE_ID, &ready)
        );

        let succeeded = encode_operation_succeeded(9);
        assert_eq!(8, succeeded.len());
        assert_eq!(
            Response::OperationSucceeded { correlation_id: 9 },
            decode_response(ON_OPERATION_SUCCEEDED_TYPE_ID, &succeeded)
        );

        let timeout = encode_client_timeout(9);
        assert_eq!(8, timeout.len());
        assert_eq!(
            Response::ClientTimeout { client_id: 9 },
            decode_response(ON_CLIENT_TIMEOUT_TYPE_ID, &timeout)
        );
    }

    #[test]
    fn an_unrecognised_type_is_reported_not_swallowed() {
        assert_eq!(
            Response::Other { type_id: 0x0F03 },
            decode_response(0x0F03, &[0u8; 32]),
            "the caller counts these; the decoder does not hide them"
        );
    }

    #[test]
    fn encodes_a_subscription_without_a_terminator() {
        let command = AddSubscription {
            client_id: 1,
            correlation_id: 2,
            registration_correlation_id: -1,
            stream_id: 1001,
            channel: "aeron:ipc",
        };
        let mut out = vec![0u8; command.encoded_length()];
        assert!(command.encode_into(&mut out));

        assert_eq!(32 + 9, out.len(), "the header plus the raw channel bytes");
        assert_eq!(1i64.to_le_bytes(), out[0..8]);
        assert_eq!(2i64.to_le_bytes(), out[8..16]);
        assert_eq!((-1i64).to_le_bytes(), out[16..24], "the dead field");
        assert_eq!(1001i32.to_le_bytes(), out[24..28]);
        assert_eq!(9i32.to_le_bytes(), out[28..32]);
        assert_eq!(b"aeron:ipc", &out[32..], "no NUL, exactly the length");
    }

    #[test]
    fn a_removal_is_read_with_and_without_its_flags_word() {
        // The current shape: 32 bytes.
        let mut payload = Vec::new();
        payload.extend_from_slice(&7i64.to_le_bytes());
        payload.extend_from_slice(&9i64.to_le_bytes());
        payload.extend_from_slice(&42i64.to_le_bytes());
        payload.extend_from_slice(&REMOVE_PUBLICATION_FLAG_REVOKE.to_le_bytes());

        assert_eq!(
            Some(RemovePublication {
                correlated: Correlated {
                    client_id: 7,
                    correlation_id: 9,
                },
                registration_id: 42,
                flags: REMOVE_PUBLICATION_FLAG_REVOKE,
            }),
            decode_remove_publication(&payload)
        );

        // And the older one, which ends where the flags would have begun: a
        // client built before the word existed still gets its publication
        // removed, with no revocation.
        assert_eq!(
            Some(RemovePublication {
                correlated: Correlated {
                    client_id: 7,
                    correlation_id: 9,
                },
                registration_id: 42,
                flags: 0,
            }),
            decode_remove_publication(&payload[..24])
        );

        assert!(decode_remove_publication(&payload[..20]).is_none());

        assert_eq!(
            Some(RemoveSubscription {
                correlated: Correlated {
                    client_id: 7,
                    correlation_id: 9,
                },
                registration_id: 42,
            }),
            decode_remove_subscription(&payload[..24])
        );
    }

    #[test]
    fn the_unavailable_image_carries_the_subscriptions_channel_unaligned() {
        let channel = b"aeron:ipc?session-id=5";
        let message = encode_unavailable_image(42, 9, 1001, channel);

        assert_eq!(24 + channel.len(), message.len(), "no padding, no NUL");
        assert_eq!(42i64.to_le_bytes(), message[0..8], "the publication");
        assert_eq!(9i64.to_le_bytes(), message[8..16], "the subscription");
        assert_eq!((channel.len() as i32).to_le_bytes(), message[20..24]);
        assert_eq!(channel, &message[24..]);
    }

    #[test]
    fn a_subscription_command_is_what_the_client_encoder_writes() {
        let request = AddSubscription {
            client_id: 7,
            correlation_id: 9,
            registration_correlation_id: -1,
            stream_id: 1001,
            channel: "aeron:ipc?session-id=5",
        };
        let mut out = vec![0u8; request.encoded_length()];
        assert!(request.encode_into(&mut out));

        assert_eq!(32 + 22, out.len());
        assert_eq!(
            Some(AddSubscriptionCommand {
                client_id: 7,
                correlation_id: 9,
                registration_correlation_id: -1,
                stream_id: 1001,
                channel: b"aeron:ipc?session-id=5",
            }),
            decode_add_subscription(&out)
        );

        // A channel length that runs past the payload is refused, as it is for
        // a publication.
        let mut short = vec![0u8; ADD_SUBSCRIPTION_HEADER_LENGTH];
        short[28..32].copy_from_slice(&4096i32.to_le_bytes());
        assert!(decode_add_subscription(&short).is_none());
    }

    #[test]
    fn the_subscription_ready_is_the_correlated_head_and_a_status_counter() {
        let ready = encode_subscription_ready(9, CHANNEL_STATUS_INDICATOR_NOT_ALLOCATED);

        assert_eq!(12, ready.len(), "packed(4)");
        assert_eq!(
            Response::SubscriptionReady {
                correlation_id: 9,
                channel_status_indicator_id: CHANNEL_STATUS_INDICATOR_NOT_ALLOCATED,
            },
            decode_response(ON_SUBSCRIPTION_READY_TYPE_ID, &ready)
        );
    }

    #[test]
    fn the_available_image_pads_its_path_and_not_its_identity() {
        // The path's length is chosen so that the padding is *not* zero: a
        // decoder that skipped the alignment, or one that applied it to the
        // identity as well, reads a different string here.
        for path in [
            "/tmp/aeron/publications/42.logbuffer",
            "/tmp/aeron/publications/7.logbuffer",
            "/p",
        ] {
            let ready = ImageBuffersReady {
                correlation_id: 42,
                session_id: 100,
                stream_id: 1001,
                subscriber_registration_id: 9,
                subscriber_position_id: 3,
                log_file: path.as_bytes(),
                source_identity: b"aeron:ipc",
            }
            .encode();

            assert_eq!(
                28 + 4 + align_up_four(path.len()) + 4 + 9,
                ready.len(),
                "{path}"
            );

            // And it decodes back to what was encoded, through the client's own
            // decoder — the two halves of one message, checked against each
            // other rather than against a transcription of the layout.
            assert_eq!(
                Response::AvailableImage {
                    publication_registration_id: 42,
                    session_id: 100,
                    stream_id: 1001,
                    subscriber_registration_id: 9,
                    subscriber_position_id: 3,
                    log_file: path.as_bytes(),
                    source_identity: b"aeron:ipc",
                },
                decode_response(ON_AVAILABLE_IMAGE_TYPE_ID, &ready),
                "{path}"
            );
        }
    }

    #[test]
    fn a_publication_command_is_what_the_client_encoder_writes() {
        // The two directions are written in different crates and have to agree
        // on the same 24-byte head. Encoding and decoding one set of values is
        // what pins them to each other — a field either side inserts or drops
        // shows up here as a mismatch rather than as a publication the driver
        // creates with the wrong stream.
        let request = AddPublication {
            client_id: 7,
            correlation_id: 9,
            stream_id: 1001,
            channel: "aeron:ipc?session-id=5",
        };
        let mut out = vec![0u8; request.encoded_length()];
        assert!(request.encode_into(&mut out));

        assert_eq!(24 + 22, out.len());
        assert_eq!(
            Some(AddPublicationCommand {
                client_id: 7,
                correlation_id: 9,
                stream_id: 1001,
                channel: b"aeron:ipc?session-id=5",
            }),
            decode_add_publication(&out)
        );
    }

    #[test]
    fn a_channel_length_past_the_payload_is_refused() {
        // A URI is what the driver writes into counters other processes read,
        // and it forms a file name. Taking a short one silently would produce a
        // publication nothing can be matched against.
        let mut payload = vec![0u8; ADD_PUBLICATION_HEADER_LENGTH];
        payload[16..20].copy_from_slice(&1001i32.to_le_bytes());
        payload[20..24].copy_from_slice(&4096i32.to_le_bytes());

        assert!(decode_add_publication(&payload).is_none());
        assert!(
            decode_add_publication(&payload[..16]).is_none(),
            "not even the head"
        );
    }

    #[test]
    fn the_publication_ready_is_thirty_six_bytes_plus_an_unaligned_path() {
        let path = b"/tmp/aeron/publications/42.logbuffer";
        let ready = PublicationBuffersReady {
            correlation_id: 7,
            registration_id: 42,
            session_id: 100,
            stream_id: 1001,
            position_limit_counter_id: 3,
            channel_status_indicator_id: CHANNEL_STATUS_INDICATOR_NOT_ALLOCATED,
            log_file: path,
        }
        .encode();

        assert_eq!(36 + path.len(), ready.len(), "no padding after the path");
        assert_eq!(7i64.to_le_bytes(), ready[0..8]);
        assert_eq!(42i64.to_le_bytes(), ready[8..16]);
        assert_eq!(100i32.to_le_bytes(), ready[16..20], "session before stream");
        assert_eq!(1001i32.to_le_bytes(), ready[20..24]);
        assert_eq!(3i32.to_le_bytes(), ready[24..28]);
        assert_eq!((-1i32).to_le_bytes(), ready[28..32], "no channel status");
        assert_eq!((path.len() as i32).to_le_bytes(), ready[32..36]);
        assert_eq!(path, &ready[36..]);

        // And it is the shape this build's client decodes.
        assert_eq!(
            Response::PublicationReady {
                correlation_id: 7,
                registration_id: 42,
                session_id: 100,
                stream_id: 1001,
                position_limit_counter_id: 3,
                channel_status_indicator_id: CHANNEL_STATUS_INDICATOR_NOT_ALLOCATED,
                log_file: path,
            },
            decode_response(ON_PUBLICATION_READY_TYPE_ID, &ready)
        );
        assert_eq!(
            Response::PublicationReady {
                correlation_id: 7,
                registration_id: 42,
                session_id: 100,
                stream_id: 1001,
                position_limit_counter_id: 3,
                channel_status_indicator_id: CHANNEL_STATUS_INDICATOR_NOT_ALLOCATED,
                log_file: path,
            },
            decode_response(ON_EXCLUSIVE_PUBLICATION_READY_TYPE_ID, &ready),
            "the exclusive reply carries the same payload under a different id"
        );
    }

    /// The record a destination command is, byte for byte, against the layout
    /// the reference declares under `#pragma pack(4)`
    /// (`aeron_control_protocol.h:162-168`).
    ///
    /// The offsets below were **measured** against that header on this machine,
    /// not read off the pragma: `sizeof(aeron_destination_command_t)` is 28,
    /// `registration_id` sits at 16 and `channel_length` at 24. Declaring the
    /// same fields without the pragma gives 32 and moves the URI to 32 — which
    /// is why the number is worth pinning to a real compile rather than to a
    /// reading of the source.
    #[test]
    fn a_destination_command_is_the_record_the_reference_declares() {
        let command = DestinationCommand {
            client_id: 7,
            correlation_id: 9,
            registration_id: 42,
            channel: "aeron:udp?endpoint=localhost:40456",
        };
        let mut out = vec![0u8; command.encoded_length()];
        assert!(command.encode_into(&mut out));

        assert_eq!(
            28 + 34,
            out.len(),
            "28 bytes of header, then the URI with no terminator"
        );
        assert_eq!(7i64.to_le_bytes(), out[0..8], "client_id");
        assert_eq!(9i64.to_le_bytes(), out[8..16], "correlation_id");
        assert_eq!(42i64.to_le_bytes(), out[16..24], "registration_id at 16");
        assert_eq!(34i32.to_le_bytes(), out[24..28], "channel_length at 24");
        assert_eq!(
            b"aeron:udp?endpoint=localhost:40456",
            &out[28..],
            "the URI begins at 28, where the packed struct ends"
        );
    }

    /// `aeron_destination_by_id_command_t`: 32 bytes and no channel
    /// (`aeron_control_protocol.h:170-176`).
    #[test]
    fn a_destination_by_id_command_is_the_record_the_reference_declares() {
        let command = DestinationByIdCommand {
            client_id: 7,
            correlation_id: 9,
            resource_registration_id: 42,
            destination_registration_id: 43,
        };
        let mut out = vec![0u8; DestinationByIdCommand::ENCODED_LENGTH];
        assert!(command.encode_into(&mut out));

        assert_eq!(32, out.len(), "no channel, so a fixed 32 bytes");
        assert_eq!(7i64.to_le_bytes(), out[0..8], "client_id");
        assert_eq!(9i64.to_le_bytes(), out[8..16], "correlation_id");
        assert_eq!(42i64.to_le_bytes(), out[16..24], "the resource at 16");
        assert_eq!(43i64.to_le_bytes(), out[24..32], "the destination at 24");
    }

    #[test]
    fn a_destination_command_round_trips_through_its_decoder() {
        let command = DestinationCommand {
            client_id: 7,
            correlation_id: 9,
            registration_id: 42,
            channel: "aeron:udp?endpoint=localhost:40456",
        };
        let mut out = vec![0u8; command.encoded_length()];
        assert!(command.encode_into(&mut out));

        assert_eq!(
            Some(DestinationCommandReceived {
                client_id: 7,
                correlation_id: 9,
                registration_id: 42,
                channel: b"aeron:udp?endpoint=localhost:40456",
            }),
            decode_destination_command(&out)
        );
    }

    #[test]
    fn a_destination_by_id_command_round_trips_through_its_decoder() {
        let command = DestinationByIdCommand {
            client_id: 7,
            correlation_id: 9,
            resource_registration_id: 42,
            destination_registration_id: 43,
        };
        let mut out = vec![0u8; DestinationByIdCommand::ENCODED_LENGTH];
        assert!(command.encode_into(&mut out));

        assert_eq!(Some(command), decode_destination_by_id_command(&out));
    }

    /// The boundary the length check sits on. A destination with an empty URI is
    /// not something a client sends — the parser would refuse it — but a decoder
    /// that refused the *record* for it would be refusing for a reason that
    /// belongs to the layer above.
    #[test]
    fn a_destination_command_with_no_channel_still_decodes() {
        let command = DestinationCommand {
            client_id: 7,
            correlation_id: 9,
            registration_id: 42,
            channel: "",
        };
        let mut out = vec![0u8; command.encoded_length()];
        assert!(command.encode_into(&mut out));

        assert_eq!(28, out.len(), "the header alone");
        assert_eq!(
            Some(DestinationCommandReceived {
                client_id: 7,
                correlation_id: 9,
                registration_id: 42,
                channel: &[],
            }),
            decode_destination_command(&out)
        );
    }

    #[test]
    fn a_destination_channel_that_runs_past_the_payload_is_refused() {
        let command = DestinationCommand {
            client_id: 7,
            correlation_id: 9,
            registration_id: 42,
            channel: "aeron:ipc",
        };
        let mut out = vec![0u8; command.encoded_length()];
        assert!(command.encode_into(&mut out));
        assert!(
            decode_destination_command(&out).is_some(),
            "the record as written decodes"
        );

        // The same bytes with a length word claiming four more than are there.
        out[24..28].copy_from_slice(&(9i32 + 4).to_le_bytes());
        assert_eq!(
            None,
            decode_destination_command(&out),
            "a channel that runs past the payload is refused, not clamped"
        );

        // And a payload too short to hold the header at all.
        assert_eq!(None, decode_destination_command(&out[..27]));
        assert_eq!(
            None,
            decode_destination_by_id_command(&out[..31]),
            "the fixed record is refused when it is short too"
        );
    }

    #[test]
    fn the_destination_type_ids_are_the_references() {
        assert_eq!(
            (0x07, 0x08, 0x0C, 0x0D, 0x11),
            (
                ADD_DESTINATION_TYPE_ID,
                REMOVE_DESTINATION_TYPE_ID,
                ADD_RECEIVE_DESTINATION_TYPE_ID,
                REMOVE_RECEIVE_DESTINATION_TYPE_ID,
                REMOVE_DESTINATION_BY_ID_TYPE_ID,
            ),
            "aeron_control_protocol.h:32-42"
        );
    }
}
