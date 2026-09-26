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

/// `AERON_RESPONSE_ON_ERROR` (`aeron_control_protocol.h:46`).
pub const ON_ERROR_TYPE_ID: i32 = 0x0F01;

/// `AERON_RESPONSE_ON_COUNTER_READY` (`aeron_control_protocol.h:53`).
pub const ON_COUNTER_READY_TYPE_ID: i32 = 0x0F08;

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
        ON_PUBLICATION_READY_TYPE_ID => decode_publication_ready(payload),
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

/// Decode `ON_PUBLICATION_READY`, whose path is appended with no alignment.
fn decode_publication_ready(payload: &[u8]) -> Response<'_> {
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
        return Response::Other {
            type_id: ON_PUBLICATION_READY_TYPE_ID,
        };
    };

    let start = PUBLICATION_BUFFERS_READY_LENGTH;
    let end = start.saturating_add(log_file_length.max(0) as usize);
    if log_file_length < 0 || end > payload.len() {
        return Response::Other {
            type_id: ON_PUBLICATION_READY_TYPE_ID,
        };
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
}
