//! The bounds table in front of the archive's response-side SBE decoders.
//!
//! The control requests have their own guard in
//! [`crate::server::control_adapter::control_request_bounds`]; this is the
//! other half of schema 101 — the messages the archive *sends* and the client
//! (or the server's response proxy) decodes. The generated decoders read fixed
//! fields at schema offsets and variable-length fields through `u32` prefixes
//! they do not bounds-check; the reference's Java throws where this build
//! panics, and these messages arrive over a UDP socket.
//!
//! The table is version-aware because the decoder skips a versioned field when
//! `acting_version < sinceVersion`: an older archive's response legitimately
//! carries a shorter fixed block. Rows are `(sinceVersion, blockLength,
//! variableFieldCount)`, ascending; the last row at or below the message's
//! version is the one the decoder will read.

use deepmsg_codec::archive::message_header_codec::{self, MessageHeaderDecoder};
use deepmsg_codec::archive::{ReadBuf, SBE_SCHEMA_ID};

/// A response was too short for the template it named.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BoundsError {
    /// Which part of the decode was being checked.
    pub what: &'static str,
    /// How many bytes the decoder needs from the start of the message.
    pub needed: usize,
    /// How many the payload has.
    pub available: usize,
}

impl core::fmt::Display for BoundsError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "{}: needed {} bytes, {} available",
            self.what, self.needed, self.available
        )
    }
}

impl std::error::Error for BoundsError {}

/// `(sinceVersion, blockLength, variableFieldCount)`, ascending by version.
type Rows = &'static [(u16, u16, u8)];

/// The rows that apply to `template_id`, or `None` when this build has no
/// decoder for it.
fn rows(template_id: u16) -> Option<Rows> {
    use deepmsg_codec::archive::*;

    Some(match template_id {
        control_response_codec::SBE_TEMPLATE_ID => &[(0, 28, 1), (4, 32, 1)],
        catalog_header_codec::SBE_TEMPLATE_ID => &[(0, 21, 0)],
        recording_descriptor_header_codec::SBE_TEMPLATE_ID => &[(0, 13, 0)],
        recording_descriptor_codec::SBE_TEMPLATE_ID => &[(0, 80, 3)],
        recording_subscription_descriptor_codec::SBE_TEMPLATE_ID => &[(0, 28, 1)],
        recording_signal_event_codec::SBE_TEMPLATE_ID => &[(0, 44, 0)],
        challenge_codec::SBE_TEMPLATE_ID => &[(0, 20, 1)],
        recording_started_codec::SBE_TEMPLATE_ID => &[(0, 24, 2)],
        recording_progress_codec::SBE_TEMPLATE_ID => &[(0, 24, 0)],
        recording_stopped_codec::SBE_TEMPLATE_ID => &[(0, 24, 0)],
        ping_codec::SBE_TEMPLATE_ID => &[(0, 8, 0)],
        _ => return None,
    })
}

/// The row a decoder at `acting_version` reads.
fn row_at(table: Rows, acting_version: u16) -> (u16, u8) {
    let mut chosen = (table[0].1, table[0].2);
    for &(since_version, block_length, variable_fields) in table {
        if since_version > acting_version {
            break;
        }
        chosen = (block_length, variable_fields);
    }

    chosen
}

/// Whether every variable-length field the decoder walks fits in `payload`,
/// each a `u32` length followed by that many bytes, starting at `at`.
fn variable_data_fits(payload: &[u8], at: usize, count: u8) -> bool {
    let mut at = at;
    for _ in 0..count {
        let Some(prefix_end) = at.checked_add(4) else {
            return false;
        };
        let Some(prefix) = payload.get(at..prefix_end) else {
            return false;
        };
        let length = u32::from_le_bytes([prefix[0], prefix[1], prefix[2], prefix[3]]);
        let Ok(length) = usize::try_from(length) else {
            return false;
        };
        let Some(next) = prefix_end.checked_add(length) else {
            return false;
        };
        if next > payload.len() {
            return false;
        }
        at = next;
    }

    true
}

/// The fixed-block and variable-field check for a message whose header and
/// schema the caller has already validated.
///
/// # Errors
///
/// [`BoundsError`] naming the fixed block or variable-field walk that does not
/// fit.
pub fn fits(payload: &[u8], header: &MessageHeaderDecoder<ReadBuf<'_>>) -> Result<(), BoundsError> {
    let Some(table) = rows(header.template_id()) else {
        return Ok(());
    };
    let (block_length, variable_fields) = row_at(table, header.version());

    let needed = message_header_codec::ENCODED_LENGTH + usize::from(block_length);
    if payload.len() < needed {
        return Err(BoundsError {
            what: "archive response fixed block",
            needed,
            available: payload.len(),
        });
    }

    if variable_fields > 0 {
        let at = message_header_codec::ENCODED_LENGTH + usize::from(header.block_length());
        if !variable_data_fits(payload, at, variable_fields) {
            return Err(BoundsError {
                what: "archive response variable field",
                needed: at.saturating_add(4),
                available: payload.len(),
            });
        }
    }

    Ok(())
}

/// The same check from the raw payload: header length, schema id, then
/// [`fits`]. This is the entrance the fuzz targets and callers without a
/// header decoder use.
///
/// # Errors
///
/// [`BoundsError`] for a payload shorter than a header, a foreign schema, or a
/// fixed/variable field that does not fit.
pub fn message_fits(payload: &[u8]) -> Result<(), BoundsError> {
    if payload.len() < message_header_codec::ENCODED_LENGTH {
        return Err(BoundsError {
            what: "archive response header",
            needed: message_header_codec::ENCODED_LENGTH,
            available: payload.len(),
        });
    }

    let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(payload), 0);
    if header.schema_id() != SBE_SCHEMA_ID {
        return Err(BoundsError {
            what: "archive response schema",
            needed: usize::from(SBE_SCHEMA_ID),
            available: usize::from(header.schema_id()),
        });
    }

    fits(payload, &header)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(template_id: u16, version: u16, block: u16, variables: u8) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&block.to_le_bytes());
        payload.extend_from_slice(&template_id.to_le_bytes());
        payload.extend_from_slice(&SBE_SCHEMA_ID.to_le_bytes());
        payload.extend_from_slice(&version.to_le_bytes());
        payload.resize(message_header_codec::ENCODED_LENGTH + usize::from(block), 0);
        for _ in 0..variables {
            payload.extend_from_slice(&0_u32.to_le_bytes());
        }

        payload
    }

    #[test]
    fn every_row_accepts_its_own_message_at_every_breakpoint() {
        for template_id in 0..=110 {
            let Some(table) = rows(template_id) else {
                continue;
            };
            for &(version, block, variables) in table {
                let payload = message(template_id, version, block, variables);
                assert_eq!(
                    Ok(()),
                    message_fits(&payload),
                    "template {template_id} version {version}"
                );

                let mut short = payload.clone();
                short.pop();
                assert!(
                    message_fits(&short).is_err(),
                    "template {template_id} version {version} accepted one byte short"
                );
            }
        }
    }

    #[test]
    fn an_older_control_response_may_carry_a_shorter_block() {
        let response = deepmsg_codec::archive::control_response_codec::SBE_TEMPLATE_ID;
        assert!(message_fits(&message(response, 0, 28, 1)).is_ok());
        assert!(message_fits(&message(response, 4, 32, 1)).is_ok());
    }

    #[test]
    fn a_variable_field_past_the_end_is_refused() {
        let mut payload = message(
            deepmsg_codec::archive::recording_descriptor_codec::SBE_TEMPLATE_ID,
            0,
            80,
            0,
        );
        payload.extend_from_slice(&u32::MAX.to_le_bytes());
        assert!(message_fits(&payload).is_err());
    }

    #[test]
    fn an_unknown_template_is_not_decoded_and_not_refused() {
        assert!(rows(999).is_none());
        assert!(message_fits(&message(999, 0, 0, 0)).is_ok());
    }
}
