//! The bounds table in front of every cluster SBE decoder.
//!
//! The generated decoders read fixed fields at schema offsets and
//! variable-length fields through `u32` prefixes they do not bounds-check; the
//! reference's Java throws where this build panics. Cluster messages arrive
//! from UDP sockets and client publications, so a short message is ordinary
//! hostile input — every decode entry point calls [`message_fits`] (or
//! [`fits`] when it already holds the header) first.
//!
//! The table is version-aware because the decoder skips a versioned field when
//! `acting_version < sinceVersion`: an older peer's message legitimately
//! carries a shorter fixed block. Rows are `(sinceVersion, blockLength,
//! variableFields)`, in ascending order; the last row at or below the message's
//! version is the one the decoder will read.
//!
//! Four messages carry a repeating group (`BackupResponse`,
//! `SnapshotRecordings`, `ClusterMembersExtendedResponse`, `StandbySnapshot`).
//! Their variable fields after the group cannot be located without walking the
//! group, so the table gives them `variableFields = 0`: the fixed block is
//! checked here, and the callers that read the group do their own walk
//! (`backup/agent.rs`, `tool/operator.rs`).

use deepmsg_codec_cluster::message_header_codec::{self, MessageHeaderDecoder};
use deepmsg_codec_cluster::{ReadBuf, SBE_SCHEMA_ID};

use crate::decode::DecodeError;

/// `(sinceVersion, blockLength, variableFieldCount)`, ascending by version.
type Rows = &'static [(u16, u16, u8)];

/// The rows that apply to `template_id`, or `None` when this build has no
/// decoder for it (the production switches fall through to a log line).
fn rows(template_id: u16) -> Option<Rows> {
    use deepmsg_codec_cluster::*;

    Some(match template_id {
        session_message_header_codec::SBE_TEMPLATE_ID => &[(0, 24, 0)],
        session_event_codec::SBE_TEMPLATE_ID => &[(0, 32, 1), (6, 36, 1), (13, 44, 1)],
        session_connect_request_codec::SBE_TEMPLATE_ID => &[(0, 12, 2), (2, 16, 2), (14, 16, 3)],
        session_close_request_codec::SBE_TEMPLATE_ID => &[(0, 16, 0)],
        session_keep_alive_codec::SBE_TEMPLATE_ID => &[(0, 16, 0)],
        new_leader_event_codec::SBE_TEMPLATE_ID => &[(0, 20, 1)],
        challenge_codec::SBE_TEMPLATE_ID => &[(0, 16, 1)],
        challenge_response_codec::SBE_TEMPLATE_ID => &[(0, 16, 1)],
        timer_event_codec::SBE_TEMPLATE_ID => &[(0, 24, 0)],
        session_open_event_codec::SBE_TEMPLATE_ID => &[(0, 36, 2)],
        session_close_event_codec::SBE_TEMPLATE_ID => &[(0, 28, 0)],
        cluster_action_request_codec::SBE_TEMPLATE_ID => &[(0, 28, 0), (11, 32, 0), (18, 40, 0)],
        new_leadership_term_event_codec::SBE_TEMPLATE_ID => &[(0, 40, 0), (4, 48, 0)],
        membership_change_event_codec::SBE_TEMPLATE_ID => &[(0, 40, 1)],
        admin_request_codec::SBE_TEMPLATE_ID => &[(0, 28, 1)],
        admin_response_codec::SBE_TEMPLATE_ID => &[(0, 24, 2)],
        close_session_codec::SBE_TEMPLATE_ID => &[(0, 8, 0)],
        schedule_timer_codec::SBE_TEMPLATE_ID => &[(0, 16, 0)],
        cancel_timer_codec::SBE_TEMPLATE_ID => &[(0, 8, 0)],
        service_ack_codec::SBE_TEMPLATE_ID => &[(0, 36, 0)],
        cluster_members_query_codec::SBE_TEMPLATE_ID => &[(0, 8, 0), (5, 12, 0)],
        remove_member_codec::SBE_TEMPLATE_ID => &[(0, 8, 0)],
        join_log_codec::SBE_TEMPLATE_ID => &[(0, 36, 1), (16, 40, 1)],
        cluster_members_response_codec::SBE_TEMPLATE_ID => &[(0, 12, 2)],
        service_termination_position_codec::SBE_TEMPLATE_ID => &[(0, 8, 0), (17, 12, 0)],
        // Two groups; the caller walks them (`tool/operator.rs`).
        cluster_members_extended_response_codec::SBE_TEMPLATE_ID => &[(0, 24, 0)],
        request_service_ack_codec::SBE_TEMPLATE_ID => &[(0, 8, 0)],
        canvass_position_codec::SBE_TEMPLATE_ID => &[(0, 28, 0), (9, 32, 0)],
        request_vote_codec::SBE_TEMPLATE_ID => &[(0, 28, 0), (9, 32, 0)],
        vote_codec::SBE_TEMPLATE_ID => &[(0, 36, 0)],
        new_leadership_term_codec::SBE_TEMPLATE_ID => &[(0, 88, 0), (15, 96, 0)],
        append_position_codec::SBE_TEMPLATE_ID => &[(0, 20, 0), (8, 21, 0)],
        commit_position_codec::SBE_TEMPLATE_ID => &[(0, 20, 0)],
        catchup_position_codec::SBE_TEMPLATE_ID => &[(0, 20, 1)],
        stop_catchup_codec::SBE_TEMPLATE_ID => &[(0, 12, 0)],
        add_passive_member_codec::SBE_TEMPLATE_ID => &[(0, 8, 1)],
        cluster_members_change_codec::SBE_TEMPLATE_ID => &[(0, 12, 2)],
        snapshot_recording_query_codec::SBE_TEMPLATE_ID => &[(0, 12, 0)],
        // A group, then one variable field; the group is the caller's (`agent.rs`).
        snapshot_recordings_codec::SBE_TEMPLATE_ID => &[(0, 8, 0)],
        join_cluster_codec::SBE_TEMPLATE_ID => &[(0, 12, 0)],
        termination_position_codec::SBE_TEMPLATE_ID => &[(0, 16, 0)],
        termination_ack_codec::SBE_TEMPLATE_ID => &[(0, 20, 0)],
        backup_query_codec::SBE_TEMPLATE_ID => &[(0, 12, 2), (2, 16, 2), (16, 24, 2)],
        // A group, then one variable field; `backup/agent.rs` walks both.
        backup_response_codec::SBE_TEMPLATE_ID => &[(0, 56, 0), (10, 60, 0)],
        heartbeat_request_codec::SBE_TEMPLATE_ID => &[(0, 12, 2)],
        heartbeat_response_codec::SBE_TEMPLATE_ID => &[(0, 8, 0)],
        // A group, then two variable fields; consensus reads the fixed block only.
        standby_snapshot_codec::SBE_TEMPLATE_ID => &[(0, 16, 0)],
        snapshot_marker_codec::SBE_TEMPLATE_ID => &[(0, 32, 0), (4, 40, 0), (18, 48, 0)],
        client_session_codec::SBE_TEMPLATE_ID => &[(0, 12, 2)],
        cluster_session_codec::SBE_TEMPLATE_ID => &[(0, 40, 1)],
        timer_codec::SBE_TEMPLATE_ID => &[(0, 16, 0)],
        consensus_module_codec::SBE_TEMPLATE_ID => &[(0, 8, 0), (3, 28, 0)],
        cluster_members_codec::SBE_TEMPLATE_ID => &[(0, 8, 1)],
        pending_message_tracker_codec::SBE_TEMPLATE_ID => &[(0, 24, 0)],
        _ => return None,
    })
}

/// Whether the template carries a repeating group directly after its fixed
/// block. The group's own header starts at the message's **acting** block
/// length, which is where the decoder's limit is; whether the entries fit is
/// the caller's walk.
fn has_group(template_id: u16) -> bool {
    use deepmsg_codec_cluster::*;

    matches!(
        template_id,
        backup_response_codec::SBE_TEMPLATE_ID
            | snapshot_recordings_codec::SBE_TEMPLATE_ID
            | cluster_members_extended_response_codec::SBE_TEMPLATE_ID
            | standby_snapshot_codec::SBE_TEMPLATE_ID
    )
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
/// A template this build does not decode is a no-op: the production switch has
/// no arm for it.
///
/// # Errors
///
/// [`DecodeError`] naming the fixed block or variable-field walk that does not
/// fit.
pub fn fits(payload: &[u8], header: &MessageHeaderDecoder<ReadBuf<'_>>) -> Result<(), DecodeError> {
    let Some(table) = rows(header.template_id()) else {
        return Ok(());
    };
    let (block_length, variable_fields) = row_at(table, header.version());

    let needed = message_header_codec::ENCODED_LENGTH + usize::from(block_length);
    if payload.len() < needed {
        return Err(DecodeError {
            what: "cluster message fixed block",
            needed,
            available: payload.len(),
        });
    }

    // A group's header is read from the decoder's limit, which the message's
    // acting block length fixes. The entries behind it are the caller's walk.
    if has_group(header.template_id()) {
        let at = message_header_codec::ENCODED_LENGTH + usize::from(header.block_length());
        if payload.len() < at.saturating_add(4) {
            return Err(DecodeError {
                what: "cluster message group header",
                needed: at.saturating_add(4),
                available: payload.len(),
            });
        }
    }

    if variable_fields > 0 {
        let at = message_header_codec::ENCODED_LENGTH + usize::from(header.block_length());
        if !variable_data_fits(payload, at, variable_fields) {
            return Err(DecodeError {
                what: "cluster message variable field",
                needed: at.saturating_add(4),
                available: payload.len(),
            });
        }
    }

    Ok(())
}

/// The same check from the raw payload: header length, schema id, then
/// [`fits`]. This is the entrance the fuzz target and callers without a header
/// decoder use.
///
/// # Errors
///
/// [`DecodeError`] for a payload shorter than a header, a foreign schema, or a
/// fixed/variable field that does not fit.
pub fn message_fits(payload: &[u8]) -> Result<(), DecodeError> {
    if payload.len() < message_header_codec::ENCODED_LENGTH {
        return Err(DecodeError {
            what: "cluster message header",
            needed: message_header_codec::ENCODED_LENGTH,
            available: payload.len(),
        });
    }

    let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(payload), 0);
    if header.schema_id() != SBE_SCHEMA_ID {
        return Err(DecodeError {
            what: "cluster message schema",
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
        if has_group(template_id) {
            // An empty group header: block length 0, no entries.
            payload.extend_from_slice(&0_u32.to_le_bytes());
        }

        payload
    }

    #[test]
    fn every_row_accepts_its_own_message_at_every_breakpoint() {
        for template_id in 0..=120 {
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
                if variables > 0 || block > 0 {
                    assert!(
                        message_fits(&short).is_err(),
                        "template {template_id} version {version} accepted one byte short"
                    );
                }
            }
        }
    }

    #[test]
    fn an_older_version_may_carry_a_shorter_block() {
        // SessionEvent: 32 bytes before v6, 44 from v13.
        let event = deepmsg_codec_cluster::session_event_codec::SBE_TEMPLATE_ID;
        assert!(message_fits(&message(event, 0, 32, 1)).is_ok());
        assert!(message_fits(&message(event, 6, 36, 1)).is_ok());
        assert!(message_fits(&message(event, 13, 44, 1)).is_ok());
    }

    #[test]
    fn an_unknown_template_is_not_decoded_and_not_refused() {
        assert!(rows(999).is_none());
        assert!(message_fits(&message(999, 0, 0, 0)).is_ok());
    }

    /// Found by target 08: template 43 declared a 49-byte acting block, so the
    /// decoder reads the group header at 57, past the 39-byte payload. The
    /// active-group walk alone was not enough; the group's *start* is the
    /// message's declared block length.
    #[test]
    fn a_group_header_past_the_payload_is_refused() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&49_u16.to_le_bytes());
        payload.extend_from_slice(&43_u16.to_le_bytes());
        payload.extend_from_slice(&SBE_SCHEMA_ID.to_le_bytes());
        payload.extend_from_slice(&17_u16.to_le_bytes());
        payload.resize(39, 0);

        assert!(message_fits(&payload).is_err());
    }

    #[test]
    fn a_variable_field_past_the_end_is_refused() {
        let mut payload = message(
            deepmsg_codec_cluster::session_event_codec::SBE_TEMPLATE_ID,
            13,
            44,
            0,
        );
        payload.extend_from_slice(&u32::MAX.to_le_bytes());
        assert!(message_fits(&payload).is_err());
    }
}
