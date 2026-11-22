#![no_main]

//! Target 08: the cluster's SBE messages, nested group included.
//!
//! `BackupResponse` is the shape that matters: a fixed block, then a repeating
//! group whose count and block length come from the bytes under test, then a
//! variable-length member list. The generated decoder's `advance` only moves a
//! cursor, and its field reads past the end panic — so the production decoder
//! (`backup/agent.rs::decode_backup_message`) now validates the fixed part, the
//! group walk (a zero-length block never advances) and the variable-length tail
//! before it reads. This target mirrors that guard exactly.

use deepmsg_cluster_common::decode::with_length;
use deepmsg_codec::cluster::backup_response_codec::{
    BackupResponseDecoder, SBE_BLOCK_LENGTH as BACKUP_RESPONSE_BLOCK_LENGTH,
    SBE_TEMPLATE_ID as BACKUP_RESPONSE_TEMPLATE_ID,
};
use deepmsg_codec::cluster::message_header_codec::{ENCODED_LENGTH, MessageHeaderDecoder};
use deepmsg_codec::cluster::session_event_codec::{
    SBE_BLOCK_LENGTH as SESSION_EVENT_BLOCK_LENGTH, SBE_TEMPLATE_ID as SESSION_EVENT_TEMPLATE_ID,
    SessionEventDecoder,
};
use deepmsg_codec::cluster::{ReadBuf, SBE_SCHEMA_ID};
use deepmsg_fuzz::{fixed_part_fits, variable_data_fits};
use libfuzzer_sys::fuzz_target;

/// The schema's block length for one `snapshots` group entry (see
/// `backup/agent.rs::SNAPSHOT_BLOCK_LENGTH`).
const SNAPSHOT_BLOCK_LENGTH: usize = 44;

fn header(payload: &[u8]) -> Option<MessageHeaderDecoder<ReadBuf<'_>>> {
    if payload.len() < ENCODED_LENGTH {
        return None;
    }

    Some(MessageHeaderDecoder::default().wrap(ReadBuf::new(payload), 0))
}

/// The production guard, before any decoder reads a field.
fn guard(payload: &[u8], header: &MessageHeaderDecoder<ReadBuf<'_>>) -> bool {
    let acting_at = ENCODED_LENGTH + usize::from(header.block_length());

    match header.template_id() {
        BACKUP_RESPONSE_TEMPLATE_ID => {
            if !fixed_part_fits(payload, BACKUP_RESPONSE_BLOCK_LENGTH) {
                return false;
            }
            let Some(group_header_end) = acting_at.checked_add(4) else {
                return false;
            };
            let Some(group_header) = payload.get(acting_at..group_header_end) else {
                return false;
            };
            let block_length = usize::from(u16::from_le_bytes([group_header[0], group_header[1]]));
            let count = usize::from(u16::from_le_bytes([group_header[2], group_header[3]]));

            if block_length < SNAPSHOT_BLOCK_LENGTH {
                return false;
            }

            let mut at = group_header_end;
            for _ in 0..count {
                let Some(next) = at.checked_add(block_length) else {
                    return false;
                };
                if next > payload.len() {
                    return false;
                }
                at = next;
            }

            variable_data_fits(payload, at, 1)
        }
        deepmsg_codec::cluster::challenge_codec::SBE_TEMPLATE_ID => {
            fixed_part_fits(
                payload,
                deepmsg_codec::cluster::challenge_codec::SBE_BLOCK_LENGTH,
            ) && variable_data_fits(payload, acting_at, 1)
        }
        SESSION_EVENT_TEMPLATE_ID => {
            fixed_part_fits(payload, SESSION_EVENT_BLOCK_LENGTH)
                && variable_data_fits(payload, acting_at, 1)
        }
        _ => true,
    }
}

fn backup_response(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let decoder = BackupResponseDecoder::default().header(header, 0);
    let _ = decoder.member_id();
    let _ = decoder.correlation_id();

    let mut group = decoder.snapshots_decoder();
    while group
        .advance()
        .expect("the snapshots group reads")
        .is_some()
    {
        let _ = group.recording_id();
        let _ = group.leadership_term_id();
        let _ = group.term_base_log_position();
        let _ = group.log_position();
        let _ = group.timestamp();
        let _ = group.service_id();
    }

    let mut decoder = group.parent().expect("the group has a parent");
    let coordinates = decoder.cluster_members_decoder();
    let _ = decoder.cluster_members_slice(coordinates);
    let _ = decoder.log_recording_id();
    let _ = decoder.log_leadership_term_id();
    let _ = decoder.log_term_base_log_position();
    let _ = decoder.last_leadership_term_id();
    let _ = decoder.last_term_base_log_position();
}

fn session_event(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let mut decoder = SessionEventDecoder::default().header(header, 0);
    let _ = decoder.correlation_id();
    let _ = decoder.code();
    let coordinates = decoder.detail_decoder();
    let _ = decoder.detail_slice(coordinates);
}

fuzz_target!(|data: &[u8]| {
    // The cluster's own length guard, on its own: it answers a slice or an
    // error, and the offsets are the fuzzer's.
    let offset = usize::from(data.first().copied().unwrap_or(0));
    let needed = usize::from(data.get(1).copied().unwrap_or(0));
    let _ = with_length(data, offset.min(data.len()), needed, "fuzz");

    let Some(header) = header(data) else {
        return;
    };

    if header.schema_id() != SBE_SCHEMA_ID {
        return;
    }

    if !guard(data, &header) {
        return;
    }

    match header.template_id() {
        BACKUP_RESPONSE_TEMPLATE_ID => backup_response(header),
        SESSION_EVENT_TEMPLATE_ID => session_event(header),
        _ => {}
    }
});
