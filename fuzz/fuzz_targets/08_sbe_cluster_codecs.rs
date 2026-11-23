#![no_main]

//! Target 08: every cluster SBE message a production entry point decodes.
//!
//! The bounds check is `deepmsg_cluster_common::sbe`, the table production
//! uses; `deepmsg_fuzz::cluster_messages` holds the decoder walk. The two
//! group-bearing messages that production reads as groups get their group walk
//! here (`BackupResponse` as `backup/agent.rs` does, `ClusterMembersExtendedResponse`
//! as `tool/operator.rs` does), because a generic table cannot locate a field
//! behind a group.
//!
//! The archive's own length guard (`with_length`) is fuzzed too: it is a slice
//! or an error, never an index past the end.

use deepmsg_cluster_common::decode::with_length;
use deepmsg_codec::cluster::backup_response_codec::{
    SBE_BLOCK_LENGTH as BACKUP_RESPONSE_BLOCK_LENGTH,
    SBE_TEMPLATE_ID as BACKUP_RESPONSE_TEMPLATE_ID,
};
use deepmsg_codec::cluster::cluster_members_extended_response_codec::SBE_TEMPLATE_ID as EXTENDED_MEMBERS_RESPONSE_TEMPLATE_ID;
use deepmsg_codec::cluster::message_header_codec::{ENCODED_LENGTH, MessageHeaderDecoder};
use deepmsg_codec::cluster::{ReadBuf, SBE_SCHEMA_ID};
use deepmsg_fuzz::cluster_messages::decode_one;
use deepmsg_fuzz::variable_data_fits;
use libfuzzer_sys::fuzz_target;

/// The schema's block length for one `snapshots` group entry.
const SNAPSHOT_BLOCK_LENGTH: usize = 44;
/// One `activeMembers` entry in `ClusterMembersExtendedResponse`.
const MEMBER_BLOCK_LENGTH: usize = 28;
/// The five endpoint strings on an `activeMembers` entry.
const MEMBER_VARIABLES: usize = 5;

fn header(payload: &[u8]) -> Option<MessageHeaderDecoder<ReadBuf<'_>>> {
    if payload.len() < ENCODED_LENGTH {
        return None;
    }

    Some(MessageHeaderDecoder::default().wrap(ReadBuf::new(payload), 0))
}

/// `BackupResponse`: fixed block, then the `snapshots` group, then the member
/// list — the walk `backup/agent.rs::decode_backup_response` makes.
fn backup_response_fits(payload: &[u8], acting_at: usize) -> bool {
    if payload.len() < ENCODED_LENGTH + usize::from(BACKUP_RESPONSE_BLOCK_LENGTH) {
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

/// `ClusterMembersExtendedResponse`: fixed block, then the `activeMembers`
/// group — the walk `tool/operator.rs::extended_members_group_fits` makes.
fn extended_members_fits(payload: &[u8], acting_at: usize) -> bool {
    let Some(group_header) = payload.get(acting_at..acting_at + 4) else {
        return false;
    };
    let block_length = usize::from(u16::from_le_bytes([group_header[0], group_header[1]]));
    let count = usize::from(u16::from_le_bytes([group_header[2], group_header[3]]));
    if block_length < MEMBER_BLOCK_LENGTH {
        return false;
    }

    let mut at = acting_at + 4;
    for _ in 0..count {
        let Some(next) = at.checked_add(block_length) else {
            return false;
        };
        if next > payload.len() {
            return false;
        }
        at = next;

        for _ in 0..MEMBER_VARIABLES {
            let Some(prefix_end) = at.checked_add(4) else {
                return false;
            };
            let Some(prefix) = payload.get(at..prefix_end) else {
                return false;
            };
            let length = usize::try_from(u32::from_le_bytes([
                prefix[0], prefix[1], prefix[2], prefix[3],
            ]))
            .unwrap_or(usize::MAX);
            let Some(next) = prefix_end.checked_add(length) else {
                return false;
            };
            if next > payload.len() {
                return false;
            }
            at = next;
        }
    }

    true
}

fn guard(payload: &[u8], header: &MessageHeaderDecoder<ReadBuf<'_>>) -> bool {
    let acting_at = ENCODED_LENGTH + usize::from(header.block_length());

    match header.template_id() {
        BACKUP_RESPONSE_TEMPLATE_ID => backup_response_fits(payload, acting_at),
        EXTENDED_MEMBERS_RESPONSE_TEMPLATE_ID => {
            deepmsg_cluster_common::sbe::fits(payload, header).is_ok()
                && extended_members_fits(payload, acting_at)
        }
        _ => deepmsg_cluster_common::sbe::fits(payload, header).is_ok(),
    }
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

    decode_one(header.template_id(), header);
});
