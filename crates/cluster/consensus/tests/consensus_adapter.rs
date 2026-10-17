//! `ConsensusAdapter`: the fourteen templates the reference dispatches, and
//! the two substitutions its decoder makes (a null `flags` field and a null
//! commit position).
//!
//! The messages are encoded with the schema's own generated encoders — the same
//! code the publisher calls — so these pin the adapter's field order and the
//! template ids against schema 111.

use deepmsg_cluster::consensus::{ConsensusMessage, decode_consensus_message};
use deepmsg_codec_cluster::WriteBuf;
use deepmsg_codec_cluster::append_position_codec::AppendPositionEncoder;
use deepmsg_codec_cluster::boolean_type::BooleanType;
use deepmsg_codec_cluster::canvass_position_codec::CanvassPositionEncoder;
use deepmsg_codec_cluster::catchup_position_codec::CatchupPositionEncoder;
use deepmsg_codec_cluster::commit_position_codec::CommitPositionEncoder;
use deepmsg_codec_cluster::message_header_codec::ENCODED_LENGTH as MESSAGE_HEADER_LENGTH;
use deepmsg_codec_cluster::new_leadership_term_codec::NewLeadershipTermEncoder;
use deepmsg_codec_cluster::request_vote_codec::RequestVoteEncoder;
use deepmsg_codec_cluster::stop_catchup_codec::StopCatchupEncoder;
use deepmsg_codec_cluster::termination_ack_codec::TerminationAckEncoder;
use deepmsg_codec_cluster::termination_position_codec::TerminationPositionEncoder;
use deepmsg_codec_cluster::vote_codec::VoteEncoder;

fn decode_after<F: FnOnce(&mut Vec<u8>)>(encode: F) -> ConsensusMessage {
    let mut buffer = vec![0_u8; 512];
    encode(&mut buffer);
    decode_consensus_message(&buffer)
        .expect("it decodes")
        .expect("a dispatched template")
}

#[test]
fn canvass_request_vote_and_vote_round_trip() {
    let message = decode_after(|buffer| {
        let encoder =
            CanvassPositionEncoder::default().wrap(WriteBuf::new(buffer), MESSAGE_HEADER_LENGTH);
        let mut header = encoder.header(0);
        let mut body = header.parent().expect("just wrapped");
        body.log_leadership_term_id(1)
            .log_position(2)
            .leadership_term_id(3)
            .follower_member_id(4)
            .protocol_version(17);
        let length = MESSAGE_HEADER_LENGTH + body.encoded_length();
        buffer.truncate(length);
    });
    assert_eq!(
        ConsensusMessage::CanvassPosition {
            log_leadership_term_id: 1,
            log_position: 2,
            leadership_term_id: 3,
            follower_member_id: 4,
            protocol_version: 17,
        },
        message
    );

    let message = decode_after(|buffer| {
        let encoder =
            RequestVoteEncoder::default().wrap(WriteBuf::new(buffer), MESSAGE_HEADER_LENGTH);
        let mut header = encoder.header(0);
        let mut body = header.parent().expect("just wrapped");
        body.log_leadership_term_id(5)
            .log_position(6)
            .candidate_term_id(7)
            .candidate_member_id(1)
            .protocol_version(17);
        let length = MESSAGE_HEADER_LENGTH + body.encoded_length();
        buffer.truncate(length);
    });
    assert_eq!(
        ConsensusMessage::RequestVote {
            log_leadership_term_id: 5,
            log_position: 6,
            candidate_term_id: 7,
            candidate_member_id: 1,
            protocol_version: 17,
        },
        message
    );

    let message = decode_after(|buffer| {
        let encoder = VoteEncoder::default().wrap(WriteBuf::new(buffer), MESSAGE_HEADER_LENGTH);
        let mut header = encoder.header(0);
        let mut body = header.parent().expect("just wrapped");
        body.candidate_term_id(7)
            .log_leadership_term_id(5)
            .log_position(6)
            .candidate_member_id(1)
            .follower_member_id(2)
            .vote(BooleanType::TRUE);
        let length = MESSAGE_HEADER_LENGTH + body.encoded_length();
        buffer.truncate(length);
    });
    assert_eq!(
        ConsensusMessage::Vote {
            log_leadership_term_id: 5,
            log_position: 6,
            candidate_term_id: 7,
            candidate_member_id: 1,
            follower_member_id: 2,
            vote: true,
        },
        message
    );
}

#[test]
fn a_new_leadership_term_round_trips_every_number() {
    let message = decode_after(|buffer| {
        let encoder =
            NewLeadershipTermEncoder::default().wrap(WriteBuf::new(buffer), MESSAGE_HEADER_LENGTH);
        let mut header = encoder.header(0);
        let mut body = header.parent().expect("just wrapped");
        body.log_leadership_term_id(1)
            .next_leadership_term_id(2)
            .next_term_base_log_position(3)
            .next_log_position(4)
            .leadership_term_id(5)
            .term_base_log_position(6)
            .log_position(7)
            .commit_position(8)
            .leader_recording_id(9)
            .timestamp(10)
            .leader_member_id(11)
            .log_session_id(12)
            .app_version(13)
            .is_startup(BooleanType::TRUE);
        let length = MESSAGE_HEADER_LENGTH + body.encoded_length();
        buffer.truncate(length);
    });

    assert_eq!(
        ConsensusMessage::NewLeadershipTerm {
            log_leadership_term_id: 1,
            next_leadership_term_id: 2,
            next_term_base_log_position: 3,
            next_log_position: 4,
            leadership_term_id: 5,
            term_base_log_position: 6,
            log_position: 7,
            commit_position: 8,
            leader_recording_id: 9,
            timestamp: 10,
            leader_member_id: 11,
            log_session_id: 12,
            app_version: 13,
            is_startup: true,
        },
        message
    );
}

#[test]
fn a_null_flags_field_becomes_the_none_flag() {
    let message = decode_after(|buffer| {
        let encoder =
            AppendPositionEncoder::default().wrap(WriteBuf::new(buffer), MESSAGE_HEADER_LENGTH);
        let mut header = encoder.header(0);
        let mut body = header.parent().expect("just wrapped");
        body.leadership_term_id(1)
            .log_position(2)
            .follower_member_id(3)
            .flags(255);
        let length = MESSAGE_HEADER_LENGTH + body.encoded_length();
        buffer.truncate(length);
    });
    assert_eq!(
        ConsensusMessage::AppendPosition {
            leadership_term_id: 1,
            log_position: 2,
            follower_member_id: 3,
            flags: 0,
        },
        message
    );

    let message = decode_after(|buffer| {
        let encoder =
            AppendPositionEncoder::default().wrap(WriteBuf::new(buffer), MESSAGE_HEADER_LENGTH);
        let mut header = encoder.header(0);
        let mut body = header.parent().expect("just wrapped");
        body.leadership_term_id(1)
            .log_position(2)
            .follower_member_id(3)
            .flags(1);
        let length = MESSAGE_HEADER_LENGTH + body.encoded_length();
        buffer.truncate(length);
    });
    assert_eq!(
        ConsensusMessage::AppendPosition {
            leadership_term_id: 1,
            log_position: 2,
            follower_member_id: 3,
            flags: 1,
        },
        message
    );
}

#[test]
fn commit_catchup_stop_and_termination_round_trip() {
    assert_eq!(
        ConsensusMessage::CommitPosition {
            leadership_term_id: 1,
            log_position: 2,
            leader_member_id: 3,
        },
        decode_after(|buffer| {
            let encoder =
                CommitPositionEncoder::default().wrap(WriteBuf::new(buffer), MESSAGE_HEADER_LENGTH);
            let mut header = encoder.header(0);
            let mut body = header.parent().expect("just wrapped");
            body.leadership_term_id(1)
                .log_position(2)
                .leader_member_id(3);
            let length = MESSAGE_HEADER_LENGTH + body.encoded_length();
            buffer.truncate(length);
        })
    );

    assert_eq!(
        ConsensusMessage::CatchupPosition {
            leadership_term_id: 1,
            log_position: 2,
            follower_member_id: 3,
            catchup_endpoint: "localhost:9002".to_owned(),
        },
        decode_after(|buffer| {
            let encoder = CatchupPositionEncoder::default()
                .wrap(WriteBuf::new(buffer), MESSAGE_HEADER_LENGTH);
            let mut header = encoder.header(0);
            let mut body = header.parent().expect("just wrapped");
            body.leadership_term_id(1)
                .log_position(2)
                .follower_member_id(3)
                .catchup_endpoint(b"localhost:9002");
            let length = MESSAGE_HEADER_LENGTH + body.encoded_length();
            buffer.truncate(length);
        })
    );

    assert_eq!(
        ConsensusMessage::StopCatchup {
            leadership_term_id: 1,
            follower_member_id: 3,
        },
        decode_after(|buffer| {
            let encoder =
                StopCatchupEncoder::default().wrap(WriteBuf::new(buffer), MESSAGE_HEADER_LENGTH);
            let mut header = encoder.header(0);
            let mut body = header.parent().expect("just wrapped");
            body.leadership_term_id(1).follower_member_id(3);
            let length = MESSAGE_HEADER_LENGTH + body.encoded_length();
            buffer.truncate(length);
        })
    );

    assert_eq!(
        ConsensusMessage::TerminationPosition {
            leadership_term_id: 1,
            log_position: 2,
        },
        decode_after(|buffer| {
            let encoder = TerminationPositionEncoder::default()
                .wrap(WriteBuf::new(buffer), MESSAGE_HEADER_LENGTH);
            let mut header = encoder.header(0);
            let mut body = header.parent().expect("just wrapped");
            body.leadership_term_id(1).log_position(2);
            let length = MESSAGE_HEADER_LENGTH + body.encoded_length();
            buffer.truncate(length);
        })
    );

    assert_eq!(
        ConsensusMessage::TerminationAck {
            leadership_term_id: 1,
            log_position: 2,
            member_id: 3,
        },
        decode_after(|buffer| {
            let encoder =
                TerminationAckEncoder::default().wrap(WriteBuf::new(buffer), MESSAGE_HEADER_LENGTH);
            let mut header = encoder.header(0);
            let mut body = header.parent().expect("just wrapped");
            body.leadership_term_id(1).log_position(2).member_id(3);
            let length = MESSAGE_HEADER_LENGTH + body.encoded_length();
            buffer.truncate(length);
        })
    );
}

#[test]
fn an_unknown_template_is_ignored_and_a_wrong_schema_is_an_error() {
    // The switch has no default: the reference ignores what it does not know.
    let mut buffer = vec![0_u8; 64];
    {
        let encoder = CanvassPositionEncoder::default()
            .wrap(WriteBuf::new(&mut buffer), MESSAGE_HEADER_LENGTH);
        let mut header = encoder.header(0);
        let mut body = header.parent().expect("just wrapped");
        body.log_leadership_term_id(1)
            .log_position(2)
            .leadership_term_id(3)
            .follower_member_id(4)
            .protocol_version(17);
        let length = MESSAGE_HEADER_LENGTH + body.encoded_length();
        buffer.truncate(length);
    }
    // The message header is blockLength, templateId, schemaId, version — 2, 4
    // and 6 are the template and schema.
    buffer[2..4].copy_from_slice(&9999_u16.to_le_bytes());
    assert_eq!(None, decode_consensus_message(&buffer).expect("decodes"));

    buffer[2..4].copy_from_slice(&50_u16.to_le_bytes());
    buffer[4..6].copy_from_slice(&999_u16.to_le_bytes());
    assert!(decode_consensus_message(&buffer).is_err());
}
