//! `ConsensusAdapter`: what a member hears on the consensus channel.
//!
//! Counterpart of `ConsensusAdapter.java:69-340`. The reference polls a
//! `FragmentAssembler` and switches on the template id inside `onFragment`;
//! here the poll hands whole messages over (the client assembles) and this
//! module turns one into a [`ConsensusMessage`]. The agent dispatches, because
//! every handler is an agent method and half of them route to the election.
//!
//! Schema 111's consensus templates are ids 50–57, 75–77, 79 and 81 (plus 8,
//! the backup challenge response). The set is the reference's switch exactly:
//! `HeartbeatResponse` is not dispatched by it either.

use deepmsg_codec_cluster::SBE_SCHEMA_ID;
use deepmsg_codec_cluster::boolean_type::BooleanType;
use deepmsg_codec_cluster::message_header_codec::ENCODED_LENGTH as MESSAGE_HEADER_LENGTH;
use deepmsg_codec_cluster::{
    ReadBuf, append_position_codec::AppendPositionDecoder, backup_query_codec::BackupQueryDecoder,
    canvass_position_codec::CanvassPositionDecoder, catchup_position_codec::CatchupPositionDecoder,
    challenge_response_codec::ChallengeResponseDecoder,
    commit_position_codec::CommitPositionDecoder, heartbeat_request_codec::HeartbeatRequestDecoder,
    new_leadership_term_codec::NewLeadershipTermDecoder, request_vote_codec::RequestVoteDecoder,
    standby_snapshot_codec::StandbySnapshotDecoder, stop_catchup_codec::StopCatchupDecoder,
    termination_ack_codec::TerminationAckDecoder,
    termination_position_codec::TerminationPositionDecoder, vote_codec::VoteDecoder,
};

use deepmsg_cluster_common::decode::at_least;

use crate::error::ClusterError;

/// One message from a peer (`ConsensusAdapter.onFragment` `:92-340`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConsensusMessage {
    CanvassPosition {
        log_leadership_term_id: i64,
        log_position: i64,
        leadership_term_id: i64,
        follower_member_id: i32,
        protocol_version: i32,
    },
    RequestVote {
        log_leadership_term_id: i64,
        log_position: i64,
        candidate_term_id: i64,
        candidate_member_id: i32,
        protocol_version: i32,
    },
    Vote {
        log_leadership_term_id: i64,
        log_position: i64,
        candidate_term_id: i64,
        candidate_member_id: i32,
        follower_member_id: i32,
        vote: bool,
    },
    NewLeadershipTerm {
        log_leadership_term_id: i64,
        next_leadership_term_id: i64,
        next_term_base_log_position: i64,
        next_log_position: i64,
        leadership_term_id: i64,
        term_base_log_position: i64,
        log_position: i64,
        commit_position: i64,
        leader_recording_id: i64,
        timestamp: i64,
        leader_member_id: i32,
        log_session_id: i32,
        app_version: i32,
        is_startup: bool,
    },
    AppendPosition {
        leadership_term_id: i64,
        log_position: i64,
        follower_member_id: i32,
        flags: i16,
    },
    CommitPosition {
        leadership_term_id: i64,
        log_position: i64,
        leader_member_id: i32,
    },
    CatchupPosition {
        leadership_term_id: i64,
        log_position: i64,
        follower_member_id: i32,
        catchup_endpoint: String,
    },
    StopCatchup {
        leadership_term_id: i64,
        follower_member_id: i32,
    },
    TerminationPosition {
        leadership_term_id: i64,
        log_position: i64,
    },
    TerminationAck {
        leadership_term_id: i64,
        log_position: i64,
        member_id: i32,
    },
    BackupQuery {
        correlation_id: i64,
        response_stream_id: i32,
        version: i32,
        log_position: i64,
        /// Where the answer goes — the backup's own consensus subscription
        /// (`ClusterBackupAgent.java:698`).
        response_channel: String,
        /// What the backup offers to prove itself with; opaque here.
        encoded_credentials: Vec<u8>,
    },
    ChallengeResponse {
        correlation_id: i64,
        cluster_session_id: i64,
        /// The client's challenge answer, opaque to the cluster
        /// (`ChallengeResponse.encodedCredentials`): a follower forwards it to
        /// the leader unchanged (`CMA:870-880`).
        encoded_credentials: Vec<u8>,
    },
    HeartbeatRequest {
        correlation_id: i64,
        response_stream_id: i32,
        response_channel: String,
    },
    StandbySnapshot {
        correlation_id: i64,
        version: i32,
        response_stream_id: i32,
    },
}

/// Decode one whole consensus message, or `None` for a template the reference
/// does not dispatch (it ignores those silently: the switch has no default).
///
/// # Errors
///
/// [`ClusterError::Cluster`] for a message too short to hold its own header, and
/// for a schema id that is not 111 — the reference throws `ClusterException` for
/// the latter (`:89-92`).
pub fn decode_consensus_message(payload: &[u8]) -> Result<Option<ConsensusMessage>, ClusterError> {
    at_least(payload, MESSAGE_HEADER_LENGTH, "consensus message header")?;
    let header = deepmsg_codec_cluster::message_header_codec::MessageHeaderDecoder::default()
        .wrap(ReadBuf::new(payload), 0);
    if header.schema_id() != SBE_SCHEMA_ID {
        return Err(ClusterError::Cluster(format!(
            "expected schemaId={SBE_SCHEMA_ID}, actual={}",
            header.schema_id()
        )));
    }

    let template_id = header.template_id();
    let message = match template_id {
        deepmsg_codec_cluster::canvass_position_codec::SBE_TEMPLATE_ID => {
            let decoder = CanvassPositionDecoder::default().header(header, 0);
            Some(ConsensusMessage::CanvassPosition {
                log_leadership_term_id: decoder.log_leadership_term_id(),
                log_position: decoder.log_position(),
                leadership_term_id: decoder.leadership_term_id(),
                follower_member_id: decoder.follower_member_id(),
                protocol_version: decoder.protocol_version().unwrap_or(0),
            })
        }
        deepmsg_codec_cluster::request_vote_codec::SBE_TEMPLATE_ID => {
            let decoder = RequestVoteDecoder::default().header(header, 0);
            Some(ConsensusMessage::RequestVote {
                log_leadership_term_id: decoder.log_leadership_term_id(),
                log_position: decoder.log_position(),
                candidate_term_id: decoder.candidate_term_id(),
                candidate_member_id: decoder.candidate_member_id(),
                protocol_version: decoder.protocol_version().unwrap_or(0),
            })
        }
        deepmsg_codec_cluster::vote_codec::SBE_TEMPLATE_ID => {
            let decoder = VoteDecoder::default().header(header, 0);
            Some(ConsensusMessage::Vote {
                log_leadership_term_id: decoder.log_leadership_term_id(),
                log_position: decoder.log_position(),
                candidate_term_id: decoder.candidate_term_id(),
                candidate_member_id: decoder.candidate_member_id(),
                follower_member_id: decoder.follower_member_id(),
                vote: decoder.vote() == BooleanType::TRUE,
            })
        }
        deepmsg_codec_cluster::new_leadership_term_codec::SBE_TEMPLATE_ID => {
            let decoder = NewLeadershipTermDecoder::default().header(header, 0);
            Some(ConsensusMessage::NewLeadershipTerm {
                log_leadership_term_id: decoder.log_leadership_term_id(),
                next_leadership_term_id: decoder.next_leadership_term_id(),
                next_term_base_log_position: decoder.next_term_base_log_position(),
                next_log_position: decoder.next_log_position(),
                leadership_term_id: decoder.leadership_term_id(),
                term_base_log_position: decoder.term_base_log_position(),
                log_position: decoder.log_position(),
                commit_position: decoder.commit_position(),
                leader_recording_id: decoder.leader_recording_id(),
                timestamp: decoder.timestamp(),
                leader_member_id: decoder.leader_member_id(),
                log_session_id: decoder.log_session_id(),
                app_version: decoder.app_version().unwrap_or(0),
                is_startup: decoder.is_startup() == BooleanType::TRUE,
            })
        }
        deepmsg_codec_cluster::append_position_codec::SBE_TEMPLATE_ID => {
            let decoder = AppendPositionDecoder::default().header(header, 0);
            let raw = decoder.flags();
            Some(ConsensusMessage::AppendPosition {
                leadership_term_id: decoder.leadership_term_id(),
                log_position: decoder.log_position(),
                follower_member_id: decoder.follower_member_id(),
                // `AppendPositionDecoder.flagsNullValue()` is 255; the
                // reference substitutes NONE (`ConsensusAdapter.java:157-160`).
                flags: if raw == 255 { 0 } else { i16::from(raw) },
            })
        }
        deepmsg_codec_cluster::commit_position_codec::SBE_TEMPLATE_ID => {
            let decoder = CommitPositionDecoder::default().header(header, 0);
            Some(ConsensusMessage::CommitPosition {
                leadership_term_id: decoder.leadership_term_id(),
                log_position: decoder.log_position(),
                leader_member_id: decoder.leader_member_id(),
            })
        }
        deepmsg_codec_cluster::catchup_position_codec::SBE_TEMPLATE_ID => {
            let mut decoder = CatchupPositionDecoder::default().header(header, 0);
            let coordinates = decoder.catchup_endpoint_decoder();
            let catchup_endpoint =
                String::from_utf8_lossy(decoder.catchup_endpoint_slice(coordinates)).into_owned();
            Some(ConsensusMessage::CatchupPosition {
                leadership_term_id: decoder.leadership_term_id(),
                log_position: decoder.log_position(),
                follower_member_id: decoder.follower_member_id(),
                catchup_endpoint,
            })
        }
        deepmsg_codec_cluster::stop_catchup_codec::SBE_TEMPLATE_ID => {
            let decoder = StopCatchupDecoder::default().header(header, 0);
            Some(ConsensusMessage::StopCatchup {
                leadership_term_id: decoder.leadership_term_id(),
                follower_member_id: decoder.follower_member_id(),
            })
        }
        deepmsg_codec_cluster::termination_position_codec::SBE_TEMPLATE_ID => {
            let decoder = TerminationPositionDecoder::default().header(header, 0);
            Some(ConsensusMessage::TerminationPosition {
                leadership_term_id: decoder.leadership_term_id(),
                log_position: decoder.log_position(),
            })
        }
        deepmsg_codec_cluster::termination_ack_codec::SBE_TEMPLATE_ID => {
            let decoder = TerminationAckDecoder::default().header(header, 0);
            Some(ConsensusMessage::TerminationAck {
                leadership_term_id: decoder.leadership_term_id(),
                log_position: decoder.log_position(),
                member_id: decoder.member_id(),
            })
        }
        deepmsg_codec_cluster::backup_query_codec::SBE_TEMPLATE_ID => {
            let mut decoder = BackupQueryDecoder::default().header(header, 0);
            let coordinates = decoder.response_channel_decoder();
            let response_channel =
                String::from_utf8_lossy(decoder.response_channel_slice(coordinates)).into_owned();
            let coordinates = decoder.encoded_credentials_decoder();
            let encoded_credentials = decoder.encoded_credentials_slice(coordinates).to_vec();
            Some(ConsensusMessage::BackupQuery {
                correlation_id: decoder.correlation_id(),
                response_stream_id: decoder.response_stream_id(),
                version: decoder.version().unwrap_or(0),
                log_position: decoder.log_position().unwrap_or(-1),
                response_channel,
                encoded_credentials,
            })
        }
        deepmsg_codec_cluster::challenge_response_codec::SBE_TEMPLATE_ID => {
            let mut decoder = ChallengeResponseDecoder::default().header(header, 0);
            let coordinates = decoder.encoded_credentials_decoder();
            let encoded_credentials = decoder.encoded_credentials_slice(coordinates).to_vec();
            Some(ConsensusMessage::ChallengeResponse {
                correlation_id: decoder.correlation_id(),
                cluster_session_id: decoder.cluster_session_id(),
                encoded_credentials,
            })
        }
        deepmsg_codec_cluster::heartbeat_request_codec::SBE_TEMPLATE_ID => {
            let mut decoder = HeartbeatRequestDecoder::default().header(header, 0);
            let coordinates = decoder.response_channel_decoder();
            let response_channel =
                String::from_utf8_lossy(decoder.response_channel_slice(coordinates)).into_owned();
            Some(ConsensusMessage::HeartbeatRequest {
                correlation_id: decoder.correlation_id(),
                response_stream_id: decoder.response_stream_id(),
                response_channel,
            })
        }
        deepmsg_codec_cluster::standby_snapshot_codec::SBE_TEMPLATE_ID => {
            let decoder = StandbySnapshotDecoder::default().header(header, 0);
            Some(ConsensusMessage::StandbySnapshot {
                correlation_id: decoder.correlation_id(),
                version: decoder.version(),
                response_stream_id: decoder.response_stream_id(),
            })
        }
        _ => None,
    };

    Ok(message)
}
