//! Target 08's decoder walk: one function per cluster template a production
//! entry point decodes.
//!
//! The bounds check is `deepmsg_cluster_common::sbe`, the same table production
//! uses. What this module repeats is the decoder calls, in the order the
//! production adapters make them; the four group-bearing messages are handled
//! where production handles them (`@0` backup, `@43` operator) or read only
//! their fixed block (`@81`).

use deepmsg_codec::cluster::SBE_SCHEMA_ID;
use deepmsg_codec::cluster::admin_request_codec::AdminRequestDecoder;
use deepmsg_codec::cluster::admin_response_codec::AdminResponseDecoder;
use deepmsg_codec::cluster::append_position_codec::AppendPositionDecoder;
use deepmsg_codec::cluster::backup_query_codec::BackupQueryDecoder;
use deepmsg_codec::cluster::backup_response_codec::BackupResponseDecoder;
use deepmsg_codec::cluster::canvass_position_codec::CanvassPositionDecoder;
use deepmsg_codec::cluster::catchup_position_codec::CatchupPositionDecoder;
use deepmsg_codec::cluster::challenge_codec::ChallengeDecoder;
use deepmsg_codec::cluster::challenge_response_codec::ChallengeResponseDecoder;
use deepmsg_codec::cluster::client_session_codec::ClientSessionDecoder;
use deepmsg_codec::cluster::close_session_codec::CloseSessionDecoder;
use deepmsg_codec::cluster::cluster_members_extended_response_codec::ClusterMembersExtendedResponseDecoder;
use deepmsg_codec::cluster::cluster_members_query_codec::ClusterMembersQueryDecoder;
use deepmsg_codec::cluster::cluster_members_response_codec::ClusterMembersResponseDecoder;
use deepmsg_codec::cluster::cluster_session_codec::ClusterSessionDecoder;
use deepmsg_codec::cluster::commit_position_codec::CommitPositionDecoder;
use deepmsg_codec::cluster::consensus_module_codec::ConsensusModuleDecoder;
use deepmsg_codec::cluster::heartbeat_request_codec::HeartbeatRequestDecoder;
use deepmsg_codec::cluster::message_header_codec::MessageHeaderDecoder;
use deepmsg_codec::cluster::new_leader_event_codec::NewLeaderEventDecoder;
use deepmsg_codec::cluster::new_leadership_term_codec::NewLeadershipTermDecoder;
use deepmsg_codec::cluster::pending_message_tracker_codec::PendingMessageTrackerDecoder;
use deepmsg_codec::cluster::request_vote_codec::RequestVoteDecoder;
use deepmsg_codec::cluster::schedule_timer_codec::ScheduleTimerDecoder;
use deepmsg_codec::cluster::service_ack_codec::ServiceAckDecoder;
use deepmsg_codec::cluster::session_close_request_codec::SessionCloseRequestDecoder;
use deepmsg_codec::cluster::session_connect_request_codec::SessionConnectRequestDecoder;
use deepmsg_codec::cluster::session_event_codec::SessionEventDecoder;
use deepmsg_codec::cluster::session_keep_alive_codec::SessionKeepAliveDecoder;
use deepmsg_codec::cluster::session_message_header_codec::SessionMessageHeaderDecoder;
use deepmsg_codec::cluster::snapshot_marker_codec::SnapshotMarkerDecoder;
use deepmsg_codec::cluster::standby_snapshot_codec::StandbySnapshotDecoder;
use deepmsg_codec::cluster::stop_catchup_codec::StopCatchupDecoder;
use deepmsg_codec::cluster::termination_ack_codec::TerminationAckDecoder;
use deepmsg_codec::cluster::termination_position_codec::TerminationPositionDecoder;
use deepmsg_codec::cluster::timer_codec::TimerDecoder;
use deepmsg_codec::cluster::vote_codec::VoteDecoder;
use deepmsg_codec::cluster::{
    ReadBuf, backup_response_codec, cluster_members_extended_response_codec,
};

fn session_message_header(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let decoder = SessionMessageHeaderDecoder::default().header(header, 0);
    let _ = decoder.leadership_term_id();
    let _ = decoder.cluster_session_id();
    let _ = decoder.timestamp();
}

fn session_event(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let mut decoder = SessionEventDecoder::default().header(header, 0);
    let _ = decoder.cluster_session_id();
    let _ = decoder.correlation_id();
    let _ = decoder.leadership_term_id();
    let _ = decoder.leader_member_id();
    let _ = decoder.code();
    let _ = decoder.version();
    let _ = decoder.leader_heartbeat_timeout_ns();
    let coordinates = decoder.detail_decoder();
    let _ = decoder.detail_slice(coordinates);
}

fn session_connect_request(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let mut decoder = SessionConnectRequestDecoder::default().header(header, 0);
    let _ = decoder.correlation_id();
    let _ = decoder.response_stream_id();
    let _ = decoder.version();
    let channel = decoder.response_channel_decoder();
    let credentials = decoder.encoded_credentials_decoder();
    let info = decoder.client_info_decoder();
    let _ = decoder.response_channel_slice(channel);
    let _ = decoder.encoded_credentials_slice(credentials);
    let _ = decoder.client_info_slice(info);
}

fn session_close_request(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let decoder = SessionCloseRequestDecoder::default().header(header, 0);
    let _ = decoder.leadership_term_id();
    let _ = decoder.cluster_session_id();
}

fn session_keep_alive(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let decoder = SessionKeepAliveDecoder::default().header(header, 0);
    let _ = decoder.leadership_term_id();
    let _ = decoder.cluster_session_id();
}

fn new_leader_event(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let mut decoder = NewLeaderEventDecoder::default().header(header, 0);
    let _ = decoder.leadership_term_id();
    let _ = decoder.cluster_session_id();
    let _ = decoder.leader_member_id();
    let coordinates = decoder.ingress_endpoints_decoder();
    let _ = decoder.ingress_endpoints_slice(coordinates);
}

fn challenge(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let mut decoder = ChallengeDecoder::default().header(header, 0);
    let _ = decoder.correlation_id();
    let _ = decoder.cluster_session_id();
    let coordinates = decoder.encoded_challenge_decoder();
    let _ = decoder.encoded_challenge_slice(coordinates);
}

fn challenge_response(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let mut decoder = ChallengeResponseDecoder::default().header(header, 0);
    let _ = decoder.correlation_id();
    let _ = decoder.cluster_session_id();
    let coordinates = decoder.encoded_credentials_decoder();
    let _ = decoder.encoded_credentials_slice(coordinates);
}

fn admin_request(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let mut decoder = AdminRequestDecoder::default().header(header, 0);
    let _ = decoder.leadership_term_id();
    let _ = decoder.cluster_session_id();
    let _ = decoder.correlation_id();
    let _ = decoder.request_type();
    let coordinates = decoder.payload_decoder();
    let _ = decoder.payload_slice(coordinates);
}

fn admin_response(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let mut decoder = AdminResponseDecoder::default().header(header, 0);
    let _ = decoder.cluster_session_id();
    let _ = decoder.correlation_id();
    let _ = decoder.request_type();
    let _ = decoder.response_code();
    let coordinates = decoder.message_decoder();
    let _ = decoder.message_slice(coordinates);
    let coordinates = decoder.payload_decoder();
    let _ = decoder.payload_slice(coordinates);
}

fn close_session(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let decoder = CloseSessionDecoder::default().header(header, 0);
    let _ = decoder.cluster_session_id();
}

fn schedule_timer(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let decoder = ScheduleTimerDecoder::default().header(header, 0);
    let _ = decoder.correlation_id();
    let _ = decoder.deadline();
}

fn cancel_timer(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let decoder =
        deepmsg_codec::cluster::cancel_timer_codec::CancelTimerDecoder::default().header(header, 0);
    let _ = decoder.correlation_id();
}

fn service_ack(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let decoder = ServiceAckDecoder::default().header(header, 0);
    let _ = decoder.log_position();
    let _ = decoder.timestamp();
    let _ = decoder.ack_id();
    let _ = decoder.relevant_id();
    let _ = decoder.service_id();
}

fn cluster_members_query(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let decoder = ClusterMembersQueryDecoder::default().header(header, 0);
    let _ = decoder.correlation_id();
    let _ = decoder.extended();
}

fn cluster_members_response(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let mut decoder = ClusterMembersResponseDecoder::default().header(header, 0);
    let _ = decoder.correlation_id();
    let _ = decoder.leader_member_id();
    let coordinates = decoder.active_members_decoder();
    let _ = decoder.active_members_slice(coordinates);
    let coordinates = decoder.passive_followers_decoder();
    let _ = decoder.passive_followers_slice(coordinates);
}

fn cluster_members_extended_response(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let decoder = ClusterMembersExtendedResponseDecoder::default().header(header, 0);
    let _ = decoder.correlation_id();
    let _ = decoder.current_time_ns();
    let _ = decoder.leader_member_id();
    let _ = decoder.member_id();

    let mut group = decoder.active_members_decoder();
    while group.advance().ok().flatten().is_some() {
        let _ = group.leadership_term_id();
        let _ = group.log_position();
        let _ = group.time_of_last_append_ns();
        let _ = group.member_id();
        let at = group.ingress_endpoint_decoder();
        let _ = group.ingress_endpoint_slice(at);
        let at = group.consensus_endpoint_decoder();
        let _ = group.consensus_endpoint_slice(at);
        let at = group.log_endpoint_decoder();
        let _ = group.log_endpoint_slice(at);
        let at = group.catchup_endpoint_decoder();
        let _ = group.catchup_endpoint_slice(at);
        let at = group.archive_endpoint_decoder();
        let _ = group.archive_endpoint_slice(at);
    }
}

fn canvass_position(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let decoder = CanvassPositionDecoder::default().header(header, 0);
    let _ = decoder.log_leadership_term_id();
    let _ = decoder.log_position();
    let _ = decoder.leadership_term_id();
    let _ = decoder.follower_member_id();
    let _ = decoder.protocol_version();
}

fn request_vote(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let decoder = RequestVoteDecoder::default().header(header, 0);
    let _ = decoder.log_leadership_term_id();
    let _ = decoder.log_position();
    let _ = decoder.candidate_term_id();
    let _ = decoder.candidate_member_id();
    let _ = decoder.protocol_version();
}

fn vote(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let decoder = VoteDecoder::default().header(header, 0);
    let _ = decoder.candidate_term_id();
    let _ = decoder.log_leadership_term_id();
    let _ = decoder.log_position();
    let _ = decoder.candidate_member_id();
    let _ = decoder.follower_member_id();
    let _ = decoder.vote();
}

fn new_leadership_term(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let decoder = NewLeadershipTermDecoder::default().header(header, 0);
    let _ = decoder.log_leadership_term_id();
    let _ = decoder.next_leadership_term_id();
    let _ = decoder.next_term_base_log_position();
    let _ = decoder.next_log_position();
    let _ = decoder.leadership_term_id();
    let _ = decoder.term_base_log_position();
    let _ = decoder.log_position();
    let _ = decoder.commit_position();
    let _ = decoder.leader_recording_id();
    let _ = decoder.timestamp();
    let _ = decoder.leader_member_id();
    let _ = decoder.log_session_id();
    let _ = decoder.app_version();
    let _ = decoder.is_startup();
}

fn append_position(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let decoder = AppendPositionDecoder::default().header(header, 0);
    let _ = decoder.leadership_term_id();
    let _ = decoder.log_position();
    let _ = decoder.follower_member_id();
    let _ = decoder.flags();
}

fn commit_position(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let decoder = CommitPositionDecoder::default().header(header, 0);
    let _ = decoder.leadership_term_id();
    let _ = decoder.log_position();
    let _ = decoder.leader_member_id();
}

fn catchup_position(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let mut decoder = CatchupPositionDecoder::default().header(header, 0);
    let _ = decoder.leadership_term_id();
    let _ = decoder.log_position();
    let _ = decoder.follower_member_id();
    let coordinates = decoder.catchup_endpoint_decoder();
    let _ = decoder.catchup_endpoint_slice(coordinates);
}

fn stop_catchup(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let decoder = StopCatchupDecoder::default().header(header, 0);
    let _ = decoder.leadership_term_id();
    let _ = decoder.follower_member_id();
}

fn termination_position(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let decoder = TerminationPositionDecoder::default().header(header, 0);
    let _ = decoder.leadership_term_id();
    let _ = decoder.log_position();
}

fn termination_ack(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let decoder = TerminationAckDecoder::default().header(header, 0);
    let _ = decoder.leadership_term_id();
    let _ = decoder.log_position();
    let _ = decoder.member_id();
}

fn backup_query(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let mut decoder = BackupQueryDecoder::default().header(header, 0);
    let coordinates = decoder.response_channel_decoder();
    let _ = decoder.response_channel_slice(coordinates);
    let coordinates = decoder.encoded_credentials_decoder();
    let _ = decoder.encoded_credentials_slice(coordinates);
    let _ = decoder.correlation_id();
    let _ = decoder.response_stream_id();
    let _ = decoder.version();
    let _ = decoder.log_position();
}

fn backup_response(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let decoder = BackupResponseDecoder::default().header(header, 0);
    let _ = decoder.correlation_id();
    let _ = decoder.log_recording_id();
    let _ = decoder.log_leadership_term_id();
    let _ = decoder.log_term_base_log_position();
    let _ = decoder.last_leadership_term_id();
    let _ = decoder.last_term_base_log_position();
    let _ = decoder.commit_position_counter_id();
    let _ = decoder.leader_member_id();
    let _ = decoder.member_id();

    let mut group = decoder.snapshots_decoder();
    while group.advance().ok().flatten().is_some() {
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
}

fn heartbeat_request(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let mut decoder = HeartbeatRequestDecoder::default().header(header, 0);
    let _ = decoder.correlation_id();
    let _ = decoder.response_stream_id();
    let coordinates = decoder.response_channel_decoder();
    let _ = decoder.response_channel_slice(coordinates);
}

fn standby_snapshot(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let decoder = StandbySnapshotDecoder::default().header(header, 0);
    let _ = decoder.correlation_id();
    let _ = decoder.version();
    let _ = decoder.response_stream_id();
}

fn snapshot_marker(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let decoder = SnapshotMarkerDecoder::default().header(header, 0);
    let _ = decoder.type_id();
    let _ = decoder.log_position();
    let _ = decoder.leadership_term_id();
    let _ = decoder.index();
    let _ = decoder.mark();
    let _ = decoder.time_unit();
    let _ = decoder.app_version();
}

fn client_session(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let mut decoder = ClientSessionDecoder::default().header(header, 0);
    let _ = decoder.cluster_session_id();
    let _ = decoder.response_stream_id();
    let coordinates = decoder.response_channel_decoder();
    let _ = decoder.response_channel_slice(coordinates);
    let coordinates = decoder.encoded_principal_decoder();
    let _ = decoder.encoded_principal_slice(coordinates);
}

fn cluster_session(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let mut decoder = ClusterSessionDecoder::default().header(header, 0);
    let _ = decoder.cluster_session_id();
    let _ = decoder.correlation_id();
    let _ = decoder.opened_log_position();
    let _ = decoder.time_of_last_activity();
    let _ = decoder.close_reason();
    let _ = decoder.response_stream_id();
    let coordinates = decoder.response_channel_decoder();
    let _ = decoder.response_channel_slice(coordinates);
}

fn timer(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let decoder = TimerDecoder::default().header(header, 0);
    let _ = decoder.correlation_id();
    let _ = decoder.deadline();
}

fn consensus_module(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let decoder = ConsensusModuleDecoder::default().header(header, 0);
    let _ = decoder.next_session_id();
    let _ = decoder.next_service_session_id();
    let _ = decoder.log_service_session_id();
    let _ = decoder.pending_message_capacity();
}

fn pending_message_tracker(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let decoder = PendingMessageTrackerDecoder::default().header(header, 0);
    let _ = decoder.next_service_session_id();
    let _ = decoder.log_service_session_id();
    let _ = decoder.pending_message_capacity();
    let _ = decoder.service_id();
}

/// The templates this module decodes, for the coverage test.
pub const DECODED_TEMPLATES: &[u16] = &[
    deepmsg_codec::cluster::session_message_header_codec::SBE_TEMPLATE_ID,
    deepmsg_codec::cluster::session_event_codec::SBE_TEMPLATE_ID,
    deepmsg_codec::cluster::session_connect_request_codec::SBE_TEMPLATE_ID,
    deepmsg_codec::cluster::session_close_request_codec::SBE_TEMPLATE_ID,
    deepmsg_codec::cluster::session_keep_alive_codec::SBE_TEMPLATE_ID,
    deepmsg_codec::cluster::new_leader_event_codec::SBE_TEMPLATE_ID,
    deepmsg_codec::cluster::challenge_codec::SBE_TEMPLATE_ID,
    deepmsg_codec::cluster::challenge_response_codec::SBE_TEMPLATE_ID,
    deepmsg_codec::cluster::admin_request_codec::SBE_TEMPLATE_ID,
    deepmsg_codec::cluster::admin_response_codec::SBE_TEMPLATE_ID,
    deepmsg_codec::cluster::close_session_codec::SBE_TEMPLATE_ID,
    deepmsg_codec::cluster::schedule_timer_codec::SBE_TEMPLATE_ID,
    deepmsg_codec::cluster::cancel_timer_codec::SBE_TEMPLATE_ID,
    deepmsg_codec::cluster::service_ack_codec::SBE_TEMPLATE_ID,
    deepmsg_codec::cluster::cluster_members_query_codec::SBE_TEMPLATE_ID,
    deepmsg_codec::cluster::cluster_members_response_codec::SBE_TEMPLATE_ID,
    cluster_members_extended_response_codec::SBE_TEMPLATE_ID,
    deepmsg_codec::cluster::canvass_position_codec::SBE_TEMPLATE_ID,
    deepmsg_codec::cluster::request_vote_codec::SBE_TEMPLATE_ID,
    deepmsg_codec::cluster::vote_codec::SBE_TEMPLATE_ID,
    deepmsg_codec::cluster::new_leadership_term_codec::SBE_TEMPLATE_ID,
    deepmsg_codec::cluster::append_position_codec::SBE_TEMPLATE_ID,
    deepmsg_codec::cluster::commit_position_codec::SBE_TEMPLATE_ID,
    deepmsg_codec::cluster::catchup_position_codec::SBE_TEMPLATE_ID,
    deepmsg_codec::cluster::stop_catchup_codec::SBE_TEMPLATE_ID,
    deepmsg_codec::cluster::termination_position_codec::SBE_TEMPLATE_ID,
    deepmsg_codec::cluster::termination_ack_codec::SBE_TEMPLATE_ID,
    deepmsg_codec::cluster::backup_query_codec::SBE_TEMPLATE_ID,
    backup_response_codec::SBE_TEMPLATE_ID,
    deepmsg_codec::cluster::heartbeat_request_codec::SBE_TEMPLATE_ID,
    deepmsg_codec::cluster::standby_snapshot_codec::SBE_TEMPLATE_ID,
    deepmsg_codec::cluster::snapshot_marker_codec::SBE_TEMPLATE_ID,
    deepmsg_codec::cluster::client_session_codec::SBE_TEMPLATE_ID,
    deepmsg_codec::cluster::cluster_session_codec::SBE_TEMPLATE_ID,
    deepmsg_codec::cluster::timer_codec::SBE_TEMPLATE_ID,
    deepmsg_codec::cluster::consensus_module_codec::SBE_TEMPLATE_ID,
    deepmsg_codec::cluster::pending_message_tracker_codec::SBE_TEMPLATE_ID,
];

macro_rules! decode_templates {
    ($( $template:path => $decode:ident ),+ $(,)?) => {
        /// Read every field a production adapter reads for the message's
        /// template; a template no production path decodes is a no-op.
        pub fn decode_one(template_id: u16, header: MessageHeaderDecoder<ReadBuf<'_>>) {
            match template_id {
                $( $template => $decode(header), )+
                _ => {}
            }
        }
    };
}

decode_templates! {
    deepmsg_codec::cluster::session_message_header_codec::SBE_TEMPLATE_ID => session_message_header,
    deepmsg_codec::cluster::session_event_codec::SBE_TEMPLATE_ID => session_event,
    deepmsg_codec::cluster::session_connect_request_codec::SBE_TEMPLATE_ID => session_connect_request,
    deepmsg_codec::cluster::session_close_request_codec::SBE_TEMPLATE_ID => session_close_request,
    deepmsg_codec::cluster::session_keep_alive_codec::SBE_TEMPLATE_ID => session_keep_alive,
    deepmsg_codec::cluster::new_leader_event_codec::SBE_TEMPLATE_ID => new_leader_event,
    deepmsg_codec::cluster::challenge_codec::SBE_TEMPLATE_ID => challenge,
    deepmsg_codec::cluster::challenge_response_codec::SBE_TEMPLATE_ID => challenge_response,
    deepmsg_codec::cluster::admin_request_codec::SBE_TEMPLATE_ID => admin_request,
    deepmsg_codec::cluster::admin_response_codec::SBE_TEMPLATE_ID => admin_response,
    deepmsg_codec::cluster::close_session_codec::SBE_TEMPLATE_ID => close_session,
    deepmsg_codec::cluster::schedule_timer_codec::SBE_TEMPLATE_ID => schedule_timer,
    deepmsg_codec::cluster::cancel_timer_codec::SBE_TEMPLATE_ID => cancel_timer,
    deepmsg_codec::cluster::service_ack_codec::SBE_TEMPLATE_ID => service_ack,
    deepmsg_codec::cluster::cluster_members_query_codec::SBE_TEMPLATE_ID => cluster_members_query,
    deepmsg_codec::cluster::cluster_members_response_codec::SBE_TEMPLATE_ID => cluster_members_response,
    cluster_members_extended_response_codec::SBE_TEMPLATE_ID => cluster_members_extended_response,
    deepmsg_codec::cluster::canvass_position_codec::SBE_TEMPLATE_ID => canvass_position,
    deepmsg_codec::cluster::request_vote_codec::SBE_TEMPLATE_ID => request_vote,
    deepmsg_codec::cluster::vote_codec::SBE_TEMPLATE_ID => vote,
    deepmsg_codec::cluster::new_leadership_term_codec::SBE_TEMPLATE_ID => new_leadership_term,
    deepmsg_codec::cluster::append_position_codec::SBE_TEMPLATE_ID => append_position,
    deepmsg_codec::cluster::commit_position_codec::SBE_TEMPLATE_ID => commit_position,
    deepmsg_codec::cluster::catchup_position_codec::SBE_TEMPLATE_ID => catchup_position,
    deepmsg_codec::cluster::stop_catchup_codec::SBE_TEMPLATE_ID => stop_catchup,
    deepmsg_codec::cluster::termination_position_codec::SBE_TEMPLATE_ID => termination_position,
    deepmsg_codec::cluster::termination_ack_codec::SBE_TEMPLATE_ID => termination_ack,
    deepmsg_codec::cluster::backup_query_codec::SBE_TEMPLATE_ID => backup_query,
    backup_response_codec::SBE_TEMPLATE_ID => backup_response,
    deepmsg_codec::cluster::heartbeat_request_codec::SBE_TEMPLATE_ID => heartbeat_request,
    deepmsg_codec::cluster::standby_snapshot_codec::SBE_TEMPLATE_ID => standby_snapshot,
    deepmsg_codec::cluster::snapshot_marker_codec::SBE_TEMPLATE_ID => snapshot_marker,
    deepmsg_codec::cluster::client_session_codec::SBE_TEMPLATE_ID => client_session,
    deepmsg_codec::cluster::cluster_session_codec::SBE_TEMPLATE_ID => cluster_session,
    deepmsg_codec::cluster::timer_codec::SBE_TEMPLATE_ID => timer,
    deepmsg_codec::cluster::consensus_module_codec::SBE_TEMPLATE_ID => consensus_module,
    deepmsg_codec::cluster::pending_message_tracker_codec::SBE_TEMPLATE_ID => pending_message_tracker,
}

/// `SBE_SCHEMA_ID`, re-exported so the target's header check and this module
/// agree on the schema it decodes.
pub const SCHEMA_ID: u16 = SBE_SCHEMA_ID;
