//! Target 07's half of the archive control surface: one decoder walk per
//! template `control_adapter::dispatch` answers.
//!
//! The bounds check itself is **not** repeated here — the target calls
//! `control_adapter::control_request_bounds`, the same function production
//! calls, so the guard and the fuzzer cannot drift apart. What this module
//! repeats is the decoder calls, in the order `dispatch` makes them: every
//! `_decoder()` before its `_slice()`, because each one moves the decoder's
//! limit over its own variable-length field.
//!
//! `DECODED_TEMPLATES` and the `match` in [`decode_one`] are generated from the
//! same list; `fuzz/tests/template_coverage.rs` pins that list to
//! `control_adapter::CONTROL_REQUEST_TEMPLATES`.

use deepmsg_codec::archive::archive_id_request_codec::ArchiveIdRequestDecoder;
use deepmsg_codec::archive::attach_segments_request_codec::AttachSegmentsRequestDecoder;
use deepmsg_codec::archive::auth_connect_request_codec::AuthConnectRequestDecoder;
use deepmsg_codec::archive::bounded_replay_request_codec::BoundedReplayRequestDecoder;
use deepmsg_codec::archive::challenge_response_codec::ChallengeResponseDecoder;
use deepmsg_codec::archive::close_session_request_codec::CloseSessionRequestDecoder;
use deepmsg_codec::archive::delete_detached_segments_request_codec::DeleteDetachedSegmentsRequestDecoder;
use deepmsg_codec::archive::detach_segments_request_codec::DetachSegmentsRequestDecoder;
use deepmsg_codec::archive::extend_recording_request_2_codec::ExtendRecordingRequest2Decoder;
use deepmsg_codec::archive::find_last_matching_recording_request_codec::FindLastMatchingRecordingRequestDecoder;
use deepmsg_codec::archive::keep_alive_request_codec::KeepAliveRequestDecoder;
use deepmsg_codec::archive::list_recording_request_codec::ListRecordingRequestDecoder;
use deepmsg_codec::archive::list_recording_subscriptions_request_codec::ListRecordingSubscriptionsRequestDecoder;
use deepmsg_codec::archive::list_recordings_for_uri_request_codec::ListRecordingsForUriRequestDecoder;
use deepmsg_codec::archive::list_recordings_request_codec::ListRecordingsRequestDecoder;
use deepmsg_codec::archive::max_recorded_position_request_codec::MaxRecordedPositionRequestDecoder;
use deepmsg_codec::archive::message_header_codec::MessageHeaderDecoder;
use deepmsg_codec::archive::purge_recording_request_codec::PurgeRecordingRequestDecoder;
use deepmsg_codec::archive::purge_segments_request_codec::PurgeSegmentsRequestDecoder;
use deepmsg_codec::archive::recording_position_request_codec::RecordingPositionRequestDecoder;
use deepmsg_codec::archive::replay_request_codec::ReplayRequestDecoder;
use deepmsg_codec::archive::replay_token_request_codec::ReplayTokenRequestDecoder;
use deepmsg_codec::archive::replicate_request_2_codec::ReplicateRequest2Decoder;
use deepmsg_codec::archive::start_position_request_codec::StartPositionRequestDecoder;
use deepmsg_codec::archive::start_recording_request_2_codec::StartRecordingRequest2Decoder;
use deepmsg_codec::archive::start_recording_request_codec::StartRecordingRequestDecoder;
use deepmsg_codec::archive::stop_all_replays_request_codec::StopAllReplaysRequestDecoder;
use deepmsg_codec::archive::stop_position_request_codec::StopPositionRequestDecoder;
use deepmsg_codec::archive::stop_recording_by_identity_request_codec::StopRecordingByIdentityRequestDecoder;
use deepmsg_codec::archive::stop_recording_request_codec::StopRecordingRequestDecoder;
use deepmsg_codec::archive::stop_recording_subscription_request_codec::StopRecordingSubscriptionRequestDecoder;
use deepmsg_codec::archive::stop_replay_request_codec::StopReplayRequestDecoder;
use deepmsg_codec::archive::stop_replication_request_codec::StopReplicationRequestDecoder;
use deepmsg_codec::archive::truncate_recording_request_codec::TruncateRecordingRequestDecoder;
use deepmsg_codec::archive::update_channel_request_codec::UpdateChannelRequestDecoder;
use deepmsg_codec::archive::{
    ReadBuf, archive_id_request_codec, attach_segments_request_codec, auth_connect_request_codec,
    bounded_replay_request_codec, challenge_response_codec, close_session_request_codec,
    delete_detached_segments_request_codec, detach_segments_request_codec,
    extend_recording_request_2_codec, find_last_matching_recording_request_codec,
    keep_alive_request_codec, list_recording_request_codec,
    list_recording_subscriptions_request_codec, list_recordings_for_uri_request_codec,
    list_recordings_request_codec, max_recorded_position_request_codec,
    purge_recording_request_codec, purge_segments_request_codec, recording_position_request_codec,
    replay_request_codec, replay_token_request_codec, replicate_request_2_codec,
    start_position_request_codec, start_recording_request_2_codec, start_recording_request_codec,
    stop_all_replays_request_codec, stop_position_request_codec,
    stop_recording_by_identity_request_codec, stop_recording_request_codec,
    stop_recording_subscription_request_codec, stop_replay_request_codec,
    stop_replication_request_codec, truncate_recording_request_codec, update_channel_request_codec,
};

fn close_session(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let decoder = CloseSessionRequestDecoder::default().header(header, 0);
    let _ = decoder.control_session_id();
}

fn start_recording(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let mut decoder = StartRecordingRequestDecoder::default().header(header, 0);
    let coordinates = decoder.channel_decoder();
    let _ = decoder.control_session_id();
    let _ = decoder.correlation_id();
    let _ = decoder.stream_id();
    let _ = decoder.source_location();
    let _ = decoder.channel_slice(coordinates);
}

fn stop_recording(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let mut decoder = StopRecordingRequestDecoder::default().header(header, 0);
    let _ = decoder.control_session_id();
    let _ = decoder.correlation_id();
    let _ = decoder.stream_id();
    let coordinates = decoder.channel_decoder();
    let _ = decoder.channel_slice(coordinates);
}

fn replay(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let mut decoder = ReplayRequestDecoder::default().header(header, 0);
    let coordinates = decoder.replay_channel_decoder();
    let _ = decoder.control_session_id();
    let _ = decoder.correlation_id();
    let _ = decoder.recording_id();
    let _ = decoder.position();
    let _ = decoder.length();
    let _ = decoder.replay_stream_id();
    let _ = decoder.file_io_max_length();
    let _ = decoder.replay_token();
    let _ = decoder.replay_channel_slice(coordinates);
}

fn stop_replay(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let decoder = StopReplayRequestDecoder::default().header(header, 0);
    let _ = decoder.control_session_id();
    let _ = decoder.correlation_id();
    let _ = decoder.replay_session_id();
}

fn list_recordings(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let decoder = ListRecordingsRequestDecoder::default().header(header, 0);
    let _ = decoder.control_session_id();
    let _ = decoder.correlation_id();
    let _ = decoder.from_recording_id();
    let _ = decoder.record_count();
}

fn list_recordings_for_uri(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let mut decoder = ListRecordingsForUriRequestDecoder::default().header(header, 0);
    let _ = decoder.control_session_id();
    let _ = decoder.correlation_id();
    let _ = decoder.from_recording_id();
    let _ = decoder.record_count();
    let _ = decoder.stream_id();
    let coordinates = decoder.channel_decoder();
    let _ = decoder.channel_slice(coordinates);
}

fn list_recording(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let decoder = ListRecordingRequestDecoder::default().header(header, 0);
    let _ = decoder.control_session_id();
    let _ = decoder.correlation_id();
    let _ = decoder.recording_id();
}

fn recording_position(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let decoder = RecordingPositionRequestDecoder::default().header(header, 0);
    let _ = decoder.control_session_id();
    let _ = decoder.correlation_id();
    let _ = decoder.recording_id();
}

fn truncate_recording(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let decoder = TruncateRecordingRequestDecoder::default().header(header, 0);
    let _ = decoder.control_session_id();
    let _ = decoder.correlation_id();
    let _ = decoder.recording_id();
    let _ = decoder.position();
}

fn stop_recording_subscription(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let decoder = StopRecordingSubscriptionRequestDecoder::default().header(header, 0);
    let _ = decoder.control_session_id();
    let _ = decoder.correlation_id();
    let _ = decoder.subscription_id();
}

fn stop_position(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let decoder = StopPositionRequestDecoder::default().header(header, 0);
    let _ = decoder.control_session_id();
    let _ = decoder.correlation_id();
    let _ = decoder.recording_id();
}

fn find_last_matching_recording(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let mut decoder = FindLastMatchingRecordingRequestDecoder::default().header(header, 0);
    let _ = decoder.control_session_id();
    let _ = decoder.correlation_id();
    let _ = decoder.min_recording_id();
    let _ = decoder.session_id();
    let _ = decoder.stream_id();
    let coordinates = decoder.channel_decoder();
    let _ = decoder.channel_slice(coordinates);
}

fn list_recording_subscriptions(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let mut decoder = ListRecordingSubscriptionsRequestDecoder::default().header(header, 0);
    let _ = decoder.control_session_id();
    let _ = decoder.correlation_id();
    let _ = decoder.pseudo_index();
    let _ = decoder.subscription_count();
    let _ = decoder.apply_stream_id();
    let _ = decoder.stream_id();
    let coordinates = decoder.channel_decoder();
    let _ = decoder.channel_slice(coordinates);
}

fn bounded_replay(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let mut decoder = BoundedReplayRequestDecoder::default().header(header, 0);
    let coordinates = decoder.replay_channel_decoder();
    let _ = decoder.control_session_id();
    let _ = decoder.correlation_id();
    let _ = decoder.recording_id();
    let _ = decoder.position();
    let _ = decoder.length();
    let _ = decoder.replay_stream_id();
    let _ = decoder.file_io_max_length();
    let _ = decoder.replay_token();
    let _ = decoder.limit_counter_id();
    let _ = decoder.replay_channel_slice(coordinates);
}

fn stop_all_replays(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let decoder = StopAllReplaysRequestDecoder::default().header(header, 0);
    let _ = decoder.control_session_id();
    let _ = decoder.correlation_id();
    let _ = decoder.recording_id();
}

fn stop_replication(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let decoder = StopReplicationRequestDecoder::default().header(header, 0);
    let _ = decoder.control_session_id();
    let _ = decoder.correlation_id();
    let _ = decoder.replication_id();
}

fn start_position(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let decoder = StartPositionRequestDecoder::default().header(header, 0);
    let _ = decoder.control_session_id();
    let _ = decoder.correlation_id();
    let _ = decoder.recording_id();
}

fn detach_segments(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let decoder = DetachSegmentsRequestDecoder::default().header(header, 0);
    let _ = decoder.control_session_id();
    let _ = decoder.correlation_id();
    let _ = decoder.recording_id();
    let _ = decoder.new_start_position();
}

fn delete_detached_segments(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let decoder = DeleteDetachedSegmentsRequestDecoder::default().header(header, 0);
    let _ = decoder.control_session_id();
    let _ = decoder.correlation_id();
    let _ = decoder.recording_id();
}

fn purge_segments(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let decoder = PurgeSegmentsRequestDecoder::default().header(header, 0);
    let _ = decoder.control_session_id();
    let _ = decoder.correlation_id();
    let _ = decoder.recording_id();
    let _ = decoder.new_start_position();
}

fn attach_segments(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let decoder = AttachSegmentsRequestDecoder::default().header(header, 0);
    let _ = decoder.control_session_id();
    let _ = decoder.correlation_id();
    let _ = decoder.recording_id();
}

fn auth_connect(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let mut decoder = AuthConnectRequestDecoder::default().header(header, 0);

    // Every `_decoder` moves the limit over its own field, so all three are
    // taken before any slice — the order the adapter uses.
    let channel = decoder.response_channel_decoder();
    let credentials = decoder.encoded_credentials_decoder();
    let client_info = decoder.client_info_decoder();

    let _ = decoder.correlation_id();
    let _ = decoder.response_stream_id();
    let _ = decoder.version();
    let _ = decoder.response_channel_slice(channel);
    let _ = decoder.encoded_credentials_slice(credentials);
    let _ = decoder.client_info_slice(client_info);
}

fn challenge_response(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let mut decoder = ChallengeResponseDecoder::default().header(header, 0);
    let _ = decoder.control_session_id();
    let _ = decoder.correlation_id();
    let credentials = decoder.encoded_credentials_decoder();
    let _ = decoder.encoded_credentials_slice(credentials);
}

fn keep_alive(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let decoder = KeepAliveRequestDecoder::default().header(header, 0);
    let _ = decoder.control_session_id();
    let _ = decoder.correlation_id();
}

fn start_recording_2(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let mut decoder = StartRecordingRequest2Decoder::default().header(header, 0);
    let coordinates = decoder.channel_decoder();
    let _ = decoder.control_session_id();
    let _ = decoder.correlation_id();
    let _ = decoder.stream_id();
    let _ = decoder.source_location();
    let _ = decoder.auto_stop();
    let _ = decoder.channel_slice(coordinates);
}

fn extend_recording_2(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let mut decoder = ExtendRecordingRequest2Decoder::default().header(header, 0);
    let _ = decoder.control_session_id();
    let _ = decoder.correlation_id();
    let _ = decoder.recording_id();
    let _ = decoder.stream_id();
    let _ = decoder.source_location();
    let _ = decoder.auto_stop();
    let coordinates = decoder.channel_decoder();
    let _ = decoder.channel_slice(coordinates);
}

fn stop_recording_by_identity(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let decoder = StopRecordingByIdentityRequestDecoder::default().header(header, 0);
    let _ = decoder.control_session_id();
    let _ = decoder.correlation_id();
    let _ = decoder.recording_id();
}

fn replicate_2(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let mut decoder = ReplicateRequest2Decoder::default().header(header, 0);

    let _ = decoder.control_session_id();
    let _ = decoder.correlation_id();
    let _ = decoder.src_recording_id();
    let _ = decoder.dst_recording_id();
    let _ = decoder.stop_position();
    let _ = decoder.channel_tag_id();
    let _ = decoder.subscription_tag_id();
    let _ = decoder.src_control_stream_id();
    let _ = decoder.file_io_max_length();
    let _ = decoder.replication_session_id();

    // The five variable-length fields are read **in schema order**: each one
    // starts where the one before it ended.
    let coordinates = decoder.src_control_channel_decoder();
    let _ = decoder.src_control_channel_slice(coordinates);
    let coordinates = decoder.live_destination_decoder();
    let _ = decoder.live_destination_slice(coordinates);
    let coordinates = decoder.replication_channel_decoder();
    let _ = decoder.replication_channel_slice(coordinates);
    let coordinates = decoder.encoded_credentials_decoder();
    let _ = decoder.encoded_credentials_slice(coordinates);
    let coordinates = decoder.src_response_channel_decoder();
    let _ = decoder.src_response_channel_slice(coordinates);
}

fn max_recorded_position(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let decoder = MaxRecordedPositionRequestDecoder::default().header(header, 0);
    let _ = decoder.control_session_id();
    let _ = decoder.correlation_id();
    let _ = decoder.recording_id();
}

fn archive_id(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let decoder = ArchiveIdRequestDecoder::default().header(header, 0);
    let _ = decoder.control_session_id();
    let _ = decoder.correlation_id();
}

fn purge_recording(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let decoder = PurgeRecordingRequestDecoder::default().header(header, 0);
    let _ = decoder.control_session_id();
    let _ = decoder.correlation_id();
    let _ = decoder.recording_id();
}

fn replay_token(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let decoder = ReplayTokenRequestDecoder::default().header(header, 0);
    let _ = decoder.control_session_id();
    let _ = decoder.correlation_id();
    let _ = decoder.recording_id();
}

fn update_channel(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let mut decoder = UpdateChannelRequestDecoder::default().header(header, 0);
    let coordinates = decoder.channel_decoder();
    let _ = decoder.control_session_id();
    let _ = decoder.correlation_id();
    let _ = decoder.recording_id();
    let _ = decoder.channel_slice(coordinates);
}

/// Generate both the template list and the match from one source, so a target
/// that forgets a decoder cannot compile and a list that disagrees with the
/// match cannot exist.
macro_rules! decode_templates {
    ($( $template:path => $decode:ident ),+ $(,)?) => {
        /// The templates this target decodes, in `CONTROL_REQUEST_TEMPLATES`
        /// order.
        pub const DECODED_TEMPLATES: &[u16] = &[ $( $template ),+ ];

        /// Read every field `dispatch` reads for the message's template.
        ///
        /// A template this build does not dispatch is a no-op: the adapter's
        /// switch has no arm for it either.
        pub fn decode_one(template_id: u16, header: MessageHeaderDecoder<ReadBuf<'_>>) {
            match template_id {
                $( $template => $decode(header), )+
                _ => {}
            }
        }
    };
}

decode_templates! {
    close_session_request_codec::SBE_TEMPLATE_ID => close_session,
    start_recording_request_codec::SBE_TEMPLATE_ID => start_recording,
    stop_recording_request_codec::SBE_TEMPLATE_ID => stop_recording,
    replay_request_codec::SBE_TEMPLATE_ID => replay,
    stop_replay_request_codec::SBE_TEMPLATE_ID => stop_replay,
    list_recordings_request_codec::SBE_TEMPLATE_ID => list_recordings,
    list_recordings_for_uri_request_codec::SBE_TEMPLATE_ID => list_recordings_for_uri,
    list_recording_request_codec::SBE_TEMPLATE_ID => list_recording,
    recording_position_request_codec::SBE_TEMPLATE_ID => recording_position,
    truncate_recording_request_codec::SBE_TEMPLATE_ID => truncate_recording,
    stop_recording_subscription_request_codec::SBE_TEMPLATE_ID => stop_recording_subscription,
    stop_position_request_codec::SBE_TEMPLATE_ID => stop_position,
    find_last_matching_recording_request_codec::SBE_TEMPLATE_ID => find_last_matching_recording,
    list_recording_subscriptions_request_codec::SBE_TEMPLATE_ID => list_recording_subscriptions,
    bounded_replay_request_codec::SBE_TEMPLATE_ID => bounded_replay,
    stop_all_replays_request_codec::SBE_TEMPLATE_ID => stop_all_replays,
    stop_replication_request_codec::SBE_TEMPLATE_ID => stop_replication,
    start_position_request_codec::SBE_TEMPLATE_ID => start_position,
    detach_segments_request_codec::SBE_TEMPLATE_ID => detach_segments,
    delete_detached_segments_request_codec::SBE_TEMPLATE_ID => delete_detached_segments,
    purge_segments_request_codec::SBE_TEMPLATE_ID => purge_segments,
    attach_segments_request_codec::SBE_TEMPLATE_ID => attach_segments,
    auth_connect_request_codec::SBE_TEMPLATE_ID => auth_connect,
    challenge_response_codec::SBE_TEMPLATE_ID => challenge_response,
    keep_alive_request_codec::SBE_TEMPLATE_ID => keep_alive,
    start_recording_request_2_codec::SBE_TEMPLATE_ID => start_recording_2,
    extend_recording_request_2_codec::SBE_TEMPLATE_ID => extend_recording_2,
    stop_recording_by_identity_request_codec::SBE_TEMPLATE_ID => stop_recording_by_identity,
    replicate_request_2_codec::SBE_TEMPLATE_ID => replicate_2,
    max_recorded_position_request_codec::SBE_TEMPLATE_ID => max_recorded_position,
    archive_id_request_codec::SBE_TEMPLATE_ID => archive_id,
    purge_recording_request_codec::SBE_TEMPLATE_ID => purge_recording,
    replay_token_request_codec::SBE_TEMPLATE_ID => replay_token,
    update_channel_request_codec::SBE_TEMPLATE_ID => update_channel,
}
