#![no_main]

//! Target 07: the archive's SBE control requests.
//!
//! The bytes here are what a client writes into its control request; the
//! archive conductor decodes them with the generated flyweights. The generated
//! decoders are **indexed reads that panic past the end** — the reference's
//! Java throws where this build panics — so the server's own length guard is
//! part of the surface: this target mirrors exactly what `control_adapter.rs`
//! does before it reads a field (an 8-byte header check), and asserts that the
//! exchange answers an error instead of taking the thread down.
//!
//! The first batch is the seven request templates the control adapter
//! dispatches. The rest of the 52 archive templates are a later pass.

use deepmsg_codec::archive::auth_connect_request_codec::AuthConnectRequestDecoder;
use deepmsg_codec::archive::challenge_response_codec::ChallengeResponseDecoder;
use deepmsg_codec::archive::keep_alive_request_codec::KeepAliveRequestDecoder;
use deepmsg_codec::archive::max_recorded_position_request_codec::MaxRecordedPositionRequestDecoder;
use deepmsg_codec::archive::message_header_codec::{ENCODED_LENGTH, MessageHeaderDecoder};
use deepmsg_codec::archive::recording_position_request_codec::RecordingPositionRequestDecoder;
use deepmsg_codec::archive::start_position_request_codec::StartPositionRequestDecoder;
use deepmsg_codec::archive::stop_position_request_codec::StopPositionRequestDecoder;
use deepmsg_codec::archive::{ReadBuf, SBE_SCHEMA_ID};
use deepmsg_fuzz::variable_data_fits;
use libfuzzer_sys::fuzz_target;

fn header(payload: &[u8]) -> Option<MessageHeaderDecoder<ReadBuf<'_>>> {
    if payload.len() < ENCODED_LENGTH {
        return None;
    }

    Some(MessageHeaderDecoder::default().wrap(ReadBuf::new(payload), 0))
}

/// The production guard, as the dispatch applies it before any decoder runs.
/// The variable-length walk is shared with the stable regression tier. The
/// archive adapter's `variable_data_fits` is the same rule, written beside it.
fn guard(payload: &[u8], header: &MessageHeaderDecoder<ReadBuf<'_>>) -> bool {
    let fixed = match header.template_id() {
        deepmsg_codec::archive::auth_connect_request_codec::SBE_TEMPLATE_ID => {
            deepmsg_codec::archive::auth_connect_request_codec::SBE_BLOCK_LENGTH
        }
        deepmsg_codec::archive::challenge_response_codec::SBE_TEMPLATE_ID => {
            deepmsg_codec::archive::challenge_response_codec::SBE_BLOCK_LENGTH
        }
        deepmsg_codec::archive::keep_alive_request_codec::SBE_TEMPLATE_ID => {
            deepmsg_codec::archive::keep_alive_request_codec::SBE_BLOCK_LENGTH
        }
        deepmsg_codec::archive::max_recorded_position_request_codec::SBE_TEMPLATE_ID => {
            deepmsg_codec::archive::max_recorded_position_request_codec::SBE_BLOCK_LENGTH
        }
        deepmsg_codec::archive::start_position_request_codec::SBE_TEMPLATE_ID => {
            deepmsg_codec::archive::start_position_request_codec::SBE_BLOCK_LENGTH
        }
        deepmsg_codec::archive::recording_position_request_codec::SBE_TEMPLATE_ID => {
            deepmsg_codec::archive::recording_position_request_codec::SBE_BLOCK_LENGTH
        }
        deepmsg_codec::archive::stop_position_request_codec::SBE_TEMPLATE_ID => {
            deepmsg_codec::archive::stop_position_request_codec::SBE_BLOCK_LENGTH
        }
        _ => 0,
    };
    if payload.len() < ENCODED_LENGTH + usize::from(fixed) {
        return false;
    }

    let variables = match header.template_id() {
        deepmsg_codec::archive::auth_connect_request_codec::SBE_TEMPLATE_ID => {
            if header.version() < 12 { 2 } else { 3 }
        }
        deepmsg_codec::archive::challenge_response_codec::SBE_TEMPLATE_ID => 1,
        _ => 0,
    };
    if variables > 0 {
        let at = ENCODED_LENGTH + usize::from(header.block_length());
        if !variable_data_fits(payload, at, variables) {
            return false;
        }
    }

    true
}

/// The same order as `control_adapter.rs`: every `_decoder` call moves the
/// limit, so they are all made before any slice is taken.
fn auth_connect(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let mut decoder = AuthConnectRequestDecoder::default().header(header, 0);

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

    let credentials = decoder.encoded_credentials_decoder();
    let _ = decoder.control_session_id();
    let _ = decoder.correlation_id();
    let _ = decoder.encoded_credentials_slice(credentials);
}

fn keep_alive(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let decoder = KeepAliveRequestDecoder::default().header(header, 0);

    let _ = decoder.control_session_id();
    let _ = decoder.correlation_id();
}

fn max_recorded_position(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let decoder = MaxRecordedPositionRequestDecoder::default().header(header, 0);

    let _ = decoder.control_session_id();
    let _ = decoder.correlation_id();
    let _ = decoder.recording_id();
}

fn start_position(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let decoder = StartPositionRequestDecoder::default().header(header, 0);

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

fn stop_position(header: MessageHeaderDecoder<ReadBuf<'_>>) {
    let decoder = StopPositionRequestDecoder::default().header(header, 0);

    let _ = decoder.control_session_id();
    let _ = decoder.correlation_id();
    let _ = decoder.recording_id();
}

fuzz_target!(|data: &[u8]| {
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
        deepmsg_codec::archive::auth_connect_request_codec::SBE_TEMPLATE_ID => {
            auth_connect(header);
        }
        deepmsg_codec::archive::challenge_response_codec::SBE_TEMPLATE_ID => {
            challenge_response(header);
        }
        deepmsg_codec::archive::keep_alive_request_codec::SBE_TEMPLATE_ID => {
            keep_alive(header);
        }
        deepmsg_codec::archive::max_recorded_position_request_codec::SBE_TEMPLATE_ID => {
            max_recorded_position(header);
        }
        deepmsg_codec::archive::start_position_request_codec::SBE_TEMPLATE_ID => {
            start_position(header);
        }
        deepmsg_codec::archive::recording_position_request_codec::SBE_TEMPLATE_ID => {
            recording_position(header);
        }
        deepmsg_codec::archive::stop_position_request_codec::SBE_TEMPLATE_ID => {
            stop_position(header);
        }
        _ => {}
    }
});
