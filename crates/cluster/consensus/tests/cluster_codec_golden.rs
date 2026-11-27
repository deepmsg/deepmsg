//! The two schema-111 messages whose v18 fields must match the reference byte
//! for byte.
//!
//! The fixtures come from the reference's own Java encoders (see the directory
//! README), so a mismatch here is this build's generated-code wiring rather
//! than an opinion about what the layout should be.

use deepmsg_codec_cluster::cluster_action::ClusterAction;
use deepmsg_codec_cluster::cluster_action_request_codec::{
    ClusterActionRequestDecoder, ClusterActionRequestEncoder,
};
use deepmsg_codec_cluster::cluster_time_unit::ClusterTimeUnit;
use deepmsg_codec_cluster::message_header_codec::{ENCODED_LENGTH, MessageHeaderDecoder};
use deepmsg_codec_cluster::snapshot_mark::SnapshotMark;
use deepmsg_codec_cluster::snapshot_marker_codec::{SnapshotMarkerDecoder, SnapshotMarkerEncoder};
use deepmsg_codec_cluster::{ReadBuf, WriteBuf};

const ACTION: &[u8] = include_bytes!("fixtures/cluster-codecs/001-023-cluster-action-request.bin");
const MARKER: &[u8] = include_bytes!("fixtures/cluster-codecs/003-100-snapshot-marker.bin");

#[test]
fn the_action_bytes_and_readings_are_the_references() {
    let mut buffer = vec![0_u8; ACTION.len()];
    {
        let encoder =
            ClusterActionRequestEncoder::default().wrap(WriteBuf::new(&mut buffer), ENCODED_LENGTH);
        let mut header = encoder.header(0);
        let mut body = header.parent().expect("just wrapped");
        body.leadership_term_id(5)
            .log_position(1_024)
            .timestamp(7)
            .action(ClusterAction::SNAPSHOT)
            .flags(0)
            .term_base_log_position(4_096);
    }
    assert_eq!(ACTION, buffer.as_slice());

    let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(ACTION), 0);
    assert_eq!(111, header.schema_id());
    assert_eq!(23, header.template_id());
    assert_eq!(18, header.version());
    assert_eq!(40, header.block_length());

    let decoder = ClusterActionRequestDecoder::default().header(header, 0);
    assert_eq!(5, decoder.leadership_term_id());
    assert_eq!(1_024, decoder.log_position());
    assert_eq!(7, decoder.timestamp());
    assert_eq!(ClusterAction::SNAPSHOT, decoder.action());
    assert_eq!(0, decoder.flags().unwrap_or(0));
    assert_eq!(Some(4_096), decoder.term_base_log_position());
}

#[test]
fn the_marker_bytes_and_readings_are_the_references() {
    let mut buffer = vec![0_u8; MARKER.len()];
    {
        let encoder =
            SnapshotMarkerEncoder::default().wrap(WriteBuf::new(&mut buffer), ENCODED_LENGTH);
        let mut header = encoder.header(0);
        let mut body = header.parent().expect("just wrapped");
        body.type_id(1)
            .log_position(1_024)
            .leadership_term_id(5)
            .index(0)
            .mark(SnapshotMark::BEGIN)
            .time_unit(ClusterTimeUnit::MILLIS)
            .app_version(7)
            .term_base_log_position(4_096);
    }
    assert_eq!(MARKER, buffer.as_slice());

    let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(MARKER), 0);
    assert_eq!(111, header.schema_id());
    assert_eq!(100, header.template_id());
    assert_eq!(18, header.version());
    assert_eq!(48, header.block_length());

    let decoder = SnapshotMarkerDecoder::default().header(header, 0);
    assert_eq!(1, decoder.type_id());
    assert_eq!(1_024, decoder.log_position());
    assert_eq!(5, decoder.leadership_term_id());
    assert_eq!(0, decoder.index());
    assert_eq!(SnapshotMark::BEGIN, decoder.mark());
    assert_eq!(ClusterTimeUnit::MILLIS, decoder.time_unit());
    assert_eq!(Some(7), decoder.app_version());
    assert_eq!(Some(4_096), decoder.term_base_log_position());
}

#[test]
fn a_v11_action_from_1_53_2_has_no_term_base() {
    let mut buffer = vec![0_u8; 48];
    {
        let encoder =
            ClusterActionRequestEncoder::default().wrap(WriteBuf::new(&mut buffer), ENCODED_LENGTH);
        let mut header = encoder.header(0);
        let mut body = header.parent().expect("just wrapped");
        body.leadership_term_id(5)
            .log_position(1_024)
            .timestamp(7)
            .action(ClusterAction::SNAPSHOT)
            .flags(0)
            .term_base_log_position(4_096);
    }

    // The v11 shape: block length 32, acting version 11, no term-base field.
    buffer[0..2].copy_from_slice(&32_u16.to_le_bytes());
    buffer[6..8].copy_from_slice(&11_u16.to_le_bytes());
    let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(&buffer[..40]), 0);
    let decoder = ClusterActionRequestDecoder::default().header(header, 0);

    assert_eq!(5, decoder.leadership_term_id());
    assert_eq!(1_024, decoder.log_position());
    assert_eq!(None, decoder.term_base_log_position());
}
