//! The two messages a backup sends to a cluster (`ConsensusPublisher.java`):
//! its query and its answer to a challenge.
//!
//! The reference keeps these on the same `ConsensusPublisher` that holds a
//! member's per-member publications, because a member also sends
//! `ChallengeResponse`s — a follower forwards a client's answer to the leader.
//! A backup's publication is not per member, it is one exclusive publication
//! that moves (`super::publication_group`), so the two encoders live here,
//! taking the publication as an argument instead of finding one.

use deepmsg_client::publication::ExclusivePublication;
use deepmsg_codec_cluster::WriteBuf;
use deepmsg_codec_cluster::backup_query_codec::BackupQueryEncoder;
use deepmsg_codec_cluster::challenge_response_codec::ChallengeResponseEncoder;
use deepmsg_codec_cluster::message_header_codec::ENCODED_LENGTH as MESSAGE_HEADER_LENGTH;

/// `ConsensusPublisher.backupQuery` (`ConsensusPublisher.java:452-480`).
///
/// The log position is the reference's `Long.MAX_VALUE` — "the latest you
/// have" — which the cluster turns into its own last commit position.
///
/// Answers whether the publication took it.
#[allow(clippy::too_many_arguments)] // one per field the reference's call takes
pub fn backup_query(
    publication: &ExclusivePublication,
    position_limit: i64,
    correlation_id: i64,
    response_stream_id: i32,
    version: i32,
    response_channel: &str,
    encoded_credentials: &[u8],
) -> bool {
    // Header, the fixed block (`SBE_BLOCK_LENGTH` = 24), two variable-length
    // words and their bytes. Not a hot path: a backup asks once per interval.
    let mut buffer =
        vec![
            0_u8;
            MESSAGE_HEADER_LENGTH + 24 + 4 + response_channel.len() + 4 + encoded_credentials.len()
        ];

    let encoder =
        BackupQueryEncoder::default().wrap(WriteBuf::new(&mut buffer), MESSAGE_HEADER_LENGTH);
    let mut header = encoder.header(0);
    let mut message = header.parent().expect("the encoder just wrapped");
    message
        .correlation_id(correlation_id)
        .response_stream_id(response_stream_id)
        .version(version)
        .log_position(i64::MAX)
        .response_channel(response_channel.as_bytes())
        .encoded_credentials(encoded_credentials);
    let length = MESSAGE_HEADER_LENGTH + message.encoded_length();

    offer(publication, position_limit, &buffer[..length])
}

/// `ConsensusPublisher.challengeResponse` (`:558-573`): a backup's answer to the
/// cluster's challenge.
pub fn challenge_response(
    publication: &ExclusivePublication,
    position_limit: i64,
    correlation_id: i64,
    cluster_session_id: i64,
    encoded_credentials: &[u8],
) -> bool {
    // Header, the fixed block (`SBE_BLOCK_LENGTH` = 16), the credentials' length
    // word and their bytes.
    let mut buffer = vec![0_u8; MESSAGE_HEADER_LENGTH + 16 + 4 + encoded_credentials.len()];

    let encoder =
        ChallengeResponseEncoder::default().wrap(WriteBuf::new(&mut buffer), MESSAGE_HEADER_LENGTH);
    let mut header = encoder.header(0);
    let mut message = header.parent().expect("the encoder just wrapped");
    message
        .correlation_id(correlation_id)
        .cluster_session_id(cluster_session_id)
        .encoded_credentials(encoded_credentials);
    let length = MESSAGE_HEADER_LENGTH + message.encoded_length();

    offer(publication, position_limit, &buffer[..length])
}

/// `sendPublication` (`:575-600`): offer, retrying the outcomes that mean
/// "not this turn".
fn offer(publication: &ExclusivePublication, position_limit: i64, bytes: &[u8]) -> bool {
    matches!(
        publication.offer(position_limit, bytes),
        deepmsg_core::logbuffer::append::Appended::Ok { .. }
    )
}
