//! The guards in front of the cluster SBE decoders.
//!
//! The generated decoders **panic** on a short buffer, and an egress
//! subscription carries messages from the network's other side — so "short
//! buffer" is ordinary hostile input, not a bug in the caller. Every decode in
//! this crate goes through one of these first, and the panic path is
//! unreachable.
//!
//! [`deepmsg_cluster_common::decode`] is the same rule one crate lower; these
//! are the two shapes the client needs on top of it — "is there a header" and
//! "is there room for the fixed block, plus the four-byte prefix a variable
//! field starts with" — plus the bound on the variable field's own length.
//! They live here rather than in the egress poller because the poller and the
//! adapter decode the same four messages.

use deepmsg_codec::cluster::ReadBuf;
use deepmsg_codec::cluster::message_header_codec::{self, MessageHeaderDecoder};

/// The SBE header of a message, or `None` when the payload cannot hold one.
pub(crate) fn sbe_header(payload: &[u8]) -> Option<MessageHeaderDecoder<ReadBuf<'_>>> {
    if payload.len() < message_header_codec::ENCODED_LENGTH {
        return None;
    }

    Some(MessageHeaderDecoder::default().wrap(ReadBuf::new(payload), 0))
}

/// Whether a payload is long enough for a template's fixed part.
pub(crate) fn holds(payload: &[u8], block_length: u16) -> bool {
    payload.len() >= message_header_codec::ENCODED_LENGTH + usize::from(block_length)
}

/// The same, plus the four-byte length prefix a variable field starts with.
pub(crate) fn holds_var(payload: &[u8], block_length: u16) -> bool {
    payload.len() >= message_header_codec::ENCODED_LENGTH + usize::from(block_length) + 4
}

/// A variable field's bytes, when the coordinates are inside the payload.
///
/// The generated `*_slice` would panic on a length that runs past the end; a
/// message that says so is not one these readers read.
pub(crate) fn var_slice(payload: &[u8], (offset, length): (usize, usize)) -> Option<&[u8]> {
    offset
        .checked_add(length)
        .filter(|end| *end <= payload.len())
        .map(|_| &payload[offset..offset + length])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_guards_refuse_what_the_decoders_would_panic_on() {
        assert!(sbe_header(&[0u8; 4]).is_none());
        assert!(sbe_header(&[0u8; message_header_codec::ENCODED_LENGTH]).is_some());

        assert!(!holds(&[0u8; 8], 24));
        assert!(holds(&[0u8; 32], 24));

        assert!(!holds_var(&[0u8; 32], 24));
        assert!(holds_var(&[0u8; 36], 24));

        assert_eq!(Some(&b"ab"[..]), var_slice(b"xxab", (2, 2)));
        assert_eq!(None, var_slice(b"xxab", (2, 3)));
        assert_eq!(None, var_slice(b"xxab", (usize::MAX, 1)));
    }
}
