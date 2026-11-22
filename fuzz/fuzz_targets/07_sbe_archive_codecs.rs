#![no_main]

//! Target 07: the archive's SBE control requests.
//!
//! The bytes here are what a client writes into its control request; the
//! archive conductor decodes them with the generated flyweights. The generated
//! decoders are **indexed reads that panic past the end** — the reference's
//! Java throws where this build panics — so the server's own bounds check is
//! part of the surface.
//!
//! The check is `control_adapter::control_request_bounds`, the function
//! production calls; this target does not keep a second copy of the table.
//! [`deepmsg_fuzz::archive_control`] holds the decoder walk, one per template
//! `dispatch` answers, in the order `dispatch` makes the calls.

use deepmsg_archive::server::control_adapter::control_request_bounds;
use deepmsg_codec::archive::ReadBuf;
use deepmsg_codec::archive::message_header_codec::{ENCODED_LENGTH, MessageHeaderDecoder};
use deepmsg_fuzz::archive_control::decode_one;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if data.len() < ENCODED_LENGTH {
        return;
    }

    // Too short for a header, the wrong schema, or short of the template's
    // fixed block / variable-length fields: production answers the client an
    // error and never builds a decoder. Anything the guard accepts must be
    // safe to decode.
    if control_request_bounds(data).is_err() {
        return;
    }

    let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(data), 0);
    decode_one(header.template_id(), header);
});
