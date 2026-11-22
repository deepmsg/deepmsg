//! The target and the adapter answer the same templates.
//!
//! `07_sbe_archive_codecs` decodes what `control_adapter::dispatch` decodes;
//! if the adapter grows an arm and the target does not, this test fails before
//! the fuzzer ever runs.

use deepmsg_archive::server::control_adapter::CONTROL_REQUEST_TEMPLATES;
use deepmsg_fuzz::archive_control::DECODED_TEMPLATES;

#[test]
fn the_fuzz_target_decodes_exactly_what_dispatch_does() {
    assert_eq!(CONTROL_REQUEST_TEMPLATES, DECODED_TEMPLATES);
}
