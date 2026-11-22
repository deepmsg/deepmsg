#![no_main]

//! Target 05: the dirty half of the UDP protocol — frames read straight off a
//! socket.
//!
//! Every parser here is handed bytes an unauthenticated sender chose. The
//! contract is `Option`/`Err`, never a panic; where a frame parses, the lengths
//! it claims must fit inside the datagram that carried it.
//!
//! The resolution walk gets the most attention: it is a cursor over a run of
//! variable-length entries, which is where a zero length or a length past the
//! end turns into an infinite loop or a slice panic.

use deepmsg_driver::protocol::{
    DataFrame, ErrorFrame, FrameHeader, NakFrame, ResolutionDatagram, ResolutionFrame,
    RspSetupFrame, RttmFrame, SetupFrame, StatusMessageFrame,
};
use libfuzzer_sys::fuzz_target;

fn walk_resolution(data: &[u8]) {
    let Some(frame) = ResolutionFrame::read(data) else {
        return;
    };

    let mut walk = frame.entries();
    let mut entries = 0_usize;
    for entry in &mut walk {
        // Every byte of an entry is inside the datagram, and an entry with a
        // zero length would leave the walk standing still.
        assert!(
            entry.entry_length() >= 8,
            "an entry shorter than its header"
        );
        assert!(entry.entry_length() <= data.len());
        assert!(entry.address_bytes() == 4 || entry.address_bytes() == 16);
        entries += 1;
        assert!(
            entries <= data.len(),
            "the walk saw more entries than bytes"
        );
    }
    // The flag means "this walk stopped on an entry it could not read"; it is
    // a fact about the input, not a failure of the parser.
    let _ = walk.malformed();
}

fuzz_target!(|data: &[u8]| {
    let _ = FrameHeader::read(data);
    let _ = DataFrame::read(data);
    let _ = DataFrame::read_in_a_term(data);
    let _ = SetupFrame::read(data);
    let _ = RspSetupFrame::read(data);
    let _ = NakFrame::read(data);
    let _ = StatusMessageFrame::read(data);
    let _ = ErrorFrame::read(data);
    let _ = RttmFrame::read(data);
    let _ = ResolutionDatagram::read(data);
    walk_resolution(data);
});
