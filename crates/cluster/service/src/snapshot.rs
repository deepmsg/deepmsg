//! Taking a service container's snapshot (`ServiceSnapshotTaker`).
//!
//! The record order is the contract (`ClusteredServiceAgent.java:1041-1054`):
//!
//! 1. `SnapshotMarker` BEGIN (100, mark 0, typeId 2)
//! 2. `ClientSession` (102) — one per session, in the session table's order,
//!    which is ascending id (`SessionTable`, fixed at P3-2a for this)
//! 3. `SnapshotMarker` END (100, mark 2, typeId 2)
//! 4. **the business's own records, after END** — `service.on_take_snapshot` is
//!    called once `snapshot_state` has returned
//!
//! `typeId` is **2**, the container's (`ClusteredServiceContainer.java:195`),
//! not 1, the consensus module's: the two share `SnapshotMarker(100)`, and the
//! type id is all that tells their snapshots apart.
//!
//! # The iteration container
//!
//! Sessions come from the container's `Vec` in id order (`CSA:628-652`), and
//! only the live table is written — a session that has been removed is not in
//! it. There is no per-session filter here beyond that, because the container
//! holds no closed sessions: the reference's `ClientSession` list is the same
//! live table.
//!
//! # Why a sink and not a publication
//!
//! A retry must idle **and poll the client**; the publication cannot do the
//! latter. The same split as the consensus module's snapshot taker
//! (`ConsensusModuleSnapshotTaker`), so the retry loop and its answers are the
//! same in both places.

use deepmsg_codec_cluster::WriteBuf;
use deepmsg_codec_cluster::client_session_codec::{
    ClientSessionEncoder, SBE_BLOCK_LENGTH as CLIENT_SESSION_BLOCK_LENGTH,
};
use deepmsg_codec_cluster::message_header_codec::ENCODED_LENGTH as MESSAGE_HEADER_LENGTH;
use deepmsg_codec_cluster::snapshot_mark::SnapshotMark;
use deepmsg_codec_cluster::snapshot_marker_codec::{
    SBE_BLOCK_LENGTH as SNAPSHOT_MARKER_BLOCK_LENGTH, SnapshotMarkerEncoder,
};

use crate::error::ContainerError;
use crate::role::TimeUnit;
use crate::session::ContainerClientSession;

/// `ClusteredServiceContainer.Configuration.SNAPSHOT_TYPE_ID` (`:195`).
pub const SNAPSHOT_TYPE_ID: i64 = 2;

/// `ServiceSnapshotLoader.FRAGMENT_LIMIT` (`:31`).
pub const FRAGMENT_LIMIT: usize = 10;

/// What a snapshot write got (`Publication.tryClaim` / `offer`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SnapshotAppend {
    /// Written.
    Ok,
    /// Back pressure, an end of log or a rotation: idle and try again.
    Retry,
    /// A fatal answer: the reference throws from `SnapshotTaker.checkResult`.
    Fatal(String),
}

/// What the taker needs from the publication.
pub trait SnapshotSend {
    /// Write a claimed record of `length` bytes.
    fn try_claim(&self, length: usize, bytes: &[u8]) -> SnapshotAppend;

    /// Offer an encoded record (the oversized-session path).
    fn offer(&self, bytes: &[u8]) -> SnapshotAppend;

    /// `Publication.maxPayloadLength()`.
    fn max_payload_length(&self) -> usize;
}

/// The idle between attempts (`SnapshotTaker`'s strategy + agent invoker).
pub trait SnapshotIdle {
    /// `IdleStrategy.reset()`.
    fn reset(&mut self);

    /// `IdleStrategy.idle()` and the agent invoker's poll.
    fn idle(&mut self);
}

/// A publication and the idle strategy together.
pub trait SnapshotSink: SnapshotSend + SnapshotIdle {}

impl<T: SnapshotSend + SnapshotIdle> SnapshotSink for T {}

/// `ServiceSnapshotTaker` (`ServiceSnapshotTaker.java:28-90`) plus the
/// `SnapshotTaker` markers it writes.
pub struct ServiceSnapshotTaker<'a> {
    sink: &'a mut dyn SnapshotSink,
    buffer: Vec<u8>,
    time_unit: TimeUnit,
    app_version: i32,
}

impl<'a> ServiceSnapshotTaker<'a> {
    /// A taker over a publication, for one snapshot.
    pub fn new(sink: &'a mut dyn SnapshotSink, time_unit: TimeUnit, app_version: i32) -> Self {
        Self {
            sink,
            buffer: vec![0_u8; 1024],
            time_unit,
            app_version,
        }
    }

    /// `SnapshotTaker.markBegin` (`SnapshotTaker.java:108-125`).
    ///
    /// # Errors
    ///
    /// [`ContainerError::Cluster`] when the record is refused.
    pub fn mark_begin(
        &mut self,
        log_position: i64,
        leadership_term_id: i64,
    ) -> Result<(), ContainerError> {
        self.mark(SnapshotMark::BEGIN, log_position, leadership_term_id)
    }

    /// `SnapshotTaker.markEnd` (`:127-144`).
    ///
    /// # Errors
    ///
    /// As [`Self::mark_begin`].
    pub fn mark_end(
        &mut self,
        log_position: i64,
        leadership_term_id: i64,
    ) -> Result<(), ContainerError> {
        self.mark(SnapshotMark::END, log_position, leadership_term_id)
    }

    /// `snapshotSession` (`ServiceSnapshotTaker.java:37-72`): claim when the
    /// record fits the maximum payload, encode into the reusable buffer and
    /// offer when it does not.
    ///
    /// # Errors
    ///
    /// As [`Self::mark_begin`].
    pub fn snapshot_session(
        &mut self,
        session: &ContainerClientSession,
    ) -> Result<(), ContainerError> {
        let response_channel = session.response_channel();
        let encoded_principal = session.encoded_principal();
        let length = MESSAGE_HEADER_LENGTH
            + usize::from(CLIENT_SESSION_BLOCK_LENGTH)
            + 4
            + response_channel.len()
            + 4
            + encoded_principal.len();
        self.size_buffer(length);

        {
            let encoder = ClientSessionEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer[..length]),
                MESSAGE_HEADER_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut message = header.parent().expect("the encoder just wrapped");
            message
                .cluster_session_id(session.id())
                .response_stream_id(session.response_stream_id())
                .response_channel(response_channel.as_bytes())
                .encoded_principal(encoded_principal);
        }

        if length <= self.sink.max_payload_length() {
            self.claim(length)
        } else {
            self.offer(length)
        }
    }

    fn mark(
        &mut self,
        mark: SnapshotMark,
        log_position: i64,
        leadership_term_id: i64,
    ) -> Result<(), ContainerError> {
        let length = MESSAGE_HEADER_LENGTH + usize::from(SNAPSHOT_MARKER_BLOCK_LENGTH);
        self.size_buffer(length);

        {
            let encoder = SnapshotMarkerEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer[..length]),
                MESSAGE_HEADER_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut message = header.parent().expect("the encoder just wrapped");
            message
                .type_id(SNAPSHOT_TYPE_ID)
                .log_position(log_position)
                .leadership_term_id(leadership_term_id)
                .index(0)
                .mark(mark)
                .time_unit(self.time_unit.to_cluster_time_unit())
                .app_version(self.app_version);
        }

        self.claim(length)
    }

    fn size_buffer(&mut self, length: usize) {
        if self.buffer.len() < length {
            self.buffer.resize(length, 0);
        }
    }

    fn claim(&mut self, length: usize) -> Result<(), ContainerError> {
        self.sink.reset();
        loop {
            match self.sink.try_claim(length, &self.buffer[..length]) {
                SnapshotAppend::Ok => return Ok(()),
                SnapshotAppend::Retry => self.sink.idle(),
                SnapshotAppend::Fatal(reason) => {
                    return Err(ContainerError::Cluster(format!(
                        "failed to claim snapshot buffer: {reason}"
                    )));
                }
            }
        }
    }

    fn offer(&mut self, length: usize) -> Result<(), ContainerError> {
        self.sink.reset();
        loop {
            match self.sink.offer(&self.buffer[..length]) {
                SnapshotAppend::Ok => return Ok(()),
                SnapshotAppend::Retry => self.sink.idle(),
                SnapshotAppend::Fatal(reason) => {
                    return Err(ContainerError::Cluster(format!(
                        "failed to offer snapshot buffer: {reason}"
                    )));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `ServiceSnapshotTakerTest`'s mock publication: records which path was
    /// taken and what it was asked to write. The recording is behind a
    /// `RefCell` because the write side of the trait takes `&self` — the same
    /// shape the production sink has, where the publication is an immutable
    /// handle.
    struct RecordingSink {
        max_payload_length: usize,
        claims: std::cell::RefCell<Vec<Vec<u8>>>,
        offers: std::cell::RefCell<Vec<Vec<u8>>>,
    }

    impl RecordingSink {
        fn new(max_payload_length: usize) -> Self {
            Self {
                max_payload_length,
                claims: std::cell::RefCell::new(Vec::new()),
                offers: std::cell::RefCell::new(Vec::new()),
            }
        }
    }

    impl SnapshotSend for RecordingSink {
        fn try_claim(&self, _length: usize, bytes: &[u8]) -> SnapshotAppend {
            self.claims.borrow_mut().push(bytes.to_vec());
            SnapshotAppend::Ok
        }

        fn offer(&self, bytes: &[u8]) -> SnapshotAppend {
            self.offers.borrow_mut().push(bytes.to_vec());
            SnapshotAppend::Ok
        }

        fn max_payload_length(&self) -> usize {
            self.max_payload_length
        }
    }

    impl SnapshotIdle for RecordingSink {
        fn reset(&mut self) {}

        fn idle(&mut self) {}
    }

    fn session(channel: &str, principal: &[u8]) -> ContainerClientSession {
        ContainerClientSession::new(1, 42, channel.to_owned(), principal.to_vec())
    }

    #[test]
    fn snapshot_session_uses_try_claim_if_data_fit_into_max_payload_size() {
        let mut sink = RecordingSink::new(1024);
        {
            let mut taker = ServiceSnapshotTaker::new(&mut sink, TimeUnit::Milliseconds, 1);
            taker
                .snapshot_session(&session("aeron:udp?endpoint=localhost:0", &[]))
                .expect("the record fits");
        }

        assert_eq!(sink.claims.borrow().len(), 1);
        assert!(sink.offers.borrow().is_empty());
    }

    #[test]
    fn snapshot_session_uses_offer_if_data_does_not_fit_into_max_payload_size() {
        let mut sink = RecordingSink::new(1);
        {
            let mut taker = ServiceSnapshotTaker::new(&mut sink, TimeUnit::Milliseconds, 1);
            taker
                .snapshot_session(&session("aeron:udp?endpoint=localhost:0", &[]))
                .expect("the record is offered");
        }

        assert_eq!(sink.offers.borrow().len(), 1);
        assert!(sink.claims.borrow().is_empty());
    }

    #[test]
    fn the_markers_carry_the_container_type_id_and_the_record_order() {
        let mut sink = RecordingSink::new(1024);
        let channel = "aeron:udp?endpoint=localhost:0";
        {
            let mut taker = ServiceSnapshotTaker::new(&mut sink, TimeUnit::Milliseconds, 7);
            taker.mark_begin(1_024, 5).expect("BEGIN");
            taker.snapshot_session(&session(channel, &[1, 2, 3])).expect("session");
            taker.mark_end(1_024, 5).expect("END");
        }

        let claims = sink.claims.borrow();
        assert_eq!(claims.len(), 3, "BEGIN, session, END");

        use deepmsg_codec_cluster::ReadBuf;
        use deepmsg_codec_cluster::client_session_codec::ClientSessionDecoder;
        use deepmsg_codec_cluster::message_header_codec::MessageHeaderDecoder;
        use deepmsg_codec_cluster::snapshot_marker_codec::SnapshotMarkerDecoder;

        let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(&claims[0]), 0);
        assert_eq!(header.template_id(), 100);
        let marker = SnapshotMarkerDecoder::default().header(header, 0);
        assert_eq!(marker.type_id(), SNAPSHOT_TYPE_ID);
        assert_eq!(marker.mark(), SnapshotMark::BEGIN);
        assert_eq!(marker.log_position(), 1_024);
        assert_eq!(marker.leadership_term_id(), 5);
        assert_eq!(marker.app_version(), Some(7));

        let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(&claims[1]), 0);
        assert_eq!(header.template_id(), 102);
        let record = ClientSessionDecoder::default().header(header, 0);
        assert_eq!(record.cluster_session_id(), 1);
        assert_eq!(record.response_stream_id(), 42);

        let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(&claims[2]), 0);
        let marker = SnapshotMarkerDecoder::default().header(header, 0);
        assert_eq!(marker.mark(), SnapshotMark::END);
    }
}
