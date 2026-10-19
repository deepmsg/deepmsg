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

use deepmsg_client::client::Client;
use deepmsg_client::fragment_assembler::{Action, ControlledHandler, FragmentAssembler, Message};
use deepmsg_codec_cluster::client_session_codec::ClientSessionDecoder;
use deepmsg_codec_cluster::message_header_codec::MessageHeaderDecoder;
use deepmsg_codec_cluster::snapshot_marker_codec::SnapshotMarkerDecoder;
use deepmsg_codec_cluster::{
    ReadBuf, SBE_SCHEMA_ID, WriteBuf,
    client_session_codec::{ClientSessionEncoder, SBE_BLOCK_LENGTH as CLIENT_SESSION_BLOCK_LENGTH},
    message_header_codec::ENCODED_LENGTH as MESSAGE_HEADER_LENGTH,
    snapshot_mark::SnapshotMark,
    snapshot_marker_codec::{
        SBE_BLOCK_LENGTH as SNAPSHOT_MARKER_BLOCK_LENGTH, SnapshotMarkerEncoder,
    },
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
    ///
    /// # Errors
    ///
    /// Whatever the idle path reports — the container's idle runs the client's
    /// clock tick, and a terminated client ends the snapshot.
    fn idle(&mut self) -> Result<(), ContainerError>;
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
                SnapshotAppend::Retry => self.sink.idle()?,
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
                SnapshotAppend::Retry => self.sink.idle()?,
                SnapshotAppend::Fatal(reason) => {
                    return Err(ContainerError::Cluster(format!(
                        "failed to offer snapshot buffer: {reason}"
                    )));
                }
            }
        }
    }
}

/// One `ClientSession` record, as the loader decoded it
/// (`ServiceSnapshotLoader.java:118-136`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SnapshotSession<'a> {
    /// `clusterSessionId`.
    pub cluster_session_id: i64,
    /// `responseStreamId`.
    pub response_stream_id: i32,
    /// `responseChannel`.
    pub response_channel: &'a str,
    /// `encodedPrincipal`.
    pub encoded_principal: &'a [u8],
}

/// What the loader does with each record (`ClusteredServiceAgent.onLoad*`).
pub trait SnapshotLoadHandler {
    /// `SnapshotMarker(BEGIN)`: the snapshot's app version and time unit.
    ///
    /// # Errors
    ///
    /// Any error the caller wants to end the load with.
    fn on_load_begin_snapshot(
        &mut self,
        app_version: i32,
        time_unit: TimeUnit,
    ) -> Result<(), ContainerError>;

    /// One `ClientSession` record.
    ///
    /// # Errors
    ///
    /// Any error the caller wants to end the load with.
    fn on_load_cluster_session(
        &mut self,
        session: SnapshotSession<'_>,
    ) -> Result<(), ContainerError>;

    /// `SnapshotMarker(END)`. The business's records follow this one; they are
    /// not the loader's to read.
    ///
    /// # Errors
    ///
    /// Any error the caller wants to end the load with.
    fn on_load_end_snapshot(&mut self) -> Result<(), ContainerError>;
}

/// `ServiceSnapshotLoader` (`ServiceSnapshotLoader.java:29-140`).
///
/// Two templates only: `SnapshotMarker(100)` and `ClientSession(102)`.
/// Anything else is ignored, as the reference's dispatch is — a snapshot is
/// allowed to carry records this build does not know, but not to be a
/// different snapshot.
pub struct ServiceSnapshotLoader {
    assembler: FragmentAssembler,
    in_snapshot: bool,
    done: bool,
    app_version: i32,
    time_unit: TimeUnit,
}

impl Default for ServiceSnapshotLoader {
    fn default() -> Self {
        Self::new()
    }
}

impl ServiceSnapshotLoader {
    /// A loader at the start of a snapshot.
    #[must_use]
    pub fn new() -> Self {
        Self {
            assembler: FragmentAssembler::new(),
            in_snapshot: false,
            done: false,
            app_version: 0,
            time_unit: TimeUnit::Milliseconds,
        }
    }

    /// Whether the END marker has been read.
    #[must_use]
    pub const fn is_done(&self) -> bool {
        self.done
    }

    /// The app version the snapshot's BEGIN carried.
    #[must_use]
    pub const fn app_version(&self) -> i32 {
        self.app_version
    }

    /// The time unit the snapshot's BEGIN carried.
    #[must_use]
    pub const fn time_unit(&self) -> TimeUnit {
        self.time_unit
    }

    /// Poll the replay image once (`ServiceSnapshotLoader.poll`,
    /// `ServiceSnapshotLoader.java:67-70`).
    ///
    /// # Errors
    ///
    /// [`ContainerError::Cluster`] for a malformed record, a schema other than
    /// this one, a snapshot type other than the container's, or a BEGIN/END
    /// mismatch.
    pub fn poll(
        &mut self,
        client: &mut Client,
        subscription_id: i64,
        image_registration_id: i64,
        handler: &mut dyn SnapshotLoadHandler,
    ) -> Result<Option<usize>, ContainerError> {
        let mut assembler = core::mem::replace(&mut self.assembler, FragmentAssembler::new());
        let mut dispatch = Dispatch {
            handler,
            in_snapshot: self.in_snapshot,
            done: self.done,
            app_version: self.app_version,
            time_unit: self.time_unit,
            error: None,
        };

        let result = client.poll_image(
            subscription_id,
            image_registration_id,
            FRAGMENT_LIMIT,
            |fragment| {
                let _ = assembler.push_controlled(fragment, &mut dispatch);
            },
        );

        self.assembler = assembler;
        self.in_snapshot = dispatch.in_snapshot;
        self.done = dispatch.done;
        self.app_version = dispatch.app_version;
        self.time_unit = dispatch.time_unit;
        if let Some(error) = dispatch.error {
            return Err(error);
        }
        Ok(result)
    }
}

/// The loader as the assembler's listener.
struct Dispatch<'a> {
    handler: &'a mut dyn SnapshotLoadHandler,
    in_snapshot: bool,
    done: bool,
    app_version: i32,
    time_unit: TimeUnit,
    error: Option<ContainerError>,
}

impl ControlledHandler for Dispatch<'_> {
    fn on_message(&mut self, message: Message<'_>) -> Action {
        match self.dispatch(message) {
            Ok(action) => action,
            Err(error) => {
                self.error = Some(error);
                Action::Abort
            }
        }
    }
}

impl Dispatch<'_> {
    fn dispatch(&mut self, message: Message<'_>) -> Result<Action, ContainerError> {
        let payload = message.payload;
        if payload.len() < MESSAGE_HEADER_LENGTH {
            return Err(ContainerError::Cluster(format!(
                "snapshot record of {} bytes is shorter than the message header",
                payload.len()
            )));
        }

        let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(payload), 0);
        let schema_id = header.schema_id();
        if schema_id != SBE_SCHEMA_ID {
            return Err(ContainerError::Cluster(format!(
                "expected schemaId={SBE_SCHEMA_ID}, actual={schema_id}"
            )));
        }

        match header.template_id() {
            100 => {
                let marker = SnapshotMarkerDecoder::default().header(header, 0);
                if marker.type_id() != SNAPSHOT_TYPE_ID {
                    return Err(ContainerError::Cluster(format!(
                        "unexpected snapshot type: {}",
                        marker.type_id()
                    )));
                }

                match marker.mark() {
                    SnapshotMark::BEGIN => {
                        if self.in_snapshot {
                            return Err(ContainerError::Cluster("already in snapshot".to_owned()));
                        }
                        self.in_snapshot = true;
                        self.app_version = marker.app_version().unwrap_or(0);
                        self.time_unit =
                            TimeUnit::from_cluster_time_unit(marker.time_unit());
                        self.handler
                            .on_load_begin_snapshot(self.app_version, self.time_unit)?;
                        Ok(Action::Continue)
                    }
                    SnapshotMark::END => {
                        if !self.in_snapshot {
                            return Err(ContainerError::Cluster(
                                "missing begin snapshot".to_owned(),
                            ));
                        }
                        self.done = true;
                        self.handler.on_load_end_snapshot()?;
                        Ok(Action::Break)
                    }
                    SnapshotMark::SECTION | SnapshotMark::NullVal => Ok(Action::Continue),
                }
            }
            102 => {
                let mut record = ClientSessionDecoder::default().header(header, 0);
                let channel_coordinates = record.response_channel_decoder();
                let principal_coordinates = record.encoded_principal_decoder();
                let response_channel =
                    core::str::from_utf8(record.response_channel_slice(channel_coordinates))
                        .map_err(|error| {
                            ContainerError::Cluster(format!(
                                "the snapshot's response channel is not UTF-8: {error}"
                            ))
                        })?;
                let encoded_principal = record.encoded_principal_slice(principal_coordinates);

                self.handler.on_load_cluster_session(SnapshotSession {
                    cluster_session_id: record.cluster_session_id(),
                    response_stream_id: record.response_stream_id(),
                    response_channel,
                    encoded_principal,
                })?;
                Ok(Action::Continue)
            }
            _ => Ok(Action::Continue),
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

        fn idle(&mut self) -> Result<(), ContainerError> {
            Ok(())
        }
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

    // -- the loader --------------------------------------------------------

    #[derive(Default)]
    struct RecordingHandler {
        begins: Vec<(i32, TimeUnit)>,
        sessions: Vec<(i64, i32, String, Vec<u8>)>,
        ends: usize,
    }

    impl SnapshotLoadHandler for RecordingHandler {
        fn on_load_begin_snapshot(
            &mut self,
            app_version: i32,
            time_unit: TimeUnit,
        ) -> Result<(), ContainerError> {
            self.begins.push((app_version, time_unit));
            Ok(())
        }

        fn on_load_cluster_session(
            &mut self,
            session: SnapshotSession<'_>,
        ) -> Result<(), ContainerError> {
            self.sessions.push((
                session.cluster_session_id,
                session.response_stream_id,
                session.response_channel.to_owned(),
                session.encoded_principal.to_vec(),
            ));
            Ok(())
        }

        fn on_load_end_snapshot(&mut self) -> Result<(), ContainerError> {
            self.ends += 1;
            Ok(())
        }
    }

    fn message(bytes: &[u8]) -> Message<'_> {
        Message {
            header: deepmsg_client::fragment_assembler::MessageHeader {
                session_id: 0,
                stream_id: 0,
                term_offset: 0,
                flags: 0,
                position: 0,
                frame_length: 0,
                fragmented_frame_length: 0,
            },
            payload: bytes,
        }
    }

    /// The three records a container snapshot of one session is made of.
    fn records() -> Vec<Vec<u8>> {
        let mut sink = RecordingSink::new(1024);
        {
            let mut taker = ServiceSnapshotTaker::new(&mut sink, TimeUnit::Milliseconds, 7);
            taker.mark_begin(1_024, 5).expect("BEGIN");
            taker
                .snapshot_session(&session("aeron:udp?endpoint=localhost:0", &[1, 2, 3]))
                .expect("session");
            taker.mark_end(1_024, 5).expect("END");
        }
        core::mem::take(&mut *sink.claims.borrow_mut())
    }

    fn drive(
        loader: &mut ServiceSnapshotLoader,
        bytes: &[u8],
        handler: &mut dyn SnapshotLoadHandler,
    ) -> Result<Action, ContainerError> {
        let mut dispatch = Dispatch {
            handler,
            in_snapshot: loader.in_snapshot,
            done: loader.done,
            app_version: loader.app_version,
            time_unit: loader.time_unit,
            error: None,
        };
        let action = dispatch.dispatch(message(bytes));
        loader.in_snapshot = dispatch.in_snapshot;
        loader.done = dispatch.done;
        loader.app_version = dispatch.app_version;
        loader.time_unit = dispatch.time_unit;
        action
    }

    #[test]
    fn the_loader_reads_begin_sessions_and_end() {
        let records = records();
        let mut handler = RecordingHandler::default();
        let mut loader = ServiceSnapshotLoader::new();

        assert_eq!(
            drive(&mut loader, &records[0], &mut handler).expect("BEGIN"),
            Action::Continue
        );
        assert_eq!(
            drive(&mut loader, &records[1], &mut handler).expect("session"),
            Action::Continue
        );
        assert!(!loader.is_done());
        assert_eq!(
            drive(&mut loader, &records[2], &mut handler).expect("END"),
            Action::Break
        );

        assert!(loader.is_done());
        assert_eq!(loader.app_version(), 7);
        assert_eq!(loader.time_unit(), TimeUnit::Milliseconds);
        assert_eq!(handler.begins, vec![(7, TimeUnit::Milliseconds)]);
        assert_eq!(handler.ends, 1);
        assert_eq!(handler.sessions.len(), 1);
        assert_eq!(handler.sessions[0].0, 1);
        assert_eq!(handler.sessions[0].1, 42);
        assert_eq!(handler.sessions[0].2, "aeron:udp?endpoint=localhost:0");
        assert_eq!(handler.sessions[0].3, vec![1, 2, 3]);
    }

    #[test]
    fn an_end_without_begin_is_refused() {
        let records = records();
        let mut handler = RecordingHandler::default();
        let mut loader = ServiceSnapshotLoader::new();

        let error = drive(&mut loader, &records[2], &mut handler).expect_err("no BEGIN");
        assert!(error.to_string().contains("missing begin snapshot"));
    }

    #[test]
    fn a_second_begin_is_refused() {
        let records = records();
        let mut handler = RecordingHandler::default();
        let mut loader = ServiceSnapshotLoader::new();

        drive(&mut loader, &records[0], &mut handler).expect("BEGIN");
        let error = drive(&mut loader, &records[0], &mut handler).expect_err("second BEGIN");
        assert!(error.to_string().contains("already in snapshot"));
    }

    #[test]
    fn a_snapshot_of_another_type_is_refused() {
        let mut records = records();
        // `typeId` is the marker block's first field, right after the header.
        records[0][MESSAGE_HEADER_LENGTH..MESSAGE_HEADER_LENGTH + 8]
            .copy_from_slice(&1_i64.to_le_bytes());

        let mut handler = RecordingHandler::default();
        let mut loader = ServiceSnapshotLoader::new();

        let error = drive(&mut loader, &records[0], &mut handler).expect_err("type 1");
        assert!(error.to_string().contains("unexpected snapshot type: 1"));
    }
}
