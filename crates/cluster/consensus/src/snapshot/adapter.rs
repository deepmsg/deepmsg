//! Reading a snapshot back (`ConsensusModuleSnapshotAdapter.java:65-202`).
//!
//! The records arrive on a replay image. The adapter assembles whole messages
//! and dispatches by template id:
//!
//! | Id | Record | Handler |
//! |---|---|---|
//! | 1 | a queued service message (verbatim) | `on_load_pending_message` |
//! | 100 | `SnapshotMarker` | begin / end; anything but BEGIN or END is ignored |
//! | 103 | `ClusterSession` | `on_load_cluster_session` |
//! | 104 | `Timer` | `on_load_timer` |
//! | 105 | `ConsensusModule` | `on_load_consensus_module_state` |
//! | 106 | `ClusterMembers` | **ignored** — the reference never writes it (`:179-181`) |
//! | 107 | `PendingMessageTracker` | `on_load_pending_message_tracker` |
//!
//! `BEGIN` opens the snapshot (a second BEGIN is an error), `END` finishes it
//! and the reference stops polling there (`Action.BREAK`). This build keeps
//! reading the batch but ignores anything after `END`; there are no records
//! after it in this slice (extension records are P3-9).

use deepmsg_client::client::Client;
use deepmsg_client::fragment_assembler::{Action, ControlledHandler, FragmentAssembler, Message};
use deepmsg_cluster_common::decode::at_least;
use deepmsg_codec_cluster::cluster_session_codec::ClusterSessionDecoder;
use deepmsg_codec_cluster::cluster_time_unit::ClusterTimeUnit;
use deepmsg_codec_cluster::consensus_module_codec::ConsensusModuleDecoder;
use deepmsg_codec_cluster::message_header_codec::{
    ENCODED_LENGTH as MESSAGE_HEADER_LENGTH, MessageHeaderDecoder,
};
use deepmsg_codec_cluster::pending_message_tracker_codec::PendingMessageTrackerDecoder;
use deepmsg_codec_cluster::session_message_header_codec::SessionMessageHeaderDecoder;
use deepmsg_codec_cluster::snapshot_mark::SnapshotMark;
use deepmsg_codec_cluster::snapshot_marker_codec::SnapshotMarkerDecoder;
use deepmsg_codec_cluster::timer_codec::TimerDecoder;
use deepmsg_codec_cluster::{ReadBuf, SBE_SCHEMA_ID};

use crate::error::ClusterError;
use crate::snapshot::taker::SNAPSHOT_TYPE_ID;

/// `ConsensusModuleSnapshotAdapter.FRAGMENT_LIMIT` (`:31`).
pub const FRAGMENT_LIMIT: usize = 10;

/// One `ClusterSession` record, as decoded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotSession {
    /// The session id.
    pub cluster_session_id: i64,
    /// The client's correlation id.
    pub correlation_id: i64,
    /// The log position its open event was appended at.
    pub opened_log_position: i64,
    /// The activity stamp — always `Aeron.NULL_VALUE` in a reference snapshot.
    pub time_of_last_activity: i64,
    /// The close reason, or `NullVal` for an open session.
    pub close_reason: deepmsg_codec_cluster::close_reason::CloseReason,
    /// The response stream.
    pub response_stream_id: i32,
    /// The response channel.
    pub response_channel: String,
}

/// What the loader does with each record (`ConsensusModuleAgent.onLoad*`).
pub trait SnapshotLoadHandler {
    /// `onLoadBeginSnapshot` (`ConsensusModuleAgent.java:632-647`).
    ///
    /// # Errors
    ///
    /// [`ClusterError`] for an incompatible app version or a time unit this
    /// build does not have.
    fn on_load_begin_snapshot(
        &mut self,
        app_version: i32,
        time_unit: ClusterTimeUnit,
    ) -> Result<(), ClusterError>;

    /// `onLoadEndSnapshot` (`:674-676`).
    ///
    /// # Errors
    ///
    /// Nothing today; the shape matches the caller's `?`.
    fn on_load_end_snapshot(&mut self) -> Result<(), ClusterError>;

    /// `onLoadClusterSession` (`:678-698`).
    ///
    /// # Errors
    ///
    /// [`ClusterError`] from the session manager.
    fn on_load_cluster_session(&mut self, session: SnapshotSession) -> Result<(), ClusterError>;

    /// `onLoadTimer` (`:744-748`).
    ///
    /// # Errors
    ///
    /// Nothing today.
    fn on_load_timer(&mut self, correlation_id: i64, deadline: i64) -> Result<(), ClusterError>;

    /// `onLoadConsensusModuleState` (`:700-716`).
    ///
    /// # Errors
    ///
    /// Nothing today.
    fn on_load_consensus_module_state(
        &mut self,
        next_session_id: i64,
        next_service_session_id: i64,
        log_service_session_id: i64,
        pending_message_capacity: i32,
    ) -> Result<(), ClusterError>;

    /// `onLoadPendingMessageTracker` (`:718-735`).
    ///
    /// # Errors
    ///
    /// [`ClusterError`] for a service id out of range.
    fn on_load_pending_message_tracker(
        &mut self,
        next_service_session_id: i64,
        log_service_session_id: i64,
        pending_message_capacity: i32,
        service_id: i32,
    ) -> Result<(), ClusterError>;

    /// `onLoadPendingMessage` (`:737-742`).
    ///
    /// # Errors
    ///
    /// [`ClusterError`] for a service id out of range or a full ring.
    fn on_load_pending_message(
        &mut self,
        cluster_session_id: i64,
        message: &[u8],
    ) -> Result<(), ClusterError>;
}

/// `ConsensusModuleSnapshotAdapter` (`:65-202`).
pub struct ConsensusModuleSnapshotAdapter {
    assembler: FragmentAssembler,
    in_snapshot: bool,
    done: bool,
}

impl Default for ConsensusModuleSnapshotAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl ConsensusModuleSnapshotAdapter {
    /// A fresh adapter.
    pub fn new() -> Self {
        Self {
            assembler: FragmentAssembler::new(),
            in_snapshot: false,
            done: false,
        }
    }

    /// `isDone()` (`:57`).
    pub const fn is_done(&self) -> bool {
        self.done
    }

    /// `poll()` (`:59-62`).
    ///
    /// # Errors
    ///
    /// The first decode or handler error; `Ok(None)` when the image is gone.
    pub fn poll(
        &mut self,
        client: &mut Client,
        subscription_id: i64,
        image_registration_id: i64,
        handler: &mut dyn SnapshotLoadHandler,
    ) -> Result<Option<usize>, ClusterError> {
        let mut assembler = core::mem::replace(&mut self.assembler, FragmentAssembler::new());
        let mut dispatch = Dispatch {
            handler,
            in_snapshot: self.in_snapshot,
            done: self.done,
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
        if let Some(error) = dispatch.error {
            return Err(error);
        }
        Ok(result)
    }

    /// The body of `onFragment` (`:65-202`), driven by the tests.
    ///
    /// # Errors
    ///
    /// A foreign schema, a malformed marker sequence, or a handler's error.
    pub fn dispatch_message(
        &mut self,
        handler: &mut dyn SnapshotLoadHandler,
        message: &[u8],
    ) -> Result<Action, ClusterError> {
        dispatch_message(handler, &mut self.in_snapshot, &mut self.done, message)
    }
}

/// One decoded record, dispatched (`ConsensusModuleSnapshotAdapter.onFragment`).
fn dispatch_message(
    handler: &mut dyn SnapshotLoadHandler,
    in_snapshot: &mut bool,
    done: &mut bool,
    message: &[u8],
) -> Result<Action, ClusterError> {
    if *done {
        return Ok(Action::Continue);
    }

    at_least(message, MESSAGE_HEADER_LENGTH, "snapshot message header")?;
    let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(message), 0);
    let schema_id = header.schema_id();
    if schema_id != SBE_SCHEMA_ID {
        return Err(ClusterError::Cluster(format!(
            "expected schemaId={SBE_SCHEMA_ID}, actual={schema_id}"
        )));
    }

    match header.template_id() {
        deepmsg_codec_cluster::session_message_header_codec::SBE_TEMPLATE_ID => {
            let decoder = SessionMessageHeaderDecoder::default().header(header, 0);
            handler.on_load_pending_message(decoder.cluster_session_id(), message)?;
            Ok(Action::Continue)
        }
        deepmsg_codec_cluster::snapshot_marker_codec::SBE_TEMPLATE_ID => {
            let decoder = SnapshotMarkerDecoder::default().header(header, 0);
            if decoder.type_id() != SNAPSHOT_TYPE_ID {
                return Err(ClusterError::Cluster(format!(
                    "unexpected snapshot typeId={}",
                    decoder.type_id()
                )));
            }
            match decoder.mark() {
                SnapshotMark::BEGIN => {
                    if *in_snapshot {
                        return Err(ClusterError::Cluster(
                            "snapshot has a nested BEGIN marker".to_owned(),
                        ));
                    }
                    *in_snapshot = true;
                    let app_version = decoder.app_version().unwrap_or(0);
                    let time_unit = decoder.time_unit();
                    handler.on_load_begin_snapshot(app_version, time_unit)?;
                    Ok(Action::Continue)
                }
                SnapshotMark::END => {
                    if !*in_snapshot {
                        return Err(ClusterError::Cluster(
                            "snapshot END marker without a BEGIN".to_owned(),
                        ));
                    }
                    handler.on_load_end_snapshot()?;
                    *done = true;
                    // The reference stops the poll here (`:117-124`).
                    Ok(Action::Break)
                }
                SnapshotMark::SECTION | SnapshotMark::NullVal => Ok(Action::Continue),
            }
        }
        deepmsg_codec_cluster::cluster_session_codec::SBE_TEMPLATE_ID => {
            let mut decoder = ClusterSessionDecoder::default().header(header, 0);
            let cluster_session_id = decoder.cluster_session_id();
            let correlation_id = decoder.correlation_id();
            let opened_log_position = decoder.opened_log_position();
            let time_of_last_activity = decoder.time_of_last_activity();
            let close_reason = decoder.close_reason();
            let response_stream_id = decoder.response_stream_id();
            let channel_coords = decoder.response_channel_decoder();
            let response_channel =
                String::from_utf8_lossy(decoder.response_channel_slice(channel_coords))
                    .into_owned();
            handler.on_load_cluster_session(SnapshotSession {
                cluster_session_id,
                correlation_id,
                opened_log_position,
                time_of_last_activity,
                close_reason,
                response_stream_id,
                response_channel,
            })?;
            Ok(Action::Continue)
        }
        deepmsg_codec_cluster::timer_codec::SBE_TEMPLATE_ID => {
            let decoder = TimerDecoder::default().header(header, 0);
            handler.on_load_timer(decoder.correlation_id(), decoder.deadline())?;
            Ok(Action::Continue)
        }
        deepmsg_codec_cluster::consensus_module_codec::SBE_TEMPLATE_ID => {
            let decoder = ConsensusModuleDecoder::default().header(header, 0);
            handler.on_load_consensus_module_state(
                decoder.next_session_id(),
                decoder.next_service_session_id().unwrap_or(0),
                decoder.log_service_session_id().unwrap_or(0),
                decoder.pending_message_capacity().unwrap_or(0),
            )?;
            Ok(Action::Continue)
        }
        // `ClusterMembers` is decoded and ignored, exactly as the reference
        // does (`:179-181`).
        deepmsg_codec_cluster::cluster_members_codec::SBE_TEMPLATE_ID => Ok(Action::Continue),
        deepmsg_codec_cluster::pending_message_tracker_codec::SBE_TEMPLATE_ID => {
            let decoder = PendingMessageTrackerDecoder::default().header(header, 0);
            handler.on_load_pending_message_tracker(
                decoder.next_service_session_id(),
                decoder.log_service_session_id(),
                decoder.pending_message_capacity().unwrap_or(0),
                decoder.service_id(),
            )?;
            Ok(Action::Continue)
        }
        _ => Ok(Action::Continue),
    }
}

struct Dispatch<'a> {
    handler: &'a mut dyn SnapshotLoadHandler,
    in_snapshot: bool,
    done: bool,
    error: Option<ClusterError>,
}

impl ControlledHandler for Dispatch<'_> {
    fn on_message(&mut self, message: Message<'_>) -> Action {
        match dispatch_message(
            self.handler,
            &mut self.in_snapshot,
            &mut self.done,
            message.payload,
        ) {
            Ok(action) => action,
            Err(error) => {
                self.error = Some(error);
                Action::Abort
            }
        }
    }
}
