//! Reading the log back (`LogAdapter.java`).
//!
//! The replay image carries the same cluster records the log publication
//! wrote. The adapter assembles whole messages and hands each to a
//! [`LogReplayHandler`]:
//!
//! | Template | Record | Handler |
//! |---|---|---|
//! | 1 | `SessionMessageHeader` | `on_replay_session_message` |
//! | 20 | `TimerEvent` | `on_replay_timer_event` |
//! | 21 | `SessionOpenEvent` | `on_replay_session_open` |
//! | 22 | `SessionCloseEvent` | `on_replay_session_close` |
//! | 23 | `ClusterActionRequest` | `on_replay_cluster_action` |
//! | 24 | `NewLeadershipTermEvent` | `on_replay_new_leadership_term_event` |
//!
//! A message with another schema is an extension message; without an extension
//! the reference throws (`LogAdapter.java:201-205`), and so does this.

use deepmsg_client::client::Client;
use deepmsg_client::fragment_assembler::{
    Action, AssemblingSink, ControlledHandler, FragmentAssembler, Message,
};
use deepmsg_client::image::ImageView;
use deepmsg_cluster_common::decode::at_least;
use deepmsg_codec_cluster::close_reason::CloseReason;
use deepmsg_codec_cluster::cluster_action::ClusterAction;
use deepmsg_codec_cluster::cluster_action_request_codec::ClusterActionRequestDecoder;
use deepmsg_codec_cluster::message_header_codec::{
    ENCODED_LENGTH as MESSAGE_HEADER_LENGTH, MessageHeaderDecoder,
};
use deepmsg_codec_cluster::new_leadership_term_event_codec::NewLeadershipTermEventDecoder;
use deepmsg_codec_cluster::session_close_event_codec::SessionCloseEventDecoder;
use deepmsg_codec_cluster::session_message_header_codec::SessionMessageHeaderDecoder;
use deepmsg_codec_cluster::session_open_event_codec::SessionOpenEventDecoder;
use deepmsg_codec_cluster::timer_event_codec::TimerEventDecoder;
use deepmsg_codec_cluster::{ReadBuf, SBE_SCHEMA_ID};

use crate::error::ClusterError;

/// What a replay applies to the module (`ConsensusModuleAgent.onReplay*`,
/// `:1496-1637`).
pub trait LogReplayHandler {
    /// `onReplaySessionMessage` (`:1496-1508`).
    ///
    /// # Errors
    ///
    /// Nothing today.
    fn on_replay_session_message(
        &mut self,
        cluster_session_id: i64,
        timestamp: i64,
    ) -> Result<(), ClusterError>;

    /// `onReplayTimerEvent` (`:1539-1545`).
    ///
    /// # Errors
    ///
    /// Nothing today.
    fn on_replay_timer_event(&mut self, correlation_id: i64) -> Result<(), ClusterError>;

    /// `onReplaySessionOpen` (`:1547-1557`).
    ///
    /// # Errors
    ///
    /// [`ClusterError`] from the session manager.
    #[allow(clippy::too_many_arguments)]
    fn on_replay_session_open(
        &mut self,
        log_position: i64,
        correlation_id: i64,
        cluster_session_id: i64,
        timestamp: i64,
        response_stream_id: i32,
        response_channel: &str,
    ) -> Result<(), ClusterError>;

    /// `onReplaySessionClose` (`:1559-1562`).
    ///
    /// # Errors
    ///
    /// [`ClusterError`] from the session manager.
    fn on_replay_session_close(
        &mut self,
        cluster_session_id: i64,
        close_reason: CloseReason,
    ) -> Result<(), ClusterError>;

    /// `onReplayClusterAction` (`:1564-1591`).
    ///
    /// # Errors
    ///
    /// [`ClusterError`] from a snapshot the replay asks for.
    #[allow(clippy::too_many_arguments)]
    fn on_replay_cluster_action(
        &mut self,
        leadership_term_id: i64,
        log_position: i64,
        timestamp: i64,
        action: ClusterAction,
        flags: i32,
    ) -> Result<(), ClusterError>;

    /// `onReplayNewLeadershipTermEvent` (`:1593-1637`).
    ///
    /// # Errors
    ///
    /// [`ClusterError`] when the event names a unit or version this build
    /// cannot accept.
    #[allow(clippy::too_many_arguments)]
    fn on_replay_new_leadership_term_event(
        &mut self,
        leadership_term_id: i64,
        log_position: i64,
        timestamp: i64,
        term_base_log_position: i64,
        time_unit: i32,
        app_version: i32,
    ) -> Result<(), ClusterError>;

    /// `onReplayExtensionMessage` (`:1510-1537`).
    ///
    /// # Errors
    ///
    /// [`ClusterError`] when there is no extension to hand it to.
    fn on_replay_extension_message(
        &mut self,
        schema_id: u16,
        template_id: u16,
        version: u16,
        message: &[u8],
    ) -> Result<(), ClusterError>;
}

/// `LogAdapter` (`LogAdapter.java:34-297`).
///
/// It **holds the image**, as the reference does (`LogAdapter.java:107-134`):
/// the driver withdrawing it does not take it away from this adapter, so
/// `is_image_closed` / `is_log_end_of_stream` / `is_log_end_of_stream_at` keep
/// answering the snapshot the close captured. Polling still goes through the
/// client by registration id, because the reader is the client's.
pub struct LogAdapter {
    assembler: FragmentAssembler,
    image_registration_id: Option<i64>,
    /// The reference's `Image image` field — a view, because the client owns
    /// the reader the image belongs to.
    image: Option<ImageView>,
    position: i64,
}

impl LogAdapter {
    /// `new LogAdapter(consensusModuleAgent, fragmentLimit)`.
    pub fn new() -> Self {
        Self {
            assembler: FragmentAssembler::new(),
            image_registration_id: None,
            image: None,
            position: 0,
        }
    }

    /// `image(Image)` (`:126-134`): attach the image the election found.
    ///
    /// `position` seeds the sticky position the adapter falls back to once the
    /// image is cleared; while one is held, [`Self::position`] answers the
    /// image's, as the reference's does.
    pub fn set_image(&mut self, image: ImageView, position: i64) {
        self.image_registration_id = Some(image.registration_id());
        self.image = Some(image);
        self.position = position;
    }

    /// `image(null)` after the image is gone: the sticky position stays, and no
    /// further poll reads anything (`LogAdapter.java:106-109`).
    pub fn clear_image(&mut self) {
        if let Some(image) = self.image.take() {
            self.position = image.position();
        }

        self.image_registration_id = None;
    }

    /// The image this adapter is reading (`LogAdapter.image()`, `:115-120`).
    ///
    /// The reference's `image` field, which stays askable after the driver
    /// withdrew it — a closed image answers its snapshot.
    pub fn image(&self) -> Option<&ImageView> {
        self.image.as_ref()
    }

    /// Whether there is no image, or the one there is has been withdrawn
    /// (`isImageClosed`, `:106-109`).
    pub fn is_image_closed(&self) -> bool {
        self.image.as_ref().is_none_or(ImageView::is_closed)
    }

    /// Whether the publisher signalled end of stream (`isLogEndOfStream`,
    /// `:111-114`). No image answers `false`, as the reference's does.
    ///
    /// A **closed** image answers what it answered at the close
    /// (`Image.java:251-254`), which is what makes a leader that stopped
    /// gracefully distinguishable from one that vanished.
    pub fn is_log_end_of_stream(&self) -> bool {
        self.image
            .as_ref()
            .and_then(ImageView::is_end_of_stream)
            .unwrap_or(false)
    }

    /// Whether the end of stream was at this position (`isLogEndOfStreamAt`,
    /// `:116-120`).
    ///
    /// The comparison is against `endOfStreamPosition()`, which a closed image
    /// answers from its close snapshot (`Image.java:267-270`).
    pub fn is_log_end_of_stream_at(&self, position: i64) -> bool {
        self.image
            .as_ref()
            .and_then(ImageView::end_of_stream_position)
            .is_some_and(|end_of_stream| position == end_of_stream)
    }

    /// `disconnect(errorHandler, maxLogPosition)` (`:70-84`): pin the last
    /// position at no more than `max_log_position` and let go of the image.
    ///
    /// Closing the image's subscription, and waiting for its local sockets to
    /// be free, is the caller's: it owns the client those counters live in.
    pub fn disconnect(&mut self, max_log_position: i64) {
        if let Some(image) = self.image.take() {
            self.position = image.position();
        }

        self.image_registration_id = None;
        self.position = self.position.min(max_log_position);
    }

    /// Whether an image has been attached.
    pub const fn has_image(&self) -> bool {
        self.image_registration_id.is_some()
    }

    /// The image's registration id, once attached.
    pub const fn image_registration_id(&self) -> Option<i64> {
        self.image_registration_id
    }

    /// `position()` (`:86-94`): the image's position, or the sticky one.
    ///
    /// A closed image answers its `finalPosition`, which is what the reference
    /// reads off the object it still holds.
    pub fn position(&self) -> i64 {
        self.image
            .as_ref()
            .map_or(self.position, ImageView::position)
    }

    /// Poll the image once, up to `bound_position`
    /// (`boundedControlledPoll`, `:96-104`).
    ///
    /// A live follower bounds this at `min(commitPosition, limit)` and a replay
    /// at the replay's stop position (`ConsensusModuleAgent.java:2523`,
    /// `:1853`) — the same number, the reference's single `LogAdapter` serves
    /// both.
    ///
    /// # Errors
    ///
    /// The first decode or handler error.
    pub fn poll(
        &mut self,
        client: &mut Client,
        subscription_id: i64,
        bound_position: i64,
        handler: &mut dyn LogReplayHandler,
        fragment_limit: usize,
    ) -> Result<Option<usize>, ClusterError> {
        let Some(image_registration_id) = self.image_registration_id else {
            return Ok(Some(0));
        };

        let mut assembler = core::mem::replace(&mut self.assembler, FragmentAssembler::new());
        let mut dispatch = Dispatch {
            handler,
            error: None,
        };
        // The answer is honoured, not discarded: `Abort` leaves the refused
        // entry where it is and `Break` stops the read where the handler said
        // to — which is what the reference's `boundedControlledPoll` does with
        // `LogAdapter`'s answer (`LogAdapter.java:103`).
        let result = {
            let mut sink = AssemblingSink::new(&mut assembler, &mut dispatch);
            client.poll_image_bounded_controlled(
                subscription_id,
                image_registration_id,
                bound_position,
                fragment_limit,
                &mut sink,
            )
        };
        self.assembler = assembler;
        if let Some(error) = dispatch.error {
            return Err(error);
        }

        // `position()` reads the image while one is held; the sticky value is
        // what the adapter falls back to after `clear_image`/`disconnect`, so
        // it is kept in step here too (`LogAdapter.java:126-134`).
        if let Some(image) = &self.image {
            self.position = image.position();
        }

        Ok(result)
    }

    /// The body of `onMessage` (`LogAdapter.java:192-297`), driven by tests.
    ///
    /// # Errors
    ///
    /// As [`Self::poll`].
    pub fn dispatch_message(
        &mut self,
        handler: &mut dyn LogReplayHandler,
        message: Message<'_>,
    ) -> Result<Action, ClusterError> {
        let payload = message.payload;
        at_least(payload, MESSAGE_HEADER_LENGTH, "log message header")?;
        let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(payload), 0);
        if header.schema_id() != SBE_SCHEMA_ID {
            handler.on_replay_extension_message(
                header.schema_id(),
                header.template_id(),
                header.version(),
                payload,
            )?;
            return Ok(Action::Continue);
        }

        match header.template_id() {
            deepmsg_codec_cluster::session_message_header_codec::SBE_TEMPLATE_ID => {
                let decoder = SessionMessageHeaderDecoder::default().header(header, 0);
                handler
                    .on_replay_session_message(decoder.cluster_session_id(), decoder.timestamp())?;
                Ok(Action::Continue)
            }
            deepmsg_codec_cluster::timer_event_codec::SBE_TEMPLATE_ID => {
                let decoder = TimerEventDecoder::default().header(header, 0);
                handler.on_replay_timer_event(decoder.correlation_id())?;
                Ok(Action::Break)
            }
            deepmsg_codec_cluster::session_open_event_codec::SBE_TEMPLATE_ID => {
                let mut decoder = SessionOpenEventDecoder::default().header(header, 0);
                let correlation_id = decoder.correlation_id();
                let cluster_session_id = decoder.cluster_session_id();
                let timestamp = decoder.timestamp();
                let response_stream_id = decoder.response_stream_id();
                let channel_coords = decoder.response_channel_decoder();
                let response_channel =
                    String::from_utf8_lossy(decoder.response_channel_slice(channel_coords))
                        .into_owned();
                // The reference's `LogAdapter` hands the session
                // `header.position()`, whose javadoc is "the offset of the
                // frame immediately after this one"
                // (`aeron-client/.../logbuffer/Header.java:94-97`, `:240-248`
                // — `computePosition(termId(), nextTermOffset(), …)` with
                // `nextTermOffset = align(termOffset + termOccupancyLength)`).
                // **Not** the frame's own start: a leader takes its
                // `openedLogPosition` from `Publication.offer`'s return, which
                // is the position after the append, and a follower has to land
                // on the same number or the two write different snapshots
                // (P3-7c's byte ruler caught exactly that).
                //
                // `fragmented_frame_length` is this build's
                // `termOccupancyLength`: what one frame carrying the whole
                // payload measures, which is `frame_length` when nothing was
                // fragmented (`fragment_assembler.rs:440-453`).
                let occupancy = message.header.fragmented_frame_length;
                let next_position = message.header.position
                    + i64::from(deepmsg_core::logbuffer::position::align_up(
                        occupancy,
                        deepmsg_core::logbuffer::descriptor::FRAME_ALIGNMENT,
                    ));
                handler.on_replay_session_open(
                    next_position,
                    correlation_id,
                    cluster_session_id,
                    timestamp,
                    response_stream_id,
                    &response_channel,
                )?;
                Ok(Action::Break)
            }
            deepmsg_codec_cluster::session_close_event_codec::SBE_TEMPLATE_ID => {
                let decoder = SessionCloseEventDecoder::default().header(header, 0);
                handler.on_replay_session_close(
                    decoder.cluster_session_id(),
                    decoder.close_reason(),
                )?;
                Ok(Action::Break)
            }
            deepmsg_codec_cluster::cluster_action_request_codec::SBE_TEMPLATE_ID => {
                let decoder = ClusterActionRequestDecoder::default().header(header, 0);
                // The reference substitutes the default (0) for a null flags
                // field (`LogAdapter.java:268-269`).
                let flags = decoder.flags().unwrap_or(0);
                handler.on_replay_cluster_action(
                    decoder.leadership_term_id(),
                    decoder.log_position(),
                    decoder.timestamp(),
                    decoder.action(),
                    flags,
                )?;
                Ok(Action::Break)
            }
            deepmsg_codec_cluster::new_leadership_term_event_codec::SBE_TEMPLATE_ID => {
                let decoder = NewLeadershipTermEventDecoder::default().header(header, 0);
                handler.on_replay_new_leadership_term_event(
                    decoder.leadership_term_id(),
                    decoder.log_position(),
                    decoder.timestamp(),
                    decoder.term_base_log_position(),
                    decoder.time_unit() as i32,
                    decoder.app_version().unwrap_or(0),
                )?;
                Ok(Action::Break)
            }
            _ => Ok(Action::Continue),
        }
    }
}

impl Default for LogAdapter {
    fn default() -> Self {
        Self::new()
    }
}

struct Dispatch<'a> {
    handler: &'a mut dyn LogReplayHandler,
    error: Option<ClusterError>,
}

impl ControlledHandler for Dispatch<'_> {
    fn on_message(&mut self, message: Message<'_>) -> Action {
        let mut adapter = LogAdapter::new();
        match adapter.dispatch_message(self.handler, message) {
            Ok(action) => action,
            Err(error) => {
                self.error = Some(error);
                Action::Abort
            }
        }
    }
}
