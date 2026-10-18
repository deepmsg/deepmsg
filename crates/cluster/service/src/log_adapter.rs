//! Reading the cluster log (`BoundedLogAdapter.java`).
//!
//! The container subscribes to the channel `JoinLog` names, waits for the image
//! whose session is the one the event named, and reads it **up to the commit
//! position counter** — never beyond, because an entry the leader has not
//! committed is an entry the service must not apply.
//!
//! # Why the image is taken out of the client
//!
//! The handler for a `SessionOpenEvent` adds a publication to the client
//! (`ContainerClientSession.connect`), and the handler for a
//! `SessionCloseEvent` removes one. A reader still inside
//! `Client::poll_image_bounded` holds `&mut Client` for the whole poll, so the
//! two could not both happen in one turn. Taking the image out
//! ([`Client::take_image`]) leaves the client free, which is the same shape the
//! consensus module's own log adapter uses for the same reason.
//!
//! # The one copy
//!
//! The generated SBE decoders read through a `&[u8]`, and this build's client
//! deliberately hands out windows over a term another process is writing, not
//! slices. So an unfragmented message is copied once into this adapter's
//! reusable buffer before it is decoded, and a fragmented one is assembled by
//! [`FragmentAssembler`], which copies by construction. The business sees a
//! slice borrowed from that buffer for the duration of its callback — the
//! reference's "borrow the term until the callback returns", with one copy in
//! between. (The performance spec's zero-copy requirement meets the client's
//! no-slice rule here; the copy is counted in the plan's deviations.)

use deepmsg_client::fragment_assembler::FragmentAssembler;
use deepmsg_client::image::{Fragment, Image};
use deepmsg_codec_cluster::close_reason::CloseReason;
use deepmsg_codec_cluster::cluster_action::ClusterAction;
use deepmsg_codec_cluster::cluster_action_request_codec::ClusterActionRequestDecoder;
use deepmsg_codec_cluster::cluster_time_unit::ClusterTimeUnit;
use deepmsg_codec_cluster::message_header_codec::{
    ENCODED_LENGTH as MESSAGE_HEADER_LENGTH, MessageHeaderDecoder,
};
use deepmsg_codec_cluster::new_leadership_term_event_codec::NewLeadershipTermEventDecoder;
use deepmsg_codec_cluster::session_close_event_codec::SessionCloseEventDecoder;
use deepmsg_codec_cluster::session_message_header_codec::SessionMessageHeaderDecoder;
use deepmsg_codec_cluster::session_open_event_codec::SessionOpenEventDecoder;
use deepmsg_codec_cluster::timer_event_codec::TimerEventDecoder;
use deepmsg_codec_cluster::{ReadBuf, SBE_SCHEMA_ID};
use deepmsg_core::logbuffer::descriptor::FRAME_ALIGNMENT;

use crate::error::ContainerError;
use crate::proxy::SESSION_HEADER_LENGTH;

/// `ConsensusModule.CLUSTER_ACTION_FLAGS_DEFAULT` (`ConsensusModule.java`).
pub const CLUSTER_ACTION_FLAGS_DEFAULT: i32 = 0;

/// One log message, decoded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LogMessage {
    /// `SessionMessageHeader(1)` plus payload.
    SessionMessage {
        /// The session that wrote it; the service's own messages carry the
        /// service id.
        cluster_session_id: i64,
        /// The log timestamp.
        timestamp: i64,
    },
    /// `TimerEvent(20)`.
    TimerEvent {
        /// The timer's correlation id.
        correlation_id: i64,
        /// The log timestamp.
        timestamp: i64,
    },
    /// `SessionOpenEvent(21)`.
    SessionOpen {
        /// The leadership term the open belongs to.
        leadership_term_id: i64,
        /// The new session.
        cluster_session_id: i64,
        /// The log timestamp.
        timestamp: i64,
        /// The client's response stream.
        response_stream_id: i32,
        /// The client's response channel.
        response_channel: String,
        /// The authenticated principal, empty when none.
        encoded_principal: Vec<u8>,
    },
    /// `SessionCloseEvent(22)`.
    SessionClose {
        /// The leadership term the close belongs to.
        leadership_term_id: i64,
        /// The closing session.
        cluster_session_id: i64,
        /// The log timestamp.
        timestamp: i64,
        /// Why it closed.
        close_reason: CloseReason,
    },
    /// `ClusterActionRequest(23)`.
    ClusterAction {
        /// The leadership term.
        leadership_term_id: i64,
        /// The position the request was made at.
        log_position: i64,
        /// The log timestamp.
        timestamp: i64,
        /// What to do.
        action: ClusterAction,
        /// The action's flags; absent means [`CLUSTER_ACTION_FLAGS_DEFAULT`].
        flags: i32,
    },
    /// `NewLeadershipTermEvent(24)`.
    NewLeadershipTerm {
        /// The new term.
        leadership_term_id: i64,
        /// The log position the term began at.
        log_position: i64,
        /// The log timestamp.
        timestamp: i64,
        /// The term's base log position.
        term_base_log_position: i64,
        /// Who won.
        leader_member_id: i32,
        /// The log publication's session.
        log_session_id: i32,
        /// The unit of the coming term's timestamps.
        time_unit: ClusterTimeUnit,
        /// The application version; absent means 0.
        app_version: i32,
    },
}

/// One message and where it ends.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogEvent<'a> {
    /// `Header.position()`: the position **after** this message
    /// (`Header.java:90-97`), which is the `logPosition` every handler stores.
    pub position: i64,
    /// The publication's session.
    pub session_id: i32,
    /// The stream.
    pub stream_id: i32,
    /// The message.
    pub message: LogMessage,
    /// The payload of a session message, without its 32-byte header; empty for
    /// the rest.
    pub payload: &'a [u8],
}

/// What the adapter calls for each decoded message.
pub trait LogHandler {
    /// Dispatch one message.
    ///
    /// # Errors
    ///
    /// [`ContainerError`] from the handler; the poll stops delivering after the
    /// first one, and the agent ends on it.
    fn on_log_event(&mut self, event: LogEvent<'_>) -> Result<(), ContainerError>;
}

/// `BoundedLogAdapter` (`BoundedLogAdapter.java`).
pub struct BoundedLogAdapter {
    subscription_registration_id: Option<i64>,
    image: Option<Image>,
    max_log_position: i64,
    fragment_limit: usize,
    assembler: FragmentAssembler,
    scratch: Vec<u8>,
}

impl Default for BoundedLogAdapter {
    fn default() -> Self {
        Self::new(0)
    }
}

impl BoundedLogAdapter {
    /// An adapter with no image, reading at most `fragment_limit` fragments a
    /// poll (`CSA:190`, default 50).
    pub fn new(fragment_limit: usize) -> Self {
        Self {
            subscription_registration_id: None,
            image: None,
            max_log_position: 0,
            fragment_limit,
            assembler: FragmentAssembler::new(),
            scratch: Vec::new(),
        }
    }

    /// `maxLogPosition(position)` (`BoundedLogAdapter.java:110-113`).
    pub fn set_max_log_position(&mut self, position: i64) {
        self.max_log_position = position;
    }

    /// The image, for the continuity checks.
    pub fn image(&self) -> Option<&Image> {
        self.image.as_ref()
    }

    /// Whether an image is attached (`logAdapter.image() != null`).
    pub fn has_image(&self) -> bool {
        self.image.is_some()
    }

    /// `image(Image)` (`:120-123`), with the subscription it came from.
    pub fn set_image(&mut self, subscription_registration_id: i64, image: Image) {
        self.subscription_registration_id = Some(subscription_registration_id);
        self.image = Some(image);
    }

    /// Where the subscription is registered, for the close path.
    pub const fn subscription_registration_id(&self) -> Option<i64> {
        self.subscription_registration_id
    }

    /// `isDone()` (`:115-118`): the image reached its end, or is over.
    pub fn is_done(&self) -> bool {
        let Some(image) = self.image.as_ref() else {
            return true;
        };
        image.position() >= self.max_log_position
            || image.is_end_of_stream().unwrap_or(false)
            || image.is_closed()
    }

    /// `poll(limit)` (`:130-134`): read fragments that start before
    /// `min(limit, maxLogPosition)`.
    ///
    /// The caller publishes the reader's position to the subscription's
    /// counter afterwards (`Image::subscriber_position_id`); it is not done
    /// here because the handler it passes may already hold the client, and one
    /// mutable borrow of it at a time is the whole point of taking the image
    /// out.
    ///
    /// # Errors
    ///
    /// The first error a handler raised.
    pub fn poll(
        &mut self,
        limit: i64,
        handler: &mut dyn LogHandler,
    ) -> Result<usize, ContainerError> {
        let Some(image) = self.image.as_mut() else {
            return Ok(0);
        };

        let bound = limit.min(self.max_log_position);
        let fragment_limit = self.fragment_limit;
        let mut error = None;

        let read = {
            let Self {
                assembler, scratch, ..
            } = self;

            image.bounded_poll(bound, fragment_limit, |fragment| {
                if error.is_some() {
                    return;
                }
                let result = if fragment.is_unfragmented() {
                    dispatch_unfragmented(scratch, fragment, handler)
                } else {
                    dispatch_fragmented(assembler, fragment, handler)
                };
                if let Err(raised) = result {
                    error = Some(raised);
                }
            })
        };

        if let Some(error) = error {
            return Err(error);
        }
        Ok(read)
    }

    /// `close()` (`:96-104`): the image's subscription is gone, so the image
    /// goes with it.
    pub fn clear_image(&mut self) {
        self.image = None;
        self.subscription_registration_id = None;
    }
}

fn dispatch_unfragmented(
    scratch: &mut Vec<u8>,
    fragment: &Fragment<'_>,
    handler: &mut dyn LogHandler,
) -> Result<(), ContainerError> {
    let length = fragment.payload_length();
    if scratch.len() < length {
        scratch.resize(length, 0);
    }
    let Some((buffer, offset, len)) = fragment.payload_region() else {
        return Err(ContainerError::Cluster(
            "a log fragment has no payload region".to_owned(),
        ));
    };
    if buffer.copy_out(offset, &mut scratch[..len]).is_none() {
        return Err(ContainerError::Cluster(
            "a log fragment's payload could not be read".to_owned(),
        ));
    }

    let end = end_position(fragment)?;
    let bytes = &scratch[..len];
    match decode(
        end,
        fragment.session_id().unwrap_or(-1),
        fragment.stream_id().unwrap_or(-1),
        bytes,
    )? {
        Some(event) => handler.on_log_event(event),
        None => Ok(()),
    }
}

fn dispatch_fragmented(
    assembler: &mut FragmentAssembler,
    fragment: &Fragment<'_>,
    handler: &mut dyn LogHandler,
) -> Result<(), ContainerError> {
    let mut error = None;
    assembler.push(fragment, &mut |message| {
        if error.is_some() {
            return;
        }
        let end =
            message.header.position + i64::from(align_up(message.header.fragmented_frame_length));
        match decode(
            end,
            message.header.session_id,
            message.header.stream_id,
            message.payload,
        ) {
            Ok(Some(event)) => {
                if let Err(raised) = handler.on_log_event(event) {
                    error = Some(raised);
                }
            }
            Ok(None) => {}
            Err(raised) => error = Some(raised),
        }
    });
    error.map_or(Ok(()), Err)
}

/// `Header.position()` for one fragment: its start plus the aligned frame
/// length (`Header.java:90-97` computes it from `nextTermOffset`).
fn end_position(fragment: &Fragment<'_>) -> Result<i64, ContainerError> {
    let length = fragment.frame_length().ok_or_else(|| {
        ContainerError::Cluster("a log fragment has no readable frame length".to_owned())
    })?;
    Ok(fragment.position() + i64::from(align_up(length)))
}

fn align_up(length: i32) -> i32 {
    (length + FRAME_ALIGNMENT - 1) & !(FRAME_ALIGNMENT - 1)
}

/// Decode one whole message. `None` for a template neither this build nor the
/// reference handles (`MembershipChangeEvent(25)`, and the reference's silent
/// default arm).
fn decode<'a>(
    end_position: i64,
    session_id: i32,
    stream_id: i32,
    bytes: &'a [u8],
) -> Result<Option<LogEvent<'a>>, ContainerError> {
    if bytes.len() < MESSAGE_HEADER_LENGTH {
        return Err(ContainerError::Cluster(
            "a log message is too short for its own header".to_owned(),
        ));
    }

    let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(bytes), 0);
    if header.schema_id() != SBE_SCHEMA_ID {
        return Err(ContainerError::Cluster(format!(
            "expected schemaId={SBE_SCHEMA_ID}, actual={}",
            header.schema_id()
        )));
    }

    let template_id = header.template_id();
    let (message, payload) = match template_id {
        deepmsg_codec_cluster::session_message_header_codec::SBE_TEMPLATE_ID => {
            let decoder = SessionMessageHeaderDecoder::default().header(header, 0);
            if bytes.len() < SESSION_HEADER_LENGTH {
                return Err(ContainerError::Cluster(
                    "a session message is shorter than its header".to_owned(),
                ));
            }
            (
                LogMessage::SessionMessage {
                    cluster_session_id: decoder.cluster_session_id(),
                    timestamp: decoder.timestamp(),
                },
                &bytes[SESSION_HEADER_LENGTH..],
            )
        }
        deepmsg_codec_cluster::timer_event_codec::SBE_TEMPLATE_ID => {
            let decoder = TimerEventDecoder::default().header(header, 0);
            (
                LogMessage::TimerEvent {
                    correlation_id: decoder.correlation_id(),
                    timestamp: decoder.timestamp(),
                },
                &[][..],
            )
        }
        deepmsg_codec_cluster::session_open_event_codec::SBE_TEMPLATE_ID => {
            let mut decoder = SessionOpenEventDecoder::default().header(header, 0);
            let channel_coords = decoder.response_channel_decoder();
            let response_channel =
                String::from_utf8_lossy(decoder.response_channel_slice(channel_coords))
                    .into_owned();
            let principal_coords = decoder.encoded_principal_decoder();
            let encoded_principal = decoder.encoded_principal_slice(principal_coords).to_vec();
            (
                LogMessage::SessionOpen {
                    leadership_term_id: decoder.leadership_term_id(),
                    cluster_session_id: decoder.cluster_session_id(),
                    timestamp: decoder.timestamp(),
                    response_stream_id: decoder.response_stream_id(),
                    response_channel,
                    encoded_principal,
                },
                &[][..],
            )
        }
        deepmsg_codec_cluster::session_close_event_codec::SBE_TEMPLATE_ID => {
            let decoder = SessionCloseEventDecoder::default().header(header, 0);
            (
                LogMessage::SessionClose {
                    leadership_term_id: decoder.leadership_term_id(),
                    cluster_session_id: decoder.cluster_session_id(),
                    timestamp: decoder.timestamp(),
                    close_reason: decoder.close_reason(),
                },
                &[][..],
            )
        }
        deepmsg_codec_cluster::cluster_action_request_codec::SBE_TEMPLATE_ID => {
            let decoder = ClusterActionRequestDecoder::default().header(header, 0);
            (
                LogMessage::ClusterAction {
                    leadership_term_id: decoder.leadership_term_id(),
                    log_position: decoder.log_position(),
                    timestamp: decoder.timestamp(),
                    action: decoder.action(),
                    flags: decoder.flags().unwrap_or(CLUSTER_ACTION_FLAGS_DEFAULT),
                },
                &[][..],
            )
        }
        deepmsg_codec_cluster::new_leadership_term_event_codec::SBE_TEMPLATE_ID => {
            let decoder = NewLeadershipTermEventDecoder::default().header(header, 0);
            (
                LogMessage::NewLeadershipTerm {
                    leadership_term_id: decoder.leadership_term_id(),
                    log_position: decoder.log_position(),
                    timestamp: decoder.timestamp(),
                    term_base_log_position: decoder.term_base_log_position(),
                    leader_member_id: decoder.leader_member_id(),
                    log_session_id: decoder.log_session_id(),
                    time_unit: decoder.time_unit(),
                    app_version: decoder.app_version().unwrap_or(0),
                },
                &[][..],
            )
        }
        _ => return Ok(None),
    };

    Ok(Some(LogEvent {
        position: end_position,
        session_id,
        stream_id,
        message,
        payload,
    }))
}
