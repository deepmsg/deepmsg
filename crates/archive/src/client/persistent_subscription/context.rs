//! A persistent subscription's context, and the ten things it refuses
//! (`aeron_archive_persistent_subscription.c:88-502`).
//!
//! # The refusals are the reference's, in the reference's order
//!
//! `context_conclude` (`:397-502`) is a straight line of ten checks, and a
//! caller that has two things wrong is told about the first one. The order is
//! therefore part of the behaviour, not an implementation detail: a test that
//! clears two fields asserts *which* refusal comes back, and this keeps them in
//! the reference's sequence and its wording.
//!
//! # Two of them are decisions rather than checks
//!
//! * the tenth is the only one that looks at the *archive* context, and it looks
//!   at it only when the replay channel says `control-mode=response`: the replay
//!   channel's media type has to match the control request channel's, because a
//!   response channel is a channel back to the same place
//!   (`ReplayAndControlChannelsTest`, 20 parameterised cases);
//! * and then `conclude` **rewrites the replay channel** with `rejoin=false`
//!   (`:490-498`). A persistent subscription manages its own rejoin — it is the
//!   thing that decides when to come back — so a client that let the driver
//!   rejoin underneath it would have two things deciding.

use core::fmt;

use deepmsg_core::uri::{ChannelUri, UriError};

use crate::client::context::ArchiveContext;
use crate::client::proxy::NULL_VALUE;

/// `AERON_ARCHIVE_PERSISTENT_SUBSCRIPTION_FROM_START` (`aeron_archive.h:1348`):
/// replay the recording from where it starts.
pub const FROM_START: i64 = -1;

/// `AERON_ARCHIVE_PERSISTENT_SUBSCRIPTION_FROM_LIVE` (`aeron_archive.h:1349`):
/// start from the live position, replaying nothing — and the default a context
/// has before anything sets one.
pub const FROM_LIVE: i64 = -2;

const CONTROL_MODE_KEY: &str = "control-mode";
const CONTROL_MODE_RESPONSE: &str = "response";
const REJOIN_KEY: &str = "rejoin";

/// What a persistent subscription is configured with
/// (`aeron_archive_persistent_subscription_context_t`).
///
/// The three "must be set" values are the reference's `AERON_NULL_VALUE`
/// (`aeronc.h:30`) until something sets them, which is what makes "unset" and
/// "set to a value the reference rejects" two different refusals.
#[derive(Debug, Clone, Default)]
pub struct PersistentSubscriptionContext {
    /// The archive this subscription asks. `None` until something sets it, and
    /// [`conclude`](Self::conclude) refuses that.
    pub archive_context: Option<ArchiveContext>,

    /// Which recording to subscribe to. Negative values are recording ids the
    /// reference cannot have, so they are refused rather than looked up.
    pub recording_id: i64,

    /// The channel the live stream is on. Its media type is not required to
    /// match the replay channel's — they are two channels to two places.
    pub live_channel: Option<String>,

    /// Which stream of [`live_channel`](Self::live_channel) to join.
    pub live_stream_id: i32,

    /// The channel the replay arrives on. `conclude` rewrites this with
    /// `rejoin=false` and the rewritten form is what [`Concluded`] carries.
    pub replay_channel: Option<String>,

    /// Which stream of [`replay_channel`](Self::replay_channel) to read.
    pub replay_stream_id: i32,

    /// Where in the recording to start, or [`FROM_LIVE`] / [`FROM_START`].
    pub start_position: i64,

    /// Where the driver is, for the client a subscription makes when it was not
    /// handed one (`:505-514`).
    pub aeron_directory_name: Option<String>,

    /// Whether a client was handed to this context. See
    /// [`set_aeron`](Self::set_aeron) and [`Concluded::owns_aeron`].
    pub has_aeron: bool,
    // The listener is the one field of the reference's context that is not
    // here yet: its type is what `on_live_joined` and friends are called on,
    // so it arrives with them rather than as a placeholder that cannot be
    // called. Nothing in this module reads it, and `conclude` does not check
    // it — see this module's header for why the reference's own null refusal
    // has no counterpart here.
}

impl PersistentSubscriptionContext {
    /// A context with nothing set: the three nullable ids at
    /// `AERON_NULL_VALUE`, and the start position at [`FROM_LIVE`]
    /// (`aeron_archive_persistent_subscription_context_init`, `:88-115`).
    #[must_use]
    pub fn new() -> Self {
        Self {
            recording_id: NULL_VALUE,
            live_stream_id: NULL_VALUE as i32,
            replay_stream_id: NULL_VALUE as i32,
            start_position: FROM_LIVE,
            ..Self::default()
        }
    }

    /// Hand this context a client that is already made, so the subscription
    /// does not make one of its own (`context_set_aeron`, `:157-164`).
    ///
    /// **The caller keeps ownership**: a context that was handed a client does
    /// not close it (`aeron_archive_persistent_subscription_context_close`,
    /// `:117-140`, closes one only `if (context->owns_aeron_client)`).
    pub fn set_aeron(&mut self) {
        self.has_aeron = true;
    }

    /// Whether this context is one a persistent subscription can be created
    /// with, and what that creation would be
    /// (`aeron_archive_persistent_subscription_context_conclude`, `:397-502`).
    ///
    /// This is the reference's ten refusals and its replay-channel rewrite, and
    /// nothing that needs a driver: the client a subscription makes for itself
    /// (`:505-514`) is made where it is used.
    ///
    /// # Errors
    ///
    /// [`ConcludeError`], one arm per refusal, in the reference's order and with
    /// the reference's own wording.
    pub fn conclude(&self) -> Result<Concluded, ConcludeError> {
        let archive_context = self
            .archive_context
            .as_ref()
            .ok_or(ConcludeError::ArchiveContextRequired)?;

        if NULL_VALUE == self.recording_id {
            return Err(ConcludeError::RecordingIdRequired);
        }
        if self.recording_id < 0 {
            return Err(ConcludeError::RecordingIdInvalid {
                recording_id: self.recording_id,
            });
        }

        let live_channel = self
            .live_channel
            .as_deref()
            .ok_or(ConcludeError::LiveChannelRequired)?;

        if NULL_VALUE as i32 == self.live_stream_id {
            return Err(ConcludeError::LiveStreamIdRequired);
        }

        let replay_channel = self
            .replay_channel
            .as_deref()
            .ok_or(ConcludeError::ReplayChannelRequired)?;

        if NULL_VALUE as i32 == self.replay_stream_id {
            return Err(ConcludeError::ReplayStreamIdRequired);
        }

        if self.start_position < FROM_LIVE {
            return Err(ConcludeError::StartPositionInvalid {
                start_position: self.start_position,
            });
        }

        let mut replay = ChannelUri::parse(replay_channel).map_err(|error| {
            ConcludeError::ReplayChannelNotAChannel {
                channel: replay_channel.to_owned(),
                error,
            }
        })?;

        // The media types have to match because a response channel is the way
        // back along the control channel, so it is the control channel's media
        // that decides whether the replay channel can be one (`:464-484`).
        //
        // **Only when the replay channel asks for one**, and only when the
        // archive context names a request channel: the reference reads a null
        // request channel as "nothing to compare with" and passes
        // (`:466-469`), which is why two of the twenty parameterised cases set
        // neither and expect success.
        if Some(CONTROL_MODE_RESPONSE) == replay.get(CONTROL_MODE_KEY) {
            if let Some(request) = archive_context.control_request_channel.as_deref() {
                let request_uri = ChannelUri::parse(request).map_err(|error| {
                    ConcludeError::ControlRequestChannelNotAChannel {
                        channel: request.to_owned(),
                        error,
                    }
                })?;

                if replay.media() != request_uri.media() {
                    return Err(ConcludeError::MediaTypeMismatch);
                }
            }
        }

        replay.put(REJOIN_KEY, "false");

        Ok(Concluded {
            replay_channel: replay.build(),
            owns_aeron: !self.has_aeron,
            recording_id: self.recording_id,
            live_channel: live_channel.to_owned(),
            live_stream_id: self.live_stream_id,
            replay_stream_id: self.replay_stream_id,
            start_position: self.start_position,
        })
    }
}

/// What a concluded context decided: the fields checked once, and the two
/// answers a subscription needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Concluded {
    /// The replay channel **with `rejoin=false` written into it** — the one to
    /// ask for, not the one that was configured.
    pub replay_channel: String,
    /// Whether the subscription must make its own client (`:505-514`) rather
    /// than use one it was handed.
    pub owns_aeron: bool,
    /// Which recording to subscribe to.
    pub recording_id: i64,
    /// The live channel.
    pub live_channel: String,
    /// The live stream id.
    pub live_stream_id: i32,
    /// The replay stream id.
    pub replay_stream_id: i32,
    /// Where in the recording to start.
    pub start_position: i64,
}

/// Why a persistent subscription's context is not one a subscription can be
/// created with.
///
/// One arm per refusal of `context_conclude` (`:397-502`), in its order. The
/// two that carry a channel are the two the reference answers by *parsing*
/// something, so they are the two that can fail for a reason the caller did not
/// write down.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConcludeError {
    /// No archive context, so there is no archive to subscribe to (`:399`).
    ArchiveContextRequired,
    /// `recording_id` was never set (`:405`).
    RecordingIdRequired,
    /// A negative `recording_id`, which the reference reads as an invalid id
    /// rather than as an unset one (`:411`).
    RecordingIdInvalid {
        /// The id that was set.
        recording_id: i64,
    },
    /// `live_channel` was never set (`:417`).
    LiveChannelRequired,
    /// `live_stream_id` was never set (`:423`).
    LiveStreamIdRequired,
    /// `replay_channel` was never set (`:429`).
    ReplayChannelRequired,
    /// `replay_stream_id` was never set (`:435`).
    ReplayStreamIdRequired,
    /// A `start_position` below [`FROM_LIVE`], which is not a position
    /// (`:441`).
    StartPositionInvalid {
        /// The position that was set.
        start_position: i64,
    },
    /// The replay channel is not a channel (`:448`).
    ReplayChannelNotAChannel {
        /// The channel as it was written.
        channel: String,
        /// Why it is not one.
        error: UriError,
    },
    /// The archive's control request channel is not a channel (`:471`).
    ControlRequestChannelNotAChannel {
        /// The channel as it was written.
        channel: String,
        /// Why it is not one.
        error: UriError,
    },
    /// A `control-mode=response` replay channel whose media type is not the
    /// control channel's (`:473-480`).
    MediaTypeMismatch,
}

impl fmt::Display for ConcludeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Every message is the reference's (`:399-480`), including the one that
        // is three sentences long.
        match self {
            Self::ArchiveContextRequired => write!(f, "archive_context must be set"),
            Self::RecordingIdRequired => write!(f, "recording_id must be set"),
            Self::RecordingIdInvalid { recording_id } => {
                write!(f, "invalid recording_id {recording_id}")
            }
            Self::LiveChannelRequired => write!(f, "live_channel must be set"),
            Self::LiveStreamIdRequired => write!(f, "live_stream_id must be set"),
            Self::ReplayChannelRequired => write!(f, "replay_channel must be set"),
            Self::ReplayStreamIdRequired => write!(f, "replay_stream_id must be set"),
            Self::StartPositionInvalid { start_position } => {
                write!(f, "invalid start_position {start_position}")
            }
            Self::ReplayChannelNotAChannel { channel, error } => {
                write!(f, "failed to parse replay channel '{channel}': {error}")
            }
            Self::ControlRequestChannelNotAChannel { channel, error } => {
                write!(
                    f,
                    "failed to parse control request channel '{channel}': {error}"
                )
            }
            Self::MediaTypeMismatch => write!(
                f,
                "Channel media type mismatch. When using `control-mode=response`, the replay \
                 channel media type must match the media type for the archive control channel."
            ),
        }
    }
}

impl std::error::Error for ConcludeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::ReplayChannelNotAChannel { error, .. }
            | Self::ControlRequestChannelNotAChannel { error, .. } => Some(error),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The context every refusal test starts from: everything set, nothing
    /// wrong. Each test then unsets exactly one thing, which is the shape the
    /// reference's cases have (`…_context_test.cpp:32-195`).
    fn a_context() -> PersistentSubscriptionContext {
        let archive_context = ArchiveContext {
            control_request_channel: Some(CONTROL_CHANNEL.to_owned()),
            ..ArchiveContext::default()
        };

        PersistentSubscriptionContext {
            archive_context: Some(archive_context),
            recording_id: 0,
            live_channel: Some("aeron:ipc".to_owned()),
            live_stream_id: 1000,
            replay_channel: Some("aeron:udp?endpoint=localhost:0".to_owned()),
            replay_stream_id: -5,
            ..PersistentSubscriptionContext::new()
        }
    }

    const CONTROL_CHANNEL: &str = "aeron:udp?endpoint=localhost:8010";

    /// `shouldFailIfArchiveContextNotSet` (`…_context_test.cpp:32`).
    #[test]
    fn an_archive_context_must_be_set() {
        let mut context = a_context();
        context.archive_context = None;

        assert_eq!(
            Err(ConcludeError::ArchiveContextRequired),
            context.conclude()
        );
    }

    /// `shouldFailIfRecordingIdNotSet` (`:47`).
    #[test]
    fn a_recording_id_must_be_set() {
        let mut context = a_context();
        context.recording_id = NULL_VALUE;

        assert_eq!(Err(ConcludeError::RecordingIdRequired), context.conclude());
    }

    /// `shouldFailIfRecordingIdIsInvalid` (`:168`).
    #[test]
    fn a_negative_recording_id_is_invalid_rather_than_unset() {
        let mut context = a_context();
        context.recording_id = -5;

        assert_eq!(
            Err(ConcludeError::RecordingIdInvalid { recording_id: -5 }),
            context.conclude()
        );
    }

    /// `shouldFailIfLiveChannelNotSet` (`:67`).
    #[test]
    fn a_live_channel_must_be_set() {
        let mut context = a_context();
        context.live_channel = None;

        assert_eq!(Err(ConcludeError::LiveChannelRequired), context.conclude());
    }

    /// `shouldFailIfLiveStreamIdNotSet` (`:87`).
    #[test]
    fn a_live_stream_id_must_be_set() {
        let mut context = a_context();
        context.live_stream_id = NULL_VALUE as i32;

        assert_eq!(Err(ConcludeError::LiveStreamIdRequired), context.conclude());
    }

    /// `shouldFailIfReplayChannelNotSet` (`:107`).
    #[test]
    fn a_replay_channel_must_be_set() {
        let mut context = a_context();
        context.replay_channel = None;

        assert_eq!(
            Err(ConcludeError::ReplayChannelRequired),
            context.conclude()
        );
    }

    /// `shouldFailIfReplayStreamIdNotSet` (`:127`).
    #[test]
    fn a_replay_stream_id_must_be_set() {
        let mut context = a_context();
        context.replay_stream_id = NULL_VALUE as i32;

        assert_eq!(
            Err(ConcludeError::ReplayStreamIdRequired),
            context.conclude()
        );
    }

    /// `shouldFailIfStartPositionIsInvalid` (`:147`): `-3` is below
    /// [`FROM_LIVE`], so it is a position the reference does not have.
    #[test]
    fn a_start_position_below_from_live_is_invalid() {
        let mut context = a_context();
        context.start_position = -3;

        assert_eq!(
            Err(ConcludeError::StartPositionInvalid { start_position: -3 }),
            context.conclude()
        );
    }

    /// The two named positions, and any real one, are accepted.
    #[test]
    fn the_named_start_positions_are_accepted() {
        for start_position in [FROM_START, FROM_LIVE, 0, 1 << 40] {
            let mut context = a_context();
            context.start_position = start_position;

            assert_eq!(
                Ok(start_position),
                context.conclude().map(|concluded| concluded.start_position)
            );
        }
    }

    /// `:490-498`: the concluded replay channel has `rejoin=false` in it, and it
    /// is the *rewritten* one that is carried forward.
    #[test]
    fn the_concluded_replay_channel_does_not_rejoin() {
        let context = a_context();

        let concluded = context.conclude().expect("a context with nothing wrong");

        let rewritten = ChannelUri::parse(&concluded.replay_channel).expect("a channel");
        assert_eq!(Some("false"), rewritten.get(REJOIN_KEY));

        // And the context itself is untouched — this is not a setter.
        assert_eq!(
            Some("aeron:udp?endpoint=localhost:0"),
            context.replay_channel.as_deref()
        );
    }

    /// A parameter that is already there keeps its place when `rejoin` is
    /// written, so the channel that goes on the wire is the one that was
    /// configured with one thing changed.
    #[test]
    fn writing_rejoin_leaves_the_rest_of_the_channel_where_it_was() {
        let mut context = a_context();
        context.replay_channel =
            Some("aeron:udp?endpoint=localhost:0|alias=replay|rejoin=true".to_owned());

        let concluded = context.conclude().expect("a context with nothing wrong");

        assert_eq!(
            "aeron:udp?endpoint=localhost:0|alias=replay|rejoin=false",
            concluded.replay_channel
        );
    }

    /// A replay channel that is not a channel is refused before anything is
    /// written into it (`:446-451`).
    #[test]
    fn a_replay_channel_that_is_not_one_is_refused() {
        let mut context = a_context();
        context.replay_channel = Some("not a channel at all".to_owned());

        assert!(matches!(
            context.conclude(),
            Err(ConcludeError::ReplayChannelNotAChannel { .. })
        ));
    }

    /// Whether the subscription makes its own client, which the reference
    /// decides by whether one was handed to it (`:505-514`).
    #[test]
    fn a_context_that_was_handed_a_client_does_not_own_one() {
        let mut context = a_context();
        assert_eq!(Ok(true), context.conclude().map(|it| it.owns_aeron));

        context.set_aeron();
        assert_eq!(Ok(false), context.conclude().map(|it| it.owns_aeron));
    }

    /// `replayAndControlChannelMediaTypesMustMatchWhenUsingResponseChannels`
    /// (`…_context_test.cpp:261`, twenty parameterised cases, table at
    /// `:296-316`).
    ///
    /// The table is the reference's, case for case, including the two that set
    /// no control request channel at all: a `control-mode=response` replay
    /// channel with nothing to compare against passes.
    #[test]
    fn a_response_replay_channel_must_be_the_control_channels_media_type() {
        const CASES: &[(bool, &str, Option<&str>)] = &[
            // (conclude succeeds, replay channel, archive control request channel)
            (true, "aeron:udp?endpoint=localhost:0", None),
            (
                true,
                "aeron:udp?endpoint=localhost:0",
                Some(CONTROL_CHANNEL),
            ),
            (
                true,
                "aeron:udp?endpoint=localhost:0",
                Some("aeron:udp?control-mode=response|control=localhost:10002"),
            ),
            (true, "aeron:udp?endpoint=localhost:0", Some("aeron:ipc")),
            (
                true,
                "aeron:udp?endpoint=localhost:0",
                Some("aeron:ipc?control-mode=response"),
            ),
            (
                true,
                "aeron:udp?control=localhost:10001|control-mode=response",
                None,
            ),
            (
                true,
                "aeron:udp?control=localhost:10001|control-mode=response",
                Some(CONTROL_CHANNEL),
            ),
            (
                true,
                "aeron:udp?control=localhost:10001|control-mode=response",
                Some("aeron:udp?control-mode=response|control=localhost:10002"),
            ),
            (
                false,
                "aeron:udp?control=localhost:10001|control-mode=response",
                Some("aeron:ipc"),
            ),
            (
                false,
                "aeron:udp?control=localhost:10001|control-mode=response",
                Some("aeron:ipc?control-mode=response"),
            ),
            (true, "aeron:ipc", None),
            (true, "aeron:ipc", Some(CONTROL_CHANNEL)),
            (
                true,
                "aeron:ipc",
                Some("aeron:udp?control-mode=response|control=localhost:10002"),
            ),
            (true, "aeron:ipc", Some("aeron:ipc")),
            (true, "aeron:ipc", Some("aeron:ipc?control-mode=response")),
            (true, "aeron:ipc?control-mode=response", None),
            (
                false,
                "aeron:ipc?control-mode=response",
                Some(CONTROL_CHANNEL),
            ),
            (
                false,
                "aeron:ipc?control-mode=response",
                Some("aeron:udp?control-mode=response|control=localhost:10002"),
            ),
            (true, "aeron:ipc?control-mode=response", Some("aeron:ipc")),
            (
                true,
                "aeron:ipc?control-mode=response",
                Some("aeron:ipc?control-mode=response"),
            ),
        ];

        for (index, (succeeds, replay_channel, control_channel)) in CASES.iter().enumerate() {
            let mut context = a_context();
            context.replay_channel = Some((*replay_channel).to_owned());
            context
                .archive_context
                .as_mut()
                .expect("the archive context a_context sets")
                .control_request_channel = control_channel.map(str::to_owned);

            let concluded = context.conclude();

            assert_eq!(
                *succeeds,
                concluded.is_ok(),
                "case {index}: replay '{replay_channel}', control {:?} answered {concluded:?}",
                control_channel
            );

            if let Ok(concluded) = concluded {
                let rewritten = ChannelUri::parse(&concluded.replay_channel).expect("a channel");
                assert_eq!(Some("false"), rewritten.get(REJOIN_KEY), "case {index}");
            }
        }
    }
}
