//! The archive client's request proxy: how a request becomes bytes on a
//! publication (`aeron_archive_proxy.c`, 1087 lines).
//!
//! Every function here is the same three steps — wrap an SBE encoder over one
//! reusable buffer, fill in the fields, offer it — repeated for thirty-two
//! requests, and the repetition is kept because that is what the file is: a
//! transcription whose value is that a reader can put one function beside its C
//! original and check the field list. What is *not* repetition is the three
//! things around them:
//!
//! * **`client_info`**, the string a client introduces itself with. It is not
//!   decoration: the archive puts it in the label of the control-session counter
//!   it allocates for this client
//!   ([`crate::server::counters::control_session_label`]), which is how one
//!   client's counters are told from another's.
//! * **`control_session_id`**, which every request but the first four carries.
//!   It is `-1` until a connect is answered, and it is set from **outside** — by
//!   `async_connect`, when the archive's `ConnectResponse` names the session it
//!   opened (`aeron_archive_async_connect.c:451-454`). A proxy does not learn it
//!   itself.
//! * **which of the two offers a request uses**, which is the next section.
//!
//! # Two ways to fail, and only one of them is worth retrying
//!
//! [`ArchiveProxy::offer`] retries; [`ArchiveProxy::offer_once`] does not. Which
//! one a request uses is not a matter of taste in the reference: the **four**
//! that open or close the session — connect, archive id, challenge response,
//! close — use `offer_once`, because there is no session yet to be
//! back-pressured against and a retry would hide the reason. The other
//! **twenty-eight** use the retrying offer.
//!
//! And the retry does not retry everything. Three results are fatal on the spot
//! (`aeron_archive_proxy.c:1069-1075`): a closed publication, one with no
//! subscriber, and one at its maximum position are not conditions that waiting
//! improves. What is left — back pressure, and the driver asking for an
//! administrative action — is what the retry is for.
//!
//! # One buffer, and what happens when a request outgrows it
//!
//! The reference writes every request into one 8 KiB buffer the proxy owns
//! (`AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH`). This does the same, and the
//! same thing happens at the edge: the generated C encoders check their limit
//! and return `-1`, while the generated Rust encoders **panic**. A channel
//! string long enough to overflow 8 KiB is a caller's bug either way, but it
//! fails loudly here and quietly there — recorded rather than papered over,
//! because wrapping every encoder in a length check would cost more than the
//! panic is worth.

use deepmsg_client::client::Client;
use deepmsg_codec::archive::WriteBuf;
use deepmsg_codec::archive::archive_id_request_codec::ArchiveIdRequestEncoder;
use deepmsg_codec::archive::attach_segments_request_codec::AttachSegmentsRequestEncoder;
use deepmsg_codec::archive::auth_connect_request_codec::AuthConnectRequestEncoder;
use deepmsg_codec::archive::boolean_type::BooleanType;
use deepmsg_codec::archive::bounded_replay_request_codec::BoundedReplayRequestEncoder;
use deepmsg_codec::archive::challenge_response_codec::ChallengeResponseEncoder;
use deepmsg_codec::archive::close_session_request_codec::CloseSessionRequestEncoder;
use deepmsg_codec::archive::delete_detached_segments_request_codec::DeleteDetachedSegmentsRequestEncoder;
use deepmsg_codec::archive::detach_segments_request_codec::DetachSegmentsRequestEncoder;
use deepmsg_codec::archive::extend_recording_request_2_codec::ExtendRecordingRequest2Encoder;
use deepmsg_codec::archive::find_last_matching_recording_request_codec::FindLastMatchingRecordingRequestEncoder;
use deepmsg_codec::archive::list_recording_request_codec::ListRecordingRequestEncoder;
use deepmsg_codec::archive::list_recording_subscriptions_request_codec::ListRecordingSubscriptionsRequestEncoder;
use deepmsg_codec::archive::list_recordings_for_uri_request_codec::ListRecordingsForUriRequestEncoder;
use deepmsg_codec::archive::list_recordings_request_codec::ListRecordingsRequestEncoder;
use deepmsg_codec::archive::max_recorded_position_request_codec::MaxRecordedPositionRequestEncoder;
use deepmsg_codec::archive::message_header_codec;
use deepmsg_codec::archive::migrate_segments_request_codec::MigrateSegmentsRequestEncoder;
use deepmsg_codec::archive::purge_recording_request_codec::PurgeRecordingRequestEncoder;
use deepmsg_codec::archive::purge_segments_request_codec::PurgeSegmentsRequestEncoder;
use deepmsg_codec::archive::recording_position_request_codec::RecordingPositionRequestEncoder;
use deepmsg_codec::archive::replay_request_codec::ReplayRequestEncoder;
use deepmsg_codec::archive::replay_token_request_codec::ReplayTokenRequestEncoder;
use deepmsg_codec::archive::replicate_request_2_codec::ReplicateRequest2Encoder;
use deepmsg_codec::archive::source_location::SourceLocation;
use deepmsg_codec::archive::start_position_request_codec::StartPositionRequestEncoder;
use deepmsg_codec::archive::start_recording_request_2_codec::StartRecordingRequest2Encoder;
use deepmsg_codec::archive::stop_all_replays_request_codec::StopAllReplaysRequestEncoder;
use deepmsg_codec::archive::stop_position_request_codec::StopPositionRequestEncoder;
use deepmsg_codec::archive::stop_recording_by_identity_request_codec::StopRecordingByIdentityRequestEncoder;
use deepmsg_codec::archive::stop_recording_request_codec::StopRecordingRequestEncoder;
use deepmsg_codec::archive::stop_recording_subscription_request_codec::StopRecordingSubscriptionRequestEncoder;
use deepmsg_codec::archive::stop_replay_request_codec::StopReplayRequestEncoder;
use deepmsg_codec::archive::stop_replication_request_codec::StopReplicationRequestEncoder;
use deepmsg_codec::archive::truncate_recording_request_codec::TruncateRecordingRequestEncoder;
use deepmsg_codec::archive::update_channel_request_codec::UpdateChannelRequestEncoder;
use deepmsg_core::logbuffer::append::Appended;
use deepmsg_core::version::{BUILD_IDENTITY, COMPAT_VERSION_TEXT};

use crate::client::context::ArchiveContext;
use crate::server::response_proxy::PROTOCOL_SEMANTIC_VERSION;

/// `AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH` (`aeron_archive_proxy.h:26`):
/// the one buffer every request is built in.
pub const REQUEST_BUFFER_LENGTH: usize = 8 * 1024;

/// `AERON_NULL_VALUE` (`aeronc.h:30`): what `control_session_id` is before an
/// archive has named one, and what every optional field here is set to.
pub const NULL_VALUE: i64 = -1;

/// `AERON_NULL_COUNTER_ID` (`aeronc.h:896`), which `AERON_NULL_VALUE` happens
/// to equal — and which is the value that says a replay is **not** bounded.
pub const NULL_COUNTER_ID: i32 = -1;

/// What a caller may ask a replay for
/// (`aeron_archive_replay_params_t`, `aeron_archive.h:87-121`).
///
/// Every field defaults to `-1`, which is each one's "unset": position and
/// length of `-1` mean "from the start" and "all of it", and a bounding counter
/// of `-1` means the replay is not bounded at all — which is the one default
/// that changes *which template is sent*, not just a field in it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplayParams {
    /// The counter to bound the replay by. Anything but `-1` sends the bounded
    /// form of the request.
    pub bounding_limit_counter_id: i32,
    /// How much of a file to read at once.
    pub file_io_max_length: i32,
    /// Where in the recording to start.
    pub position: i64,
    /// How much of it to replay; `i64::MAX` follows a live recording.
    pub length: i64,
    /// For a replay driven by an image that is not this session's.
    pub replay_token: i64,
    /// Used on the **client** side for response channels, and never sent by the
    /// proxy — carried because it is part of what a caller fills in.
    pub subscription_registration_id: i64,
}

impl Default for ReplayParams {
    /// `aeron_archive_replay_params_init` (`aeron_archive_replay_params.c:19-30`).
    fn default() -> Self {
        Self {
            bounding_limit_counter_id: NULL_COUNTER_ID,
            file_io_max_length: NULL_VALUE as i32,
            position: NULL_VALUE,
            length: NULL_VALUE,
            replay_token: NULL_VALUE,
            subscription_registration_id: NULL_VALUE,
        }
    }
}

impl ReplayParams {
    /// `aeron_archive_replay_params_is_bounded`
    /// (`aeron_archive_replay_params.c:43-46`): a counter id that is not the
    /// null one.
    #[must_use]
    pub const fn is_bounded(&self) -> bool {
        NULL_COUNTER_ID != self.bounding_limit_counter_id
    }
}

/// What a caller may ask a replication for
/// (`aeron_archive_replication_params_t`, `aeron_archive.h:134-183`).
///
/// A `-1` is "not said", and for the destination that is a decision rather than
/// a default: `dst_recording_id` of `-1` says *create* a recording at the
/// destination instead of extending one. An empty string is the same idea for
/// the channels a replication could use.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicationParams {
    /// Where to stop; `-1` replicates continuously.
    pub stop_position: i64,
    /// The destination recording to extend; `-1` makes a new one.
    pub dst_recording_id: i64,
    /// Where a live stream should be merged to, if one is.
    pub live_destination: String,
    /// The channel to replicate over; empty uses the context's default.
    pub replication_channel: String,
    /// The source archive's control address, for a replication driven over
    /// response channels.
    pub src_response_channel: String,
    /// A tag for the channel the destination subscribes on.
    pub channel_tag_id: i64,
    /// A tag for that subscription.
    pub subscription_tag_id: i64,
    /// How much of a file to read at once.
    pub file_io_max_length: i32,
    /// The session id the copy should use at the destination, for replicating
    /// one recording through several stages.
    pub replication_session_id: i32,
    /// Credentials for the source archive, if it wants any.
    pub encoded_credentials: Vec<u8>,
}

impl Default for ReplicationParams {
    /// A **trap avoided rather than kept**: a derived [`Default`] here would be
    /// all-nought, and nought is a *meaning* in two of these fields — a
    /// `dst_recording_id` of nought asks the destination to extend a recording
    /// that does not exist, where `-1` asks it to make one. So this is not
    /// derived, and it is not a second set of defaults beside
    /// [`ReplicationParams::new`] either; it is the same one.
    fn default() -> Self {
        Self::new()
    }
}

impl ReplicationParams {
    /// `aeron_archive_replication_params_init`
    /// (`aeron_archive_replication_params.c:19-33`).
    #[must_use]
    pub fn new() -> Self {
        Self {
            stop_position: NULL_VALUE,
            dst_recording_id: NULL_VALUE,
            live_destination: String::new(),
            replication_channel: String::new(),
            src_response_channel: String::new(),
            channel_tag_id: NULL_VALUE,
            subscription_tag_id: NULL_VALUE,
            file_io_max_length: NULL_VALUE as i32,
            replication_session_id: NULL_VALUE as i32,
            encoded_credentials: Vec::new(),
        }
    }
}

/// A client's requests go out through this, on one exclusive publication.
///
/// The publication is a [`Client`]'s, addressed by registration id, and the
/// client is an argument to every request rather than a field — the same split
/// [`ArchiveContext::conclude_with`] makes, and for the same reason: a proxy is
/// built where there is no client yet, and the client outlives it.
#[derive(Debug, Clone)]
pub struct ArchiveProxy {
    /// `"name=… version=… commit=…"`, built once (`:113-127`).
    client_info: String,
    /// The archive's control session, set by `async_connect` — see the module
    /// note. [`NULL_VALUE`] until then, and that is what goes on the wire.
    control_session_id: i64,
    /// How many times a retrying offer tries (`:1063`).
    retry_attempts: u32,
    /// The exclusive publication requests are offered on.
    request_publication: i64,
    /// The 8 KiB every request is written into, kept across requests.
    buffer: Vec<u8>,
}

impl ArchiveProxy {
    /// A proxy over `request_publication`, configured from `context`.
    ///
    /// `aeron_archive_proxy_init` (`:95-129`). The retry count is the context's,
    /// which is what every call site of the reference's `create` passes.
    #[must_use]
    pub fn new(context: &ArchiveContext, request_publication: i64) -> Self {
        Self {
            // The two halves are this build's, and they are the ones it writes
            // into a counter label anywhere else: the version it is compatible
            // with, and a token naming the build. The reference puts its own
            // `AERON_VERSION_TXT` and git sha here; `docs/compat.md` records the
            // difference, and it is a difference in a *string an archive stores*
            // rather than one it acts on.
            client_info: format!(
                "name={} version={} commit={}",
                context.client_name, COMPAT_VERSION_TEXT, BUILD_IDENTITY
            ),
            control_session_id: NULL_VALUE,
            retry_attempts: context.message_retry_attempts,
            request_publication,
            buffer: vec![0; REQUEST_BUFFER_LENGTH],
        }
    }

    /// The session an archive named for this client.
    #[must_use]
    pub const fn control_session_id(&self) -> i64 {
        self.control_session_id
    }

    /// Record it, which only a `ConnectResponse` may do (`:120-126`).
    pub const fn set_control_session_id(&mut self, control_session_id: i64) {
        self.control_session_id = control_session_id;
    }

    /// What this client calls itself.
    #[must_use]
    pub fn client_info(&self) -> &str {
        &self.client_info
    }

    /// The publication requests are offered on.
    #[must_use]
    pub const fn request_publication(&self) -> i64 {
        self.request_publication
    }

    // -----------------------------------------------------------------------
    // The four that do not retry
    // -----------------------------------------------------------------------

    /// Ask to be let in: `AuthConnectRequest` (`:161-181`).
    ///
    /// The first request a client sends. `response_channel` is where it wants
    /// to be answered and `response_stream_id` is the stream on it;
    /// `credentials` is empty when the client has none, which the reference
    /// spells as a null pointer and a zero length.
    pub fn try_connect(
        &mut self,
        client: &Client,
        correlation_id: i64,
        response_channel: &str,
        response_stream_id: i32,
        credentials: &[u8],
    ) -> Result<(), ProxyError> {
        let length = {
            let encoder = AuthConnectRequestEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer),
                message_header_codec::ENCODED_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut encoder = header.parent().expect("the encoder the header wrapped");

            encoder
                .correlation_id(correlation_id)
                .response_stream_id(response_stream_id)
                // The *protocol*'s version, not this build's: the archive
                // compares its major against its own
                // (`ArchiveConductor.java:483-488`).
                .version(PROTOCOL_SEMANTIC_VERSION)
                .response_channel(response_channel.as_bytes())
                .encoded_credentials(credentials)
                .client_info(self.client_info.as_bytes());

            message_header_codec::ENCODED_LENGTH + encoder.encoded_length()
        };

        self.offer_once(client, length)
    }

    /// Ask which archive this is: `ArchiveIdRequest` (`:183-201`).
    pub fn archive_id(&mut self, client: &Client, correlation_id: i64) -> Result<(), ProxyError> {
        let length = {
            let encoder = ArchiveIdRequestEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer),
                message_header_codec::ENCODED_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut encoder = header.parent().expect("the encoder the header wrapped");

            encoder
                .control_session_id(self.control_session_id)
                .correlation_id(correlation_id);

            message_header_codec::ENCODED_LENGTH + encoder.encoded_length()
        };

        self.offer_once(client, length)
    }

    /// Answer a challenge: `ChallengeResponse` (`:203-227`).
    ///
    /// The second half of a connect that was answered with a challenge rather
    /// than an acceptance.
    pub fn challenge_response(
        &mut self,
        client: &Client,
        correlation_id: i64,
        credentials: &[u8],
    ) -> Result<(), ProxyError> {
        let length = {
            let encoder = ChallengeResponseEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer),
                message_header_codec::ENCODED_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut encoder = header.parent().expect("the encoder the header wrapped");

            encoder
                .control_session_id(self.control_session_id)
                .correlation_id(correlation_id)
                .encoded_credentials(credentials);

            message_header_codec::ENCODED_LENGTH + encoder.encoded_length()
        };

        self.offer_once(client, length)
    }

    /// Leave: `CloseSessionRequest` (`:229-245`).
    ///
    /// The only request with **no correlation id** — there is nothing left to
    /// correlate it with once the session is closing — and one of the four that
    /// do not retry, because a retry would ask a session to close twice.
    pub fn close_session(&mut self, client: &Client) -> Result<(), ProxyError> {
        let length = {
            let encoder = CloseSessionRequestEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer),
                message_header_codec::ENCODED_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut encoder = header.parent().expect("the encoder the header wrapped");

            encoder.control_session_id(self.control_session_id);

            message_header_codec::ENCODED_LENGTH + encoder.encoded_length()
        };

        self.offer_once(client, length)
    }

    // -----------------------------------------------------------------------
    // The twenty-eight that retry
    // -----------------------------------------------------------------------

    /// Start a recording: `StartRecordingRequest2` (`:247-281`).
    #[allow(clippy::too_many_arguments)]
    pub fn start_recording(
        &mut self,
        client: &Client,
        correlation_id: i64,
        recording_channel: &str,
        recording_stream_id: i32,
        local_source: bool,
        auto_stop: bool,
    ) -> Result<(), ProxyError> {
        let length = {
            let encoder = StartRecordingRequest2Encoder::default().wrap(
                WriteBuf::new(&mut self.buffer),
                message_header_codec::ENCODED_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut encoder = header.parent().expect("the encoder the header wrapped");

            encoder
                .control_session_id(self.control_session_id)
                .correlation_id(correlation_id)
                .stream_id(recording_stream_id)
                .source_location(source_location(local_source))
                .auto_stop(boolean(auto_stop))
                .channel(recording_channel.as_bytes());

            message_header_codec::ENCODED_LENGTH + encoder.encoded_length()
        };

        self.offer(client, length)
    }

    /// Where a recording has got to: `RecordingPositionRequest` (`:283-304`).
    pub fn get_recording_position(
        &mut self,
        client: &Client,
        correlation_id: i64,
        recording_id: i64,
    ) -> Result<(), ProxyError> {
        let length = {
            let encoder = RecordingPositionRequestEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer),
                message_header_codec::ENCODED_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut encoder = header.parent().expect("the encoder the header wrapped");

            encoder
                .control_session_id(self.control_session_id)
                .correlation_id(correlation_id)
                .recording_id(recording_id);

            message_header_codec::ENCODED_LENGTH + encoder.encoded_length()
        };

        self.offer(client, length)
    }

    /// Where a recording begins: `StartPositionRequest` (`:306-327`).
    pub fn get_start_position(
        &mut self,
        client: &Client,
        correlation_id: i64,
        recording_id: i64,
    ) -> Result<(), ProxyError> {
        let length = {
            let encoder = StartPositionRequestEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer),
                message_header_codec::ENCODED_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut encoder = header.parent().expect("the encoder the header wrapped");

            encoder
                .control_session_id(self.control_session_id)
                .correlation_id(correlation_id)
                .recording_id(recording_id);

            message_header_codec::ENCODED_LENGTH + encoder.encoded_length()
        };

        self.offer(client, length)
    }

    /// Where a recording ends: `StopPositionRequest` (`:329-350`).
    pub fn get_stop_position(
        &mut self,
        client: &Client,
        correlation_id: i64,
        recording_id: i64,
    ) -> Result<(), ProxyError> {
        let length = {
            let encoder = StopPositionRequestEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer),
                message_header_codec::ENCODED_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut encoder = header.parent().expect("the encoder the header wrapped");

            encoder
                .control_session_id(self.control_session_id)
                .correlation_id(correlation_id)
                .recording_id(recording_id);

            message_header_codec::ENCODED_LENGTH + encoder.encoded_length()
        };

        self.offer(client, length)
    }

    /// The furthest a recording ever reached: `MaxRecordedPositionRequest`
    /// (`:352-373`).
    ///
    /// Not the stop position: a recording still going has one of those too, and
    /// this is the one that does not move backwards.
    pub fn get_max_recorded_position(
        &mut self,
        client: &Client,
        correlation_id: i64,
        recording_id: i64,
    ) -> Result<(), ProxyError> {
        let length = {
            let encoder = MaxRecordedPositionRequestEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer),
                message_header_codec::ENCODED_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut encoder = header.parent().expect("the encoder the header wrapped");

            encoder
                .control_session_id(self.control_session_id)
                .correlation_id(correlation_id)
                .recording_id(recording_id);

            message_header_codec::ENCODED_LENGTH + encoder.encoded_length()
        };

        self.offer(client, length)
    }

    /// Stop a recording by the channel and stream it was started with:
    /// `StopRecordingRequest` (`:375-401`).
    pub fn stop_recording(
        &mut self,
        client: &Client,
        correlation_id: i64,
        channel: &str,
        stream_id: i32,
    ) -> Result<(), ProxyError> {
        let length = {
            let encoder = StopRecordingRequestEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer),
                message_header_codec::ENCODED_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut encoder = header.parent().expect("the encoder the header wrapped");

            encoder
                .control_session_id(self.control_session_id)
                .correlation_id(correlation_id)
                .stream_id(stream_id)
                .channel(channel.as_bytes());

            message_header_codec::ENCODED_LENGTH + encoder.encoded_length()
        };

        self.offer(client, length)
    }

    /// Stop one recording subscription:
    /// `StopRecordingSubscriptionRequest` (`:403-424`).
    pub fn stop_recording_subscription(
        &mut self,
        client: &Client,
        correlation_id: i64,
        subscription_id: i64,
    ) -> Result<(), ProxyError> {
        let length = {
            let encoder = StopRecordingSubscriptionRequestEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer),
                message_header_codec::ENCODED_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut encoder = header.parent().expect("the encoder the header wrapped");

            encoder
                .control_session_id(self.control_session_id)
                .correlation_id(correlation_id)
                .subscription_id(subscription_id);

            message_header_codec::ENCODED_LENGTH + encoder.encoded_length()
        };

        self.offer(client, length)
    }

    /// Stop a recording by its id: `StopRecordingByIdentityRequest` (`:426-447`).
    pub fn stop_recording_by_identity(
        &mut self,
        client: &Client,
        correlation_id: i64,
        recording_id: i64,
    ) -> Result<(), ProxyError> {
        let length = {
            let encoder = StopRecordingByIdentityRequestEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer),
                message_header_codec::ENCODED_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut encoder = header.parent().expect("the encoder the header wrapped");

            encoder
                .control_session_id(self.control_session_id)
                .correlation_id(correlation_id)
                .recording_id(recording_id);

            message_header_codec::ENCODED_LENGTH + encoder.encoded_length()
        };

        self.offer(client, length)
    }

    /// The last recording matching a description:
    /// `FindLastMatchingRecordingRequest` (`:449-479`).
    ///
    /// **Searches backwards**, which is what `min_recording_id` is for: the
    /// answer is the newest recording at or above that id.
    #[allow(clippy::too_many_arguments)]
    pub fn find_last_matching_recording(
        &mut self,
        client: &Client,
        correlation_id: i64,
        min_recording_id: i64,
        channel_fragment: &str,
        stream_id: i32,
        session_id: i32,
    ) -> Result<(), ProxyError> {
        let length = {
            let encoder = FindLastMatchingRecordingRequestEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer),
                message_header_codec::ENCODED_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut encoder = header.parent().expect("the encoder the header wrapped");

            encoder
                .control_session_id(self.control_session_id)
                .correlation_id(correlation_id)
                .min_recording_id(min_recording_id)
                .session_id(session_id)
                .stream_id(stream_id)
                .channel(channel_fragment.as_bytes());

            message_header_codec::ENCODED_LENGTH + encoder.encoded_length()
        };

        self.offer(client, length)
    }

    /// One recording's descriptor: `ListRecordingRequest` (`:481-502`).
    pub fn list_recording(
        &mut self,
        client: &Client,
        correlation_id: i64,
        recording_id: i64,
    ) -> Result<(), ProxyError> {
        let length = {
            let encoder = ListRecordingRequestEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer),
                message_header_codec::ENCODED_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut encoder = header.parent().expect("the encoder the header wrapped");

            encoder
                .control_session_id(self.control_session_id)
                .correlation_id(correlation_id)
                .recording_id(recording_id);

            message_header_codec::ENCODED_LENGTH + encoder.encoded_length()
        };

        self.offer(client, length)
    }

    /// A page of descriptors: `ListRecordingsRequest` (`:504-527`).
    ///
    /// `from_recording_id` is a **cursor, not a page number** — the id the
    /// answer should start after — which is what lets a listing survive
    /// recordings being deleted underneath it.
    pub fn list_recordings(
        &mut self,
        client: &Client,
        correlation_id: i64,
        from_recording_id: i64,
        record_count: i32,
    ) -> Result<(), ProxyError> {
        let length = {
            let encoder = ListRecordingsRequestEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer),
                message_header_codec::ENCODED_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut encoder = header.parent().expect("the encoder the header wrapped");

            encoder
                .control_session_id(self.control_session_id)
                .correlation_id(correlation_id)
                .from_recording_id(from_recording_id)
                .record_count(record_count);

            message_header_codec::ENCODED_LENGTH + encoder.encoded_length()
        };

        self.offer(client, length)
    }

    /// A page of descriptors for one channel and stream:
    /// `ListRecordingsForUriRequest` (`:529-559`).
    #[allow(clippy::too_many_arguments)]
    pub fn list_recordings_for_uri(
        &mut self,
        client: &Client,
        correlation_id: i64,
        from_recording_id: i64,
        record_count: i32,
        channel_fragment: &str,
        stream_id: i32,
    ) -> Result<(), ProxyError> {
        let length = {
            let encoder = ListRecordingsForUriRequestEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer),
                message_header_codec::ENCODED_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut encoder = header.parent().expect("the encoder the header wrapped");

            encoder
                .control_session_id(self.control_session_id)
                .correlation_id(correlation_id)
                .from_recording_id(from_recording_id)
                .record_count(record_count)
                .stream_id(stream_id)
                .channel(channel_fragment.as_bytes());

            message_header_codec::ENCODED_LENGTH + encoder.encoded_length()
        };

        self.offer(client, length)
    }

    /// Ask for a recording to be sent back: `BoundedReplayRequest` or
    /// `ReplayRequest` (`:561-635`).
    ///
    /// **Two templates and one function**, and which is sent is decided by the
    /// parameters: naming a bounding limit counter asks for the bounded form
    /// (`aeron_archive_replay_params_is_bounded`,
    /// `aeron_archive_replay_params.c:43-46`), which is how a replay is kept
    /// from outrunning a consumer that named a counter to bound it by.
    /// Everything else is identical between the two, and that is the point: the
    /// same replay either way, with one field more.
    pub fn replay(
        &mut self,
        client: &Client,
        correlation_id: i64,
        recording_id: i64,
        replay_channel: &str,
        replay_stream_id: i32,
        params: &ReplayParams,
    ) -> Result<(), ProxyError> {
        let control_session_id = self.control_session_id;

        let length = if params.is_bounded() {
            let encoder = BoundedReplayRequestEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer),
                message_header_codec::ENCODED_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut encoder = header.parent().expect("the encoder the header wrapped");

            encoder
                .control_session_id(control_session_id)
                .correlation_id(correlation_id)
                .recording_id(recording_id)
                .position(params.position)
                .length(params.length)
                .limit_counter_id(params.bounding_limit_counter_id)
                .replay_stream_id(replay_stream_id)
                .file_io_max_length(params.file_io_max_length)
                .replay_token(params.replay_token)
                .replay_channel(replay_channel.as_bytes());

            message_header_codec::ENCODED_LENGTH + encoder.encoded_length()
        } else {
            let encoder = ReplayRequestEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer),
                message_header_codec::ENCODED_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut encoder = header.parent().expect("the encoder the header wrapped");

            encoder
                .control_session_id(control_session_id)
                .correlation_id(correlation_id)
                .recording_id(recording_id)
                .position(params.position)
                .length(params.length)
                .replay_stream_id(replay_stream_id)
                .file_io_max_length(params.file_io_max_length)
                .replay_token(params.replay_token)
                .replay_channel(replay_channel.as_bytes());

            message_header_codec::ENCODED_LENGTH + encoder.encoded_length()
        };

        self.offer(client, length)
    }

    /// Cut a recording short: `TruncateRecordingRequest` (`:637-660`).
    pub fn truncate_recording(
        &mut self,
        client: &Client,
        correlation_id: i64,
        recording_id: i64,
        position: i64,
    ) -> Result<(), ProxyError> {
        let length = {
            let encoder = TruncateRecordingRequestEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer),
                message_header_codec::ENCODED_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut encoder = header.parent().expect("the encoder the header wrapped");

            encoder
                .control_session_id(self.control_session_id)
                .correlation_id(correlation_id)
                .recording_id(recording_id)
                .position(position);

            message_header_codec::ENCODED_LENGTH + encoder.encoded_length()
        };

        self.offer(client, length)
    }

    /// Stop one replay: `StopReplayRequest` (`:662-683`).
    ///
    /// By **replay session id**, which is not the recording id: two replays of
    /// one recording are two sessions, and only one of them stops.
    pub fn stop_replay(
        &mut self,
        client: &Client,
        correlation_id: i64,
        replay_session_id: i64,
    ) -> Result<(), ProxyError> {
        let length = {
            let encoder = StopReplayRequestEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer),
                message_header_codec::ENCODED_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut encoder = header.parent().expect("the encoder the header wrapped");

            encoder
                .control_session_id(self.control_session_id)
                .correlation_id(correlation_id)
                .replay_session_id(replay_session_id);

            message_header_codec::ENCODED_LENGTH + encoder.encoded_length()
        };

        self.offer(client, length)
    }

    /// Stop every replay of one recording: `StopAllReplaysRequest` (`:685-706`).
    pub fn stop_all_replays(
        &mut self,
        client: &Client,
        correlation_id: i64,
        recording_id: i64,
    ) -> Result<(), ProxyError> {
        let length = {
            let encoder = StopAllReplaysRequestEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer),
                message_header_codec::ENCODED_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut encoder = header.parent().expect("the encoder the header wrapped");

            encoder
                .control_session_id(self.control_session_id)
                .correlation_id(correlation_id)
                .recording_id(recording_id);

            message_header_codec::ENCODED_LENGTH + encoder.encoded_length()
        };

        self.offer(client, length)
    }

    /// A page of recording subscriptions:
    /// `ListRecordingSubscriptionsRequest` (`:708-755`).
    ///
    /// `apply_stream_id` says whether `stream_id` is a filter or a value to
    /// ignore — one field does both, which is why it travels next to it.
    #[allow(clippy::too_many_arguments)]
    pub fn list_recording_subscriptions(
        &mut self,
        client: &Client,
        correlation_id: i64,
        pseudo_index: i32,
        subscription_count: i32,
        channel_fragment: &str,
        stream_id: i32,
        apply_stream_id: bool,
    ) -> Result<(), ProxyError> {
        let length = {
            let encoder = ListRecordingSubscriptionsRequestEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer),
                message_header_codec::ENCODED_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut encoder = header.parent().expect("the encoder the header wrapped");

            encoder
                .control_session_id(self.control_session_id)
                .correlation_id(correlation_id)
                .pseudo_index(pseudo_index)
                .subscription_count(subscription_count)
                .apply_stream_id(boolean(apply_stream_id))
                .stream_id(stream_id)
                .channel(channel_fragment.as_bytes());

            message_header_codec::ENCODED_LENGTH + encoder.encoded_length()
        };

        self.offer(client, length)
    }

    /// Delete a recording: `PurgeRecordingRequest` (`:757-778`).
    pub fn purge_recording(
        &mut self,
        client: &Client,
        correlation_id: i64,
        recording_id: i64,
    ) -> Result<(), ProxyError> {
        let length = {
            let encoder = PurgeRecordingRequestEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer),
                message_header_codec::ENCODED_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut encoder = header.parent().expect("the encoder the header wrapped");

            encoder
                .control_session_id(self.control_session_id)
                .correlation_id(correlation_id)
                .recording_id(recording_id);

            message_header_codec::ENCODED_LENGTH + encoder.encoded_length()
        };

        self.offer(client, length)
    }

    /// Grow an existing recording: `ExtendRecordingRequest2` (`:780-812`).
    ///
    /// Note the **argument order**: the reference takes `recording_id` before
    /// the channel here and `correlation_id` last, the reverse of most of this
    /// file. Kept, because a caller reading the C signature is the caller this
    /// is for.
    #[allow(clippy::too_many_arguments)]
    pub fn extend_recording(
        &mut self,
        client: &Client,
        recording_id: i64,
        recording_channel: &str,
        recording_stream_id: i32,
        local_source: bool,
        auto_stop: bool,
        correlation_id: i64,
    ) -> Result<(), ProxyError> {
        let length = {
            let encoder = ExtendRecordingRequest2Encoder::default().wrap(
                WriteBuf::new(&mut self.buffer),
                message_header_codec::ENCODED_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut encoder = header.parent().expect("the encoder the header wrapped");

            encoder
                .control_session_id(self.control_session_id)
                .correlation_id(correlation_id)
                .recording_id(recording_id)
                .stream_id(recording_stream_id)
                .source_location(source_location(local_source))
                .auto_stop(boolean(auto_stop))
                .channel(recording_channel.as_bytes());

            message_header_codec::ENCODED_LENGTH + encoder.encoded_length()
        };

        self.offer(client, length)
    }

    /// Ask this archive to replicate a recording from another:
    /// `ReplicateRequest2` (`:814-861`).
    ///
    /// The most fields of any request here, and the one S6 is built on: the
    /// source archive is named by a control channel and a stream, and everything
    /// else is where the copy should land and how.
    pub fn replicate(
        &mut self,
        client: &Client,
        correlation_id: i64,
        src_recording_id: i64,
        src_control_stream_id: i32,
        src_control_channel: &str,
        params: &ReplicationParams,
    ) -> Result<(), ProxyError> {
        let length = {
            let encoder = ReplicateRequest2Encoder::default().wrap(
                WriteBuf::new(&mut self.buffer),
                message_header_codec::ENCODED_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut encoder = header.parent().expect("the encoder the header wrapped");

            encoder
                .control_session_id(self.control_session_id)
                .correlation_id(correlation_id)
                .src_recording_id(src_recording_id)
                .dst_recording_id(params.dst_recording_id)
                .stop_position(params.stop_position)
                .channel_tag_id(params.channel_tag_id)
                .subscription_tag_id(params.subscription_tag_id)
                .src_control_stream_id(src_control_stream_id)
                .file_io_max_length(params.file_io_max_length)
                .replication_session_id(params.replication_session_id)
                .src_control_channel(src_control_channel.as_bytes())
                .live_destination(params.live_destination.as_bytes())
                .replication_channel(params.replication_channel.as_bytes())
                .encoded_credentials(&params.encoded_credentials)
                .src_response_channel(params.src_response_channel.as_bytes());

            message_header_codec::ENCODED_LENGTH + encoder.encoded_length()
        };

        self.offer(client, length)
    }

    /// Stop one replication: `StopReplicationRequest` (`:863-884`).
    pub fn stop_replication(
        &mut self,
        client: &Client,
        correlation_id: i64,
        replication_id: i64,
    ) -> Result<(), ProxyError> {
        let length = {
            let encoder = StopReplicationRequestEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer),
                message_header_codec::ENCODED_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut encoder = header.parent().expect("the encoder the header wrapped");

            encoder
                .control_session_id(self.control_session_id)
                .correlation_id(correlation_id)
                .replication_id(replication_id);

            message_header_codec::ENCODED_LENGTH + encoder.encoded_length()
        };

        self.offer(client, length)
    }

    /// Ask for the token that lets a *different* image drive a replay:
    /// `ReplayTokenRequest` (`:886-908`).
    pub fn request_replay_token(
        &mut self,
        client: &Client,
        correlation_id: i64,
        recording_id: i64,
    ) -> Result<(), ProxyError> {
        let length = {
            let encoder = ReplayTokenRequestEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer),
                message_header_codec::ENCODED_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut encoder = header.parent().expect("the encoder the header wrapped");

            encoder
                .control_session_id(self.control_session_id)
                .correlation_id(correlation_id)
                .recording_id(recording_id);

            message_header_codec::ENCODED_LENGTH + encoder.encoded_length()
        };

        self.offer(client, length)
    }

    /// Detach a recording's segments from a position on:
    /// `DetachSegmentsRequest` (`:910-934`).
    pub fn detach_segments(
        &mut self,
        client: &Client,
        correlation_id: i64,
        recording_id: i64,
        new_start_position: i64,
    ) -> Result<(), ProxyError> {
        let length = {
            let encoder = DetachSegmentsRequestEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer),
                message_header_codec::ENCODED_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut encoder = header.parent().expect("the encoder the header wrapped");

            encoder
                .control_session_id(self.control_session_id)
                .correlation_id(correlation_id)
                .recording_id(recording_id)
                .new_start_position(new_start_position);

            message_header_codec::ENCODED_LENGTH + encoder.encoded_length()
        };

        self.offer(client, length)
    }

    /// Delete the segments a detach left behind:
    /// `DeleteDetachedSegmentsRequest` (`:936-957`).
    ///
    /// The second half of a two-step delete, and the only request here that
    /// takes no position: what it deletes is what the detach named.
    pub fn delete_detached_segments(
        &mut self,
        client: &Client,
        correlation_id: i64,
        recording_id: i64,
    ) -> Result<(), ProxyError> {
        let length = {
            let encoder = DeleteDetachedSegmentsRequestEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer),
                message_header_codec::ENCODED_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut encoder = header.parent().expect("the encoder the header wrapped");

            encoder
                .control_session_id(self.control_session_id)
                .correlation_id(correlation_id)
                .recording_id(recording_id);

            message_header_codec::ENCODED_LENGTH + encoder.encoded_length()
        };

        self.offer(client, length)
    }

    /// Delete a recording's segments from a position on, in one step:
    /// `PurgeSegmentsRequest` (`:959-983`).
    pub fn purge_segments(
        &mut self,
        client: &Client,
        correlation_id: i64,
        recording_id: i64,
        new_start_position: i64,
    ) -> Result<(), ProxyError> {
        let length = {
            let encoder = PurgeSegmentsRequestEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer),
                message_header_codec::ENCODED_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut encoder = header.parent().expect("the encoder the header wrapped");

            encoder
                .control_session_id(self.control_session_id)
                .correlation_id(correlation_id)
                .recording_id(recording_id)
                .new_start_position(new_start_position);

            message_header_codec::ENCODED_LENGTH + encoder.encoded_length()
        };

        self.offer(client, length)
    }

    /// Take detached segments back: `AttachSegmentsRequest` (`:985-1006`).
    pub fn attach_segments(
        &mut self,
        client: &Client,
        correlation_id: i64,
        recording_id: i64,
    ) -> Result<(), ProxyError> {
        let length = {
            let encoder = AttachSegmentsRequestEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer),
                message_header_codec::ENCODED_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut encoder = header.parent().expect("the encoder the header wrapped");

            encoder
                .control_session_id(self.control_session_id)
                .correlation_id(correlation_id)
                .recording_id(recording_id);

            message_header_codec::ENCODED_LENGTH + encoder.encoded_length()
        };

        self.offer(client, length)
    }

    /// Renumber one recording's segments into another's:
    /// `MigrateSegmentsRequest` (`:1008-1030`).
    pub fn migrate_segments(
        &mut self,
        client: &Client,
        correlation_id: i64,
        src_recording_id: i64,
        dst_recording_id: i64,
    ) -> Result<(), ProxyError> {
        let length = {
            let encoder = MigrateSegmentsRequestEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer),
                message_header_codec::ENCODED_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut encoder = header.parent().expect("the encoder the header wrapped");

            encoder
                .control_session_id(self.control_session_id)
                .correlation_id(correlation_id)
                .src_recording_id(src_recording_id)
                .dst_recording_id(dst_recording_id);

            message_header_codec::ENCODED_LENGTH + encoder.encoded_length()
        };

        self.offer(client, length)
    }

    /// Change the channel a recording is described by:
    /// `UpdateChannelRequest` (`:1032-1043`).
    ///
    /// A descriptor records the channel as a string; this rewrites one, which is
    /// what a recording made under an alias that has since changed needs.
    pub fn update_channel(
        &mut self,
        client: &Client,
        correlation_id: i64,
        recording_id: i64,
        new_channel: &str,
    ) -> Result<(), ProxyError> {
        let length = {
            let encoder = UpdateChannelRequestEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer),
                message_header_codec::ENCODED_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut encoder = header.parent().expect("the encoder the header wrapped");

            encoder
                .control_session_id(self.control_session_id)
                .correlation_id(correlation_id)
                .recording_id(recording_id)
                .channel(new_channel.as_bytes());

            message_header_codec::ENCODED_LENGTH + encoder.encoded_length()
        };

        self.offer(client, length)
    }

    // -----------------------------------------------------------------------
    // The two offers
    // -----------------------------------------------------------------------

    /// One attempt, and the result as it came (`:1045-1054`).
    ///
    /// The four handshake requests use this one. `length` is what the encoder
    /// says, **not** counting the header, which is added here.
    fn offer_once(&self, client: &Client, length: usize) -> Result<(), ProxyError> {
        let end = message_header_codec::ENCODED_LENGTH + length;

        match client.offer_exclusive(self.request_publication, &self.buffer[..end]) {
            Some(Appended::Ok { .. }) => Ok(()),
            Some(result) => Err(ProxyError::Offer(result)),
            // This client holds no publication under that registration id, so
            // there is nothing to offer to — the reference's `CLOSED` (-4),
            // which its own `offer_once` would return from a closed handle.
            None => Err(ProxyError::Closed),
        }
    }

    /// Keep trying until `retry_attempts` are used up (`:1056-1087`).
    ///
    /// Every request but the four above uses this one. A result waiting cannot
    /// improve ends the attempt on the spot ([`is_retryable`]); anything else
    /// counts against the attempts, with a turn yielded between them.
    fn offer(&self, client: &Client, length: usize) -> Result<(), ProxyError> {
        let mut attempts = self.retry_attempts;

        loop {
            match self.offer_once(client, length) {
                Ok(()) => return Ok(()),
                Err(error) if !is_retryable(&error) => return Err(error),
                Err(_) => {}
            }

            attempts = attempts.saturating_sub(1);
            if 0 == attempts {
                return Err(ProxyError::TooManyRetries);
            }

            // The reference runs the context's idle strategy here, which
            // defaults to a backoff (`aeron_archive_context.c:420-430`). This
            // context has no idle strategy yet — P2-C1 does not build one — so
            // the turn is yielded, which is what that strategy does first.
            //
            // Nothing can be *driven* here in either implementation: what an
            // offer is refused against is the driver's position limit, and the
            // driver raises it from its own thread.
            std::thread::yield_now();
        }
    }
}

/// SBE's language-independent boolean (`boolean_type::BooleanType`).
fn boolean(value: bool) -> BooleanType {
    if value {
        BooleanType::TRUE
    } else {
        BooleanType::FALSE
    }
}

/// Which side the recording's source is on (`source_location::SourceLocation`).
fn source_location(local_source: bool) -> SourceLocation {
    if local_source {
        SourceLocation::LOCAL
    } else {
        SourceLocation::REMOTE
    }
}

/// Whether waiting can improve an offer (`aeron_archive_proxy.c:1069-1075`).
///
/// The reference gives up on three of its codes and retries the rest, which
/// works out to "give up on a closed publication, one with no subscriber, and
/// one at its maximum position". Spelled here as the states worth coming back
/// from instead — back pressure, and the driver asking for an administrative
/// action — so that a state added to [`Appended`] later is fatal until someone
/// says otherwise rather than retried until the attempts run out.
fn is_retryable(error: &ProxyError) -> bool {
    match error {
        ProxyError::Offer(result) => matches!(
            result,
            Appended::BackPressured | Appended::EndOfLog | Appended::MidRotation
        ),
        ProxyError::Closed | ProxyError::TooManyRetries => false,
    }
}

/// The reference's code for a failure, so a message says which
/// (`aeronc.h:1072-1101`).
///
/// Two of this build's states are one of the reference's: a term that ended
/// under the producer and a term caught mid-rotation are the same thing to a
/// caller, and the reference reports both as `ADMIN_ACTION` — "retry, the
/// rotation will have finished".
fn reference_code(error: &ProxyError) -> i64 {
    match error {
        ProxyError::TooManyRetries => -2,
        ProxyError::Closed => -4,
        ProxyError::Offer(result) => match result {
            Appended::Ok { .. } => 0,
            Appended::NotConnected => -1,
            Appended::BackPressured => -2,
            Appended::EndOfLog | Appended::MidRotation => -3,
            Appended::MaxPositionExceeded => -5,
            // Not one the publication returns: too wide for a frame is refused
            // before the log buffer is reached at all. `AERON_PUBLICATION_ERROR`
            // is the reference's code for exactly that kind of answer.
            Appended::MessageTooLarge | Appended::Malformed => -6,
        },
    }
}

/// Why a request was not published.
#[derive(Debug)]
pub enum ProxyError {
    /// The offer did not land, and this is what the publication said.
    Offer(Appended),
    /// This client holds no such exclusive publication.
    Closed,
    /// Every attempt was refused.
    TooManyRetries,
}

impl core::fmt::Display for ProxyError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        // The reference's own text (`:1081`), which is the whole of it.
        if let Self::TooManyRetries = self {
            return write!(f, "too many retries");
        }

        write!(f, "offer failed with result {}", reference_code(self))
    }
}

impl std::error::Error for ProxyError {}
