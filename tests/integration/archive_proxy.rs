//! The archive client's request proxy, checked one request at a time.
//!
//! Thirty-two functions in `crates/archive/src/client/proxy.rs` are the same
//! three steps with different fields, and the risk that shape carries is a
//! **field mix-up**: thirty-two near-identical encoders is thirty-two chances to
//! put `record_count` where `record_count` was not meant to go, or to reach for
//! `recordingId` in the request that takes `subscriptionId`. Nothing about the
//! types stops it — every one of those fields is an `i64` — and the compiler is
//! happy either way.
//!
//! So each request is published for real and read back off the wire, then
//! decoded with the decoder for the template it claims to be. That check is
//! worth more than its size: the template id is what an archive dispatches on,
//! and the fields are what it acts on.
//!
//! What this does **not** cover is anything an archive does with a request —
//! there is no archive here, only our driver and a reader on the same channel.
//! That is deliberate: it keeps this a test of the *bytes*, which is what the
//! proxy is, and leaves "and the archive does the right thing" to the tests
//! that have an archive.

use std::time::{Duration, Instant};

use deepmsg_archive::client::ArchiveContext;
use deepmsg_archive::client::context::{CONTROL_CHANNEL_ENV, CONTROL_RESPONSE_CHANNEL_ENV};
use deepmsg_archive::client::proxy::{ArchiveProxy, ReplayParams, ReplicationParams};
use deepmsg_client::client::Client;
use deepmsg_codec::archive::archive_id_request_codec::{self, ArchiveIdRequestDecoder};
use deepmsg_codec::archive::attach_segments_request_codec::{self, AttachSegmentsRequestDecoder};
use deepmsg_codec::archive::auth_connect_request_codec::{self, AuthConnectRequestDecoder};
use deepmsg_codec::archive::boolean_type::BooleanType;
use deepmsg_codec::archive::bounded_replay_request_codec::{self, BoundedReplayRequestDecoder};
use deepmsg_codec::archive::challenge_response_codec::{self, ChallengeResponseDecoder};
use deepmsg_codec::archive::close_session_request_codec::{self, CloseSessionRequestDecoder};
use deepmsg_codec::archive::delete_detached_segments_request_codec::{
    self, DeleteDetachedSegmentsRequestDecoder,
};
use deepmsg_codec::archive::detach_segments_request_codec::{self, DetachSegmentsRequestDecoder};
use deepmsg_codec::archive::extend_recording_request_2_codec::{
    self, ExtendRecordingRequest2Decoder,
};
use deepmsg_codec::archive::find_last_matching_recording_request_codec::{
    self, FindLastMatchingRecordingRequestDecoder,
};
use deepmsg_codec::archive::list_recording_request_codec::{self, ListRecordingRequestDecoder};
use deepmsg_codec::archive::list_recording_subscriptions_request_codec::{
    self, ListRecordingSubscriptionsRequestDecoder,
};
use deepmsg_codec::archive::list_recordings_for_uri_request_codec::{
    self, ListRecordingsForUriRequestDecoder,
};
use deepmsg_codec::archive::list_recordings_request_codec::{self, ListRecordingsRequestDecoder};
use deepmsg_codec::archive::max_recorded_position_request_codec::{
    self, MaxRecordedPositionRequestDecoder,
};
use deepmsg_codec::archive::message_header_codec::MessageHeaderDecoder;
use deepmsg_codec::archive::migrate_segments_request_codec::{self, MigrateSegmentsRequestDecoder};
use deepmsg_codec::archive::purge_recording_request_codec::{self, PurgeRecordingRequestDecoder};
use deepmsg_codec::archive::purge_segments_request_codec::{self, PurgeSegmentsRequestDecoder};
use deepmsg_codec::archive::recording_position_request_codec::{
    self, RecordingPositionRequestDecoder,
};
use deepmsg_codec::archive::replay_request_codec::{self, ReplayRequestDecoder};
use deepmsg_codec::archive::replay_token_request_codec::{self, ReplayTokenRequestDecoder};
use deepmsg_codec::archive::replicate_request_2_codec::{self, ReplicateRequest2Decoder};
use deepmsg_codec::archive::source_location::SourceLocation;
use deepmsg_codec::archive::start_position_request_codec::{self, StartPositionRequestDecoder};
use deepmsg_codec::archive::start_recording_request_2_codec::{
    self, StartRecordingRequest2Decoder,
};
use deepmsg_codec::archive::stop_all_replays_request_codec::{self, StopAllReplaysRequestDecoder};
use deepmsg_codec::archive::stop_position_request_codec::{self, StopPositionRequestDecoder};
use deepmsg_codec::archive::stop_recording_by_identity_request_codec::{
    self, StopRecordingByIdentityRequestDecoder,
};
use deepmsg_codec::archive::stop_recording_request_codec::{self, StopRecordingRequestDecoder};
use deepmsg_codec::archive::stop_recording_subscription_request_codec::{
    self, StopRecordingSubscriptionRequestDecoder,
};
use deepmsg_codec::archive::stop_replay_request_codec::{self, StopReplayRequestDecoder};
use deepmsg_codec::archive::stop_replication_request_codec::{self, StopReplicationRequestDecoder};
use deepmsg_codec::archive::truncate_recording_request_codec::{
    self, TruncateRecordingRequestDecoder,
};
use deepmsg_codec::archive::update_channel_request_codec::{self, UpdateChannelRequestDecoder};
use deepmsg_codec::archive::{ReadBuf, SBE_SCHEMA_ID, SBE_SCHEMA_VERSION};
use deepmsg_tests::driver::{self, OwnDriver};

/// Where a request goes. IPC, so the reader is in this process and the bytes
/// need no network to come back.
const CHANNEL: &str = "aeron:ipc";

/// A stream of its own: every test here has its own driver, but a stream that
/// collides with a real archive's would make a failure read as something else.
const STREAM_ID: i32 = 4211;

const TIMEOUT: Duration = Duration::from_secs(5);

/// The session id every request below is stamped with — a value no driver would
/// choose, so a request that carried the proxy's `-1` instead would be caught
/// rather than pass for a real session.
const CONTROL_SESSION_ID: i64 = 4_211_000;

/// A driver, a client, and a publication with a reader on it.
struct Rig {
    /// The driver, which has to outlive the client: dropping it kills it.
    _own: OwnDriver,
    client: Client,
    /// What the proxy offers on.
    publication: i64,
    /// Where the same bytes are read back.
    subscription: i64,
}

/// Start a rig, or answer `None` when our driver is not built.
fn rig(test_name: &str) -> Option<Rig> {
    let Some(mut own) = OwnDriver::start(test_name) else {
        driver::announce_own_skip();
        return None;
    };

    own.await_cnc(Duration::from_secs(10))
        .expect("the driver publishes its CnC file");

    let mut client = Client::connect(own.aeron_dir()).expect("connect to our driver");

    let publication = client
        .add_exclusive_publication(CHANNEL, STREAM_ID, TIMEOUT)
        .expect("the driver must accept the publication");
    let subscription = client
        .add_subscription(CHANNEL, STREAM_ID, TIMEOUT)
        .expect("the driver must accept a reader");

    // An offer against a publication whose reader has not been seen yet answers
    // `NotConnected`, which the proxy treats as fatal — correctly, and the same
    // way the reference does. So the link has to be up before the first request.
    let deadline = Instant::now() + TIMEOUT;
    while !client
        .exclusive_publication(publication)
        .and_then(|publication| publication.is_connected())
        .unwrap_or(false)
    {
        assert!(
            Instant::now() < deadline,
            "the request publication never linked to its reader"
        );
        client.poll();
        std::thread::sleep(Duration::from_millis(1));
    }

    Some(Rig {
        _own: own,
        client,
        publication,
        subscription,
    })
}

/// A proxy over the rig's publication, with a session id already stamped on it.
///
/// The stamp is `async_connect`'s job and not this module's, so it is set here
/// by hand — which is also what makes every `control_session_id` assertion below
/// a real check rather than a look at the default.
fn proxy(rig: &Rig) -> ArchiveProxy {
    let context = ArchiveContext::resolve(&[
        (CONTROL_CHANNEL_ENV.to_owned(), CHANNEL.to_owned()),
        (CONTROL_RESPONSE_CHANNEL_ENV.to_owned(), CHANNEL.to_owned()),
    ]);

    let mut proxy = ArchiveProxy::new(&context, rig.publication);
    proxy.set_control_session_id(CONTROL_SESSION_ID);
    proxy
}

/// The next request the proxy published, as the bytes that went on the wire.
///
/// One frame per call, because one is what was published: asking for more than
/// one would swallow the next request's bytes and make the test after this one
/// read the wrong frame.
fn next_request(rig: &mut Rig) -> Vec<u8> {
    let deadline = Instant::now() + TIMEOUT;

    loop {
        rig.client.poll();

        let image = rig
            .client
            .subscription(rig.subscription)
            .and_then(|subscription| subscription.images().first())
            .map(|image| image.registration_id());

        if let Some(image) = image {
            let mut payload = None;

            rig.client
                .poll_image(rig.subscription, image, 1, |fragment| {
                    let mut bytes = vec![0u8; fragment.payload_length()];
                    assert!(
                        fragment.copy_payload(&mut bytes).is_some(),
                        "the frame the proxy wrote fits in the fragment it wrote"
                    );
                    payload = Some(bytes);
                });

            if let Some(payload) = payload {
                return payload;
            }
        }

        assert!(
            Instant::now() < deadline,
            "the proxy's request never came back off the wire"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// The SBE header of a request, and the checks every request shares.
///
/// The schema id and version are not decoration: an archive reads them to decide
/// whether it can decode what follows, and a request that got them wrong would
/// be refused before any field was looked at.
fn header_of(payload: &[u8], template_id: u16) -> MessageHeaderDecoder<ReadBuf<'_>> {
    let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(payload), 0);

    assert_eq!(SBE_SCHEMA_ID, header.schema_id(), "the archive schema");
    assert_eq!(SBE_SCHEMA_VERSION, header.version(), "and its version");
    // The one that matters most: a template id is what an archive dispatches
    // on, so a request that named the wrong one would be read as a different
    // request entirely rather than refused.
    //
    // `block_length` is deliberately *not* checked here. It is the message's
    // fixed body length rather than the header's, so it differs per template —
    // and it comes out of the same generated constants the template id does, so
    // a check on it would restate this one rather than add to it.
    assert_eq!(
        template_id,
        header.template_id(),
        "the request it says it is"
    );

    header
}

/// A variable-length field of a request, as the bytes the proxy meant to write.
///
/// Two steps rather than one because the generated accessors are: the
/// coordinates come out of a `&mut` read and the slice is looked up after.
macro_rules! var_bytes {
    ($decoder:expr, $coordinates:ident, $slice:ident) => {{
        let coordinates = $decoder.$coordinates();
        $decoder.$slice(coordinates)
    }};
}

/// A variable-length field of a request, as the text the proxy meant to write.
///
/// Two steps rather than one because the generated accessors are: the
/// coordinates come out of a `&mut` read and the slice is looked up after.
macro_rules! var_field {
    ($decoder:expr, $coordinates:ident, $slice:ident) => {{
        let coordinates = $decoder.$coordinates();
        text($decoder.$slice(coordinates))
    }};
}

/// A variable-length field, as the text the proxy meant to write.
fn text(slice: &[u8]) -> String {
    String::from_utf8_lossy(slice).into_owned()
}

/// `shouldDuplicateContext`'s neighbour: what the proxy says about itself.
///
/// Checked without a driver, because it is decided before one is needed — and it
/// is checked at all because the archive puts this string in the label of the
/// control-session counter it allocates, which is the only way to tell one
/// client's counters from another's.
#[test]
fn the_client_info_is_the_shape_the_archive_reads() {
    let mut context = ArchiveContext::resolve(&[
        (CONTROL_CHANNEL_ENV.to_owned(), CHANNEL.to_owned()),
        (CONTROL_RESPONSE_CHANNEL_ENV.to_owned(), CHANNEL.to_owned()),
    ]);
    context.client_name = "a name, with a comma".to_owned();

    let proxy = ArchiveProxy::new(&context, 0);

    // The reference's format (`aeron_archive_proxy.c:113-120`), and the two
    // halves this build writes into a counter label anywhere else.
    assert_eq!(
        format!(
            "name=a name, with a comma version={} commit={}",
            deepmsg_core::version::COMPAT_VERSION_TEXT,
            deepmsg_core::version::BUILD_IDENTITY
        ),
        proxy.client_info()
    );

    // A session id is not known before an archive names one, and `-1` is what
    // goes on the wire until then.
    assert_eq!(
        deepmsg_archive::client::proxy::NULL_VALUE,
        proxy.control_session_id()
    );
}

/// The four requests that open and close a session, which do not retry.
#[test]
fn the_session_requests_are_the_ones_a_connect_sends() {
    let Some(mut rig) = rig("archive-proxy-session") else {
        return;
    };
    let mut proxy = proxy(&rig);

    proxy
        .try_connect(
            &rig.client,
            1,
            "aeron:udp?endpoint=localhost:0",
            20,
            b"credentials",
        )
        .expect("the connect is offered");
    let payload = next_request(&mut rig);
    let mut request = AuthConnectRequestDecoder::default().header(
        header_of(&payload, auth_connect_request_codec::SBE_TEMPLATE_ID),
        0,
    );
    assert_eq!(1, request.correlation_id());
    assert_eq!(20, request.response_stream_id());
    assert_eq!(
        Some(deepmsg_archive::server::response_proxy::PROTOCOL_SEMANTIC_VERSION),
        request.version(),
        "the protocol's version, which is what an archive checks the major of"
    );
    // **In the schema's declaration order**, which for this template is
    // responseChannel, encodedCredentials, clientInfo. The accessors are a
    // cursor, not a lookup: each reads a length at the current limit and
    // advances past it, so reading them out of order hands back the next
    // field's bytes under this field's name — silently, because every one of
    // them is "some bytes".
    assert_eq!(
        "aeron:udp?endpoint=localhost:0",
        var_field!(request, response_channel_decoder, response_channel_slice)
    );
    assert_eq!(
        b"credentials",
        var_bytes!(
            request,
            encoded_credentials_decoder,
            encoded_credentials_slice
        )
    );
    assert_eq!(
        proxy.client_info(),
        var_field!(request, client_info_decoder, client_info_slice)
    );

    proxy
        .archive_id(&rig.client, 2)
        .expect("the archive id request is offered");
    let payload = next_request(&mut rig);
    let request = ArchiveIdRequestDecoder::default().header(
        header_of(&payload, archive_id_request_codec::SBE_TEMPLATE_ID),
        0,
    );
    assert_eq!(CONTROL_SESSION_ID, request.control_session_id());
    assert_eq!(2, request.correlation_id());

    proxy
        .challenge_response(&rig.client, 3, b"secret")
        .expect("the challenge response is offered");
    let payload = next_request(&mut rig);
    let mut request = ChallengeResponseDecoder::default().header(
        header_of(&payload, challenge_response_codec::SBE_TEMPLATE_ID),
        0,
    );
    assert_eq!(CONTROL_SESSION_ID, request.control_session_id());
    assert_eq!(3, request.correlation_id());
    assert_eq!(
        b"secret",
        var_bytes!(
            request,
            encoded_credentials_decoder,
            encoded_credentials_slice
        )
    );

    proxy
        .close_session(&rig.client)
        .expect("the close is offered");
    let payload = next_request(&mut rig);
    let request = CloseSessionRequestDecoder::default().header(
        header_of(&payload, close_session_request_codec::SBE_TEMPLATE_ID),
        0,
    );
    assert_eq!(CONTROL_SESSION_ID, request.control_session_id());
    // A decoder's `encoded_length` is its template's **block length** — the
    // fixed part, before any variable data — so this is the claim that the
    // template is one `i64` and no more. It is the one place these tests look at
    // a block length, because close-session is the one request whose whole
    // content is "which session", and a field added to it would be invisible to
    // every assertion above.
    assert_eq!(8, request.encoded_length(), "a session and nothing else");
}

/// The requests that make and stop a recording.
#[test]
fn the_recording_lifecycle_requests_carry_what_they_name() {
    let Some(mut rig) = rig("archive-proxy-recording") else {
        return;
    };
    let mut proxy = proxy(&rig);

    proxy
        .start_recording(&rig.client, 10, "aeron:ipc?alias=recorded", 7, true, false)
        .expect("the start is offered");
    let payload = next_request(&mut rig);
    let mut request = StartRecordingRequest2Decoder::default().header(
        header_of(&payload, start_recording_request_2_codec::SBE_TEMPLATE_ID),
        0,
    );
    assert_eq!(CONTROL_SESSION_ID, request.control_session_id());
    assert_eq!(10, request.correlation_id());
    assert_eq!(7, request.stream_id());
    assert_eq!(SourceLocation::LOCAL, request.source_location());
    assert_eq!(BooleanType::FALSE, request.auto_stop());
    assert_eq!(
        "aeron:ipc?alias=recorded",
        var_field!(request, channel_decoder, channel_slice)
    );

    proxy
        .get_recording_position(&rig.client, 11, 42)
        .expect("the position query is offered");
    let payload = next_request(&mut rig);
    let request = RecordingPositionRequestDecoder::default().header(
        header_of(&payload, recording_position_request_codec::SBE_TEMPLATE_ID),
        0,
    );
    assert_eq!(CONTROL_SESSION_ID, request.control_session_id());
    assert_eq!(11, request.correlation_id());
    assert_eq!(42, request.recording_id());

    proxy
        .get_start_position(&rig.client, 12, 43)
        .expect("the start position query is offered");
    let payload = next_request(&mut rig);
    let request = StartPositionRequestDecoder::default().header(
        header_of(&payload, start_position_request_codec::SBE_TEMPLATE_ID),
        0,
    );
    assert_eq!(CONTROL_SESSION_ID, request.control_session_id());
    assert_eq!(12, request.correlation_id());
    assert_eq!(43, request.recording_id());

    proxy
        .get_stop_position(&rig.client, 13, 44)
        .expect("the stop position query is offered");
    let payload = next_request(&mut rig);
    let request = StopPositionRequestDecoder::default().header(
        header_of(&payload, stop_position_request_codec::SBE_TEMPLATE_ID),
        0,
    );
    assert_eq!(CONTROL_SESSION_ID, request.control_session_id());
    assert_eq!(13, request.correlation_id());
    assert_eq!(44, request.recording_id());

    proxy
        .get_max_recorded_position(&rig.client, 14, 45)
        .expect("the max recorded position query is offered");
    let payload = next_request(&mut rig);
    let request = MaxRecordedPositionRequestDecoder::default().header(
        header_of(
            &payload,
            max_recorded_position_request_codec::SBE_TEMPLATE_ID,
        ),
        0,
    );
    assert_eq!(CONTROL_SESSION_ID, request.control_session_id());
    assert_eq!(14, request.correlation_id());
    assert_eq!(45, request.recording_id());

    proxy
        .stop_recording(&rig.client, 15, "aeron:ipc?alias=recorded", 8)
        .expect("the stop is offered");
    let payload = next_request(&mut rig);
    let mut request = StopRecordingRequestDecoder::default().header(
        header_of(&payload, stop_recording_request_codec::SBE_TEMPLATE_ID),
        0,
    );
    assert_eq!(CONTROL_SESSION_ID, request.control_session_id());
    assert_eq!(15, request.correlation_id());
    assert_eq!(8, request.stream_id());
    assert_eq!(
        "aeron:ipc?alias=recorded",
        var_field!(request, channel_decoder, channel_slice)
    );

    proxy
        .stop_recording_subscription(&rig.client, 16, 99)
        .expect("the subscription stop is offered");
    let payload = next_request(&mut rig);
    let request = StopRecordingSubscriptionRequestDecoder::default().header(
        header_of(
            &payload,
            stop_recording_subscription_request_codec::SBE_TEMPLATE_ID,
        ),
        0,
    );
    assert_eq!(CONTROL_SESSION_ID, request.control_session_id());
    assert_eq!(16, request.correlation_id());
    assert_eq!(
        99,
        request.subscription_id(),
        "a subscription, not a recording"
    );

    proxy
        .stop_recording_by_identity(&rig.client, 17, 46)
        .expect("the identity stop is offered");
    let payload = next_request(&mut rig);
    let request = StopRecordingByIdentityRequestDecoder::default().header(
        header_of(
            &payload,
            stop_recording_by_identity_request_codec::SBE_TEMPLATE_ID,
        ),
        0,
    );
    assert_eq!(CONTROL_SESSION_ID, request.control_session_id());
    assert_eq!(17, request.correlation_id());
    assert_eq!(46, request.recording_id());
}

/// The requests that list recordings and subscriptions.
#[test]
fn the_listing_requests_still_do_their_jobs_through_the_session() {
    let Some(mut rig) = rig("archive-proxy-listing") else {
        return;
    };
    let mut proxy = proxy(&rig);

    proxy
        .find_last_matching_recording(&rig.client, 20, 5, "alias=recorded", 9, 1234)
        .expect("the search is offered");
    let payload = next_request(&mut rig);
    let mut request = FindLastMatchingRecordingRequestDecoder::default().header(
        header_of(
            &payload,
            find_last_matching_recording_request_codec::SBE_TEMPLATE_ID,
        ),
        0,
    );
    assert_eq!(CONTROL_SESSION_ID, request.control_session_id());
    assert_eq!(20, request.correlation_id());
    assert_eq!(5, request.min_recording_id());
    assert_eq!(1234, request.session_id());
    assert_eq!(9, request.stream_id());
    assert_eq!(
        "alias=recorded",
        var_field!(request, channel_decoder, channel_slice)
    );

    proxy
        .list_recording(&rig.client, 21, 47)
        .expect("the one-descriptor list is offered");
    let payload = next_request(&mut rig);
    let request = ListRecordingRequestDecoder::default().header(
        header_of(&payload, list_recording_request_codec::SBE_TEMPLATE_ID),
        0,
    );
    assert_eq!(CONTROL_SESSION_ID, request.control_session_id());
    assert_eq!(21, request.correlation_id());
    assert_eq!(47, request.recording_id());

    proxy
        .list_recordings(&rig.client, 22, 48, 64)
        .expect("the page is offered");
    let payload = next_request(&mut rig);
    let request = ListRecordingsRequestDecoder::default().header(
        header_of(&payload, list_recordings_request_codec::SBE_TEMPLATE_ID),
        0,
    );
    assert_eq!(CONTROL_SESSION_ID, request.control_session_id());
    assert_eq!(22, request.correlation_id());
    assert_eq!(48, request.from_recording_id(), "the cursor, not a page");
    assert_eq!(64, request.record_count());

    proxy
        .list_recordings_for_uri(&rig.client, 23, 49, 10, "alias=recorded", 11)
        .expect("the filtered page is offered");
    let payload = next_request(&mut rig);
    let mut request = ListRecordingsForUriRequestDecoder::default().header(
        header_of(
            &payload,
            list_recordings_for_uri_request_codec::SBE_TEMPLATE_ID,
        ),
        0,
    );
    assert_eq!(CONTROL_SESSION_ID, request.control_session_id());
    assert_eq!(23, request.correlation_id());
    assert_eq!(49, request.from_recording_id());
    assert_eq!(10, request.record_count());
    assert_eq!(11, request.stream_id());
    assert_eq!(
        "alias=recorded",
        var_field!(request, channel_decoder, channel_slice)
    );

    proxy
        .list_recording_subscriptions(&rig.client, 24, 1, 100, "alias=recorded", 12, true)
        .expect("the subscription page is offered");
    let payload = next_request(&mut rig);
    let mut request = ListRecordingSubscriptionsRequestDecoder::default().header(
        header_of(
            &payload,
            list_recording_subscriptions_request_codec::SBE_TEMPLATE_ID,
        ),
        0,
    );
    assert_eq!(CONTROL_SESSION_ID, request.control_session_id());
    assert_eq!(24, request.correlation_id());
    assert_eq!(1, request.pseudo_index());
    assert_eq!(100, request.subscription_count());
    assert_eq!(BooleanType::TRUE, request.apply_stream_id());
    assert_eq!(12, request.stream_id());
    assert_eq!(
        "alias=recorded",
        var_field!(request, channel_decoder, channel_slice)
    );
}

/// The replay requests — including the one that picks its template.
#[test]
fn a_replay_picks_its_template_from_its_parameters() {
    let Some(mut rig) = rig("archive-proxy-replay") else {
        return;
    };
    let mut proxy = proxy(&rig);

    // Unbounded: no counter named, so the plain form.
    let params = ReplayParams {
        position: 128,
        length: 4096,
        replay_token: 77,
        file_io_max_length: 8192,
        ..ReplayParams::default()
    };
    assert!(!params.is_bounded());

    proxy
        .replay(&rig.client, 30, 50, "aeron:ipc?alias=replayed", 13, &params)
        .expect("the replay is offered");
    let payload = next_request(&mut rig);
    let mut request = ReplayRequestDecoder::default().header(
        header_of(&payload, replay_request_codec::SBE_TEMPLATE_ID),
        0,
    );
    assert_eq!(CONTROL_SESSION_ID, request.control_session_id());
    assert_eq!(30, request.correlation_id());
    assert_eq!(50, request.recording_id());
    assert_eq!(128, request.position());
    assert_eq!(4096, request.length());
    assert_eq!(13, request.replay_stream_id());
    assert_eq!(8192, request.file_io_max_length());
    assert_eq!(77, request.replay_token());
    assert_eq!(
        "aeron:ipc?alias=replayed",
        var_field!(request, replay_channel_decoder, replay_channel_slice)
    );

    // Bounded: a counter id that is not the null one, and one field more.
    let bounded = ReplayParams {
        bounding_limit_counter_id: 12,
        position: 256,
        length: 512,
        replay_token: 78,
        file_io_max_length: 4096,
        ..ReplayParams::default()
    };
    assert!(bounded.is_bounded());

    proxy
        .replay(
            &rig.client,
            31,
            51,
            "aeron:ipc?alias=replayed",
            14,
            &bounded,
        )
        .expect("the bounded replay is offered");
    let payload = next_request(&mut rig);
    let mut request = BoundedReplayRequestDecoder::default().header(
        header_of(&payload, bounded_replay_request_codec::SBE_TEMPLATE_ID),
        0,
    );
    assert_eq!(CONTROL_SESSION_ID, request.control_session_id());
    assert_eq!(31, request.correlation_id());
    assert_eq!(51, request.recording_id());
    assert_eq!(256, request.position());
    assert_eq!(512, request.length());
    assert_eq!(12, request.limit_counter_id(), "the field the form is for");
    assert_eq!(14, request.replay_stream_id());
    assert_eq!(4096, request.file_io_max_length());
    assert_eq!(78, request.replay_token());
    assert_eq!(
        "aeron:ipc?alias=replayed",
        var_field!(request, replay_channel_decoder, replay_channel_slice)
    );

    proxy
        .truncate_recording(&rig.client, 32, 52, 4096)
        .expect("the truncate is offered");
    let payload = next_request(&mut rig);
    let request = TruncateRecordingRequestDecoder::default().header(
        header_of(&payload, truncate_recording_request_codec::SBE_TEMPLATE_ID),
        0,
    );
    assert_eq!(CONTROL_SESSION_ID, request.control_session_id());
    assert_eq!(32, request.correlation_id());
    assert_eq!(52, request.recording_id());
    assert_eq!(4096, request.position());

    proxy
        .stop_replay(&rig.client, 33, 4242)
        .expect("the stop is offered");
    let payload = next_request(&mut rig);
    let request = StopReplayRequestDecoder::default().header(
        header_of(&payload, stop_replay_request_codec::SBE_TEMPLATE_ID),
        0,
    );
    assert_eq!(CONTROL_SESSION_ID, request.control_session_id());
    assert_eq!(33, request.correlation_id());
    assert_eq!(
        4242,
        request.replay_session_id(),
        "a session, not a recording"
    );

    proxy
        .stop_all_replays(&rig.client, 34, 53)
        .expect("the stop-all is offered");
    let payload = next_request(&mut rig);
    let request = StopAllReplaysRequestDecoder::default().header(
        header_of(&payload, stop_all_replays_request_codec::SBE_TEMPLATE_ID),
        0,
    );
    assert_eq!(CONTROL_SESSION_ID, request.control_session_id());
    assert_eq!(34, request.correlation_id());
    assert_eq!(53, request.recording_id());

    proxy
        .request_replay_token(&rig.client, 35, 54)
        .expect("the token request is offered");
    let payload = next_request(&mut rig);
    let request = ReplayTokenRequestDecoder::default().header(
        header_of(&payload, replay_token_request_codec::SBE_TEMPLATE_ID),
        0,
    );
    assert_eq!(CONTROL_SESSION_ID, request.control_session_id());
    assert_eq!(35, request.correlation_id());
    assert_eq!(54, request.recording_id());
}

/// The segment and retention requests.
#[test]
fn the_segment_requests_are_one_recording_each() {
    let Some(mut rig) = rig("archive-proxy-segments") else {
        return;
    };
    let mut proxy = proxy(&rig);

    proxy
        .purge_recording(&rig.client, 40, 55)
        .expect("the purge is offered");
    let payload = next_request(&mut rig);
    let request = PurgeRecordingRequestDecoder::default().header(
        header_of(&payload, purge_recording_request_codec::SBE_TEMPLATE_ID),
        0,
    );
    assert_eq!(CONTROL_SESSION_ID, request.control_session_id());
    assert_eq!(40, request.correlation_id());
    assert_eq!(55, request.recording_id());

    proxy
        .detach_segments(&rig.client, 41, 56, 1024)
        .expect("the detach is offered");
    let payload = next_request(&mut rig);
    let request = DetachSegmentsRequestDecoder::default().header(
        header_of(&payload, detach_segments_request_codec::SBE_TEMPLATE_ID),
        0,
    );
    assert_eq!(CONTROL_SESSION_ID, request.control_session_id());
    assert_eq!(41, request.correlation_id());
    assert_eq!(56, request.recording_id());
    assert_eq!(1024, request.new_start_position());

    proxy
        .delete_detached_segments(&rig.client, 42, 57)
        .expect("the delete is offered");
    let payload = next_request(&mut rig);
    let request = DeleteDetachedSegmentsRequestDecoder::default().header(
        header_of(
            &payload,
            delete_detached_segments_request_codec::SBE_TEMPLATE_ID,
        ),
        0,
    );
    assert_eq!(CONTROL_SESSION_ID, request.control_session_id());
    assert_eq!(42, request.correlation_id());
    assert_eq!(57, request.recording_id());
    assert_eq!(
        24,
        request.encoded_length(),
        "a detach step it is not: no position travels with it"
    );

    proxy
        .purge_segments(&rig.client, 43, 58, 2048)
        .expect("the segment purge is offered");
    let payload = next_request(&mut rig);
    let request = PurgeSegmentsRequestDecoder::default().header(
        header_of(&payload, purge_segments_request_codec::SBE_TEMPLATE_ID),
        0,
    );
    assert_eq!(CONTROL_SESSION_ID, request.control_session_id());
    assert_eq!(43, request.correlation_id());
    assert_eq!(58, request.recording_id());
    assert_eq!(2048, request.new_start_position());

    proxy
        .attach_segments(&rig.client, 44, 59)
        .expect("the attach is offered");
    let payload = next_request(&mut rig);
    let request = AttachSegmentsRequestDecoder::default().header(
        header_of(&payload, attach_segments_request_codec::SBE_TEMPLATE_ID),
        0,
    );
    assert_eq!(CONTROL_SESSION_ID, request.control_session_id());
    assert_eq!(44, request.correlation_id());
    assert_eq!(59, request.recording_id());

    proxy
        .migrate_segments(&rig.client, 45, 60, 61)
        .expect("the migrate is offered");
    let payload = next_request(&mut rig);
    let request = MigrateSegmentsRequestDecoder::default().header(
        header_of(&payload, migrate_segments_request_codec::SBE_TEMPLATE_ID),
        0,
    );
    assert_eq!(CONTROL_SESSION_ID, request.control_session_id());
    assert_eq!(45, request.correlation_id());
    assert_eq!(60, request.src_recording_id(), "the source first");
    assert_eq!(61, request.dst_recording_id(), "then the destination");

    proxy
        .update_channel(&rig.client, 46, 62, "aeron:ipc?alias=renamed")
        .expect("the channel update is offered");
    let payload = next_request(&mut rig);
    let mut request = UpdateChannelRequestDecoder::default().header(
        header_of(&payload, update_channel_request_codec::SBE_TEMPLATE_ID),
        0,
    );
    assert_eq!(CONTROL_SESSION_ID, request.control_session_id());
    assert_eq!(46, request.correlation_id());
    assert_eq!(62, request.recording_id());
    assert_eq!(
        "aeron:ipc?alias=renamed",
        var_field!(request, channel_decoder, channel_slice)
    );
}

/// The two replication requests — what S6 will be built on.
#[test]
fn the_replication_request_names_both_ends() {
    let Some(mut rig) = rig("archive-proxy-replication") else {
        return;
    };
    let mut proxy = proxy(&rig);

    let params = ReplicationParams {
        dst_recording_id: 71,
        stop_position: 4096,
        channel_tag_id: 5,
        subscription_tag_id: 6,
        file_io_max_length: 8192,
        replication_session_id: 12,
        live_destination: "aeron:ipc?alias=live".to_owned(),
        replication_channel: "aeron:ipc?alias=replicated".to_owned(),
        src_response_channel: "aeron:ipc?alias=source-responses".to_owned(),
        encoded_credentials: b"creds".to_vec(),
    };

    proxy
        .replicate(&rig.client, 50, 70, 15, "aeron:ipc?alias=source", &params)
        .expect("the replicate is offered");
    let payload = next_request(&mut rig);
    let mut request = ReplicateRequest2Decoder::default().header(
        header_of(&payload, replicate_request_2_codec::SBE_TEMPLATE_ID),
        0,
    );
    assert_eq!(CONTROL_SESSION_ID, request.control_session_id());
    assert_eq!(50, request.correlation_id());
    assert_eq!(70, request.src_recording_id());
    assert_eq!(71, request.dst_recording_id());
    assert_eq!(4096, request.stop_position());
    assert_eq!(5, request.channel_tag_id());
    assert_eq!(6, request.subscription_tag_id());
    assert_eq!(15, request.src_control_stream_id());
    assert_eq!(8192, request.file_io_max_length());
    assert_eq!(12, request.replication_session_id());
    assert_eq!(
        "aeron:ipc?alias=source",
        var_field!(
            request,
            src_control_channel_decoder,
            src_control_channel_slice
        )
    );
    assert_eq!(
        "aeron:ipc?alias=live",
        var_field!(request, live_destination_decoder, live_destination_slice)
    );
    assert_eq!(
        "aeron:ipc?alias=replicated",
        var_field!(
            request,
            replication_channel_decoder,
            replication_channel_slice
        )
    );
    assert_eq!(
        b"creds",
        var_bytes!(
            request,
            encoded_credentials_decoder,
            encoded_credentials_slice
        )
    );
    assert_eq!(
        "aeron:ipc?alias=source-responses",
        var_field!(
            request,
            src_response_channel_decoder,
            src_response_channel_slice
        )
    );

    proxy
        .stop_replication(&rig.client, 51, 4242)
        .expect("the stop is offered");
    let payload = next_request(&mut rig);
    let request = StopReplicationRequestDecoder::default().header(
        header_of(&payload, stop_replication_request_codec::SBE_TEMPLATE_ID),
        0,
    );
    assert_eq!(CONTROL_SESSION_ID, request.control_session_id());
    assert_eq!(51, request.correlation_id());
    assert_eq!(
        4242,
        request.replication_id(),
        "a replication, not a recording"
    );
}

/// An `ExtendRecordingRequest2` reaches the right template too.
///
/// Alone among these, `extend_recording` takes its arguments in the reference's
/// order — recording, channel, stream, flags, **correlation last** — which is a
/// shape a caller can get backwards without the compiler noticing, since the two
/// ids are both `i64`.
#[test]
fn extending_a_recording_keeps_the_references_argument_order() {
    let Some(mut rig) = rig("archive-proxy-extend") else {
        return;
    };
    let mut proxy = proxy(&rig);

    proxy
        .extend_recording(
            &rig.client,
            80,
            "aeron:ipc?alias=extended",
            16,
            false,
            true,
            81,
        )
        .expect("the extend is offered");
    let payload = next_request(&mut rig);
    let mut request = ExtendRecordingRequest2Decoder::default().header(
        header_of(&payload, extend_recording_request_2_codec::SBE_TEMPLATE_ID),
        0,
    );
    assert_eq!(CONTROL_SESSION_ID, request.control_session_id());
    assert_eq!(81, request.correlation_id(), "the last argument");
    assert_eq!(80, request.recording_id(), "the first");
    assert_eq!(16, request.stream_id());
    assert_eq!(SourceLocation::REMOTE, request.source_location());
    assert_eq!(BooleanType::TRUE, request.auto_stop());
    assert_eq!(
        "aeron:ipc?alias=extended",
        var_field!(request, channel_decoder, channel_slice)
    );
}
