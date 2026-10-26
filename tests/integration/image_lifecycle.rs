//! Batch two of the Java alignment: what a client can see of its images.
//!
//! Three things the client could not do before, each of them something the
//! reference has had all along:
//!
//! * learn that an image arrived or left without walking every subscription
//!   (`AvailableImageHandler`/`UnavailableImageHandler`, `Aeron.java:417`);
//! * poll a subscription **by fragment** rather than by reassembled message
//!   (`Subscription.poll`, `Subscription.java:188`), which is what makes a
//!   zero-copy reader possible at all;
//! * ask an image where it came from, where the reader joined, and whether the
//!   stream has ended (`Image.sourceIdentity`, `Image.joinPosition`,
//!   `Image.isEndOfStream`).
//!
//! Everything here is our own driver and our own client, so it needs no
//! reference checkout and runs in CI. All but one use `aeron:ipc`; where a
//! second subscription joins a running image, the answer depends on what the
//! image has received rather than on what a shared log buffer holds, and that
//! case uses `aeron:udp`.

use std::time::{Duration, Instant};

use deepmsg_client::client::{Client, DEFAULT_TIMEOUT};
use deepmsg_client::image_event::ImageEvent;
use deepmsg_core::logbuffer::append::Appended;
use deepmsg_core::logbuffer::descriptor::{END_OF_STREAM_OPEN, FRAME_ALIGNMENT};
use deepmsg_core::logbuffer::frame::DATA_HEADER_LENGTH;
use deepmsg_core::logbuffer::position::align_up;
use deepmsg_tests::driver::{self, OwnDriver};

/// The stream every test here uses.
const STREAM_ID: i32 = 1001;

/// Shared memory with a term small enough to fill: the default IPC term is
/// orders of magnitude larger, and the fragmentation below wants a window it
/// can actually move.
const CHANNEL: &str = "aeron:ipc?term-length=64k";

/// `AERON_IPC_MTU_LENGTH_DEFAULT` (`aeron_driver_context.c`), which is what a
/// channel that names no `mtu` gets.
const IPC_MTU: i32 = 1408;

/// How long the whole exchange may take before the test is a failure.
const DEADLINE: Duration = Duration::from_secs(30);

/// A driver of ours with a client connected to it.
fn own_driver_and_client(name: &str) -> Option<(OwnDriver, Client)> {
    let mut own = OwnDriver::start(name)?;

    own.await_cnc(Duration::from_secs(10))
        .expect("the driver publishes its CnC file");

    let client = Client::connect(own.aeron_dir()).expect("connect to our driver");

    Some((own, client))
}

/// Poll until the subscription holds an image.
fn await_image(client: &mut Client, subscription: i64) {
    let deadline = Instant::now() + DEADLINE;

    while client
        .subscription(subscription)
        .expect("the subscription")
        .images()
        .is_empty()
    {
        assert!(Instant::now() < deadline, "no image arrived");
        client.poll();
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// Drain image events, polling, until at least one has arrived.
fn await_image_event(client: &mut Client) -> Vec<ImageEvent> {
    let deadline = Instant::now() + DEADLINE;
    let mut events = Vec::new();

    while events.is_empty() {
        assert!(Instant::now() < deadline, "no image event arrived");
        client.poll();
        events.extend(client.image_events());
        std::thread::sleep(Duration::from_millis(1));
    }

    events
}

#[test]
fn an_image_event_announces_the_image_and_its_removal() {
    let Some((_own, mut client)) = own_driver_and_client("image-events") else {
        driver::announce_own_skip();
        return;
    };

    let publication = client
        .add_publication(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a publication");
    let subscription = client
        .add_subscription(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a subscription");

    // The event is the announcement, not the image: it has to arrive without
    // anything walking the subscription's image list.
    let events = await_image_event(&mut client);
    assert_eq!(1, events.len(), "one image, one event: {events:?}");

    await_image(&mut client, subscription);

    // Copied out rather than borrowed, because the removal below needs the
    // client mutably and the image lives inside it.
    let (session_id, join_position) = {
        let image = client
            .subscription(subscription)
            .expect("the subscription")
            .images()
            .first()
            .expect("the image the event named");

        (image.session_id(), image.join_position())
    };

    assert_eq!(
        ImageEvent::Available {
            subscription_registration_id: subscription,
            publication_registration_id: publication,
            session_id,
            stream_id: STREAM_ID,
            position: join_position,
        },
        events[0],
        "the event names the image the subscription actually holds"
    );

    // Removing the publication takes the image with it, and that is announced
    // too.
    client
        .remove_publication(publication, DEFAULT_TIMEOUT)
        .expect("the publication is removed");

    let gone = await_image_event(&mut client);
    assert_eq!(
        ImageEvent::Unavailable {
            subscription_registration_id: subscription,
            publication_registration_id: publication,
            session_id,
            stream_id: STREAM_ID,
            position: join_position,
        },
        gone[0],
        "and that the image went away, from where this reader had got to"
    );

    assert!(
        client
            .subscription(subscription)
            .expect("the subscription")
            .images()
            .is_empty(),
        "the image is gone from the subscription as well"
    );
}

/// Poll until the subscription holds no image.
fn await_no_image(client: &mut Client, subscription: i64) {
    let deadline = Instant::now() + DEADLINE;

    while !client
        .subscription(subscription)
        .expect("the subscription")
        .images()
        .is_empty()
    {
        assert!(Instant::now() < deadline, "the image never went away");
        client.poll();
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// A subscription is connected when it has an image and not when it merely has
/// a channel.
///
/// The distinction is the whole of `Subscription.isConnected`
/// (`Subscription.java:295-311`), and it is one a **live driver** is needed to
/// draw: the question that is easy to answer by mistake is "has the driver
/// bound this subscription's socket", which is true from the moment the
/// subscription is registered and says nothing about whether anything is
/// publishing to it. A merge that took that answer would go looking for an
/// image the driver has not matched yet, on every turn, forever.
#[test]
fn a_subscription_is_connected_when_it_has_an_image_and_not_before() {
    let Some((_own, mut client)) = own_driver_and_client("subscription-connected") else {
        driver::announce_own_skip();
        return;
    };

    // Registered, and its channel is up by the time `add_subscription`
    // answers — so everything below is about the image and not about the
    // registration.
    let subscription = client
        .add_subscription(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a subscription");

    client.poll();
    assert!(
        !client
            .subscription(subscription)
            .expect("the subscription")
            .is_connected(),
        "a subscription nothing publishes to is not connected"
    );

    let publication = client
        .add_publication(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a publication");

    await_image(&mut client, subscription);
    assert!(
        client
            .subscription(subscription)
            .expect("the subscription")
            .is_connected(),
        "the image is what connecting is"
    );

    client
        .remove_publication(publication, DEFAULT_TIMEOUT)
        .expect("the publication is removed");

    await_no_image(&mut client, subscription);
    assert!(
        !client
            .subscription(subscription)
            .expect("the subscription")
            .is_connected(),
        "and the subscription is disconnected again when the image goes"
    );
}

#[test]
fn a_fragmented_message_is_delivered_once_per_frame() {
    let Some((_own, mut client)) = own_driver_and_client("fragment-poll") else {
        driver::announce_own_skip();
        return;
    };

    let publication = client
        .add_publication(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a publication");
    let subscription = client
        .add_subscription(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a subscription");

    await_image(&mut client, subscription);

    // Larger than one frame can carry, so the append path splits it — which is
    // the whole point of the fragment-level poll.
    let payload = vec![0x5au8; 4000];
    let per_frame = client
        .publication(publication)
        .expect("the publication")
        .max_payload_length()
        .expect("a payload length");
    let expected_frames = payload.len().div_ceil(per_frame);
    assert!(
        expected_frames > 1,
        "the message has to fragment for this test to mean anything: {payload:?} bytes fits \
         {per_frame} per frame"
    );

    let deadline = Instant::now() + DEADLINE;
    loop {
        assert!(
            Instant::now() < deadline,
            "the publication never took the message"
        );
        client.poll();

        match client.offer(publication, &payload) {
            Some(Appended::Ok { .. }) => break,
            Some(Appended::BackPressured) => {}
            other => panic!("offer came back {other:?}"),
        }

        std::thread::sleep(Duration::from_millis(1));
    }

    let mut frames: Vec<Vec<u8>> = Vec::new();
    let deadline = Instant::now() + DEADLINE;
    while frames.len() < expected_frames {
        assert!(
            Instant::now() < deadline,
            "read {} of {expected_frames} frames",
            frames.len()
        );

        client.poll();
        client.poll_subscription_fragments(subscription, 10, |fragment| {
            let mut payload = vec![0u8; fragment.payload_length()];
            fragment.copy_payload(&mut payload);
            frames.push(payload);
        });

        std::thread::sleep(Duration::from_millis(1));
    }

    assert_eq!(
        expected_frames,
        frames.len(),
        "the frames are delivered one at a time, not reassembled"
    );

    let rebuilt: Vec<u8> = frames.concat();
    assert_eq!(payload, rebuilt, "and they still add up to the message");
}

#[test]
fn an_images_state_is_readable_from_the_client() {
    let Some((_own, mut client)) = own_driver_and_client("image-state") else {
        driver::announce_own_skip();
        return;
    };

    let publication = client
        .add_publication(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a publication");
    let subscription = client
        .add_subscription(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a subscription");

    await_image(&mut client, subscription);

    let image = client
        .subscription(subscription)
        .expect("the subscription")
        .images()
        .first()
        .expect("the image");

    // The driver's answer to the channel, not the string this client wrote:
    // an IPC image reports the constant (`aeron_driver_conductor.c:3612-3613`).
    assert_eq!("aeron:ipc", image.source_identity());

    assert_eq!(
        image.position(),
        image.join_position(),
        "a reader that has read nothing is where it joined"
    );

    assert_eq!(Some(IPC_MTU), image.mtu_length());

    assert_eq!(
        Some(END_OF_STREAM_OPEN),
        image.end_of_stream_position(),
        "the stream is open, so it has no end position yet"
    );
    assert_eq!(
        Some(false),
        image.is_end_of_stream(),
        "and a reader that has not reached one is not at the end"
    );

    assert_eq!(
        Some(0),
        image.active_transport_count(),
        "an IPC image has no transports to count"
    );
    assert_eq!(Some(false), image.is_publication_revoked());

    // And the publication is still there, so nothing above was the last gasp.
    assert!(client.publication(publication).is_some());
}

/// Publish one message, waiting out back pressure.
///
/// The log buffer is finite and the subscriber in these tests has not read
/// anything yet, so an offer can come back `BackPressured` before it is taken.
fn offer(client: &mut Client, publication: i64, payload: &[u8]) {
    let deadline = Instant::now() + DEADLINE;

    loop {
        assert!(
            Instant::now() < deadline,
            "the publication never took the message"
        );
        client.poll();

        match client.offer(publication, payload) {
            Some(Appended::Ok { .. }) => return,
            Some(Appended::BackPressured) => {}
            other => panic!("offer came back {other:?}"),
        }

        std::thread::sleep(Duration::from_millis(1));
    }
}

/// The other name an image has: the **session** its stream is in.
///
/// The registration id an image carries is this client's own bookkeeping — a
/// publication on another client never had one here — so a caller handed "the
/// image on session 7" has nothing else to look it up by. The session is the
/// id the driver gave the publication, and every frame of the stream carries
/// it, which is what makes it the name the archive's replay-merge can use.
#[test]
fn an_image_is_found_by_the_session_its_stream_is_in() {
    let Some((_own, mut client)) = own_driver_and_client("image-by-session") else {
        driver::announce_own_skip();
        return;
    };

    let publication = client
        .add_publication(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a publication");
    let subscription = client
        .add_subscription(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a subscription");

    await_image(&mut client, subscription);

    let image_registration_id = client
        .subscription(subscription)
        .expect("the subscription")
        .images()
        .first()
        .expect("the image")
        .registration_id();
    let session_id = client
        .subscription(subscription)
        .expect("the subscription")
        .images()
        .first()
        .expect("the image")
        .session_id();

    let found = client
        .subscription(subscription)
        .expect("the subscription")
        .image_by_session_id(session_id)
        .expect("the image, by the session it is in");

    assert_eq!(image_registration_id, found.registration_id());

    assert!(
        client
            .subscription(subscription)
            .expect("the subscription")
            .image_by_session_id(session_id.wrapping_add(1))
            .is_none(),
        "a session this subscription is not reading answers nothing"
    );

    assert!(client.publication(publication).is_some());
}

/// The block face, reached the way a caller reaches it: through the client.
///
/// `Image::block_poll` cannot be called from outside — this client lends
/// images out borrowed, and it is the client that publishes the reader's
/// position afterwards — so `block_poll_image` is the door, and this is what
/// it is for: a recorder copying runs of frames out rather than being called
/// once per frame (`RecordingSession.java:237`).
#[test]
fn a_run_of_frames_can_be_read_in_one_call() {
    let Some((_own, mut client)) = own_driver_and_client("image-block-poll") else {
        driver::announce_own_skip();
        return;
    };

    let publication = client
        .add_publication(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a publication");
    let subscription = client
        .add_subscription(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a subscription");

    await_image(&mut client, subscription);

    const MESSAGES: usize = 4;
    const PAYLOAD: &[u8] = b"four";

    for _ in 0..MESSAGES {
        offer(&mut client, publication, PAYLOAD);
    }

    let image = client
        .subscription(subscription)
        .expect("the subscription")
        .images()
        .first()
        .expect("the image");

    let image_registration_id = image.registration_id();
    let session_id = image.session_id();
    let position_before = image.position();
    let subscriber_position_id = image.subscriber_position_id();

    let mut blocks = Vec::new();
    let read = client
        .block_poll_image(subscription, image_registration_id, 4096, |block| {
            let mut bytes = vec![0u8; block.length()];
            assert!(block.copy_out(&mut bytes).is_some(), "the block fits");

            blocks.push((block.length(), block.session_id(), bytes));
        })
        .expect("the image this client holds");

    // One call, one run: four frames that were written one after another.
    let reach = usize::try_from(align_up(
        i32::try_from(DATA_HEADER_LENGTH + PAYLOAD.len()).expect("small"),
        FRAME_ALIGNMENT,
    ))
    .expect("positive");

    assert_eq!(1, blocks.len(), "one run, one call");
    assert_eq!(reach * MESSAGES, read, "and the count is in bytes");
    assert_eq!(&read, &blocks[0].0);
    assert_eq!(&session_id, &blocks[0].1, "the stream the frames came from");

    for index in 0..MESSAGES {
        let start = index * reach + DATA_HEADER_LENGTH;
        assert_eq!(
            PAYLOAD,
            &blocks[0].2[start..start + PAYLOAD.len()],
            "frame {index} of the run"
        );
    }

    // The position moved over the run, and it is the counter's now — which the
    // next call sees as "nothing left" rather than as the same run again.
    assert_eq!(
        position_before + read as i64,
        client
            .subscription(subscription)
            .expect("the subscription")
            .images()
            .first()
            .expect("the image")
            .position()
    );

    // And it reached the **counter**, not just this client's field, which is
    // the half of a poll only the client can do: the position is what says the
    // run was consumed, to the driver and to any other reader of the same log.
    assert_eq!(
        Some(position_before + read as i64),
        client
            .counters_reader()
            .and_then(|reader| reader.value(subscriber_position_id)),
        "the reader's position was published"
    );
    assert_eq!(
        Some(0),
        client.block_poll_image(subscription, image_registration_id, 4096, |_| panic!(
            "nothing is left to hand over"
        )),
        "the run was consumed, not re-delivered"
    );
}

/// The bounded face: a caller that may only apply what has been committed
/// polls up to the commit position and comes back for the rest
/// (`Image.boundedControlledPoll`, `Image.java:565-665`).
///
/// The bound is a **start** position: a frame that begins at or after it is
/// left in the term even though it has been written, which is what lets a
/// cluster follower refuse to apply an entry the leader has not committed
/// (`ConsensusModuleAgent.java:2523`).
#[test]
fn a_bounded_poll_stops_at_the_bound() {
    let Some((_own, mut client)) = own_driver_and_client("image-bounded-poll") else {
        driver::announce_own_skip();
        return;
    };

    let publication = client
        .add_publication(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a publication");
    let subscription = client
        .add_subscription(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a subscription");

    await_image(&mut client, subscription);

    const PAYLOAD: &[u8] = b"one";
    for index in 0..3_u8 {
        let mut bytes = PAYLOAD.to_vec();
        bytes[2] = b'1' + index;
        offer(&mut client, publication, &bytes);
    }

    let image = client
        .subscription(subscription)
        .expect("the subscription")
        .images()
        .first()
        .expect("the image");

    let image_registration_id = image.registration_id();
    let position_before = image.position();
    let subscriber_position_id = image.subscriber_position_id();

    let reach = i64::from(align_up(
        i32::try_from(DATA_HEADER_LENGTH + PAYLOAD.len()).expect("small"),
        FRAME_ALIGNMENT,
    ));

    // The bound names the second frame's start, so only the first is read.
    let mut delivered = Vec::new();
    let read = client
        .poll_image_bounded(
            subscription,
            image_registration_id,
            position_before + reach,
            10,
            |fragment| delivered.push(fragment.position()),
        )
        .expect("the image this client holds");

    assert_eq!(1, read, "one frame starts before the bound");
    assert_eq!(vec![position_before], delivered);
    assert_eq!(
        position_before + reach,
        client
            .subscription(subscription)
            .expect("the subscription")
            .images()
            .first()
            .expect("the image")
            .position(),
        "the reader stopped at the bound"
    );
    assert_eq!(
        Some(position_before + reach),
        client
            .counters_reader()
            .and_then(|reader| reader.value(subscriber_position_id)),
        "and the stop was published to the counter"
    );

    // The bound is gone, so the rest of the term is read in one poll.
    let read = client
        .poll_image(subscription, image_registration_id, 10, |_| {})
        .expect("the image this client holds");
    assert_eq!(2, read, "the two frames the bound withheld");
}

/// An image that **left** the subscription is still told when its stream is
/// over — both ways it can be over.
///
/// This is what a reader that took an image out needs, and there is nowhere
/// else for it to come from: `Client::take_image` gives the image to a thread
/// that polls no client (the archive's recorder is the caller this exists for),
/// so the `ImageEvent`s the client queues are not on its path, and
/// `Subscription::image` no longer finds it. What reaches it is
/// [`Image::is_closed`], flipped by the client on the two endings below.
///
/// ★ The second ending is the one that is easy to miss, and it was: a
/// subscription leaving a live client is the **normal** end of a recording
/// (`stop subscription`), and before this test it left a handed-out image
/// looking open — an archive session that read a stream nobody was serving any
/// more, and never ended.
#[test]
fn an_image_that_left_the_subscription_hears_that_it_is_over() {
    let Some((_own, mut client)) = own_driver_and_client("image-handover") else {
        driver::announce_own_skip();
        return;
    };

    let publication = client
        .add_publication(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a publication");
    let subscription = client
        .add_subscription(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a subscription");

    await_image(&mut client, subscription);

    let image = client
        .take_image(subscription, publication)
        .expect("the subscription hands the image over");
    assert!(!image.is_closed(), "it is open when it is handed over");
    assert!(
        client
            .subscription(subscription)
            .expect("the subscription")
            .images()
            .is_empty(),
        "and it is no longer the subscription's"
    );

    // Ending one: the driver withdraws it, which the client hears as an
    // `Unavailable` event and the holder as the flag.
    client
        .remove_publication(publication, DEFAULT_TIMEOUT)
        .expect("the publication is removed");

    let deadline = Instant::now() + DEADLINE;
    while !image.is_closed() {
        assert!(Instant::now() < deadline, "the withdrawal never reached it");
        client.poll();
        std::thread::sleep(Duration::from_millis(1));
    }

    // Ending two: the subscription itself goes, with an image still in hand.
    let second_publication = client
        .add_publication(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a publication for the second ending");
    let second_subscription = client
        .add_subscription(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a subscription for the second ending");

    await_image(&mut client, second_subscription);

    let held = client
        .take_image(second_subscription, second_publication)
        .expect("the second subscription hands its image over");

    client
        .remove_subscription(second_subscription, DEFAULT_TIMEOUT)
        .expect("the subscription goes");

    assert!(
        held.is_closed(),
        "a subscription that leaves the client ends the images it gave away"
    );
}

/// A subscription that joins an image already running starts where the
/// **slowest reader** is, not where the image has got to
/// (`aeron_publication_image_join_position`,
/// `aeron-driver/src/main/c/aeron_publication_image.h:376-396`).
///
/// A lone subscriber cannot tell the difference; a second one can. Joining at
/// the image's position would start it past the frames the first reader has
/// not read yet, and those frames are never replayed — the late reader would
/// call the stream continuous in the middle of a gap.
///
/// What made this a hang rather than a loss: a restarted cluster member's
/// archive starts recording the catch-up replay a turn **after** the member's
/// own log subscription has joined it, by which time the replay of the gap may
/// be over. The recording subscription then joined past the gap, and the
/// archive refused to extend the recording (`cannot extend recording …
/// image.joinPosition … != rec.stopPosition`), so the member looped
/// `FOLLOWER_CATCHUP_AWAIT` until the harness gave up.
#[test]
fn a_second_subscription_joins_where_the_slowest_reader_is() {
    let Some((_own, mut client)) = own_driver_and_client("image-join-position") else {
        driver::announce_own_skip();
        return;
    };

    let port = free_port();
    let channel = format!("aeron:udp?endpoint=127.0.0.1:{port}|term-length=64k");

    let publication = client
        .add_publication(&channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a publication on the channel");
    let first = client
        .add_subscription(&channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("the first subscription");
    await_image(&mut client, first);

    // Frames reach the image but not the reader: polling the *client* drains
    // driver events, and only `poll_subscription` moves a reader's position.
    let deadline = Instant::now() + DEADLINE;
    let mut offered = 0;
    while offered < 20 {
        assert!(
            Instant::now() < deadline,
            "{offered} messages were offered in {DEADLINE:?}"
        );
        if let Some(Appended::Ok { .. }) = client.offer(publication, &[0_u8; 512]) {
            offered += 1;
        } else {
            client.poll();
        }
        client.poll();
    }

    // The image has to have received them, or the two answers agree for the
    // wrong reason. `rcv-pos` is the number the broken answer used.
    let received = await_received_position(&mut client);
    assert!(
        received > 0,
        "the image never received a frame, so this test proves nothing"
    );

    let first_join = image_join_position(&client, first);
    assert_eq!(0, first_join, "the first reader joined at the start");

    let second = client
        .add_subscription(&channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("the second subscription on the same stream");
    await_image(&mut client, second);

    assert_eq!(
        first_join,
        image_join_position(&client, second),
        "a reader that joins later starts where the slowest reader is, not at rcv-pos={received}"
    );
}

/// The position the driver's image has received to (`rcv-pos`), whichever
/// image holds one.
fn await_received_position(client: &mut Client) -> i64 {
    let deadline = Instant::now() + DEADLINE;
    loop {
        client.poll();
        let received = client.counters_reader().map_or(0, |counters| {
            let mut position = 0_i64;
            counters.for_each(|descriptor| {
                if descriptor.type_id == deepmsg_driver::position::type_id::RECEIVER_POSITION {
                    position = position.max(descriptor.value);
                }
            });
            position
        });
        if received > 0 || Instant::now() >= deadline {
            return received;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// Where the subscription's image says its reader started.
fn image_join_position(client: &Client, subscription: i64) -> i64 {
    client
        .subscription(subscription)
        .expect("the subscription")
        .images()
        .first()
        .expect("the subscription holds an image")
        .join_position()
}

/// A UDP port nothing else on this machine is likely to hold.
///
/// **Above the ephemeral range on purpose** — this binary's block is 61000 to
/// 61499. Linux hands an outbound socket a port from `ip_local_port_range`
/// (32768 to 60999 here, the same range this build's `EPHEMERAL_PORTS`
/// carries), so a test that binds inside it is racing every socket the machine
/// opens. The `40_000 + pid % 20_000` this file used before was inside it, and
/// a run whose pid landed on avahi's `0.0.0.0:46004` met `Address already in
/// use` before the test did anything. `short_datagram` takes 62000 to 64999;
/// `unreliable_stream` (61500 to 61999) and the cluster binaries own theirs.
fn free_port() -> u16 {
    #[allow(clippy::cast_possible_truncation)] // the low bits of a pid
    let base = 61_000 + (std::process::id() as u16 % 500);

    base
}
