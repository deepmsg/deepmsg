//! Response channels: a subscription that is a place to be answered, not a
//! reader.
//!
//! `control-mode=response` names the channel a responder sends its response
//! setup to, and its subscription deliberately reads nothing until that setup
//! arrives (`aeron_driver_conductor.c:5072-5092`). The reference calls
//! `incref_to_response_stream` where every other mode calls
//! `add_network_subscription_to_receiver`, so no interest is registered for its
//! stream and no image can form on one however much traffic reaches its socket.
//!
//! Two things are asserted, and both are needed. The client seeing no image is
//! what a caller observes, but it is downstream of the conductor's matching
//! rules as well as of the registration, so on its own it would pass for a
//! driver that had registered the interest and then failed to link it. The
//! second assertion is the one that pins the registration itself: a frame that
//! arrives for a stream nobody reads is **elicited** — the driver answers with
//! a status message carrying `SEND_SETUP` — where a registered stream is turned
//! into an image and answered with an ordinary position report.
//!
//! And both are asserted **against a control**: the same driver, the same
//! hand-built `SETUP`, the same stream, sent at a subscription that is an
//! ordinary reader. The control forms an image and elicits nothing.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use deepmsg_client::client::{Client, CommandError, DEFAULT_TIMEOUT};
use deepmsg_driver::protocol::{RspSetupFrame, SetupFrame, StatusMessageFrame, header_flags};
use deepmsg_driver::sys::AddressFamily;
use deepmsg_driver::sys::socket::{DatagramSocket, Datagrams};
use deepmsg_tests::driver::{self, OwnDriver, READY_TIMEOUT};

const STREAM_ID: i32 = 1001;

/// The session the far end's hand-built `SETUP` announces where nothing else
/// is being asked of it.
const READER_SESSION: i32 = 7;

/// A UDP port nobody is listening on, derived from the process id so that two
/// drivers on one machine collide only by choosing the same pair.
fn free_udp_port(offset: u16) -> u16 {
    #[allow(clippy::cast_possible_truncation)] // the low bits of a pid
    let base = 20_000 + (std::process::id() as u16 % 20_000);

    base.saturating_add(offset)
}

/// What a publisher's `SETUP` said: the session it is running, the flags byte,
/// and where it came from.
///
/// That last one is the only way to reach a publication at all — it binds a port
/// it never named, so a responder learns it from the first thing the publisher
/// sends, which is the whole reason a response channel has a handshake.
struct Announced {
    session_id: i32,
    flags: u8,
    from: SocketAddr,
    /// Where the publication says its stream starts. A subscriber's status
    /// message reports the position it has *read*, and the only position a
    /// subscriber with a fresh socket can honestly report is the one the
    /// `SETUP` named — which is also the only way a hand-built status message
    /// passes the validity band the publication applies to it
    /// (`is_valid_status_message`, `aeron_network_publication.c:841-856`).
    active_term_id: i32,
    term_offset: i32,
}

/// The far end of a session: a socket that is not the driver.
///
/// It is kept open for the length of a wait — a sender that has already closed
/// is a different case than the one under test — and it is where the driver
/// answers, so what it receives is the whole of the driver's reply.
struct FarEnd {
    socket: DatagramSocket,
    buffers: Vec<Vec<u8>>,
    datagrams: Datagrams,
}

impl FarEnd {
    fn open(control: u16) -> Self {
        let socket = DatagramSocket::open(AddressFamily::Inet).expect("a socket");
        socket
            .bind(format!("127.0.0.1:{control}").parse().expect("an address"))
            .expect("a bind");
        socket.set_nonblocking().expect("non-blocking");

        Self {
            socket,
            buffers: vec![vec![0u8; 1500]; 8],
            datagrams: Datagrams::new(),
        }
    }

    /// A `SETUP` for `stream_id` announcing `session_id`, sent to `port`, with
    /// nothing asked for.
    fn send_a_setup(&self, port: u16, stream_id: i32, session_id: i32) {
        self.send_a_setup_asking(port, stream_id, session_id, 0);
    }

    /// The same, with `flags` in the header.
    ///
    /// The one flag that matters here is
    /// [`header_flags::SETUP_SEND_RESPONSE`], which is the sender saying it
    /// wants a response channel: it is the header's to say, so it cannot be
    /// left out of a hand-built frame that is meant to be one.
    fn send_a_setup_asking(&self, port: u16, stream_id: i32, session_id: i32, flags: u8) {
        let setup = SetupFrame {
            term_offset: 0,
            session_id,
            stream_id,
            initial_term_id: 1_000,
            active_term_id: 1_000,
            term_length: 64 * 1024,
            mtu: 1408,
            ttl: 0,
        };
        let mut frame = [0u8; SetupFrame::LENGTH];
        assert!(setup.write_with_flags(&mut frame, flags).is_some());

        let _ = self.socket.send_batch(
            Some(format!("127.0.0.1:{port}").parse().expect("an address")),
            &[&frame],
        );
    }

    /// Take one datagram, if one has arrived, with where it came from.
    fn take(&mut self) -> Option<(Vec<u8>, Option<SocketAddr>)> {
        match self
            .socket
            .receive_batch(&mut self.buffers, &mut self.datagrams)
        {
            Ok(0) | Err(_) => None,
            Ok(_) => {
                let datagram = self.datagrams.as_slice()[0];
                Some((self.buffers[0][..datagram.length].to_vec(), datagram.source))
            }
        }
    }
    /// Send `frame` to `address`.
    fn send(&self, address: SocketAddr, frame: &[u8]) {
        let _ = self.socket.send_batch(Some(address), &[frame]);
    }

    /// A status message for `described`, sent to `to`.
    ///
    /// This is what a subscriber says to a publication, and the only thing a
    /// publication counts as a reader: without one, a publication has no live
    /// receiver and nothing it does about response channels ever moves on.
    fn send_a_status_message(&self, to: SocketAddr, described: &Announced) {
        let sm = StatusMessageFrame {
            session_id: described.session_id,
            stream_id: STREAM_ID,
            consumption_term_id: described.active_term_id,
            consumption_term_offset: described.term_offset,
            receiver_window: 64 * 1024,
            receiver_id: 1,
        };

        let mut frame = [0u8; StatusMessageFrame::LENGTH];
        assert!(sm.write_with_flags(&mut frame, 0).is_some());

        let _ = self.socket.send_batch(Some(to), &[&frame]);
    }

    /// The ask a receive endpoint makes when it knows a stream and a session
    /// but has no image for them: a status message that is nothing but
    /// `SEND_SETUP` (`aeron_receive_channel_endpoint_elicit_setup`,
    /// `media/aeron_receive_channel_endpoint.c:259-289` — stream, session, and
    /// three zeroes, which is why the window here is zero and not the 64K a
    /// position report would carry).
    ///
    /// It is the **session** that makes this one work, and it is why a
    /// requester has to wait for the far end's `RSP_SETUP` before it can ask:
    /// the far end finds its publication by `(stream_id << 32) | session_id`
    /// (`aeron_send_channel_endpoint.c:614-616`), so an ask that names no
    /// session reaches no publication at all.
    ///
    /// It goes to the channel's `control=` address, where the far end's send
    /// endpoint is bound — and that far end learns **this** socket's address as
    /// the only one it may ever answer
    /// (`aeron_network_publication.h:263-273`).
    fn elicit(&self, control: u16, stream_id: i32, session_id: i32) {
        let sm = StatusMessageFrame {
            session_id,
            stream_id,
            consumption_term_id: 0,
            consumption_term_offset: 0,
            receiver_window: 0,
            receiver_id: 1,
        };

        let mut frame = [0u8; StatusMessageFrame::LENGTH];
        assert!(
            sm.write_with_flags(&mut frame, header_flags::SM_SEND_SETUP)
                .is_some()
        );

        let _ = self.socket.send_batch(
            Some(format!("127.0.0.1:{control}").parse().expect("an address")),
            &[&frame],
        );
    }

    /// Every `RSP_SETUP` that arrives within `within`.
    fn take_rsp_setups(&mut self, within: Duration) -> Vec<RspSetupFrame> {
        let deadline = Instant::now() + within;
        let mut setups = Vec::new();

        while Instant::now() < deadline {
            match self.take() {
                Some((bytes, _)) => {
                    if let Some(setup) = RspSetupFrame::read(&bytes) {
                        setups.push(setup);
                    }
                }
                None => std::thread::sleep(Duration::from_millis(5)),
            }
        }

        setups
    }

    /// Wait for the driver's `SETUP` on `stream_id`, and report what it said.
    fn await_setup(&mut self, stream_id: i32, within: Duration) -> Option<Announced> {
        let deadline = Instant::now() + within;

        while Instant::now() < deadline {
            if let Some((bytes, source)) = self.take() {
                if let Some(setup) = SetupFrame::read(&bytes) {
                    if setup.stream_id == stream_id {
                        return source.map(|from| Announced {
                            session_id: setup.session_id,
                            flags: bytes[5],
                            from,
                            active_term_id: setup.active_term_id,
                            term_offset: setup.term_offset,
                        });
                    }
                }
            }

            std::thread::sleep(Duration::from_millis(5));
        }

        None
    }
}

/// What a subscriber did with a `SETUP` it was sent, in the window `within`.
///
/// `answers` counts what the far end heard from the driver. It is the coarse
/// half of the pair and the one that pins the registration: a driver that had
/// told its receiver to read this stream would form an image and that image
/// would report its position here, whatever the conductor did with the link
/// afterwards.
#[derive(Debug)]
struct Outcome {
    image: bool,
    answers: usize,
}

/// Poll the client and the far end together for `within`.
fn watch_for(
    client: &mut Client,
    far_end: &mut FarEnd,
    registration_id: i64,
    within: Duration,
) -> Outcome {
    let deadline = Instant::now() + within;
    let mut outcome = Outcome {
        image: false,
        answers: 0,
    };

    while Instant::now() < deadline {
        client.poll();

        while far_end.take().is_some() {
            outcome.answers += 1;
        }

        if client
            .subscription(registration_id)
            .is_some_and(|subscription| !subscription.images().is_empty())
        {
            outcome.image = true;
        }

        std::thread::sleep(Duration::from_millis(5));
    }

    outcome
}

/// ⑨: two subscriptions that differ only in their control mode, sent the same
/// `SETUP` for the same stream, read it and do not.
#[test]
fn a_response_subscription_is_not_a_reader() {
    let Some(mut own) = OwnDriver::start("response-channel") else {
        driver::announce_own_skip();
        return;
    };

    own.await_cnc(READY_TIMEOUT)
        .expect("this driver must publish a readable CnC file");

    let mut client = Client::connect(own.aeron_dir()).expect("connect our client");

    // The control: an ordinary subscriber on the same shape of channel — an
    // `endpoint=` it binds and a `control=` it answers through. A `SETUP` sent
    // at its endpoint names a session it reads, so an image forms and the
    // answer is ordinary position reports.
    let reader_endpoint = free_udp_port(61);
    let reader_control = free_udp_port(62);
    let reader_id = client
        .add_subscription(
            &format!(
                "aeron:udp?endpoint=127.0.0.1:{reader_endpoint}|control=127.0.0.1:{reader_control}"
            ),
            STREAM_ID,
            DEFAULT_TIMEOUT,
        )
        .expect("our driver must confirm the UDP subscription");

    let mut reader_far = FarEnd::open(reader_control);
    reader_far.send_a_setup(reader_endpoint, STREAM_ID, READER_SESSION);

    let ordinary = watch_for(
        &mut client,
        &mut reader_far,
        reader_id,
        Duration::from_secs(2),
    );

    assert!(
        ordinary.image,
        "an ordinary subscription forms an image on the first SETUP sent at it"
    );
    assert!(
        ordinary.answers > 0,
        "and the image answers on the channel's control address: {ordinary:?}"
    );

    // The same channel with `control-mode=response`, which is the only
    // difference between the two phases. It is *served* rather than refused,
    // which is what this slice changed.
    let response_endpoint = free_udp_port(63);
    let response_control = free_udp_port(64);
    let response_id = client
        .add_subscription(
            &format!(
                "aeron:udp?endpoint=127.0.0.1:{response_endpoint}\
                 |control=127.0.0.1:{response_control}|control-mode=response"
            ),
            STREAM_ID,
            DEFAULT_TIMEOUT,
        )
        .expect("a response channel is one this driver serves");

    let mut responder_far = FarEnd::open(response_control);
    responder_far.send_a_setup(response_endpoint, STREAM_ID, READER_SESSION);

    let responded = watch_for(
        &mut client,
        &mut responder_far,
        response_id,
        Duration::from_secs(1),
    );

    assert!(
        !responded.image,
        "no image forms: a response subscription reads nothing until a RSP_SETUP \
         names its session"
    );
    assert_eq!(
        0, responded.answers,
        "and nothing answers the SETUP at all — a stream no subscription is \
         registered for is silence, not an image that failed to link"
    );

    drop(client);
    let _ = own.stop();
}

/// ⑨: a `RSP_SETUP` is what makes a response subscription readable, and what
/// it says is the **session** — the one thing the subscription could not name
/// for itself.
///
/// The whole exchange is here, both halves on this driver: a subscription with
/// `control-mode=response`, the publication made for it, and a responder that
/// hears the publication's `SETUP` and answers it. What the responder learns
/// from that `SETUP` is the publisher's port, which is the only way to reach a
/// publication at all — it binds a port it never named.
///
/// The assertion is a pair again, and the order matters: after the setup
/// completes, a `SETUP` for a session the subscription was **not** told to read
/// is still silence, and only the one it was told to read makes an image. A
/// driver that had simply registered the subscription on any session would pass
/// the second half and fail the first.
#[test]
fn a_response_setup_is_what_makes_a_session_readable() {
    let Some(mut own) = OwnDriver::start("response-setup") else {
        driver::announce_own_skip();
        return;
    };

    own.await_cnc(READY_TIMEOUT)
        .expect("this driver must publish a readable CnC file");

    let mut client = Client::connect(own.aeron_dir()).expect("connect our client");

    let endpoint = free_udp_port(71);
    let control = free_udp_port(72);
    let subscription_id = client
        .add_subscription(
            &format!(
                "aeron:udp?endpoint=127.0.0.1:{endpoint}|control=127.0.0.1:{control}\
                 |control-mode=response"
            ),
            STREAM_ID,
            DEFAULT_TIMEOUT,
        )
        .expect("a response channel is one this driver serves");

    // The other half: a publication made to answer that subscription, which is
    // what puts its correlation id on the publication and makes a `RSP_SETUP`
    // about it something the conductor will act on.
    let publication_port = free_udp_port(73);
    let _publication_id = client
        .add_publication(
            &format!(
                "aeron:udp?endpoint=127.0.0.1:{publication_port}\
                 |response-correlation-id={subscription_id}"
            ),
            STREAM_ID,
            DEFAULT_TIMEOUT,
        )
        .expect("our driver must confirm the UDP publication");

    let mut responder = FarEnd::open(publication_port);
    let announced = responder
        .await_setup(STREAM_ID, Duration::from_secs(5))
        .expect("a publication describes itself before anything can answer it");

    let response_session = 4242;
    let frame = RspSetupFrame {
        session_id: announced.session_id,
        stream_id: STREAM_ID,
        response_session_id: response_session,
    };
    let mut bytes = [0u8; RspSetupFrame::LENGTH];
    frame.write(&mut bytes).expect("written");
    responder.send(announced.from, &bytes);

    // The session the subscription was *not* told to read. It is the one the
    // far end would use if this were an ordinary subscriber, which is exactly
    // what it must no longer be.
    let mut reader = FarEnd::open(control);
    reader.send_a_setup(endpoint, STREAM_ID, READER_SESSION);

    let wrong_session = watch_for(
        &mut client,
        &mut reader,
        subscription_id,
        Duration::from_millis(500),
    );

    assert!(
        !wrong_session.image,
        "the completed subscription reads one session, not any: {wrong_session:?}"
    );

    // And the one it was told to read.
    reader.send_a_setup(endpoint, STREAM_ID, response_session);

    let right_session = watch_for(
        &mut client,
        &mut reader,
        subscription_id,
        Duration::from_secs(2),
    );

    assert!(
        right_session.image,
        "the session the RSP_SETUP named is the one it reads: {right_session:?}"
    );

    drop(client);
    let _ = own.stop();
}

/// ⑨: a `RSP_SETUP` whose session contradicts the one the subscription named
/// leaves it unreadable rather than quietly re-pointing it.
///
/// `aeron_driver_conductor.c:7084-7098`: a subscription that named
/// `session-id=` has said which stream it will read, and a response publication
/// that says otherwise is a setup failure, not a correction. The reference
/// drops the named session, poisons the link so later setups for the same
/// correlation id are ignored, and records the error.
///
/// The assertion is that **neither** session is read afterwards: not the one
/// the subscription named, and not the one the responder offered. A conductor
/// that took the frame at its word would read the offered session and pass a
/// test that only asked about the named one.
#[test]
fn a_session_the_subscription_did_not_name_poisons_the_link() {
    let Some(mut own) = OwnDriver::start("response-mismatch") else {
        driver::announce_own_skip();
        return;
    };

    own.await_cnc(READY_TIMEOUT)
        .expect("this driver must publish a readable CnC file");

    let mut client = Client::connect(own.aeron_dir()).expect("connect our client");

    let named_session = 999;
    let offered_session = 4242;

    let endpoint = free_udp_port(81);
    let control = free_udp_port(82);
    let subscription_id = client
        .add_subscription(
            &format!(
                "aeron:udp?endpoint=127.0.0.1:{endpoint}|control=127.0.0.1:{control}\
                 |control-mode=response|session-id={named_session}"
            ),
            STREAM_ID,
            DEFAULT_TIMEOUT,
        )
        .expect("a response channel is one this driver serves");

    let publication_port = free_udp_port(83);
    let _publication_id = client
        .add_publication(
            &format!(
                "aeron:udp?endpoint=127.0.0.1:{publication_port}\
                 |response-correlation-id={subscription_id}"
            ),
            STREAM_ID,
            DEFAULT_TIMEOUT,
        )
        .expect("our driver must confirm the UDP publication");

    let mut responder = FarEnd::open(publication_port);
    let announced = responder
        .await_setup(STREAM_ID, Duration::from_secs(5))
        .expect("a publication describes itself before anything can answer it");

    let frame = RspSetupFrame {
        session_id: announced.session_id,
        stream_id: STREAM_ID,
        response_session_id: offered_session,
    };
    let mut bytes = [0u8; RspSetupFrame::LENGTH];
    frame.write(&mut bytes).expect("written");
    responder.send(announced.from, &bytes);

    let mut reader = FarEnd::open(control);

    // Let the conductor take the frame before anything is sent at what it
    // registered. The driver's sender and receiver are separate threads and the
    // registration is the conductor's, so a `SETUP` sent in the same instant as
    // the `RSP_SETUP` can arrive before there is anything to match it — which
    // would make the assertions below true for the wrong reason.
    std::thread::sleep(Duration::from_millis(300));

    reader.send_a_setup(endpoint, STREAM_ID, offered_session);
    let offered = watch_for(
        &mut client,
        &mut reader,
        subscription_id,
        Duration::from_millis(500),
    );

    assert!(
        !offered.image,
        "the session the subscription did not name is not adopted: {offered:?}"
    );

    reader.send_a_setup(endpoint, STREAM_ID, named_session);
    let named = watch_for(
        &mut client,
        &mut reader,
        subscription_id,
        Duration::from_millis(500),
    );

    assert!(
        !named.image,
        "and the one it did name is dropped rather than read (`:7094-7096`): {named:?}"
    );

    drop(client);
    let _ = own.stop();
}

/// ⑩: a publication that claims to answer a subscription this driver does not
/// hold is refused, rather than created to answer nothing.
///
/// `aeron_driver_conductor.c:631-656`. The correlation id is a **registration
/// id**, which never crosses the wire, so a driver that accepted one it does
/// not hold would create a publication whose response half can never be found —
/// and the client would wait for an answer to a question nobody was asked.
///
/// The positive half of this is the two tests above, which name a subscription
/// this driver really holds and are created: a check that refused everything
/// would fail them.
#[test]
fn a_publication_that_names_no_ones_subscription_is_refused() {
    let Some(mut own) = OwnDriver::start("response-unknown-subscription") else {
        driver::announce_own_skip();
        return;
    };

    own.await_cnc(READY_TIMEOUT)
        .expect("this driver must publish a readable CnC file");

    let mut client = Client::connect(own.aeron_dir()).expect("connect our client");

    let nowhere = free_udp_port(91);
    let error = client
        .add_publication(
            &format!("aeron:udp?endpoint=127.0.0.1:{nowhere}|response-correlation-id=424242"),
            STREAM_ID,
            DEFAULT_TIMEOUT,
        )
        .expect_err("a correlation id no subscription holds is not a subscription");

    match error {
        CommandError::Driver { code, message } => {
            assert_eq!(
                deepmsg_cnc::command::ERROR_CODE_GENERIC_ERROR,
                code,
                "an EINVAL reaches a client as the generic code (`:2326-2341`)"
            );
            assert_eq!(
                "unable to find response subscription for response-correlation-id=424242",
                message
            );
        }
        other => panic!("the driver refuses it: {other:?}"),
    }

    drop(client);
    let _ = own.stop();
}

/// ⑩: the publication that asks for a response channel says so in its `SETUP`,
/// and one that *is* the answer does not.
///
/// `aeron_network_publication.c:392-400`: `SEND_RESPONSE` is set by a
/// publication that is not itself a response channel and names a correlation id
/// — the question — and never by one that is — the answer. That single bit is
/// the whole of how the far end learns it must reply with a `RSP_SETUP`, and it
/// is not echoed back, or the two ends would ask each other forever.
///
/// Three publications, one test, because the rule has two halves and a flag
/// that was always set would pass a test of either alone.
#[test]
fn the_publication_that_asks_for_a_response_says_so_in_its_setup() {
    let Some(mut own) = OwnDriver::start("response-setup-flag") else {
        driver::announce_own_skip();
        return;
    };

    own.await_cnc(READY_TIMEOUT)
        .expect("this driver must publish a readable CnC file");

    let mut client = Client::connect(own.aeron_dir()).expect("connect our client");

    // The subscription a response publication would answer. It is an ordinary
    // one: what `validate_response_subscription` asks is that the id names a
    // network subscription here, not what kind.
    let answer_endpoint = free_udp_port(101);
    let subscription_id = client
        .add_subscription(
            &format!("aeron:udp?endpoint=127.0.0.1:{answer_endpoint}"),
            STREAM_ID,
            DEFAULT_TIMEOUT,
        )
        .expect("our driver must confirm the UDP subscription");

    // The question: not a response channel, but made to answer one.
    let asking_port = free_udp_port(102);
    let _asking = client
        .add_publication(
            &format!(
                "aeron:udp?endpoint=127.0.0.1:{asking_port}\
                 |response-correlation-id={subscription_id}"
            ),
            STREAM_ID,
            DEFAULT_TIMEOUT,
        )
        .expect("our driver must confirm the UDP publication");

    let mut asker = FarEnd::open(asking_port);
    let asked = asker
        .await_setup(STREAM_ID, Duration::from_secs(5))
        .expect("a publication describes itself");

    assert_ne!(
        0,
        asked.flags & header_flags::SETUP_SEND_RESPONSE,
        "the publication that asks for a response channel says so: {:#04x}",
        asked.flags
    );

    // The answer: a response channel names a correlation id too, and must not
    // ask for a response of its own.
    //
    // The id it names is an **image's** (`find_response_publication_image`),
    // which is what the reference's own `response_server` names — it learns it
    // from the image the requester's `SETUP` made, and asks for no image of its
    // own. So one has to exist here: a requester sends a `SETUP` that asks for
    // a response channel, and this driver makes the image it describes.
    let mut requester = FarEnd::open(free_udp_port(105));
    requester.send_a_setup_asking(
        answer_endpoint,
        STREAM_ID,
        21,
        header_flags::SETUP_SEND_RESPONSE,
    );

    let image = await_image(&mut client, subscription_id, Duration::from_secs(5))
        .expect("a SETUP that asks for a response channel makes an image");

    // And the channel it answers on is the reference's response channel: a
    // `control=` and **no** `endpoint=` (`samples_configuration.h:34`). That
    // is not decoration. It is what makes the two ends meet — the requester's
    // receive endpoint elicits *to* the control address, and the publication's
    // send endpoint is bound there (`aeron_send_channel_endpoint.c:112-116`),
    // so the ask arrives; while the publication itself has nowhere to send
    // until the ask tells it where (`aeron_network_publication.c:355-378`).
    let response_control = free_udp_port(103);
    let _answering = client
        .add_publication(
            &format!(
                "aeron:udp?control=127.0.0.1:{response_control}\
                 |response-correlation-id={image}|control-mode=response"
            ),
            STREAM_ID,
            DEFAULT_TIMEOUT,
        )
        .expect("a response publication is one this driver serves");

    // The one fact a requester cannot guess is which session the publication
    // speaks on, and the image is where it is told
    // (`aeron_publication_image.c:899-923`).
    let said = requester
        .take_rsp_setups(Duration::from_secs(5))
        .pop()
        .expect("the image says which session it answers on");

    // Only now can the ask be made, and it is made by name: that session is
    // what the far end looks its publication up by.
    let mut answerer = FarEnd::open(free_udp_port(106));
    answerer.elicit(response_control, STREAM_ID, said.response_session_id);

    let answered = answerer
        .await_setup(STREAM_ID, Duration::from_secs(5))
        .expect("a publication describes itself");

    assert_eq!(
        0,
        answered.flags & header_flags::SETUP_SEND_RESPONSE,
        "the answer does not ask for one of its own, or the two ends would ask \
         each other forever: {:#04x}",
        answered.flags
    );

    // And the ordinary case, which is the one every other test relies on.
    let plain_port = free_udp_port(104);
    let _plain = client
        .add_publication(
            &format!("aeron:udp?endpoint=127.0.0.1:{plain_port}"),
            STREAM_ID,
            DEFAULT_TIMEOUT,
        )
        .expect("our driver must confirm the UDP publication");

    let mut plain = FarEnd::open(plain_port);
    let described = plain
        .await_setup(STREAM_ID, Duration::from_secs(5))
        .expect("a publication describes itself");

    assert_eq!(0, described.flags & header_flags::SETUP_SEND_RESPONSE);

    drop(client);
    let _ = own.stop();
}

/// The registration id of the first image a subscription sees, polled for.
fn await_image(client: &mut Client, registration_id: i64, within: Duration) -> Option<i64> {
    let deadline = Instant::now() + within;

    while Instant::now() < deadline {
        client.poll();

        if let Some(image) = client
            .subscription(registration_id)
            .and_then(|subscription| subscription.images().first())
        {
            return Some(image.registration_id());
        }

        std::thread::sleep(Duration::from_millis(5));
    }

    None
}

/// What the driver said when it refused a publication.
#[track_caller]
fn refusal(result: Result<i64, CommandError>) -> (i32, String) {
    match result {
        Err(CommandError::Driver { code, message }) => (code, message),
        Ok(registration_id) => {
            panic!("the driver served a publication it must refuse: {registration_id}")
        }
        Err(error) => panic!("the driver must refuse this: {error:?}"),
    }
}

/// ⑩: the image a response publication names is found by registration id — and
/// the id alone is not enough.
///
/// `find_response_publication_image` (`aeron_driver_conductor.c:1787-1833`)
/// walks the images for the id in `response-correlation-id` and then asks the
/// one it found whether its sender wanted a response channel. That is the flags
/// byte of the `SETUP` that made the image, which is the header's to say and is
/// nowhere in the frame's body — so it is the one thing a driver has to carry
/// across from the socket to the image to answer this question at all.
///
/// One driver, one command shape, and two hand-built `SETUP`s that differ by
/// that single byte. The control is what makes the first half evidence: a
/// driver that never carried the flag across refuses *both*, and a driver that
/// never looked at it serves both.
///
/// The four refusals are the reference's, and the last two are on a channel
/// that already has a publication — which is the *other* call site
/// (`:4350-4370`, where the image is looked for after the agreement with the
/// publication being shared, not before it).
#[test]
fn a_response_publication_finds_only_the_image_that_asked_for_one() {
    let Some(mut own) = OwnDriver::start("response-publication-image") else {
        driver::announce_own_skip();
        return;
    };

    own.await_cnc(READY_TIMEOUT)
        .expect("this driver must publish a readable CnC file");

    let mut client = Client::connect(own.aeron_dir()).expect("connect our client");

    // Two ordinary subscriptions. What a response publication names is an
    // *image*, and a response subscription never has one (⑨), so the image has
    // to be made for a reader.
    let asking_endpoint = free_udp_port(111);
    let asking = client
        .add_subscription(
            &format!("aeron:udp?endpoint=127.0.0.1:{asking_endpoint}"),
            STREAM_ID,
            DEFAULT_TIMEOUT,
        )
        .expect("our driver must confirm the UDP subscription");

    let quiet_endpoint = free_udp_port(112);
    let quiet = client
        .add_subscription(
            &format!("aeron:udp?endpoint=127.0.0.1:{quiet_endpoint}"),
            STREAM_ID + 1,
            DEFAULT_TIMEOUT,
        )
        .expect("our driver must confirm the UDP subscription");

    // The far end makes both images. The one difference between them is the
    // byte under test.
    let asker = FarEnd::open(free_udp_port(113));
    asker.send_a_setup_asking(
        asking_endpoint,
        STREAM_ID,
        11,
        header_flags::SETUP_SEND_RESPONSE,
    );

    let quiet_far = FarEnd::open(free_udp_port(114));
    quiet_far.send_a_setup(quiet_endpoint, STREAM_ID + 1, 12);

    let asking_image = await_image(&mut client, asking, Duration::from_secs(5))
        .expect("a SETUP for a stream we read makes an image");
    let quiet_image = await_image(&mut client, quiet, Duration::from_secs(5))
        .expect("a SETUP for a stream we read makes an image");

    // The image that asked for a response channel is the one this answers, and
    // this is the whole of what the id means — it names an image on this
    // driver, and nothing about it crossed the wire.
    let answering_endpoint = free_udp_port(115);
    let answered_channel = format!(
        "aeron:udp?endpoint=127.0.0.1:{answering_endpoint}\
         |control-mode=response|response-correlation-id={asking_image}"
    );
    let answering = client
        .add_publication(&answered_channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("the image that asked for a response channel is one this answers");

    // The same command on the same channel is the *same* publication, so this
    // is the shared path — where the reference looks the image up after the
    // agreement rather than before it, and finds it again.
    let _shared = client
        .add_publication(&answered_channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a publication that already answers this image may be shared");

    // The image that never asked. Same channel, same stream, same command — the
    // correlation id is the only difference, and it is one the driver must
    // refuse: the image is real, and it is answering nothing.
    let (code, message) = refusal(client.add_publication(
        &format!(
            "aeron:udp?endpoint=127.0.0.1:{answering_endpoint}\
             |control-mode=response|response-correlation-id={quiet_image}"
        ),
        STREAM_ID,
        DEFAULT_TIMEOUT,
    ));

    assert_eq!(deepmsg_cnc::command::ERROR_CODE_GENERIC_ERROR, code);
    assert_eq!(
        format!("image.correlationId={quiet_image} did not request a response channel"),
        message
    );

    // An id that names no image at all.
    let absent = i64::MAX - 1;
    let (code, message) = refusal(client.add_publication(
        &format!(
            "aeron:udp?endpoint=127.0.0.1:{answering_endpoint}\
             |control-mode=response|response-correlation-id={absent}"
        ),
        STREAM_ID,
        DEFAULT_TIMEOUT,
    ));

    assert_eq!(deepmsg_cnc::command::ERROR_CODE_GENERIC_ERROR, code);
    assert_eq!(format!("image.correlationId={absent} not found"), message);

    // A response channel that names nothing cannot name an image either. This
    // one is a channel of its own, so it is the *other* call site: the image is
    // looked for before the log buffer is asked for.
    let unnamed_endpoint = free_udp_port(116);
    let (code, message) = refusal(client.add_publication(
        &format!("aeron:udp?endpoint=127.0.0.1:{unnamed_endpoint}|control-mode=response"),
        STREAM_ID,
        DEFAULT_TIMEOUT,
    ));

    assert_eq!(deepmsg_cnc::command::ERROR_CODE_GENERIC_ERROR, code);
    assert_eq!(
        "control-mode=response was specified, but no response-correlation-id set",
        message
    );

    // What was served is still served: the refusals did not take the
    // publication with them.
    assert!(client.publication(answering).is_some());

    drop(client);
    let _ = own.stop();
}

/// ⑩: the image answers the publication that asked for a response channel, and
/// then stops once it has been answered.
///
/// The publisher that asked for a response channel cannot know the session its
/// responses will arrive on — that is the *image's* session, on the far side of
/// the handshake — and a `RSP_SETUP` is the only frame that says it
/// (`aeron_receive_channel_endpoint_send_response_setup`,
/// `media/aeron_receive_channel_endpoint.c:432-466`). It goes out on the
/// status-message timer, because an image has no other timer, and it goes out
/// through the connection the request arrived on
/// (`aeron_publication_image_send_pending_status_message`, `:899-923`).
///
/// It stops when the publication reports a live receiver, which is the end of
/// the handshake: the conductor clears what it set
/// (`aeron_driver_conductor.c:4227-4232` sets it, `:7117-7131` clears it).
///
/// Both halves are asserted, and the second is what makes the first mean
/// something: an image that said it once and an image that says it forever both
/// satisfy "a `RSP_SETUP` arrived", and only the second is a handshake that
/// finishes.
#[test]
fn the_image_answers_the_publication_that_asked_for_a_response_channel() {
    let Some(mut own) = OwnDriver::start("response-image-answers") else {
        driver::announce_own_skip();
        return;
    };

    own.await_cnc(READY_TIMEOUT)
        .expect("this driver must publish a readable CnC file");

    let mut client = Client::connect(own.aeron_dir()).expect("connect our client");

    // The request side: an ordinary subscription, because an image is made for
    // a stream this driver reads.
    let request_endpoint = free_udp_port(121);
    let request = client
        .add_subscription(
            &format!("aeron:udp?endpoint=127.0.0.1:{request_endpoint}"),
            STREAM_ID,
            DEFAULT_TIMEOUT,
        )
        .expect("our driver must confirm the UDP subscription");

    // The far end that asks for a response channel. It binds its own port, and
    // that address is where the answer will come back — it is the connection's
    // control address, because this channel named no `control=`.
    let mut requester = FarEnd::open(free_udp_port(122));
    requester.send_a_setup_asking(
        request_endpoint,
        STREAM_ID,
        31,
        header_flags::SETUP_SEND_RESPONSE,
    );

    let image = await_image(&mut client, request, Duration::from_secs(5))
        .expect("a SETUP that asks for a response channel makes an image");

    // The answer: a response publication naming that image, on the reference's
    // response channel — a `control=` and no `endpoint=` at all
    // (`samples_configuration.h:34`), which is what gives the requester's ask
    // somewhere to arrive.
    let response_control = free_udp_port(123);
    let _answering = client
        .add_publication(
            &format!(
                "aeron:udp?control=127.0.0.1:{response_control}\
                 |control-mode=response|response-correlation-id={image}"
            ),
            STREAM_ID,
            DEFAULT_TIMEOUT,
        )
        .expect("the image that asked for a response channel is one this answers");

    // The first half. The session the image says is the publication's own,
    // read by the *other* end of the pair — which is the point: it is the one
    // fact the publisher could not have known and had to be told, and it is
    // also the only thing that makes the ask below land, because the far end
    // looks its publication up by `(stream << 32) | session`.
    let said = requester
        .take_rsp_setups(Duration::from_secs(5))
        .pop()
        .expect("the image answers a publication that asked for a response channel");

    // Now the requester can ask — from the socket that means to receive the
    // answer, since that is the address the publication is entitled to reply
    // to (`aeron_network_publication.h:263-273`).
    let mut answerer = FarEnd::open(free_udp_port(124));
    answerer.elicit(response_control, STREAM_ID, said.response_session_id);

    let described = answerer
        .await_setup(STREAM_ID, Duration::from_secs(5))
        .expect("a publication describes itself");

    assert_eq!(
        described.session_id, said.response_session_id,
        "the response session is the publication's, which is what the handshake is for"
    );

    // The second half. A subscriber answers the publication, so it has a live
    // receiver — and the image that was saying the session has nothing left to
    // say. Two status-message periods is long enough that an image still saying
    // it would have said it again.
    let _ = requester.take_rsp_setups(Duration::from_millis(50));

    answerer.send_a_status_message(described.from, &described);

    let afterwards = requester.take_rsp_setups(Duration::from_millis(700));

    assert!(
        afterwards.is_empty(),
        "an answered image stops saying the session: {} more arrived",
        afterwards.len()
    );

    drop(client);
    let _ = own.stop();
}
