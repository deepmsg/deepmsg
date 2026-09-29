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

use std::time::{Duration, Instant};

use deepmsg_client::client::{Client, DEFAULT_TIMEOUT};
use deepmsg_driver::protocol::SetupFrame;
use deepmsg_driver::sys::AddressFamily;
use deepmsg_driver::sys::socket::{DatagramSocket, Datagrams};
use deepmsg_tests::driver::{self, OwnDriver, READY_TIMEOUT};

const STREAM_ID: i32 = 1001;

/// A UDP port nobody is listening on, derived from the process id so that two
/// drivers on one machine collide only by choosing the same pair.
fn free_udp_port(offset: u16) -> u16 {
    #[allow(clippy::cast_possible_truncation)] // the low bits of a pid
    let base = 20_000 + (std::process::id() as u16 % 20_000);

    base.saturating_add(offset)
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

    /// A `SETUP` for `stream_id`, sent to `port`.
    fn send_a_setup(&self, port: u16, stream_id: i32) {
        let setup = SetupFrame {
            term_offset: 0,
            session_id: 7,
            stream_id,
            initial_term_id: 1_000,
            active_term_id: 1_000,
            term_length: 64 * 1024,
            mtu: 1408,
            ttl: 0,
        };
        let mut frame = [0u8; SetupFrame::LENGTH];
        assert!(setup.write_with_flags(&mut frame, 0).is_some());

        let _ = self.socket.send_batch(
            Some(format!("127.0.0.1:{port}").parse().expect("an address")),
            &[&frame],
        );
    }

    /// Take one datagram, if one has arrived.
    fn take(&mut self) -> Option<Vec<u8>> {
        match self
            .socket
            .receive_batch(&mut self.buffers, &mut self.datagrams)
        {
            Ok(0) | Err(_) => None,
            Ok(_) => {
                let datagram = self.datagrams.as_slice()[0];
                Some(self.buffers[0][..datagram.length].to_vec())
            }
        }
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
    reader_far.send_a_setup(reader_endpoint, STREAM_ID);

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
    responder_far.send_a_setup(response_endpoint, STREAM_ID);

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
