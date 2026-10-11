//! Reading here and writing there, in one pass and without the copy between.
//!
//! The shape an echo node, a proxy or a replay bridge has: a message arrives on
//! a subscription and goes out on a publication, and the caller wants to do
//! both from inside the poll rather than copy every message out, wait for the
//! poll to return, and publish it afterwards. The reference gets this for free
//! because its `Publication` and `Subscription` are objects that borrow no
//! client (`EchoNode.java:98-99`); `Client::poll_subscription_fragments_with_publication`
//! is the same thing for one call, and the offer it lends reads its window
//! limit out of the CnC counter on every call, exactly as the reference's does
//! (`ExclusivePublication.java:244`).
//!
//! Everything here is our own driver and our own client over `aeron:ipc`, so it
//! needs no reference checkout and runs in CI.

use std::time::{Duration, Instant};

use deepmsg_client::client::{Client, DEFAULT_TIMEOUT};
use deepmsg_client::fragment_assembler::FragmentAssembler;
use deepmsg_core::logbuffer::append::Appended;
use deepmsg_tests::driver::{self, OwnDriver};

/// Shared memory with a term small enough that a long message has to be split:
/// `AERON_IPC_MTU_LENGTH_DEFAULT` is 1408 (`aeron_driver_context.c`), and the
/// term only has to be long enough to hold a few of them.
const CHANNEL: &str = "aeron:ipc?term-length=64k";

/// The stream a message is published on, and the one it is forwarded to. Two
/// streams so that the forwarder does not read back what it wrote.
const INBOUND_STREAM: i32 = 7001;
const OUTBOUND_STREAM: i32 = 7002;

/// A message that fits one frame, and one that does not.
const MESSAGE: &[u8] = b"a message a forwarder has to carry without changing";
const LONG_MESSAGE: &[u8] = &[0x5a_u8; 4_000];

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

/// The four resources a forwarding run holds: something to read, something to
/// write, and the same again on the far side, so that what was forwarded can be
/// read back and compared.
struct Links {
    inbound_publication: i64,
    inbound_subscription: i64,
    outbound_publication: i64,
    outbound_subscription: i64,
}

fn links(client: &mut Client) -> Links {
    let inbound_publication = client
        .add_publication(CHANNEL, INBOUND_STREAM, DEFAULT_TIMEOUT)
        .expect("an inbound publication");
    let inbound_subscription = client
        .add_subscription(CHANNEL, INBOUND_STREAM, DEFAULT_TIMEOUT)
        .expect("an inbound subscription");
    let outbound_publication = client
        .add_exclusive_publication(CHANNEL, OUTBOUND_STREAM, DEFAULT_TIMEOUT)
        .expect("an outbound publication");
    let outbound_subscription = client
        .add_subscription(CHANNEL, OUTBOUND_STREAM, DEFAULT_TIMEOUT)
        .expect("an outbound subscription");

    Links {
        inbound_publication,
        inbound_subscription,
        outbound_publication,
        outbound_subscription,
    }
}

/// Poll until both subscriptions hold an image, which is what a forwarder waits
/// for before it can move anything.
fn await_images(client: &mut Client, links: &Links) {
    let deadline = Instant::now() + DEADLINE;

    while client
        .subscription(links.inbound_subscription)
        .is_none_or(|s| s.images().is_empty())
        || client
            .subscription(links.outbound_subscription)
            .is_none_or(|s| s.images().is_empty())
    {
        assert!(
            Instant::now() < deadline,
            "the subscriptions never found their images"
        );
        client.poll();
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// Publish `payload` on the inbound publication, offering until the driver
/// takes it.
fn publish(client: &mut Client, publication: i64, payload: &[u8]) {
    let deadline = Instant::now() + DEADLINE;

    loop {
        assert!(Instant::now() < deadline, "the publication never took it");

        // Either kind of publication, because this run has both: a message
        // goes in on a shared one and comes out of an exclusive one, and the
        // two live in different lists of the client.
        let outcome = client
            .offer(publication, payload)
            .or_else(|| client.offer_exclusive(publication, payload))
            .expect("the publication this is offering on");

        match outcome {
            Appended::Ok { .. } => return,
            _ => {
                client.poll();
                std::thread::sleep(Duration::from_millis(1));
            }
        }
    }
}

/// The **messages** readable on a subscription, in order, copied out.
///
/// The message door and not the fragment door, deliberately: what came out of
/// the forwarder may be a message of several frames — the long one is — and a
/// reader comparing forwarded bytes against sent ones is comparing messages.
fn drain(client: &mut Client, subscription: i64, wanted: usize) -> Vec<Vec<u8>> {
    let deadline = Instant::now() + DEADLINE;
    let mut messages: Vec<Vec<u8>> = Vec::new();

    while messages.len() < wanted {
        assert!(
            Instant::now() < deadline,
            "the forwarded message never arrived: {} of {wanted}",
            messages.len()
        );

        client.poll();
        client.poll_subscription(subscription, 10, |message| {
            messages.push(message.payload.to_vec());
        });

        std::thread::sleep(Duration::from_millis(1));
    }

    messages
}

/// One pass of the forwarder: read what is readable and write it on.
///
/// The two doors, as a node has them: a message in one frame goes straight
/// through — `PublicationWriter::offer` takes the fragment and copies its
/// payload into the outbound log without the caller ever holding it — and a
/// message in several frames is put back together first, because a fragment is
/// not a message.
fn forward(
    client: &mut Client,
    from: i64,
    to: i64,
    assembler: &mut FragmentAssembler,
) -> Vec<Appended> {
    let mut outcomes = Vec::new();

    client.poll_subscription_fragments_with_publication(from, 10, to, |fragment, writer| {
        if fragment.is_unfragmented() {
            outcomes.push(writer.offer(fragment));
            return;
        }

        assembler.push(fragment, &mut |message| {
            outcomes.push(writer.offer_bytes(message.payload));
        });
    });

    outcomes
}

/// The forwarder's own poll loop, for as long as it takes to move `wanted`
/// messages.
fn forward_until(
    client: &mut Client,
    links: &Links,
    wanted: usize,
    assembler: &mut FragmentAssembler,
) {
    let deadline = Instant::now() + DEADLINE;
    let mut forwarded = 0;

    while forwarded < wanted {
        assert!(
            Instant::now() < deadline,
            "the forwarder only moved {forwarded} of {wanted}"
        );

        client.poll();

        let outcomes = forward(
            client,
            links.inbound_subscription,
            links.outbound_publication,
            assembler,
        );
        for outcome in outcomes {
            assert!(
                matches!(outcome, Appended::Ok { .. }),
                "a forward was not taken: {outcome:?}"
            );
            forwarded += 1;
        }

        std::thread::sleep(Duration::from_millis(1));
    }
}

/// A message goes in on one stream and comes out on the other, unchanged.
#[test]
fn a_message_forwarded_from_a_poll_arrives_unchanged() {
    let Some((_own, mut client)) = own_driver_and_client("forwarding-poll") else {
        driver::announce_own_skip();
        return;
    };

    let links = links(&mut client);
    await_images(&mut client, &links);

    publish(&mut client, links.inbound_publication, MESSAGE);

    let mut assembler = FragmentAssembler::new();
    forward_until(&mut client, &links, 1, &mut assembler);

    let forwarded = drain(&mut client, links.outbound_subscription, 1);

    assert_eq!(vec![MESSAGE.to_vec()], forwarded);
}

/// The same message moved the other way — copied out of the poll, published
/// afterwards — reaches the outbound stream as the same bytes.
///
/// The two ways of forwarding have to agree, because the point of the first one
/// is that it is the second one without a copy: a difference here would be the
/// copy doing something a copy must not.
#[test]
fn the_forwarded_bytes_are_the_ones_the_copying_path_writes() {
    let Some((_own, mut client)) = own_driver_and_client("forwarding-copy") else {
        driver::announce_own_skip();
        return;
    };

    let links = links(&mut client);
    await_images(&mut client, &links);

    publish(&mut client, links.inbound_publication, MESSAGE);

    // Read it the way a caller without a lent writer has to: a message at a
    // time, into a buffer of the caller's own, published once the poll returns.
    let deadline = Instant::now() + DEADLINE;
    let mut copied: Vec<Vec<u8>> = Vec::new();

    while copied.is_empty() {
        assert!(Instant::now() < deadline, "nothing to copy out");
        client.poll();
        client.poll_subscription(links.inbound_subscription, 10, |message| {
            copied.push(message.payload.to_vec());
        });
        std::thread::sleep(Duration::from_millis(1));
    }

    for payload in &copied {
        publish(&mut client, links.outbound_publication, payload);
    }

    let written = drain(&mut client, links.outbound_subscription, copied.len());

    assert_eq!(copied, written, "the copy path changed nothing");
    assert_eq!(vec![MESSAGE.to_vec()], written, "and it is the message");
}

/// A message wider than one frame is put back together and forwarded whole.
///
/// The branch a forwarder cannot skip: a fragment is not a message, so
/// `PublicationWriter::offer` refuses one that is not a whole message and the
/// caller reassembles first. Forwarding each fragment would put three messages
/// on the outbound stream where one went in.
#[test]
fn a_forwarded_message_of_several_frames_arrives_whole() {
    let Some((_own, mut client)) = own_driver_and_client("forwarding-fragments") else {
        driver::announce_own_skip();
        return;
    };

    let links = links(&mut client);
    await_images(&mut client, &links);

    publish(&mut client, links.inbound_publication, LONG_MESSAGE);

    let mut assembler = FragmentAssembler::new();
    forward_until(&mut client, &links, 1, &mut assembler);

    let forwarded = drain(&mut client, links.outbound_subscription, 1);

    assert_eq!(
        vec![LONG_MESSAGE.to_vec()],
        forwarded,
        "one message in, one message out, and the same bytes"
    );
}
