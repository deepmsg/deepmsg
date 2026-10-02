//! Two publications on one channel, which are two handles on one publication.
//!
//! The reference's wire answers a client with **two** ids
//! (`aeron_publication_buffers_ready_t`, `aeron_driver_conductor.c:2387-2388`):
//! the `correlation_id` of the `ADD_PUBLICATION` that is being answered, which
//! is the handle the client keeps and removes by, and the publication's own
//! `registration_id`, which every client on that channel shares. A client that
//! took the second as its handle would hold the *same* handle twice, and closing
//! one would close the channel under the other.
//!
//! That is not hypothetical: it is what made the reference's own
//! `NameReResolutionTest.shouldReResolveUnicastAddressWhenSendChannelEndpointIsReused`
//! fail here with `send_channel_endpoint found in CLOSING state, please retry`
//! — the second publication closed the first's endpoint, and the third
//! `ADD_PUBLICATION` found it gone.
//!
//! Everything here is our own client against our own driver, so it needs no
//! reference checkout and runs in CI.

use std::time::{Duration, Instant};

use deepmsg_client::client::{Client, DEFAULT_TIMEOUT};
use deepmsg_tests::driver::{self, OwnDriver};

const STREAM_ID: i32 = 1001;
const CHANNEL: &str = "aeron:udp?endpoint=127.0.0.1:24326";

const DEADLINE: Duration = Duration::from_secs(30);

#[test]
fn a_second_publication_on_one_channel_is_its_own_handle() {
    let Some(own) = OwnDriver::start("publication-links") else {
        driver::announce_own_skip();
        return;
    };

    let mut client = Client::connect(own.aeron_dir()).expect("a client");

    let first = client
        .add_publication(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a publication");

    let second = client
        .add_publication(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a second publication on the same channel");

    assert_ne!(
        first, second,
        "each add is its own handle, though both map one log buffer"
    );

    // Closing one leaves the other publishing: the endpoint is shared, and the
    // first client's removal must not take it.
    client
        .remove_publication(first, DEFAULT_TIMEOUT)
        .expect("the first is removed");
    client
        .remove_publication(second, DEFAULT_TIMEOUT)
        .expect("and the second is still there to remove");

    // And once both are gone the channel is free again, which is what the
    // reference's own test waits for.
    let start = Instant::now();
    let mut attempts = 0;

    loop {
        attempts += 1;

        match client.add_publication(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT) {
            Ok(registration_id) => {
                client
                    .remove_publication(registration_id, DEFAULT_TIMEOUT)
                    .expect("the publication is removed");

                return;
            }
            Err(error) => {
                assert!(
                    error.to_string().contains("CLOSING"),
                    "a publication that cannot be added on a free channel: {error}"
                );
            }
        }

        assert!(
            start.elapsed() < DEADLINE,
            "the endpoint never became reusable ({attempts} attempts)"
        );

        std::thread::sleep(Duration::from_millis(10));
    }
}
