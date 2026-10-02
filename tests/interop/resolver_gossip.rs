//! Q3: our `driver` name resolver and the reference's gossip with each other.
//!
//! This is the slice's positive evidence for the "can replace the driver"
//! criterion, and it is the one thing a test of ours alone cannot show: a
//! resolver that speaks the RES datagram correctly to *itself* is a resolver
//! whose dialect is its own invention. Two of ours agreeing proves nothing.
//!
//! The arrangement is the reference's own test's **asymmetric** one
//! (`DriverNameResolverSystemTest.shouldSeeNeighbor`,
//! `aeron-system-tests/src/test/java/io/aeron/driver/DriverNameResolverSystemTest.java:121-146`):
//! only the *reference* driver is told where to look; ours is told nothing and
//! has no one to ask. It can only come to know the reference's resolver by
//! **receiving a RES frame from it and parsing it** — which is the whole of
//! what "they speak the same dialect" means, and it cannot be arranged by
//! accident: a resolver that answered only its own frames would stay at zero.
//!
//! Both are then read from the counter files, because the two drivers have no
//! API in common except that one. The counter is
//! `Resolver neighbors: bound <one's own socket>` with the count as its value,
//! so `1` on each side is one neighbor each, and the only neighbor either could
//! have is the other.

use std::time::{Duration, Instant};

use deepmsg_cnc::CncFile;
use deepmsg_tests::driver::{self, OwnDriver, READY_TIMEOUT, ReferenceDriver};

/// The socket each resolver announces, and binds.
///
/// Fixed, as the reference's own test fixes them, and distinct from the ports
/// that test uses (8050/8051) so that a stray run of either cannot collide with
/// this one.
const OUR_INTERFACE: &str = "0.0.0.0:8121";
const THEIR_INTERFACE: &str = "0.0.0.0:8122";

/// `AERON_COUNTER_NAME_RESOLVER_NEIGHBORS_COUNTER_TYPE_ID`
/// (`aeron-client/src/main/c/aeron_counters.h`), the counter each resolver
/// keeps of the neighbors it believes in.
const NEIGHBORS_TYPE_ID: i32 = 15;

/// How long two resolvers are given to find each other.
///
/// Their own intervals are the defaults — a self-resolution every second, a
/// neighbor resolution (and so a bootstrap retry) every two — so this is a
/// generous multiple of the slowest of them rather than a tuned number.
const GOSSIP_DEADLINE: Duration = Duration::from_secs(30);

/// What one driver's counter file says about its neighbors: each counter's
/// value, and the label it was given.
type Neighbors = Vec<(i64, String)>;

/// The neighbors one driver reports.
fn neighbors(dir: &std::path::Path) -> Neighbors {
    let Ok(cnc) = CncFile::try_open(dir) else {
        return Vec::new();
    };

    let Some(reader) = cnc.counters() else {
        return Vec::new();
    };

    let mut found = Vec::new();
    reader.for_each(|counter| {
        if NEIGHBORS_TYPE_ID == counter.type_id {
            found.push((counter.value, counter.label.clone()));
        }
    });

    found
}

/// The one neighbor a driver reports, if it has exactly one.
fn sole_neighbor(reported: &[(i64, String)]) -> Option<&str> {
    let mut with_a_neighbor = reported.iter().filter(|(value, _)| *value > 0);

    match (with_a_neighbor.next(), with_a_neighbor.next()) {
        (Some((value, label)), None) if *value == 1 => Some(label),
        _ => None,
    }
}

/// Poll both drivers until each reports one neighbor, or the deadline passes.
fn await_both(ours: &std::path::Path, theirs: &std::path::Path) -> (Neighbors, Neighbors) {
    let start = Instant::now();
    let mut our_neighbors = Vec::new();
    let mut their_neighbors = Vec::new();

    while start.elapsed() < GOSSIP_DEADLINE {
        our_neighbors = neighbors(ours);
        their_neighbors = neighbors(theirs);

        if sole_neighbor(&our_neighbors).is_some() && sole_neighbor(&their_neighbors).is_some() {
            break;
        }

        std::thread::sleep(Duration::from_millis(100));
    }

    (our_neighbors, their_neighbors)
}

#[test]
fn our_resolver_and_the_reference_resolver_see_each_other() {
    let Some(binary) = driver::locate() else {
        driver::announce_skip();
        return;
    };

    // **No bootstrap neighbor**: the whole point of the arrangement. This
    // resolver has no one to ask and nothing to resolve, so the only way it
    // can come to hold a neighbor is by hearing from one.
    let Some(mut ours) = OwnDriver::start_with(
        "resolver-gossip-ours",
        &[
            "-Daeron.name.resolver.supplier=driver",
            "-Daeron.driver.resolver.name=deepmsg",
            &format!("-Daeron.driver.resolver.interface={OUR_INTERFACE}"),
        ],
    ) else {
        driver::announce_own_skip();
        return;
    };

    let Ok(mut theirs) = ReferenceDriver::start_with(
        &binary,
        "resolver-gossip-theirs",
        &[
            "-Daeron.name.resolver.supplier=driver",
            "-Daeron.driver.resolver.name=aeron",
            &format!("-Daeron.driver.resolver.interface={THEIR_INTERFACE}"),
            "-Daeron.driver.resolver.bootstrap.neighbor=127.0.0.1:8121",
        ],
    ) else {
        driver::announce_skip();
        return;
    };

    ours.await_cnc(READY_TIMEOUT)
        .expect("our driver must publish a readable CnC file");
    theirs
        .await_cnc(READY_TIMEOUT)
        .expect("the reference's must too");

    let (our_neighbors, their_neighbors) = await_both(ours.aeron_dir(), theirs.aeron_dir());

    let Some(our_label) = sole_neighbor(&our_neighbors) else {
        panic!(
            "our resolver never came to know the reference's within {GOSSIP_DEADLINE:?}, and it \
             had no bootstrap neighbor to learn it from second-hand.\n\
             it reported: {our_neighbors:?}\n\
             our driver said:\n{}\n\
             their driver said:\n{}",
            ours.log_tail(20),
            theirs.log_tail(20),
        );
    };

    let Some(their_label) = sole_neighbor(&their_neighbors) else {
        panic!(
            "the reference's resolver never came to know ours within {GOSSIP_DEADLINE:?}: \
             {their_neighbors:?}\n\
             their driver said:\n{}",
            theirs.log_tail(20),
        );
    };

    // The label is each resolver's **own** bound socket, whatever wildcard it
    // was configured with — the reference asserts the same substitution on its
    // side (`DriverNameResolverSystemTest.java:143-146`), and it is what a
    // resolver announces of itself, so a mismatch here would mean the two ends
    // disagree about who is who.
    assert!(
        our_label.ends_with(OUR_INTERFACE),
        "our resolver's counter does not name its own socket: {our_label}"
    );
    assert!(
        their_label.ends_with(THEIR_INTERFACE),
        "the reference resolver's counter does not name its own socket: {their_label}"
    );
}
