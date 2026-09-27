//! Client records: who this driver knows, and when it stops knowing them.
//!
//! There is no client-registration command in this protocol and no client
//! record region in the CnC file. A client exists to the driver because a
//! **resource command** carried its id — `get_or_add_client`
//! (`aeron-driver/src/main/c/aeron_driver_conductor.c:982-1035`), called from
//! the seven resource paths and from nowhere else — and it stops existing when
//! its **heartbeat counter** goes quiet for longer than
//! `aeron.client.liveness.timeout`.
//!
//! # The heartbeat is a counter, not a message
//!
//! Registration allocates a counter of type `11` labelled
//! `client-heartbeat: id=<clientId>` with the client's id as its key,
//! registration and owner (`aeron_position.c:320-337`), and stamps it with the
//! current millisecond. From then on the *client* writes that value on its own
//! duty cycle (`aeron_client_conductor.c:1376`) — `CLIENT_KEEPALIVE` exists in
//! the protocol, but this driver's only use for it is refreshing a client it
//! already knows, and the reference C client never sends it at all.
//!
//! # A pass is two phases, and the split is visible on the wire
//!
//! `aeron_client_on_time_event` (`:1038-1056`) runs over **every** client
//! first: it counts the timeout in system counter 24, announces
//! `ON_CLIENT_TIMEOUT` — unless the client closed itself — and announces
//! `ON_UNAVAILABLE_COUNTER` for the heartbeat counter. Only then does the
//! reclamation pass call `aeron_client_delete` (`:1218-1295`) for each client
//! that reached end of life, freeing its counters one by one (each announced)
//! and the heartbeat last. So [`Clients::on_time_event`] marks and announces,
//! and [`Clients::reap_expired`] frees — and when two clients expire in the
//! same tick, both of their timeouts precede both of their reclamations, which
//! is the order a C driver emits and an interleaved loop does not.
//!
//! A client that is already gone must not be announced as timed out:
//! `CLIENT_CLOSE` sets the heartbeat to zero so the next tick collects it, and
//! `closed_by_command` is what tells the two apart (`:5269-5280`, `:6321-6331`).

use deepmsg_cnc::command::{ImageBuffersReady, PublicationBuffersReady};
use deepmsg_cnc::counters::CLIENT_HEARTBEAT_TYPE_ID;
use deepmsg_cnc::{CounterManager, CounterRegions};

use crate::ipc_publications::IpcPublications;
use crate::ipc_subscriptions::IpcSubscriptions;
use crate::system_counters;

/// Where a driver→client event goes.
///
/// Implemented by the conductor, which encodes and broadcasts it; implemented
/// by tests as a recording sink. The events are named after the protocol's
/// messages rather than after the driver's internals, because the order they
/// arrive in *is* the wire order — and the order is the part that is easy to
/// get wrong and impossible to see in a byte dump.
pub trait ClientEvents {
    /// `ON_COUNTER_READY` for the counter at `counter_id`; the client knows it
    /// by `registration_id` (its own client id, when this is its heartbeat).
    fn counter_ready(&mut self, registration_id: i64, counter_id: i32);

    /// `ON_UNAVAILABLE_COUNTER`: the counter is gone, and a reader holding the
    /// id must let it go.
    fn counter_unavailable(&mut self, registration_id: i64, counter_id: i32);

    /// `ON_CLIENT_TIMEOUT`: the client stopped being heard from. System counter
    /// 24 is incremented by the pool before this is raised
    /// (`aeron_driver_conductor.c:1047-1049`), which is also why it is *not*
    /// raised for a client that closed itself.
    fn client_timed_out(&mut self, client_id: i64);

    /// `ON_OPERATION_SUCCEEDED`: the command with this correlation id is done.
    ///
    /// The completion signal for a command that has no reply of its own. A real
    /// client blocks on it: without it, a removal that *worked* leaves that
    /// client waiting for a deadline and then reporting a timeout.
    fn operation_succeeded(&mut self, correlation_id: i64);

    /// `ON_ERROR`: the command with this correlation id failed.
    fn error(&mut self, correlation_id: i64, error_code: i32, message: &[u8]);

    /// `ON_SUBSCRIPTION_READY`: the subscription exists. It is sent *before*
    /// any image, because a client that heard about an image first would have
    /// an image for a subscription it has not been told about.
    fn subscription_ready(&mut self, registration_id: i64, channel_status_indicator_id: i32);

    /// `ON_AVAILABLE_IMAGE`: a publication this subscription matches exists,
    /// and its log buffer can be mapped from [`ImageBuffersReady::log_file`].
    fn available_image(&mut self, ready: &ImageBuffersReady<'_>);

    /// `ON_UNAVAILABLE_IMAGE`: an image this subscription was reading is gone,
    /// and the client must unmap it. The channel is the **subscription's** —
    /// the one the client subscribed with — except where a publication is
    /// revoked, which sends the constant `aeron:ipc` instead
    /// (`aeron_driver_conductor.c:510-517` against `:5975-5982`).
    fn unavailable_image(
        &mut self,
        correlation_id: i64,
        subscription_registration_id: i64,
        stream_id: i32,
        channel: &[u8],
    );

    /// `ON_PUBLICATION_READY` or `ON_EXCLUSIVE_PUBLICATION_READY`: the log
    /// buffer exists and the client may map it.
    ///
    /// Which of the two type ids is sent says whether the log buffer may be
    /// shared with another producer, so it is part of the message rather than
    /// something the client can work out for itself.
    fn publication_ready(&mut self, ready: &PublicationBuffersReady<'_>, is_exclusive: bool);
}

/// One counter a client owns, by the id the client knows it as.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CounterLink {
    /// The client's correlation id for the allocation — not the client id.
    pub registration_id: i64,
    /// The counter itself.
    pub counter_id: i32,
}

/// What the driver knows about one client.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientRecord {
    /// The client's id; `-1` once reaped.
    pub client_id: i64,
    /// The client sent `CLIENT_CLOSE` (or its conductor did on shutdown).
    pub closed_by_command: bool,
    /// The timeout has fired and this record's turn to be reaped is now.
    pub reached_end_of_life: bool,
    /// The heartbeat counter this client lives by; `-1` once reaped.
    pub heartbeat_counter_id: i32,
    /// `aeron.client.liveness.timeout`, in milliseconds, taken per client at
    /// registration the way the reference takes it (`:1020-1021`).
    pub liveness_timeout_ms: i64,
    /// Counters this client allocated, in allocation order.
    pub counter_links: Vec<CounterLink>,
    /// Publications this client holds, in the order it asked for them.
    pub publication_links: Vec<PublicationLink>,
}

/// One client's hold on a publication (`aeron_publication_link_t`,
/// `aeron-driver/src/main/c/aeron_driver_common.h:212-217`).
///
/// Two ids, and they are not interchangeable: the first is what the client
/// called this `ADD_PUBLICATION`, the second is the publication itself. A
/// removal is matched by the first and acted on the second.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PublicationLink {
    /// The client's correlation id for the `ADD_PUBLICATION` that made it.
    pub registration_id: i64,
    /// The publication's own registration id — the log file's name.
    pub publication_registration_id: i64,
}

/// The clients this driver knows about.
///
/// Reaping iterates from the back and removes by swap, as the reference's
/// managed-resource pool does (`:1680-1690`), so the *last* client reaped is the
/// one whose events arrive first. Nothing in the protocol depends on that
/// order; it is kept because a test that sees it change should be able to point
/// at the line it changed from.
#[derive(Debug, Default)]
pub struct Clients {
    records: Vec<ClientRecord>,
}

impl Clients {
    /// No clients yet. A driver starts with none, the way the reference does.
    pub const fn new() -> Self {
        Self {
            records: Vec::new(),
        }
    }

    /// How many clients this driver knows.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Whether this driver knows no clients at all.
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// The record for a client, if there is one.
    pub fn find(&self, client_id: i64) -> Option<&ClientRecord> {
        self.records
            .iter()
            .find(|record| record.client_id == client_id)
    }

    /// The record for a client, for a caller about to change it.
    pub fn find_mut(&mut self, client_id: i64) -> Option<&mut ClientRecord> {
        self.records
            .iter_mut()
            .find(|record| record.client_id == client_id)
    }

    /// Find this client, or register it — the reference's `get_or_add_client`.
    ///
    /// The heartbeat counter is allocated here and announced with
    /// `ON_COUNTER_READY` under the **client id** as the correlation id
    /// (`:1030`), which is what makes a fresh client's first response its own
    /// heartbeat. `None` when the counter cannot be allocated, which leaves the
    /// client unregistered — the reference returns `NULL` and the command that
    /// asked fails.
    pub fn get_or_add(
        &mut self,
        client_id: i64,
        now_ms: i64,
        liveness_timeout_ns: i64,
        manager: &mut CounterManager,
        regions: &CounterRegions<'_>,
        events: &mut impl ClientEvents,
    ) -> Option<&mut ClientRecord> {
        if self.find(client_id).is_some() {
            return self.find_mut(client_id);
        }

        let label = format!("client-heartbeat: id={client_id}");
        let key = client_id.to_le_bytes();
        let heartbeat_counter_id = manager.allocate(
            regions,
            CLIENT_HEARTBEAT_TYPE_ID,
            &key,
            label.as_bytes(),
            now_ms,
        )?;

        manager.set_registration_id(regions, heartbeat_counter_id, client_id)?;
        manager.set_owner_id(regions, heartbeat_counter_id, client_id)?;
        // Registered is alive: the reference stamps the counter before it
        // announces it, so a reader that acts on the announcement cannot find
        // a counter that already looks stale.
        manager.set_value(regions, heartbeat_counter_id, now_ms)?;

        self.records.push(ClientRecord {
            client_id,
            closed_by_command: false,
            reached_end_of_life: false,
            heartbeat_counter_id,
            liveness_timeout_ms: liveness_timeout_ms(liveness_timeout_ns),
            counter_links: Vec::new(),
            publication_links: Vec::new(),
        });

        events.counter_ready(client_id, heartbeat_counter_id);
        self.records.last_mut()
    }

    /// A `CLIENT_KEEPALIVE` from a client, which does nothing for one the
    /// driver has not seen (`:5269-5280`).
    ///
    /// Returns whether there was a client to refresh.
    pub fn on_keepalive(
        &mut self,
        client_id: i64,
        now_ms: i64,
        manager: &CounterManager,
        regions: &CounterRegions<'_>,
    ) -> bool {
        let Some(record) = self.find(client_id) else {
            return false;
        };

        manager
            .set_value(regions, record.heartbeat_counter_id, now_ms)
            .is_some()
    }

    /// A `CLIENT_CLOSE` — the client says it is going away.
    ///
    /// Nothing is freed here. The heartbeat is zeroed, which makes
    /// `now > value + timeout` true on the very next timeout tier, and
    /// `closed_by_command` is what stops that tick from announcing a timeout
    /// for a client that left on purpose (`:6321-6331`).
    pub fn on_close(
        &mut self,
        client_id: i64,
        manager: &CounterManager,
        regions: &CounterRegions<'_>,
    ) -> bool {
        let Some(record) = self.find_mut(client_id) else {
            return false;
        };

        record.closed_by_command = true;
        manager
            .set_value(regions, record.heartbeat_counter_id, 0)
            .is_some()
    }

    /// Phase one of a pass: **mark the expired clients and announce them**.
    ///
    /// The reference splits a pass in two, and the split is visible on the wire
    /// when more than one client expires in the same tick. `aeron_client_on_time_event`
    /// (`aeron_driver_conductor.c:1038-1056`) runs over every client first —
    /// counting the timeout in system counter 24, then broadcasting
    /// `ON_CLIENT_TIMEOUT` and the heartbeat's `ON_UNAVAILABLE_COUNTER` — and
    /// only then does the pool's reclamation pass call `aeron_client_delete`
    /// for each client that reached end of life (`:1692-1712`). Interleaving
    /// the two per client produces an order a C driver never emits: A's
    /// counters going unavailable *before* B's timeout.
    ///
    /// Counter 24 is incremented here, before the announcement, and only for a
    /// client that did not close itself — the reference's order (`:1047-1049`),
    /// and why a tool that drains the ring and reads the counter together never
    /// sees the event arrive first.
    ///
    /// Nothing is freed here, and nothing is removed: that is
    /// [`Clients::reap_expired`], and keeping the two apart is what makes the
    /// order above possible.
    pub fn on_time_event(
        &mut self,
        now_ms: i64,
        manager: &CounterManager,
        regions: &CounterRegions<'_>,
        events: &mut impl ClientEvents,
    ) {
        for index in (0..self.records.len()).rev() {
            let record = &mut self.records[index];
            if record.reached_end_of_life {
                continue;
            }

            let Some(held) = manager.value(regions, record.heartbeat_counter_id) else {
                continue;
            };

            // Wrap, not saturate. The reference compares
            // `now > timestamp + timeout` with C's wrapping arithmetic
            // (`aeron_driver_conductor.c:1042`), so a heartbeat near `i64::MAX`
            // wraps negative and the client expires at once. Saturating instead
            // would make that heartbeat *never* expire: a client — or anything
            // that can write a counter — could pin a record and its counter for
            // the driver's whole life.
            if now_ms <= held.wrapping_add(record.liveness_timeout_ms) {
                continue;
            }

            record.reached_end_of_life = true;

            if !record.closed_by_command {
                system_counters::increment(manager, regions, system_counters::id::CLIENT_TIMEOUTS);
                events.client_timed_out(record.client_id);
            }
            events.counter_unavailable(record.client_id, record.heartbeat_counter_id);
        }
    }

    /// Phase two: free everything a marked client owned, announcing each
    /// counter as it goes (`aeron_client_delete`, `:1218-1295`).
    ///
    /// The order is the reference's: the client's own counters first, each
    /// announced before it is freed, then the heartbeat — whose announcement
    /// went out with the timeout in phase one, which is why this does not
    /// repeat it.
    ///
    /// What the client held elsewhere goes with it: its publications are let
    /// go of, and its subscriptions are detached from the publications they
    /// read. Since the client is not there to hear it, none of that is
    /// announced.
    ///
    /// Returns how many clients were reaped.
    #[allow(clippy::too_many_arguments)] // one per collaborator
    pub fn reap_expired(
        &mut self,
        now_ms: i64,
        manager: &mut CounterManager,
        regions: &CounterRegions<'_>,
        events: &mut impl ClientEvents,
        publications: &mut IpcPublications,
        subscriptions: &mut IpcSubscriptions,
    ) -> usize {
        let mut reaped = 0;
        let mut index = self.records.len();

        while index > 0 {
            index -= 1;

            if !self.records[index].reached_end_of_life {
                continue;
            }

            self.reap(
                index,
                now_ms,
                manager,
                regions,
                events,
                publications,
                subscriptions,
            );
            self.records.swap_remove(index);
            reaped += 1;
        }

        reaped
    }

    /// Free everything one client owned, announcing each counter as it goes
    /// (`aeron_client_delete`, `:1218-1295`).
    ///
    /// The order is the reference's: the client's own counters first, each
    /// announced before it is freed, then the heartbeat — whose announcement
    /// already went out with the timeout, which is why this one does not repeat
    /// it.
    #[allow(clippy::too_many_arguments)] // one per collaborator
    fn reap(
        &mut self,
        index: usize,
        now_ms: i64,
        manager: &mut CounterManager,
        regions: &CounterRegions<'_>,
        events: &mut impl ClientEvents,
        publications: &mut IpcPublications,
        subscriptions: &mut IpcSubscriptions,
    ) {
        let record = &mut self.records[index];

        // The publications first, in the reference's order (`:1220-1241`): a
        // client that dies takes its links with it, and a publication whose last
        // link that was starts draining.
        publications.release_links(&record.publication_links, manager, regions);
        record.publication_links.clear();

        for link in std::mem::take(&mut record.counter_links) {
            events.counter_unavailable(link.registration_id, link.counter_id);
            manager.free(regions, link.counter_id, now_ms);
        }

        subscriptions.remove_for_client(record.client_id, manager, regions, publications, now_ms);

        manager.free(regions, record.heartbeat_counter_id, now_ms);
        record.client_id = -1;
        record.heartbeat_counter_id = -1;
    }
}

/// The reference's per-client truncation of the configured timeout
/// (`aeron_driver_conductor.c:1020-1021`): milliseconds, and never zero — a
/// client whose timeout rounded to zero would be reaped on arrival.
fn liveness_timeout_ms(liveness_timeout_ns: i64) -> i64 {
    if liveness_timeout_ns < 1_000_000 {
        1
    } else {
        liveness_timeout_ns / 1_000_000
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deepmsg_core::buffer::{AtomicBuffer, ReadWrite};

    #[repr(align(64))]
    struct Region(Vec<u8>);

    impl Region {
        fn zeroed(len: usize) -> Self {
            Self(vec![0u8; len])
        }

        fn writable(&mut self) -> AtomicBuffer<'_, ReadWrite> {
            AtomicBuffer::from_slice_mut(&mut self.0).expect("aligned region")
        }
    }

    /// The publication and subscription managers a teardown needs, with no
    /// publications in them: what these tests are about is the client pool.
    fn managers() -> (IpcPublications, IpcSubscriptions) {
        (
            IpcPublications::start(-1, 1000).expect("an agent thread"),
            IpcSubscriptions::new(),
        )
    }

    /// A recording sink: the events, in the order they were raised.
    #[derive(Debug, Default)]
    struct Events(Vec<String>);

    impl ClientEvents for Events {
        fn counter_ready(&mut self, registration_id: i64, counter_id: i32) {
            self.0.push(format!("ready:{registration_id}:{counter_id}"));
        }

        fn counter_unavailable(&mut self, registration_id: i64, counter_id: i32) {
            self.0
                .push(format!("unavailable:{registration_id}:{counter_id}"));
        }

        fn client_timed_out(&mut self, client_id: i64) {
            self.0.push(format!("timeout:{client_id}"));
        }

        fn operation_succeeded(&mut self, correlation_id: i64) {
            self.0.push(format!("succeeded:{correlation_id}"));
        }

        fn error(&mut self, correlation_id: i64, error_code: i32, _message: &[u8]) {
            self.0.push(format!("error:{correlation_id}:{error_code}"));
        }

        fn publication_ready(&mut self, ready: &PublicationBuffersReady<'_>, is_exclusive: bool) {
            self.0.push(format!(
                "publication:{}:{}:exclusive={is_exclusive}",
                ready.correlation_id, ready.registration_id
            ));
        }

        fn subscription_ready(&mut self, registration_id: i64, channel_status_indicator_id: i32) {
            self.0.push(format!(
                "subscription:{registration_id}:{channel_status_indicator_id}"
            ));
        }

        fn available_image(&mut self, ready: &ImageBuffersReady<'_>) {
            self.0.push(format!(
                "image:{}:{}",
                ready.correlation_id, ready.subscriber_registration_id
            ));
        }

        fn unavailable_image(
            &mut self,
            correlation_id: i64,
            subscription_registration_id: i64,
            _stream_id: i32,
            _channel: &[u8],
        ) {
            self.0.push(format!(
                "unavailable:{correlation_id}:{subscription_registration_id}"
            ));
        }
    }

    struct Fixture {
        metadata: Region,
        values: Region,
    }

    const VALUES_LENGTH: usize = 64 * 1024;

    impl Fixture {
        fn new() -> Self {
            Self {
                metadata: Region::zeroed(VALUES_LENGTH * 4),
                values: Region::zeroed(VALUES_LENGTH),
            }
        }

        fn open(&mut self) -> (CounterManager, CounterRegions<'_>) {
            let regions = CounterRegions::new(self.metadata.writable(), self.values.writable())
                .expect("four-to-one");
            let manager = CounterManager::new(VALUES_LENGTH, 1_000).expect("room");
            (manager, regions)
        }
    }

    const TIMEOUT_NS: i64 = 10_000_000_000;

    /// A believable epoch millisecond: the timeout logic compares the
    /// configured window against a *clock*, so a test that registers at 0 and
    /// ticks at 1 is testing arithmetic the driver never does.
    const NOW: i64 = 1_700_000_000_000;

    #[test]
    fn a_client_is_registered_on_first_sight_and_announced() {
        let mut fixture = Fixture::new();
        let (mut manager, regions) = fixture.open();
        let mut clients = Clients::new();
        let mut events = Events::default();

        let record = clients
            .get_or_add(7, 1_000, TIMEOUT_NS, &mut manager, &regions, &mut events)
            .expect("registered");

        assert_eq!(7, record.client_id);
        assert!(!record.closed_by_command);
        assert_eq!(10_000, record.liveness_timeout_ms);
        assert_eq!(
            vec!["ready:7:0"],
            events.0,
            "the heartbeat goes out at once"
        );

        // The counter is a heartbeat: type 11, the client id as key,
        // registration and owner, and it already reads as alive.
        let observer = regions.reader();
        let descriptor = observer
            .find_by_type_and_registration(CLIENT_HEARTBEAT_TYPE_ID, 7)
            .expect("allocated");
        assert_eq!(0, descriptor);
        assert_eq!(
            Some(1_000),
            observer.value(descriptor),
            "stamped with the time it registered"
        );
        assert_eq!(
            "client-heartbeat: id=7",
            observer
                .find_by_type_id(CLIENT_HEARTBEAT_TYPE_ID)
                .expect("allocated")
                .label
        );
        let key = observer.key(descriptor).expect("a key");
        assert_eq!(7i64.to_le_bytes(), key[..8]);

        // Registering again is not a second registration.
        clients
            .get_or_add(7, 2_000, TIMEOUT_NS, &mut manager, &regions, &mut events)
            .expect("still registered");
        assert_eq!(1, clients.len());
        assert_eq!(
            vec!["ready:7:0"],
            events.0,
            "and nothing was announced twice"
        );
    }

    #[test]
    fn a_second_client_gets_its_own_heartbeat() {
        let mut fixture = Fixture::new();
        let (mut manager, regions) = fixture.open();
        let mut clients = Clients::new();
        let mut events = Events::default();

        clients
            .get_or_add(7, 0, TIMEOUT_NS, &mut manager, &regions, &mut events)
            .expect("registered");
        clients
            .get_or_add(9, 0, TIMEOUT_NS, &mut manager, &regions, &mut events)
            .expect("registered");

        assert_eq!(vec!["ready:7:0", "ready:9:1"], events.0);
        assert_eq!(2, clients.len());
    }

    #[test]
    fn keepalive_refreshes_a_known_client_and_ignores_an_unknown_one() {
        let mut fixture = Fixture::new();
        let (mut manager, regions) = fixture.open();
        let mut clients = Clients::new();
        let mut events = Events::default();
        clients
            .get_or_add(7, 1_000, TIMEOUT_NS, &mut manager, &regions, &mut events)
            .expect("registered");

        assert!(clients.on_keepalive(7, 5_000, &manager, &regions));
        assert_eq!(Some(5_000), manager.value(&regions, 0));

        assert!(
            !clients.on_keepalive(8, 5_000, &manager, &regions),
            "a client this driver has never seen is not registered by a keepalive"
        );
        assert_eq!(1, clients.len());
    }

    #[test]
    fn close_marks_the_client_and_zeroes_its_heartbeat() {
        let mut fixture = Fixture::new();
        let (mut manager, regions) = fixture.open();
        let mut clients = Clients::new();
        let mut events = Events::default();
        clients
            .get_or_add(7, 1_000, TIMEOUT_NS, &mut manager, &regions, &mut events)
            .expect("registered");

        assert!(clients.on_close(7, &manager, &regions));
        assert_eq!(Some(0), manager.value(&regions, 0), "zeroed, so it expires");
        assert!(clients.find(7).expect("still here").closed_by_command);
        assert_eq!(1, clients.len(), "and nothing is freed until the tier runs");
        assert!(!clients.on_close(8, &manager, &regions));
    }

    #[test]
    fn a_silent_client_is_reaped_and_announced_in_the_references_order() {
        let mut fixture = Fixture::new();
        let (mut manager, regions) = fixture.open();
        let (mut publications, mut subscriptions) = managers();
        let mut clients = Clients::new();
        let mut events = Events::default();
        clients
            .get_or_add(
                7,
                NOW + 1_000,
                TIMEOUT_NS,
                &mut manager,
                &regions,
                &mut events,
            )
            .expect("registered");
        events.0.clear();

        // Not yet: the timeout is ten seconds and the value is one second old.
        clients.on_time_event(NOW + 11_000, &manager, &regions, &mut events);
        assert!(events.0.is_empty());
        assert_eq!(1, clients.len(), "and nothing was reclaimed either");

        // One millisecond past the deadline: phase one announces…
        clients.on_time_event(NOW + 11_001, &manager, &regions, &mut events);
        assert_eq!(1, clients.len(), "…and phase two is what reclaims");
        assert_eq!(
            1,
            clients.reap_expired(
                NOW + 11_001,
                &mut manager,
                &regions,
                &mut events,
                &mut publications,
                &mut subscriptions
            )
        );
        assert_eq!(
            vec!["timeout:7", "unavailable:7:0"],
            events.0,
            "the timeout, then the heartbeat it named"
        );
        assert!(clients.is_empty());

        // Reclaimed, not erased: the state is what changed. The value word is
        // left exactly as it was — the reference's `free` does not touch it
        // (`aeron_counters_manager.c:246-282`), which is why a client is told
        // to stop using an id by the *state* and not by a sentinel value.
        let observer = regions.reader();
        assert_eq!(1, observer.for_each(|_| {}).reclaimed);
        assert_eq!(Some(NOW + 1_000), observer.value(0));
    }

    #[test]
    fn a_client_that_closed_itself_is_reaped_without_a_timeout_announcement() {
        let mut fixture = Fixture::new();
        let (mut manager, regions) = fixture.open();
        let (mut publications, mut subscriptions) = managers();
        let mut clients = Clients::new();
        let mut events = Events::default();
        clients
            .get_or_add(7, 0, TIMEOUT_NS, &mut manager, &regions, &mut events)
            .expect("registered");
        clients.on_close(7, &manager, &regions);
        events.0.clear();

        clients.on_time_event(NOW + 1, &manager, &regions, &mut events);
        assert_eq!(
            1,
            clients.reap_expired(
                NOW + 1,
                &mut manager,
                &regions,
                &mut events,
                &mut publications,
                &mut subscriptions
            ),
            "a zeroed heartbeat expires on the next tick, without a timeout"
        );
        assert_eq!(
            vec!["unavailable:7:0"],
            events.0,
            "no ON_CLIENT_TIMEOUT for a client that said goodbye"
        );
    }

    #[test]
    fn a_clients_counters_go_before_its_heartbeat() {
        let mut fixture = Fixture::new();
        let (mut manager, regions) = fixture.open();
        let (mut publications, mut subscriptions) = managers();
        let mut clients = Clients::new();
        let mut events = Events::default();
        clients
            .get_or_add(7, NOW, TIMEOUT_NS, &mut manager, &regions, &mut events)
            .expect("registered");

        // An application counter, as ADD_COUNTER would have left it.
        let app_counter = manager
            .allocate(&regions, 100, &[], b"app counter", 0)
            .expect("allocated");
        manager
            .set_registration_id(&regions, app_counter, 42)
            .expect("in range");
        clients
            .find_mut(7)
            .expect("registered")
            .counter_links
            .push(CounterLink {
                registration_id: 42,
                counter_id: app_counter,
            });
        events.0.clear();

        clients.on_time_event(NOW + 20_000, &manager, &regions, &mut events);
        clients.reap_expired(
            NOW + 20_000,
            &mut manager,
            &regions,
            &mut events,
            &mut publications,
            &mut subscriptions,
        );

        assert_eq!(
            vec!["timeout:7", "unavailable:7:0", "unavailable:42:1"],
            events.0,
            "the timeout and the heartbeat first, then the counters it owned"
        );
        assert_eq!(
            2,
            regions.reader().for_each(|_| {}).reclaimed,
            "both are freed"
        );
    }

    #[test]
    fn a_heartbeat_at_the_top_of_the_range_expires_instead_of_never() {
        // The comparison wraps, as the reference's does
        // (`aeron_driver_conductor.c:1042`): `i64::MAX + anything` is negative,
        // so the client is expired on the spot. Saturating instead would make
        // it *never* expire — a counter anyone can write, pinning a client
        // record and its counters for the driver's whole life.
        let mut fixture = Fixture::new();
        let (mut manager, regions) = fixture.open();
        let (mut publications, mut subscriptions) = managers();
        let mut clients = Clients::new();
        let mut events = Events::default();

        clients
            .get_or_add(7, NOW, TIMEOUT_NS, &mut manager, &regions, &mut events)
            .expect("registered");
        manager.set_value(&regions, 0, i64::MAX).expect("in range");
        events.0.clear();

        clients.on_time_event(NOW, &manager, &regions, &mut events);
        assert_eq!(
            vec!["timeout:7", "unavailable:7:0"],
            events.0,
            "a heartbeat that cannot be inside the window is outside it"
        );
        assert_eq!(
            1,
            clients.reap_expired(
                NOW,
                &mut manager,
                &regions,
                &mut events,
                &mut publications,
                &mut subscriptions
            )
        );
        assert!(clients.is_empty());
    }

    #[test]
    fn every_announcement_precedes_every_reclamation() {
        // Two clients expiring in the same tick, which is when the reference's
        // two-phase pass is visible on the ring: both timeouts, then both
        // reclamations. An interleaved loop emits A's counters going away
        // before B's timeout, which no C driver ever does
        // (`aeron_driver_conductor.c:1038-1056` then `:1692-1712`).
        let mut fixture = Fixture::new();
        let (mut manager, regions) = fixture.open();
        let (mut publications, mut subscriptions) = managers();
        let mut clients = Clients::new();
        let mut events = Events::default();

        for client_id in [7, 9] {
            clients
                .get_or_add(
                    client_id,
                    NOW,
                    TIMEOUT_NS,
                    &mut manager,
                    &regions,
                    &mut events,
                )
                .expect("registered");
        }
        events.0.clear();

        // Both are silent, so both expire in the same tick.
        clients.on_time_event(NOW + 20_000, &manager, &regions, &mut events);
        assert_eq!(
            vec![
                "timeout:9",
                "unavailable:9:1",
                "timeout:7",
                "unavailable:7:0"
            ],
            events.0,
            "phase one: every announcement, in one pass — newest client first, \
             because the reference's pool walks its array backwards to remove by swap"
        );
        assert_eq!(2, clients.len(), "and nothing reclaimed yet");

        events.0.clear();
        clients.reap_expired(
            NOW + 20_000,
            &mut manager,
            &regions,
            &mut events,
            &mut publications,
            &mut subscriptions,
        );
        assert!(
            events
                .0
                .iter()
                .all(|event| event.starts_with("unavailable:")),
            "phase two only reclaims: {:?}",
            events.0
        );
        assert!(clients.is_empty());
    }

    #[test]
    fn the_timeout_is_counted_before_it_is_announced() {
        // System counter 24 is incremented before the broadcast
        // (`aeron_driver_conductor.c:1047-1049`), so a reader that drains the
        // ring and reads the counter in the same breath never sees the event
        // arrive first. The sink here reads the counter at the moment the
        // event is raised, which is the only place the order is observable.
        struct Counting<'a> {
            manager: &'a CounterManager,
            regions: &'a CounterRegions<'a>,
            seen: Vec<i64>,
        }

        impl ClientEvents for Counting<'_> {
            fn counter_ready(&mut self, _registration_id: i64, _counter_id: i32) {}
            fn counter_unavailable(&mut self, _registration_id: i64, _counter_id: i32) {}
            fn operation_succeeded(&mut self, _correlation_id: i64) {}
            fn error(&mut self, _correlation_id: i64, _error_code: i32, _message: &[u8]) {}
            fn publication_ready(
                &mut self,
                _ready: &PublicationBuffersReady<'_>,
                _is_exclusive: bool,
            ) {
            }
            fn subscription_ready(&mut self, _registration_id: i64, _status: i32) {}
            fn available_image(&mut self, _ready: &ImageBuffersReady<'_>) {}
            fn unavailable_image(
                &mut self,
                _correlation_id: i64,
                _subscription_registration_id: i64,
                _stream_id: i32,
                _channel: &[u8],
            ) {
            }

            fn client_timed_out(&mut self, _client_id: i64) {
                self.seen.push(
                    self.manager
                        .value(self.regions, crate::system_counters::id::CLIENT_TIMEOUTS)
                        .unwrap_or(-1),
                );
            }
        }

        let mut fixture = Fixture::new();
        let (mut manager, regions) = fixture.open();
        let mut clients = Clients::new();
        let mut ready = Events::default();

        clients
            .get_or_add(7, NOW, TIMEOUT_NS, &mut manager, &regions, &mut ready)
            .expect("registered");

        let mut counting = Counting {
            manager: &manager,
            regions: &regions,
            seen: Vec::new(),
        };
        clients.on_time_event(NOW + 20_000, &manager, &regions, &mut counting);

        assert_eq!(
            vec![1],
            counting.seen,
            "the counter already reads 1 when the event is raised"
        );
    }

    #[test]
    fn the_liveness_timeout_is_milliseconds_and_never_zero() {
        assert_eq!(1, liveness_timeout_ms(0));
        assert_eq!(1, liveness_timeout_ms(999_999));
        assert_eq!(1, liveness_timeout_ms(1_000_000));
        assert_eq!(10_000, liveness_timeout_ms(10_000_000_000));
    }
}
