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
//! # Reaping is two announcements and three frees, in that order
//!
//! `aeron_client_on_time_event` (`:1038-1056`) fires first: it announces
//! `ON_CLIENT_TIMEOUT` — unless the client closed itself — and
//! `ON_UNAVAILABLE_COUNTER` for the heartbeat counter. Only then does
//! `aeron_client_delete` (`:1218-1295`) run, freeing the client's counters one
//! by one (each announced) and last the heartbeat counter. A client that is
//! already gone must not be announced as timed out: `CLIENT_CLOSE` sets the
//! heartbeat to zero so the next tick reaps it, and `closed_by_command` is what
//! tells the two apart (`:5269-5280`, `:6321-6331`).

use deepmsg_cnc::counters::CLIENT_HEARTBEAT_TYPE_ID;
use deepmsg_cnc::{CounterManager, CounterRegions};

/// Where a client-lifecycle event goes.
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

    /// `ON_CLIENT_TIMEOUT`: the client stopped being heard from. The conductor
    /// counts it in system counter 24 as well
    /// (`aeron_driver_conductor.c:1048`), which is why this event is *not*
    /// raised for a client that closed itself.
    fn client_timed_out(&mut self, client_id: i64);
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
        self.records.iter().find(|record| record.client_id == client_id)
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
        let heartbeat_counter_id =
            manager.allocate(regions, CLIENT_HEARTBEAT_TYPE_ID, &key, label.as_bytes(), now_ms)?;

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

    /// One pass of the reference's `aeron_client_on_time_event`
    /// (`:1038-1056`), followed by the reaping of anything that just expired.
    ///
    /// Returns how many clients were reaped.
    pub fn on_time_event(
        &mut self,
        now_ms: i64,
        manager: &mut CounterManager,
        regions: &CounterRegions<'_>,
        events: &mut impl ClientEvents,
    ) -> usize {
        let mut reaped = 0;
        let mut index = self.records.len();

        while index > 0 {
            index -= 1;

            {
                let record = &mut self.records[index];
                if !record.reached_end_of_life {
                    let timestamp = manager.value(regions, record.heartbeat_counter_id);
                    if timestamp.is_some_and(|held| {
                        now_ms > held.saturating_add(record.liveness_timeout_ms)
                    }) {
                        record.reached_end_of_life = true;

                        if !record.closed_by_command {
                            events.client_timed_out(record.client_id);
                        }
                        events.counter_unavailable(record.client_id, record.heartbeat_counter_id);
                    }
                }
            }

            if self.records[index].reached_end_of_life {
                self.reap(index, now_ms, manager, regions, events);
                self.records.swap_remove(index);
                reaped += 1;
            }
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
    fn reap(
        &mut self,
        index: usize,
        now_ms: i64,
        manager: &mut CounterManager,
        regions: &CounterRegions<'_>,
        events: &mut impl ClientEvents,
    ) {
        let record = &mut self.records[index];

        for link in std::mem::take(&mut record.counter_links) {
            events.counter_unavailable(link.registration_id, link.counter_id);
            manager.free(regions, link.counter_id, now_ms);
        }

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

    /// A recording sink: the events, in the order they were raised.
    #[derive(Debug, Default)]
    struct Events(Vec<String>);

    impl ClientEvents for Events {
        fn counter_ready(&mut self, registration_id: i64, counter_id: i32) {
            self.0.push(format!("ready:{registration_id}:{counter_id}"));
        }

        fn counter_unavailable(&mut self, registration_id: i64, counter_id: i32) {
            self.0.push(format!("unavailable:{registration_id}:{counter_id}"));
        }

        fn client_timed_out(&mut self, client_id: i64) {
            self.0.push(format!("timeout:{client_id}"));
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
        assert_eq!(vec!["ready:7:0"], events.0, "the heartbeat goes out at once");

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
        assert_eq!(vec!["ready:7:0"], events.0, "and nothing was announced twice");
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
        let mut clients = Clients::new();
        let mut events = Events::default();
        clients
            .get_or_add(7, NOW + 1_000, TIMEOUT_NS, &mut manager, &regions, &mut events)
            .expect("registered");
        events.0.clear();

        // Not yet: the timeout is ten seconds and the value is one second old.
        assert_eq!(
            0,
            clients.on_time_event(NOW + 11_000, &mut manager, &regions, &mut events)
        );
        assert!(events.0.is_empty());

        // One millisecond past the deadline.
        assert_eq!(
            1,
            clients.on_time_event(NOW + 11_001, &mut manager, &regions, &mut events)
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
        let mut clients = Clients::new();
        let mut events = Events::default();
        clients
            .get_or_add(7, 0, TIMEOUT_NS, &mut manager, &regions, &mut events)
            .expect("registered");
        clients.on_close(7, &manager, &regions);
        events.0.clear();

        assert_eq!(
            1,
            clients.on_time_event(NOW + 1, &mut manager, &regions, &mut events),
            "a zeroed heartbeat expires on the next tick"
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

        clients.on_time_event(NOW + 20_000, &mut manager, &regions, &mut events);

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
    fn the_liveness_timeout_is_milliseconds_and_never_zero() {
        assert_eq!(1, liveness_timeout_ms(0));
        assert_eq!(1, liveness_timeout_ms(999_999));
        assert_eq!(1, liveness_timeout_ms(1_000_000));
        assert_eq!(10_000, liveness_timeout_ms(10_000_000_000));
    }
}
