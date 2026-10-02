//! The destinations a multi-destination send endpoint fans out to
//! (`media/aeron_udp_destination_tracker.c`).
//!
//! A channel whose control mode is `manual` or `dynamic` is a channel with
//! several receivers
//! ([`crate::udp_channel::UdpChannel::is_multi_destination`]), and a publication
//! on it sends every datagram to each of them. Which ones it has is not fixed:
//! a `manual` channel is told (`ADD_DESTINATION`), and a `dynamic` one *learns*
//! — a receiver that has never been heard of becomes a destination the moment
//! it reports a position.
//!
//! # manual and dynamic differ in six places, and one of them is a surprise
//!
//! `manual` destinations **never time out**. They are added, they stay, and the
//! only thing that moves their activity timestamp is the re-resolution scan
//! that runs every five idle seconds (`:408-425`). A `dynamic` destination is
//! removed once five seconds pass with no status message from it (`:73-97`),
//! and the removal happens on the next `send` — so a dynamic channel that stops
//! being sent to keeps its stale destinations until it is.
//!
//! # `AF_UNSPEC`
//!
//! An address that was asked for but could not be resolved is kept, and
//! skipped: the reference stores a `sockaddr_storage` whose family is
//! `AF_UNSPEC` (`:125-128`, `:146-149`) and sends to the others. `None` is that
//! address here, because a `SocketAddr` cannot be of no family.

use std::net::SocketAddr;

use deepmsg_cnc::layout::NULL_VALUE;
use deepmsg_cnc::{CounterManager, CounterRegions};

use super::Transport;
use crate::udp_channel::UdpChannel;

/// How long a **dynamic** destination may go without a status message before it
/// is removed (`AERON_UDP_DESTINATION_TRACKER_DESTINATION_TIMEOUT_NS`,
/// `media/aeron_udp_destination_tracker.h:24`).
pub const DESTINATION_TIMEOUT_NS: i64 = 5 * 1_000 * 1_000 * 1_000;

/// One destination (`aeron_udp_destination_entry_t`, `:26-38`).
///
/// The reference's entry also carries a `destination_timeout_ns` field, which
/// `add` writes from the module constant and nothing ever reads — expiry is
/// measured against the tracker's own copy (`:106`, `:83`). It is not carried
/// here: a field that cannot be read is not a fact about the wire.
#[derive(Clone, Debug)]
pub struct Destination {
    /// When this destination was last heard from, or added.
    pub time_of_last_activity_ns: i64,
    /// The receiver's own id, once it has told us one
    /// (`is_receiver_id_valid`).
    pub receiver_id: i64,
    /// What `ADD_DESTINATION` named it — the id a client removes it by.
    /// [`NULL_VALUE`] for a destination a status message created.
    pub registration_id: i64,
    /// Whether `receiver_id` means anything yet. A manually added destination
    /// starts without one and is matched by address until a status message
    /// binds it (`:270-275`).
    pub is_receiver_id_valid: bool,
    /// The channel this destination was added with, kept so that a
    /// re-resolution (or a removal) has the text it came from. [`None`] for a
    /// destination a status message created.
    pub uri: Option<UdpChannel>,
    /// Where to send. [`None`] is the reference's `AF_UNSPEC`: a destination
    /// that is known and not reachable, which is skipped rather than sent to.
    pub addr: Option<SocketAddr>,
}

impl Destination {
    /// Whether a **dynamic** tracker should have removed this by `now_ns`.
    fn is_expired(&self, now_ns: i64, timeout_ns: i64) -> bool {
        now_ns > self.time_of_last_activity_ns + timeout_ns
    }
}

/// Whether an arriving frame belongs to a destination
/// (`aeron_udp_destination_tracker_is_match`, `:215-225`).
///
/// The two arms are not symmetric, and the asymmetry is the point. A
/// destination that knows its receiver id is matched on **the id and the port,
/// never the address**: a receiver behind a NAT reports from an address the
/// sender has never seen, and the id is what says it is the same receiver. A
/// destination that has no id yet can only be matched by address — and by port.
fn is_match(destination: &Destination, receiver_id: i64, addr: &SocketAddr) -> bool {
    if destination.is_receiver_id_valid {
        return receiver_id == destination.receiver_id
            && destination
                .addr
                .is_some_and(|known| known.port() == addr.port());
    }

    destination.addr == Some(*addr)
}

/// The destinations a send endpoint fans out to
/// (`aeron_udp_destination_tracker_t`, `:40-60`).
#[derive(Debug)]
pub struct DestinationTracker {
    destinations: Vec<Destination>,
    /// `is_manual_control_mode`: whether this channel's destinations were all
    /// named by a client, which is what stops them ever expiring.
    is_manual_control_mode: bool,
    destination_timeout_ns: i64,
    /// Where the rotation starts, so that a burst of datagrams does not always
    /// reach the first destination first (`:113-118`).
    round_robin_index: usize,
    /// The `mdc-num-dest` counter, whose value is [`DestinationTracker::len`].
    num_destinations_counter_id: i32,
}

impl DestinationTracker {
    /// A tracker with no destinations
    /// (`aeron_udp_destination_tracker_init`, `:38-55`).
    pub fn new(
        is_manual_control_mode: bool,
        destination_timeout_ns: i64,
        num_destinations_counter_id: i32,
    ) -> Self {
        Self {
            destinations: Vec::new(),
            is_manual_control_mode,
            destination_timeout_ns,
            round_robin_index: 0,
            num_destinations_counter_id,
        }
    }

    /// How many destinations the tracker holds — the number `mdc-num-dest`
    /// carries.
    pub fn len(&self) -> usize {
        self.destinations.len()
    }

    /// Whether it holds none. A multi-destination channel starts this way and
    /// stays this way until a destination arrives.
    pub fn is_empty(&self) -> bool {
        self.destinations.is_empty()
    }

    /// The destinations, in the order they were added.
    pub fn destinations(&self) -> &[Destination] {
        &self.destinations
    }

    /// Whether this channel's destinations were all named by a client.
    pub fn is_manual_control_mode(&self) -> bool {
        self.is_manual_control_mode
    }

    /// The `mdc-num-dest` counter, so that whoever removes the endpoint can
    /// give it back.
    pub fn num_destinations_counter_id(&self) -> i32 {
        self.num_destinations_counter_id
    }

    /// Send one datagram to every destination
    /// (`aeron_udp_destination_tracker_send`, `:99-169`).
    ///
    /// Returns the number of datagrams handed over — the whole batch, or
    /// **zero if any destination refused it**. A partial send is not reported
    /// as a partial success: the caller retries the batch, and the destinations
    /// that already took it take it again, which is what a datagram protocol
    /// tolerates and what the reference does (`result = 0`, `:133`, `:154`).
    ///
    /// A **dynamic** destination that has gone quiet is not sent to and is
    /// scheduled for removal; the removal itself runs at the end of the pass,
    /// so that a destination does not vanish from under the loop.
    pub fn send(
        &mut self,
        transport: &mut dyn Transport,
        buffers: &[&[u8]],
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
        now_ns: i64,
    ) -> usize {
        let length = self.destinations.len();
        let mut result = buffers.len();
        let mut to_be_removed = 0;

        // The rotation: the index is the start, and it moves on by one each
        // pass. Past the end it wraps to zero *and is left at one*, because
        // zero is where this pass is going anyway (`:113-118`).
        let mut starting_index = self.round_robin_index;
        self.round_robin_index += 1;
        if starting_index >= length {
            starting_index = 0;
            self.round_robin_index = 1;
        }

        let is_dynamic_control_mode = !self.is_manual_control_mode;

        for index in (starting_index..length).chain(0..starting_index) {
            let destination = &self.destinations[index];

            if is_dynamic_control_mode
                && destination.is_expired(now_ns, self.destination_timeout_ns)
            {
                to_be_removed += 1;
            } else if let Some(address) = destination.addr {
                if transport.send(Some(address), buffers).is_err() {
                    result = 0;
                }
            }
        }

        if to_be_removed > 0 {
            self.remove_inactive(counters, regions, now_ns);
        }

        result
    }

    /// A status message arrived. Answer whether it belongs to a destination we
    /// already had (`aeron_udp_destination_tracker_on_status_message`,
    /// `:257-293`).
    ///
    /// A destination that had no receiver id learns this one and is refreshed.
    /// On a **dynamic** channel a status message from somewhere unknown
    /// *creates* a destination — that is the whole of how a dynamic channel
    /// finds its receivers. On a manual one it does nothing: the destinations
    /// are the ones a client named, and a status message is not a way to add
    /// one.
    pub fn on_status_message(
        &mut self,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
        receiver_id: i64,
        addr: &SocketAddr,
        now_ns: i64,
    ) -> bool {
        let mut is_existing = false;

        for destination in &mut self.destinations {
            is_existing = is_match(destination, receiver_id, addr);
            if is_existing {
                if !destination.is_receiver_id_valid {
                    destination.receiver_id = receiver_id;
                    destination.is_receiver_id_valid = true;
                }
                destination.time_of_last_activity_ns = now_ns;

                break;
            }
        }

        if !self.is_manual_control_mode && !is_existing {
            self.add(
                counters,
                regions,
                receiver_id,
                true,
                now_ns,
                None,
                Some(*addr),
                NULL_VALUE,
            );
        }

        is_existing
    }

    /// Add a destination a client named
    /// (`aeron_udp_destination_tracker_manual_add_destination`, `:295-309`).
    ///
    /// A **dynamic** channel's destinations are the ones its status messages
    /// brought, so this is a no-op there — and answers so, which is what lets
    /// the conductor tell the two apart.
    pub fn manual_add(
        &mut self,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
        now_ns: i64,
        uri: UdpChannel,
        addr: Option<SocketAddr>,
        registration_id: i64,
    ) -> bool {
        if !self.is_manual_control_mode {
            return false;
        }

        self.add(
            counters,
            regions,
            0,
            false,
            now_ns,
            Some(uri),
            addr,
            registration_id,
        );

        true
    }

    /// Remove a destination by the address it was added with
    /// (`aeron_udp_destination_tracker_remove_destination`, `:323-350`).
    ///
    /// Answers with the channel the destination was added with, so that whoever
    /// removes it can close what it opened. The comparison is the reference's
    /// `address_compare` (`:311-321`): family first, then address **and port**.
    /// The manual destinations that have gone quiet long enough to have their
    /// names resolved again
    /// (`aeron_udp_destination_tracker_check_for_re_resolution`, `:402-428`).
    ///
    /// One per destination, and only on a **manual** channel: a dynamic one
    /// learns its destinations from status messages, so an address it stops
    /// hearing from is one it drops rather than one it re-resolves (`:409-412`).
    ///
    /// The activity time is reset by the caller that acts on this, which is the
    /// reference's own order — the check stamps `time_of_last_activity_ns` as
    /// it reports, so a destination is asked about once per timeout and not
    /// once per pass (`:427`).
    pub fn destinations_to_re_resolve(&self, now_ns: i64, timeout_ns: i64) -> Vec<(usize, String)> {
        if !self.is_manual_control_mode {
            return Vec::new();
        }

        self.destinations
            .iter()
            .enumerate()
            .filter(|(_, destination)| destination.is_expired(now_ns, timeout_ns))
            .filter_map(|(index, destination)| {
                destination
                    .uri
                    .as_ref()
                    .and_then(|channel| channel.endpoint_name.clone())
                    .map(|name| (index, name))
            })
            .collect()
    }

    /// What one of those destinations is called, for the counter's label and
    /// for the resolver's question.
    pub fn destination_name(&self, index: usize) -> Option<&str> {
        self.destinations
            .get(index)
            .and_then(|destination| destination.uri.as_ref())
            .and_then(|channel| channel.endpoint_name.as_deref())
    }

    /// Stamp a destination as just checked, which is what keeps the re-resolution
    /// to one per timeout (`:427`, `update_last_activity_ns`).
    pub fn mark_re_resolution_checked(&mut self, index: usize, now_ns: i64) {
        if let Some(destination) = self.destinations.get_mut(index) {
            destination.time_of_last_activity_ns = now_ns;
        }
    }

    /// The address a destination currently sends to, which is what a
    /// re-resolution's answer is compared against
    /// (`destination->addr`, `:424`).
    pub fn destination_addr(&self, index: usize) -> Option<SocketAddr> {
        self.destinations.get(index).and_then(|entry| entry.addr)
    }

    /// Take an answer: every manual destination whose channel named that
    /// endpoint moves to the new address
    /// (`aeron_udp_destination_tracker_resolution_change`, `:431-445`, which
    /// matches by **name**).
    ///
    /// A destination that is not there is not an error: the answer may arrive
    /// after the destination was removed, and the reference's loop simply finds
    /// nothing to move.
    pub fn on_resolution_change(&mut self, endpoint_name: &str, addr: SocketAddr) {
        if !self.is_manual_control_mode {
            return;
        }

        for destination in &mut self.destinations {
            let matches = destination
                .uri
                .as_ref()
                .and_then(|channel| channel.endpoint_name.as_deref())
                .is_some_and(|name| name == endpoint_name);

            if matches {
                destination.addr = Some(addr);
            }
        }
    }

    pub fn remove(
        &mut self,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
        addr: &SocketAddr,
    ) -> Option<UdpChannel> {
        let index = self
            .destinations
            .iter()
            .position(|destination| destination.addr.as_ref() == Some(addr))?;

        let removed = self.destinations.swap_remove(index);
        self.set_num_destinations(counters, regions);

        removed.uri
    }

    /// Remove a destination by the registration id it was added under
    /// (`aeron_udp_destination_tracker_remove_destination_by_id`, `:352-379`).
    pub fn remove_by_id(
        &mut self,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
        registration_id: i64,
    ) -> Option<UdpChannel> {
        let index = self
            .destinations
            .iter()
            .position(|destination| destination.registration_id == registration_id)?;

        let removed = self.destinations.swap_remove(index);
        self.set_num_destinations(counters, regions);

        removed.uri
    }

    /// The registration id of the destination an error frame came from, or
    /// [`NULL_VALUE`] (`aeron_udp_destination_tracker_find_registration_id`,
    /// `:381-400`).
    ///
    /// An error is attributed to a destination the same way a status message
    /// is, which is what lets the client that added it be told.
    pub fn find_registration_id(&self, receiver_id: i64, addr: &SocketAddr) -> i64 {
        self.destinations
            .iter()
            .find(|destination| is_match(destination, receiver_id, addr))
            .map_or(NULL_VALUE, |destination| destination.registration_id)
    }

    /// The reference's `aeron_udp_destination_tracker_add_destination`
    /// (`:227-255`), which every path that grows the table goes through.
    #[allow(clippy::too_many_arguments)] // one per field of the reference's entry
    fn add(
        &mut self,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
        receiver_id: i64,
        is_receiver_id_valid: bool,
        now_ns: i64,
        uri: Option<UdpChannel>,
        addr: Option<SocketAddr>,
        registration_id: i64,
    ) {
        self.destinations.push(Destination {
            time_of_last_activity_ns: now_ns,
            receiver_id,
            registration_id,
            is_receiver_id_valid,
            uri,
            addr,
        });

        self.set_num_destinations(counters, regions);
    }

    /// Drop every expired destination
    /// (`aeron_udp_destination_tracker_remove_inactive_destinations`, `:73-97`).
    ///
    /// **Only a dynamic channel has any**: a manual one's destinations are the
    /// ones a client named, and naming one is a statement that it should be
    /// there, not a lease on it.
    ///
    /// This is the reference's *second* guard on that — `send` filters by
    /// control mode before it will even count an expired destination (`:125`),
    /// so nothing reaches the check below through the one caller this build
    /// has. It stays because it is where the reference's re-resolution scan
    /// relies on it (`:408-425`), and because a function that removes
    /// destinations should say for itself whose destinations may go.
    fn remove_inactive(
        &mut self,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
        now_ns: i64,
    ) {
        if self.is_manual_control_mode {
            return;
        }

        self.destinations
            .retain(|destination| !destination.is_expired(now_ns, self.destination_timeout_ns));

        self.set_num_destinations(counters, regions);
    }

    /// `mdc-num-dest` is the table's length, and it is written whenever the
    /// table changes (`:100`, `:249`, `:341`, `:370`) — including when nothing
    /// changed, which costs one store and saves a comparison.
    fn set_num_destinations(&self, counters: &CounterManager, regions: &CounterRegions<'_>) {
        #[allow(clippy::cast_possible_wrap)] // a table of destinations is not 2^63 long
        let length = self.destinations.len() as i64;

        let _ = counters.set_value(regions, self.num_destinations_counter_id, length);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use deepmsg_core::buffer::{AtomicBuffer, ReadWrite};

    use crate::position::{
        MDC_NUM_DESTINATIONS_NAME, allocate_channel_status_counter, channel_type_id,
    };

    const VALUES_LENGTH: usize = 64 * 1024;

    #[repr(align(64))]
    struct Region(Vec<u8>);

    impl Region {
        fn zeroed(len: usize) -> Self {
            Self(vec![0u8; len])
        }

        fn writable(&mut self) -> AtomicBuffer<'_, ReadWrite> {
            AtomicBuffer::from_slice_mut(&mut self.0).expect("aligned")
        }
    }

    /// The two regions a counter lives in, and the counter manager over them.
    ///
    /// Handed out together rather than one at a time, because a
    /// `CounterRegions` borrows the regions it was built from and a test needs
    /// both it and the manager at once.
    struct Fixture {
        metadata: Region,
        values: Region,
    }

    impl Fixture {
        fn new() -> Self {
            Self {
                metadata: Region::zeroed(VALUES_LENGTH * 4),
                values: Region::zeroed(VALUES_LENGTH),
            }
        }

        /// A manager, its regions, and the `mdc-num-dest` counter's id.
        fn open(&mut self) -> (CounterManager, CounterRegions<'_>, i32) {
            let regions = CounterRegions::new(self.metadata.writable(), self.values.writable())
                .expect("four-to-one");
            let mut counters = CounterManager::new(VALUES_LENGTH, 1_000).expect("room");
            let counter_id = allocate_channel_status_counter(
                &mut counters,
                &regions,
                MDC_NUM_DESTINATIONS_NAME,
                channel_type_id::MDC_NUM_DESTINATIONS,
                7,
                b"aeron:udp?endpoint=127.0.0.1:40123",
                0,
            )
            .expect("a counter");

            (counters, regions, counter_id)
        }
    }

    /// A transport that records where it was asked to send, and can be told to
    /// refuse some of them.
    #[derive(Default)]
    struct Recorder {
        sent_to: Vec<SocketAddr>,
        refuse: Vec<SocketAddr>,
    }

    impl Transport for Recorder {
        fn send(
            &mut self,
            address: Option<SocketAddr>,
            _buffers: &[&[u8]],
        ) -> std::io::Result<usize> {
            let address = address.expect("the tracker never sends to an unspecified address");
            self.sent_to.push(address);

            if self.refuse.contains(&address) {
                return Err(std::io::Error::other("the socket refused it"));
            }

            Ok(1)
        }

        fn receive(
            &mut self,
            _buffers: &mut [Vec<u8>],
            _datagrams: &mut crate::sys::socket::Datagrams,
        ) -> std::io::Result<usize> {
            unreachable!("the tracker never receives")
        }

        fn reconnect(&mut self, _address: std::net::SocketAddr) -> std::io::Result<()> {
            Ok(())
        }

        fn local_address(&self) -> std::io::Result<SocketAddr> {
            unreachable!("the tracker never asks")
        }

        fn receive_buffer_size(&self) -> std::io::Result<usize> {
            unreachable!("the tracker never asks")
        }
    }

    fn address(port: u16) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], port))
    }

    fn channel(port: u16) -> UdpChannel {
        let uri = format!("aeron:udp?endpoint=127.0.0.1:{port}|control-mode=manual");
        let parsed = crate::channel_uri::ChannelUri::parse(uri.as_bytes()).expect("a URI");

        UdpChannel::resolve(uri.as_bytes(), &parsed).expect("a channel")
    }

    const NOW: i64 = 1_000_000_000;

    #[test]
    fn the_destination_count_is_what_the_counter_says() {
        let mut fixture = Fixture::new();
        let (counters, regions, counter_id) = fixture.open();
        let mut tracker = DestinationTracker::new(true, DESTINATION_TIMEOUT_NS, counter_id);

        assert_eq!(
            Some(0),
            counters.value(&regions, counter_id),
            "an empty table"
        );

        assert!(tracker.manual_add(
            &counters,
            &regions,
            NOW,
            channel(40124),
            Some(address(40124)),
            42,
        ));
        assert_eq!(Some(1), counters.value(&regions, counter_id));

        tracker.manual_add(
            &counters,
            &regions,
            NOW,
            channel(40125),
            Some(address(40125)),
            43,
        );
        assert_eq!(Some(2), counters.value(&regions, counter_id));

        assert!(tracker.remove_by_id(&counters, &regions, 42).is_some());
        assert_eq!(Some(1), counters.value(&regions, counter_id));

        assert!(
            tracker
                .remove(&counters, &regions, &address(40125))
                .is_some()
        );
        assert_eq!(Some(0), counters.value(&regions, counter_id));

        assert!(
            tracker.remove_by_id(&counters, &regions, 42).is_none(),
            "removing what is not there removes nothing"
        );
        assert_eq!(Some(0), counters.value(&regions, counter_id));
    }

    /// The channel a removal answers with, which is what whoever removes it
    /// closes.
    #[test]
    fn a_removal_answers_with_the_channel_it_removed() {
        let mut fixture = Fixture::new();
        let (counters, regions, counter_id) = fixture.open();
        let mut tracker = DestinationTracker::new(true, DESTINATION_TIMEOUT_NS, counter_id);

        tracker.manual_add(
            &counters,
            &regions,
            NOW,
            channel(40124),
            Some(address(40124)),
            42,
        );

        let removed = tracker.remove(&counters, &regions, &address(40124));
        assert!(removed.is_some(), "the destination was there");
        assert!(tracker.is_empty());
    }

    /// A **dynamic** channel's table is fed by its status messages: a receiver
    /// that has never been heard of becomes a destination
    /// (`aeron_udp_destination_tracker_on_status_message`, `:286-290`).
    #[test]
    fn a_status_message_teaches_a_dynamic_channel_a_destination() {
        let mut fixture = Fixture::new();
        let (counters, regions, counter_id) = fixture.open();
        let mut tracker = DestinationTracker::new(false, DESTINATION_TIMEOUT_NS, counter_id);

        let known = tracker.on_status_message(&counters, &regions, -1234, &address(40124), NOW);
        assert!(!known, "nothing was there to match");
        assert_eq!(1, tracker.len(), "and it is a destination now");
        assert_eq!(NULL_VALUE, tracker.destinations()[0].registration_id);

        assert!(
            tracker.on_status_message(&counters, &regions, -1234, &address(40124), NOW + 1),
            "the same receiver again is matched, not added twice"
        );
        assert_eq!(1, tracker.len());
        assert_eq!(Some(1), counters.value(&regions, counter_id));
    }

    /// A **manual** channel's destinations are the ones a client named, and a
    /// status message from somewhere unknown is not one of them (`:293`).
    #[test]
    fn a_status_message_does_not_teach_a_manual_channel_one() {
        let mut fixture = Fixture::new();
        let (counters, regions, counter_id) = fixture.open();
        let mut tracker = DestinationTracker::new(true, DESTINATION_TIMEOUT_NS, counter_id);

        assert!(!tracker.on_status_message(&counters, &regions, -1234, &address(40124), NOW));
        assert!(
            tracker.is_empty(),
            "manual destinations are named, not learned"
        );
    }

    /// The two arms of the match are not symmetric: a destination that knows
    /// its receiver id is matched on the id and the **port**, never the
    /// address (`:220-222`).
    ///
    /// This is what makes a receiver behind a NAT work — the id is what says it
    /// is the same receiver, and the address it reports from is not.
    #[test]
    fn a_bound_destination_is_matched_by_id_and_port_not_by_address() {
        let mut fixture = Fixture::new();
        let (counters, regions, counter_id) = fixture.open();
        let mut tracker = DestinationTracker::new(false, DESTINATION_TIMEOUT_NS, counter_id);

        tracker.on_status_message(&counters, &regions, -1234, &address(40124), NOW);

        let from_elsewhere = SocketAddr::from(([10, 0, 0, 9], 40124));
        assert!(
            tracker.on_status_message(&counters, &regions, -1234, &from_elsewhere, NOW + 1),
            "the id and the port are what matched"
        );
        assert_eq!(1, tracker.len(), "not a second destination");

        // A different port is a different destination, whatever the id says.
        let other_port = SocketAddr::from(([10, 0, 0, 9], 40125));
        assert!(!tracker.on_status_message(&counters, &regions, -1234, &other_port, NOW + 2));
        assert_eq!(2, tracker.len(), "a second destination, learned from it");
    }

    /// **A manual destination never times out.** Only a dynamic one is removed
    /// when five seconds pass without a status message, and the removal happens
    /// on the next `send` (`:77`, `:125`).
    #[test]
    fn only_a_dynamic_channel_forgets_a_destination_that_went_quiet() {
        for (is_manual, expected) in [(true, 1usize), (false, 0usize)] {
            let mut fixture = Fixture::new();
            let (counters, regions, counter_id) = fixture.open();
            let mut tracker =
                DestinationTracker::new(is_manual, DESTINATION_TIMEOUT_NS, counter_id);
            let mut transport = Recorder::default();

            tracker.manual_add(
                &counters,
                &regions,
                NOW,
                channel(40124),
                Some(address(40124)),
                42,
            );

            // A manual channel has to be given it: `manual_add` is a no-op on a
            // dynamic one, so that side is fed by a status message instead.
            if !is_manual {
                tracker.on_status_message(&counters, &regions, -1, &address(40124), NOW);
            }
            assert_eq!(1, tracker.len());

            let sent = tracker.send(
                &mut transport,
                &[b"payload"],
                &counters,
                &regions,
                NOW + DESTINATION_TIMEOUT_NS + 1,
            );

            assert_eq!(expected, tracker.len(), "manual {is_manual}");
            assert_eq!(
                expected,
                transport.sent_to.len(),
                "and a channel that forgot it did not send to it either"
            );
            assert_eq!(
                1, sent,
                "the batch was not refused — the return says nothing about how many \
                 destinations took it, only that none turned it away (`:110`)"
            );
            assert_eq!(
                Some(expected as i64),
                counters.value(&regions, counter_id),
                "and the counter followed"
            );
        }
    }

    /// A quiet manual destination is one to resolve again, and an answer moves
    /// it **by name** — which is why a destination keeps the channel it was
    /// added with (`aeron_udp_destination_tracker_check_for_re_resolution`,
    /// `:402-428`, and `..._resolution_change`, `:431-445`).
    ///
    /// A dynamic channel has none of this: it learns its destinations from
    /// status messages, so one it stops hearing from is one it drops.
    #[test]
    fn a_quiet_manual_destination_is_resolved_again() {
        let mut fixture = Fixture::new();
        let (counters, regions, counter_id) = fixture.open();
        let mut tracker = DestinationTracker::new(true, DESTINATION_TIMEOUT_NS, counter_id);

        tracker.manual_add(&counters, &regions, NOW, channel(40124), None, 42);
        tracker.manual_add(&counters, &regions, NOW, channel(40125), None, 43);

        let quiet_at = NOW + DESTINATION_TIMEOUT_NS + 1;

        assert!(
            tracker
                .destinations_to_re_resolve(NOW + DESTINATION_TIMEOUT_NS, DESTINATION_TIMEOUT_NS)
                .is_empty(),
            "five seconds is not enough"
        );

        let due = tracker.destinations_to_re_resolve(quiet_at, DESTINATION_TIMEOUT_NS);
        assert_eq!(2, due.len(), "both are quiet");
        assert_eq!(
            Some("127.0.0.1:40124"),
            tracker.destination_name(due[0].0),
            "and a destination is asked about by the name its channel wrote"
        );
        assert_eq!(
            None,
            tracker.destination_addr(due[0].0),
            "which never resolved"
        );

        // Checking one stamps it, so it is not asked about again until another
        // timeout has passed (`:427`).
        tracker.mark_re_resolution_checked(due[0].0, quiet_at);
        assert_eq!(
            1,
            tracker
                .destinations_to_re_resolve(quiet_at, DESTINATION_TIMEOUT_NS)
                .len()
        );

        // The answer moves the destinations whose channel named that endpoint,
        // and only those.
        let moved: SocketAddr = "127.0.0.2:40124".parse().expect("an address");
        tracker.on_resolution_change("127.0.0.1:40124", moved);

        assert_eq!(Some(moved), tracker.destination_addr(due[0].0));
        assert_eq!(
            None,
            tracker.destination_addr(due[1].0),
            "the other destination's name is not this one's"
        );
    }

    /// The same check on a **dynamic** channel finds nothing, which is the
    /// reference's own `if (tracker->is_manual_control_mode)` (`:409-412`).
    #[test]
    fn a_dynamic_channel_has_no_destinations_to_resolve_again() {
        let mut fixture = Fixture::new();
        let (counters, regions, counter_id) = fixture.open();
        let mut tracker = DestinationTracker::new(false, DESTINATION_TIMEOUT_NS, counter_id);

        tracker.on_status_message(&counters, &regions, 1, &address(40124), NOW);

        assert!(!tracker.is_empty(), "a dynamic channel did learn one");
        assert!(
            tracker
                .destinations_to_re_resolve(
                    NOW + DESTINATION_TIMEOUT_NS + 1,
                    DESTINATION_TIMEOUT_NS
                )
                .is_empty()
        );
    }

    /// `manual_add_destination` is a no-op on a dynamic channel (`:302-305`),
    /// which is what stops a client from putting a destination on a channel
    /// that is supposed to learn its own.
    #[test]
    fn manual_add_does_nothing_on_a_dynamic_channel() {
        let mut fixture = Fixture::new();
        let (counters, regions, counter_id) = fixture.open();
        let mut tracker = DestinationTracker::new(false, DESTINATION_TIMEOUT_NS, counter_id);

        assert!(!tracker.manual_add(
            &counters,
            &regions,
            NOW,
            channel(40124),
            Some(address(40124)),
            42,
        ));
        assert!(tracker.is_empty());
        assert_eq!(Some(0), counters.value(&regions, counter_id));
    }

    /// An address that could not be resolved is kept and skipped
    /// (`:125-128`): the destination is real, its address is not known, and
    /// sending to `AF_UNSPEC` is not something a socket can do.
    #[test]
    fn an_unresolved_destination_is_skipped_rather_than_sent_to() {
        let mut fixture = Fixture::new();
        let (counters, regions, counter_id) = fixture.open();
        let mut tracker = DestinationTracker::new(true, DESTINATION_TIMEOUT_NS, counter_id);
        let mut transport = Recorder::default();

        tracker.manual_add(&counters, &regions, NOW, channel(40124), None, 42);
        tracker.manual_add(
            &counters,
            &regions,
            NOW,
            channel(40125),
            Some(address(40125)),
            43,
        );

        let sent = tracker.send(&mut transport, &[b"payload"], &counters, &regions, NOW);

        assert_eq!(vec![address(40125)], transport.sent_to);
        assert_eq!(1, sent, "the batch went out");
        assert_eq!(2, tracker.len(), "and both destinations are still there");
    }

    /// A destination that refuses the datagram makes the whole pass report
    /// nothing sent (`:133`, `:154`) — the batch is retried whole, and the
    /// destinations that already took it take it again.
    #[test]
    fn one_destination_refusing_the_batch_reports_none_of_it() {
        let mut fixture = Fixture::new();
        let (counters, regions, counter_id) = fixture.open();
        let mut tracker = DestinationTracker::new(true, DESTINATION_TIMEOUT_NS, counter_id);
        let mut transport = Recorder {
            refuse: vec![address(40124)],
            ..Recorder::default()
        };

        for port in [40124u16, 40125] {
            tracker.manual_add(
                &counters,
                &regions,
                NOW,
                channel(port),
                Some(address(port)),
                i64::from(port),
            );
        }

        let sent = tracker.send(&mut transport, &[b"payload"], &counters, &regions, NOW);

        assert_eq!(0, sent, "a partial send is reported as none of it");
        assert_eq!(
            vec![address(40124), address(40125)],
            transport.sent_to,
            "and the destinations after the one that refused were still tried"
        );
    }

    /// The rotation moves on by one each pass, so the destination that goes
    /// first is not always the one that was added first (`:113-118`).
    #[test]
    fn the_rotation_moves_which_destination_goes_first() {
        let mut fixture = Fixture::new();
        let (counters, regions, counter_id) = fixture.open();
        let mut tracker = DestinationTracker::new(true, DESTINATION_TIMEOUT_NS, counter_id);

        for port in [40124u16, 40125, 40126] {
            tracker.manual_add(
                &counters,
                &regions,
                NOW,
                channel(port),
                Some(address(port)),
                i64::from(port),
            );
        }

        let mut first_of_each_pass = Vec::new();
        for _ in 0..3 {
            let mut transport = Recorder::default();
            let sent = tracker.send(&mut transport, &[b"payload"], &counters, &regions, NOW);

            assert_eq!(1, sent);
            assert_eq!(3, transport.sent_to.len(), "every destination each pass");
            first_of_each_pass.push(transport.sent_to[0]);
        }

        assert_eq!(
            vec![address(40124), address(40125), address(40126)],
            first_of_each_pass,
            "each pass starts one further along"
        );
    }

    /// An error frame is attributed to a destination the same way a status
    /// message is, which is what lets the client that added it be told
    /// (`:381-400`).
    #[test]
    fn an_error_is_attributed_to_the_destination_it_came_from() {
        let mut fixture = Fixture::new();
        let (counters, regions, counter_id) = fixture.open();
        let mut tracker = DestinationTracker::new(true, DESTINATION_TIMEOUT_NS, counter_id);

        tracker.manual_add(
            &counters,
            &regions,
            NOW,
            channel(40124),
            Some(address(40124)),
            42_i64,
        );

        assert_eq!(42, tracker.find_registration_id(-1, &address(40124)));
        assert_eq!(
            NULL_VALUE,
            tracker.find_registration_id(-1, &address(40999)),
            "an address no destination has"
        );
    }
}
