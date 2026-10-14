//! What the transports of one machine share: the port table, the rings their
//! frames wait in, the three ports, and the clock.
//!
//! A stack is built by whoever owns the ports — a test builds one over a
//! [`super::fake_port::Wire`], the DPDK build over a queue pair — and handed to
//! the driver as part of the bindings. Nothing here is per-transport except the
//! rings, and the rings are per *slot* so that a transport that closes and one
//! that binds the same port afterwards share one and cannot see each other's
//! frames.
//!
//! # Why the ready ring looks the way it does
//!
//! A kernel socket buffers between the NIC and the receiver thread; this stack
//! has to buffer in software, and the ring is where. Three things about it are
//! consequences rather than choices:
//!
//! * **The capacity is derived from the receive window, not written down.** A
//!   ring smaller than what the window promises loses frames under an ordinary
//!   burst, and the losing is invisible — it lands on the same counter (88,
//!   `QUEUE_DROP_COUNT`) that a deliberately overloaded ring does, so the
//!   falsification the design asks for stops distinguishing anything. The
//!   floor is one window; the default is two, because a burst of
//!   [`RX_BURST`] frames for one destination arrives between two drains that
//!   take one frame each (§3.2 of the plan).
//! * **The length is an atomic outside the lock.** With the threads split, the
//!   sender thread asks a slot it does not own whether anything is waiting,
//!   every pass, about a million times a second; a lock there is a cache line
//!   bouncing between two cores to be told "no". The atomic is what makes an
//!   empty ring cost a load.
//! * **A frame is filed under the binding it was routed under.** A ring
//!   outlives the binding it was made for, so a frame in flight while a
//!   transport closes and another binds the same port must not arrive at the
//!   new one — which is the ABA the design names (§3.2, S5). The generation is
//!   what carries it, and it travels with the frame from the routing decision
//!   to the push.

use std::collections::VecDeque;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicI64, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::config::{MTU_LENGTH_DEFAULT, RCV_INITIAL_WINDOW_LENGTH_DEFAULT};
use crate::media::Affinity;
use crate::sys::socket::BindFailure;

use super::fake_port::{Side, Wire};
use super::frame::{self, Received};
use super::packet_io::{Disposition, ReadyFrame, RxFrame, RxPort, SlotId, TxPort};
use super::ports::{Binding, EPHEMERAL_PORTS, PortTable, SLOT_CAPACITY};

/// How many frames one pass takes off the port.
///
/// The design's §4.5 number. It is a batch and not a budget: what one pass
/// does with them is bounded by the ring capacity, which is why the two
/// numbers have to be read together (§3.2).
pub const RX_BURST: usize = 32;

/// How a stack tells the time.
///
/// The ARP timers need a monotonic clock, and the tests need one they can wind
/// by hand: an entry that ages out after five minutes cannot be tested by
/// waiting five minutes. A function pointer cannot do it — a `fn() -> i64`
/// cannot capture, so a hand-wound clock behind one would have to be a
/// `static`, and the test binaries run their cases in parallel, where one
/// case's hand would move another's. Two variants, no allocation per read, and
/// the shape the bindings enum already uses.
#[derive(Clone, Default)]
pub enum Clock {
    /// The process's own monotonic clock, in nanoseconds.
    #[default]
    Monotonic,
    /// A clock a test winds itself.
    Manual(Arc<AtomicI64>),
}

impl Clock {
    /// The time now, in nanoseconds.
    pub fn now(&self) -> i64 {
        match self {
            Self::Monotonic => deepmsg_core::clock::monotonic_nano_time(),
            Self::Manual(nanos) => nanos.load(Ordering::Acquire),
        }
    }

    /// A clock a test winds, starting at `nanos`.
    pub fn manual(nanos: i64) -> Self {
        Self::Manual(Arc::new(AtomicI64::new(nanos)))
    }
}

impl std::fmt::Debug for Clock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Monotonic => write!(f, "Clock::Monotonic"),
            Self::Manual(nanos) => write!(f, "Clock::Manual({})", nanos.load(Ordering::Acquire)),
        }
    }
}

/// What one machine's stack is configured with.
///
/// The address is the port's own — the one a wildcard binding hears and the
/// one every frame it sends is from. One address, not a list: the design's
/// refusal list (§4.11) has no IPv6 and no multi-homing, and a stack with one
/// address is a stack whose wildcard question has one answer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StackParams {
    /// This port's IPv4 address.
    pub address: Ipv4Addr,
    /// How many leading bits of it are the subnet, which decides whether a
    /// next hop is on this link or behind the gateway.
    pub prefix_length: u8,
    /// This port's MAC.
    pub mac: [u8; 6],
    /// The next hop for destinations off this link, if one was configured.
    pub gateway: Option<Ipv4Addr>,
    /// The largest frame this port will send or accept.
    pub mtu: usize,
    /// How many bytes of datagrams the transport believes it can hold — the
    /// receive window the ring's capacity is derived from.
    pub receive_window: usize,
    /// The range an ephemeral bind draws from.
    pub ephemeral_ports: std::ops::RangeInclusive<u16>,
    /// How long to wait between ARP requests for one address.
    pub arp_interval: Duration,
    /// How long an ARP entry stays good without being refreshed.
    pub arp_timeout: Duration,
}

impl StackParams {
    /// The stack's own defaults — the driver's MTU and window, Linux's
    /// ephemeral range, and ARP's two timers.
    pub fn new(address: Ipv4Addr, prefix_length: u8, mac: [u8; 6]) -> Self {
        Self {
            address,
            prefix_length,
            mac,
            gateway: None,
            mtu: MTU_LENGTH_DEFAULT as usize,
            receive_window: RCV_INITIAL_WINDOW_LENGTH_DEFAULT as usize,
            ephemeral_ports: EPHEMERAL_PORTS,
            arp_interval: Duration::from_secs(1),
            arp_timeout: Duration::from_secs(300),
        }
    }

    /// How many frames one transport's ring holds: the window rounded up,
    /// doubled, and rounded to a power of two (see the module note).
    pub fn ring_capacity(&self) -> usize {
        let per_window = self.receive_window.div_ceil(self.mtu.max(1));

        per_window.saturating_mul(2).next_power_of_two().max(2)
    }
}

/// The counters a stack keeps, named rather than numbered.
///
/// The numbers are the reference's
/// (`aeron-client/src/main/c/aeron_counters.h:155-173`) and belong to the CnC
/// file, which the conductor owns; a stack is built before the driver's
/// counters exist and cannot allocate one. So the stack counts into atomics it
/// owns, named for the design's §4.10 table, and who publishes them — and how
/// — is a question for the slice that has a conductor.
#[derive(Debug, Default)]
pub struct StackCounters {
    /// 85 `DPDK_ARP_MISS`: sends that went nowhere for want of a MAC.
    pub arp_miss: AtomicU64,
    /// 86 `DPDK_RX_SENDER_DISCARD`: control frames no transport claimed.
    pub rx_sender_discard: AtomicU64,
    /// 87 `DPDK_POLLER`: passes a poller pumped — one poller per agent
    /// thread, so the sender's empty pass counts as well as the receiver's
    /// ([`ThreadQueues::pump`]).
    pub poller: AtomicU64,
    /// 88 `DPDK_QUEUE_DROP_COUNT`: frames a ready ring had no room for.
    pub queue_drop: AtomicU64,
    /// 89 `DPDK_CHECKSUM_FAILURE`: frames whose checksums did not hold.
    pub checksum_failure: AtomicU64,
    /// 90 `DPDK_FRAGMENTED_PACKETS`: fragments, counted with the optioned
    /// headers the stack will not parse either.
    pub fragmented: AtomicU64,
    /// 93 `DPDK_RX_UNSUPPORTED_ETHERNET_TYPE`: ethertypes that are neither
    /// IPv4 nor ARP.
    pub unsupported_ethernet_type: AtomicU64,
    /// 94 `DPDK_RX_UNSUPPORTED_PROTOCOL`: IPv4 that is not UDP or ICMP.
    pub unsupported_protocol: AtomicU64,
    /// 95 `DPDK_RX_RECEIVER_DISCARD`: data frames no transport claimed.
    pub rx_receiver_discard: AtomicU64,
    /// 78 `DPDK_TX_EAGAIN`: frames a transmit port would not take.
    pub tx_eagain: AtomicU64,
}

/// One transport's frames, in the order they arrived, with what a reader may
/// ask without taking the lock.
#[derive(Debug)]
pub struct ReadyRing {
    frames: Mutex<VecDeque<ReadyFrame>>,
    capacity: usize,
    /// How many frames are waiting. Read without the lock.
    len: AtomicUsize,
    /// Which binding these frames belong to; zero when no binding does.
    generation: AtomicU32,
}

/// What became of a frame offered to a ring.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Pushed {
    /// It is waiting for its transport.
    Taken,
    /// The ring was full, so the frame was dropped (counter 88).
    Full,
    /// The ring belongs to a binding that has gone.
    Stale,
}

impl ReadyRing {
    /// A ring that holds `capacity` frames and belongs to nobody.
    pub fn new(capacity: usize) -> Self {
        Self {
            frames: Mutex::new(VecDeque::with_capacity(capacity)),
            capacity,
            len: AtomicUsize::new(0),
            generation: AtomicU32::new(0),
        }
    }

    /// How many frames are waiting.
    pub fn len(&self) -> usize {
        self.len.load(Ordering::Acquire)
    }

    /// Whether anything is waiting.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// How many frames it holds at most.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Which binding these frames are for, or zero.
    pub fn generation(&self) -> u32 {
        self.generation.load(Ordering::Acquire)
    }

    /// Hand the ring to a binding, dropping whatever the last one left.
    ///
    /// This is the whole of the answer to "can a new transport be handed the
    /// frames of the one before it": it cannot, because they are gone before
    /// its binding is announced.
    pub fn rebind(&self, generation: u32) {
        let mut frames = self.frames.lock().expect("the ring");
        frames.clear();
        self.len.store(0, Ordering::Release);
        self.generation.store(generation, Ordering::Release);
    }

    /// Take the ring away from its binding, dropping what is waiting.
    pub fn forget(&self) {
        self.rebind(0);
    }

    /// Offer a frame to the binding that was routed to `generation`.
    pub fn push(&self, generation: u32, frame: ReadyFrame) -> Pushed {
        if self.generation.load(Ordering::Acquire) != generation {
            return Pushed::Stale;
        }

        let mut frames = self.frames.lock().expect("the ring");
        if frames.len() == self.capacity {
            return Pushed::Full;
        }

        frames.push_back(frame);
        self.len.store(frames.len(), Ordering::Release);

        Pushed::Taken
    }

    /// Take the oldest frame, if there is one.
    pub fn pop(&self) -> Option<ReadyFrame> {
        // The cheap answer first: a reader that finds the ring empty does not
        // take the lock at all, and most asks are for an empty ring.
        if self.is_empty() {
            return None;
        }

        let mut frames = self.frames.lock().expect("the ring");
        let frame = frames.pop_front();
        self.len.store(frames.len(), Ordering::Release);

        frame
    }
}

/// The rings, one per slot, alive as long as the stack.
#[derive(Debug)]
pub struct SlotTable {
    rings: Box<[Arc<ReadyRing>]>,
}

impl SlotTable {
    fn new(capacity: usize, ring_capacity: usize) -> Self {
        let rings = (0..capacity)
            .map(|_| Arc::new(ReadyRing::new(ring_capacity)))
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Self { rings }
    }

    /// The ring for a slot. An `Arc` because the transport holds one too: a
    /// transport that is closing has to be able to give its slot back without
    /// waiting for the stack to be dropped with it (G12).
    pub fn get(&self, slot: SlotId) -> Option<&Arc<ReadyRing>> {
        self.rings.get(usize::from(slot))
    }

    /// How many slots there are.
    pub fn len(&self) -> usize {
        self.rings.len()
    }

    /// Whether there are none, which a stack only is if it was built with no
    /// capacity.
    pub fn is_empty(&self) -> bool {
        self.rings.is_empty()
    }
}

/// One thread's view of a stack: which of the two it is, and the stack.
///
/// The three ports are the stack's, not this type's — this is what the poller
/// holds, and what `TransportBindings::poller` makes for each agent thread
/// (`poller_init_func`, `media/aeron_udp_channel_transport_bindings.h:91-94`).
///
/// **The two threads pump differently, and that is the whole of the queue
/// model.** The receiver's thread takes frames off the port and hands them to
/// every transport's ring; the sender's thread takes nothing, because its
/// frames were put in its rings by the receiver's pump — the design's §4.7
/// has one receive queue for the machine and one transmit queue per thread,
/// and a sender that pumped too would take the same frames twice.
#[derive(Clone)]
pub struct ThreadQueues {
    stack: Arc<Stack>,
    affinity: Affinity,
}

impl ThreadQueues {
    /// This thread's view of `stack`.
    pub const fn new(stack: Arc<Stack>, affinity: Affinity) -> Self {
        Self { stack, affinity }
    }

    /// Which thread this is.
    pub const fn affinity(&self) -> Affinity {
        self.affinity
    }

    /// The stack underneath.
    pub const fn stack(&self) -> &Arc<Stack> {
        &self.stack
    }

    /// One pass, answering how many frames came off the port.
    ///
    /// Nothing for the sender's thread: its frames are already in its rings by
    /// the time it asks (`pump_poll_func`'s first half).
    pub fn pump(&self) -> usize {
        self.stack.counters.poller.fetch_add(1, Ordering::Relaxed);

        match self.affinity {
            Affinity::Receiver => self.stack.pump(),
            Affinity::Sender => 0,
        }
    }
}

/// One machine's userspace stack.
///
/// Not `Debug`: its ports are trait objects, and there is nothing useful to
/// print about a stack that its accessors do not already answer.
pub struct Stack {
    params: StackParams,
    ports: PortTable,
    slots: SlotTable,
    rx: Arc<Mutex<Box<dyn RxPort>>>,
    tx: [Arc<Mutex<Box<dyn TxPort>>>; 2],
    counters: StackCounters,
    now: Clock,
}

impl Stack {
    /// A stack over the three ports a machine has: one receive queue and one
    /// transmit queue per agent thread.
    ///
    /// `tx` is indexed by [`Affinity`]: `0` is the sender's queue and `1` the
    /// receiver's, which is the design's §4.7 mapping and the reference's own
    /// discriminants.
    pub fn new(
        params: StackParams,
        now: Clock,
        rx: Box<dyn RxPort>,
        tx: [Box<dyn TxPort>; 2],
    ) -> Arc<Self> {
        let capacity = params.ring_capacity();

        Arc::new(Self {
            ports: PortTable::new(SLOT_CAPACITY, params.ephemeral_ports.clone()),
            slots: SlotTable::new(SLOT_CAPACITY, capacity),
            params,
            rx: Arc::new(Mutex::new(rx)),
            tx: tx.map(|port| Arc::new(Mutex::new(port))),
            counters: StackCounters::default(),
            now,
        })
    }

    /// A stack whose three ports are all on one side of a [`Wire`] — the shape
    /// every test here uses, and the shape two drivers talking to each other
    /// over one need.
    pub fn over_a_fake_wire(wire: &Wire, side: Side, params: StackParams, now: Clock) -> Arc<Self> {
        let mac = params.mac;
        let mtu = params.mtu;
        let port = || wire.port(side, mac, mtu);

        Self::new(
            params,
            now,
            Box::new(port()),
            [Box::new(port()), Box::new(port())],
        )
    }

    /// What this stack was configured with.
    pub const fn params(&self) -> &StackParams {
        &self.params
    }

    /// What it has counted.
    pub const fn counters(&self) -> &StackCounters {
        &self.counters
    }

    /// The clock it reads.
    pub const fn clock(&self) -> &Clock {
        &self.now
    }

    /// The transmit port for an affinity.
    pub fn transmit(&self, affinity: Affinity) -> &Arc<Mutex<Box<dyn TxPort>>> {
        match affinity {
            Affinity::Sender => &self.tx[0],
            Affinity::Receiver => &self.tx[1],
        }
    }

    /// Bind a port and hand back the ring its frames will wait in.
    ///
    /// # Errors
    ///
    /// Whatever the port table answers: `EADDRINUSE` for a port somebody
    /// holds, `EADDRNOTAVAIL` for an exhausted ephemeral range.
    pub fn bind(
        &self,
        address: Option<Ipv4Addr>,
        port: u16,
        peer: Option<SocketAddr>,
        failure_at: SocketAddr,
    ) -> Result<(Binding, Arc<ReadyRing>), BindFailure> {
        let binding = self.ports.bind(address, port, peer, failure_at)?;

        let ring = self
            .slots
            .get(binding.slot)
            .expect("a slot the table just handed out")
            .clone();
        ring.rebind(binding.generation);

        Ok((binding, ring))
    }

    /// Give a port back: the ring stops accepting frames for the binding that
    /// held it, and what is waiting in it is dropped.
    ///
    /// Called when the transport goes, which is what makes the port usable
    /// again at once (G12) — there is no `TIME_WAIT` to serve, because there
    /// is no kernel.
    pub fn release(&self, binding: Binding) {
        self.ports.release(binding);
        if let Some(ring) = self.slots.get(binding.slot) {
            ring.forget();
        }
    }

    /// Point a connected transport at a different peer, which is who it hears
    /// from (G10) and, with a neighbour table, who it sends to.
    pub fn reconnect(&self, binding: Binding, peer: SocketAddr) {
        self.ports.reconnect(binding, peer);
    }

    /// How many ports are bound.
    pub fn bound_ports(&self) -> usize {
        self.ports.in_use()
    }

    /// The ring for a slot.
    pub fn ring(&self, slot: SlotId) -> Option<&Arc<ReadyRing>> {
        self.slots.get(slot)
    }

    /// One pass over the receive port: take up to [`RX_BURST`] frames and hand
    /// each to the transport it is addressed to.
    ///
    /// This is what a poller's `pump` does for a userspace stack
    /// (`poller_poll_func`'s first half): the frames are read here and the
    /// transports are left to read their own rings afterwards, which is the
    /// same two-step shape the kernel's sockets have — the driver asks each
    /// transport for datagrams, and this is what put them there.
    ///
    /// Answers how many frames were taken off the port. The pass itself is
    /// counted by the poller that made it ([`ThreadQueues::pump`]), not here:
    /// a stack has no threads of its own.
    pub fn pump(&self) -> usize {
        let mut kept = Vec::new();
        let handed = {
            let mut rx = self.rx.lock().expect("the receive port");
            rx.rx_burst(RX_BURST, &mut |frame| self.route(frame), &mut kept)
        };

        for (binding, frame) in kept {
            self.deliver(binding, frame);
        }

        handed
    }

    /// Decide what becomes of one frame: the port's callback, and deliberately
    /// not where a frame is filed. A kept frame is pushed after the port's
    /// lock is released, so nothing takes a slot's lock while holding a
    /// port's (the design's §3.8 lock order).
    fn route(&self, frame: RxFrame<'_>) -> Disposition {
        // A frame addressed to some other port is not this port's, and the
        // design drops it **silently** — no counter, because a port that is
        // not in promiscuous mode is never handed one to begin with (§4.5).
        match frame::read_ethernet_header(frame.bytes) {
            Ok(ethernet) if ethernet.is_for(self.params.mac) => {}
            _ => return Disposition::Consumed,
        }

        let Ok(received) = frame::read(frame.bytes, frame.checksums) else {
            // A frame the stack cannot place is counted and dropped by the
            // slice that owns the counters; until then it is dropped.
            return Disposition::Consumed;
        };

        match received {
            Received::Udp { ipv4, datagram } => {
                let source = SocketAddr::from((ipv4.source, datagram.source_port));

                match self.ports.lookup(
                    self.params.address,
                    ipv4.destination,
                    datagram.destination_port,
                    source,
                ) {
                    Some(binding) => Disposition::Keep(binding),
                    None => Disposition::Consumed,
                }
            }
            // ARP is answered by the slice that owns the neighbour table, ICMP
            // by the one that owns the counters.
            Received::Arp(_) | Received::Echo { .. } => Disposition::Consumed,
        }
    }

    /// Put a frame into the ring of the slot that claimed it.
    fn deliver(&self, binding: Binding, frame: ReadyFrame) {
        let Some(ring) = self.slots.get(binding.slot) else {
            return;
        };

        if ring.push(binding.generation, frame) == Pushed::Full {
            self.counters.queue_drop.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::media::userspace::fake_port::{FakePort, Injection};

    const OWN: Ipv4Addr = Ipv4Addr::new(10, 10, 0, 1);
    const PEER: Ipv4Addr = Ipv4Addr::new(10, 10, 0, 2);

    fn params() -> StackParams {
        StackParams::new(OWN, 24, [0x02, 0, 0, 0, 0, 0x01])
    }

    fn stack(wire: &Wire) -> Arc<Stack> {
        Stack::over_a_fake_wire(wire, Side::A, params(), Clock::Monotonic)
    }

    fn bind(stack: &Stack, port: u16) -> (Binding, Arc<ReadyRing>) {
        stack
            .bind(Some(OWN), port, None, SocketAddr::from((OWN, port)))
            .expect("a bind")
    }

    /// Send one UDP frame carrying `payload`, from the far end of the wire to
    /// `(destination, port)`.
    fn send_udp(port: &mut FakePort, destination: Ipv4Addr, to: u16, payload: &[u8]) {
        let mut bytes = vec![0u8; frame::IPV4_UDP_HEADER_LENGTH + payload.len()];
        bytes[frame::IPV4_UDP_HEADER_LENGTH..].copy_from_slice(payload);
        let written = frame::write_ipv4_udp(
            &mut bytes,
            &frame::UdpWrite {
                ipv4: frame::Ipv4Write {
                    destination_hardware: [0x02, 0, 0, 0, 0, 0x01],
                    source_hardware: [0x02, 0, 0, 0, 0, 0x02],
                    source: PEER,
                    destination,
                    identification: 1,
                },
                source_port: 40000,
                destination_port: to,
                payload_length: payload.len(),
            },
        )
        .expect("a frame");

        assert_eq!(
            1,
            port.tx_burst(1, &mut |buffer| {
                buffer[..written].copy_from_slice(&bytes[..written]);
                written
            })
        );
    }

    fn far_end(wire: &Wire) -> FakePort {
        wire.port(Side::B, [0x02, 0, 0, 0, 0, 0x02], 1500)
    }

    /// The ring's capacity is a consequence of the window and the MTU, and it
    /// is not 64: one window of the defaults is 94 frames, so a ring of 64
    /// overflows under a burst the window itself promises to hold.
    #[test]
    fn the_ring_capacity_comes_from_the_window() {
        let defaults = params();
        assert_eq!(1408, defaults.mtu);
        assert_eq!(128 * 1024, defaults.receive_window);
        assert_eq!(
            94,
            (128 * 1024usize).div_ceil(1408),
            "one window, rounded up"
        );
        assert_eq!(
            256,
            defaults.ring_capacity(),
            "two windows, rounded to a power of two"
        );

        let narrow = StackParams {
            receive_window: 1408 * 10,
            ..defaults
        };
        assert_eq!(32, narrow.ring_capacity(), "ten frames is twenty, rounded");
    }

    #[test]
    fn a_frame_reaches_the_transport_bound_to_its_destination() {
        let wire = Wire::new();
        let stack = stack(&wire);
        let (binding, ring) = bind(&stack, 40456);

        send_udp(&mut far_end(&wire), OWN, 40456, b"hello");

        assert_eq!(1, stack.pump());
        assert_eq!(1, ring.len(), "and it is waiting for its transport");

        let frame = ring.pop().expect("a frame");
        let Received::Udp { datagram, .. } =
            frame::read(frame.as_bytes(), frame::Checksums::Software).expect("a UDP frame")
        else {
            panic!("a UDP frame");
        };
        assert_eq!(b"hello", datagram.payload);
        assert_eq!(40456, binding.port);
        assert!(ring.pop().is_none());
    }

    /// A frame for a port nobody bound is not filed anywhere, and a connected
    /// transport hears its peer and nobody else (G5, at the level of the
    /// ring).
    #[test]
    fn a_frame_nobody_claims_is_not_filed() {
        let wire = Wire::new();
        let stack = stack(&wire);
        let far = &mut far_end(&wire);
        let (_, unclaimed) = bind(&stack, 40457);

        send_udp(far, OWN, 40458, b"not for you");
        assert_eq!(1, stack.pump());
        assert!(unclaimed.is_empty());

        let (_, connected) = stack
            .bind(
                Some(OWN),
                40459,
                Some(SocketAddr::from((PEER, 40000))),
                SocketAddr::from((OWN, 40459)),
            )
            .expect("a bind");
        send_udp(far, OWN, 40459, b"from somebody else");
        // The far end's source port is 40000 and its address is PEER, so this
        // one is the peer's; the next is not.
        assert_eq!(1, stack.pump());
        assert_eq!(1, connected.len());

        send_udp(far, OWN, 40459, b"still the peer");
        assert_eq!(1, stack.pump());
        assert_eq!(2, connected.len());
    }

    /// G12 at the level of the ring: a released port's frames are gone, and
    /// the ring refuses anything routed under the binding that held it.
    #[test]
    fn a_released_ring_stops_accepting_its_old_binding() {
        let wire = Wire::new();
        let stack = stack(&wire);
        let (binding, ring) = bind(&stack, 40460);

        send_udp(&mut far_end(&wire), OWN, 40460, b"waiting");
        assert_eq!(1, stack.pump());
        assert_eq!(1, ring.len());

        stack.release(binding);

        assert!(ring.is_empty(), "what was waiting went with the binding");
        assert_eq!(0, ring.generation(), "and the ring belongs to nobody");
        assert_eq!(0, stack.bound_ports());
        assert_eq!(
            Pushed::Stale,
            ring.push(binding.generation, ReadyFrame::Bytes(vec![0u8; 4].into())),
            "a frame routed under the old binding is refused"
        );

        let (next, ring) = bind(&stack, 40460);
        assert!(next.generation > binding.generation);
        assert!(
            ring.is_empty(),
            "a new transport sees nothing of the old one"
        );
    }

    /// The ring drops what it has no room for, and says so on 88 — the counter
    /// the design's falsification turns on.
    #[test]
    fn a_full_ring_drops_and_counts() {
        let wire = Wire::new();
        let stack = stack(&wire);
        let (_, ring) = bind(&stack, 40461);

        for _ in 0..ring.capacity() {
            assert_eq!(
                Pushed::Taken,
                ring.push(ring.generation(), ReadyFrame::Bytes(vec![0u8; 4].into()))
            );
        }

        assert_eq!(
            Pushed::Full,
            ring.push(ring.generation(), ReadyFrame::Bytes(vec![0u8; 4].into()))
        );
        assert_eq!(ring.capacity(), ring.len());
    }

    /// A window's worth of frames arrives intact: the criterion a ring of 64
    /// would fail, and the reason the capacity is derived rather than written
    /// down.
    #[test]
    fn a_window_of_frames_survives_a_burst() {
        let wire = Wire::new();
        let stack = stack(&wire);
        let (_, ring) = bind(&stack, 40462);
        let far = &mut far_end(&wire);

        let window = stack.params().receive_window / stack.params().mtu;
        for _ in 0..=window {
            send_udp(far, OWN, 40462, b"one window's worth");
        }

        while wire.queued(Side::B) > 0 {
            stack.pump();
        }

        assert_eq!(window + 1, ring.len());
        assert_eq!(0, stack.counters().queue_drop.load(Ordering::Relaxed));
    }

    /// G3 through the whole path: a wildcard binding hears what is addressed
    /// to the port's own address, and nothing else.
    #[test]
    fn a_wildcard_binding_hears_the_ports_own_address() {
        let wire = Wire::new();
        let stack = stack(&wire);
        let (_, ring) = stack
            .bind(None, 40463, None, SocketAddr::from((OWN, 40463)))
            .expect("a bind");
        let far = &mut far_end(&wire);

        send_udp(far, OWN, 40463, b"to the wildcard");
        stack.pump();
        assert_eq!(1, ring.len());

        send_udp(far, PEER, 40463, b"to somebody else");
        stack.pump();
        assert_eq!(1, ring.len(), "and only that address");
    }

    /// A frame addressed to another port's MAC is read and not filed: the
    /// design drops it silently, with no counter, because a port that is not
    /// in promiscuous mode is never handed one.
    #[test]
    fn a_frame_addressed_elsewhere_is_dropped() {
        let wire = Wire::new();
        let stack = stack(&wire);
        let (_, ring) = bind(&stack, 40464);

        let mut bytes = vec![0u8; frame::IPV4_UDP_HEADER_LENGTH];
        let written = frame::write_ipv4_udp(
            &mut bytes,
            &frame::UdpWrite {
                ipv4: frame::Ipv4Write {
                    destination_hardware: [0x02, 0, 0, 0, 0, 0x09],
                    source_hardware: [0x02, 0, 0, 0, 0, 0x02],
                    source: PEER,
                    destination: OWN,
                    identification: 1,
                },
                source_port: 40000,
                destination_port: 40464,
                payload_length: 0,
            },
        )
        .expect("a frame");

        far_end(&wire).tx_burst(1, &mut |buffer| {
            buffer[..written].copy_from_slice(&bytes[..written]);
            written
        });

        stack.pump();
        assert!(ring.is_empty(), "read, and not filed anywhere");
    }

    /// A wire whose every frame is corrupted reaches no transport, because
    /// nothing verifies.
    #[test]
    fn a_corrupted_frame_never_reaches_a_transport() {
        let wire = Wire::new();
        let stack = stack(&wire);
        let (_, ring) = bind(&stack, 40465);

        wire.set_injection(
            Side::B,
            Injection {
                corrupt_checksum_every: Some(1),
                ..Injection::default()
            },
        );
        send_udp(&mut far_end(&wire), OWN, 40465, b"damaged");

        stack.pump();
        assert!(ring.is_empty());
    }

    /// The sender's thread pumps nothing: its frames are put in its rings by
    /// the receiver's pump, and a second pump would take the same frames
    /// twice.
    #[test]
    fn only_the_receivers_thread_pumps() {
        let wire = Wire::new();
        let stack = stack(&wire);
        let (_, ring) = bind(&stack, 40467);

        send_udp(&mut far_end(&wire), OWN, 40467, b"one frame");

        let sender = ThreadQueues::new(Arc::clone(&stack), Affinity::Sender);
        assert_eq!(0, sender.pump());
        assert_eq!(1, wire.queued(Side::B), "the frame is still on the wire");

        let receiver = ThreadQueues::new(Arc::clone(&stack), Affinity::Receiver);
        assert_eq!(1, receiver.pump());
        assert_eq!(1, ring.len());

        assert_eq!(
            2,
            stack.counters().poller.load(Ordering::Relaxed),
            "both threads count their passes, and the sender's found nothing to do"
        );
    }

    /// A port that is bound hears nothing until somebody sends to it: the
    /// criterion every "nothing arrived" rests on (§5.4 of the plan).
    #[test]
    fn a_bound_port_with_nothing_sent_receives_nothing() {
        let wire = Wire::new();
        let stack = stack(&wire);
        let (_, ring) = bind(&stack, 40466);

        assert_eq!(0, stack.pump());
        assert_eq!(0, wire.queued(Side::B));
        assert!(ring.is_empty());
    }
}
