//! A port over a wire of queues, and the wire's own knobs.
//!
//! The whole userspace stack — two drivers talking to each other, the
//! demultiplexer, the port table, ARP — runs over this in the default test
//! build, with no privileges and no hardware. It is therefore written to be
//! **like a wire and not like a switch**: what one end sends the other end is
//! handed, in order, whatever it is addressed to. Splitting frames by
//! destination is the *stack's* job ([`super::demux`]) and putting it here
//! would leave the port table and the demultiplexer untested, which is most of
//! what G1–G6 and G12 are about.
//!
//! # The five knobs, and why the fifth exists
//!
//! Losing, reordering, duplicating and corrupting a frame are the four things
//! a real link does to a protocol, and the stack has answers for each of them.
//! The fifth — a ceiling on how many frames a direction delivers per burst —
//! is what makes G7 and G8 reachable at all: "the port took three of the five
//! it was offered" and "the port took none" are answers about a *capacity*,
//! and no amount of packet loss produces the first one. A frame the ceiling
//! holds back is not dropped: it is still queued, waiting for the next burst,
//! which is what makes the difference between back pressure and loss
//! observable.
//!
//! **A wire with nothing on it delivers nothing**, and that is the only thing
//! the tests need to know about emptiness. It is worth saying out loud because
//! the DPDK path taught this stack that a port can invent frames: `net_null`
//! does exactly that (the D3 record's "实测还教了一件事"), so "nothing arrived"
//! has to rest on "nothing was pushed", which is what
//! [`Wire::queued`] answers, rather than on a port's own silence.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use super::frame::{Checksums, L4Checksum, TxOffloads};
use super::packet_io::{Disposition, Offloads, ReadyFrame, RxFrame, RxPort, TxPort};
use super::ports::Binding;

/// Which end of a wire a port is on.
///
/// Frames a port on one side sends come out of the other side's ports, and
/// nowhere else — that is the whole of the routing a wire does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Side {
    /// The first end, conventionally `10.10.0.1`.
    A,
    /// The second end, conventionally `10.10.0.2`.
    B,
}

impl Side {
    /// The side frames sent from here arrive at.
    pub const fn other(self) -> Self {
        match self {
            Self::A => Self::B,
            Self::B => Self::A,
        }
    }

    const fn index(self) -> usize {
        match self {
            Self::A => 0,
            Self::B => 1,
        }
    }
}

/// What a direction of the wire does to the frames that cross it.
///
/// Every knob counts the frames that direction has carried, so `every` is a
/// period: `drop_every = 3` loses the third, sixth, ninth. The default is a
/// wire that carries everything, in order, unchanged.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Injection {
    /// Lose every nth frame.
    pub drop_every: Option<u64>,
    /// Deliver every nth frame twice.
    pub duplicate_every: Option<u64>,
    /// Deliver the frames out of order: each new frame is placed in front of
    /// the previous one, so the two swap whenever both are still queued.
    pub reorder: bool,
    /// Flip the last byte of every nth frame, which leaves the UDP checksum
    /// covering bytes that are no longer there — a frame a receiver that
    /// checks its checksums must drop, and one a receiver that does not will
    /// deliver with a bad payload.
    pub corrupt_checksum_every: Option<u64>,
    /// Hand over at most this many frames per receive, holding the rest back
    /// rather than dropping them.
    pub capacity: Option<usize>,
}

impl Injection {
    /// Whether `count` — the number of frames this direction has carried,
    /// counting from one — is a frame the knob named by `every` acts on.
    fn every(count: u64, every: Option<u64>) -> bool {
        every.is_some_and(|every| every > 0 && count % every == 0)
    }
}

/// The wire's shared state: two queues and two sets of knobs.
#[derive(Debug)]
struct WireState {
    /// What each side has sent and the other has not yet taken.
    queues: [VecDeque<Box<[u8]>>; 2],
    /// Frames carried per direction, for the period knobs.
    carried: [u64; 2],
    /// What each direction does to them.
    injection: [Injection; 2],
}

/// A wire between two ends, and the ports on it.
///
/// Cloning a `Wire` shares the same wire, which is how the two stacks of a T2
/// test (and the six ports they need between them) end up on one link.
#[derive(Clone, Debug)]
pub struct Wire {
    state: Arc<Mutex<WireState>>,
}

impl Wire {
    /// An empty wire that carries everything, both ways.
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(WireState {
                queues: [VecDeque::new(), VecDeque::new()],
                carried: [0, 0],
                injection: [Injection::default(), Injection::default()],
            })),
        }
    }

    /// A port on `side` of this wire, with the MAC and maximum frame length it
    /// answers for.
    ///
    /// The three ports a stack needs are made from this: its receive port and
    /// the two transmit ports, all on the same side.
    pub fn port(&self, side: Side, mac: [u8; 6], mtu: usize) -> FakePort {
        FakePort {
            wire: self.clone(),
            side,
            mac,
            mtu,
        }
    }

    /// What the direction leaving `side` does to the frames on it.
    pub fn set_injection(&self, side: Side, injection: Injection) {
        self.state.lock().expect("the wire").injection[side.index()] = injection;
    }

    /// How many frames `side` has sent that the other end has not taken —
    /// which is also how a test says "nothing was sent".
    pub fn queued(&self, side: Side) -> usize {
        self.state.lock().expect("the wire").queues[side.index()].len()
    }

    /// How many frames have crossed the direction leaving `side`, dropped and
    /// duplicated ones included. This is the count the period knobs run on.
    pub fn carried(&self, side: Side) -> u64 {
        self.state.lock().expect("the wire").carried[side.index()]
    }

    /// Put a frame on the wire, doing to it whatever the knobs for that
    /// direction say.
    fn enqueue(&self, side: Side, frame: Box<[u8]>) {
        let mut state = self.state.lock().expect("the wire");
        let carried = &mut state.carried[side.index()];
        *carried += 1;
        let count = *carried;
        let injection = state.injection[side.index()];

        if Injection::every(count, injection.drop_every) {
            return;
        }

        let frame = if Injection::every(count, injection.corrupt_checksum_every) {
            let mut corrupted = frame.into_vec();
            if let Some(last) = corrupted.last_mut() {
                *last ^= 0xff;
            }
            corrupted.into_boxed_slice()
        } else {
            frame
        };

        let copy = Injection::every(count, injection.duplicate_every).then(|| frame.clone());
        let queue = &mut state.queues[side.index()];

        if injection.reorder && !queue.is_empty() {
            queue.insert(queue.len() - 1, frame);
        } else {
            queue.push_back(frame);
        }

        if let Some(copy) = copy {
            queue.push_back(copy);
        }
    }

    /// Put a frame on the wire from `side`, as a port on it would — the
    /// injection and all, because it is the same path.
    ///
    /// For a test that has something to say to a stack without wanting to be a
    /// stack: the far end of a capture, an ARP request from a peer that does
    /// not exist here, a frame that is deliberately wrong.
    pub fn send(&self, side: Side, frame: &[u8]) {
        self.enqueue(side, frame.to_vec().into_boxed_slice());
    }

    /// Take up to `max` frames that crossed into `side`, in order.
    ///
    /// The wire as a wire, for reading what a stack sent without a port to
    /// read it through.
    pub fn take(&self, side: Side, max: usize) -> Vec<Box<[u8]>> {
        std::iter::from_fn(|| self.pop(side)).take(max).collect()
    }

    /// The next frame crossing into `side`, if any.
    fn pop(&self, side: Side) -> Option<Box<[u8]>> {
        self.state.lock().expect("the wire").queues[side.other().index()].pop_front()
    }

    /// How many frames a port on `side` may take in one burst: the ceiling the
    /// other direction's knob sets, or as many as it asks for.
    fn delivery_capacity(&self, side: Side) -> usize {
        self.state.lock().expect("the wire").injection[side.other().index()]
            .capacity
            .unwrap_or(usize::MAX)
    }
}

impl Default for Wire {
    fn default() -> Self {
        Self::new()
    }
}

/// A port on a [`Wire`]: whatever crosses the wire into this side comes out of
/// [`RxPort::rx_burst`], and whatever [`TxPort::tx_burst`] is given goes to the
/// other side.
#[derive(Clone, Debug)]
pub struct FakePort {
    wire: Wire,
    side: Side,
    mac: [u8; 6],
    mtu: usize,
}

impl FakePort {
    /// The side of the wire this port is on.
    pub const fn side(&self) -> Side {
        self.side
    }
}

impl RxPort for FakePort {
    fn rx_burst(
        &mut self,
        max: usize,
        f: &mut dyn FnMut(RxFrame<'_>) -> Disposition,
        kept: &mut Vec<(Binding, ReadyFrame)>,
    ) -> usize {
        let limit = max.min(self.wire.delivery_capacity(self.side));

        let mut handed = 0;
        while handed < limit {
            let Some(frame) = self.wire.pop(self.side) else {
                break;
            };

            match f(RxFrame {
                bytes: &frame,
                // A wire has no NIC to ask, so every frame's checksums are the
                // stack's to verify.
                checksums: Checksums::Software,
            }) {
                Disposition::Consumed => {}
                Disposition::Keep(binding) => kept.push((binding, ReadyFrame::Bytes(frame))),
            }
            handed += 1;
        }

        handed
    }

    /// Nothing: a wire has no hardware to ask, and every frame it carries
    /// arrives with `Checksums::Software` for exactly that reason.
    fn offloads(&self) -> Offloads {
        Offloads::NONE
    }

    fn mac(&self) -> [u8; 6] {
        self.mac
    }

    fn mtu(&self) -> usize {
        self.mtu
    }
}

impl TxPort for FakePort {
    fn tx_burst(
        &mut self,
        count: usize,
        fill: &mut dyn FnMut(&mut [u8]) -> (usize, TxOffloads),
    ) -> usize {
        let mut scratch = vec![0u8; self.mtu];
        let mut taken = 0;

        for _ in 0..count {
            let (length, offloads) = fill(&mut scratch);

            // A wire has no hardware to compute anything, so a frame that asks
            // it for a checksum is a frame whose author and whose port
            // disagree — which is the one thing the per-frame offload exists to
            // make impossible. Asserted rather than ignored: silently sending
            // the pseudo-header sum as if it were a checksum is exactly the
            // failure this build would then be unable to see.
            assert!(
                !offloads.asked().0,
                "the in-memory wire computes no checksums, and this frame asks \
                 for one"
            );

            // A frame builder that answers more than the buffer holds gets
            // nothing transmitted: truncating it would put a frame on the wire
            // that is not the one the caller built, and the caller would count
            // it as sent.
            if length == 0 || length > scratch.len() {
                continue;
            }

            self.wire
                .enqueue(self.side, scratch[..length].to_vec().into_boxed_slice());
            taken += 1;
        }

        taken
    }

    fn checksums(&self) -> L4Checksum {
        // Every frame this port carries has its checksums written by whoever
        // built it — which is what the assertion in `tx_burst` above holds the
        // callers to.
        L4Checksum::Software
    }

    fn mac(&self) -> [u8; 6] {
        self.mac
    }

    fn mtu(&self) -> usize {
        self.mtu
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn port(wire: &Wire, side: Side) -> FakePort {
        let mac = match side {
            Side::A => [0x02, 0, 0, 0, 0, 0x01],
            Side::B => [0x02, 0, 0, 0, 0, 0x02],
        };

        wire.port(side, mac, 1500)
    }

    /// Send one frame of `length` bytes, filled with `byte`.
    fn send(port: &mut FakePort, byte: u8, length: usize) -> usize {
        port.tx_burst(1, &mut |buffer| {
            buffer[..length].fill(byte);

            (length, TxOffloads::NONE)
        })
    }

    /// Everything a port has to hand, and what it was asked to keep.
    fn receive(port: &mut FakePort, max: usize) -> Vec<Vec<u8>> {
        let mut kept = Vec::new();
        // Nothing here binds a port, so the binding is a placeholder: what it
        // holds is the caller's business, and the wire only has to carry it.
        let binding = Binding {
            slot: 0,
            port: 0,
            generation: 0,
        };
        port.rx_burst(max, &mut |_| Disposition::Keep(binding), &mut kept);

        kept.into_iter()
            .map(|(_, frame)| frame.as_bytes().to_vec())
            .collect()
    }

    #[test]
    fn what_one_end_sends_the_other_ends_ports_receive() {
        let wire = Wire::new();
        let mut sender = port(&wire, Side::A);
        let mut receiver = port(&wire, Side::B);

        assert_eq!(1, send(&mut sender, 0xab, 8));
        assert_eq!(vec![vec![0xab; 8]], receive(&mut receiver, 8));
    }

    /// Frames do not come back to their sender: a wire between two ends is two
    /// directions, not a loop.
    #[test]
    fn a_frame_does_not_come_back_to_the_port_that_sent_it() {
        let wire = Wire::new();
        let mut sender = port(&wire, Side::A);
        let mut same_side = port(&wire, Side::A);

        assert_eq!(1, send(&mut sender, 0x01, 4));
        assert!(receive(&mut same_side, 8).is_empty());
    }

    /// A frame the callback consumes is gone; one it keeps is the caller's,
    /// with the slot it named.
    #[test]
    fn a_kept_frame_comes_out_with_its_slot() {
        let wire = Wire::new();
        let mut sender = port(&wire, Side::A);
        let mut receiver = port(&wire, Side::B);

        assert_eq!(2, {
            let mut taken = 0;
            taken += send(&mut sender, 0x11, 4);
            taken += send(&mut sender, 0x22, 4);
            taken
        });

        let mut kept = Vec::new();
        let handed = receiver.rx_burst(
            8,
            &mut |frame| {
                if frame.bytes[0] == 0x11 {
                    Disposition::Keep(Binding {
                        slot: 7,
                        port: 44000,
                        generation: 3,
                    })
                } else {
                    Disposition::Consumed
                }
            },
            &mut kept,
        );

        assert_eq!(2, handed, "both were handed over");
        assert_eq!(1, kept.len(), "and one was kept");
        assert_eq!(
            Binding {
                slot: 7,
                port: 44000,
                generation: 3
            },
            kept[0].0
        );
        assert_eq!(&[0x11; 4], kept[0].1.as_bytes());
        assert!(receive(&mut receiver, 8).is_empty(), "the other is gone");
    }

    /// A wire with nothing on it delivers nothing — the judgement §5.4 of the
    /// plan requires to rest on what was pushed rather than on a port's
    /// silence.
    #[test]
    fn an_empty_wire_delivers_nothing() {
        let wire = Wire::new();
        let mut port = port(&wire, Side::B);

        assert_eq!(0, wire.queued(Side::A));
        assert!(receive(&mut port, 8).is_empty());
    }

    #[test]
    fn the_knobs_are_per_direction() {
        let wire = Wire::new();
        let mut a = port(&wire, Side::A);
        let mut b = port(&wire, Side::B);
        wire.set_injection(
            Side::A,
            Injection {
                drop_every: Some(1),
                ..Injection::default()
            },
        );

        assert_eq!(1, send(&mut a, 0xaa, 4), "A's port took it");
        assert_eq!(0, wire.queued(Side::A), "and the wire lost it");
        assert!(receive(&mut b, 8).is_empty());

        assert_eq!(1, send(&mut b, 0xbb, 4));
        assert_eq!(
            vec![vec![0xbb; 4]],
            receive(&mut a, 8),
            "B's direction is untouched"
        );
    }

    #[test]
    fn dropping_every_nth_frame_leaves_the_others() {
        let wire = Wire::new();
        let mut a = port(&wire, Side::A);
        let mut b = port(&wire, Side::B);
        wire.set_injection(
            Side::A,
            Injection {
                drop_every: Some(2),
                ..Injection::default()
            },
        );

        for byte in 1..=4 {
            assert_eq!(1, send(&mut a, byte, 4));
        }

        assert_eq!(
            vec![vec![1; 4], vec![3; 4]],
            receive(&mut b, 8),
            "the second and the fourth were lost"
        );
        assert_eq!(4, wire.carried(Side::A), "four were offered");
    }

    #[test]
    fn duplicating_every_nth_frame_delivers_it_twice() {
        let wire = Wire::new();
        let mut a = port(&wire, Side::A);
        let mut b = port(&wire, Side::B);
        wire.set_injection(
            Side::A,
            Injection {
                duplicate_every: Some(2),
                ..Injection::default()
            },
        );

        assert_eq!(1, send(&mut a, 1, 4));
        assert_eq!(1, send(&mut a, 2, 4));

        assert_eq!(vec![vec![1; 4], vec![2; 4], vec![2; 4]], receive(&mut b, 8));
    }

    #[test]
    fn reordering_delivers_a_later_frame_first() {
        let wire = Wire::new();
        let mut a = port(&wire, Side::A);
        let mut b = port(&wire, Side::B);
        wire.set_injection(
            Side::A,
            Injection {
                reorder: true,
                ..Injection::default()
            },
        );

        assert_eq!(1, send(&mut a, 1, 4));
        assert_eq!(1, send(&mut a, 2, 4));
        assert_eq!(1, send(&mut a, 3, 4));

        let order: Vec<u8> = receive(&mut b, 8)
            .into_iter()
            .map(|frame| frame[0])
            .collect();
        assert_eq!(vec![2, 3, 1], order, "the first frame arrives last");
    }

    /// Corrupting is done to the bytes, not to a checksum field, so a receiver
    /// that verifies sees it and one that does not gets a wrong payload.
    #[test]
    fn corrupting_every_nth_frame_changes_its_bytes() {
        let wire = Wire::new();
        let mut a = port(&wire, Side::A);
        let mut b = port(&wire, Side::B);
        wire.set_injection(
            Side::A,
            Injection {
                corrupt_checksum_every: Some(1),
                ..Injection::default()
            },
        );

        assert_eq!(1, send(&mut a, 0x0f, 4));
        assert_eq!(vec![vec![0x0f, 0x0f, 0x0f, 0xf0]], receive(&mut b, 8));
    }

    /// The fifth knob: a delivery ceiling holds frames back rather than losing
    /// them, which is what separates back pressure from loss.
    #[test]
    fn a_capacity_holds_frames_back_for_the_next_burst() {
        let wire = Wire::new();
        let mut a = port(&wire, Side::A);
        let mut b = port(&wire, Side::B);
        wire.set_injection(
            Side::A,
            Injection {
                capacity: Some(1),
                ..Injection::default()
            },
        );

        for byte in 1..=3 {
            assert_eq!(1, send(&mut a, byte, 4));
        }

        assert_eq!(vec![vec![1; 4]], receive(&mut b, 8));
        assert_eq!(2, wire.queued(Side::A), "the rest are still on the wire");
        assert_eq!(vec![vec![2; 4]], receive(&mut b, 8), "one a burst, still");
        assert_eq!(vec![vec![3; 4]], receive(&mut b, 8));
        assert_eq!(0, wire.queued(Side::A), "and nothing was lost");
    }

    /// A frame the builder could not fit gets nothing transmitted rather than
    /// a truncated frame the caller believes it sent.
    /// **A frame that asks the wire for a checksum is refused**, and that
    /// assertion is what every test that sends through a fake port leans on.
    ///
    /// A wire has no hardware to compute anything, so a frame asking it for one
    /// is a frame whose author and whose port disagree about who writes the
    /// checksums — and in a build with a NIC on the other end of that
    /// disagreement, the frame goes out carrying the pseudo-header sum as if it
    /// were a checksum. Nothing here can read a sent frame back, so the only
    /// place it can be caught is before it leaves.
    ///
    /// The ask comes from a frame builder, which is the only thing that can
    /// make one (`frame::TxOffloads`), and not from a value written out by
    /// hand: that is the same rule seen from the other side.
    #[test]
    #[should_panic(expected = "computes no checksums")]
    fn a_frame_that_asks_the_wire_for_a_checksum_is_refused() {
        let wire = Wire::new();
        let mut port = port(&wire, Side::A);
        let write = crate::media::userspace::frame::UdpWrite {
            ipv4: crate::media::userspace::frame::Ipv4Write {
                destination_hardware: [0x02, 0, 0, 0, 0, 0x02],
                source_hardware: [0x02, 0, 0, 0, 0, 0x01],
                source: std::net::Ipv4Addr::new(10, 10, 0, 1),
                destination: std::net::Ipv4Addr::new(10, 10, 0, 2),
                identification: 1,
            },
            source_port: 40_000,
            destination_port: 40_001,
            payload_length: 8,
        };

        port.tx_burst(1, &mut |room| {
            crate::media::userspace::frame::write_ipv4_udp(
                room,
                &write,
                crate::media::userspace::frame::L4Checksum::ByTheNic,
            )
            .expect("a frame")
        });
    }

    #[test]
    fn a_frame_longer_than_the_port_carries_is_not_sent() {
        let wire = Wire::new();
        let mut a = wire.port(Side::A, [0x02, 0, 0, 0, 0, 0x01], 16);

        let taken = a.tx_burst(1, &mut |buffer| (buffer.len() + 1, TxOffloads::NONE));

        assert_eq!(0, taken);
        assert_eq!(0, wire.queued(Side::A));
    }
}
