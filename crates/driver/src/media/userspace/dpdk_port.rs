//! The DPDK port: the stack's two traits over a DPDK queue pair.
//!
//! This is the only place the userspace stack touches hardware, and it is a
//! seam of two calls. Everything above it — Ethernet and IPv4 framing, ARP,
//! the port and neighbour tables, demultiplexing — is the safe Rust in this
//! module's siblings and knows nothing about DPDK; everything below it is
//! `deepmsg-dpdk`, ADR-0002's sixth zone, which knows nothing about this
//! stack. What crosses is a frame's bytes and one verdict about its checksums.
//!
//! # Why a kept frame costs no copy
//!
//! [`DpdkRxPort::rx_burst`] hands the callback a borrowed frame and takes an
//! answer back. `Keep` does not copy the bytes anywhere: the frame is already
//! an mbuf the port owns, and it leaves the burst as
//! [`ReadyFrame::Mbuf`] — the same mbuf, parked on a transport's ready list
//! until the transport's own `receive` copies it into the caller's buffer.
//! That single copy is what the kernel path pays in `recvmmsg` as well, so the
//! two media cost the same number of copies (§3.3 of the design) and the
//! difference between them is where the frames live in between, not how often
//! they are moved.
//!
//! The pool is a reference with the process's lifetime rather than a borrow of
//! the port, which is the D4 owed item from D2 closed: the crate's
//! `ParkedFrame<'pool>` borrows the pool it came from, and a pool that lives as
//! long as the process makes that lifetime `'static` without the crate's type
//! changing. The price is that the pool is never dropped in a normal exit, so
//! "every frame went back to it" is a thing to assert rather than a thing the
//! type guarantees — which the tests below do.

use std::sync::{Arc, Mutex};

use deepmsg_dpdk::{
    Checksum as DpdkChecksum, Disposition as DpdkDisposition, Mempool, QueuePair,
    RxFrame as DpdkFrame, RxQueue, TxOffloads as DpdkOffloads, TxQueue,
};

use super::frame::{ETHERNET_HEADER_LENGTH, L4Checksum, TxOffloads};
use super::packet_io::{Disposition, Offloads, ReadyFrame, RxFrame, RxPort, TxPort, Verdict};
use super::ports::Binding;

/// What a receive port polls, and whether the port beside it drives the same
/// queues too.
///
/// §4.7's map does not always give each role one queue of its own: `DEDICATED`
/// has a thread per role, the receive thread polls **every** receive queue the
/// device was given, and the two sending agents take a transmit queue each;
/// the modes with both agents on one thread have **one** pair between them —
/// the receive port takes its receive queue and the transmit port its
/// transmit queue, out of the same pair.
///
/// Why every queue: which queue the **device** puts an inbound frame on is not
/// this build's to choose. A virtio port announces
/// `RTE_MAX(nb_rx_queues, nb_tx_queues)` queue pairs at device start
/// (`drivers/net/virtio/virtio_ethdev.c:2383-2385`) and the backend hashes inbound flows across
/// all of them, so a port that polled one receive queue would never see the
/// flows that hashed to the others. That is the 2026-11-23 Aliyun finding:
/// `DEDICATED` over a virtio ENI lost every burst whose tuple landed on a
/// queue nobody polled, while `SHARED` (one pair) took them all.
///
/// The two shapes are one type because the difference is not worth two: a port
/// that owns its queues pays nothing, and a port that shares a pair pays an
/// uncontended lock — which is what keeps the crate's invariant true, since
/// `QueuePair` is deliberately not `Sync` and two roles reaching one queue at
/// once is exactly what that says must not happen.
enum Receiving {
    /// This port's own receive queues, in the order the port takes them, and
    /// the queue this call starts with. The one receive thread holds all of
    /// them, so the rotation needs no lock.
    Owned { queues: Vec<RxQueue>, next: usize },
    /// One pair, shared with the port on the other side of the stack.
    Shared(Arc<Mutex<QueuePair>>),
}

impl std::fmt::Debug for Receiving {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Owned { queues, .. } => f.debug_list().entries(queues.iter()).finish(),
            Self::Shared(pair) => pair.lock().expect("the queue pair").fmt(f),
        }
    }
}

impl Receiving {
    /// What the port behind these queues is, whichever shape this is.
    fn port(&self) -> Arc<deepmsg_dpdk::Port> {
        match self {
            Self::Owned { queues, .. } => {
                Arc::clone(queues.first().expect("a receive port polls a queue").port())
            }
            Self::Shared(pair) => Arc::clone(pair.lock().expect("the queue pair").port()),
        }
    }
}

/// The transmit queue a port sends on, and whether the port beside it shares
/// the pair it came from.
///
/// `DEDICATED` gives each sending agent a queue of its own — two agents
/// sharing one transmit queue is the 2026-11-16 lost-frame shape — and the
/// single-thread modes give the one pair to both directions.
enum Transmitting {
    /// This port's own transmit queue.
    Owned(TxQueue),
    /// One pair, shared with the port on the other side of the stack.
    Shared(Arc<Mutex<QueuePair>>),
}

impl std::fmt::Debug for Transmitting {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Owned(queue) => queue.fmt(f),
            Self::Shared(pair) => pair.lock().expect("the queue pair").fmt(f),
        }
    }
}

impl Transmitting {
    /// What the port behind this queue is, whichever shape this is.
    fn port(&self) -> Arc<deepmsg_dpdk::Port> {
        match self {
            Self::Owned(queue) => Arc::clone(queue.port()),
            Self::Shared(pair) => Arc::clone(pair.lock().expect("the queue pair").port()),
        }
    }
}

/// The receive side of a queue pair, as this stack's [`RxPort`].
pub struct DpdkRxPort {
    queues: Receiving,
    /// The pool the queues were set up with, which every frame they hand over
    /// borrows. One per process, which is why the borrow can be `'static`.
    pool: &'static Mempool,
    mac: [u8; 6],
    mtu: usize,
    offloads: Offloads,
}

impl DpdkRxPort {
    /// A receive port over `pair`'s receive queue, whose frames come from
    /// `pool`.
    ///
    /// The pair's transmit half is dropped: a case that builds a receive port
    /// from one pair receives and does not send, and the queue it does not use
    /// is the device's to leave idle.
    pub fn new(pair: QueuePair, pool: &'static Mempool) -> Self {
        Self::over(vec![pair.split().0], pool)
    }

    /// A receive port polling **every** queue in `queues`, in that order.
    ///
    /// This is the `DEDICATED` shape: one receive thread, and every receive
    /// queue the device was given, because the backend's hash decides which of
    /// them a frame arrives on ([`Receiving`]).
    pub fn over(queues: Vec<RxQueue>, pool: &'static Mempool) -> Self {
        debug_assert!(
            !queues.is_empty(),
            "a receive port polls at least one queue"
        );
        Self::of(Receiving::Owned { queues, next: 0 }, pool)
    }

    /// A receive port sharing `pair` with the transmit port beside it.
    ///
    /// What a mode with both agents on one thread has: the one queue pair the
    /// machine gives it, with the receive port taking one direction and the
    /// transmit port the other.
    pub fn sharing(pair: Arc<Mutex<QueuePair>>, pool: &'static Mempool) -> Self {
        Self::of(Receiving::Shared(pair), pool)
    }

    fn of(queues: Receiving, pool: &'static Mempool) -> Self {
        let port = queues.port();
        let info = port.info();
        let offloads = Offloads {
            ipv4_checksum: info.can_judge_ipv4_checksum(),
            udp_checksum: info.can_judge_udp_checksum(),
        };
        let (mac, mtu) = describe(&port);

        Self {
            queues,
            pool,
            mac,
            mtu,
            offloads,
        }
    }
}

impl RxPort for DpdkRxPort {
    fn rx_burst(
        &mut self,
        max: usize,
        f: &mut dyn FnMut(RxFrame<'_>) -> Disposition,
        kept: &mut Vec<(Binding, ReadyFrame)>,
    ) -> usize {
        let pool = self.pool;
        let offloads = self.offloads;

        // Two lists for one burst, and they pair up by position: the crate's
        // burst answers with the frames it kept, and which binding each one
        // belongs to is known only inside the callback. Both are appended in
        // callback order, and only for this burst — `kept` is the caller's and
        // holds whatever an earlier one left in it.
        let mut bindings = Vec::new();
        let mut parked = Vec::new();

        let mut answer = |frame: DpdkFrame<'_>| {
            let answer = f(RxFrame {
                bytes: frame.bytes(),
                checksums: offloads.checksums(verdict(frame.checksum())),
            });

            match answer {
                Disposition::Consumed => DpdkDisposition::Release,
                Disposition::Keep(binding) => {
                    bindings.push(binding);
                    DpdkDisposition::Keep
                }
            }
        };

        let handed = match &mut self.queues {
            Receiving::Owned { queues, next } => {
                // One burst per queue, starting where the last call left off:
                // the budget stays the caller's, the queues take turns, and a
                // queue that is empty costs one `rte_eth_rx_burst` and no
                // more. `max` is spent across the queues, not given to each —
                // a pass carries the frames the caller asked for however many
                // queues they arrived on.
                let count = queues.len();
                let mut total = 0;
                for _ in 0..count {
                    if total >= max {
                        break;
                    }

                    let queue = &mut queues[*next];
                    *next = (*next + 1) % count;
                    total += queue.rx_burst(pool, max - total, &mut answer, &mut parked);
                }
                total
            }
            Receiving::Shared(pair) => {
                let mut pair = pair.lock().expect("the queue pair");
                pair.rx_burst(pool, max, &mut answer, &mut parked)
            }
        };

        debug_assert_eq!(
            bindings.len(),
            parked.len(),
            "the crate keeps one frame per `Keep`, so the two lists are the same length"
        );
        kept.extend(
            bindings
                .into_iter()
                .zip(parked)
                .map(|(binding, frame)| (binding, ReadyFrame::Mbuf(frame))),
        );

        handed
    }

    fn offloads(&self) -> Offloads {
        self.offloads
    }

    fn mac(&self) -> [u8; 6] {
        self.mac
    }

    fn mtu(&self) -> usize {
        self.mtu
    }
}

impl std::fmt::Debug for DpdkRxPort {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DpdkRxPort")
            .field("queues", &self.queues)
            .field("mac", &self.mac)
            .field("mtu", &self.mtu)
            .field("offloads", &self.offloads)
            .finish()
    }
}

/// The transmit side of a queue pair, as this stack's [`TxPort`].
///
/// Nothing here is copied either: the sender writes its headers and its
/// payload straight into the room the port allocated, and the port hands that
/// frame to the NIC. A frame the NIC did not take is freed inside the burst, so
/// what this answers is exactly what left the machine (§4.4).
pub struct DpdkTxPort {
    queue: Transmitting,
    pool: &'static Mempool,
    mac: [u8; 6],
    mtu: usize,
    /// Whether this port's NIC computes the two checksums a frame carries
    /// ([`L4Checksum`]), read when the port was built.
    ///
    /// **The started port's answer, not the device's wish**: the media asks for
    /// exactly the checksum offloads the port says it has
    /// (`PortSettings::asking_for_checksum_offloads`), and DPDK refuses a bit
    /// the device has not got — so "the port can" and "the queue was configured
    /// for it" are one fact here, the same way they are for the receive side
    /// (`DpdkRxPort::of`).
    ///
    /// Both halves are required together, which is the other half of the rule:
    /// a frame's mode decides the IPv4 header's field **and** the datagram's,
    /// so a port that computes one of them and not the other is asked for
    /// neither and its frames are written out in full.
    computes_checksums: bool,
}

impl DpdkTxPort {
    /// A transmit port over `pair`'s transmit queue, whose frames come from
    /// `pool`.
    ///
    /// The pair's receive half is dropped: a case that builds a transmit port
    /// from one pair sends and does not receive.
    pub fn new(pair: QueuePair, pool: &'static Mempool) -> Self {
        Self::on(pair.split().1, pool)
    }

    /// A transmit port over its own `queue`.
    ///
    /// This is the `DEDICATED` shape: each sending agent gets one transmit
    /// queue, and no agent shares one with another.
    pub fn on(queue: TxQueue, pool: &'static Mempool) -> Self {
        Self::of(Transmitting::Owned(queue), pool)
    }

    /// A transmit port sharing `pair` with the receive port beside it; see
    /// [`DpdkRxPort::sharing`].
    pub fn sharing(pair: Arc<Mutex<QueuePair>>, pool: &'static Mempool) -> Self {
        Self::of(Transmitting::Shared(pair), pool)
    }

    fn of(queue: Transmitting, pool: &'static Mempool) -> Self {
        let port = queue.port();
        let (mac, mtu) = describe(&port);
        let computes_checksums = port.info().can_compute_both_checksums();

        Self {
            queue,
            pool,
            mac,
            mtu,
            computes_checksums,
        }
    }
}

impl TxPort for DpdkTxPort {
    fn tx_burst(
        &mut self,
        count: usize,
        fill: &mut dyn FnMut(&mut [u8]) -> (usize, TxOffloads),
    ) -> usize {
        let pool = self.pool;
        let computes = self.computes_checksums;

        // **The one place the stack's vocabulary becomes DPDK's**, and the one
        // place a frame's ask can be refused. A frame whose author wrote the
        // pseudo-header sum for a NIC that is not going to compute it carries a
        // wrong checksum on the wire and nothing this side would ever read
        // back, so the disagreement stops here rather than being sent: it is a
        // programming error, not a transient condition, and a burst that
        // reported success for it would be a burst that lied.
        let mut frame = |buffer: &mut [u8]| {
            let (written, offloads) = fill(buffer);
            let (computes_here, l2_len, l3_len) = offloads.asked();

            assert!(
                !computes_here || computes,
                "a frame asks the NIC for a checksum this port was not \
                 configured to compute (`tx_checksums` says software)"
            );

            let offloads = if computes_here {
                DpdkOffloads::ipv4_and_udp_checksums(l2_len, l3_len)
            } else {
                DpdkOffloads::NONE
            };

            (written, offloads)
        };

        match &mut self.queue {
            Transmitting::Owned(queue) => queue.tx_burst(pool, count, &mut frame),
            Transmitting::Shared(pair) => {
                let mut pair = pair.lock().expect("the queue pair");
                pair.tx_burst(pool, count, &mut frame)
            }
        }
    }

    fn checksums(&self) -> L4Checksum {
        if self.computes_checksums {
            L4Checksum::ByTheNic
        } else {
            L4Checksum::Software
        }
    }

    fn mac(&self) -> [u8; 6] {
        self.mac
    }

    fn mtu(&self) -> usize {
        self.mtu
    }
}

impl std::fmt::Debug for DpdkTxPort {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DpdkTxPort")
            .field("queue", &self.queue)
            .field("mac", &self.mac)
            .field("mtu", &self.mtu)
            .finish()
    }
}

/// What the device says about itself, read once: the MAC every frame it sends
/// carries, and the largest frame it carries.
///
/// The MTU is the *frame's*, the device's own with the Ethernet header
/// counted, because that is what a frame builder has to fit in — the number
/// [`RxPort::mtu`] promises.
fn describe(port: &deepmsg_dpdk::Port) -> ([u8; 6], usize) {
    (port.mac(), usize::from(port.mtu()) + ETHERNET_HEADER_LENGTH)
}

/// The crate's three answers about a frame's checksums, in this stack's words.
fn verdict(checksum: DpdkChecksum) -> Verdict {
    match checksum {
        DpdkChecksum::Good => Verdict::Passed,
        DpdkChecksum::Bad => Verdict::Failed,
        DpdkChecksum::NotJudged => Verdict::NotJudged,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::media::userspace::frame::Checksums;

    /// The crate's three answers keep their meaning across the seam, which is
    /// the half of this port that can be tested without a port: a `Good` that
    /// arrived as `Failed`, or a `NotJudged` that arrived as `Passed`, would be
    /// a frame let past the stack's own arithmetic by a mapping nobody looked
    /// at.
    #[test]
    fn the_crates_three_answers_keep_their_meaning() {
        let judging = Offloads {
            ipv4_checksum: true,
            udp_checksum: true,
        };

        assert_eq!(
            Checksums::Verified,
            judging.checksums(verdict(DpdkChecksum::Good)),
            "a frame the hardware passed is not verified a second time"
        );
        assert_eq!(
            Checksums::Software,
            judging.checksums(verdict(DpdkChecksum::Bad)),
            "and one it rejected is the stack's to reach the same answer about"
        );
        assert_eq!(
            Checksums::Software,
            judging.checksums(verdict(DpdkChecksum::NotJudged))
        );
    }
}
