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
    RxFrame as DpdkFrame,
};

use super::frame::ETHERNET_HEADER_LENGTH;
use super::packet_io::{Disposition, Offloads, ReadyFrame, RxFrame, RxPort, TxPort, Verdict};
use super::ports::Binding;

/// The queue pair a port drives, and whether the port beside it drives it too.
///
/// A pair is one receive queue and one transmit queue at the same index, and
/// §4.7's map does not always give each role one of its own: `DEDICATED` has a
/// thread per role and a queue each, while the modes with both agents on one
/// thread have **one** pair between them — the receive port takes its receive
/// queue and the transmit port its transmit queue, out of the same pair.
///
/// The two shapes are one type because the difference is not worth two: a port
/// that owns its pair pays nothing, and a port that shares one pays an
/// uncontended lock — which is what keeps the crate's invariant true, since
/// `QueuePair` is deliberately not `Sync` and two roles reaching one queue at
/// once is exactly what that says must not happen.
enum Pair {
    /// This port's own, one per role.
    Owned(QueuePair),
    /// One pair, shared with the port on the other side of the stack.
    Shared(Arc<Mutex<QueuePair>>),
}

impl std::fmt::Debug for Pair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Owned(pair) => pair.fmt(f),
            Self::Shared(pair) => pair.lock().expect("the queue pair").fmt(f),
        }
    }
}

impl Pair {
    /// Run `f` on the pair, taking the lock where there is one.
    fn with<R>(&mut self, f: impl FnOnce(&mut QueuePair) -> R) -> R {
        match self {
            Self::Owned(pair) => f(pair),
            Self::Shared(pair) => f(&mut pair.lock().expect("the queue pair")),
        }
    }

    /// What the port behind the pair is, whichever shape this is.
    fn port(&self) -> Arc<deepmsg_dpdk::Port> {
        match self {
            Self::Owned(pair) => Arc::clone(pair.port()),
            Self::Shared(pair) => Arc::clone(pair.lock().expect("the queue pair").port()),
        }
    }
}

/// The receive half of a queue pair, as this stack's [`RxPort`].
pub struct DpdkRxPort {
    pair: Pair,
    /// The pool the queue was set up with, which every frame it hands over
    /// borrows. One per process, which is why the borrow can be `'static`.
    pool: &'static Mempool,
    mac: [u8; 6],
    mtu: usize,
    offloads: Offloads,
}

impl DpdkRxPort {
    /// A receive port over `pair`, whose frames come from `pool`.
    ///
    /// The port describes itself rather than being told: the MAC every frame
    /// carries and the largest frame it carries come from the device, and what
    /// it judges comes from its receive capabilities. Those are the same two
    /// offloads the driver asks for when it configures the port, so what
    /// [`RxPort::offloads`] answers is what the frames' verdicts mean.
    pub fn new(pair: QueuePair, pool: &'static Mempool) -> Self {
        Self::of(Pair::Owned(pair), pool)
    }

    /// A receive port sharing `pair` with the transmit port beside it.
    ///
    /// What a mode with both agents on one thread has: the one queue pair the
    /// machine gives it, with the receive port taking one direction and the
    /// transmit port the other.
    pub fn sharing(pair: Arc<Mutex<QueuePair>>, pool: &'static Mempool) -> Self {
        Self::of(Pair::Shared(pair), pool)
    }

    fn of(pair: Pair, pool: &'static Mempool) -> Self {
        let port = pair.port();
        let info = port.info();
        let offloads = Offloads {
            ipv4_checksum: info.can_judge_ipv4_checksum(),
            udp_checksum: info.can_judge_udp_checksum(),
        };
        let (mac, mtu) = describe(&port);

        Self {
            pair,
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

        let handed = self.pair.with(|pair| {
            pair.rx_burst(
                pool,
                max,
                &mut |frame: DpdkFrame<'_>| {
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
                },
                &mut parked,
            )
        });

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
            .field("pair", &self.pair)
            .field("mac", &self.mac)
            .field("mtu", &self.mtu)
            .field("offloads", &self.offloads)
            .finish()
    }
}

/// The transmit half of a queue pair, as this stack's [`TxPort`].
///
/// Nothing here is copied either: the sender writes its headers and its
/// payload straight into the room the port allocated, and the port hands that
/// frame to the NIC. A frame the NIC did not take is freed inside the burst, so
/// what this answers is exactly what left the machine (§4.4).
pub struct DpdkTxPort {
    pair: Pair,
    pool: &'static Mempool,
    mac: [u8; 6],
    mtu: usize,
}

impl DpdkTxPort {
    /// A transmit port over `pair`, whose frames come from `pool`.
    pub fn new(pair: QueuePair, pool: &'static Mempool) -> Self {
        Self::of(Pair::Owned(pair), pool)
    }

    /// A transmit port sharing `pair` with the receive port beside it; see
    /// [`DpdkRxPort::sharing`].
    pub fn sharing(pair: Arc<Mutex<QueuePair>>, pool: &'static Mempool) -> Self {
        Self::of(Pair::Shared(pair), pool)
    }

    fn of(pair: Pair, pool: &'static Mempool) -> Self {
        let (mac, mtu) = describe(&pair.port());

        Self {
            pair,
            pool,
            mac,
            mtu,
        }
    }
}

impl TxPort for DpdkTxPort {
    fn tx_burst(&mut self, count: usize, fill: &mut dyn FnMut(&mut [u8]) -> usize) -> usize {
        let pool = self.pool;
        self.pair.with(|pair| pair.tx_burst(pool, count, fill))
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
            .field("pair", &self.pair)
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
