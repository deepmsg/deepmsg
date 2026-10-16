//! What a port is: frames in, frames out, and what happens to a frame the
//! reader keeps.
//!
//! This is the design's `PacketIo` cut in two. One trait would have had to
//! carry both directions of a queue pair, and in this stack's shape the two
//! halves are held by different roles — the receiver thread owns the one
//! receive queue, and each agent thread owns the transmit queue that belongs
//! to it (§4.7's three roles). A `PacketIo` with a `tx_burst` that some
//! holders must never call, and an `rx_burst` the others must never call, is
//! two traits wearing one name; and in the DPDK build it is worse than
//! untidy — a receive queue and a transmit queue can be the same lock there,
//! and a call that crosses over inside a receive callback is how that lock
//! deadlocks.
//!
//! # What a frame the reader keeps is
//!
//! [`RxPort::rx_burst`] hands each frame to a callback and takes an answer
//! back. `Consumed` means the port may release it — for a DPDK port, that is
//! `rte_pktmbuf_free` before the burst returns. `Keep(slot)` means the frame
//! has to survive the callback, leave the port, and still be there when the
//! transport that owns `slot` is asked to receive: the port moves it into
//! `kept`, which is what [`ReadyFrame`] is. That is the whole reason the
//! callback answers instead of the port deciding — and the reason `slot`
//! travels with the frame rather than beside it, since two lists that have to
//! stay in step are one list too many (§3.3 of the plan).

use super::frame::{Checksums, L4Checksum, TxOffloads};
use super::ports::Binding;

/// Which of the stack's slots a kept frame belongs to.
///
/// A slot is one bound `(address, port)`, and the port table that finds them
/// is indexed by UDP port, so this is narrow on purpose.
pub type SlotId = u16;

/// A frame a port has handed over, borrowed for the length of the callback.
///
/// The DPDK port's `RxFrame` carries an mbuf handle beside the bytes, which is
/// how the zero-copy path parks the frame; this stack's carries only bytes,
/// and its `Keep` moves them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RxFrame<'a> {
    /// The whole frame, Ethernet header first.
    pub bytes: &'a [u8],
    /// Where this frame's checksum verdict came from, which is the port's to
    /// say: a NIC that checked them hands the answer over, one that did not
    /// leaves the arithmetic to [`super::frame::read`].
    pub checksums: Checksums,
}

/// What the reader wants done with the frame it was just handed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Disposition {
    /// Nobody wants it: the port releases it.
    Consumed,
    /// It belongs to the transport holding this binding: the port moves it
    /// into the burst's `kept` list.
    ///
    /// The whole binding travels, not just the slot, and the port does not
    /// need to know why: what the identity buys is that the frame is filed
    /// under the binding it was *routed* under. A slot is handed on to the
    /// next transport that binds the port, and the frame that was in flight
    /// while that happened must not arrive at the new one (§3.2 of the plan).
    Keep(Binding),
}

/// A frame that has left its port but not yet its transport.
///
/// One variant in a build without DPDK — the bytes, owned. The build that has
/// it adds a second, a frame still parked in the port's own pool, and the
/// transport's `receive` is the one place both end up as the caller's buffer.
/// That is where the zero-copy path stops: the frame is never copied on its way
/// from the NIC to the ready ring, and `receive` is the single copy the kernel
/// path pays too.
#[derive(Debug)]
pub enum ReadyFrame {
    /// A whole Ethernet frame, owned by the stack from here on.
    Bytes(Box<[u8]>),
    /// A frame the DPDK port parked without copying, held in the pool it came
    /// from. It goes back to that pool when this is dropped.
    #[cfg(feature = "dpdk")]
    Mbuf(deepmsg_dpdk::ParkedFrame<'static>),
}

impl ReadyFrame {
    /// The frame's bytes, whichever arm holds it.
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Bytes(bytes) => bytes,
            #[cfg(feature = "dpdk")]
            Self::Mbuf(frame) => frame.bytes(),
        }
    }
}

/// What a port's hardware did to a frame's checksums.
///
/// The DPDK crate's `Checksum`, in this stack's words, and here rather than
/// there on purpose: what the three answers *mean* for this stack is a rule
/// this module owns, tests where the `dpdk` feature is off, and would otherwise
/// be untestable without an EAL and a port.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    /// The hardware checked them and they held.
    Passed,
    /// It checked them and they did not. A frame no transport sees.
    Failed,
    /// It checked nothing: nothing in this port looks at checksums.
    NotJudged,
}

/// Which checksums a port's hardware judges for it.
///
/// The design's §4.4 asks this of the port because the answer changes what the
/// stack has to do with a frame: a port that judges a checksum hands a verdict
/// over with every frame, and one that does not leaves all of it to
/// [`super::frame::read`], whose arithmetic is the same either way.
///
/// Both are reported and both are read: a port that judges the IPv4 header and
/// not the datagram behind it can call a frame good whose UDP checksum nobody
/// looked at, which is why [`Offloads::checksums`] wants the pair and not
/// either half.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Offloads {
    /// The port checks the IPv4 header checksum.
    pub ipv4_checksum: bool,
    /// The port checks the UDP checksum.
    pub udp_checksum: bool,
}

impl Offloads {
    /// A port that judges nothing, so every frame it hands over is the
    /// stack's to verify. The in-memory wire, and every virtual device this
    /// build's CI runs on (D0).
    pub const NONE: Self = Self {
        ipv4_checksum: false,
        udp_checksum: false,
    };

    /// Whether the port's verdict on a frame accounts for both checksums this
    /// stack cares about.
    pub const fn judges_both(self) -> bool {
        self.ipv4_checksum && self.udp_checksum
    }

    /// What this stack may believe about a frame the port's hardware answered
    /// `verdict` about.
    ///
    /// `Passed` is worth taking only when the port judges both checksums: the
    /// answer that comes back for a frame is one value
    /// (`rte_mbuf`'s `ol_flags`), and a port that judged the IPv4 header alone
    /// says "good" about a frame whose UDP checksum nobody has computed.
    /// Anything less than both falls through to the stack's own arithmetic,
    /// which is the same arithmetic and reaches the same answer.
    ///
    /// `Failed` answers `Software` too, and that is not a second look at the
    /// same question: a frame the hardware rejected is a frame this stack
    /// drops, and dropping it is [`super::frame::read`]'s answer to arithmetic
    /// that does not hold — which is where counter 89 is counted and where the
    /// transport that would have received it never hears of it. Dropping it at
    /// the port instead would lose the count and the reason.
    pub const fn checksums(self, verdict: Verdict) -> Checksums {
        match verdict {
            Verdict::Passed if self.judges_both() => Checksums::Verified,
            Verdict::Passed | Verdict::Failed | Verdict::NotJudged => Checksums::Software,
        }
    }
}

/// A port's receive side.
///
/// `Send` and not `Sync`, both deliberately: a queue pair belongs to exactly
/// one thread — the design's §4.7, one queue per agent thread — and sharing
/// one between threads is the mistake the type is there to prevent. What is
/// shared between threads in this stack is the `Stack`, and what it hands
/// around is a lock over a port, never the port itself.
pub trait RxPort: Send {
    /// Hand over up to `max` frames, `f` deciding each one's fate.
    ///
    /// Answers how many frames were handed over — which is how many times `f`
    /// was called, not how many were kept.
    ///
    /// A frame `f` answers `Keep(binding)` for is moved into `kept` as a
    /// `(binding, frame)` pair, appending. The list is the caller's and is not
    /// cleared here: a burst may be run into a list that already holds a
    /// previous burst's frames.
    fn rx_burst(
        &mut self,
        max: usize,
        f: &mut dyn FnMut(RxFrame<'_>) -> Disposition,
        kept: &mut Vec<(Binding, ReadyFrame)>,
    ) -> usize;

    /// Which checksums this port's hardware judges, so that the reader knows
    /// what a frame's [`Checksums`] is worth ([`Offloads::checksums`]).
    ///
    /// The port decides each frame's answer itself, in the same place it gets
    /// the hardware's verdict; this is the same fact said once, for the
    /// startup that reports what the media is running on and for the tests
    /// that hold the two together.
    fn offloads(&self) -> Offloads;

    /// This port's MAC address, which every frame it sends has to carry as its
    /// source.
    fn mac(&self) -> [u8; 6];

    /// The largest frame this port will send or accept, payload included and
    /// the Ethernet header counted — the number a frame builder has to fit in.
    fn mtu(&self) -> usize;
}

/// A port's transmit side.
pub trait TxPort: Send {
    /// Ask `fill` for up to `count` frames and hand them to the port.
    ///
    /// `fill` is given a buffer that will hold one frame and answers how many
    /// bytes it wrote — the frame's length, headers included — or zero for
    /// "nothing this time". Framing the call this way is what lets a sender
    /// write its headers and its payload straight into the buffer the port
    /// will transmit, instead of building a frame somewhere else and copying
    /// it in.
    ///
    /// Answers how many frames were taken. A frame the port did not take —
    /// past the port's own capacity, or longer than it will carry — is not
    /// counted, and the caller's count of what was taken is the truth about
    /// what left (G7).
    fn tx_burst(
        &mut self,
        count: usize,
        fill: &mut dyn FnMut(&mut [u8]) -> (usize, TxOffloads),
    ) -> usize;

    /// **Which checksums a frame this port sends should carry, and who writes
    /// them.**
    ///
    /// Asked by the transport before it fills a frame, because the answer
    /// changes what goes in the frame's bytes: a port whose NIC computes the
    /// checksums wants the pseudo-header sum in the datagram's field and zero
    /// in the header's, and one whose NIC does not wants both written out
    /// ([`L4Checksum`]).
    ///
    /// A capability of the *started* port rather than of the device, which is
    /// the distinction that matters: DPDK computes nothing for a frame unless
    /// the queue was configured for it, so a port that answers `ByTheNic` is
    /// one that was asked — and one that was not answers `Software` even if the
    /// hardware could have.
    fn checksums(&self) -> L4Checksum;

    /// This port's MAC address, which every frame it sends carries.
    fn mac(&self) -> [u8; 6];

    /// The largest frame this port will send (see [`RxPort::mtu`]).
    fn mtu(&self) -> usize;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A port that judges both checksums answers the same arithmetic the stack
    /// would have done, so taking its word for a frame costs nothing and saves
    /// the work — which is the whole of what an offload buys (§4.5).
    #[test]
    fn a_port_that_judges_both_checksums_has_its_verdict_taken() {
        let judging = Offloads {
            ipv4_checksum: true,
            udp_checksum: true,
        };

        assert!(judging.judges_both());
        assert_eq!(judging.checksums(Verdict::Passed), Checksums::Verified);
    }

    /// The falsification the plan names: a port that judges neither — or one
    /// of the two — must not carry a frame past the stack's own verification
    /// on the strength of a "good" nobody qualified.
    ///
    /// The half-offload case is the one that is easy to get wrong and
    /// invisible when wrong: a port that judges the IPv4 header and not the
    /// datagram behind it answers `Passed` about a frame whose UDP checksum
    /// nobody computed, and a stack that took it would hand a corrupt datagram
    /// to a transport.
    #[test]
    fn a_port_that_judges_one_checksum_vouches_for_nothing() {
        for offloads in [
            Offloads::NONE,
            Offloads {
                ipv4_checksum: true,
                udp_checksum: false,
            },
            Offloads {
                ipv4_checksum: false,
                udp_checksum: true,
            },
        ] {
            assert!(!offloads.judges_both(), "{offloads:?}");
            assert_eq!(
                offloads.checksums(Verdict::Passed),
                Checksums::Software,
                "{offloads:?} does not judge both, so the frame is the stack's"
            );
        }
    }

    /// A frame the hardware rejected is not one the port drops quietly: it is
    /// one the stack's own arithmetic is allowed to reach the same answer
    /// about, which is where counter 89 is counted and where the transport
    /// that would have received it never hears of it.
    #[test]
    fn a_frame_the_hardware_rejected_is_the_stacks_to_check() {
        let judging = Offloads {
            ipv4_checksum: true,
            udp_checksum: true,
        };

        assert_eq!(judging.checksums(Verdict::Failed), Checksums::Software);
        assert_eq!(judging.checksums(Verdict::NotJudged), Checksums::Software);
    }
}
