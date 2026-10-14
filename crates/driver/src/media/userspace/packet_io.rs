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

use super::frame::Checksums;
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
/// One variant today — the bytes, owned. The DPDK build adds a second, a
/// parked mbuf, and the transport's `receive` is the one place both end up as
/// the caller's buffer.
#[derive(Debug)]
pub enum ReadyFrame {
    /// A whole Ethernet frame, owned by the stack from here on.
    Bytes(Box<[u8]>),
}

impl ReadyFrame {
    /// The frame's bytes, whichever arm holds it.
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Bytes(bytes) => bytes,
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
    fn tx_burst(&mut self, count: usize, fill: &mut dyn FnMut(&mut [u8]) -> usize) -> usize;

    /// This port's MAC address, which every frame it sends carries.
    fn mac(&self) -> [u8; 6];

    /// The largest frame this port will send (see [`RxPort::mtu`]).
    fn mtu(&self) -> usize;
}
