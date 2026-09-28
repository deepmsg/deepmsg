//! A transport that drops datagrams on purpose.
//!
//! The reference ships the same idea twice over: a *fixed* generator that drops
//! the frames inside a term and offset range
//! (`media/aeron_fixed_loss_generator.c:33-95`) and a randomised one, both
//! wired in as data paths on a transport a test constructs directly.
//!
//! It exists because loss recovery cannot be tested without loss. Loopback does
//! not drop, so a test that waits for a real network to lose a packet waits
//! forever; injecting the loss makes the gap, the NAK, the retransmission and
//! the de-duplication deterministic.
//!
//! # This is a test seam, not a feature
//!
//! Nothing in the driver constructs one. It is `pub` because the tests that use
//! it live in other crates, and it is deliberately as dumb as it can be: the
//! `n`-th datagram of every `n` is dropped and the rest are sent unchanged. The
//! shape a *deployment* would need — a rate, a seed, a packet filter — is not
//! this.

use std::io;
use std::net::SocketAddr;

use super::{Datagrams, Transport};

/// Wraps another transport and drops every `drop_every`-th datagram it is
/// asked to send.
///
/// Receiving is untouched: the direction that matters for a retransmission test
/// is the publisher's, where a dropped datagram is a gap the subscriber has to
/// notice.
pub struct LossTransport {
    inner: Box<dyn Transport>,
    drop_every: u64,
    sent: u64,
    dropped: u64,
}

impl LossTransport {
    /// Wrap `inner`, dropping one outgoing datagram in every `drop_every`.
    ///
    /// A `drop_every` of zero or one stops nothing — the first datagram would
    /// be the dropped one and nothing would ever arrive — so it is read as
    /// "drop nothing" rather than as a trap.
    pub fn new(inner: Box<dyn Transport>, drop_every: u64) -> Self {
        Self {
            inner,
            drop_every: if drop_every <= 1 { 0 } else { drop_every },
            sent: 0,
            dropped: 0,
        }
    }

    /// How many datagrams this transport refused to send.
    pub const fn dropped(&self) -> u64 {
        self.dropped
    }

    /// How many it was asked to send, dropped ones included.
    pub const fn attempted(&self) -> u64 {
        self.sent
    }
}

impl std::fmt::Debug for LossTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LossTransport")
            .field("drop_every", &self.drop_every)
            .field("sent", &self.sent)
            .field("dropped", &self.dropped)
            .finish_non_exhaustive()
    }
}

impl Transport for LossTransport {
    fn send(&mut self, address: Option<SocketAddr>, buffers: &[&[u8]]) -> io::Result<usize> {
        if self.drop_every == 0 {
            return self.inner.send(address, buffers);
        }

        // Keep the datagrams that are not due to be dropped, in order, and
        // count the ones that are as sent-but-lost: a sender advances its
        // position by what it *handed over*, and the point of a loss injector
        // is that the far end never sees it.
        let mut kept = Vec::with_capacity(buffers.len());
        let mut dropped_here = 0;

        for buffer in buffers {
            self.sent += 1;

            if self.sent % self.drop_every == 0 {
                self.dropped += 1;
                dropped_here += 1;
            } else {
                kept.push(*buffer);
            }
        }

        let sent = self.inner.send(address, &kept)?;

        #[allow(clippy::cast_possible_truncation)] // a batch is at most sixteen
        Ok(sent + dropped_here as usize)
    }

    fn receive(&mut self, buffers: &mut [Vec<u8>], datagrams: &mut Datagrams) -> io::Result<usize> {
        self.inner.receive(buffers, datagrams)
    }

    fn local_address(&self) -> io::Result<SocketAddr> {
        self.inner.local_address()
    }

    fn receive_buffer_size(&self) -> io::Result<usize> {
        self.inner.receive_buffer_size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::media::TransportParams;
    use crate::media::UdpTransport;
    use std::net::{IpAddr, Ipv4Addr};

    fn loopback(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
    }

    /// A transport whose datagrams go nowhere, for counting what was sent.
    #[derive(Debug, Default)]
    struct Recorder {
        batches: Vec<Vec<Vec<u8>>>,
    }

    impl Transport for Recorder {
        fn send(&mut self, _: Option<SocketAddr>, buffers: &[&[u8]]) -> io::Result<usize> {
            self.batches
                .push(buffers.iter().map(|buffer| buffer.to_vec()).collect());
            Ok(buffers.len())
        }

        fn receive(&mut self, _: &mut [Vec<u8>], _: &mut Datagrams) -> io::Result<usize> {
            Ok(0)
        }

        fn local_address(&self) -> io::Result<SocketAddr> {
            Ok(loopback(0))
        }

        fn receive_buffer_size(&self) -> io::Result<usize> {
            Ok(0)
        }
    }

    #[test]
    fn every_nth_datagram_is_dropped_and_the_rest_go_through_in_order() {
        let mut transport = LossTransport::new(Box::new(Recorder::default()), 3);

        // Nine datagrams, one per call: the third, sixth and ninth are dropped.
        for index in 0..9 {
            let payload = [u8::try_from(index).expect("small")];
            let sent = transport.send(None, &[&payload]).expect("a send");
            assert_eq!(1, sent, "a dropped datagram still counts as handed over");
        }

        assert_eq!(3, transport.dropped());
        assert_eq!(9, transport.attempted());
    }

    #[test]
    fn a_batch_loses_only_its_share() {
        let mut transport = LossTransport::new(Box::new(Recorder::default()), 4);

        // Four datagrams in one call: every fourth is dropped, which is the
        // last of the batch.
        let buffers: [&[u8]; 4] = [b"a", b"b", b"c", b"d"];
        let sent = transport.send(None, &buffers).expect("a send");

        assert_eq!(4, sent);
        assert_eq!(1, transport.dropped());
    }

    #[test]
    fn a_transport_that_drops_nothing_is_the_transport_underneath() {
        let mut transport = LossTransport::new(Box::new(Recorder::default()), 0);
        let sent = transport.send(None, &[b"a", b"b"]).expect("a send");

        assert_eq!(2, sent);
        assert_eq!(0, transport.dropped());

        let mut receiver =
            UdpTransport::open(loopback(0), None, &TransportParams::default()).expect("a socket");
        let bound = receiver.local_address().expect("an address");

        let inner = UdpTransport::open(loopback(0), Some(bound), &TransportParams::default())
            .expect("a socket");
        let mut transport = LossTransport::new(Box::new(inner), 1);

        assert_eq!(1, transport.send(None, &[b"whole"]).expect("a send"));

        let mut buffers = vec![vec![0u8; 1408]];
        let mut datagrams = Datagrams::new();
        assert_eq!(
            1,
            receiver
                .receive(&mut buffers, &mut datagrams)
                .expect("a receive"),
            "a drop rate of one is read as no loss at all"
        );
    }
}
