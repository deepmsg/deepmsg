//! Which transports have something to read (G4-3).
//!
//! The reference's poller has two branches and the choice between them is one
//! number: at or below `AERON_UDP_TRANSPORT_POLLER_ITERATION_THRESHOLD`
//! transports it calls `recvmmsg` on each in turn — a syscall each, and no
//! bookkeeping — and above it asks `epoll_wait` which descriptors are readable
//! and reads only those
//! (`aeron-driver/src/main/c/media/aeron_udp_transport_poller.c:190-206`,
//! threshold at `media/aeron_udp_transport_poller.h:22`).
//!
//! Both branches are here, because the threshold is what makes the second one
//! worth having: below it the epoll bookkeeping costs more than the syscalls it
//! saves, and above it a driver with a hundred transports should not make a
//! hundred calls to find out that two have data.
//!
//! **Being in an `epoll` set is not free.** A registered socket has a waiter on
//! its sleep queue, so *every* datagram delivered to it runs the wake-up walk —
//! `sock_def_readable` → `__wake_up_common` → `ep_poll_callback`, plus two
//! interrupt-disabling spinlocks — whether or not anything ever asked about
//! readiness. On the loopback that delivery happens in the *sending* thread's
//! softirq, so a driver pays it for its own sends. The reference registers on
//! every add (`:96`), including the ones its own below-threshold branch is
//! about to read one syscall at a time (`:190-206`) and never wait on: at
//! 501,000 messages/s that walk measured 6.09% of its sender thread's kernel
//! instructions, and it goes to zero with the sockets left out of the set. So
//! here the set is empty — and the instance itself absent — at or below the
//! threshold: what is registered is exactly what `epoll_wait` is about to be
//! asked about.
//!
//! The poller is per **transport**, which for a receive endpoint is per
//! destination — an endpoint with three destinations has three sockets, and the
//! reference registers each one (`poller_add_func`, `:120-145`).

use std::io;

use crate::sys::socket::{Descriptor, Poller};

/// `AERON_UDP_TRANSPORT_POLLER_ITERATION_THRESHOLD`
/// (`media/aeron_udp_transport_poller.h:22`): the number of transports at or
/// below which the reference simply reads each one.
pub const ITERATION_THRESHOLD: usize = 5;

/// The transports a driver reads each pass, and how to find out which of them
/// have anything.
///
/// The caller hands it the descriptors in the order it means to visit them and
/// gets back the **indices** to visit — the same order, filtered. Keeping the
/// caller's order matters: which endpoint is read first decides which image
/// sees a datagram first, and that is not something a poller should shuffle.
pub struct TransportPoller {
    /// Made the first time a pass leaves more than [`ITERATION_THRESHOLD`]
    /// transports to watch, and given back — with every registration it held —
    /// when a pass falls back to that many or fewer. A driver with a handful of
    /// channels never makes one, and never has a socket in a set nobody waits
    /// on either. The reference is the other way round: it makes its instance
    /// once, in `_init` (`:45`), and fills it on every add (`:96`) whether or
    /// not the branch it is on will ever wait on it.
    poller: Option<Poller>,
    /// The descriptors the poller was last told about, to avoid a syscall per
    /// pass when nothing has changed. Registration happens when this differs
    /// from what the caller hands in.
    registered: Vec<Descriptor>,
    /// The descriptors the kernel said were readable, reused across passes.
    readable: Vec<Descriptor>,
}

impl TransportPoller {
    /// A poller that has registered nothing yet.
    pub const fn new() -> Self {
        Self {
            poller: None,
            registered: Vec::new(),
            readable: Vec::new(),
        }
    }

    /// The indices of `descriptors` this pass should read, appended to `ready`.
    ///
    /// At or below [`ITERATION_THRESHOLD`] that is every index and no syscall
    /// is made — and the set is left empty, so that a pass which arrives here
    /// after having been above the threshold takes its sockets out with it;
    /// above it, the ones `epoll_wait` reported. A descriptor that is not found
    /// in `descriptors` — a socket closed between the registration and the wait
    /// — is skipped rather than guessed at.
    ///
    /// # Errors
    ///
    /// The error from `epoll_ctl` or `epoll_wait`, if either fails for a reason
    /// other than the two the reference treats as "nothing this pass".
    pub fn ready(&mut self, descriptors: &[Descriptor], ready: &mut Vec<usize>) -> io::Result<()> {
        ready.clear();

        if descriptors.len() <= ITERATION_THRESHOLD {
            self.unregister();
            ready.extend(0..descriptors.len());
            return Ok(());
        }

        self.register(descriptors)?;

        let poller = self
            .poller
            .as_ref()
            .expect("a poller exists once something was registered");

        let mut readable = std::mem::take(&mut self.readable);
        let result = poller.ready(descriptors.len(), &mut readable);

        for descriptor in &readable {
            if let Some(index) = descriptors
                .iter()
                .position(|candidate| candidate == descriptor)
            {
                ready.push(index);
            }
        }

        readable.clear();
        self.readable = readable;

        result.map(|_| ())
    }

    /// Make the poller's registrations match `descriptors`.
    fn register(&mut self, descriptors: &[Descriptor]) -> io::Result<()> {
        if self.registered == descriptors {
            return Ok(());
        }

        let poller = match self.poller.as_mut() {
            Some(poller) => poller,
            None => self.poller.insert(Poller::new()?),
        };

        for descriptor in &self.registered {
            if !descriptors.contains(descriptor) {
                poller.remove(*descriptor)?;
            }
        }

        for descriptor in descriptors {
            if !self.registered.contains(descriptor) {
                poller.add(*descriptor)?;
            }
        }

        self.registered.clear();
        self.registered.extend_from_slice(descriptors);

        Ok(())
    }

    /// Leave the set empty, and give up the instance that held it.
    ///
    /// This is the other half of the branch rule the module note explains: the
    /// set belongs to the branch that waits on it, so the branch that reads
    /// every transport directly is the one that has to give it back. A pass
    /// that falls to at or below the threshold after having been above it has
    /// registrations to drop; one that has always been below it has neither a
    /// registration nor an instance, and this does nothing.
    ///
    /// Dropping the instance is what does the work: `close(2)` takes it out of
    /// every wait queue it installed, so this is one call rather than an
    /// `EPOLL_CTL_DEL` for each transport — and it leaves the driver in the same
    /// state as one that never crossed the threshold at all.
    fn unregister(&mut self) {
        self.poller = None;
        self.registered.clear();
        self.readable.clear();
    }
}

impl Default for TransportPoller {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Sockets bound to a port the kernel picked, in the order given.
    fn sockets(count: usize) -> Vec<(crate::sys::socket::DatagramSocket, Descriptor)> {
        (0..count)
            .map(|_| {
                let socket =
                    crate::sys::socket::DatagramSocket::open(crate::sys::AddressFamily::Inet)
                        .expect("a socket");
                socket
                    .bind("127.0.0.1:0".parse().expect("an address"))
                    .expect("a bind");
                socket.set_nonblocking().expect("non-blocking");
                let descriptor = socket.descriptor();

                (socket, descriptor)
            })
            .collect()
    }

    /// At or below the threshold every transport is read, which is the branch
    /// the reference takes for a driver with a handful of channels — and the
    /// one this build has always been on.
    #[test]
    fn below_the_threshold_every_transport_is_read() {
        let owned = sockets(ITERATION_THRESHOLD);
        let descriptors: Vec<Descriptor> = owned.iter().map(|(_, d)| *d).collect();

        let mut poller = TransportPoller::new();
        let mut ready = Vec::new();
        poller.ready(&descriptors, &mut ready).expect("a poll");

        assert_eq!(vec![0, 1, 2, 3, 4], ready);
        assert!(poller.poller.is_none(), "and no epoll instance was made");
    }

    /// Above it, only the descriptors the kernel says are readable — and in
    /// the caller's order, not the kernel's.
    #[test]
    fn above_the_threshold_only_the_readable_ones_are_read() {
        let owned = sockets(ITERATION_THRESHOLD + 1);
        let descriptors: Vec<Descriptor> = owned.iter().map(|(_, d)| *d).collect();

        // Nothing is readable yet: a pass with no datagrams reads nowhere.
        let mut poller = TransportPoller::new();
        let mut ready = Vec::new();
        poller.ready(&descriptors, &mut ready).expect("a poll");
        assert!(ready.is_empty(), "nothing was sent, so nothing is ready");

        // Send to the last and the third: the poller answers with those, in
        // whatever order the kernel reports them.
        let sender = crate::sys::socket::DatagramSocket::open(crate::sys::AddressFamily::Inet)
            .expect("a socket");
        for index in [5, 2] {
            let address = owned[index].0.local_address().expect("a bound address");
            sender
                .send_batch(Some(address), &[b"here"])
                .expect("a send");
        }

        poller.ready(&descriptors, &mut ready).expect("a poll");
        ready.sort_unstable();
        assert_eq!(vec![2, 5], ready);
    }

    /// A transport that goes away is deregistered, and one that appears is
    /// registered: the list is the driver's, and it changes as channels come
    /// and go.
    #[test]
    fn the_registration_follows_the_transports() {
        let owned = sockets(ITERATION_THRESHOLD + 2);
        let descriptors: Vec<Descriptor> = owned.iter().map(|(_, d)| *d).collect();
        let mut poller = TransportPoller::new();
        let mut ready = Vec::new();

        poller.ready(&descriptors, &mut ready).expect("a poll");
        assert_eq!(descriptors.len(), poller.registered.len());

        poller.ready(&descriptors[1..], &mut ready).expect("a poll");
        assert_eq!(&descriptors[1..], poller.registered.as_slice());
    }

    /// A driver that shrinks back to the threshold keeps nothing registered —
    /// and a driver that grows past it again registers from nothing. Below the
    /// threshold a registration is not idle bookkeeping: the socket pays the
    /// wake-up walk for every datagram delivered to it (see the module note).
    #[test]
    fn falling_back_to_the_threshold_empties_the_set() {
        let many = sockets(ITERATION_THRESHOLD + 1);
        let descriptors: Vec<Descriptor> = many.iter().map(|(_, d)| *d).collect();
        let mut poller = TransportPoller::new();
        let mut ready = Vec::new();

        poller.ready(&descriptors, &mut ready).expect("a poll");
        assert!(
            poller.poller.is_some(),
            "above it, epoll is what it waits on"
        );
        assert_eq!(descriptors.len(), poller.registered.len());

        // One transport goes away, and the rest are read directly again.
        poller
            .ready(&descriptors[..ITERATION_THRESHOLD], &mut ready)
            .expect("a poll");
        assert_eq!(vec![0, 1, 2, 3, 4], ready);
        assert!(
            poller.poller.is_none(),
            "the instance it waited through went with it"
        );
        assert!(poller.registered.is_empty());

        // And crossing back up registers the whole list, from an empty set.
        let more = sockets(ITERATION_THRESHOLD + 1);
        let descriptors: Vec<Descriptor> = more.iter().map(|(_, d)| *d).collect();
        poller.ready(&descriptors, &mut ready).expect("a poll");
        assert!(poller.poller.is_some());
        assert_eq!(descriptors.len(), poller.registered.len());
    }
}
