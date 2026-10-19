//! The client operations a connect and an egress poll are made of.
//!
//! # Why this exists
//!
//! The reference's connect tests mock the `Aeron` object itself and assert
//! *which* operations it was asked for, how many times, in what order —
//! `AeronClusterAsyncConnectTest` is eight of those. A Rust struct is not
//! mockable, and the alternative a real driver offers is weaker in a way that
//! matters: "the subscription was removed with `asyncRemoveSubscription` rather
//! than closed" and "the second member's publication add was retried nine
//! times" are both invisible from the outside, and both are the tests.
//!
//! So the operations live behind this trait, [`Client`] is one implementation,
//! and the tests are another that records what it was asked. Nothing here
//! branches: each implementation method is one call into `Client`, so the
//! production path is the same code with one extra indirection.
//!
//! The trait is crate-private on purpose. The public surface says `&mut Client`
//! (see `crate::async_connect` and `crate::egress_poller`), because a caller
//! with a driver has a `Client` and this seam is a test's business.

use std::time::Duration;

use deepmsg_client::client::{AsyncAdd, AsyncAddPoll, Client, CommandError};
use deepmsg_client::fragment_assembler::{Action, ControlledHandler, Message};
use deepmsg_core::logbuffer::append::Appended;

/// Which of the client's two publication lists a publication is in.
///
/// The reference has one `Publication` type with two implementations; here the
/// lists are separate and every lookup has to say which it means
/// (`Client::publication` vs `Client::exclusive_publication`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PublicationKind {
    /// `Client::publication` — an `ADD_PUBLICATION`.
    Concurrent,
    /// `Client::exclusive_publication` — an `ADD_EXCLUSIVE_PUBLICATION`.
    Exclusive,
}

/// Everything [`crate::AsyncConnect`] and [`crate::EgressPoller`] ask of a
/// client.
pub(crate) trait ClientOps {
    /// A fresh correlation id for the next request.
    fn next_correlation_id(&mut self) -> Result<i64, CommandError>;

    /// `Aeron.asyncAddSubscription` (`AC:2357`).
    fn async_add_subscription(
        &mut self,
        channel: &str,
        stream_id: i32,
        timeout: Duration,
    ) -> Result<AsyncAdd, CommandError>;

    /// `Aeron.asyncAddPublication` (`AC:2391`, `AC:2625`).
    fn async_add_publication(
        &mut self,
        channel: &str,
        stream_id: i32,
        timeout: Duration,
    ) -> Result<AsyncAdd, CommandError>;

    /// `Aeron.asyncAddExclusivePublication` (`AC:795-803`).
    fn async_add_exclusive_publication(
        &mut self,
        channel: &str,
        stream_id: i32,
        timeout: Duration,
    ) -> Result<AsyncAdd, CommandError>;

    /// What became of an add (`Aeron.getSubscription`/`getPublication`'s
    /// pending answer, `AC:2362`, `:2396`).
    fn async_add_poll(&mut self, add: AsyncAdd) -> AsyncAddPoll;

    /// Give a subscription back (`Subscription.close`).
    fn async_remove_subscription(
        &mut self,
        registration_id: i64,
        timeout: Duration,
    ) -> Result<(), CommandError>;

    /// Give a publication back (`Publication.close`).
    fn async_remove_publication(
        &mut self,
        registration_id: i64,
        timeout: Duration,
    ) -> Result<(), CommandError>;

    /// Whether this client still holds the subscription.
    fn subscription_present(&self, registration_id: i64) -> bool;

    /// The channel the subscription was added on (`Subscription.channel()`).
    fn subscription_channel(&self, registration_id: i64) -> Option<String>;

    /// The resolved endpoint of a wildcard-port channel, or `None` when the
    /// driver has not assigned one yet (`tryResolveChannelEndpointPort`).
    fn subscription_resolved_endpoint(&self, registration_id: i64) -> Option<String>;

    /// Whether this client holds the publication.
    fn publication_present(&self, kind: PublicationKind, registration_id: i64) -> bool;

    /// Whether the publication has met a subscriber (`isConnected`).
    fn publication_is_connected(&self, kind: PublicationKind, registration_id: i64) -> bool;

    /// Offer one message (`Publication.offer`).
    fn offer(
        &self,
        kind: PublicationKind,
        registration_id: i64,
        payload: &[u8],
    ) -> Option<Appended>;

    /// Offer a message made of two parts — the cluster session header and the
    /// caller's payload — as one message (`Publication.offer(bufferOne, …,
    /// bufferTwo, …)`).
    fn offer_two_slices(
        &self,
        kind: PublicationKind,
        registration_id: i64,
        first: &[u8],
        second: &[u8],
    ) -> Option<Appended>;

    /// Drive the client one turn: read what the driver has said and answer
    /// nothing. The session's own loops call it where the reference calls
    /// `runAgentInvokers` — adding a publication blocks until the driver makes
    /// it, and only a turn of the client lets the driver say so.
    fn poll(&mut self);

    /// Whether the image a session is connected through has closed.
    ///
    /// An image the subscription no longer holds is counted as closed: the
    /// connection it stood for is gone either way, and the session's reaction
    /// to both is the same.
    fn image_is_closed(&self, subscription_id: i64, image_registration_id: i64) -> bool;

    /// Read whole messages from the subscription under a controlled handler
    /// (`Subscription.controlledPoll`).
    fn poll_egress(
        &mut self,
        subscription_id: i64,
        fragment_limit: usize,
        handler: &mut dyn for<'a> FnMut(Message<'a>) -> Action,
    ) -> usize;
}

impl ClientOps for Client {
    fn next_correlation_id(&mut self) -> Result<i64, CommandError> {
        Client::next_correlation_id(self)
    }

    fn async_add_subscription(
        &mut self,
        channel: &str,
        stream_id: i32,
        timeout: Duration,
    ) -> Result<AsyncAdd, CommandError> {
        Client::async_add_subscription(self, channel, stream_id, timeout)
    }

    fn async_add_publication(
        &mut self,
        channel: &str,
        stream_id: i32,
        timeout: Duration,
    ) -> Result<AsyncAdd, CommandError> {
        Client::async_add_publication(self, channel, stream_id, timeout)
    }

    fn async_add_exclusive_publication(
        &mut self,
        channel: &str,
        stream_id: i32,
        timeout: Duration,
    ) -> Result<AsyncAdd, CommandError> {
        Client::async_add_exclusive_publication(self, channel, stream_id, timeout)
    }

    fn async_add_poll(&mut self, add: AsyncAdd) -> AsyncAddPoll {
        Client::async_add_poll(self, add)
    }

    fn async_remove_subscription(
        &mut self,
        registration_id: i64,
        timeout: Duration,
    ) -> Result<(), CommandError> {
        Client::async_remove_subscription(self, registration_id, timeout).map(|_| ())
    }

    fn async_remove_publication(
        &mut self,
        registration_id: i64,
        timeout: Duration,
    ) -> Result<(), CommandError> {
        Client::async_remove_publication(self, registration_id, timeout).map(|_| ())
    }

    fn subscription_present(&self, registration_id: i64) -> bool {
        Client::subscription(self, registration_id).is_some()
    }

    fn subscription_channel(&self, registration_id: i64) -> Option<String> {
        Client::subscription(self, registration_id)
            .map(|subscription| subscription.channel().to_owned())
    }

    fn subscription_resolved_endpoint(&self, registration_id: i64) -> Option<String> {
        let subscription = Client::subscription(self, registration_id)?;
        subscription.resolved_endpoint(&Client::counters_reader(self)?)
    }

    fn publication_present(&self, kind: PublicationKind, registration_id: i64) -> bool {
        match kind {
            PublicationKind::Concurrent => Client::publication(self, registration_id).is_some(),
            PublicationKind::Exclusive => {
                Client::exclusive_publication(self, registration_id).is_some()
            }
        }
    }

    fn publication_is_connected(&self, kind: PublicationKind, registration_id: i64) -> bool {
        match kind {
            PublicationKind::Concurrent => Client::publication(self, registration_id)
                .and_then(deepmsg_client::publication::Publication::is_connected)
                .unwrap_or(false),
            PublicationKind::Exclusive => Client::exclusive_publication(self, registration_id)
                .and_then(deepmsg_client::publication::ExclusivePublication::is_connected)
                .unwrap_or(false),
        }
    }

    fn offer(
        &self,
        kind: PublicationKind,
        registration_id: i64,
        payload: &[u8],
    ) -> Option<Appended> {
        match kind {
            PublicationKind::Concurrent => Client::offer(self, registration_id, payload),
            PublicationKind::Exclusive => Client::offer_exclusive(self, registration_id, payload),
        }
    }

    fn offer_two_slices(
        &self,
        kind: PublicationKind,
        registration_id: i64,
        first: &[u8],
        second: &[u8],
    ) -> Option<Appended> {
        match kind {
            PublicationKind::Concurrent => {
                Client::offer_two_slices(self, registration_id, first, second)
            }
            PublicationKind::Exclusive => {
                Client::offer_two_slices_exclusive(self, registration_id, first, second)
            }
        }
    }

    fn poll(&mut self) {
        let _ = Client::poll(self);
    }

    fn image_is_closed(&self, subscription_id: i64, image_registration_id: i64) -> bool {
        Client::subscription(self, subscription_id)
            .and_then(|subscription| subscription.image(image_registration_id))
            .is_none_or(deepmsg_client::image::Image::is_closed)
    }

    fn poll_egress(
        &mut self,
        subscription_id: i64,
        fragment_limit: usize,
        handler: &mut dyn for<'a> FnMut(Message<'a>) -> Action,
    ) -> usize {
        // The scan wants a `ControlledHandler`, and a `&mut dyn FnMut` is not
        // one — the blanket implementation is for a sized `FnMut`. The relay is
        // how a trait object is one without a second blanket implementation.
        let mut relay = Relay { handler };
        Client::poll_subscription_controlled(self, subscription_id, fragment_limit, &mut relay)
    }
}

/// A [`ControlledHandler`] over a `&mut dyn FnMut`.
struct Relay<'h> {
    handler: &'h mut dyn for<'a> FnMut(Message<'a>) -> Action,
}

impl ControlledHandler for Relay<'_> {
    fn on_message(&mut self, message: Message<'_>) -> Action {
        (self.handler)(message)
    }
}
