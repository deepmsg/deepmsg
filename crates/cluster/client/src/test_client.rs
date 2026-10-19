//! The client a connect and a session are tested against.
//!
//! The reference's connect and session tests mock `Aeron`, its publications,
//! subscriptions and images, and assert what those objects were asked. A Rust
//! struct cannot be mocked, so the operations live behind
//! [`crate::client_ops::ClientOps`] and this is the other implementation: a
//! client that answers from a script and records every call.
//!
//! It is `#[cfg(test)]` and crate-private — the seam's production side is
//! `impl ClientOps for Client`.

use std::collections::{BTreeMap, VecDeque};
use std::time::Duration;

use deepmsg_client::client::{AsyncAdd, AsyncAddPoll, CommandError};
use deepmsg_client::fragment_assembler::{Action, Message};
use deepmsg_core::logbuffer::append::Appended;
use deepmsg_core::logbuffer::position::Position;

use crate::client_ops::{ClientOps, PublicationKind};

/// What an add answers the next time it is polled.
#[derive(Debug)]
pub(crate) enum AddAnswer {
    Pending,
    Ready,
    /// The add answers "no such handle": the reference's `NULL_VALUE`
    /// registration id, which makes the connect try again next turn.
    Unknown,
    Failed(CommandError),
}

struct Subscription {
    channel: String,
    resolved: Option<String>,
    ready: bool,
}

struct Publication {
    connected: bool,
    ready: bool,
}

/// A client that answers from a script and records what it was asked.
///
/// The reference's eight connect tests mock `Aeron` with Mockito and
/// assert the calls it saw; this is the same object said as a value.
/// Every method appends one line to `calls` — the order the connect asked
/// in is the order the lines appear.
#[derive(Default)]
pub(crate) struct RecordingClient {
    calls: Vec<String>,
    next_id: i64,
    next_correlation: i64,
    pending: BTreeMap<i64, AddAnswer>,
    subscriptions: BTreeMap<i64, Subscription>,
    publications: BTreeMap<i64, Publication>,
    /// Whether an egress image is open. Missing means closed, as it does for a
    /// real client whose subscription no longer holds the image.
    images: BTreeMap<i64, bool>,
    egress: VecDeque<Vec<u8>>,
    offered: std::cell::RefCell<Vec<Vec<u8>>>,
    /// Every new add answers `Ready` on its first poll, as a driver whose
    /// publication is already made would.
    adds_ready: bool,
    /// The next N polls of a pending add answer
    /// `RESOURCE_TEMPORARILY_UNAVAILABLE`.
    fail_add_polls: usize,
    /// What offers answer instead of the publication's own state.
    offer_override: Option<Appended>,
}

impl RecordingClient {
    pub(crate) fn new() -> Self {
        Self {
            next_correlation: 100,
            next_id: 1,
            ..Self::default()
        }
    }

    pub(crate) fn last_registration_id(&self) -> i64 {
        self.next_id - 1
    }

    pub(crate) fn answer(&mut self, id: i64, answer: AddAnswer) {
        self.pending.insert(id, answer);
    }

    pub(crate) fn connect(&mut self, id: i64) {
        if let Some(publication) = self.publications.get_mut(&id) {
            publication.ready = true;
            publication.connected = true;
        }
    }

    pub(crate) fn land(&mut self, id: i64) {
        // The answer the next poll gives: `Ready`. The resource's own
        // `ready` flag is set when that answer is read, as the driver's
        // answer is what makes the resource usable.
        self.pending.insert(id, AddAnswer::Ready);
    }

    pub(crate) fn push_egress(&mut self, bytes: Vec<u8>) {
        self.egress.push_back(bytes);
    }

    /// A publication that exists, is ready and is connected.
    pub(crate) fn add_publication(&mut self) -> i64 {
        let id = self.next_id;
        self.next_id += 1;
        self.publications.insert(
            id,
            Publication {
                connected: true,
                ready: true,
            },
        );
        id
    }

    /// The publication lost its subscriber.
    pub(crate) fn disconnect(&mut self, registration_id: i64) {
        if let Some(publication) = self.publications.get_mut(&registration_id) {
            publication.connected = false;
        }
    }

    /// Every new add answers `Ready` on its first poll.
    pub(crate) fn adds_ready(&mut self) {
        self.adds_ready = true;
    }

    /// The next `count` polls of a pending add are refused as "try again".
    pub(crate) fn fail_next_add_polls(&mut self, count: usize) {
        self.fail_add_polls = count;
    }

    /// What offers answer, whatever the publication's state.
    pub(crate) fn offer_returns(&mut self, result: Appended) {
        self.offer_override = Some(result);
    }

    /// The egress image is open, as a connected subscription's is.
    pub(crate) fn open_image(&mut self, registration_id: i64) {
        self.images.insert(registration_id, true);
    }

    /// The egress image has closed.
    pub(crate) fn close_image(&mut self, registration_id: i64) {
        self.images.insert(registration_id, false);
    }

    pub(crate) fn calls(&self) -> &[String] {
        &self.calls
    }

    pub(crate) fn offered(&self) -> Vec<Vec<u8>> {
        self.offered.borrow().clone()
    }
}

impl ClientOps for RecordingClient {
    fn next_correlation_id(&mut self) -> Result<i64, CommandError> {
        self.calls.push("next_correlation_id".to_owned());
        self.next_correlation += 1;
        Ok(self.next_correlation - 1)
    }

    fn async_add_subscription(
        &mut self,
        channel: &str,
        stream_id: i32,
        _timeout: Duration,
    ) -> Result<AsyncAdd, CommandError> {
        let id = self.next_id;
        self.next_id += 1;
        self.calls
            .push(format!("async_add_subscription({channel}, {stream_id})"));
        self.subscriptions.insert(
            id,
            Subscription {
                channel: channel.to_owned(),
                resolved: None,
                ready: false,
            },
        );
        self.pending.insert(id, AddAnswer::Pending);
        Ok(AsyncAdd::subscription(id))
    }

    fn async_add_publication(
        &mut self,
        channel: &str,
        stream_id: i32,
        _timeout: Duration,
    ) -> Result<AsyncAdd, CommandError> {
        let id = self.next_id;
        self.next_id += 1;
        self.calls
            .push(format!("async_add_publication({channel}, {stream_id})"));
        self.publications.insert(
            id,
            Publication {
                connected: false,
                ready: false,
            },
        );
        self.pending.insert(id, AddAnswer::Pending);
        Ok(AsyncAdd::publication(id))
    }

    fn async_add_exclusive_publication(
        &mut self,
        channel: &str,
        stream_id: i32,
        _timeout: Duration,
    ) -> Result<AsyncAdd, CommandError> {
        let id = self.next_id;
        self.next_id += 1;
        self.calls.push(format!(
            "async_add_exclusive_publication({channel}, {stream_id})"
        ));
        self.publications.insert(
            id,
            Publication {
                connected: false,
                ready: false,
            },
        );
        self.pending.insert(id, AddAnswer::Pending);
        Ok(AsyncAdd::publication(id))
    }

    fn async_add_poll(&mut self, add: AsyncAdd) -> AsyncAddPoll {
        let id = add.registration_id();
        self.calls.push(format!("async_add_poll({id})"));

        match self.pending.remove(&id) {
            Some(AddAnswer::Pending) => {
                if self.fail_add_polls > 0 {
                    self.fail_add_polls -= 1;
                    return AsyncAddPoll::Failed(CommandError::Driver {
                        code: 10,
                        message: "send_channel_endpoint found in CLOSING state, please retry"
                            .to_owned(),
                    });
                }

                if self.adds_ready {
                    if let Some(publication) = self.publications.get_mut(&id) {
                        publication.ready = true;
                    }
                    if let Some(subscription) = self.subscriptions.get_mut(&id) {
                        subscription.ready = true;
                    }
                    return AsyncAddPoll::Ready;
                }

                // Still pending: put it back, so every turn of the connect
                // sees the same answer until the test changes it.
                self.pending.insert(id, AddAnswer::Pending);
                AsyncAddPoll::Awaiting
            }
            Some(AddAnswer::Ready) => {
                if let Some(publication) = self.publications.get_mut(&id) {
                    publication.ready = true;
                }
                if let Some(subscription) = self.subscriptions.get_mut(&id) {
                    subscription.ready = true;
                }
                AsyncAddPoll::Ready
            }
            Some(AddAnswer::Unknown) | None => AsyncAddPoll::Unknown,
            Some(AddAnswer::Failed(error)) => AsyncAddPoll::Failed(error),
        }
    }

    fn async_remove_subscription(
        &mut self,
        registration_id: i64,
        _timeout: Duration,
    ) -> Result<(), CommandError> {
        self.calls
            .push(format!("async_remove_subscription({registration_id})"));
        self.subscriptions.remove(&registration_id);
        self.pending.remove(&registration_id);
        Ok(())
    }

    fn async_remove_publication(
        &mut self,
        registration_id: i64,
        _timeout: Duration,
    ) -> Result<(), CommandError> {
        self.calls
            .push(format!("async_remove_publication({registration_id})"));
        self.publications.remove(&registration_id);
        self.pending.remove(&registration_id);
        Ok(())
    }

    fn subscription_present(&self, registration_id: i64) -> bool {
        self.subscriptions.contains_key(&registration_id)
    }

    fn subscription_channel(&self, registration_id: i64) -> Option<String> {
        self.subscriptions
            .get(&registration_id)
            .map(|subscription| subscription.channel.clone())
    }

    fn subscription_resolved_endpoint(&self, registration_id: i64) -> Option<String> {
        self.subscriptions
            .get(&registration_id)
            .and_then(|subscription| subscription.resolved.clone())
    }

    fn publication_present(&self, _kind: PublicationKind, registration_id: i64) -> bool {
        self.publications
            .get(&registration_id)
            .is_some_and(|publication| publication.ready)
    }

    fn publication_is_connected(&self, _kind: PublicationKind, registration_id: i64) -> bool {
        self.publications
            .get(&registration_id)
            .is_some_and(|publication| publication.connected)
    }

    fn offer(
        &self,
        _kind: PublicationKind,
        registration_id: i64,
        payload: &[u8],
    ) -> Option<Appended> {
        self.offered.borrow_mut().push(payload.to_vec());

        if let Some(result) = self.offer_override {
            return Some(result);
        }

        let publication = self.publications.get(&registration_id)?;
        if !publication.connected {
            return Some(Appended::NotConnected);
        }

        Some(Appended::Ok {
            position: Position::from_raw(i64::from(payload.len() as u32)),
            term_offset: 0,
        })
    }

    fn offer_two_slices(
        &self,
        _kind: PublicationKind,
        registration_id: i64,
        first: &[u8],
        second: &[u8],
    ) -> Option<Appended> {
        let mut payload = Vec::with_capacity(first.len() + second.len());
        payload.extend_from_slice(first);
        payload.extend_from_slice(second);
        self.offered.borrow_mut().push(payload);

        if let Some(result) = self.offer_override {
            return Some(result);
        }

        let publication = self.publications.get(&registration_id)?;
        if !publication.connected {
            return Some(Appended::NotConnected);
        }

        Some(Appended::Ok {
            position: Position::from_raw(i64::from((first.len() + second.len()) as u32)),
            term_offset: 0,
        })
    }

    fn poll(&mut self) {
        self.calls.push("poll".to_owned());
    }

    fn image_is_closed(&self, _subscription_id: i64, image_registration_id: i64) -> bool {
        !self
            .images
            .get(&image_registration_id)
            .copied()
            .unwrap_or(false)
    }

    fn poll_egress(
        &mut self,
        _subscription_id: i64,
        _fragment_limit: usize,
        handler: &mut dyn for<'a> FnMut(Message<'a>) -> Action,
    ) -> usize {
        self.calls.push("poll_egress".to_owned());

        let Some(bytes) = self.egress.pop_front() else {
            return 0;
        };

        let message = Message {
            header: deepmsg_client::fragment_assembler::MessageHeader {
                session_id: 0,
                stream_id: 102,
                term_offset: 0,
                flags: 0,
                position: 0,
                frame_length: 0,
                fragmented_frame_length: 0,
                image_registration_id: 11,
            },
            payload: &bytes,
        };

        let _ = handler(message);
        1
    }
}
