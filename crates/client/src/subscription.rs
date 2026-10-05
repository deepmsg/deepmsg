//! A subscription, and the images currently attached to it.
//!
//! A subscription is a standing request: "give me stream *N* on channel *C*".
//! It owns no data. What it owns is a set of **images**, one per matching
//! publication, and each image is a mapping of that publication's log buffer.
//! The reference's wording is worth keeping: the subscription is what the
//! client asked for, the image is what it got
//! (`aeron-client/src/main/c/aeron_subscription.h:24-40`).
//!
//! Mirrors `aeron-client/src/main/c/aeron_client_conductor.c:1040-1080`.

use deepmsg_cnc::command::CHANNEL_STATUS_INDICATOR_NOT_ALLOCATED;

use crate::fragment_assembler::{Action, ControlledHandler, FragmentAssembler};
use crate::image::{ControlledFragments, Fragment, Image};

/// A subscription and the images attached to it.
/// One object a controlled scan can call: the assembler, and the handler it
/// answers to.
///
/// The scan reads **fragments**; this build's controlled trait speaks in
/// **messages**. The reassembly therefore sits between them, and the scan needs
/// a single thing to call — which is all this is. It is private on purpose: a
/// caller supplies a message handler and never meets the seam, and the
/// alternative (handing the scan an assembler and a handler side by side) would
/// put that seam in the scan's signature instead.
struct AssemblingSink<'a, H> {
    assembler: &'a mut FragmentAssembler,
    handler: &'a mut H,
}

impl<H: ControlledHandler> ControlledFragments for AssemblingSink<'_, H> {
    fn on_fragment(&mut self, fragment: &Fragment<'_>) -> Action {
        self.assembler.push_controlled(fragment, self.handler)
    }
}

pub struct Subscription {
    /// The id the driver keys this subscription by — the correlation id the
    /// `ADD_SUBSCRIPTION` used.
    registration_id: i64,
    /// The channel-status counter, or
    /// [`CHANNEL_STATUS_INDICATOR_NOT_ALLOCATED`].
    channel_status_indicator_id: i32,
    /// The channel this subscription asked for, as it was sent.
    channel: String,
    stream_id: i32,
    images: Vec<Image>,
    /// Which image the next poll starts from.
    ///
    /// The reference's `roundRobinIndex` (`Subscription.java`, in both `poll`
    /// and `controlledPoll`): a subscription whose fragment budget runs out
    /// before it reaches its last image would otherwise read the first one
    /// forever — and a subscription with a replay image and a live image is
    /// exactly where that shows, which is the arrangement the archive's
    /// persistent subscriptions use.
    round_robin: usize,
    /// What the fragments of this subscription's images are reassembled into
    /// when it is polled for whole messages.
    ///
    /// One per subscription, which is the arrangement the reference's own
    /// samples use (`aeron-samples/src/main/c/basic_subscriber.c` creates one
    /// assembler per subscription): a message is reassembled per **session**,
    /// and a second subscription reading a different stream has no business
    /// sharing the buffer its messages are copied into.
    pub(crate) assembler: FragmentAssembler,
}

impl Subscription {
    /// A subscription with no images yet.
    pub fn new(
        registration_id: i64,
        channel: String,
        stream_id: i32,
        channel_status_indicator_id: i32,
    ) -> Self {
        Self {
            registration_id,
            channel_status_indicator_id,
            channel,
            stream_id,
            images: Vec::new(),
            round_robin: 0,
            assembler: FragmentAssembler::new(),
        }
    }

    /// Record the channel-status counter the driver allocated, which arrives
    /// with the ready response — **after** this subscription is already
    /// registered, so the id is written onto it rather than carried back to the
    /// caller. Java does the same (`ClientConductor.java:396`,
    /// `subscription.channelStatusId(id)`), which is why its subscriptions are
    /// registered before the response is awaited.
    pub(crate) fn set_channel_status_indicator_id(&mut self, id: i32) {
        self.channel_status_indicator_id = id;
    }

    /// The assembler this subscription's messages are reassembled in, and how
    /// many messages it abandoned on the way.
    pub const fn assembler(&self) -> &FragmentAssembler {
        &self.assembler
    }

    /// Poll every image, delivering whole messages, and hand back the counter
    /// positions the caller has to publish.
    ///
    /// The positions are returned rather than written because the counters live
    /// in the CnC file and this type does not own it: writing them here would
    /// need the file borrowed while the subscription is borrowed mutably, which
    /// is exactly the aliasing the borrow checker exists to refuse. The caller
    /// — [`crate::Client::poll_subscription`] — writes them after the poll.
    /// Where the next poll begins, and how many images there are to cover.
    ///
    /// One image later each call, wrapping. The reference advances a counter and
    /// resets it to one when it runs past the end (`Subscription.java`); taking
    /// the remainder does the same thing without the special case, because the
    /// only thing anyone does with it is index.
    fn rotation(&self) -> (usize, usize) {
        let length = self.images.len();
        if 0 == length {
            return (0, 0);
        }
        (self.round_robin % length, length)
    }

    /// The next poll's index, after this one has taken it.
    fn take_rotation(&mut self) -> (usize, usize) {
        let (start, length) = self.rotation();
        self.round_robin = if 0 == length { 0 } else { (start + 1) % length };
        (start, length)
    }

    pub(crate) fn poll_messages<F>(
        &mut self,
        fragment_limit: usize,
        handler: &mut F,
    ) -> (usize, Vec<(i32, i64)>)
    where
        F: FnMut(crate::fragment_assembler::Message<'_>),
    {
        let mut messages = 0;
        let mut fragments = 0;
        let mut counter_writes = Vec::new();

        // The images and the assembler are borrowed apart here because both are
        // needed at once: the images are what is read, the assembler is where
        // their fragments go.
        let (start, length) = self.take_rotation();

        let Self {
            images, assembler, ..
        } = self;

        for offset in 0..length {
            let image = &mut images[(start + offset) % length];
            let remaining = fragment_limit.saturating_sub(fragments);
            if 0 == remaining {
                break;
            }

            // The assembler counts what it delivers, so this needs no second
            // closure to count for it — and a closure inside a closure is where
            // the borrow checker's higher-ranked inference gives up.
            let before = assembler.delivered();

            // The parameter's type is spelled out because an unannotated
            // closure gets one lifetime inferred from its first use, and the
            // poll hands it fragments of *every* lifetime it reads — the
            // compiler's "implementation of `FnMut` is not general enough".
            fragments += image.poll(remaining, &mut |fragment: &Fragment<'_>| {
                assembler.push(fragment, handler);
            });

            messages += usize::try_from(assembler.delivered() - before).unwrap_or(0);

            counter_writes.push((image.subscriber_position_id(), image.position()));
        }

        (messages, counter_writes)
    }

    /// Read up to `fragment_limit` fragments from every image, reassembling
    /// them, and hand each whole message to `handler` — which answers.
    ///
    /// The answers are what the reader's position does, and they are why this
    /// exists at all: `Abort` leaves a message unconsumed so it arrives again,
    /// `Commit` publishes the position at that message so a later refusal
    /// cannot take it back, `Break` stops with the message consumed. The
    /// arithmetic lives in [`crate::image`]'s controlled scan; this is where it
    /// is given something to publish through.
    ///
    /// `publish` is handed the counter and the position, because a subscription
    /// has one counter per image and a commit belongs to the image it happened
    /// on.
    pub(crate) fn controlled_poll<H>(
        &mut self,
        fragment_limit: usize,
        handler: &mut H,
        publish: &mut dyn FnMut(i32, i64),
    ) -> usize
    where
        H: ControlledHandler,
    {
        // Borrowed apart for the same reason `poll_messages` does it: the
        // images are what is read, the assembler is where their fragments go.
        let (start, length) = self.take_rotation();

        let Self {
            images, assembler, ..
        } = self;

        let mut fragments = 0;

        for offset in 0..length {
            let image = &mut images[(start + offset) % length];
            let remaining = fragment_limit.saturating_sub(fragments);
            if 0 == remaining {
                break;
            }

            let counter_id = image.subscriber_position_id();
            let mut sink = AssemblingSink { assembler, handler };
            let mut relay = |position: i64| publish(counter_id, position);

            fragments += image.controlled_poll(remaining, &mut sink, &mut relay);
        }

        fragments
    }

    /// Read up to `fragment_limit` fragments from every image, handing each to
    /// `handler` as it lies in the term.
    ///
    /// The fragment-level counterpart of [`Self::poll_messages`]: nothing is
    /// reassembled and nothing is copied, so a message that arrived in three
    /// frames is delivered three times. That is what
    /// `aeron_subscription_poll` does when it is given a plain fragment handler
    /// (`aeron_subscription.c:1040-1080`), and what Java's `Subscription.poll`
    /// always does (`Subscription.java:188`).
    ///
    /// Returns how many fragments were delivered, and the counter writes the
    /// caller has to make afterwards — the same contract as [`Self::poll_messages`].
    pub(crate) fn poll_fragments<F>(
        &mut self,
        fragment_limit: usize,
        handler: &mut F,
    ) -> (usize, Vec<(i32, i64)>)
    where
        F: FnMut(&Fragment<'_>),
    {
        let (start, length) = self.take_rotation();
        let mut fragments = 0;
        let mut counter_writes = Vec::new();

        for offset in 0..length {
            let image = &mut self.images[(start + offset) % length];
            let remaining = fragment_limit.saturating_sub(fragments);
            if 0 == remaining {
                break;
            }

            fragments += image.poll(remaining, &mut *handler);

            counter_writes.push((image.subscriber_position_id(), image.position()));
        }

        (fragments, counter_writes)
    }

    /// The subscription's registration id.
    pub const fn registration_id(&self) -> i64 {
        self.registration_id
    }

    /// The channel it asked for.
    pub fn channel(&self) -> &str {
        &self.channel
    }

    /// The stream it asked for.
    pub const fn stream_id(&self) -> i32 {
        self.stream_id
    }

    /// The channel-status counter, or
    /// [`CHANNEL_STATUS_INDICATOR_NOT_ALLOCATED`].
    ///
    /// `None` is the ordinary case for `aeron:ipc`, where there are no
    /// endpoints whose status could be reported.
    pub const fn channel_status_indicator_id(&self) -> Option<i32> {
        if self.channel_status_indicator_id == CHANNEL_STATUS_INDICATOR_NOT_ALLOCATED {
            None
        } else {
            Some(self.channel_status_indicator_id)
        }
    }

    /// The images currently attached.
    pub fn images(&self) -> &[Image] {
        &self.images
    }

    /// The images currently attached, mutably, for a poll.
    pub fn images_mut(&mut self) -> &mut [Image] {
        &mut self.images
    }

    /// An image by the **publication's** registration id.
    pub fn image(&self, publication_registration_id: i64) -> Option<&Image> {
        self.images
            .iter()
            .find(|image| image.registration_id() == publication_registration_id)
    }

    /// Attach a newly available image.
    ///
    /// Replacing an image that is already there, because the driver can send
    /// `ON_AVAILABLE_IMAGE` for a publication this subscription already holds —
    /// after a rejoin, for instance — and two images over one log buffer would
    /// double-count every fragment.
    pub fn add_image(&mut self, image: Image) {
        let registration_id = image.registration_id();
        if let Some(existing) = self
            .images
            .iter_mut()
            .find(|held| held.registration_id() == registration_id)
        {
            *existing = image;
        } else {
            self.images.push(image);
        }
    }

    /// Detach an image, reporting whether there was one.
    pub fn remove_image(&mut self, publication_registration_id: i64) -> bool {
        let before = self.images.len();
        self.images
            .retain(|image| image.registration_id() != publication_registration_id);
        self.images.len() != before
    }
}

impl std::fmt::Debug for Subscription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Subscription")
            .field("registration_id", &self.registration_id)
            .field("channel", &self.channel)
            .field("stream_id", &self.stream_id)
            .field("images", &self.images.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::Subscription;
    use crate::fragment_assembler::{Action, Message};
    use crate::image::tests::{TempLog, write_message};

    /// A subscription over two images, the first of them holding two messages.
    ///
    /// The imbalance is the point: a poll with a budget of one can never reach
    /// the second image if it always begins at the first, and that is what the
    /// reference's round-robin exists to prevent.
    fn a_subscription_over_two_images() -> (Subscription, TempLog, TempLog) {
        let first = TempLog::new("robin-a");
        let second = TempLog::new("robin-b");

        first.write(|appender| {
            write_message(appender, b"a1");
            write_message(appender, b"a2");
        });
        second.write(|appender| write_message(appender, b"b1"));

        let mut subscription = Subscription::new(1, "aeron:ipc".to_string(), 1, 0);
        subscription.add_image(first.image(1));
        subscription.add_image(second.image(2));

        // The files have to outlive the images, which map them — so they go
        // back to the caller rather than being leaked, and the `Drop` that
        // removes them runs when the test ends.
        (subscription, first, second)
    }

    /// Each poll begins one image later, so a budget of one still reaches all
    /// of them.
    ///
    /// Without the rotation the third poll is where the second image is finally
    /// reached — the first image's two messages spend the first two polls — and
    /// with more messages in front it would never be reached at all. That is
    /// the starvation the reference names in `Subscription.java`, and a
    /// subscription carrying a replay image beside a live one is where it
    /// matters.
    #[test]
    fn a_budget_of_one_still_reaches_every_image() {
        let (mut subscription, _first, _second) = a_subscription_over_two_images();
        let mut publish = |_: i32, _: i64| {};
        let mut order = Vec::new();

        for poll in 0..3 {
            let mut handler = |message: Message<'_>| {
                order.push(message.payload.to_vec());
                Action::Continue
            };
            let read = subscription.controlled_poll(1, &mut handler, &mut publish);
            assert_eq!(1, read, "poll {poll} read one fragment");
        }

        // The order is the whole assertion. Counting fragments would pass
        // without the rotation too — the third poll reaches the second image
        // once the first has run dry — and it is reaching it *first* that the
        // reference's round-robin buys.
        assert_eq!(
            vec![b"a1".to_vec(), b"b1".to_vec(), b"a2".to_vec()],
            order,
            "each poll begins one image later"
        );
    }

    /// The same for the plain fragment path, which had the same loop.
    #[test]
    fn the_fragment_path_rotates_too() {
        let (mut subscription, _first, _second) = a_subscription_over_two_images();
        let mut seen = Vec::new();

        for _ in 0..3 {
            let mut handler = |fragment: &crate::image::Fragment<'_>| {
                seen.push((fragment.session_id(), fragment.position()));
            };
            assert_eq!(1, subscription.poll_fragments(1, &mut handler).0);
        }

        // Two images, both written by session 7, so the positions are what
        // tells them apart: the second image's first message begins at 0 of its
        // own term, and the first image's second message does not.
        assert_eq!(
            3,
            seen.len(),
            "one fragment a poll, and all three were read"
        );
        assert_eq!(
            seen[0].1, seen[1].1,
            "the third poll went back to the first image"
        );
    }

    /// A rotation that ran off the end comes back to the start rather than
    /// leaving the remainder behind — the reference resets its index to one.
    #[test]
    fn the_rotation_wraps() {
        let (mut subscription, _first, _second) = a_subscription_over_two_images();
        assert_eq!((0, 2), subscription.rotation());

        assert_eq!((0, 2), subscription.take_rotation());
        assert_eq!((1, 2), subscription.take_rotation());
        assert_eq!((0, 2), subscription.take_rotation());
    }
}
