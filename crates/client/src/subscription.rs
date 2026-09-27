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

use crate::fragment_assembler::FragmentAssembler;
use crate::image::Fragment;
use crate::image::Image;

/// A subscription and the images attached to it.
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
            assembler: FragmentAssembler::new(),
        }
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
        let Self {
            images, assembler, ..
        } = self;

        for image in images.iter_mut() {
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
