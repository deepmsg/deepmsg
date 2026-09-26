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
        }
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
