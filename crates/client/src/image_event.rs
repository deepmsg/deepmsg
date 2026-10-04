//! An image appearing under, or leaving, a subscription.
//!
//! `ON_AVAILABLE_IMAGE` and `ON_UNAVAILABLE_IMAGE` are the two messages that
//! tell a client its set of images changed. Until this module existed they were
//! applied in silence — the image was pushed into, or taken out of,
//! [`crate::subscription::Subscription::images`] and nothing said so.
//!
//! The reference delivers both through callbacks registered per subscription
//! (`Aeron.addSubscription(…, availableImageHandler, unavailableImageHandler)`,
//! `Aeron.java:417`; `aeron_async_add_subscription(…, on_available_image_handler,
//! …, on_unavailable_image_handler, …)`, `aeronc.h:606-611`), and Java also
//! takes a default pair on the context (`Aeron.java:1134`). Both hand over the
//! [`Image`](crate::image::Image) itself, and both forbid calling back into the
//! client from inside the callback (`AvailableImageHandler.java:21-28`).
//!
//! This crate's shape is a queue the caller drains
//! ([`crate::client::Client::image_events`]), for the reason
//! [`crate::counter::CounterEvent`] is one: a poll-driven client has no thread
//! to run a callback on. The handle is the one thing that cannot be carried
//! across, because the image lives inside the client and the poll already holds
//! it mutably — so an event names the image instead, by the same two ids the
//! protocol used, and the caller finds it in
//! [`Subscription::image`](crate::subscription::Subscription::image).

/// One image arriving under a subscription, or one leaving it.
///
/// The pair is the whole story of an image's life, so an application that
/// tracks what it is reading can keep its own record from these and never walk
/// the subscription at all.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ImageEvent {
    /// An image became available.
    Available {
        /// **This client's** registration id for the subscription — the
        /// correlation id its `ADD_SUBSCRIPTION` used.
        subscription_registration_id: i64,
        /// The **publication's** registration id, which is what names the image
        /// everywhere else ([`Image::registration_id`](crate::image::Image::registration_id)).
        publication_registration_id: i64,
        /// The publication's session id.
        session_id: i32,
        /// The publication's stream id.
        stream_id: i32,
        /// Where the reader starts: the join position the driver wrote into the
        /// subscriber-position counter before announcing the image.
        position: i64,
    },
    /// An image went away — the driver withdrew it, or the publication was
    /// revoked.
    ///
    /// A withdrawal and a revocation read the same here, as they do in the
    /// reference's handler; an application that needs to tell them apart asks
    /// [`Image::is_publication_revoked`](crate::image::Image::is_publication_revoked)
    /// before the image is gone.
    Unavailable {
        /// The subscription it was under.
        subscription_registration_id: i64,
        /// The publication whose image it was.
        publication_registration_id: i64,
        /// The publication's session id.
        session_id: i32,
        /// The publication's stream id.
        stream_id: i32,
        /// Where the reader had got to.
        position: i64,
    },
}

impl ImageEvent {
    /// The subscription the event belongs to, whichever kind it is.
    pub const fn subscription_registration_id(&self) -> i64 {
        match self {
            Self::Available {
                subscription_registration_id,
                ..
            }
            | Self::Unavailable {
                subscription_registration_id,
                ..
            } => *subscription_registration_id,
        }
    }

    /// The publication whose image the event is about, whichever kind it is.
    pub const fn publication_registration_id(&self) -> i64 {
        match self {
            Self::Available {
                publication_registration_id,
                ..
            }
            | Self::Unavailable {
                publication_registration_id,
                ..
            } => *publication_registration_id,
        }
    }
}
