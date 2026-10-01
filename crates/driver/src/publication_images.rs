//! The publication images a driver owns, and the thread that maps their log
//! buffers.
//!
//! Mirrors the conductor's half of the image path in
//! `aeron-driver/src/main/c/aeron_driver_conductor.c`:
//! `execute_create_publication_image_*` (`:6490-6800`), the match rule a
//! subscription has to pass to read an image
//! (`network_subscription_link_matches_allowing_wildcard`, `:6440-6470`), and
//! the registration id an image is named by — which is a correlation id the
//! *driver burns*, not one a client sent (`:6530`).
//!
//! # The shape is the publications' shape, one step further out
//!
//! A client asks for an image by *not* asking for one: it subscribes, a
//! publisher starts sending, and the receiver notices a session nothing serves
//! and asks the conductor to build one. So the state machine has one more
//! entering edge than the others — a `SETUP` from the wire rather than a
//! command — and the same three steps after it: map a log buffer, create the
//! thing, answer whoever was waiting.
//!
//! # The path is a byte contract
//!
//! An image's log buffer is named `images/<registration id>.logbuffer`
//! (`aeron-client/src/main/c/util/aeron_fileutil.c:1222-1232`), and that name
//! is sent to the client in `ON_AVAILABLE_IMAGE`. The registration id is
//! therefore not an internal detail: it is in a file name a client maps.

use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;

use crate::ipc_subscriptions::SubscriptionLink;

use deepmsg_cnc::{CounterManager, CounterRegions};

use crate::ipc_publications::{AddError, Now};
use crate::native_resource_agent::{Completion, NativeResourceAgent, StorageWarning};
use crate::protocol::SetupFrame;
use crate::publication_image::{ImageCounters, ImageState, PublicationImage};
use crate::receiver::ReceiverProxy;
use crate::{position as counter_position, sys};

/// What the conductor keeps about an image the receiver owns.
#[derive(Clone, Debug)]
pub struct PublicationImageRecord {
    /// The id the log buffer is named after and `ON_AVAILABLE_IMAGE` reports.
    pub registration_id: i64,
    /// The session this image carries.
    pub session_id: i32,
    /// The stream it carries.
    pub stream_id: i32,
    /// Which receive endpoint serves it.
    pub endpoint_id: u64,
    /// The channel as the client wrote it.
    pub channel: Vec<u8>,
    /// Where the packets came from, formatted as the reference formats a
    /// source identity — what `ON_AVAILABLE_IMAGE` tells a client its image is
    /// reading (`aeron_publication_image.c:336-343`).
    pub source_identity: String,
    /// Where its log buffer is.
    pub path: PathBuf,
    /// The counters a client reads.
    pub counters: ImageCounters,
    /// Where it is in its life (`aeron_publication_image_on_time_event`).
    pub state: ImageState,
    /// When that changed.
    pub time_of_last_state_change_ns: i64,
    /// How many subscriptions read it.
    pub refcount: i32,
    /// The header flags of the `SETUP` that started it
    /// (`aeron_publication_image.h:73`, where the image carries the sender's
    /// byte across from the frame).
    ///
    /// Kept on the record rather than only on the receiver's image because the
    /// one reader is the conductor, which never touches the image itself: a
    /// response publication names an image by registration id and has to be
    /// told whether that image's sender asked for a response channel at all
    /// ([`Self::has_send_response_setup`], `find_response_publication_image`).
    pub setup_flags: u8,
}

impl PublicationImageRecord {
    /// Whether the `SETUP` that started this image asked for a response
    /// channel (`aeron_publication_image_has_send_response_setup`,
    /// `aeron_publication_image.h:346-349`).
    pub const fn has_send_response_setup(&self) -> bool {
        self.setup_flags & crate::protocol::header_flags::SETUP_SEND_RESPONSE != 0
    }
}

/// An image whose log buffer is being created.
struct PendingImage {
    registration_id: i64,
    endpoint_id: u64,
    session_id: i32,
    stream_id: i32,
    channel: Vec<u8>,
    counters: ImageCounters,
    path: PathBuf,
    /// The `SETUP` that started it, which is where the stream's first position,
    /// term length and MTU come from.
    setup: SetupFrame,
    /// The header flags of that `SETUP`, which the frame's own body does not
    /// carry ([`PublicationImage::setup_flags`]).
    setup_flags: u8,
    /// Where the packets came from.
    source: SocketAddr,
    /// Where a control frame goes.
    control_address: SocketAddr,
    /// Whether the image was rejected while it was being built, and the words
    /// the `ERR` frame will carry.
    invalidation: Option<Vec<u8>>,
    /// The untethered timeouts the channel named, which the image's readers
    /// inherit (`untethered-window-limit-timeout` and its two siblings).
    untethered: crate::publication_params::SubscriptionParams,
    /// Whether this image is one of a group, which lands in the log buffer's
    /// `group` byte (`aeron_driver_conductor_treat_image_as_multicast`,
    /// `aeron_driver_conductor.c:674-680`).
    group_semantics: bool,
}

/// Whether an image is one of a group
/// (`aeron_driver_conductor_treat_image_as_multicast`,
/// `aeron-driver/src/main/c/aeron_driver_conductor.c:674-680`): the channel's
/// own group semantics, **or** the `SETUP`'s `GROUP` flag.
///
/// The two are one question because both name a channel that may have several
/// receivers at once, which is what the log buffer's `group` byte is about — so
/// a channel whose URI does not say it is one is still a group if the far end
/// says so. The reference's third arm, a `group=` parameter forced true, is not
/// parsed in this build.
fn image_group_semantics(uri: &crate::channel_uri::ChannelUri<'_>, setup_flags: u8) -> bool {
    crate::udp_channel::UdpChannel::uri_has_group_semantics(uri)
        || setup_flags & crate::protocol::header_flags::SETUP_GROUP != 0
}

/// The images a driver owns.
pub struct PublicationImages {
    images: Vec<PublicationImageRecord>,
    pending: Vec<PendingImage>,
    agent: NativeResourceAgent,
}

impl PublicationImages {
    /// Start the manager and the thread that maps log buffers.
    ///
    /// # Errors
    ///
    /// [`io::Error`] if the agent thread cannot be spawned.
    pub fn start(storage: crate::native_resource_agent::StorageChecks) -> io::Result<Self> {
        Ok(Self {
            images: Vec::new(),
            pending: Vec::new(),
            agent: NativeResourceAgent::start(storage)?,
        })
    }

    /// The images, in creation order.
    pub fn images(&self) -> &[PublicationImageRecord] {
        &self.images
    }

    /// The mutable images.
    pub fn images_mut(&mut self) -> &mut [PublicationImageRecord] {
        &mut self.images
    }

    /// One image by its registration id.
    pub fn find(&self, registration_id: i64) -> Option<&PublicationImageRecord> {
        self.images
            .iter()
            .find(|image| image.registration_id == registration_id)
    }

    /// How many images are waiting for a log buffer.
    pub fn pending(&self) -> usize {
        self.pending.len()
    }

    /// The images a subscription joins as it is created
    /// (`aeron_driver_conductor.c:5121-5143`).
    ///
    /// The rule is [`SubscriptionLink::matches_image`], which the live
    /// image-to-subscription path asks too — the two have to agree, because an
    /// image a subscription is linked to at creation is one it reads for as
    /// long as the image exists.
    pub fn matching(&self, link: &SubscriptionLink) -> Vec<i64> {
        self.images
            .iter()
            .filter(|image| {
                link.matches_image(image.endpoint_id, image.stream_id, image.session_id)
            })
            .map(|image| image.registration_id)
            .collect()
    }

    /// Begin building an image for a session a `SETUP` announced
    /// (`execute_create_publication_image_validate`, `:6490-6560`).
    ///
    /// The registration id is burned here, before the log buffer is asked for,
    /// because the buffer's *name* carries it.
    ///
    /// # Errors
    ///
    /// [`AddError`] for an MTU the endpoint cannot serve, or an agent that has
    /// stopped.
    #[allow(clippy::too_many_arguments)]
    pub fn begin_create(
        &mut self,
        registration_id: i64,
        client_id: i64,
        endpoint_id: u64,
        channel: &[u8],
        setup: &SetupFrame,
        setup_flags: u8,
        source: SocketAddr,
        control_address: SocketAddr,
        config: &crate::config::DriverConfig,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        now: Now,
    ) -> Result<(), AddError> {
        // The sender's MTU has to fit the endpoint's socket and the window this
        // receiver offers (`validate_sender_mtu_length`,
        // `media/aeron_receive_channel_endpoint.c:990-1040`).
        if setup.mtu > config.mtu_length.max(setup.mtu) || setup.mtu <= 0 {
            return Err(AddError::InvalidChannel(format!(
                "mtuLength={} > MAX_UDP_PAYLOAD_LENGTH",
                setup.mtu
            )));
        }

        // The channel's subscription parameters: an image is created by a
        // `SETUP`, so the timeouts its readers are held to come from the
        // *channel* rather than from each subscription
        // (`aeron_driver_uri_subscription_params` on the channel's URI).
        let (untethered, group_semantics) = match crate::channel_uri::ChannelUri::parse(channel) {
            Ok(uri) => (
                crate::publication_params::SubscriptionParams::resolve(&uri, config)
                    .unwrap_or_else(|_| {
                        crate::publication_params::SubscriptionParams::defaults(config)
                    }),
                image_group_semantics(&uri, setup_flags),
            ),
            Err(_) => (
                crate::publication_params::SubscriptionParams::defaults(config),
                false,
            ),
        };

        let counters_pair = allocate_counters(
            counters,
            regions,
            registration_id,
            client_id,
            setup.session_id,
            setup.stream_id,
            channel,
            now.ms,
        )?;

        // Both position counters start at the join position, not at zero
        // (`aeron_publication_image.c:391-392`). An image is created for a
        // stream that is already running, so its own position *is* where the
        // stream is: a zero here says the image has read nothing and is owed
        // everything from the beginning, which is a position the sender
        // overwrote long ago.
        //
        // It is not only the image's own accounting. A subscription that links
        // to this image starts where `rcv-pos` says (`join_position`,
        // `aeron_publication_image.h:376-396`), so an unseeded `rcv-pos` starts
        // every late subscriber — and every subscriber that joins after a
        // restart — reading from position zero, where there is nothing left to
        // read.
        let (join_position, _) = crate::publication_image::stream_start(setup);

        let _ = counters.set_value(regions, counters_pair.rcv_hwm, join_position);
        let _ = counters.set_value(regions, counters_pair.rcv_pos, join_position);

        let path = image_path(&config.aeron_dir, registration_id);

        self.agent
            .map_log_buffer(
                &path,
                setup.term_length,
                config.layout.page_size,
                config.term_buffer_sparse_file,
            )
            .map_err(|_| AddError::AgentStopped)?;

        self.pending.push(PendingImage {
            registration_id,
            endpoint_id,
            session_id: setup.session_id,
            stream_id: setup.stream_id,
            channel: channel.to_vec(),
            counters: counters_pair,
            path,
            setup: *setup,
            setup_flags,
            source,
            control_address,
            invalidation: None,
            untethered,
            group_semantics,
        });

        Ok(())
    }

    /// Reject a pending image, saying why: an image that cannot be built still
    /// has to tell its sender (`aeron_publication_image_invalidate`).
    ///
    /// The reason is this driver's own here — it is a refusal this build
    /// produced, not words a client sent — so it is taken as text and stored as
    /// the bytes the `ERR` frame will carry.
    pub fn reject_pending(&mut self, registration_id: i64, reason: &str) {
        if let Some(pending) = self
            .pending
            .iter_mut()
            .find(|pending| pending.registration_id == registration_id)
        {
            pending.invalidation = Some(reason.as_bytes().to_vec());
        }
    }

    /// Take the agent's completions and build the images whose log buffers have
    /// landed.
    ///
    /// Returns how many images were created.
    #[allow(clippy::too_many_arguments)]
    pub fn poll(
        &mut self,
        config: &crate::config::DriverConfig,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        endpoints: &mut crate::receive_endpoints::ReceiveChannelEndpoints,
        receiver: &ReceiverProxy,
        now: Now,
        storage_warnings: &mut Vec<StorageWarning>,
    ) -> Vec<i64> {
        let completions = self.agent.poll();
        let mut created = Vec::new();

        storage_warnings.extend(self.agent.poll_warnings());

        for completion in completions {
            match completion {
                Completion::Mapped { path, log } => {
                    let Some(index) = self.pending.iter().position(|entry| entry.path == path)
                    else {
                        let _ = self.agent.free_log_buffer(*log);
                        continue;
                    };

                    let pending = self.pending.swap_remove(index);
                    created.push(self.create_image(
                        config, counters, regions, endpoints, receiver, pending, *log, now,
                    ));
                }
                Completion::MapFailed { .. } => {
                    // The image never existed as far as a client is concerned:
                    // no announcement was made, so nothing has to be taken
                    // back — the receiver's pending setup simply times out and
                    // asks again.
                }
                Completion::Freed { .. } => {}
            }
        }

        created.into_iter().flatten().collect()
    }

    /// Build one image, hand it to the receiver, and answer the fact that an
    /// endpoint is now serving it.
    #[allow(clippy::too_many_arguments)]
    fn create_image(
        &mut self,
        config: &crate::config::DriverConfig,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        endpoints: &mut crate::receive_endpoints::ReceiveChannelEndpoints,
        receiver: &ReceiverProxy,
        pending: PendingImage,
        log: deepmsg_core::logbuffer::logfile::LogFile,
        now: Now,
    ) -> Option<i64> {
        let window = crate::flowcontrol::receiver_window_length(
            config.receiver_window_length.unsigned_abs() as usize,
            pending.setup.term_length.unsigned_abs() as usize,
        );
        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        let window = window as i32;

        let mut image = PublicationImage::create(
            pending.registration_id,
            pending.endpoint_id,
            &pending.channel,
            Box::new(log),
            &pending.setup,
            pending.source,
            pending.control_address,
            pending.counters,
            window,
            config.status_message_timeout_ns,
            config.layout.page_size,
            pending.untethered,
            pending.group_semantics,
            now.ns,
        );

        if let Some(reason) = pending.invalidation {
            image.invalidate(&reason);
        }

        if receiver.add_image(Box::new(image)).is_err() {
            return None;
        }

        endpoints.attach_image(pending.endpoint_id);
        let _ = (counters, regions);

        self.images.push(PublicationImageRecord {
            registration_id: pending.registration_id,
            session_id: pending.session_id,
            stream_id: pending.stream_id,
            endpoint_id: pending.endpoint_id,
            channel: pending.channel,
            source_identity: crate::udp_channel::format_source_identity(pending.source)
                .unwrap_or_default(),
            path: pending.path,
            counters: pending.counters,
            state: ImageState::Active,
            time_of_last_state_change_ns: now.ns,
            refcount: 0,
            setup_flags: pending.setup_flags,
        });

        Some(pending.registration_id)
    }

    /// A subscription was linked to an image, so the image counts a reader.
    pub fn incref(&mut self, registration_id: i64) {
        if let Some(image) = self
            .images
            .iter_mut()
            .find(|image| image.registration_id == registration_id)
        {
            image.refcount += 1;
        }
    }

    /// A subscription let go.
    pub fn decref(&mut self, registration_id: i64) {
        if let Some(image) = self
            .images
            .iter_mut()
            .find(|image| image.registration_id == registration_id)
        {
            image.refcount -= 1;
        }
    }

    /// The join position a subscription starts reading at
    /// (`aeron_publication_image_join_position`,
    /// `aeron-driver/src/main/c/aeron_publication_image.h:376-396`): where the
    /// image has been rebuilt to, and never before it.
    ///
    /// The reference takes the *slowest reader* when there are several, which
    /// is a position the conductor can only get by asking the receiver. This
    /// reads the counter instead: `rcv-pos` is by construction the position a
    /// reader may start at without seeing a hole, and for the one-reader case —
    /// which is every case until a second client subscribes to one stream —
    /// the two are the same number.
    pub fn join_position(
        &self,
        registration_id: i64,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
    ) -> i64 {
        self.find(registration_id)
            .and_then(|image| counters.value(regions, image.counters.rcv_pos))
            .unwrap_or(0)
    }

    /// The image a subscription should read: the one on this stream, and on
    /// this session when the subscription named one
    /// (`network_subscription_link_matches_allowing_wildcard`, `:6440-6470`).
    pub fn find_for_subscription(&self, stream_id: i32, session_id: Option<i32>) -> Option<i64> {
        self.images
            .iter()
            .find(|image| {
                image.stream_id == stream_id
                    && session_id.is_none_or(|session_id| image.session_id == session_id)
            })
            .map(|image| image.registration_id)
    }

    /// The timeout tier's turn for every image
    /// (`aeron_publication_image_on_time_event`): the receiver runs the
    /// transitions, and this records what it saw.
    pub fn on_time_event(&mut self, states: &[(i64, ImageState)], now_ns: i64) {
        for (registration_id, state) in states {
            if let Some(image) = self
                .images
                .iter_mut()
                .find(|image| image.registration_id == *registration_id)
            {
                if image.state != *state {
                    image.state = *state;
                    image.time_of_last_state_change_ns = now_ns;
                }
            }
        }
    }

    /// Let go of the images a client's subscription held, which is what a
    /// client's death does.
    pub fn release_links(&mut self, links: &[(i64, i64)]) {
        for (_, image_registration_id) in links {
            self.decref(*image_registration_id);
        }
    }

    /// Take the record out of the collection.
    ///
    /// The log buffer is not freed here: the *receiver* owns the mapping (the
    /// image went there at create) and unmaps and unlinks it when its
    /// `RemoveImage` command arrives — which is the same order every other
    /// release in this driver keeps, and the one that cannot leave a file
    /// mapped by a thread that no longer knows about it.
    pub fn remove(&mut self, registration_id: i64) -> Option<PublicationImageRecord> {
        let index = self
            .images
            .iter()
            .position(|image| image.registration_id == registration_id)?;

        Some(self.images.swap_remove(index))
    }

    /// Close everything: the agent's thread, and the images with it.
    pub fn close(&mut self) {
        self.images.clear();
        self.pending.clear();
    }

    /// Every storage warning the agent raised since the last call.
    pub fn poll_storage_warnings(&self) -> Vec<StorageWarning> {
        self.agent.poll_warnings()
    }
}

/// The counters an image's client reads: `rcv-hwm` and `rcv-pos`
/// (`aeron_position.c:157-199`) and `rcv-naks-sent`
/// (`aeron_driver_conductor.c:6680-6683`).
///
/// All three are owned by the client that asked for the subscription, not by
/// the driver that allocated them: the reference passes
/// `subscription_link->client_id` to each, and a tool that groups counters by
/// owner — or reclaims them when a client goes — reads that field, not the
/// registration id.
#[allow(clippy::too_many_arguments)] // the identity the three keys are built from
fn allocate_counters(
    counters: &mut CounterManager,
    regions: &CounterRegions<'_>,
    registration_id: i64,
    client_id: i64,
    session_id: i32,
    stream_id: i32,
    channel: &[u8],
    now_ms: i64,
) -> Result<ImageCounters, AddError> {
    let allocate = |counters: &mut CounterManager, name: &str, type_id: i32| {
        counter_position::allocate_stream_counter(
            counters,
            regions,
            name,
            type_id,
            client_id,
            registration_id,
            session_id,
            stream_id,
            channel,
            "",
            now_ms,
        )
    };

    let Some(rcv_hwm) = allocate(counters, "rcv-hwm", counter_position::type_id::RECEIVER_HWM)
    else {
        return Err(AddError::NoCounterRecord);
    };

    let Some(rcv_pos) = allocate(
        counters,
        "rcv-pos",
        counter_position::type_id::RECEIVER_POSITION,
    ) else {
        return Err(AddError::NoCounterRecord);
    };

    let Some(rcv_naks_sent) = allocate(
        counters,
        "rcv-naks-sent",
        counter_position::type_id::RECEIVER_NAKS_SENT,
    ) else {
        return Err(AddError::NoCounterRecord);
    };

    Ok(ImageCounters {
        rcv_hwm,
        rcv_pos,
        rcv_naks_sent,
    })
}

/// Where an image's log buffer goes
/// (`aeron-client/src/main/c/util/aeron_fileutil.c:1222-1232`): the `images`
/// directory, named after the image's registration id.
pub fn image_path(aeron_dir: &std::path::Path, registration_id: i64) -> PathBuf {
    aeron_dir
        .join("images")
        .join(format!("{registration_id}.logbuffer"))
}

/// The kernel's buffer lengths, for a caller that wants to report them. Zeroes
/// when the kernel cannot be asked, which is what a driver that cannot make a
/// socket would have written anyway.
pub fn os_defaults() -> sys::SocketBufferLengths {
    sys::default_socket_buffers().unwrap_or(sys::SocketBufferLengths {
        rcvbuf: 0,
        sndbuf: 0,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::protocol::header_flags;

    fn uri(text: &str) -> crate::channel_uri::ChannelUri<'_> {
        crate::channel_uri::ChannelUri::parse(text.as_bytes()).expect("a URI")
    }

    /// Both halves of the predicate, and each on its own: a channel that says
    /// it is a group, a channel that does not but whose `SETUP` does, and one
    /// that is neither. A hardcoded answer satisfies at most two of the three.
    #[test]
    fn an_image_is_a_group_when_either_the_channel_or_the_setup_says_so() {
        let multi_destination = uri("aeron:udp?endpoint=127.0.0.1:40123|control-mode=manual");
        let plain = uri("aeron:udp?endpoint=127.0.0.1:40123");

        assert!(
            image_group_semantics(&multi_destination, 0),
            "a multi-destination channel is a group on its own"
        );
        assert!(
            image_group_semantics(&plain, header_flags::SETUP_GROUP),
            "and a channel that is not one is still a group if the far end says so"
        );
        assert!(
            !image_group_semantics(&plain, 0),
            "but neither saying it is not a group"
        );
    }
}
