//! The publications a driver owns, and the state machine that makes them.
//!
//! Mirrors the IPC half of `aeron-driver/src/main/c/aeron_driver_conductor.c`:
//! the command's four states (`:3956-4080`), the session id speculation
//! (`:1062-1105`), the shared-publication lookup (`:1765-1785`), and the link
//! that turns a client's request into a publication plus a reply
//! (`:3693-3767`). (M20)
//!
//! # Four states, one of them asynchronous
//!
//! The reference runs `VALIDATE` → `RESOLVE_PUBLICATION` →
//! `AWAIT_LOG_BUFFER` → `CREATE_PUBLICATION`, and the last two are what this
//! module's shape is about:
//!
//! * **Validate and resolve** happen inside one [`IpcPublications::add_publication`]
//!   call: the URI is parsed, its parameters resolved, and the driver asks
//!   whether it already has a publication this one may share.
//! * **Await** means a [`PendingPublication`] the native resource agent
//!   answers on a later pass — creating a log buffer means a 192 MiB file at
//!   the default term length, and the conductor must not be inside that. The
//!   client is *already registered* by then, so its heartbeat runs and a
//!   publication that takes a while to appear does not look like a dead client.
//! * **Create** happens in [`IpcPublications::poll`], when the mapping arrives:
//!   the session id is speculated, the two counters are allocated, the metadata
//!   is written, the client is linked and the reply goes out.
//!
//! # Why this is not four methods on the conductor
//!
//! The four states are one thing's life cycle, and the conductor already
//! carries the command dispatch, the client pool, the counters and the ring.
//! Keeping the state machine's *data* — which publications are being built, and
//! the session id cursor — next to its *transitions* is what a reader of the
//! session id rule or the sharing rule has to see together.

use std::io;
use std::path::PathBuf;

use deepmsg_cnc::command::{
    AddPublicationCommand, CHANNEL_STATUS_INDICATOR_NOT_ALLOCATED, ERROR_CODE_GENERIC_ERROR,
    ERROR_CODE_INVALID_CHANNEL, ERROR_CODE_NOT_SUPPORTED, ERROR_CODE_STORAGE_SPACE,
    ImageBuffersReady, PublicationBuffersReady,
};
use deepmsg_cnc::{CounterManager, CounterRegions};
use deepmsg_core::logbuffer::logfile::LogFile;
use deepmsg_core::logbuffer::position;

use crate::channel_uri::{ChannelUri, Transport, UriError};
use crate::clients::{ClientEvents, ClientRecord, Clients, PublicationLink};
use crate::config::DriverConfig;
use crate::dir::PUBLICATIONS_DIR;
use crate::ipc_publication::{IpcPublication, PublicationIdentity, ShareMismatch, State};
use crate::ipc_subscriptions::{IPC_CHANNEL, IpcSubscriptions};
use crate::native_resource_agent::{
    Completion, NativeResourceAgent, StorageChecks, StorageWarning,
};
use crate::position as counter_position;
use crate::publication_params::{PublicationParams, PublicationParamsError};
use crate::subscribable::UntetheredEvent;
use crate::sys;

/// The moment a command is served at, and how long a client this driver has
/// not seen before is given to live.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Now {
    /// Milliseconds since the epoch, from the cached clock.
    pub ms: i64,
    /// Nanoseconds since the epoch, from the same clock.
    pub ns: i64,
    /// `aeron.client.liveness.timeout`, for a client this driver is meeting
    /// for the first time.
    pub client_liveness_timeout_ns: i64,
}

/// Why an `ADD_PUBLICATION` could not be served.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AddError {
    /// The channel URI or one of its parameters was refused.
    Params(PublicationParamsError),
    /// A channel this build does not serve. UDP publications arrive in P1-4;
    /// until then the honest answer is "not supported" rather than silence,
    /// which a client would wait out and then report as a driver timeout.
    UnsupportedTransport,
    /// The client could not be registered, so nothing may be published on its
    /// behalf.
    NoClientRecord,
    /// An existing publication on this stream has this session id and cannot
    /// be shared with (`aeron_driver_conductor.c:3919-3952`).
    SessionClash {
        /// The session id both asked for.
        session_id: i32,
        /// The stream they are on.
        stream_id: i32,
    },
    /// An existing publication could be shared except for one parameter.
    Share(ShareMismatch),
    /// A UDP channel this build refused, with the reference's own reason
    /// ([`crate::udp_channel::UdpChannelError`]).
    Channel(Box<crate::udp_channel::UdpChannelError>),
    /// A channel a *publication* may not use — the checks the reference makes
    /// on the endpoint and the control address before it creates anything
    /// (`aeron_driver_conductor.c:569-608`). Its message is the reference's,
    /// error code and all.
    InvalidChannel(String),
    /// A send endpoint could not be created or shared. Carried as its code and
    /// its words because the underlying error is a syscall's, which is neither
    /// cloneable nor comparable — and those two things are all a caller of
    /// this enum uses.
    Endpoint {
        /// The `ON_ERROR` code the reference answers with.
        error_code: i32,
        /// The reference's message.
        message: String,
    },
    /// A publication's counters could not be allocated.
    NoCounterRecord,
    /// The native resource agent is not there to create the log buffer — its
    /// thread has died, which nothing else in this build can cause.
    AgentStopped,
}

impl std::fmt::Display for AddError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Params(error) => write!(f, "{error}"),
            Self::UnsupportedTransport => {
                f.write_str("only `aeron:ipc` channels are served by this driver")
            }
            Self::NoClientRecord => f.write_str("failed to add client"),
            Self::SessionClash {
                session_id,
                stream_id,
            } => write!(
                f,
                "existing publication has clashing sessionId={session_id} for streamId={stream_id}"
            ),
            Self::Share(mismatch) => write!(f, "{mismatch}"),
            Self::Channel(error) => write!(f, "{error}"),
            Self::InvalidChannel(message) => f.write_str(message),
            Self::Endpoint { message, .. } => f.write_str(message),
            Self::NoCounterRecord => f.write_str("could not allocate the publication's counters"),
            Self::AgentStopped => f.write_str("the native resource agent has stopped"),
        }
    }
}

impl AddError {
    /// The `ON_ERROR` code this failure is reported under.
    ///
    /// The reference's code comes from what the failing function *set*, not
    /// from where in the state machine it failed
    /// (`aeron_driver_conductor.c:2344-2352`: a negative `aeron_errcode()`
    /// becomes its own negation, anything else becomes the generic code). The
    /// two codes that matter are therefore:
    ///
    /// * [`ERROR_CODE_INVALID_CHANNEL`] for a channel whose URI the driver
    ///   cannot read — the scheme, the transport, the length, the *shape* of a
    ///   parameter — and for a session id clash: the places the reference
    ///   raises `-AERON_ERROR_CODE_INVALID_CHANNEL` (`aeron_uri.c:269-273`,
    ///   `:311-314`, `:48`, `:84`).
    /// * [`ERROR_CODE_GENERIC_ERROR`] for a parameter *value* the reference's
    ///   readers reject, because those return a bare `-1` or raise `EINVAL`,
    ///   which the conductor's composition turns into the generic code
    ///   (`aeron_driver_conductor.c:2326-2341`) — and for every other
    ///   parameter check that refuses.
    ///
    /// The unsupported transport has no reference answer — the reference serves
    /// UDP channels — so it reports the code the protocol has for exactly this
    /// ([`ERROR_CODE_NOT_SUPPORTED`]).
    pub const fn error_code(&self) -> i32 {
        match self {
            Self::Params(PublicationParamsError::Uri(
                UriError::InvalidScheme
                | UriError::TooLong { .. }
                | UriError::NotUtf8
                | UriError::MissingKey { .. }
                | UriError::MissingValue { .. },
            ))
            | Self::SessionClash { .. } => ERROR_CODE_INVALID_CHANNEL,
            Self::UnsupportedTransport => ERROR_CODE_NOT_SUPPORTED,
            // A UDP channel the reference refuses *by name* is an invalid
            // channel; a resolution failure is an errno and reads as generic.
            Self::InvalidChannel(_) => ERROR_CODE_INVALID_CHANNEL,
            Self::Channel(error) => error.error_code(),
            Self::Endpoint { error_code, .. } => *error_code,
            Self::Params(_)
            | Self::NoClientRecord
            | Self::Share(_)
            | Self::NoCounterRecord
            | Self::AgentStopped => ERROR_CODE_GENERIC_ERROR,
        }
    }
}

impl std::error::Error for AddError {}

impl From<PublicationParamsError> for AddError {
    fn from(error: PublicationParamsError) -> Self {
        Self::Params(error)
    }
}

impl From<crate::channel_uri::UriError> for AddError {
    fn from(error: crate::channel_uri::UriError) -> Self {
        Self::Params(error.into())
    }
}

/// The session ids this driver hands out for its publications.
///
/// The reference keeps two fields and three functions for this
/// (`aeron_driver_conductor.c:1062-1105`), and the rule they implement is worth
/// seeing whole: the cursor is *random*, the reserved range is skipped rather
/// than used, and a speculation asks which of the ids from the cursor onwards
/// this stream is not already running under. Two publications created a moment
/// apart on the same stream therefore get different sessions, and a driver that
/// restarts does not collide with one that is still running.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SessionIds {
    next: i32,
    reserved_low: i32,
    reserved_high: i32,
}

impl SessionIds {
    /// Start from a random id, as the conductor does when it initialises
    /// (`:826`).
    pub fn start(reserved_low: i32, reserved_high: i32) -> Self {
        Self {
            next: sys::random_i32(),
            reserved_low,
            reserved_high,
        }
    }

    /// Where the next speculation starts, skipping the reserved range
    /// (`:1062-1069`).
    ///
    /// The skipped-to value is `high + 1` **wrapping**, because these are
    /// `i32`s a client compares for equality and never does arithmetic on.
    pub fn cursor(&self) -> i32 {
        if self.reserved_low <= self.next && self.next <= self.reserved_high {
            self.reserved_high.wrapping_add(1)
        } else {
            self.next
        }
    }

    /// The first id at or after the cursor that no publication in `used` holds
    /// (`:1077-1105`).
    ///
    /// `used` is `(stream_id, session_id)` for the publications that count:
    /// the ones that are `ACTIVE` or `DRAINING`, which is the state test the
    /// reference makes before it sets a bit. The set it fills is one longer
    /// than the number of publications, which is why there is always an
    /// answer.
    pub fn speculate(&self, stream_id: i32, used: impl IntoIterator<Item = (i32, i32)>) -> i32 {
        let cursor = self.cursor();
        let mut taken = [false; Self::MAX_SPECULATION];

        for (used_stream_id, session_id) in used {
            if used_stream_id != stream_id {
                continue;
            }

            let offset = session_id.wrapping_sub(cursor);
            if let Ok(offset) = usize::try_from(offset) {
                if let Some(slot) = taken.get_mut(offset) {
                    *slot = true;
                }
            }
        }

        let first_free = taken.iter().position(|is_taken| !is_taken).unwrap_or(0);

        #[allow(clippy::cast_possible_truncation)] // bounded by MAX_SPECULATION
        cursor.wrapping_add(first_free as i32)
    }

    /// Move the cursor past `session_id` (`:1071-1075`).
    ///
    /// Only a *speculated* id moves it: a URI that named its own session is
    /// not an allocation, and advancing on one would skip ids for no reason.
    pub fn advance(&mut self, session_id: i32) {
        self.next = session_id.wrapping_add(1);
    }

    /// How many ids one speculation can distinguish.
    ///
    /// The reference's bit set is `ipc_publications.length + 1` bits long, so
    /// it grows with the driver. This is a fixed window instead, and the two
    /// agree for every driver with fewer publications than this: the ids that
    /// collide are the ones this driver's own publications hold, and a set of
    /// that size always has a free bit.
    const MAX_SPECULATION: usize = 1024;
}

/// A publication whose log buffer is being created.
///
/// Everything the create needs that is not the log buffer itself, held while
/// the agent works.
#[derive(Debug)]
struct PendingPublication {
    identity: PublicationIdentity,
    params: PublicationParams,
    /// The log buffer's path, as the agent was given it. The reply carries
    /// the same path — it is the log buffer's own name, and the reference
    /// sends that rather than a copy it kept.
    path: PathBuf,
}

/// The publications a driver owns, and the thread that maps their log buffers.
#[derive(Debug)]
pub struct IpcPublications {
    publications: Vec<IpcPublication>,
    pending: Vec<PendingPublication>,
    session_ids: SessionIds,
    agent: NativeResourceAgent,
}

impl IpcPublications {
    /// Start the manager, and the thread that will create log buffers —
    /// asking `storage`'s filesystem first, when the checks are on.
    ///
    /// # Errors
    ///
    /// [`io::Error`] if the agent thread cannot be spawned.
    pub fn start(
        reserved_session_id_low: i32,
        reserved_session_id_high: i32,
        storage: StorageChecks,
    ) -> io::Result<Self> {
        Ok(Self {
            publications: Vec::new(),
            pending: Vec::new(),
            session_ids: SessionIds::start(reserved_session_id_low, reserved_session_id_high),
            agent: NativeResourceAgent::start(storage)?,
        })
    }

    /// The publications that exist, in creation order.
    pub fn publications(&self) -> &[IpcPublication] {
        &self.publications
    }

    /// The same, for a subscription attaching a reader to one of them: the
    /// subscription's `sub-pos` counter and the publication's reader set are
    /// two halves of one link (`crate::ipc_subscriptions`).
    pub fn publications_mut(&mut self) -> &mut [IpcPublication] {
        &mut self.publications
    }

    /// The publication a subscription reads, by the id its images name.
    pub fn find(&self, registration_id: i64) -> Option<&IpcPublication> {
        self.publications
            .iter()
            .find(|publication| publication.registration_id == registration_id)
    }

    /// How many publications are being built.
    pub fn pending(&self) -> usize {
        self.pending.len()
    }

    /// The session id cursor, for a report.
    pub const fn session_ids(&self) -> SessionIds {
        self.session_ids
    }

    /// Serve an `ADD_PUBLICATION` or `ADD_EXCLUSIVE_PUBLICATION`.
    ///
    /// Nothing is sent from here unless the publication already exists: a new
    /// one is announced by [`IpcPublications::poll`] when its log buffer lands.
    ///
    /// # Errors
    ///
    /// [`AddError`] for everything the reference refuses, in the order it
    /// refuses it.
    #[allow(clippy::too_many_arguments)] // one per collaborator, not one per decision
    pub fn add_publication(
        &mut self,
        request: &AddPublicationCommand<'_>,
        is_exclusive: bool,
        config: &DriverConfig,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        clients: &mut Clients,
        subscriptions: &mut IpcSubscriptions,
        now: Now,
        events: &mut impl ClientEvents,
    ) -> Result<(), AddError> {
        // VALIDATE (`:3963-3981`).
        let uri = ChannelUri::parse(request.channel)?;
        if uri.transport() != Transport::Ipc {
            return Err(AddError::UnsupportedTransport);
        }
        let params = PublicationParams::resolve(&uri, config)?;

        // The client is registered before anything else happens for this
        // command — the reference's `get_or_add_client` is the first thing the
        // link does (`:3733-3739`) — and it is what keeps a slow log buffer
        // from looking like a dead client: registering it starts the heartbeat
        // that the timeout tier reads.
        let Some(client) = clients.get_or_add(
            request.client_id,
            now.ms,
            now.client_liveness_timeout_ns,
            counters,
            regions,
            events,
        ) else {
            return Err(AddError::NoClientRecord);
        };

        let registration_id = request.correlation_id;
        let stream_id = request.stream_id;

        // RESOLVE_PUBLICATION (`:3989-4008`): a concurrent publication on the
        // same stream in the same response scope may be shared, and then no log
        // buffer is created and no counter is allocated.
        if !is_exclusive {
            if let Some(index) = self.find_shared(stream_id, params.response_correlation_id) {
                self.publications[index]
                    .can_be_shared_with(&params)
                    .map_err(AddError::Share)?;

                self.link(index, registration_id, is_exclusive, client, events);
                link_subscriptions(self, index, subscriptions, counters, regions, now, events);

                return Ok(());
            }
        }

        // The clash check runs *after* the shared lookup: an exclusive or
        // draining publication is not shareable, so a session id one of those
        // holds is a clash rather than a sharing (`:4011-4015`).
        if let Some(session_id) = params.session_id {
            if self.find_session_clash(stream_id, session_id) {
                return Err(AddError::SessionClash {
                    session_id,
                    stream_id,
                });
            }
        }

        // The log buffer's name, then AWAIT_LOG_BUFFER (`:4017-4047`).
        let path = publication_path(&config.aeron_dir, registration_id);

        self.agent
            .map_log_buffer(
                &path,
                params.term_length,
                config.layout.page_size,
                params.is_sparse,
            )
            .map_err(|_| AddError::AgentStopped)?;

        self.pending.push(PendingPublication {
            identity: PublicationIdentity {
                registration_id,
                client_id: request.client_id,
                // Filled in at create: the session id is speculated from the
                // publications that exist *then*, which is what the reference
                // does too — its create is a later pass than this.
                session_id: 0,
                stream_id,
                channel: request.channel.to_vec(),
                is_exclusive,
            },
            params,
            path,
        });

        Ok(())
    }

    /// Write `pub-pos` and recompute `pub-lmt` for every publication
    /// (`aeron_ipc_publication_update_pub_pos_and_lmt`,
    /// `aeron-driver/src/main/c/aeron_ipc_publication.c:278-328`), and return
    /// how many of them did work.
    pub fn update_limits(
        &mut self,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
    ) -> usize {
        let mut worked = 0;

        for publication in &mut self.publications {
            if publication.update_pub_pos_and_lmt(counters, regions) {
                worked += 1;
            }
        }

        worked
    }

    /// What the untethered machine's three outcomes become for a client
    /// (`aeron_ipc_publication.c:396-402`, `:435-448`, `:421-425`).
    ///
    /// The same three messages a *network* reader gets from its image, and the
    /// same shapes: an unavailable image naming the subscription, an available
    /// one carrying the publication's own log file, and a counter going back
    /// to the manager. The channel is the **constant** `aeron:ipc`, not the
    /// one the client subscribed with — the reference sends the constant here
    /// as it does when a publication drains, because the reader is holding a
    /// mapping of a log buffer rather than a description of a channel.
    fn on_untethered(
        &mut self,
        index: usize,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        events: &mut impl ClientEvents,
        now_ns: i64,
        now_ms: i64,
    ) -> usize {
        let publication = &mut self.publications[index];
        let registration_id = publication.registration_id;
        let stream_id = publication.stream_id;
        let session_id = publication.session_id;

        let moved = publication.check_untethered_subscriptions(counters, regions, now_ns);
        let mut work = 0;

        for event in &moved {
            work += 1;

            match *event {
                UntetheredEvent::Unavailable {
                    subscription_registration_id,
                    ..
                } => {
                    events.unavailable_image(
                        registration_id,
                        subscription_registration_id,
                        stream_id,
                        IPC_CHANNEL,
                    );
                }
                UntetheredEvent::Available {
                    subscription_registration_id,
                    counter_id,
                    ..
                } => {
                    // The same message `link_subscribable` sends a reader that
                    // has just arrived, down to the source identity
                    // (`ipc_subscriptions.rs:950-958`): a woken reader cannot
                    // tell the difference, which is the point of waking it this
                    // way rather than inventing a second message.
                    events.available_image(&ImageBuffersReady {
                        correlation_id: registration_id,
                        session_id,
                        stream_id,
                        subscriber_registration_id: subscription_registration_id,
                        subscriber_position_id: counter_id,
                        log_file: publication.path_bytes(),
                        source_identity: IPC_CHANNEL,
                    });
                }
                UntetheredEvent::Closed { counter_id } => {
                    counters.free(regions, counter_id, now_ms);
                }
            }
        }

        work
    }

    /// Take everything the agent finished since the last call and create the
    /// publications whose log buffers have landed.
    ///
    /// Returns the work done, which is what the conductor's cycle accounting
    /// wants. Nothing here fails the pass: a log buffer that could not be made
    /// is an `ON_ERROR` to the client that asked for it, which is what the
    /// reference's own completion handler does.
    #[allow(clippy::too_many_arguments)] // one per collaborator
    pub fn poll(
        &mut self,
        config: &DriverConfig,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        clients: &mut Clients,
        subscriptions: &mut IpcSubscriptions,
        now: Now,
        events: &mut impl ClientEvents,
    ) -> usize {
        let completions = self.agent.poll();
        let mut work = 0;

        for completion in completions {
            work += 1;

            match completion {
                Completion::Mapped { path, log } => {
                    let Some(index) = self.pending.iter().position(|entry| entry.path == path)
                    else {
                        // The agent only answers requests, so this cannot
                        // happen; dropping the mapping (which is what this
                        // does) is still better than leaking it, and the
                        // removal is the agent's job.
                        let _ = self.agent.free_log_buffer(*log);
                        continue;
                    };
                    let mut pending = self.pending.swap_remove(index);

                    self.create_publication(
                        &mut pending,
                        *log,
                        config,
                        counters,
                        regions,
                        clients,
                        subscriptions,
                        now,
                        events,
                    );
                }
                Completion::MapFailed { path, error } => {
                    let Some(index) = self.pending.iter().position(|entry| entry.path == path)
                    else {
                        continue;
                    };
                    let pending = self.pending.swap_remove(index);

                    events.error(
                        pending.identity.registration_id,
                        storage_space_or_generic(&error),
                        format!("could not create the log buffer: {error}").as_bytes(),
                    );
                }
                Completion::Freed { .. } => {}
            }
        }

        work
    }

    /// Every storage warning the agent raised since the last call. Not this
    /// pool's to answer — a warning never stopped a create — so they pass
    /// straight through to the conductor, which owns the log they go to.
    pub fn poll_storage_warnings(&self) -> Vec<StorageWarning> {
        self.agent.poll_warnings()
    }

    /// Let go of the publications a client held, as its death or its
    /// `REMOVE_PUBLICATION` requires (`aeron_client_delete`, `:1218-1226`, and
    /// `aeron_driver_conductor_on_remove_publication`, `:4705-4735`).
    ///
    /// Every link is released, and a publication whose last link that was
    /// starts draining: its limit is pulled back, its log says where the stream
    /// ended, and its readers will find it drained on the next timeout tier.
    /// Nothing is closed here — a draining publication is still readable, which
    /// is the whole point of the state.
    pub fn release_links(
        &mut self,
        links: &[crate::clients::PublicationLink],
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
    ) {
        for link in links {
            let Some(publication) = self.publications.iter_mut().find(|publication| {
                publication.registration_id == link.publication_registration_id
            }) else {
                continue;
            };

            publication.release(counters, regions);
        }
    }

    /// The timeout tier's turn for every publication
    /// (`aeron_driver_conductor_on_check_managed_resources`, `:1691-1712`):
    /// advance the ones on their way out, and remove the ones that are done.
    ///
    /// Removing means: tell the subscriptions to forget it, give its readers'
    /// positions and its own two counters back, hand the log buffer to the
    /// agent, and drop it from the list — the reference's
    /// `aeron_ipc_publication_entry_delete` (`:1428-1446`) followed by
    /// `aeron_ipc_publication_close` (`aeron_ipc_publication.c:196-214`).
    ///
    /// Returns the work done, for the cycle counter.
    pub fn on_time_event(
        &mut self,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        subscriptions: &mut IpcSubscriptions,
        events: &mut impl ClientEvents,
        now_ns: i64,
        now_ms: i64,
    ) -> usize {
        let mut work = 0;

        for index in 0..self.publications.len() {
            // The revoke is the active state's only duty here: a publication
            // whose client asked for `REMOVE_PUBLICATION` with the revoke flag
            // cuts its stream off at the next timeout tier, and tells its
            // readers (`aeron_ipc_publication.c:492-520`).
            if self.publications[index].state() == State::Active
                && self.publications[index].is_revoked()
            {
                let publication = &mut self.publications[index];
                let registration_id = publication.registration_id;
                let stream_id = publication.stream_id;

                work += usize::from(publication.revoke(counters, regions));

                for link in subscriptions.readers_of(registration_id) {
                    events.unavailable_image(
                        registration_id,
                        link.registration_id,
                        stream_id,
                        IPC_CHANNEL,
                    );
                }

                crate::system_counters::increment(
                    counters,
                    regions,
                    crate::system_counters::id::PUBLICATIONS_REVOKED,
                );

                continue;
            }

            // The reference's switch is exclusive: a publication revoked in
            // this pass has *become* lingering, and its lingering case runs on
            // the next turn — which is what makes a revoked publication
            // readable for one more tier than a removed one.
            let before = self.publications[index].state();

            // A reader that has stopped reading is put aside, woken or closed
            // (`aeron_ipc_publication.c:530-534`): the actives' first duty, and
            // one the revoke above skips — a publication on its way out is
            // already telling its readers so.
            if before == State::Active {
                work += self.on_untethered(index, counters, regions, events, now_ns, now_ms);

                // The reference's next line, on the same tier
                // (`aeron_ipc_publication.c:533-534`): a reader the machine
                // just took out of the working count is one the producer can
                // no longer see reading.
                self.publications[index].update_connected_status();
            }

            work += usize::from(self.publications[index].on_time_event(counters, regions, now_ns));

            // The moment a publication finishes draining, its readers are told
            // the image is gone: they hold a mapping of a log buffer that is
            // about to be deleted (`aeron_ipc_publication.c:561-577` sends one
            // message per linked subscription, naming the *constant* channel
            // rather than the one the client subscribed with).
            if before == State::Draining && self.publications[index].state() == State::Linger {
                let registration_id = self.publications[index].registration_id;
                let stream_id = self.publications[index].stream_id;

                for link in subscriptions.readers_of(registration_id) {
                    events.unavailable_image(
                        registration_id,
                        link.registration_id,
                        stream_id,
                        IPC_CHANNEL,
                    );
                }
            }
        }

        // And the ones that have reached the end of their life go.
        let mut index = self.publications.len();
        while index > 0 {
            index -= 1;

            if !self.publications[index].has_reached_end_of_life() {
                continue;
            }

            // The readers were told when the publication started draining —
            // this is only forgetting, which is what the reference's
            // `unlink_subscribable` does on its way past.
            let publication = self.publications.swap_remove(index);
            subscriptions.forget_publication(publication.registration_id);

            for reader in publication.subscribers.positions() {
                counters.free(regions, reader.counter_id, now_ms);
            }
            counters.free(regions, publication.pub_lmt_counter_id, now_ms);
            counters.free(regions, publication.pub_pos_counter_id, now_ms);

            let _ = self.agent.free_log_buffer(*publication.into_log());
            work += 1;
        }

        work
    }

    /// Give every publication's log buffer back, close the publications, and
    /// stop the agent.
    ///
    /// The counters go back first and the files after, which is the
    /// reference's order at shutdown (`aeron_driver_conductor.c:3427-3436`:
    /// `aeron_ipc_publication_close` then
    /// `aeron_driver_conductor_delete_log_buffer`).
    pub fn close(
        &mut self,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        now_ms: i64,
    ) {
        for publication in std::mem::take(&mut self.publications) {
            for subscriber in publication.subscribers.positions() {
                counters.free(regions, subscriber.counter_id, now_ms);
            }
            counters.free(regions, publication.pub_lmt_counter_id, now_ms);
            counters.free(regions, publication.pub_pos_counter_id, now_ms);

            let _ = self.agent.free_log_buffer(*publication.into_log());
        }

        // A log buffer still being created when the driver stops is one the
        // agent finishes and nobody collects: `shutdown` waits for the queue
        // to drain, so the file exists and is removed by nothing. The
        // reference has the same shape — its commands are freed without their
        // mappings being deleted (`:3415-3425`) — and a restarted driver
        // removes a stale log buffer by name.
        self.pending.clear();
        self.agent.shutdown();
    }

    /// The create: speculate a session id, allocate the counters, write the
    /// metadata, link the client and answer it (`:3786-3911`, `:3729-3767`).
    #[allow(clippy::too_many_arguments)] // one per collaborator
    fn create_publication(
        &mut self,
        pending: &mut PendingPublication,
        log: LogFile,
        config: &DriverConfig,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        clients: &mut Clients,
        subscriptions: &mut IpcSubscriptions,
        now: Now,
        events: &mut impl ClientEvents,
    ) {
        let stream_id = pending.identity.stream_id;

        // The session id: what the URI asked for, or the first one this stream
        // is not already running under. Only a speculated id moves the cursor
        // (`:3827-3832`).
        let session_id = match pending.params.session_id {
            Some(session_id) => session_id,
            None => {
                let speculated = self.session_ids.speculate(stream_id, self.used_sessions());
                self.session_ids.advance(speculated);
                speculated
            }
        };
        pending.identity.session_id = session_id;

        // The two counters, allocated before anything can see the publication
        // so that a client which reads the reply finds them.
        let Some(pub_pos_counter_id) = counter_position::allocate_publisher_position(
            counters,
            regions,
            pending.identity.client_id,
            pending.identity.registration_id,
            session_id,
            stream_id,
            &pending.identity.channel,
            pending.identity.is_exclusive,
            now.ms,
        ) else {
            self.failed(
                pending,
                log,
                "failed to allocate publisher counters",
                events,
            );
            return;
        };
        let Some(pub_lmt_counter_id) = counter_position::allocate_publisher_limit(
            counters,
            regions,
            pending.identity.client_id,
            pending.identity.registration_id,
            session_id,
            stream_id,
            &pending.identity.channel,
            now.ms,
        ) else {
            counters.free(regions, pub_pos_counter_id, now.ms);
            self.failed(
                pending,
                log,
                "failed to allocate publisher counters",
                events,
            );
            return;
        };

        // A publication resuming a stream starts both counters at the position
        // it resumes from (`:3860-3870`); one that is not resuming leaves them
        // at zero, which is where the counter allocator leaves a fresh counter.
        if let Err(message) = start_counters_at_position(
            pending,
            counters,
            regions,
            pub_pos_counter_id,
            pub_lmt_counter_id,
        ) {
            counters.free(regions, pub_lmt_counter_id, now.ms);
            counters.free(regions, pub_pos_counter_id, now.ms);
            self.failed(pending, log, message, events);
            return;
        }

        // The metadata's page size is an `i32` on the wire and a `usize` in the
        // configuration; a page size that does not fit one is not a page size.
        #[allow(clippy::cast_possible_truncation)]
        let page_size = config.layout.page_size as i32;

        let publication = match IpcPublication::create(
            Box::new(log),
            pending.identity.clone(),
            &pending.params,
            page_size,
            config.socket_buffers,
            pub_pos_counter_id,
            pub_lmt_counter_id,
        ) {
            Ok(publication) => publication,
            Err(log) => {
                counters.free(regions, pub_lmt_counter_id, now.ms);
                counters.free(regions, pub_pos_counter_id, now.ms);
                self.failed(
                    pending,
                    *log,
                    "the log buffer could not be initialised",
                    events,
                );
                return;
            }
        };

        self.publications.push(publication);

        // CREATE_PUBLICATION ends in the link, and the client is looked up
        // again rather than carried from the request: it may have been
        // reclaimed while the log buffer was being built, and the reference
        // re-registers it here too (`:3733-3739`).
        let registration_id = pending.identity.registration_id;
        let is_exclusive = pending.identity.is_exclusive;

        let Some(client) = clients.get_or_add(
            pending.identity.client_id,
            now.ms,
            now.client_liveness_timeout_ns,
            counters,
            regions,
            events,
        ) else {
            // The publication exists and nobody can be told about it. It
            // stays: the client's removal or timeout collects it, and removing
            // it here would free counters a client may already have read.
            return;
        };

        self.link(
            self.publications.len() - 1,
            registration_id,
            is_exclusive,
            client,
            events,
        );

        link_subscriptions(
            self,
            self.publications.len() - 1,
            subscriptions,
            counters,
            regions,
            now,
            events,
        );
    }

    /// Tell a client that its publication could not be created, and remove the
    /// log buffer it would have had.
    fn failed(
        &self,
        pending: &PendingPublication,
        log: LogFile,
        message: &str,
        events: &mut impl ClientEvents,
    ) {
        events.error(
            pending.identity.registration_id,
            ERROR_CODE_GENERIC_ERROR,
            message.as_bytes(),
        );

        let _ = self.agent.free_log_buffer(log);
    }

    /// Link a client to a publication that exists, and tell it so.
    ///
    /// The order is the reference's (`:3744-3767`): the link is recorded and
    /// the publication's reference count goes up, then the client is answered.
    /// (Attaching the subscriptions that are already waiting for this stream is
    /// the next thing that happens, in `remove_publication`'s counterpart —
    /// P1-2's subscription half.)
    fn link(
        &mut self,
        index: usize,
        registration_id: i64,
        is_exclusive: bool,
        client: &mut ClientRecord,
        events: &mut impl ClientEvents,
    ) {
        let publication = &mut self.publications[index];
        publication.incref();

        client.publication_links.push(PublicationLink {
            registration_id,
            publication_registration_id: publication.registration_id,
        });

        // The reply is keyed by the *client's* registration id and names the
        // publication's: two ids, and only one of them is the log file's name.
        let log_file = publication.path_bytes();
        let ready = PublicationBuffersReady {
            correlation_id: registration_id,
            registration_id: publication.registration_id,
            session_id: publication.session_id,
            stream_id: publication.stream_id,
            position_limit_counter_id: publication.pub_lmt_counter_id,
            channel_status_indicator_id: CHANNEL_STATUS_INDICATOR_NOT_ALLOCATED,
            log_file,
        };

        events.publication_ready(&ready, is_exclusive);
    }

    /// The publications that count towards a session id speculation: the ones
    /// that are `ACTIVE` or `DRAINING` (`:3792-3802`).
    fn used_sessions(&self) -> Vec<(i32, i32)> {
        self.publications
            .iter()
            .filter(|publication| matches!(publication.state(), State::Active | State::Draining))
            .map(|publication| (publication.stream_id, publication.session_id))
            .collect()
    }

    /// The publication this request may share, if any
    /// (`aeron_driver_conductor_find_shared_ipc_publication`, `:1765-1785`).
    ///
    /// The state test is the reference's and looks redundant — the outer test
    /// admits `ACTIVE` or `DRAINING` and the inner one then requires `ACTIVE`.
    /// Kept as it is, because together the two are what says a *draining*
    /// publication is never shared: it is on its way out, and a client linked
    /// to it would be linked to a stream that is about to end.
    fn find_shared(&self, stream_id: i32, response_correlation_id: i64) -> Option<usize> {
        self.publications.iter().position(|publication| {
            publication.stream_id == stream_id
                && matches!(publication.state(), State::Active | State::Draining)
                && publication.state() == State::Active
                && !publication.is_exclusive
                && publication.response_correlation_id == response_correlation_id
        })
    }

    /// Whether an existing publication on this stream already runs under
    /// `session_id` (`aeron_driver_conductor_check_session_clash_ipc_publication`,
    /// `:3931-3952`).
    fn find_session_clash(&self, stream_id: i32, session_id: i32) -> bool {
        self.publications.iter().any(|publication| {
            publication.stream_id == stream_id
                && matches!(publication.state(), State::Active | State::Draining)
                && publication.session_id == session_id
        })
    }
}

/// Give a publication to the subscriptions that were waiting for it.
///
/// Split out of the two places that need it — a publication created now, and a
/// request linked to one that already existed — because the borrow is awkward
/// in both: the publication is inside `self` and the subscriptions are beside
/// it, so the call needs the manager's own field split off by hand.
/// The error code the reference composes for a log buffer that could not be
/// made (`aeron_driver_conductor.c:2326-2341`): an `ENOSPC` — the kernel's or
/// the pre-creation storage check's — becomes `STORAGE_SPACE`, and everything
/// else the generic code.
///
/// The reference's third branch, a *negative* errno standing in for a
/// protocol code, cannot arrive here: an `io::Error` from the file layer
/// carries the kernel's positive errnos or no errno at all.
fn storage_space_or_generic(error: &io::Error) -> i32 {
    if error.raw_os_error() == Some(libc::ENOSPC) {
        ERROR_CODE_STORAGE_SPACE
    } else {
        ERROR_CODE_GENERIC_ERROR
    }
}

fn link_subscriptions(
    publications: &mut IpcPublications,
    index: usize,
    subscriptions: &mut IpcSubscriptions,
    counters: &mut CounterManager,
    regions: &CounterRegions<'_>,
    now: Now,
    events: &mut impl ClientEvents,
) {
    let Some(publication) = publications.publications_mut().get_mut(index) else {
        return;
    };

    subscriptions.link_publication(publication, counters, regions, now, events);
}

/// Put both position counters on the position a resumed stream resumes from
/// (`aeron_driver_conductor.c:3860-3870`).
///
/// Returns the message to report when the position cannot be written, which is
/// a term length that is not a power of two — impossible here, because the
/// parameters were resolved from a term length that was checked, and an error
/// rather than a panic because "impossible" is what a `debug_assert` is for.
fn start_counters_at_position(
    pending: &PendingPublication,
    counters: &mut CounterManager,
    regions: &CounterRegions<'_>,
    pub_pos_counter_id: i32,
    pub_lmt_counter_id: i32,
) -> Result<(), &'static str> {
    let Some(starting) = pending.params.starting_position else {
        return Ok(());
    };

    let Some(bits_to_shift) = position::bits_to_shift(pending.params.term_length) else {
        return Err("the term length is not a power of two");
    };

    // Bounded by the term length, which the parameters checked.
    #[allow(clippy::cast_possible_truncation)]
    let term_offset = starting.term_offset as i32;

    let start = position::Position::new(
        starting.term_id,
        term_offset,
        bits_to_shift,
        pending.params.initial_term_id,
    )
    .raw();

    if counters
        .set_value(regions, pub_pos_counter_id, start)
        .is_none()
        || counters
            .set_value(regions, pub_lmt_counter_id, start)
            .is_none()
    {
        return Err("could not publish the starting position");
    }

    Ok(())
}

/// Where a publication's log buffer lives
/// (`aeron_ipc_publication_location`, `aeron-client/src/main/c/util/aeron_fileutil.c:1208-1214`):
/// `<aeron_dir>/publications/<registration_id>.logbuffer`.
pub fn publication_path(aeron_dir: &std::path::Path, registration_id: i64) -> PathBuf {
    aeron_dir
        .join(PUBLICATIONS_DIR)
        .join(format!("{registration_id}.logbuffer"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_out_of_space_log_buffer_keeps_its_own_error_code() {
        // The reference's composition (`aeron_driver_conductor.c:2326-2341`):
        // an `ENOSPC` — the kernel's or the pre-creation check's — arrives as
        // `STORAGE_SPACE`, and everything else as the generic code.
        assert_eq!(
            ERROR_CODE_STORAGE_SPACE,
            storage_space_or_generic(&io::Error::from_raw_os_error(libc::ENOSPC))
        );
        assert_eq!(
            ERROR_CODE_GENERIC_ERROR,
            storage_space_or_generic(&io::Error::from_raw_os_error(libc::EACCES))
        );
        assert_eq!(
            ERROR_CODE_GENERIC_ERROR,
            storage_space_or_generic(&io::Error::other("the agent has stopped"))
        );
    }

    #[test]
    fn the_path_is_the_registration_id_under_the_publications_directory() {
        let path = publication_path(std::path::Path::new("/tmp/aeron"), 42);

        assert_eq!(
            std::path::Path::new("/tmp/aeron/publications/42.logbuffer"),
            path
        );
    }

    #[test]
    fn the_session_cursor_steps_over_the_reserved_range() {
        // The reserved range is the reference's default, and the rule is that a
        // cursor *inside* it becomes one past its end — the ids in it are the
        // driver's own and are never handed out.
        let mut ids = SessionIds::start(-1, 1000);

        for reserved in [-1, 0, 500, 1000] {
            ids.next = reserved;
            assert_eq!(1001, ids.cursor(), "cursor at {reserved}");
        }

        // Outside the range the cursor is itself, on both sides of it.
        ids.next = 1001;
        assert_eq!(1001, ids.cursor());
        ids.next = -2;
        assert_eq!(-2, ids.cursor());
    }

    #[test]
    fn a_speculated_session_is_the_first_one_the_stream_is_not_using() {
        let mut ids = SessionIds::start(-1, 1000);
        ids.next = 1001;

        assert_eq!(1001, ids.speculate(7, []), "nothing in use");

        // The cursor's own id is in use on this stream, so the next one is
        // taken instead.
        assert_eq!(1002, ids.speculate(7, [(7, 1001)]));

        // A run of ids in use is skipped whole, and another stream's ids do
        // not count.
        assert_eq!(
            1004,
            ids.speculate(7, [(7, 1001), (7, 1002), (7, 1003), (9, 1004)])
        );

        // An id *behind* the cursor is not in the way: the reference's bit set
        // index is negative there and drops the bit.
        assert_eq!(1001, ids.speculate(7, [(7, 900)]));
    }

    #[test]
    fn only_a_speculated_id_moves_the_cursor() {
        // `update_next_session_id` is the reference's, and it is called only
        // when the URI did not name a session (`:3827-3832`).
        let mut ids = SessionIds::start(-1, 1000);
        ids.next = 1001;

        ids.advance(1005);

        assert_eq!(1006, ids.cursor());
    }

    #[test]
    fn a_speculation_always_answers_even_when_every_id_is_in_use() {
        // The reference's bit set is one longer than the number of
        // publications, which is what guarantees this; here it is a fixed
        // window, and the ids it covers are the ones this driver's own
        // publications hold.
        let ids = SessionIds::start(-1, 1000);
        let cursor = ids.cursor();
        let used: Vec<(i32, i32)> = (0..SessionIds::MAX_SPECULATION)
            .map(|offset| (7, cursor.wrapping_add(offset as i32)))
            .collect();

        let speculated = ids.speculate(7, used);

        assert_eq!(cursor, speculated, "falls back to the cursor's own id");
    }
}
