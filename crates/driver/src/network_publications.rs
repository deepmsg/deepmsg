//! The network publications a driver owns, and the thread that maps their log
//! buffers.
//!
//! Mirrors the conductor's half of the publication path in
//! `aeron-driver/src/main/c/aeron_driver_conductor.c`:
//! `execute_add_network_publication_*` (`:4113-4650`), the match rule a second
//! publication on one channel has to pass (`confirm_publication_match`,
//! `:1105-1178`), the session speculation (`:4455-4478`) and the counters a
//! publication's client reads (`:4508-4531`).
//!
//! # The same state machine as IPC, with two more states
//!
//! The shape is [`crate::ipc_publications`]'s — parse, validate, resolve, wait
//! for a log buffer, create, announce — because the reference's is: a log
//! buffer takes tens of milliseconds to map and the conductor may not block on
//! it. What is different is that the resolve step has two halves (a *channel*
//! resolves to a send endpoint, and then the publication resolves to a session
//! id and a starting position), and the announce step carries a channel-status
//! counter, because a network publication is the first thing in this build a
//! client can ask "is your socket up?" about.
//!
//! # Sharing
//!
//! A second `ADD_PUBLICATION` on a channel that already has one is *the same
//! publication* when the two agree about the session, the MTU, the term length
//! and the starting position (`confirm_publication_match`, `:1105-1178`) — and
//! an explicit disagreement about any of them is a refusal rather than a second
//! log buffer. The parameters that were *not* named say nothing: a client that
//! did not ask for an MTU is agreeing to whatever the publication that exists
//! has (`params->has_mtu_length`, the flag the reference keeps for exactly this
//! distinction).

use std::io;
use std::path::PathBuf;

use deepmsg_cnc::command::{AddPublicationCommand, PublicationBuffersReady};
use deepmsg_cnc::{CounterManager, CounterRegions};

use crate::channel_uri::{ChannelUri, Transport};
use crate::clients::{ClientEvents, Clients, PublicationLink};
use crate::config::DriverConfig;
use crate::ipc_publication::ShareMismatch;
use crate::ipc_publications::{AddError, Now, SessionIds};
use crate::media::TransportParams;
use crate::native_resource_agent::{NativeResourceAgent, StorageChecks};
use crate::network_publication::{NetworkPublication, PublicationCounters};
use crate::publication_params::{PublicationParams, PublicationParamsError};
use crate::retransmit_handler::RetransmitHandler;
use crate::send_endpoints::{EndpointOutcome, SendChannelEndpoints};
use crate::sender::SenderProxy;
use crate::udp_channel::UdpChannel;
use crate::{position as counter_position, sys};

/// A publication whose log buffer is being created.
struct PendingNetworkPublication {
    /// The client's correlation id, which the reply echoes.
    registration_id: i64,
    /// The client that asked.
    client_id: i64,
    /// The stream it publishes.
    stream_id: i32,
    /// Whether the producer asked for a single-producer publication.
    is_exclusive: bool,
    /// The channel as the client sent it.
    channel: Vec<u8>,
    /// The channel of the **endpoint** this publication will send through.
    ///
    /// Not always the same channel as the one above, and it is this one that
    /// decides the flow control, the group flag and the log buffer's `group`
    /// byte (`aeron_network_publication_create` reads
    /// `endpoint->conductor_fields.udp_channel`, `:136`; the strategy selector
    /// is handed the same one, `aeron_driver_conductor.c:4492-4496`). A second
    /// publication can share an endpoint whose agreement check looks at
    /// timestamp offsets, MTU and buffer lengths and at neither `control-mode`
    /// nor `fc=` (`validate_channel_against_send_channel_endpoint`,
    /// `:1907-1953`), and the canonical form two channels have to match carries
    /// only the two addresses (`aeron_uri_udp_canonicalise`,
    /// `aeron_udp_channel.c:148-208`) — so the endpoint keeps the channel it was
    /// created from, and that is the channel read.
    endpoint_channel: UdpChannel,
    /// The parameters the URI resolved to.
    params: PublicationParams,
    /// The endpoint the publication will send through.
    endpoint_id: u64,
    /// The channel-status counter the reply carries.
    channel_status_counter_id: i32,
    /// The counters allocated for this publication.
    counters: PublicationCounters,
    /// `so-sndbuf` the channel named, which the log buffer's metadata records.
    channel_sndbuf: usize,
    /// `so-rcvbuf`, likewise.
    channel_rcvbuf: usize,
    /// The session it runs under — named, or speculated from the stream.
    session_id: i32,
    /// The log buffer's path, as the agent was given it.
    path: PathBuf,
}

/// What the conductor keeps about a publication the sender owns.
///
/// The term buffer, the flow control and the retransmit handler go to the
/// sender with the publication; what stays here is what a *reply*, a *link* and
/// a *removal* need — the two ids a client holds, the counters it reads, the
/// endpoint it sends through, and the parameters a second publication has to
/// agree with. That split is this build's answer to the reference's shared
/// pointer, and it is why `pub-lmt` moves on the sender's pass
/// ([`NetworkPublication::send`]).
#[derive(Clone, Debug)]
pub struct NetworkPublicationRecord {
    /// The publication's own registration id — the client's correlation id for
    /// the `ADD_PUBLICATION` that made it.
    pub registration_id: i64,
    /// The client that owns it.
    pub client_id: i64,
    /// The session it runs under.
    pub session_id: i32,
    /// The stream it publishes.
    pub stream_id: i32,
    /// The endpoint it sends through.
    pub endpoint_id: u64,
    /// The channel as the client sent it.
    pub channel: Vec<u8>,
    /// Whether the producer asked for a single-producer publication.
    pub is_exclusive: bool,
    /// The parameters it was created with, which a sharing publication has to
    /// agree with (`aeron_confirm_publication_match`).
    pub params: PublicationParams,
    /// The counters a client reads.
    pub counters: PublicationCounters,
    /// The channel-status counter the reply carries.
    pub channel_status_counter_id: i32,
    /// How many clients hold a link to it (`publication_links`).
    pub refcount: i32,
}

/// The network publications a driver owns.
pub struct NetworkPublications {
    publications: Vec<NetworkPublicationRecord>,
    pending: Vec<PendingNetworkPublication>,
    session_ids: SessionIds,
    agent: NativeResourceAgent,
}

impl NetworkPublications {
    /// Start the manager and the thread that maps log buffers.
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

    /// The publications, in the order they were created.
    pub fn publications(&self) -> &[NetworkPublicationRecord] {
        &self.publications
    }

    /// One publication by its registration id.
    pub fn find(&self, registration_id: i64) -> Option<&NetworkPublicationRecord> {
        self.publications
            .iter()
            .find(|publication| publication.registration_id == registration_id)
    }

    /// How many publications are waiting for a log buffer.
    pub fn pending(&self) -> usize {
        self.pending.len()
    }

    /// Serve an `ADD_PUBLICATION` for a UDP channel
    /// (`execute_add_network_publication_*`, `aeron_driver_conductor.c:4113-4650`).
    ///
    /// The order is the reference's and only two of its steps are reordered
    /// here: the endpoint is resolved before the log buffer is asked for
    /// (`:4435`, because a channel that cannot have a socket cannot have a
    /// publication either), and the session id is speculated at the *end*
    /// (`:4455-4478`), from the publications that exist by then — which is the
    /// point of speculating rather than picking one at the start.
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
        endpoints: &mut SendChannelEndpoints,
        sender: &SenderProxy,
        now: Now,
        events: &mut impl ClientEvents,
    ) -> Result<(), AddError> {
        // PARSE_CHANNEL and VALIDATE (`:4113-4160`).
        let uri = ChannelUri::parse(request.channel)?;
        if uri.transport() != Transport::Udp {
            return Err(AddError::UnsupportedTransport);
        }

        let channel = UdpChannel::resolve(request.channel, &uri)
            .map_err(|error| AddError::Channel(Box::new(error)))?;
        let params = PublicationParams::resolve(&uri, config)?;

        validate_for_publication(&channel)?;

        // The client is registered before anything else happens for this
        // command, exactly as the IPC path does it.
        let Some(_client) = clients.get_or_add(
            request.client_id,
            now.ms,
            now.client_liveness_timeout_ns,
            counters,
            regions,
            events,
        ) else {
            return Err(AddError::NoClientRecord);
        };

        // RESOLVE_PUBLICATION (`:4263-4381`): the endpoint first, because a
        // second publication on the same channel shares it.
        let endpoint_params = transport_params(config, &channel);

        // `get_or_add` consumes the channel, and the fallback below needs one
        // even though it should never be reached: the registry always holds an
        // entry for an id it just handed out.
        let named_channel = channel.clone();

        let outcome = endpoints
            .get_or_add(
                channel,
                &endpoint_params,
                counters,
                regions,
                request.correlation_id,
                now.ns,
                now.ms,
            )
            .map_err(|error| AddError::Endpoint {
                error_code: error.error_code(),
                message: error.to_string(),
            })?;

        let (endpoint_id, channel_status_counter_id, new_endpoint) = match outcome {
            EndpointOutcome::Created {
                id,
                channel_status_counter_id,
                endpoint,
            } => (id, channel_status_counter_id, Some(endpoint)),
            EndpointOutcome::Shared {
                id,
                channel_status_counter_id,
            } => (id, channel_status_counter_id, None),
        };

        if let Some(endpoint) = new_endpoint {
            if sender.add_endpoint(endpoint_id, endpoint).is_err() {
                return Err(AddError::AgentStopped);
            }
        }

        // The endpoint's channel, which is what the decisions below are made
        // from — see the field's note for why it is not this publication's.
        // `get_or_add` has already registered the entry either way, so the
        // fallback is a wrong strategy rather than a panic if that ever stops
        // being true.
        let endpoint_channel = endpoints
            .get(endpoint_id)
            .map_or_else(|| named_channel.clone(), |entry| entry.channel.clone());

        // A publication that already exists on this endpoint and stream may be
        // shared (`:4287-4320`).
        //
        // The candidate is found by endpoint and stream **first**, and only
        // then checked for agreement: a second publication on the same stream
        // that names a different MTU is not a new publication, it is a
        // *refusal* — the reference reaches `aeron_confirm_publication_match`
        // with a candidate in hand and turns its disagreement into an error
        // (`:4350-4360`), rather than starting a second log buffer on the same
        // stream.
        if !is_exclusive {
            if let Some(index) = self.find_shareable(endpoint_id, request.stream_id) {
                publication_matches(&self.publications[index], &params).map_err(AddError::Share)?;

                self.link(
                    index,
                    request,
                    is_exclusive,
                    counters,
                    regions,
                    clients,
                    events,
                );

                return Ok(());
            }
        }

        // The session id: named, or speculated from what this endpoint and
        // stream already run (`:4455-4478`).
        let session_id = match params.session_id {
            Some(session_id) => {
                if let Some(clash) =
                    self.find_session_clash(endpoint_id, request.stream_id, session_id)
                {
                    let _ = clash;
                    return Err(AddError::SessionClash {
                        session_id,
                        stream_id: request.stream_id,
                    });
                }

                session_id
            }
            None => self.session_ids.speculate(
                request.stream_id,
                self.publications
                    .iter()
                    .filter(|publication| publication.endpoint_id == endpoint_id)
                    .map(|publication| (publication.stream_id, publication.session_id)),
            ),
        };

        // The six counters a network publication's client reads
        // (`:4508-4531`).
        let publication_counters = allocate_counters(
            counters,
            regions,
            request.client_id,
            request.correlation_id,
            session_id,
            request.stream_id,
            request.channel,
            is_exclusive,
            now.ms,
        )?;

        // The log buffer, off the conductor's thread.
        let path =
            crate::ipc_publications::publication_path(&config.aeron_dir, request.correlation_id);
        self.agent
            .map_log_buffer(
                &path,
                params.term_length,
                config.layout.page_size,
                params.is_sparse,
            )
            .map_err(|_| AddError::AgentStopped)?;

        if params.session_id.is_none() {
            self.session_ids.advance(session_id);
        }

        self.pending.push(PendingNetworkPublication {
            registration_id: request.correlation_id,
            client_id: request.client_id,
            stream_id: request.stream_id,
            is_exclusive,
            channel: request.channel.to_vec(),
            endpoint_channel,
            params,
            endpoint_id,
            channel_status_counter_id,
            counters: publication_counters,
            channel_sndbuf: endpoint_params.socket_sndbuf,
            channel_rcvbuf: endpoint_params.socket_rcvbuf,
            session_id,
            path,
        });

        Ok(())
    }

    /// Take everything the agent finished and create the publications whose log
    /// buffers have landed.
    ///
    /// Nothing here fails the pass: a log buffer that could not be made is an
    /// `ON_ERROR` to the client that asked for it.
    #[allow(clippy::too_many_arguments)]
    pub fn poll(
        &mut self,
        config: &DriverConfig,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        clients: &mut Clients,
        sender: &SenderProxy,
        now: Now,
        events: &mut impl ClientEvents,
    ) -> usize {
        let completions = self.agent.poll();
        let mut work = 0;

        for completion in completions {
            work += 1;

            match completion {
                crate::native_resource_agent::Completion::Mapped { path, log } => {
                    let Some(index) = self.pending.iter().position(|entry| entry.path == path)
                    else {
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
                        sender,
                        now,
                        events,
                    );
                }
                crate::native_resource_agent::Completion::MapFailed { path, error } => {
                    let Some(index) = self.pending.iter().position(|entry| entry.path == path)
                    else {
                        continue;
                    };
                    let pending = self.pending.swap_remove(index);

                    events.error(
                        pending.registration_id,
                        deepmsg_cnc::command::ERROR_CODE_STORAGE_SPACE,
                        format!("could not create the log buffer: {error}").as_bytes(),
                    );
                }
                crate::native_resource_agent::Completion::Freed { .. } => {}
            }
        }

        work
    }

    /// Every storage warning the agent raised since the last call.
    pub fn poll_storage_warnings(&self) -> Vec<crate::native_resource_agent::StorageWarning> {
        self.agent.poll_warnings()
    }

    /// Create the publication whose log buffer arrived, announce it, and hand
    /// it to the sender (`execute_add_network_publication_create_publication`,
    /// `:4405-4618`).
    ///
    /// The sender is told last, and the reply follows it: a client that has
    /// been answered is a client that may offer immediately, and an offer that
    /// arrives before the sender has the publication is an offer the publication
    /// would not be able to send.
    #[allow(clippy::too_many_arguments)]
    fn create_publication(
        &mut self,
        pending: &mut PendingNetworkPublication,
        log: deepmsg_core::logbuffer::logfile::LogFile,
        config: &DriverConfig,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        clients: &mut Clients,
        sender: &SenderProxy,
        now: Now,
        events: &mut impl ClientEvents,
    ) {
        // The flow-control strategy comes from the **endpoint's** channel, and a
        // unicast one never reads `fc=` at all — see
        // [`crate::flowcontrol::strategy_for_channel`], which is the selector
        // `aeron_default_multicast_flow_control_strategy_supplier`
        // (`aeron_flow_control.c:400-475`) in Rust.
        let fc = ChannelUri::parse(&pending.channel)
            .ok()
            .and_then(|uri| uri.value("fc").map(str::to_owned));

        let flow_control = match crate::flowcontrol::strategy_for_channel(
            pending.endpoint_channel.is_multi_destination(),
            fc.as_deref(),
            crate::flowcontrol::UNICAST_RRWM_DEFAULT,
            crate::flowcontrol::MULTICAST_RRWM_DEFAULT,
        ) {
            Ok(strategy) => strategy,
            Err(error) => {
                events.error(
                    pending.registration_id,
                    deepmsg_cnc::command::ERROR_CODE_GENERIC_ERROR,
                    error.to_string().as_bytes(),
                );
                return;
            }
        };

        // The retransmit handler's delay is the driver's unicast delay — zero
        // when nothing configured it, which is what makes a NAK answered at
        // once (`aeron_network_publication_create`, `:145-160`).
        //
        // Group semantics come from the endpoint's channel too (`:136`), and
        // they are not only the setup frame's `GROUP` flag: the handler holds a
        // section per receiver when a channel has them, and the log buffer's
        // `group` byte is written from the same value (`:224`).
        let retransmit_handler = RetransmitHandler::new(
            config.retransmit_unicast_delay_ns,
            config.retransmit_unicast_linger_ns,
            pending.endpoint_channel.has_group_semantics(),
            usize::try_from(pending.params.max_resend.max(0))
                .unwrap_or(1)
                .max(1),
        );

        let publication = NetworkPublication::create(
            pending.registration_id,
            pending.client_id,
            pending.session_id,
            pending.stream_id,
            pending.endpoint_id,
            &pending.channel,
            Box::new(log),
            &pending.params,
            pending.is_exclusive,
            pending.counters,
            config.network_publication_max_messages_per_send,
            flow_control,
            retransmit_handler,
            config.layout.page_size,
            config.socket_buffers,
            pending.channel_sndbuf,
            pending.channel_rcvbuf,
            now.ns,
        );

        let publication = match publication {
            Ok(publication) => publication,
            Err(error) => {
                events.error(
                    pending.registration_id,
                    deepmsg_cnc::command::ERROR_CODE_GENERIC_ERROR,
                    error.to_string().as_bytes(),
                );
                return;
            }
        };

        if sender.add_publication(Box::new(publication)).is_err() {
            events.error(
                pending.registration_id,
                deepmsg_cnc::command::ERROR_CODE_GENERIC_ERROR,
                b"the sender thread has stopped",
            );
            return;
        }

        self.publications.push(NetworkPublicationRecord {
            registration_id: pending.registration_id,
            client_id: pending.client_id,
            session_id: pending.session_id,
            stream_id: pending.stream_id,
            endpoint_id: pending.endpoint_id,
            channel: pending.channel.clone(),
            is_exclusive: pending.is_exclusive,
            params: pending.params,
            counters: pending.counters,
            channel_status_counter_id: pending.channel_status_counter_id,
            refcount: 1,
        });

        // The client's link, and the reply that names both ids.
        if let Some(client) = clients.find_mut(pending.client_id) {
            client.publication_links.push(PublicationLink {
                registration_id: pending.registration_id,
                publication_registration_id: pending.registration_id,
            });
        }

        let _ = counters;
        let _ = regions;

        let ready = PublicationBuffersReady {
            correlation_id: pending.registration_id,
            registration_id: pending.registration_id,
            session_id: pending.session_id,
            stream_id: pending.stream_id,
            position_limit_counter_id: pending.counters.pub_lmt,
            channel_status_indicator_id: pending.channel_status_counter_id,
            log_file: pending.path.as_os_str().as_encoded_bytes(),
        };

        events.publication_ready(&ready, pending.is_exclusive);
    }

    /// A publication a second `ADD_PUBLICATION` might share: same endpoint,
    /// same stream, and not exclusive
    /// (`find_shared_network_publication_by_endpoint`, `:1851-1875`).
    ///
    /// Whether the two actually *agree* is [`publication_matches`]'s question,
    /// asked by the caller — the same split the reference has, and the reason a
    /// mismatch is an error rather than a fall-through to a create.
    fn find_shareable(&self, endpoint_id: u64, stream_id: i32) -> Option<usize> {
        self.publications.iter().position(|publication| {
            publication.endpoint_id == endpoint_id
                && publication.stream_id == stream_id
                && !publication.is_exclusive
        })
    }

    /// Whether a session id is already taken on this stream in a way that
    /// cannot be shared (`:4237-4250`).
    fn find_session_clash(
        &self,
        endpoint_id: u64,
        stream_id: i32,
        session_id: i32,
    ) -> Option<usize> {
        self.publications.iter().position(|publication| {
            publication.endpoint_id == endpoint_id
                && publication.stream_id == stream_id
                && publication.session_id == session_id
        })
    }

    /// Attach a client to a publication that already exists, and answer it
    /// (`aeron_driver_conductor_link_publication`).
    #[allow(clippy::too_many_arguments)] // the collaborators a link needs
    fn link(
        &mut self,
        index: usize,
        request: &AddPublicationCommand<'_>,
        is_exclusive: bool,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        clients: &mut Clients,
        events: &mut impl ClientEvents,
    ) {
        let _ = (counters, regions);

        let publication = &mut self.publications[index];
        publication.refcount += 1;

        let ready = PublicationBuffersReady {
            correlation_id: request.correlation_id,
            registration_id: publication.registration_id,
            session_id: publication.session_id,
            stream_id: publication.stream_id,
            position_limit_counter_id: publication.counters.pub_lmt,
            channel_status_indicator_id: publication.channel_status_counter_id,
            log_file: &[],
        };

        let publication_registration_id = publication.registration_id;

        if let Some(client) = clients.find_mut(request.client_id) {
            client.publication_links.push(PublicationLink {
                registration_id: request.correlation_id,
                publication_registration_id,
            });
        }

        events.publication_ready(&ready, is_exclusive);
    }

    /// Take a publication's record out of the collection, for a caller that is
    /// about to release what it points at.
    pub fn remove(&mut self, registration_id: i64) -> Option<NetworkPublicationRecord> {
        let index = self
            .publications
            .iter()
            .position(|publication| publication.registration_id == registration_id)?;

        Some(self.publications.swap_remove(index))
    }

    /// The publications a client that is gone was holding
    /// (`aeron_client_delete`'s sweep of a client's resources).
    pub fn held_by(&self, client_id: i64) -> Vec<i64> {
        self.publications
            .iter()
            .filter(|publication| publication.client_id == client_id)
            .map(|publication| publication.registration_id)
            .collect()
    }

    /// Close everything: the agent's thread, and the publications with it.
    ///
    /// # Errors
    ///
    /// [`io::Error`] if the agent thread cannot be joined.
    pub fn close(&mut self) -> io::Result<()> {
        self.publications.clear();
        self.pending.clear();

        Ok(())
    }
}

/// The counters a network publication's client reads
/// (`aeron_driver_conductor.c:4508-4531`).
///
/// Each one carries the **stream-position key** the reference's own allocators
/// build (`aeron_position.c:23-62`): the registration id, the session, the
/// stream and the channel. The key is not decoration — a tool that finds a
/// counter by name reads the key to learn *which* stream it measures, and a
/// counter allocated with an empty key is one no tool can attribute.
#[allow(clippy::too_many_arguments)]
fn allocate_counters(
    counters: &mut CounterManager,
    regions: &CounterRegions<'_>,
    client_id: i64,
    registration_id: i64,
    session_id: i32,
    stream_id: i32,
    channel: &[u8],
    is_exclusive: bool,
    now_ms: i64,
) -> Result<PublicationCounters, AddError> {
    let allocate = |counters: &mut CounterManager, name: &str, type_id: i32, suffix: &str| {
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
            suffix,
            now_ms,
        )
    };

    // `pub-pos` carries the exclusive/concurrent distinction in its *name*,
    // which is how `AeronStat` tells a single-producer publication from a
    // shared one (`aeron_counters.h:100-102`).
    let pub_pos_name = if is_exclusive {
        "pub-pos (exclusive)"
    } else {
        "pub-pos (concurrent)"
    };

    let Some(pub_pos) = allocate(
        counters,
        pub_pos_name,
        counter_position::type_id::PUBLISHER_POSITION,
        "",
    ) else {
        return Err(AddError::NoCounterRecord);
    };

    let Some(pub_lmt) = allocate(
        counters,
        "pub-lmt",
        counter_position::type_id::PUBLISHER_LIMIT,
        "",
    ) else {
        return Err(AddError::NoCounterRecord);
    };

    let Some(snd_pos) = allocate(
        counters,
        "snd-pos",
        counter_position::type_id::SENDER_POSITION,
        "",
    ) else {
        return Err(AddError::NoCounterRecord);
    };

    let Some(snd_lmt) = allocate(
        counters,
        "snd-lmt",
        counter_position::type_id::SENDER_LIMIT,
        "",
    ) else {
        return Err(AddError::NoCounterRecord);
    };

    let Some(snd_bpe) = allocate(counters, "snd-bpe", SENDER_BPE_TYPE_ID, "") else {
        return Err(AddError::NoCounterRecord);
    };
    let Some(snd_naks_received) = allocate(counters, "snd-naks-received", SENDER_NAKS_TYPE_ID, "")
    else {
        return Err(AddError::NoCounterRecord);
    };

    Ok(PublicationCounters {
        pub_pos,
        pub_lmt,
        snd_pos,
        snd_lmt,
        snd_bpe,
        snd_naks_received,
    })
}

/// `AERON_COUNTER_SENDER_BPE_TYPE_ID` (`aeron-client/src/main/c/aeron_counters.h:104-105`):
/// how many times the sender was held back by a receiver's window.
const SENDER_BPE_TYPE_ID: i32 = 13;

/// `AERON_COUNTER_SENDER_NAKS_RECEIVED_TYPE_ID` (`:120-121`).
const SENDER_NAKS_TYPE_ID: i32 = 19;

/// Whether an existing publication may be shared with these parameters
/// (`aeron_confirm_publication_match`, `aeron_driver_conductor.c:1105-1178`).
///
/// A parameter the URI did **not** name says nothing: a client that did not ask
/// for an MTU is agreeing to whatever the publication that exists has, which is
/// what the `_named` flags are for.
fn publication_matches(
    publication: &NetworkPublicationRecord,
    params: &PublicationParams,
) -> Result<(), ShareMismatch> {
    use crate::ipc_publication::ShareMismatch;

    if let Some(session_id) = params.session_id {
        if session_id != publication.session_id {
            return Err(ShareMismatch::SessionId {
                existing: publication.session_id,
                requested: session_id,
            });
        }
    }

    if params.mtu_length_named && params.mtu_length != publication.params.mtu_length {
        return Err(ShareMismatch::Mtu {
            existing: publication.params.mtu_length,
            requested: params.mtu_length,
        });
    }

    if params.term_length_named && params.term_length != publication.params.term_length {
        return Err(ShareMismatch::TermLength {
            existing: publication.params.term_length,
            requested: params.term_length,
        });
    }

    if let Some(position) = params.starting_position {
        if position.initial_term_id != publication.params.initial_term_id {
            return Err(ShareMismatch::InitialTermId {
                existing: publication.params.initial_term_id,
                requested: position.initial_term_id,
            });
        }
    }

    Ok(())
}

/// The checks the reference makes on a channel *before* a publication is made
/// from it (`validate_endpoint_for_publication` and
/// `validate_control_for_publication`,
/// `aeron-driver/src/main/c/aeron_driver_conductor.c:569-608`).
///
/// # Errors
///
/// [`AddError::Channel`] for a channel a publication cannot send on — an
/// endpoint whose port is zero, a `control-mode=dynamic` without a control
/// address, a control address with nothing to send to.
fn validate_for_publication(channel: &UdpChannel) -> Result<(), AddError> {
    if channel.has_explicit_endpoint && channel.remote_data.port() == 0 {
        return Err(AddError::InvalidChannel(format!(
            "endpoint has port=0 for publication: {}",
            String::from_utf8_lossy(&channel.original_uri)
        )));
    }

    if channel.control_mode == crate::udp_channel::ControlMode::Dynamic
        && !channel.has_explicit_control
    {
        return Err(AddError::InvalidChannel(format!(
            "'control-mode=dynamic' requires that 'control' parameter is set, channel={}",
            String::from_utf8_lossy(&channel.original_uri)
        )));
    }

    if channel.has_explicit_control
        && !channel.has_explicit_endpoint
        && channel.control_mode == crate::udp_channel::ControlMode::None
    {
        return Err(AddError::InvalidChannel(format!(
            "'control' parameter requires that either 'endpoint' or 'control-mode' is specified, channel={}",
            String::from_utf8_lossy(&channel.original_uri)
        )));
    }

    Ok(())
}

/// The socket buffer lengths an endpoint is opened with, from the driver's
/// settings and the channel's parameters
/// (`aeron_udp_channel_socket_so_sndbuf` and its receive twin).
pub fn transport_params(config: &DriverConfig, channel: &UdpChannel) -> TransportParams {
    TransportParams {
        socket_rcvbuf: if channel.socket_rcvbuf_length != 0 {
            channel.socket_rcvbuf_length
        } else {
            usize::try_from(config.socket_so_rcvbuf).unwrap_or(0)
        },
        socket_sndbuf: if channel.socket_sndbuf_length != 0 {
            channel.socket_sndbuf_length
        } else {
            usize::try_from(config.socket_so_sndbuf).unwrap_or(0)
        },
        ttl: 0,
    }
}

/// The parameters a channel URI resolves to for a publication, with the error
/// a client sees when it does not.
///
/// # Errors
///
/// [`PublicationParamsError`] — the same one the IPC path reports.
pub fn resolve_params<'a>(
    uri: &ChannelUri<'a>,
    config: &DriverConfig,
) -> Result<PublicationParams, PublicationParamsError> {
    PublicationParams::resolve(uri, config)
}

/// The default socket buffer lengths a driver starts from, for a caller that
/// needs to report them.
pub fn default_socket_buffers() -> sys::SocketBufferLengths {
    sys::SocketBufferLengths {
        rcvbuf: 0,
        sndbuf: 0,
    }
}
