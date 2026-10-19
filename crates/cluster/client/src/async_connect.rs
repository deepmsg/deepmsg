//! Establishing a cluster session, as a state machine
//! (`AeronCluster.AsyncConnect`, `AC:2116-2660`).
//!
//! Seven steps, and a poll that is never waited on: each turn asks one question
//! — has the driver answered the egress add, has the ingress publication met
//! its subscriber, has an answer come back — and moves on exactly one step when
//! it has. A caller drives it a turn at a time; nothing here blocks, and
//! nothing here drives the [`Client`] (see [`crate::AeronCluster::connect`]
//! for the loop that does).
//!
//! # One deadline, taken once
//!
//! [`AsyncConnect::new`] takes the clock **once** and adds the context's
//! `message_timeout_ns`; every later poll compares against that fixed instant
//! (`AC:2198-2205`, `:2320-2351`). A deadline recomputed per state would be a
//! deadline that never arrives — each state would grant the timeout again, and
//! a connect that is being ignored would wait for ever rather than for five
//! seconds.
//!
//! # One step per poll
//!
//! Unlike the archive's connect, the reference's `AsyncConnect.poll` runs
//! **one** state and returns (`AC:2288-2317`), and its tests count polls to
//! reach each state. That is copied rather than "improved": a connect that
//! advanced several steps in one turn would make the state a caller observes
//! depend on how fast the driver answered.
//!
//! # The member ingress
//!
//! When `ingressEndpoints` names several members, the connect adds a
//! publication to each of them and sends its request over **the first that
//! connects** (`AC:2419-2450`). A redirect (`SessionEvent.REDIRECT`) replaces
//! the member table with the one the event names and tries again
//! (`AC:2543-2570`); a member whose add failed is remembered with its failure
//! and does not stop the others (`AC:2637-2646`).
//!
//! # Where the resources go
//!
//! A failed or abandoned connect gives back what it drew ([`AsyncConnect::close`]);
//! a successful one hands the publication, the subscription and the identity of
//! the session to [`crate::AeronCluster`], which owns them from then on.

use std::time::{Duration, Instant};

use deepmsg_client::client::{AsyncAdd, AsyncAddPoll, Client, CommandError};
use deepmsg_codec::cluster::WriteBuf;
use deepmsg_codec::cluster::challenge_response_codec::ChallengeResponseEncoder;
use deepmsg_codec::cluster::message_header_codec;
use deepmsg_codec::cluster::session_connect_request_codec::SessionConnectRequestEncoder;
use deepmsg_core::logbuffer::append::Appended;
use deepmsg_core::uri::ChannelUri;
use deepmsg_core::version::{BUILD_IDENTITY, COMPAT_VERSION_TEXT};

use crate::client_ops::ClientOps;
use crate::cluster::{AeronCluster, IngressPublication};
use crate::context::{AeronClusterContext, PROTOCOL_SEMANTIC_VERSION};
use crate::egress_poller::EgressPoller;
use crate::error::ClusterClientError;

/// `FRAGMENT_LIMIT` (`AC:103`).
const FRAGMENT_LIMIT: usize = 10;

/// `ConsensusModule.Configuration.LEADER_HEARTBEAT_TIMEOUT_DEFAULT_NS`
/// (`ConsensusModule.java:751`): ten seconds.
const LEADER_HEARTBEAT_TIMEOUT_DEFAULT_NS: i64 = 10 * 1_000 * 1_000 * 1_000;

/// `Aeron.NULL_VALUE` as an `i32` — the reference stores `leaderMemberId`
/// through an `int` and its null is -1.
const NULL_MEMBER_ID: i32 = -1;

/// Where the connect is (`AC:2121-2168`). The numbers are the reference's
/// `State.step`, which its tests and logs compare by.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnectState {
    /// Create the egress subscription.
    CreateEgressSubscription = -1,
    /// Create the ingress publication(s).
    CreateIngressPublications = 0,
    /// Await an ingress publication connected to a subscriber.
    AwaitPublicationConnected = 1,
    /// Send the connect request (or a challenge response).
    SendMessage = 2,
    /// Poll the egress subscription for the answer.
    PollResponse = 3,
    /// Assemble the session.
    ConcludeConnect = 4,
    /// Done.
    Done = 5,
}

impl ConnectState {
    /// The reference's `step` (`AC:2262-2267`).
    #[must_use]
    pub const fn step(self) -> i32 {
        self as i32
    }

    /// The reference's state name.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::CreateEgressSubscription => "CREATE_EGRESS_SUBSCRIPTION",
            Self::CreateIngressPublications => "CREATE_INGRESS_PUBLICATIONS",
            Self::AwaitPublicationConnected => "AWAIT_PUBLICATION_CONNECTED",
            Self::SendMessage => "SEND_MESSAGE",
            Self::PollResponse => "POLL_RESPONSE",
            Self::ConcludeConnect => "CONCLUDE_CONNECT",
            Self::Done => "DONE",
        }
    }

    /// `State.fromStep` (`AC:2161-2168`).
    #[must_use]
    pub const fn from_step(step: i32) -> Option<Self> {
        match step {
            -1 => Some(Self::CreateEgressSubscription),
            0 => Some(Self::CreateIngressPublications),
            1 => Some(Self::AwaitPublicationConnected),
            2 => Some(Self::SendMessage),
            3 => Some(Self::PollResponse),
            4 => Some(Self::ConcludeConnect),
            5 => Some(Self::Done),
            _ => None,
        }
    }
}

/// What a poll answered.
#[derive(Debug)]
pub enum Polled {
    /// The connect is not over. Drive the client a turn and come back.
    Awaiting,
    /// The connect is over, and this is the session it established.
    ///
    /// Boxed because the other arm carries nothing and a connect returns this
    /// on every turn.
    Done(Box<AeronCluster>),
}

/// One member's ingress, while a connect is choosing between them
/// (`MemberIngress`, `AC:2601-2671`).
pub(crate) struct MemberIngress {
    member_id: i32,
    endpoint: String,
    /// The add that draws this member's publication, until it lands.
    add: Option<AsyncAdd>,
    /// The publication, once the driver has made it.
    publication: Option<i64>,
    /// Why this member's add failed for good (`publicationException`,
    /// `AC:2607`). It does not stop the other members.
    exception: Option<String>,
}

impl MemberIngress {
    const fn new(member_id: i32, endpoint: String) -> Self {
        Self {
            member_id,
            endpoint,
            add: None,
            publication: None,
            exception: None,
        }
    }

    /// The member's id.
    #[must_use]
    pub(crate) const fn member_id(&self) -> i32 {
        self.member_id
    }
}

/// A connect in progress.
pub struct AsyncConnect {
    context: AeronClusterContext,
    state: ConnectState,
    /// **Taken once**, in [`AsyncConnect::new`] (`AC:2198-2205`).
    deadline: Instant,

    correlation_id: Option<i64>,
    cluster_session_id: Option<i64>,
    leadership_term_id: Option<i64>,
    leader_member_id: Option<i32>,
    egress_image: Option<i64>,
    leader_heartbeat_timeout_ns: Option<i64>,

    /// The egress subscription's add, until it lands.
    egress_add: Option<AsyncAdd>,
    egress_subscription: Option<i64>,
    egress_poller: Option<EgressPoller>,

    /// The single ingress publication's add, until it lands. Only used when
    /// `ingressEndpoints` is absent.
    ingress_add: Option<AsyncAdd>,
    ingress_publication: Option<IngressPublication>,

    members: Vec<MemberIngress>,

    /// The encoded request waiting to be offered — reused, not rebuilt.
    buffer: Vec<u8>,
    message_length: usize,
    /// What a challenge asked for, between reading it and answering it.
    challenge_credentials: Vec<u8>,
}

impl AsyncConnect {
    /// Start a connect: parse the member table and take the deadline
    /// (`AC:2193-2205`).
    ///
    /// The client is not touched until the first [`Self::poll`] — the
    /// reference's constructor only parses.
    ///
    /// # Errors
    ///
    /// [`ClusterClientError::Configuration`] when `ingressEndpoints` is not the
    /// `<memberId>=<endpoint>,…` form (`AC:735-756`).
    pub fn new(context: AeronClusterContext) -> Result<Self, ClusterClientError> {
        let members = parse_ingress_endpoints(context.ingress_endpoints.as_deref())?;
        let timeout = Duration::from_nanos(u64::try_from(context.message_timeout_ns).unwrap_or(0));

        Ok(Self {
            context,
            state: ConnectState::CreateEgressSubscription,
            deadline: Instant::now() + timeout,
            correlation_id: None,
            cluster_session_id: None,
            leadership_term_id: None,
            leader_member_id: None,
            egress_image: None,
            leader_heartbeat_timeout_ns: None,
            egress_add: None,
            egress_subscription: None,
            egress_poller: None,
            ingress_add: None,
            ingress_publication: None,
            members,
            buffer: Vec::new(),
            message_length: 0,
            challenge_credentials: Vec::new(),
        })
    }

    /// A connect whose deadline is `deadline` rather than
    /// `now + messageTimeoutNs` — the reference's package-private constructor
    /// (`AC:2198`), with the same audience: tests.
    #[cfg(test)]
    pub(crate) fn with_deadline(
        context: AeronClusterContext,
        deadline: Instant,
    ) -> Result<Self, ClusterClientError> {
        let mut connect = Self::new(context)?;
        connect.deadline = deadline;
        Ok(connect)
    }

    /// Which state the connect is in.
    #[must_use]
    pub const fn state(&self) -> ConnectState {
        self.state
    }

    /// The reference's `step` (`AC:2243-2246`).
    #[must_use]
    pub const fn step(&self) -> i32 {
        self.state.step()
    }

    /// `State.fromStep` (`AC:2272-2276`).
    #[must_use]
    pub fn step_name(step: i32) -> &'static str {
        ConnectState::from_step(step).map_or("<unknown>", ConnectState::name)
    }

    /// The request the connect is waiting on, once it has sent one.
    #[must_use]
    pub const fn correlation_id(&self) -> Option<i64> {
        self.correlation_id
    }

    /// Drive the machine a turn (`AC:2283-2318`).
    ///
    /// # Errors
    ///
    /// [`ClusterClientError`] when the connect cannot go on.
    pub fn poll(&mut self, client: &mut Client) -> Result<Polled, ClusterClientError> {
        self.poll_with(client)
    }

    /// The same, over the crate's client seam.
    pub(crate) fn poll_with(
        &mut self,
        client: &mut impl ClientOps,
    ) -> Result<Polled, ClusterClientError> {
        self.check_deadline(client)?;

        match self.state {
            ConnectState::CreateEgressSubscription => self.create_egress_subscription(client)?,
            ConnectState::CreateIngressPublications => self.create_ingress_publications(client)?,
            ConnectState::AwaitPublicationConnected => {
                self.await_publication_connected(client)?;
            }
            ConnectState::SendMessage => self.send_message(client)?,
            ConnectState::PollResponse => self.poll_response(client)?,
            ConnectState::ConcludeConnect => {
                let session = self.conclude_connect(client);
                return Ok(Polled::Done(Box::new(session)));
            }
            ConnectState::Done => {}
        }

        Ok(Polled::Awaiting)
    }

    /// Give back what was drawn and stop being usable (`AC:2207-2236`).
    ///
    /// The reference's `ctx.close()` — closing an `Aeron` client the context
    /// owns — has no counterpart: this build's `Client` is always the caller's
    /// (Q4, and the crate's module note).
    pub fn close(&mut self, client: &mut Client) {
        self.close_with(client);
    }

    /// The same, over the crate's client seam.
    pub(crate) fn close_with(&mut self, client: &mut impl ClientOps) {
        if ConnectState::Done == self.state {
            return;
        }

        let timeout = self.add_timeout();

        if let Some(publication) = self.ingress_publication.take() {
            let _ = client.async_remove_publication(publication.registration_id(), timeout);
        } else if let Some(add) = self.ingress_add.take() {
            let _ = client.async_remove_publication(add.registration_id(), timeout);
        }

        if let Some(subscription) = self.egress_subscription {
            let _ = client.async_remove_subscription(subscription, timeout);
        } else if let Some(add) = self.egress_add.take() {
            let _ = client.async_remove_subscription(add.registration_id(), timeout);
        }

        for member in &mut self.members {
            if let Some(publication) = member.publication.take() {
                let _ = client.async_remove_publication(publication, timeout);
            } else if let Some(add) = member.add.take() {
                let _ = client.async_remove_publication(add.registration_id(), timeout);
            }
        }
    }

    fn add_timeout(&self) -> Duration {
        Duration::from_nanos(u64::try_from(self.context.message_timeout_ns).unwrap_or(0))
    }

    /// `checkDeadline` (`AC:2320-2351`).
    fn check_deadline(&self, client: &impl ClientOps) -> Result<(), ClusterClientError> {
        if Instant::now() <= self.deadline {
            return Ok(());
        }

        let egress_connected = self
            .egress_subscription
            .is_some_and(|subscription| client.subscription_present(subscription));
        let response_channel = self
            .egress_subscription
            .and_then(|subscription| {
                self.resolve_response_channel(client, subscription)
                    .ok()
                    .flatten()
            })
            .unwrap_or_else(|| "<unknown>".to_owned());
        let ingress_publication = self.ingress_publication.map_or_else(
            || "null".to_owned(),
            |publication| format!("{:?}", publication.registration_id()),
        );

        let mut message = format!(
            "cluster connect timeout: state={} messageTimeout={}ns ingressChannel={} \
             ingressEndpoints={} ingressPublication={ingress_publication} \
             egress.isConnected={egress_connected} responseChannel={response_channel}",
            self.state.name(),
            self.context.message_timeout_ns,
            self.context.ingress_channel.as_deref().unwrap_or("null"),
            self.context.ingress_endpoints.as_deref().unwrap_or("null"),
        );

        for member in &self.members {
            if let Some(exception) = &member.exception {
                message.push_str(&format!("; member {}: {exception}", member.member_id));
            }
        }

        Err(ClusterClientError::Timeout(message))
    }

    /// `CREATE_EGRESS_SUBSCRIPTION` (`AC:2353-2380`).
    fn create_egress_subscription(
        &mut self,
        client: &mut impl ClientOps,
    ) -> Result<(), ClusterClientError> {
        if self.egress_add.is_none() && self.egress_subscription.is_none() {
            let Some(channel) = self.context.egress_channel.clone() else {
                return Err(ClusterClientError::Configuration(
                    "egressChannel must be specified".to_owned(),
                ));
            };
            self.egress_add = Some(
                client
                    .async_add_subscription(
                        &channel,
                        self.context.egress_stream_id,
                        self.add_timeout(),
                    )
                    .map_err(client_error)?,
            );
        }

        if let Some(add) = self.egress_add {
            match client.async_add_poll(add) {
                AsyncAddPoll::Awaiting => {}
                AsyncAddPoll::Ready => {
                    self.egress_subscription = Some(add.registration_id());
                    self.egress_add = None;
                }
                AsyncAddPoll::Unknown => {
                    // The handle is spent; the subscription is either in the
                    // client's list or the add is over.
                    if client.subscription_present(add.registration_id()) {
                        self.egress_subscription = Some(add.registration_id());
                    }
                    self.egress_add = None;
                }
                AsyncAddPoll::Failed(error) => {
                    self.egress_add = None;
                    if is_temporarily_unavailable(&error) {
                        // `RESOURCE_TEMPORARILY_UNAVAILABLE` is an add to retry
                        // (`AC:2364-2372`), not a failed connect.
                        return Ok(());
                    }
                    return Err(client_error(error));
                }
            }
        }

        if let Some(subscription) = self.egress_subscription {
            self.egress_poller = Some(EgressPoller::new(subscription, FRAGMENT_LIMIT));
            self.state = ConnectState::CreateIngressPublications;
        }

        Ok(())
    }

    /// `CREATE_INGRESS_PUBLICATIONS` (`AC:2382-2417`).
    fn create_ingress_publications(
        &mut self,
        client: &mut impl ClientOps,
    ) -> Result<(), ClusterClientError> {
        if self.context.ingress_endpoints.is_none() {
            if self.ingress_publication.is_none() {
                if self.ingress_add.is_none() {
                    let Some(channel) = self.context.ingress_channel.clone() else {
                        return Err(ClusterClientError::Configuration(
                            "ingressChannel must be specified".to_owned(),
                        ));
                    };
                    self.ingress_add = Some(async_add_ingress(client, &self.context, &channel)?);
                }

                let add = self.ingress_add.expect("just added");

                match client.async_add_poll(add) {
                    AsyncAddPoll::Awaiting => {}
                    AsyncAddPoll::Ready => {
                        self.ingress_publication =
                            Some(ingress_kind(&self.context, add.registration_id()));
                        self.ingress_add = None;
                    }
                    AsyncAddPoll::Unknown => {
                        let kind = ingress_kind(&self.context, add.registration_id()).kind();
                        if client.publication_present(kind, add.registration_id()) {
                            self.ingress_publication =
                                Some(ingress_kind(&self.context, add.registration_id()));
                        }
                        self.ingress_add = None;
                    }
                    AsyncAddPoll::Failed(error) => {
                        self.ingress_add = None;
                        if is_temporarily_unavailable(&error) {
                            return Ok(());
                        }
                        return Err(client_error(error));
                    }
                }
            } else {
                self.ingress_add = None;
                self.state = ConnectState::AwaitPublicationConnected;
            }
        } else {
            self.state = ConnectState::AwaitPublicationConnected;
        }

        Ok(())
    }

    /// `AWAIT_PUBLICATION_CONNECTED` (`AC:2419-2450`).
    fn await_publication_connected(
        &mut self,
        client: &mut impl ClientOps,
    ) -> Result<(), ClusterClientError> {
        let Some(subscription) = self.egress_subscription else {
            return Ok(());
        };
        let Some(response_channel) = self.resolve_response_channel(client, subscription)? else {
            // A wildcard-port egress channel the driver has not assigned a port
            // to yet: there is no channel to name in the request.
            return Ok(());
        };

        let context = &self.context;

        if self.ingress_publication.is_none() {
            let mut chosen = None;

            for member in &mut self.members {
                if member.publication.is_none() && member.exception.is_none() {
                    if member.add.is_none() {
                        let channel = member_channel(context, &member.endpoint);
                        match async_add_ingress(client, context, &channel) {
                            Ok(add) => member.add = Some(add),
                            Err(error) => {
                                member.exception = Some(error.to_string());
                                continue;
                            }
                        }
                    }

                    if let Some(add) = member.add {
                        match client.async_add_poll(add) {
                            AsyncAddPoll::Awaiting => {}
                            AsyncAddPoll::Ready => {
                                member.publication = Some(add.registration_id());
                                member.add = None;
                            }
                            AsyncAddPoll::Unknown => {
                                if client.publication_present(
                                    ingress_kind(context, add.registration_id()).kind(),
                                    add.registration_id(),
                                ) {
                                    member.publication = Some(add.registration_id());
                                }
                                member.add = None;
                            }
                            AsyncAddPoll::Failed(error) => {
                                member.add = None;
                                if !is_temporarily_unavailable(&error) {
                                    member.exception = Some(error.to_string());
                                }
                            }
                        }
                    }
                }

                if let Some(publication) = member.publication {
                    if client.publication_is_connected(
                        ingress_kind(context, publication).kind(),
                        publication,
                    ) {
                        chosen = Some(publication);
                        break;
                    }
                }
            }

            if let Some(publication) = chosen {
                self.ingress_publication = Some(ingress_kind(context, publication));
                self.prepare_connect_request(client, &response_channel)?;
            }
        } else if let Some(publication) = self.ingress_publication {
            if client.publication_is_connected(publication.kind(), publication.registration_id()) {
                self.prepare_connect_request(client, &response_channel)?;
            }
        }

        Ok(())
    }

    /// `prepareConnectRequest` (`AC:2452-2471`).
    fn prepare_connect_request(
        &mut self,
        client: &mut impl ClientOps,
        response_channel: &str,
    ) -> Result<(), ClusterClientError> {
        let correlation_id = client.next_correlation_id().map_err(client_error)?;
        self.correlation_id = Some(correlation_id);

        let encoded_credentials = self.context.credentials.encoded();
        let client_info = client_info(&self.context.client_name);

        let length = message_header_codec::ENCODED_LENGTH
            + usize::from(deepmsg_codec::cluster::session_connect_request_codec::SBE_BLOCK_LENGTH)
            + 4
            + response_channel.len()
            + 4
            + encoded_credentials.len()
            + 4
            + client_info.len();
        self.buffer.resize(length, 0);

        {
            let encoder = SessionConnectRequestEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer),
                message_header_codec::ENCODED_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut encoder = header.parent().expect("the encoder the header wrapped");

            encoder
                .correlation_id(correlation_id)
                .response_stream_id(self.context.egress_stream_id)
                // The *protocol's* version, not this build's.
                .version(PROTOCOL_SEMANTIC_VERSION)
                .response_channel(response_channel.as_bytes())
                .encoded_credentials(&encoded_credentials)
                .client_info(client_info.as_bytes());

            self.message_length = message_header_codec::ENCODED_LENGTH + encoder.encoded_length();
        }

        self.state = ConnectState::SendMessage;
        Ok(())
    }

    /// `SEND_MESSAGE` (`AC:2473-2484`).
    fn send_message(&mut self, client: &mut impl ClientOps) -> Result<(), ClusterClientError> {
        let Some(publication) = self.ingress_publication else {
            return Err(ClusterClientError::Cluster(
                "unexpected loss of connection to cluster".to_owned(),
            ));
        };

        let payload = &self.buffer[..self.message_length];
        match client.offer(publication.kind(), publication.registration_id(), payload) {
            Some(Appended::Ok { .. }) => self.state = ConnectState::PollResponse,
            // The reference's `CLOSED` and `NOT_CONNECTED` (`AC:2480-2483`).
            Some(Appended::NotConnected) | None => {
                return Err(ClusterClientError::Cluster(
                    "unexpected loss of connection to cluster".to_owned(),
                ));
            }
            // Back-pressured or mid-rotation: the same turn again.
            Some(_) => {}
        }

        Ok(())
    }

    /// `POLL_RESPONSE` (`AC:2486-2541`).
    fn poll_response(&mut self, client: &mut impl ClientOps) -> Result<(), ClusterClientError> {
        let Some(poller) = self.egress_poller.as_mut() else {
            return Ok(());
        };

        let fragments = poller.poll_with(client);

        // Completeness first, and the fragment count is only ever a hint: the
        // reference asks `is_poll_complete && correlation_id ==` and never
        // looks at the count (`AC:2488-2490`).
        if 0 == fragments
            || !poller.is_poll_complete()
            || poller.correlation_id() != self.correlation_id
        {
            return Ok(());
        }

        if poller.is_challenged() {
            self.correlation_id = None;
            self.cluster_session_id = poller.cluster_session_id();
            self.challenge_credentials = self
                .context
                .credentials
                .on_challenge(poller.encoded_challenge())
                .map_err(ClusterClientError::Authentication)?;
            return self.prepare_challenge_response(client);
        }

        let detail = poller.detail().to_owned();

        match poller.event_code() {
            Some(deepmsg_codec::cluster::event_code::EventCode::OK) => {
                self.leadership_term_id = poller.leadership_term_id();
                self.leader_member_id = poller.leader_member_id();
                self.cluster_session_id = poller.cluster_session_id();
                self.leader_heartbeat_timeout_ns = poller.leader_heartbeat_timeout_ns();
                self.egress_image = poller.egress_image();
                self.state = ConnectState::ConcludeConnect;
            }
            Some(deepmsg_codec::cluster::event_code::EventCode::ERROR) => {
                return Err(ClusterClientError::Cluster(detail));
            }
            Some(deepmsg_codec::cluster::event_code::EventCode::REDIRECT) => {
                self.update_members(client)?;
            }
            Some(deepmsg_codec::cluster::event_code::EventCode::AUTHENTICATION_REJECTED) => {
                return Err(ClusterClientError::Authentication(detail));
            }
            // `CLOSED` and the null code are no-ops (`AC:2521-2523`); a slot
            // with no code is the same answer.
            Some(deepmsg_codec::cluster::event_code::EventCode::CLOSED)
            | Some(deepmsg_codec::cluster::event_code::EventCode::NullVal)
            | None => {}
        }

        Ok(())
    }

    /// `prepareChallengeResponse` (`AC:2528-2541`).
    fn prepare_challenge_response(
        &mut self,
        client: &mut impl ClientOps,
    ) -> Result<(), ClusterClientError> {
        let correlation_id = client.next_correlation_id().map_err(client_error)?;
        self.correlation_id = Some(correlation_id);
        let cluster_session_id = self.cluster_session_id.unwrap_or(-1);

        let length = message_header_codec::ENCODED_LENGTH
            + usize::from(deepmsg_codec::cluster::challenge_response_codec::SBE_BLOCK_LENGTH)
            + 4
            + self.challenge_credentials.len();
        self.buffer.resize(length, 0);

        {
            let encoder = ChallengeResponseEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer),
                message_header_codec::ENCODED_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut encoder = header.parent().expect("the encoder the header wrapped");

            encoder
                .correlation_id(correlation_id)
                .cluster_session_id(cluster_session_id)
                .encoded_credentials(&self.challenge_credentials);

            self.message_length = message_header_codec::ENCODED_LENGTH + encoder.encoded_length();
        }

        self.challenge_credentials.clear();
        self.state = ConnectState::SendMessage;
        Ok(())
    }

    /// `updateMembers` (`AC:2543-2570`): the redirect's table replaces ours.
    fn update_members(&mut self, client: &mut impl ClientOps) -> Result<(), ClusterClientError> {
        let Some(leader_member_id) = self
            .egress_poller
            .as_ref()
            .and_then(EgressPoller::leader_member_id)
        else {
            return Err(ClusterClientError::Cluster(
                "a redirect without a leader member id".to_owned(),
            ));
        };
        self.leader_member_id = Some(leader_member_id);

        let old_leader = self
            .members
            .iter()
            .position(|member| member.member_id == leader_member_id)
            .map(|index| self.members.remove(index));

        let timeout = self.add_timeout();

        if let Some(publication) = self.ingress_publication.take() {
            let _ = client.async_remove_publication(publication.registration_id(), timeout);
        }

        for member in &mut self.members {
            if let Some(publication) = member.publication.take() {
                let _ = client.async_remove_publication(publication, timeout);
            }
            if let Some(add) = member.add.take() {
                let _ = client.async_remove_publication(add.registration_id(), timeout);
            }
        }

        let endpoints = self
            .egress_poller
            .as_ref()
            .map_or_else(String::new, |poller| poller.detail().to_owned());
        self.members = parse_ingress_endpoints(Some(&endpoints))?;

        let Some(index) = self
            .members
            .iter()
            .position(|member| member.member_id == leader_member_id)
        else {
            return Err(ClusterClientError::Cluster(format!(
                "the redirect names member {leader_member_id}, which {endpoints:?} does not"
            )));
        };

        let reusable = old_leader
            .as_ref()
            .filter(|old| old.exception.is_none() && old.endpoint == self.members[index].endpoint);

        if let Some(old) = reusable {
            self.members[index].publication = old.publication;
            self.ingress_publication = old.publication.map(|id| ingress_kind(&self.context, id));
        } else {
            if let Some(old) = old_leader {
                if let Some(publication) = old.publication {
                    let _ = client.async_remove_publication(publication, timeout);
                }
            }

            let channel = member_channel(&self.context, &self.members[index].endpoint);
            let add = async_add_ingress(client, &self.context, &channel)?;
            self.members[index].add = Some(add);
        }

        self.state = ConnectState::AwaitPublicationConnected;
        Ok(())
    }

    /// `concludeConnect` (`AC:2572-2598`).
    fn conclude_connect(&mut self, client: &mut impl ClientOps) -> AeronCluster {
        if self.context.new_leader_timeout_ns.is_none() {
            let heartbeat = self
                .leader_heartbeat_timeout_ns
                .unwrap_or(LEADER_HEARTBEAT_TIMEOUT_DEFAULT_NS);
            self.context.new_leader_timeout_ns = Some(2 * heartbeat);
        }

        let ingress = self
            .ingress_publication
            .take()
            .expect("the connect was concluded with a publication");
        let egress_subscription = self.egress_subscription.expect("and a subscription");

        // The other members' publications are the connect's, and a session
        // that has chosen its leader does not need them (`AC:2591-2593`).
        let timeout = self.add_timeout();
        for member in &mut self.members {
            if member.publication == Some(ingress.registration_id())
                && member.member_id == self.leader_member_id.unwrap_or(NULL_MEMBER_ID)
            {
                continue;
            }
            if let Some(publication) = member.publication.take() {
                let _ = client.async_remove_publication(publication, timeout);
            } else if let Some(add) = member.add.take() {
                let _ = client.async_remove_publication(add.registration_id(), timeout);
            }
        }

        self.state = ConnectState::Done;

        AeronCluster::new(
            std::mem::take(&mut self.context),
            ingress,
            egress_subscription,
            self.egress_image,
            self.cluster_session_id.unwrap_or(-1),
            self.leadership_term_id.unwrap_or(-1),
            self.leader_member_id.unwrap_or(NULL_MEMBER_ID),
        )
    }

    /// `tryResolveChannelEndpointPort` for the egress subscription
    /// (`AC:2421`): the channel to name in the connect request, or `None` when
    /// a wildcard port has not been assigned one yet.
    fn resolve_response_channel(
        &self,
        client: &impl ClientOps,
        subscription: i64,
    ) -> Result<Option<String>, ClusterClientError> {
        let Some(channel) = client.subscription_channel(subscription) else {
            return Ok(None);
        };

        let mut uri = ChannelUri::parse(&channel).map_err(|error| {
            ClusterClientError::Configuration(format!("egressChannel is not a channel: {error}"))
        })?;

        if !should_replace_wildcard_port(&uri) {
            return Ok(Some(channel));
        }

        let Some(resolved) = client.subscription_resolved_endpoint(subscription) else {
            return Ok(None);
        };
        uri.put(ENDPOINT_KEY, resolved);

        Ok(Some(uri.build()))
    }
}

/// The synchronous connect: [`AsyncConnect`] in a loop (`AC:141-200`).
///
/// **The client is driven here, and nowhere else.** `AsyncConnect::poll` asks
/// the client questions; it is this loop's `client.poll()` that lets the driver
/// answer them. A connect written without that call is a connect that waits for
/// answers nobody reads.
///
/// # Errors
///
/// Whatever the connect fails with.
pub fn connect(
    mut context: AeronClusterContext,
    client: &mut Client,
) -> Result<AeronCluster, ClusterClientError> {
    context.conclude()?;
    let mut async_connect = AsyncConnect::new(context)?;

    loop {
        match async_connect.poll(client)? {
            Polled::Awaiting => {
                let _ = client.poll();
                std::thread::yield_now();
            }
            Polled::Done(session) => return Ok(*session),
        }
    }
}

/// `parseIngressEndpoints` (`AC:735-756`).
fn parse_ingress_endpoints(
    endpoints: Option<&str>,
) -> Result<Vec<MemberIngress>, ClusterClientError> {
    let Some(endpoints) = endpoints else {
        return Ok(Vec::new());
    };

    let mut members = Vec::new();

    for endpoint in endpoints.split(',') {
        let Some(separator) = endpoint.find('=') else {
            return Err(ClusterClientError::Configuration(format!(
                "endpoint missing '=' separator: {endpoints}"
            )));
        };

        let member_id = endpoint[..separator].parse().map_err(|_| {
            ClusterClientError::Configuration(format!(
                "member id is not a number in endpoints: {endpoints}"
            ))
        })?;

        members.push(MemberIngress::new(
            member_id,
            endpoint[separator + 1..].to_owned(),
        ));
    }

    // The reference iterates an `Int2ObjectHashMap`, whose order is its hash
    // order. Sorting by id is this build's: the answer to "who goes first" is
    // not part of the protocol — the first *connected* publication is — and a
    // deterministic order is one less thing a test has to be lucky about.
    members.sort_by_key(MemberIngress::member_id);

    Ok(members)
}

/// The channel a member's ingress publication is added on (`AC:2617-2627`):
/// a UDP ingress channel has its `endpoint` replaced with the member's.
fn member_channel(context: &AeronClusterContext, endpoint: &str) -> String {
    let Some(channel) = context.ingress_channel.as_deref() else {
        return String::new();
    };

    let Ok(mut uri) = ChannelUri::parse(channel) else {
        return channel.to_owned();
    };

    if "udp" == uri.media() {
        uri.put(ENDPOINT_KEY, endpoint);
        uri.build()
    } else {
        channel.to_owned()
    }
}

/// `asyncAddIngressPublication` (`AC:795-805`).
fn async_add_ingress(
    client: &mut impl ClientOps,
    context: &AeronClusterContext,
    channel: &str,
) -> Result<AsyncAdd, ClusterClientError> {
    let result = if context.is_ingress_exclusive {
        client.async_add_exclusive_publication(
            channel,
            context.ingress_stream_id,
            Duration::from_nanos(u64::try_from(context.message_timeout_ns).unwrap_or(0)),
        )
    } else {
        client.async_add_publication(
            channel,
            context.ingress_stream_id,
            Duration::from_nanos(u64::try_from(context.message_timeout_ns).unwrap_or(0)),
        )
    };

    result.map_err(client_error)
}

/// The kind an ingress publication is (`AC:795-805`).
fn ingress_kind(context: &AeronClusterContext, registration_id: i64) -> IngressPublication {
    if context.is_ingress_exclusive {
        IngressPublication::Exclusive(registration_id)
    } else {
        IngressPublication::Concurrent(registration_id)
    }
}

/// `AeronCounters.formatVersionInfo`'s shape (`AeronCounters.java:1633-1636`)
/// with this build's two halves: the version it is compatible with, and a token
/// naming the build (`docs/compat.md`).
fn client_info(client_name: &str) -> String {
    format!("name={client_name} version={COMPAT_VERSION_TEXT} commit={BUILD_IDENTITY}")
}

/// `AERON_ERROR_CODE_RESOURCE_TEMPORARILY_UNAVAILABLE` is an add to retry.
fn is_temporarily_unavailable(error: &CommandError) -> bool {
    matches!(
        error,
        CommandError::Driver { code, .. }
            if *code == deepmsg_cnc::command::ERROR_CODE_RESOURCE_TEMPORARILY_UNAVAILABLE
    )
}

fn client_error(error: CommandError) -> ClusterClientError {
    ClusterClientError::Client(error.to_string())
}

/// `aeron_subscription_should_replace_wildcard_port`
/// (`aeron_subscription.c:660-672`): a **UDP** channel whose endpoint names
/// port **zero**, and which is not `control-mode=manual`.
fn should_replace_wildcard_port(uri: &ChannelUri) -> bool {
    if "udp" != uri.media() {
        return false;
    }

    if Some(CONTROL_MODE_MANUAL) == uri.get(CONTROL_MODE_KEY) {
        return false;
    }

    uri.get(ENDPOINT_KEY)
        .is_some_and(|endpoint| endpoint.ends_with(":0"))
}

/// `AERON_UDP_CHANNEL_ENDPOINT_KEY`.
const ENDPOINT_KEY: &str = "endpoint";
/// `AERON_UDP_CHANNEL_CONTROL_MODE_KEY`.
const CONTROL_MODE_KEY: &str = "control-mode";
/// `AERON_UDP_CHANNEL_CONTROL_MODE_MANUAL_VALUE`.
const CONTROL_MODE_MANUAL: &str = "manual";

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, VecDeque};

    use deepmsg_client::fragment_assembler::{Action, Message};
    use deepmsg_codec::cluster::event_code::EventCode;
    use deepmsg_codec::cluster::message_header_codec::MessageHeaderDecoder;
    use deepmsg_codec::cluster::session_connect_request_codec::SessionConnectRequestDecoder;
    use deepmsg_codec::cluster::session_event_codec::SessionEventEncoder;
    use deepmsg_core::logbuffer::append::Appended;
    use deepmsg_core::logbuffer::position::Position;

    use super::*;
    use crate::client_ops::PublicationKind;

    /// What an add answers the next time it is polled.
    #[derive(Debug)]
    enum AddAnswer {
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
    struct RecordingClient {
        calls: Vec<String>,
        next_id: i64,
        next_correlation: i64,
        pending: BTreeMap<i64, AddAnswer>,
        subscriptions: BTreeMap<i64, Subscription>,
        publications: BTreeMap<i64, Publication>,
        egress: VecDeque<Vec<u8>>,
        offered: std::cell::RefCell<Vec<Vec<u8>>>,
    }

    impl RecordingClient {
        fn new() -> Self {
            Self {
                next_correlation: 100,
                next_id: 1,
                ..Self::default()
            }
        }

        fn last_registration_id(&self) -> i64 {
            self.next_id - 1
        }

        fn answer(&mut self, id: i64, answer: AddAnswer) {
            self.pending.insert(id, answer);
        }

        fn connect(&mut self, id: i64) {
            if let Some(publication) = self.publications.get_mut(&id) {
                publication.ready = true;
                publication.connected = true;
            }
        }

        fn land(&mut self, id: i64) {
            // The answer the next poll gives: `Ready`. The resource's own
            // `ready` flag is set when that answer is read, as the driver's
            // answer is what makes the resource usable.
            self.pending.insert(id, AddAnswer::Ready);
        }

        fn push_egress(&mut self, bytes: Vec<u8>) {
            self.egress.push_back(bytes);
        }

        fn calls(&self) -> &[String] {
            &self.calls
        }

        fn offered(&self) -> Vec<Vec<u8>> {
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
            // `offer` is `&self` on the trait (the reference's is too), so the
            // recording of what was offered happens through interior
            // mutability-free means: the tests read `offered` after the fact
            // through `&mut self` only. Kept simple by returning a position.
            if self.publications.contains_key(&registration_id) {
                self.offered.borrow_mut().push(payload.to_vec());
                Some(Appended::Ok {
                    position: Position::from_raw(i64::from(payload.len() as u32)),
                    term_offset: 0,
                })
            } else {
                None
            }
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

    fn context() -> AeronClusterContext {
        AeronClusterContext::default()
            .ingress_channel(Some("aeron:ipc"))
            .egress_channel(Some("aeron:ipc"))
    }

    /// The five fields every accepting test starts from.
    fn connect() -> AsyncConnect {
        AsyncConnect::new(context()).expect("a connect")
    }

    /// Drive the connect until the egress subscription has landed and the
    /// state is `CREATE_INGRESS_PUBLICATIONS`.
    fn land_egress(connect: &mut AsyncConnect, client: &mut RecordingClient) {
        connect.poll_with(client).expect("the first poll");
        let subscription = client.last_registration_id();
        client.land(subscription);
        connect
            .poll_with(client)
            .expect("the egress subscription lands");
        assert_eq!(ConnectState::CreateIngressPublications, connect.state());
    }

    /// `initialState`.
    #[test]
    fn an_async_connect_starts_at_create_egress_subscription() {
        let connect = connect();

        assert_eq!(ConnectState::CreateEgressSubscription, connect.state());
        assert_eq!(-1, connect.step());
        assert_eq!(
            "CREATE_EGRESS_SUBSCRIPTION",
            AsyncConnect::step_name(connect.step())
        );
    }

    /// The deadline taken at construction is the whole connect's, and it says
    /// which step it expired in (`AC:2320-2351`).
    #[test]
    fn a_connect_that_runs_out_of_time_says_which_state_it_was_in() {
        let mut client = RecordingClient::new();
        let mut connect =
            AsyncConnect::with_deadline(context(), Instant::now() - Duration::from_nanos(1))
                .expect("a connect");

        let error = connect
            .poll_with(&mut client)
            .expect_err("a deadline in the past");

        assert_eq!(
            "ERROR - cluster connect timeout: state=CREATE_EGRESS_SUBSCRIPTION \
             messageTimeout=5000000000ns ingressChannel=aeron:ipc ingressEndpoints=null \
             ingressPublication=null egress.isConnected=false responseChannel=<unknown>",
            error.to_string()
        );
        assert!(client.calls().is_empty(), "nothing was asked of the client");
    }

    /// `shouldCloseAsyncSubscription`: nothing has landed, so the add itself is
    /// given back.
    #[test]
    fn closing_before_the_egress_subscription_lands_removes_the_pending_add() {
        let mut client = RecordingClient::new();
        let mut connect = connect();

        assert!(matches!(
            connect.poll_with(&mut client),
            Ok(Polled::Awaiting)
        ));
        assert_eq!(ConnectState::CreateEgressSubscription, connect.state());

        connect.close_with(&mut client);

        assert_eq!(
            vec![
                "async_add_subscription(aeron:ipc, 102)",
                "async_add_poll(1)",
                "async_remove_subscription(1)",
            ],
            client.calls()
        );
    }

    /// `shouldCloseEgressSubscription`: the subscription has landed, so the
    /// subscription is given back — one removal, and the same one the row
    /// above sends (see the module note on the collapsed close paths).
    #[test]
    fn closing_after_the_egress_subscription_lands_removes_the_subscription() {
        let mut client = RecordingClient::new();
        let mut connect = connect();

        land_egress(&mut connect, &mut client);
        connect.close_with(&mut client);

        assert_eq!(
            vec![
                "async_add_subscription(aeron:ipc, 102)",
                "async_add_poll(1)",
                "async_add_poll(1)",
                "async_remove_subscription(1)",
            ],
            client.calls()
        );
    }

    /// `shouldCloseAsyncPublication`: the exclusive ingress add is in flight.
    #[test]
    fn closing_before_the_ingress_publication_lands_removes_the_pending_add() {
        let mut client = RecordingClient::new();
        let mut connect = connect();

        land_egress(&mut connect, &mut client);
        connect.poll_with(&mut client).expect("the ingress add");

        connect.close_with(&mut client);

        assert_eq!(
            vec![
                "async_add_subscription(aeron:ipc, 102)",
                "async_add_poll(1)",
                "async_add_poll(1)",
                "async_add_exclusive_publication(aeron:ipc, 101)",
                "async_add_poll(2)",
                "async_remove_publication(2)",
                "async_remove_subscription(1)",
            ],
            client.calls()
        );
    }

    /// `shouldCloseIngressPublication`: the concurrent ingress publication has
    /// landed.
    #[test]
    fn closing_after_the_ingress_publication_lands_removes_the_publication() {
        let mut client = RecordingClient::new();
        let mut connect = connect().context_is_concurrent();

        land_egress(&mut connect, &mut client);
        connect.poll_with(&mut client).expect("the ingress add");
        let publication = client.last_registration_id();
        client.answer(publication, AddAnswer::Ready);
        connect
            .poll_with(&mut client)
            .expect("the ingress publication lands");
        connect.poll_with(&mut client).expect("await connected");

        connect.close_with(&mut client);

        assert_eq!(
            vec![
                "async_add_subscription(aeron:ipc, 102)",
                "async_add_poll(1)",
                "async_add_poll(1)",
                "async_add_publication(aeron:ipc, 101)",
                "async_add_poll(2)",
                "async_add_poll(2)",
                "async_remove_publication(2)",
                "async_remove_subscription(1)",
            ],
            client.calls()
        );
    }

    /// `shouldCloseIngressPublicationsOnMembers`: one add per member per turn,
    /// a member whose add is unknown is tried again, and the first publication
    /// that connects wins.
    #[test]
    fn member_publications_are_added_once_each_and_the_first_connected_one_wins() {
        let mut context = AeronClusterContext::default()
            .ingress_channel(Some("aeron:udp"))
            .ingress_endpoints(Some(
                "0=localhost:20000,1=localhost:20001,2=localhost:20002",
            ))
            .egress_channel(Some("aeron:ipc"));
        context.is_ingress_exclusive = true;

        let mut client = RecordingClient::new();
        let mut connect = AsyncConnect::new(context).expect("a connect");

        land_egress(&mut connect, &mut client);

        // The member table is drawn from the first round after the state moves
        // to AWAIT, exactly as the reference's `awaitPublicationConnected`.
        connect
            .poll_with(&mut client)
            .expect("the state moves to await");
        connect.poll_with(&mut client).expect("members added");
        let member0 = client.last_registration_id() - 2;
        let member1 = member0 + 1;
        client.answer(member0, AddAnswer::Ready);
        // Member 1's add answers Unknown: the reference's `NULL_VALUE`
        // registration id, which means "try again" — so it is re-added every
        // turn. Member 2 stays pending for the whole test.
        client.answer(member1, AddAnswer::Unknown);

        for _ in 0..9 {
            connect.poll_with(&mut client).expect("a member round");
        }
        assert_eq!(ConnectState::AwaitPublicationConnected, connect.state());

        // Member 0 connected after its add landed, and is chosen.
        client.connect(member0);
        connect
            .poll_with(&mut client)
            .expect("the first connected member wins");
        assert_eq!(
            ConnectState::SendMessage,
            connect.state(),
            "calls: {:?}",
            client.calls()
        );

        let adds = |endpoint: &str| {
            client
                .calls()
                .iter()
                .filter(|call| call.contains(endpoint))
                .count()
        };
        assert_eq!(
            1,
            adds("localhost:20000"),
            "the first member was added once"
        );
        assert!(adds("localhost:20001") > 1, "the unknown add is retried");
        assert_eq!(1, adds("localhost:20002"), "a pending add is not re-added");
    }

    /// `shouldRetryMemberIngressPublicationWhenSendChannelEndpointIsClosing`:
    /// `RESOURCE_TEMPORARILY_UNAVAILABLE` is a retry, and the member's
    /// publication is used when it lands.
    #[test]
    fn a_temporarily_unavailable_member_add_is_retried() {
        let context = AeronClusterContext::default()
            .ingress_channel(Some("aeron:udp"))
            .ingress_endpoints(Some("0=localhost:20000"))
            .egress_channel(Some("aeron:ipc"));

        let mut client = RecordingClient::new();
        let mut connect = AsyncConnect::new(context).expect("a connect");

        land_egress(&mut connect, &mut client);

        connect.poll_with(&mut client).expect("the member add");
        let member = client.last_registration_id();
        client.answer(
            member,
            AddAnswer::Failed(CommandError::Driver {
                code: deepmsg_cnc::command::ERROR_CODE_RESOURCE_TEMPORARILY_UNAVAILABLE,
                message: "send_channel_endpoint found in CLOSING state, please retry".to_owned(),
            }),
        );
        connect
            .poll_with(&mut client)
            .expect("the refusal is retried");
        assert_eq!(ConnectState::AwaitPublicationConnected, connect.state());

        // The next turn makes a new add; that one lands and connects.
        connect.poll_with(&mut client).expect("a new add");
        let retried = client.last_registration_id();
        assert_ne!(member, retried, "a new add was made");
        client.answer(retried, AddAnswer::Ready);
        client.connect(retried);

        connect
            .poll_with(&mut client)
            .expect("the publication connects");
        assert_eq!(ConnectState::SendMessage, connect.state());
    }

    fn session_event(correlation_id: i64) -> Vec<u8> {
        session_event_with(correlation_id, EventCode::OK, b"you are now connected")
    }

    fn session_event_with(correlation_id: i64, code: EventCode, detail: &[u8]) -> Vec<u8> {
        let mut buffer = vec![0u8; 256];
        let length = {
            let encoder = SessionEventEncoder::default().wrap(
                WriteBuf::new(&mut buffer),
                message_header_codec::ENCODED_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut encoder = header.parent().expect("the encoder the header wrapped");

            encoder
                .cluster_session_id(888)
                .correlation_id(correlation_id)
                .leadership_term_id(5)
                .leader_member_id(2)
                .code(code)
                .version(PROTOCOL_SEMANTIC_VERSION)
                .leader_heartbeat_timeout_ns_opt(None)
                .detail(detail);

            message_header_codec::ENCODED_LENGTH + encoder.encoded_length()
        };

        buffer.truncate(length);
        buffer
    }

    /// A connect that has offered its request and is waiting for the answer.
    fn driven_to_poll_response() -> (AsyncConnect, RecordingClient) {
        let mut client = RecordingClient::new();
        let mut connect = connect().context_is_concurrent();

        land_egress(&mut connect, &mut client);
        connect.poll_with(&mut client).expect("the ingress add");
        let publication = client.last_registration_id();
        client.answer(publication, AddAnswer::Ready);
        connect
            .poll_with(&mut client)
            .expect("the ingress publication lands");
        client.connect(publication);
        connect
            .poll_with(&mut client)
            .expect("the state moves to await");
        connect
            .poll_with(&mut client)
            .expect("the request is prepared");
        connect.poll_with(&mut client).expect("the request is sent");

        assert_eq!(ConnectState::PollResponse, connect.state());
        (connect, client)
    }

    /// The request's bytes are the contract (`AC:2452-2471`).
    #[test]
    fn the_connect_request_carries_the_references_fields() {
        let mut client = RecordingClient::new();
        let mut context = context().client_name("test");
        context.is_ingress_exclusive = false;
        let mut connect = AsyncConnect::new(context).expect("a connect");

        land_egress(&mut connect, &mut client);
        connect.poll_with(&mut client).expect("the ingress add");
        let publication = client.last_registration_id();
        client.answer(publication, AddAnswer::Ready);
        connect
            .poll_with(&mut client)
            .expect("the ingress publication lands");
        client.connect(publication);
        connect
            .poll_with(&mut client)
            .expect("the state moves to await");
        connect
            .poll_with(&mut client)
            .expect("the request is prepared");
        assert_eq!(ConnectState::SendMessage, connect.state());

        connect.poll_with(&mut client).expect("the request is sent");

        let offered = client.offered();
        assert_eq!(1, offered.len(), "one request was offered");
        let request = &offered[0];

        let header =
            MessageHeaderDecoder::default().wrap(deepmsg_codec::cluster::ReadBuf::new(request), 0);
        assert_eq!(111, header.schema_id());
        assert_eq!(3, header.template_id(), "SessionConnectRequest");

        let mut decoder = SessionConnectRequestDecoder::default().header(header, 0);
        assert_eq!(100, decoder.correlation_id());
        assert_eq!(102, decoder.response_stream_id());
        assert_eq!(Some(PROTOCOL_SEMANTIC_VERSION), decoder.version());

        let coordinates = decoder.response_channel_decoder();
        let response_channel = decoder.response_channel_slice(coordinates);
        assert_eq!(b"aeron:ipc", response_channel);

        let coordinates = decoder.encoded_credentials_decoder();
        assert_eq!(b"" as &[u8], decoder.encoded_credentials_slice(coordinates));

        let coordinates = decoder.client_info_decoder();
        let client_info = decoder.client_info_slice(coordinates);
        assert_eq!(
            format!("name=test version={COMPAT_VERSION_TEXT} commit={BUILD_IDENTITY}"),
            String::from_utf8_lossy(client_info)
        );
    }

    /// `shouldConnectViaIngressChannel`: an OK session event completes the
    /// connect, and the session carries the reference's five values.
    #[test]
    fn an_ok_session_event_completes_the_connect() {
        let (mut connect, mut client) = driven_to_poll_response();
        let publication = connect
            .ingress_publication
            .expect("the connect chose a publication")
            .registration_id();
        let correlation_id = connect.correlation_id().expect("a request");

        client.push_egress(session_event(correlation_id));
        connect.poll_with(&mut client).expect("the answer");
        assert_eq!(ConnectState::ConcludeConnect, connect.state());

        let Polled::Done(session) = connect.poll_with(&mut client).expect("the session") else {
            panic!("the connect concluded");
        };

        assert_eq!(publication, session.ingress_publication().registration_id());
        assert_eq!(1, session.egress_subscription());
        assert_eq!(5, session.leadership_term_id());
        assert_eq!(2, session.leader_member_id());
        assert_eq!(888, session.cluster_session_id());
        assert_eq!(
            Some(2 * LEADER_HEARTBEAT_TIMEOUT_DEFAULT_NS),
            session.new_leader_timeout_ns(),
            "no heartbeat in the answer, so the reference's ten-second default"
        );
    }

    /// The correlation id is the whole of "this answer is mine"
    /// (`AC:2488-2490`): an answer to another request does not complete the
    /// connect.
    #[test]
    fn an_answer_to_another_request_does_not_complete_the_connect() {
        let (mut connect, mut client) = driven_to_poll_response();
        let correlation_id = connect.correlation_id().expect("a request");

        client.push_egress(session_event(correlation_id + 1));
        connect
            .poll_with(&mut client)
            .expect("someone else's answer");

        assert_eq!(ConnectState::PollResponse, connect.state());
    }

    /// An `ERROR` event is the cluster refusing the connect, in its own words
    /// (`AC:2511-2512`).
    #[test]
    fn an_error_event_fails_the_connect_with_the_clusters_words() {
        let (mut connect, mut client) = driven_to_poll_response();
        let correlation_id = connect.correlation_id().expect("a request");

        client.push_egress(session_event_with(
            correlation_id,
            EventCode::ERROR,
            b"session limit reached",
        ));
        let error = connect
            .poll_with(&mut client)
            .expect_err("the cluster refused it");

        assert_eq!("ERROR - session limit reached", error.to_string());
    }

    impl AsyncConnect {
        /// The reference's `context.isIngressExclusive(false)`.
        fn context_is_concurrent(mut self) -> Self {
            self.context.is_ingress_exclusive = false;
            self
        }
    }
}
