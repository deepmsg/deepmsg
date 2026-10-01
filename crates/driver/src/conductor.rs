//! The conductor: the duty cycle that owns the CnC file.
//!
//! One call to [`Conductor::do_work`] is one pass of the reference's loop
//! (`aeron-driver/src/main/c/aeron_driver_conductor.c:3369-3407`), which runs
//! at two rates on purpose:
//!
//! | tier | every | what it does here |
//! |---|---|---|
//! | timeout | `timer_interval_ns`, 1 s by default | the driver's liveness, and the client pool |
//! | commands | every pass | drain the to-driver ring |
//!
//! The drain limit is **one** command per pass
//! (`AERON_COMMAND_DRAIN_LIMIT`, `aeron-driver/src/main/c/aeron_driver_context.h:53`).
//! That is not a tuning knob: it is the control plane's latency floor, because
//! a pass is also where every other thing that has to happen this cycle
//! happens, and batching commands would let a burst push the timer tier out.
//!
//! # What a pass owns
//!
//! The conductor owns the counters, the clients and the to-clients ring, and
//! the three are one story: a client exists because a command named it, its
//! liveness is a counter, and its departure is announced on the ring. The
//! reference spreads that across `get_or_add_client` (`:982-1035`),
//! `aeron_client_on_time_event` (`:1038-1056`) and `client_transmit`
//! (`:2233-2241`); here the client half is [`crate::clients`], the counter half
//! is [`crate::system_counters`], and this module is the wiring between them.
//!
//! # The cycle time counters
//!
//! Two of the forty-six system counters measure this loop: the longest pass
//! (26) and the number of passes that ran longer than a threshold (27)
//! (`aeron_duty_cycle_tracker.h:50-62`). They are how a deployment sees a
//! driver that is keeping up, so they are updated here from the first pass
//! rather than left at zero.
//!
//! # What this conductor still does not do
//!
//! Commands whose resources are not implemented yet — publications,
//! subscriptions, images — are counted and named rather than answered. The
//! reference replies `ON_ERROR` on the to-clients ring, and until that reply
//! exists for them a client that sent one reaches its own deadline and reports
//! a timeout, which is the same shape as the reference's own timeout leak
//! (`docs/protocol/cnc-layout.md`, "A leak in the reference"). The commands
//! this build *does* implement answer with the same replies the reference uses:
//! `ON_COUNTER_READY` when a counter is allocated, `ON_OPERATION_SUCCEEDED` when
//! a removal is done, and `ON_ERROR` with the reference's error code when one
//! fails. A malformed command — one whose payload is shorter than its own
//! lengths claim — is counted separately, because it is a bug or a hostile
//! client rather than a feature this build has not reached.

use deepmsg_cnc::command::{
    ERROR_CODE_GENERIC_ERROR, ERROR_CODE_INVALID_CHANNEL, ERROR_CODE_MALFORMED_COMMAND,
    ERROR_CODE_NOT_SUPPORTED, ERROR_CODE_RESOURCE_TEMPORARILY_UNAVAILABLE,
    ERROR_CODE_STORAGE_SPACE, ERROR_CODE_UNKNOWN_COMMAND_TYPE_ID, ERROR_CODE_UNKNOWN_COUNTER,
    ERROR_CODE_UNKNOWN_PUBLICATION, ERROR_CODE_UNKNOWN_SUBSCRIPTION, ImageBuffersReady,
    ON_AVAILABLE_IMAGE_TYPE_ID, ON_CLIENT_TIMEOUT_TYPE_ID, ON_COUNTER_READY_TYPE_ID,
    ON_ERROR_TYPE_ID, ON_NEXT_AVAILABLE_SESSION_ID_TYPE_ID, ON_OPERATION_SUCCEEDED_TYPE_ID,
    ON_PUBLICATION_ERROR_TYPE_ID, ON_STATIC_COUNTER_TYPE_ID, ON_SUBSCRIPTION_READY_TYPE_ID,
    ON_UNAVAILABLE_COUNTER_TYPE_ID, ON_UNAVAILABLE_IMAGE_TYPE_ID, PublicationBuffersReady,
    PublicationError, REMOVE_PUBLICATION_FLAG_REVOKE, RejectImageError, decode_add_counter,
    decode_add_publication, decode_add_static_counter, decode_add_subscription, decode_correlated,
    decode_destination_by_id_command, decode_destination_command,
    decode_get_next_available_session_id, decode_reject_image, decode_remove_counter,
    decode_remove_publication, decode_remove_subscription, encode_client_timeout,
    encode_counter_update, encode_error, encode_next_available_session_id,
    encode_operation_succeeded, encode_publication_error, encode_static_counter,
    encode_subscription_ready, encode_unavailable_image,
};
use deepmsg_cnc::error_log::compose_description;
use deepmsg_cnc::layout;
use deepmsg_cnc::{
    CncCreateError, CncFile, CounterManager, DistinctErrorLog, ToClientsTransmitter,
    ToDriverRingConsumer,
};
use std::sync::Arc;

use deepmsg_core::buffer::{AtomicBuffer, ReadWrite};
use deepmsg_core::clock::{self, CachedClock};

use crate::channel_uri::{ChannelUri, Transport};
use crate::clients::{ClientEvents, Clients, CounterLink};
use crate::config::{DriverConfig, TerminationPolicy};
use crate::ipc_publications::{IpcPublications, Now};
use crate::ipc_subscriptions::IpcSubscriptions;
use crate::media::receive_endpoint::ReceiveDestination;
use crate::native_resource_agent::StorageChecks;
use crate::network_publications::NetworkPublications;
use crate::publication_images::PublicationImages;
use crate::receive_endpoints::ReceiveChannelEndpoints;
use crate::receiver::{Receiver, ReceiverEvent};
use crate::send_endpoints::SendChannelEndpoints;
use crate::sender::Sender;
use crate::system_counters::{self, SystemCounterError, SystemCounters};
use crate::udp_channel::{
    IPC_PREFIX, UdpChannel, UdpChannelError, is_spy_channel, validate_destination_prefix,
    validate_destination_uri_params, validate_send_destination_uri,
};

/// At most one command per duty cycle
/// (`aeron-driver/src/main/c/aeron_driver_context.h:53`).
pub const COMMAND_DRAIN_LIMIT: usize = 1;

/// How often the cached epoch-millisecond value is refreshed
/// (`AERON_DRIVER_CONDUCTOR_CLOCK_UPDATE_INTERNAL_NS` = 1 ms,
/// `aeron-driver/src/main/c/aeron_driver_conductor.h:38`).
pub const CLOCK_UPDATE_INTERVAL_NS: i64 = 1_000_000;

/// A command in the control protocol.
///
/// The ids are the record header's `msg_type_id`
/// (`aeron-client/src/main/c/command/aeron_control_protocol.h:26-44`). They are
/// listed in full rather than only the ones implemented, so that a command
/// arriving here is named in a report instead of being an anonymous number.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Command {
    /// `0x01`.
    AddPublication,
    /// `0x02`.
    RemovePublication,
    /// `0x03`.
    AddExclusivePublication,
    /// `0x04`.
    AddSubscription,
    /// `0x05`.
    RemoveSubscription,
    /// `0x06`.
    ClientKeepalive,
    /// `0x07`.
    AddDestination,
    /// `0x08`.
    RemoveDestination,
    /// `0x09`.
    AddCounter,
    /// `0x0A`.
    RemoveCounter,
    /// `0x0B`.
    ClientClose,
    /// `0x0C`.
    AddReceiveDestination,
    /// `0x0D`.
    RemoveReceiveDestination,
    /// `0x0E`.
    TerminateDriver,
    /// `0x0F`.
    AddStaticCounter,
    /// `0x10`.
    RejectImage,
    /// `0x11`.
    RemoveDestinationById,
    /// `0x12`.
    GetNextAvailableSessionId,
    /// A type id the protocol does not define.
    Unknown(i32),
}

impl Command {
    /// Name the command a record's type id carries.
    pub const fn from_type_id(type_id: i32) -> Self {
        match type_id {
            0x01 => Self::AddPublication,
            0x02 => Self::RemovePublication,
            0x03 => Self::AddExclusivePublication,
            0x04 => Self::AddSubscription,
            0x05 => Self::RemoveSubscription,
            0x06 => Self::ClientKeepalive,
            0x07 => Self::AddDestination,
            0x08 => Self::RemoveDestination,
            0x09 => Self::AddCounter,
            0x0A => Self::RemoveCounter,
            0x0B => Self::ClientClose,
            0x0C => Self::AddReceiveDestination,
            0x0D => Self::RemoveReceiveDestination,
            0x0E => Self::TerminateDriver,
            0x0F => Self::AddStaticCounter,
            0x10 => Self::RejectImage,
            0x11 => Self::RemoveDestinationById,
            0x12 => Self::GetNextAvailableSessionId,
            other => Self::Unknown(other),
        }
    }

    /// The name the reference's command table gives it, for reports.
    pub const fn name(self) -> &'static str {
        match self {
            Self::AddPublication => "ADD_PUBLICATION",
            Self::RemovePublication => "REMOVE_PUBLICATION",
            Self::AddExclusivePublication => "ADD_EXCLUSIVE_PUBLICATION",
            Self::AddSubscription => "ADD_SUBSCRIPTION",
            Self::RemoveSubscription => "REMOVE_SUBSCRIPTION",
            Self::ClientKeepalive => "CLIENT_KEEPALIVE",
            Self::AddDestination => "ADD_DESTINATION",
            Self::RemoveDestination => "REMOVE_DESTINATION",
            Self::AddCounter => "ADD_COUNTER",
            Self::RemoveCounter => "REMOVE_COUNTER",
            Self::ClientClose => "CLIENT_CLOSE",
            Self::AddReceiveDestination => "ADD_RCV_DESTINATION",
            Self::RemoveReceiveDestination => "REMOVE_RCV_DESTINATION",
            Self::TerminateDriver => "TERMINATE_DRIVER",
            Self::AddStaticCounter => "ADD_STATIC_COUNTER",
            Self::RejectImage => "REJECT_IMAGE",
            Self::RemoveDestinationById => "REMOVE_DESTINATION_BY_ID",
            Self::GetNextAvailableSessionId => "GET_NEXT_AVAILABLE_SESSION_ID",
            Self::Unknown(_) => "UNKNOWN",
        }
    }
}

/// Why a conductor could not take over a CnC file.
#[derive(Debug)]
pub enum ConductorError {
    /// The sender thread could not be started.
    Sender(std::io::Error),
    /// The ready version could not be stored.
    Publish(CncCreateError),
    /// The to-driver region is not a ring this build can consume. Validation
    /// accepts only lengths that describe one, so reaching this means the file
    /// was not created by this build.
    NoCommandRing,
    /// The to-clients region is not a broadcast ring this build can write. Same
    /// reasoning as [`ConductorError::NoCommandRing`].
    NoEventRing,
    /// The counter regions cannot carry the counters this driver must publish.
    NoCounterRegions,
    /// A system counter could not be allocated — see [`SystemCounterError`].
    /// The file must not be published: the counters are the contract.
    SystemCounters(SystemCounterError),
    /// The native resource agent — the thread that creates log buffers — could
    /// not be started. Nothing can be published without it.
    Agent(std::io::Error),
}

impl std::fmt::Display for ConductorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Publish(error) => write!(f, "the CnC file could not be published: {error}"),
            Self::NoCommandRing => f.write_str("the to-driver region is not a command ring"),
            Self::NoEventRing => f.write_str("the to-clients region is not a broadcast ring"),
            Self::NoCounterRegions => {
                f.write_str("the counter regions cannot hold a driver's counters")
            }
            Self::SystemCounters(error) => {
                write!(f, "the system counters were not published: {error}")
            }
            Self::Agent(error) => write!(f, "the native resource agent did not start: {error}"),
            Self::Sender(error) => write!(f, "the sender thread did not start: {error}"),
        }
    }
}

impl std::error::Error for ConductorError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Publish(error) => Some(error),
            Self::SystemCounters(error) => Some(error),
            Self::Agent(error) | Self::Sender(error) => Some(error),
            Self::NoCommandRing | Self::NoEventRing | Self::NoCounterRegions => None,
        }
    }
}

/// The to-clients ring, as the client pool's event sink.
///
/// Sends, and counts what it could not send. The count is *pending* rather than
/// written straight into system counter 15, because the counter needs a borrow
/// this sink holds mutably; [`Conductor::flush_broadcast_failures`] applies it
/// once the sink is gone. A failure here is not fatal — the reference logs it
/// and carries on (`aeron_driver_conductor.c:2233-2241`), and the failure mode
/// that matters, a client whose response never arrived, is a client timeout.
struct Transmit<'a> {
    transmitter: &'a mut ToClientsTransmitter,
    region: &'a AtomicBuffer<'a, ReadWrite>,
    failures: &'a mut u64,
    /// Errors this pass noticed but cannot record while it runs: recording
    /// takes the file's error-log window, and the pass is already holding the
    /// file's other windows. The conductor drains this once the pass is over.
    faults: &'a mut Vec<(i32, String)>,
}

impl Transmit<'_> {
    fn send(&mut self, type_id: i32, payload: &[u8]) {
        if self
            .transmitter
            .transmit(self.region, type_id, payload)
            .is_err()
        {
            *self.failures += 1;
        }
    }

    /// Leave an error for the conductor to record after the pass.
    ///
    /// The code is the one the log is written under — for an `ON_ERROR` that
    /// is the **protocol** code the client is told, not the negated code a
    /// recording site set, because the reference's `log_explicit_error` is
    /// handed the former (`aeron_driver_conductor.c:2369` takes `code`, not
    /// `errcode`).
    fn record_fault(&mut self, error_code: i32, description: String) {
        self.faults.push((error_code, description));
    }
}

impl ClientEvents for Transmit<'_> {
    fn counter_ready(&mut self, registration_id: i64, counter_id: i32) {
        let payload = encode_counter_update(registration_id, counter_id);
        self.send(ON_COUNTER_READY_TYPE_ID, &payload);
    }

    fn counter_unavailable(&mut self, registration_id: i64, counter_id: i32) {
        let payload = encode_counter_update(registration_id, counter_id);
        self.send(ON_UNAVAILABLE_COUNTER_TYPE_ID, &payload);
    }

    fn client_timed_out(&mut self, client_id: i64) {
        let payload = encode_client_timeout(client_id);
        self.send(ON_CLIENT_TIMEOUT_TYPE_ID, &payload);
    }

    fn operation_succeeded(&mut self, correlation_id: i64) {
        let payload = encode_operation_succeeded(correlation_id);
        self.send(ON_OPERATION_SUCCEEDED_TYPE_ID, &payload);
    }

    /// Answer with an `ON_ERROR` — and record it, because the reference does
    /// not do the one without the other.
    ///
    /// `aeron_driver_conductor_on_error` transmits the response and then falls
    /// through to its own `log_error:` label, which records the error and
    /// raises the errors counter for every code but one
    /// (`aeron_driver_conductor.c:2366-2370`). So an `ON_ERROR` a client sees
    /// is always an entry in the distinct error log and a bump of counter 15
    /// — the two halves are one act, and this is where this build keeps them
    /// together.
    ///
    /// The words are diagnostic rather than contract (`docs/compat.md`, "The
    /// words that ride an `ON_ERROR`"), so the entry carries what the client
    /// was told. The reference carries its per-thread composition there
    /// instead, which is the same divergence, recorded in the same place.
    fn error(&mut self, correlation_id: i64, error_code: i32, message: &[u8]) {
        let payload = encode_error(correlation_id, error_code, message);
        self.send(ON_ERROR_TYPE_ID, &payload);

        self.record_fault(error_code, String::from_utf8_lossy(message).into_owned());
    }

    /// Answer with an `ON_PUBLICATION_ERROR` — and record it, because the
    /// reference does not do the one without the other.
    ///
    /// The entry is written *before* the message and in a different shape from
    /// an `ON_ERROR`'s: the reference builds one line of its own
    /// (`aeron_driver_conductor.c:2269-2287`) and hands it to
    /// `aeron_driver_conductor_log_explicit_error`, which records it verbatim
    /// and raises the errors counter (`:1203-1211`). There is no `AERON_SET_ERR`
    /// site in it, so unlike every other entry this one has no `[func,
    /// file:line]` half — the line itself is the whole of what is recorded.
    ///
    /// Every field of the command goes into it, including the ones the response
    /// does not carry (`destination_registration_id`) and the message with its
    /// length, so the entry says what arrived rather than what was sent.
    fn publication_error(&mut self, error: &PublicationError<'_>) {
        self.record_fault(
            error.error_code,
            format!(
                "onPublicationError: registrationId={}, destinationRegistrationId={}, \
                 sessionId={}, streamId={}, receiverId={}, groupId={}, errorCode={}, \
                 errorMessage={}",
                error.registration_id,
                error.destination_registration_id,
                error.session_id,
                error.stream_id,
                error.receiver_id,
                error.group_tag,
                error.error_code,
                String::from_utf8_lossy(error.message),
            ),
        );

        let payload = encode_publication_error(error);
        self.send(ON_PUBLICATION_ERROR_TYPE_ID, &payload);
    }

    fn publication_ready(&mut self, ready: &PublicationBuffersReady<'_>, is_exclusive: bool) {
        let payload = ready.encode();
        self.send(PublicationBuffersReady::type_id(is_exclusive), &payload);
    }

    fn subscription_ready(&mut self, registration_id: i64, channel_status_indicator_id: i32) {
        let payload = encode_subscription_ready(registration_id, channel_status_indicator_id);
        self.send(ON_SUBSCRIPTION_READY_TYPE_ID, &payload);
    }

    fn available_image(&mut self, ready: &ImageBuffersReady<'_>) {
        let payload = ready.encode();
        self.send(ON_AVAILABLE_IMAGE_TYPE_ID, &payload);
    }

    fn unavailable_image(
        &mut self,
        correlation_id: i64,
        subscription_registration_id: i64,
        stream_id: i32,
        channel: &[u8],
    ) {
        let payload = encode_unavailable_image(
            correlation_id,
            subscription_registration_id,
            stream_id,
            channel,
        );
        self.send(ON_UNAVAILABLE_IMAGE_TYPE_ID, &payload);
    }
}

/// Count a command whose payload is shorter than its own header, and describe
/// it for the error log in the reference adapter's words
/// (`aeron_driver_conductor.c:3231-3235`).
///
/// The code is recorded negated because the reference's `AERON_SET_ERR` is
/// given `-AERON_ERROR_CODE_MALFORMED_COMMAND` there and the log keeps
/// whatever that left — a detail of the in-process table, since the code never
/// reaches the region. The description carries the reference's composition —
/// the code's own line, then the recording site at the `AERON_SET_ERR`'s own
/// line — because that text is what `ErrorStat` prints and what dedup keys
/// on.
fn malformed_command(
    type_id: i32,
    payload_len: usize,
    malformed: &mut u64,
    faults: &mut Transmit<'_>,
) {
    *malformed += 1;
    faults.record_fault(
        -ERROR_CODE_MALFORMED_COMMAND,
        compose_description(
            -ERROR_CODE_MALFORMED_COMMAND,
            "aeron_driver_conductor_on_command",
            "aeron_driver_conductor.c",
            3232,
            &format!("command={type_id} too short: length={payload_len}"),
        ),
    );
}

/// The driver's control plane.
pub struct Conductor {
    /// The settings every publication's parameters default to, kept because
    /// they are read on the command path and not only at start-up.
    config: DriverConfig,
    /// The shared-memory file, behind an `Arc` because the data plane's agents
    /// each derive their own counter views over the same pages (P1-4). Every
    /// use below is a shared borrow, which is what makes that possible.
    cnc: Arc<CncFile>,
    commands: ToDriverRingConsumer,
    transmitter: ToClientsTransmitter,
    counters: CounterManager,
    /// What this driver allocated for itself, so its shutdown gives back
    /// exactly that (`aeron_system_counters_close`, `:3487`).
    system_counters: SystemCounters,
    /// The process's half of the distinct error log; the region it writes
    /// comes from the CnC file per call, like the counters.
    error_log: DistinctErrorLog,
    clients: Clients,
    /// The publications this driver owns, and the thread that maps their log
    /// buffers.
    publications: IpcPublications,
    /// The subscriptions reading them.
    subscriptions: IpcSubscriptions,
    /// The send endpoints a network publication shares, one per canonical
    /// channel (`aeron_driver_conductor.c:1961-2030`).
    send_endpoints: SendChannelEndpoints,
    /// The publications that send over UDP, and the thread that maps their log
    /// buffers.
    network_publications: NetworkPublications,
    /// The sender thread, whose proxy is how anything reaches it.
    sender: Sender,
    /// The receive endpoints a subscription listens on, one per canonical
    /// channel (`aeron_driver_conductor.c:2046-2115`).
    receive_endpoints: ReceiveChannelEndpoints,
    /// The images built from datagrams, and the thread that maps their log
    /// buffers.
    images: PublicationImages,
    /// The receiver thread.
    receiver: Receiver,
    termination: TerminationPolicy,
    timer_interval_ns: i64,
    liveness_timeout_ns: i64,
    clock: CachedClock,
    clock_update_deadline_ns: i64,
    timeout_check_deadline_ns: i64,
    /// When the previous pass started, for the cycle-time counters.
    last_cycle_ns: i64,
    /// The command ring's consumer position as of the last pass that saw it
    /// move, and when that was — the two halves of the stall detector
    /// (`aeron_driver_conductor.c:3246-3266`).
    last_command_consumer_position: i64,
    time_of_last_position_change_ns: i64,
    now_ms: i64,
    running: bool,
    /// Broadcast failures not yet added to system counter 15.
    pending_broadcast_failures: u64,
    broadcast_failures: u64,
    counter_failures: u64,
    /// `ADD_PUBLICATION`s this driver could not serve.
    publication_failures: u64,
    /// `ADD_SUBSCRIPTION`s this driver could not serve.
    subscription_failures: u64,
    malformed: u64,
    unknown_counters: u64,
    unhandled: u64,
    unknown: u64,
    last_unhandled: Option<Command>,
    /// Errors this pass noticed but could not record yet — the command
    /// adapter's own faults, and every error it answered a client with. The
    /// pass holds the CnC file's other windows while it runs, so these wait
    /// for it, the way broadcast failures do.
    pending_log_errors: Vec<(i32, String)>,
    /// Readers a publication put aside, woke or closed, waiting for the pass
    /// that can send a client a message about them
    /// (`aeron_network_publication_check_untethered_subscriptions`,
    /// `aeron_network_publication.c:1120-1236`).
    ///
    /// The machine runs on the **sender**, because what it moves is the
    /// publication's own set of readers; what it produces is three client
    /// messages, which are the conductor's. The queue is that hand-off.
    pending_untethered: Vec<(i64, Vec<crate::subscribable::UntetheredEvent>)>,
    /// Publications a receiver refused, as the sender heard it: each owes its
    /// client an `ON_PUBLICATION_ERROR`, and the words have to outlive the
    /// datagram they arrived in.
    pending_publication_errors: Vec<deepmsg_cnc::command::OwnedPublicationError>,
}

impl Conductor {
    /// Take over a freshly created CnC file and publish it as ready.
    ///
    /// The order here is the reference's, and it is the reason
    /// [`CncFile::publish`] is a separate step: allocate the counters the file
    /// must carry (`aeron_driver_conductor.c:697-718`), burn a correlation id
    /// (`aeron-driver/src/main/c/aeron_driver.c:970` — which is why the first
    /// client sees id 1 rather than 0), publish the driver's liveness on the
    /// to-driver ring (`:971`), and only then store the ready version (`:972`)
    /// and flush it (`:973`).
    ///
    /// # Errors
    ///
    /// [`ConductorError`] if the file cannot hold the counters, the rings are
    /// not rings, or the file cannot be published.
    pub fn new(cnc: CncFile, config: &DriverConfig) -> Result<Self, ConductorError> {
        let commands = {
            let region = cnc
                .to_driver_region()
                .ok_or(ConductorError::NoCommandRing)?;
            ToDriverRingConsumer::new(&region.as_read_only())
                .ok_or(ConductorError::NoCommandRing)?
        };
        let transmitter = {
            let region = cnc
                .to_clients_region_writable()
                .ok_or(ConductorError::NoEventRing)?;
            ToClientsTransmitter::new(&region).ok_or(ConductorError::NoEventRing)?
        };

        // Two readings with two jobs, and they cannot be the same one: `now_ms`
        // is a date that goes into shared memory, while `now_ns` seeds the duty
        // cycle's deadlines, which are only ever compared against other
        // monotonic readings. Seeding those from the epoch would put every
        // deadline 1.7e18 nanoseconds in the future and freeze the clock cache
        // and the timeout tier with it.
        let mut clock = CachedClock::new();
        let now_ms = clock.update(clock::epoch_nano_time());
        let now_ns = clock::monotonic_nano_time();

        let mut counters = CounterManager::new(
            cnc.layout().counters_values.len(),
            free_to_reuse_ms(config.counter_free_to_reuse_ns),
        )
        .ok_or(ConductorError::NoCounterRegions)?;

        let owned_counters = {
            let regions = cnc
                .counter_regions()
                .ok_or(ConductorError::NoCounterRegions)?;
            #[allow(clippy::cast_possible_wrap)] // a file length, far below i64::MAX
            let bytes_mapped = cnc.file_length() as i64;
            system_counters::allocate_all(&mut counters, &regions, now_ms, bytes_mapped)
                .map_err(ConductorError::SystemCounters)?
        };

        // The id the driver burns at startup belongs to the same counter a
        // client takes its client id from (`aeron-driver/src/main/c/aeron_driver.c:970`),
        // which is why the first client sees id 1 rather than 0. The ring was
        // proved readable twenty lines above, so a `None` here means the file
        // changed underneath this process — and starting anyway would hand that
        // first client id 0, a byte-level divergence with no other symptom.
        let ring = cnc.to_driver_ring().ok_or(ConductorError::NoCommandRing)?;
        ring.next_correlation_id()
            .ok_or(ConductorError::NoCommandRing)?;

        // Where the command ring stands now, so that the stall detector starts
        // from a position rather than from zero: the reference seeds the same
        // field the same way (`aeron_driver_conductor.c:829`).
        let last_command_consumer_position = cnc
            .to_driver_region()
            .and_then(|region| commands.consume_position(&region))
            .unwrap_or(0);

        // The agent thread comes up before the CnC file is published: a driver
        // that cannot create log buffers is a driver that must not claim to be
        // ready.
        let publications = IpcPublications::start(
            config.publication_reserved_session_id_low,
            config.publication_reserved_session_id_high,
            StorageChecks::new(
                config.perform_storage_checks,
                config.low_file_store_warning_threshold,
                config.aeron_dir.clone(),
            ),
        )
        .map_err(ConductorError::Agent)?;

        // The network side: the sender thread first, because the publications
        // manager hands it what it creates, then the manager itself (which is
        // another agent thread — the one that maps *network* log buffers).
        // The heartbeat and the ready version, *before* the file is shared:
        // `publish` updates the in-memory metadata as well as the mapping, and
        // after the `Arc` below there is no longer a mutable reference to it.
        // The order is the contract (`aeron-driver/src/main/c/aeron_driver.c:971-972`)
        // and it is enforced inside `publish`, which refuses a file whose
        // heartbeat is still zero.
        {
            let region = cnc
                .to_driver_region()
                .ok_or(ConductorError::NoCommandRing)?;
            let written = commands.write_consumer_heartbeat(&region, now_ms);
            debug_assert!(written.is_some(), "the region was validated above");
        }

        let mut cnc = cnc;
        cnc.publish().map_err(ConductorError::Publish)?;

        // The file moves into the `Arc` here, which is what lets the agents
        // below share it; everything above this line borrowed it directly.
        let cnc = Arc::new(cnc);

        let sender = Sender::start(
            Arc::clone(&cnc),
            // The counters region's length is what fixes a counter id's meaning
            // on both sides of the handover.
            cnc.layout().counters_values.len(),
            free_to_reuse_ms(config.counter_free_to_reuse_ns),
            usize::try_from(config.mtu_length).unwrap_or(1408),
            system_counters::CONDUCTOR_CYCLE_THRESHOLD_NS,
        )
        .map_err(ConductorError::Sender)?;

        let network_publications = NetworkPublications::start(
            config.publication_reserved_session_id_low,
            config.publication_reserved_session_id_high,
            StorageChecks::new(
                config.perform_storage_checks,
                config.low_file_store_warning_threshold,
                config.aeron_dir.clone(),
            ),
        )
        .map_err(ConductorError::Agent)?;

        // The receive side: the receiver thread first (it owns the sockets and
        // the images), then the images manager, which is the other agent thread
        // — the one that maps an *image's* log buffer.
        let receiver = Receiver::start(
            Arc::clone(&cnc),
            cnc.layout().counters_values.len(),
            free_to_reuse_ms(config.counter_free_to_reuse_ns),
            usize::try_from(config.mtu_length).unwrap_or(1408),
            config.status_message_timeout_ns,
            config.receiver_window_length,
            system_counters::CONDUCTOR_CYCLE_THRESHOLD_NS,
        )
        .map_err(ConductorError::Sender)?;

        let images = PublicationImages::start(StorageChecks::new(
            config.perform_storage_checks,
            config.low_file_store_warning_threshold,
            config.aeron_dir.clone(),
        ))
        .map_err(ConductorError::Agent)?;

        let conductor = Self {
            config: config.clone(),
            cnc,
            commands,
            transmitter,
            counters,
            system_counters: owned_counters,
            error_log: DistinctErrorLog::new(),
            clients: Clients::new(),
            publications,
            subscriptions: IpcSubscriptions::new(),
            send_endpoints: {
                // The supplier (`aeron_driver_context.c:2979-2989`), set at
                // start-up: every endpoint made from here on carries a loss
                // generator of its own. Nothing is attached unless the driver
                // was configured to inject loss.
                let mut endpoints = SendChannelEndpoints::new();
                if let Some(drop_every) = config.data_loss_drop_every {
                    endpoints.attach_data_loss_generator(drop_every);
                }
                endpoints
            },
            network_publications,
            sender,
            receive_endpoints: ReceiveChannelEndpoints::new(),
            images,
            receiver,
            termination: config.termination,
            timer_interval_ns: config.timer_interval_ns,
            liveness_timeout_ns: config.client_liveness_timeout_ns,
            clock,
            clock_update_deadline_ns: now_ns.saturating_add(CLOCK_UPDATE_INTERVAL_NS),
            // Seeded to now, so the first pass runs the timeout tier: the
            // reference does the same (`aeron_driver_conductor.c:824`), and it
            // means the heartbeat is set before anything can read the version.
            timeout_check_deadline_ns: now_ns,
            last_cycle_ns: now_ns,
            last_command_consumer_position,
            time_of_last_position_change_ns: now_ns,
            now_ms,
            running: true,
            pending_broadcast_failures: 0,
            broadcast_failures: 0,
            counter_failures: 0,
            publication_failures: 0,
            subscription_failures: 0,
            malformed: 0,
            unknown_counters: 0,
            unhandled: 0,
            unknown: 0,
            last_unhandled: None,
            pending_log_errors: Vec::new(),
            pending_untethered: Vec::new(),
            pending_publication_errors: Vec::new(),
        };

        Ok(conductor)
    }

    /// One duty cycle. The return value is the reference's `work_count`.
    pub fn do_work(&mut self) -> usize {
        // The cycle's deadlines are measured against a clock that cannot go
        // backwards, and `now_ms` — which every timestamp another process reads
        // is built from — against the epoch. The reference keeps the same two
        // apart: a monotonic `nano_clock` for the driver's timing, and a
        // realtime reading taken once per millisecond for the cache
        // (`aeron_driver_conductor.c:3276-3280`).
        let now_ns = clock::monotonic_nano_time();
        let mut work_count = 0;

        self.track_cycle(now_ns);

        if now_ns > self.clock_update_deadline_ns {
            self.now_ms = self.clock.update(clock::epoch_nano_time());
            self.clock_update_deadline_ns = now_ns.saturating_add(CLOCK_UPDATE_INTERVAL_NS);
        }

        if now_ns > self.timeout_check_deadline_ns {
            self.write_heartbeat();
            work_count += 1;
            work_count += self.check_clients();
            work_count += self.check_publications(now_ns);
            work_count += usize::from(self.check_for_blocked_commands(now_ns));
            self.timeout_check_deadline_ns = now_ns.saturating_add(self.timer_interval_ns);
        }

        let work = work_count
            + self.process_commands(now_ns)
            + self.poll_publications()
            + self.update_publication_limits()
            + self.poll_receiver(now_ns);
        // What the pass noticed and could not record while it held the file:
        // the errors behind the `ON_ERROR`s it sent, the command adapter's own
        // faults, the storage warnings, and the broadcasts the ring refused.
        // The reference records each of these inside the pass; nothing outside
        // the pass can tell the difference, and a window cannot be held twice.
        self.record_pending_faults();
        self.record_storage_warnings();
        self.flush_broadcast_failures();
        work
    }

    /// Write every publication's `pub-pos`, recompute its `pub-lmt` from its
    /// readers, and clean what they have finished with
    /// (`aeron_driver_conductor.c:3401-3404`, at the end of the reference's own
    /// duty cycle).
    ///
    /// This is what makes an IPC publication *run*: nothing else writes
    /// `pub-pos`, so without it a producer's position counter never moves and
    /// its limit stays where the create left it — zero — which reports
    /// back-pressure for ever.
    fn update_publication_limits(&mut self) -> usize {
        let Some(counter_regions) = self.cnc.counter_regions() else {
            return 0;
        };

        self.publications
            .update_limits(&mut self.counters, &counter_regions)
    }

    /// Take the native resource agent's completions: this is where a
    /// publication whose log buffer was still being created becomes one, and
    /// where its client is answered.
    ///
    /// The reference polls its agent from the same duty cycle
    /// (`aeron_driver_conductor.c:4042-4067` calls it from `do_work`'s main
    /// sequence), and for the same reason: the create has to happen on the
    /// conductor's thread, where the command ring and the counters are.
    fn poll_publications(&mut self) -> usize {
        // The order matters and is left to right: the events are taken off the
        // sender first, and what was taken is what the flushes send.
        let mut work =
            self.poll_sender_events() + self.flush_untethered() + self.flush_publication_errors();

        if self.publications.pending() == 0 && self.network_publications.pending() == 0 {
            return work;
        }

        let Some(counter_regions) = self.cnc.counter_regions() else {
            return 0;
        };
        let Some(event_region) = self.cnc.to_clients_region_writable() else {
            return 0;
        };

        let now = Now {
            // `ms` is a date and goes into shared memory; `ns` is only ever
            // compared against other `ns` readings, so it is monotonic.
            ms: self.now_ms,
            ns: clock::monotonic_nano_time(),
            client_liveness_timeout_ns: self.liveness_timeout_ns,
        };

        let mut transmit = Transmit {
            transmitter: &mut self.transmitter,
            region: &event_region,
            failures: &mut self.pending_broadcast_failures,
            faults: &mut self.pending_log_errors,
        };

        work += self.publications.poll(
            &self.config,
            &mut self.counters,
            &counter_regions,
            &mut self.clients,
            &mut self.subscriptions,
            now,
            &mut transmit,
        );

        // The network side's own pending list, whose create also hands the
        // publication to the sender.
        work += self.network_publications.poll(
            &self.config,
            &mut self.counters,
            &counter_regions,
            &mut self.clients,
            &mut self.subscriptions,
            self.sender.proxy(),
            self.receiver.proxy(),
            now,
            &mut transmit,
        );

        work
    }

    /// What the receiver thread has to say, and what follows from it.
    ///
    /// Two of its events are *commands in disguise*: a `SETUP` that arrived for
    /// a session nothing serves is a request to build an image — the reference
    /// sends it the same way, over its conductor proxy — and an image that has
    /// finished its life is a request to release it. The third is a fault, for
    /// the error log this thread does not write itself.
    ///
    /// The create itself is like every other create here: it burns a
    /// registration id, asks the agent for a log buffer off-thread, and the
    /// image is built when the buffer lands (`poll_images`).
    fn poll_receiver(&mut self, now_ns: i64) -> usize {
        let events = self.receiver.proxy().poll();
        let mut work = 0;

        for event in events {
            work += 1;

            match event {
                ReceiverEvent::CreateImage {
                    endpoint_id,
                    stream_id,
                    session_id,
                    initial_term_id,
                    active_term_id,
                    term_offset,
                    term_length,
                    mtu,
                    setup_flags,
                    control_address,
                    source,
                } => {
                    let Some(ring) = self.cnc.to_driver_ring() else {
                        continue;
                    };
                    let Some(registration_id) = ring.next_correlation_id() else {
                        continue;
                    };

                    let Some(entry) = self.receive_endpoints.get(endpoint_id) else {
                        continue;
                    };
                    let channel = entry.channel.original_uri.clone();

                    let setup = crate::protocol::SetupFrame {
                        term_offset,
                        session_id,
                        stream_id,
                        initial_term_id,
                        active_term_id,
                        term_length,
                        mtu,
                        ttl: 0,
                    };

                    let now = Now {
                        ms: self.now_ms,
                        ns: now_ns,
                        client_liveness_timeout_ns: self.liveness_timeout_ns,
                    };

                    let Some(regions) = self.cnc.counter_regions() else {
                        continue;
                    };

                    // The image's counters belong to the client that asked for
                    // the subscription, not to the driver
                    // (`aeron_driver_conductor.c:6652`, `:6667`, `:6682`, each
                    // passing `subscription_link->client_id`). The reference
                    // carries the link on the command itself; here the link is
                    // the one this image is for — the same stream, and either
                    // it named no session or it named this one. A stream two
                    // clients both read is owned by the first of them, which is
                    // the same one-link answer the reference gives.
                    //
                    // The link is also where `group=` was left, for the same
                    // reason the reference keeps it there: the `SETUP` that
                    // creates this image arrives long after the subscription
                    // that will read it (`aeron_driver_conductor.c:6702-6703`).
                    // With no link there is nobody's `group=` to read, so the
                    // driver's own consideration stands.
                    let link = self.subscriptions.links().iter().find(|link| {
                        link.stream_id == setup.stream_id
                            && link
                                .session_id
                                .is_none_or(|session_id| session_id == setup.session_id)
                    });

                    let client_id = link.map_or(0, |link| link.client_id);
                    let is_group =
                        link.map_or(self.config.receiver_group_consideration, |link| link.group);

                    let result = self.images.begin_create(
                        registration_id,
                        client_id,
                        endpoint_id,
                        &channel,
                        &setup,
                        setup_flags,
                        entry.channel.is_multicast,
                        is_group,
                        source,
                        control_address,
                        // The window this receiver offers and the socket it
                        // offers it through, both read off the *endpoint's*
                        // channel (`aeron_driver_conductor.c:6496-6497`,
                        // `:6506`).
                        ReceiveChannelEndpoints::initial_window_length(
                            &self.config,
                            &entry.channel,
                        ),
                        entry.socket_rcvbuf,
                        &self.config,
                        &mut self.counters,
                        &regions,
                        now,
                    );

                    if let Err(error) = result {
                        self.pending_log_errors
                            .push((error.recorded_error_code(), error.to_string()));
                    }
                }
                ReceiverEvent::ImageDone { registration_id } => {
                    work += self.release_image(registration_id);
                }
                ReceiverEvent::Untethered {
                    registration_id,
                    events,
                } => {
                    work += self.on_untethered(registration_id, &events);
                }
                ReceiverEvent::Fault {
                    error_code,
                    description,
                } => {
                    self.pending_log_errors.push((error_code, description));
                }
            }
        }

        work + self.poll_images(now_ns)
    }

    /// The untethered state machine moved one or more readers of an image
    /// (`aeron_publication_image_check_untethered_subscriptions`'s three
    /// outcomes, `aeron-driver/src/main/c/aeron_publication_image.c:1199-1270`).
    ///
    /// Three client-visible events, and they are not symmetric. A reader put
    /// aside is told its image is gone; a reader woken is told the image is
    /// there again, at the join position — which is why the message it gets is
    /// the same `ON_AVAILABLE_IMAGE` it got when it first linked. A reader that
    /// was not rejoining is told *nothing*: its counter is freed and its
    /// subscription keeps the images it has.
    fn on_untethered(
        &mut self,
        registration_id: i64,
        events: &[crate::publication_image::UntetheredEvent],
    ) -> usize {
        let Some(image) = self.images.find(registration_id).cloned() else {
            return 0;
        };

        let Some(event_region) = self.cnc.to_clients_region_writable() else {
            return 0;
        };

        let mut transmit = Transmit {
            transmitter: &mut self.transmitter,
            region: &event_region,
            failures: &mut self.pending_broadcast_failures,
            faults: &mut self.pending_log_errors,
        };

        let mut work = 0;

        for event in events {
            work += 1;

            match *event {
                crate::publication_image::UntetheredEvent::Unavailable {
                    subscription_registration_id,
                    ..
                } => {
                    // The channel is the *subscription's*, which is what the
                    // reference's image-transition path sends
                    // (`aeron_driver_conductor.c:1657-1663`).
                    let channel = self
                        .subscriptions
                        .links()
                        .iter()
                        .find(|link| link.registration_id == subscription_registration_id)
                        .map(|link| link.channel.clone())
                        .unwrap_or_else(|| image.channel.clone());

                    transmit.unavailable_image(
                        registration_id,
                        subscription_registration_id,
                        image.stream_id,
                        &channel,
                    );
                }
                crate::publication_image::UntetheredEvent::Available {
                    subscription_registration_id,
                    counter_id,
                    ..
                } => {
                    transmit.available_image(&deepmsg_cnc::command::ImageBuffersReady {
                        correlation_id: registration_id,
                        session_id: image.session_id,
                        stream_id: image.stream_id,
                        subscriber_registration_id: subscription_registration_id,
                        subscriber_position_id: counter_id,
                        log_file: image.path.as_os_str().as_encoded_bytes(),
                        source_identity: image.source_identity.as_bytes(),
                    });
                }
                crate::publication_image::UntetheredEvent::Closed { counter_id } => {
                    if let Some(region) = self.cnc.counter_regions() {
                        let _ = self.counters.free(&region, counter_id, self.now_ms);
                    }
                }
            }
        }

        work
    }

    /// Let go of a network publication: stop sending it, give its counters
    /// back, and count one less reader on its endpoint
    /// (`aeron_network_publication_close`,
    /// `aeron-driver/src/main/c/aeron_network_publication.c:326-354`).
    ///
    /// The IPC path does the same for its own publications
    /// ([`IpcPublications::release_links`]); this is the half that was missing,
    /// and its absence was silent: a client could remove a UDP publication and
    /// the sender would keep sending it.
    fn release_network_publication(&mut self, registration_id: i64) -> bool {
        let Some(record) = self.network_publications.remove(registration_id) else {
            return false;
        };

        let _ = self.sender.proxy().remove_publication(registration_id);

        self.release_spies_of(registration_id);

        if let Some(region) = self.cnc.counter_regions() {
            let mut counter_ids = vec![
                record.counters.pub_pos,
                record.counters.pub_lmt,
                record.counters.snd_pos,
                record.counters.snd_lmt,
                record.counters.snd_bpe,
                record.counters.snd_naks_received,
            ];

            // Only a strategy that keeps receivers was given one
            // (`aeron_min_flow_control.c:454-470` frees it with the strategy).
            counter_ids.extend(record.counters.fc_receivers);

            for counter_id in counter_ids {
                let _ = self.counters.free(&region, counter_id, self.now_ms);
            }
        }

        self.send_endpoints.detach_publication(record.endpoint_id);

        true
    }

    /// Send what a publication's tether cycle decided to the readers it
    /// decided it about
    /// (`aeron_network_publication_check_untethered_subscriptions`'s three
    /// outcomes, `aeron_network_publication.c:1151-1208`).
    ///
    /// The three are not symmetric, and the asymmetry is the reference's: a
    /// reader put aside is told its image is gone; a reader woken is told the
    /// image is there again, at `snd-pos`; and a reader that was **not**
    /// rejoining is told nothing at all — its counter goes back and its
    /// subscription keeps whatever images it has left.
    ///
    /// The channel on the unavailable message is the IPC **constant**, not the
    /// channel the client spied with (`:1156`). Both publication-side machines
    /// send it that way — the reader is holding a mapping of a log buffer
    /// rather than a description of a channel — and it is the same constant an
    /// image that is being linked carries.
    fn flush_untethered(&mut self) -> usize {
        if self.pending_untethered.is_empty() {
            return 0;
        }

        let pending = std::mem::take(&mut self.pending_untethered);

        let Some(event_region) = self.cnc.to_clients_region_writable() else {
            return 0;
        };
        let Some(counter_regions) = self.cnc.counter_regions() else {
            return 0;
        };

        let mut transmit = Transmit {
            transmitter: &mut self.transmitter,
            region: &event_region,
            failures: &mut self.pending_broadcast_failures,
            faults: &mut self.pending_log_errors,
        };

        let mut work = 0;

        for (registration_id, events) in &pending {
            // A publication that has gone since the machine ran is one whose
            // readers were told so by the removal itself; what is left here is
            // a message about a buffer nobody holds.
            let Some(publication) = self.network_publications.find(*registration_id) else {
                continue;
            };

            for event in events {
                work += 1;

                match *event {
                    crate::subscribable::UntetheredEvent::Unavailable {
                        subscription_registration_id,
                        ..
                    } => {
                        transmit.unavailable_image(
                            *registration_id,
                            subscription_registration_id,
                            publication.stream_id,
                            crate::ipc_subscriptions::IPC_CHANNEL,
                        );
                    }
                    crate::subscribable::UntetheredEvent::Available {
                        subscription_registration_id,
                        counter_id,
                        ..
                    } => {
                        // The same message a reader that has just linked gets,
                        // down to the source identity: a woken reader cannot
                        // tell the difference, which is the point of waking it
                        // this way rather than inventing a second message.
                        transmit.available_image(&ImageBuffersReady {
                            correlation_id: *registration_id,
                            session_id: publication.session_id,
                            stream_id: publication.stream_id,
                            subscriber_registration_id: subscription_registration_id,
                            subscriber_position_id: counter_id,
                            log_file: publication.path.as_os_str().as_encoded_bytes(),
                            source_identity: crate::ipc_subscriptions::IPC_CHANNEL,
                        });
                    }
                    crate::subscribable::UntetheredEvent::Closed { counter_id } => {
                        let _ = self
                            .counters
                            .free(&counter_regions, counter_id, self.now_ms);
                    }
                }
            }
        }

        work
    }

    /// Tell the publications a receiver refused, in the words that receiver
    /// used (`aeron_driver_conductor_on_publication_error`,
    /// `aeron_driver_conductor.c:2263-2323`).
    ///
    /// The handler is the same one the IPC path reaches through
    /// `aeron_ipc_publication_reject` (`aeron_ipc_publication.c:249`) — one
    /// function, two ways in, which is why the response is built here from the
    /// same [`PublicationError`] either way.
    fn flush_publication_errors(&mut self) -> usize {
        if self.pending_publication_errors.is_empty() {
            return 0;
        }

        let pending = std::mem::take(&mut self.pending_publication_errors);

        let Some(event_region) = self.cnc.to_clients_region_writable() else {
            return 0;
        };

        let mut transmit = Transmit {
            transmitter: &mut self.transmitter,
            region: &event_region,
            failures: &mut self.pending_broadcast_failures,
            faults: &mut self.pending_log_errors,
        };

        for error in &pending {
            transmit.publication_error(&error.as_error());
        }

        pending.len()
    }

    /// Tell every spy reading a publication that it is gone, and give their
    /// readers back (`aeron_driver_conductor_cleanup_spies`, `:1502-1519`).
    ///
    /// The message goes out **before** the counters come back, which is the
    /// reference's order and the only one that works: the message names the
    /// channel the spy read with, and a client told about an image it no longer
    /// has is a client that stops advancing the position the publication's
    /// limit is computed from.
    ///
    /// Nothing is sent when there is no spy — the common case — because there
    /// is nothing to send it about; the link walk that finds that out is the
    /// same walk that would send.
    fn release_spies_of(&mut self, registration_id: i64) -> usize {
        let Some(event_region) = self.cnc.to_clients_region_writable() else {
            return 0;
        };
        let Some(counter_regions) = self.cnc.counter_regions() else {
            return 0;
        };

        let mut transmit = Transmit {
            transmitter: &mut self.transmitter,
            region: &event_region,
            failures: &mut self.pending_broadcast_failures,
            faults: &mut self.pending_log_errors,
        };

        self.subscriptions.unlink_spies_of(
            registration_id,
            &mut self.counters,
            &counter_regions,
            self.now_ms,
            &mut transmit,
        )
    }

    /// Whatever a client left behind: the network publications it was holding
    /// when it stopped being a client this driver knows.
    ///
    /// Called after the client paths that *remove* a record — a close and a
    /// timeout — rather than from inside them, because the release needs the
    /// sender's proxy and the endpoint registry, and neither belongs in the
    /// client pool.
    fn release_orphaned_network_publications(&mut self) -> usize {
        let orphans: Vec<i64> = self
            .network_publications
            .publications()
            .iter()
            .filter(|publication| !self.clients.knows(publication.client_id))
            .map(|publication| publication.registration_id)
            .collect();

        let mut released = 0;

        for registration_id in orphans {
            released += usize::from(self.release_network_publication(registration_id));
        }

        released
    }

    /// An image has finished its life: unlink it, tell its readers, give its
    /// counters and its log buffer back, and let the endpoint go if nothing
    /// reads it any more (`aeron_driver_conductor_image_transition_to_linger`
    /// and the delete that follows it, `aeron_driver_conductor.c:5680-5720`).
    ///
    /// This is the receiving side's answer to a publication's revoke: a
    /// subscriber is told the image is gone with `ON_UNAVAILABLE_IMAGE` — one
    /// message per **subscription** that was reading it, as the reference sends
    /// them (`:5690-5700`) — and only then is the log buffer unmapped.
    fn release_image(&mut self, registration_id: i64) -> usize {
        let Some(image) = self.images.find(registration_id).cloned() else {
            return 0;
        };

        let _ = self.receiver.proxy().remove_image(registration_id);

        // The readers, told — before anything is freed, because the message
        // names the file they were reading.
        let Some(event_region) = self.cnc.to_clients_region_writable() else {
            return 0;
        };

        let mut transmit = Transmit {
            transmitter: &mut self.transmitter,
            region: &event_region,
            failures: &mut self.pending_broadcast_failures,
            faults: &mut self.pending_log_errors,
        };

        for link in self.subscriptions.readers_of(registration_id) {
            transmit.unavailable_image(
                registration_id,
                link.registration_id,
                image.stream_id,
                &image.channel,
            );
        }

        // The transmit borrows the faults list and the ring; it goes out of
        // scope here so the counter regions can be taken below.
        let _ = &transmit;

        self.subscriptions.forget_publication(registration_id);
        self.receive_endpoints.detach_image(image.endpoint_id);

        // The counters and the log buffer. The image is gone from the
        // receiver, so nothing is reading either of them.
        if let Some(region) = self.cnc.counter_regions() {
            let _ = self
                .counters
                .free(&region, image.counters.rcv_hwm, self.now_ms);
            let _ = self
                .counters
                .free(&region, image.counters.rcv_pos, self.now_ms);
        }

        // The image's log buffer goes back through the agent, because a delete
        // unmaps and unlinks a file — the same rule every other log buffer
        // follows (`crate::native_resource_agent`).
        let _ = self.images.remove(registration_id);

        1
    }

    /// Take the image agent's completions: this is where an image whose log
    /// buffer was being created becomes one, where the subscriptions waiting
    /// for that stream are given it, and where each of them is told.
    fn poll_images(&mut self, now_ns: i64) -> usize {
        if self.images.pending() == 0 {
            return 0;
        }

        let Some(counter_regions) = self.cnc.counter_regions() else {
            return 0;
        };
        let Some(event_region) = self.cnc.to_clients_region_writable() else {
            return 0;
        };

        let now = Now {
            ms: self.now_ms,
            ns: now_ns,
            client_liveness_timeout_ns: self.liveness_timeout_ns,
        };

        let mut warnings = Vec::new();
        let mut transmit = Transmit {
            transmitter: &mut self.transmitter,
            region: &event_region,
            failures: &mut self.pending_broadcast_failures,
            faults: &mut self.pending_log_errors,
        };

        let created = self.images.poll(
            &self.config,
            &mut self.counters,
            &counter_regions,
            &mut self.receive_endpoints,
            self.receiver.proxy(),
            now,
            &mut warnings,
        );

        for registration_id in &created {
            // A link that fails is a subscription that will never read this
            // image, and the client is waiting for exactly that message: it is
            // recorded rather than dropped, because the alternative is a
            // driver that looks healthy and delivers nothing.
            let links = self.subscriptions.links().len();

            if self
                .subscriptions
                .link_new_image(
                    *registration_id,
                    &mut self.images,
                    &mut self.counters,
                    &counter_regions,
                    self.receiver.proxy(),
                    now,
                    &mut transmit,
                )
                .is_err()
            {
                transmit.record_fault(
                    deepmsg_cnc::command::ERROR_CODE_GENERIC_ERROR,
                    format!(
                        "could not link a subscription to image {registration_id} ({links} links)"
                    ),
                );
            }
        }

        for warning in warnings {
            // The words are the reference's own shape for a low-space warning
            // (`aeron_driver_context_run_storage_checks`), recorded where every
            // other fault this pass noticed is.
            transmit.record_fault(
                deepmsg_cnc::command::ERROR_CODE_GENERIC_ERROR,
                format!(
                    "usable fs space of {} bytes is below the {} byte threshold for {}",
                    warning.usable,
                    warning.threshold,
                    warning.dir.display()
                ),
            );
        }

        created.len()
    }

    /// What the sender thread has to say: an endpoint it closed, a publication
    /// it let go, and the faults it could not record itself (it has no error
    /// log of its own — the distinct log is the conductor's).
    ///
    /// The reference's arrangement is the same one seen from the other side:
    /// its sender calls `aeron_driver_sender_log_error` for exactly these, and
    /// that records into the shared log on the sender's own thread. This build
    /// keeps every writer of that log on one thread, which is why the fault
    /// travels here first.
    fn poll_sender_events(&mut self) -> usize {
        let events = self.sender.proxy().poll();
        let mut work = 0;

        for event in events {
            work += 1;

            match event {
                crate::sender::SenderEvent::Untethered {
                    registration_id,
                    events,
                } => {
                    self.pending_untethered.push((registration_id, events));
                }
                crate::sender::SenderEvent::Fault {
                    error_code,
                    description,
                } => {
                    self.pending_log_errors.push((error_code, description));
                }
                crate::sender::SenderEvent::PublicationError { error } => {
                    // A response rather than a fault: the publication's client
                    // is owed an `ON_PUBLICATION_ERROR`, which only the pass
                    // that holds the ring can write.
                    self.pending_publication_errors.push(error);
                }
                crate::sender::SenderEvent::ResponseSetup {
                    response_correlation_id,
                    response_session_id,
                } => {
                    if let Some((error_code, description)) = self.subscriptions.on_response_setup(
                        response_correlation_id,
                        response_session_id,
                        self.receiver.proxy(),
                    ) {
                        self.pending_log_errors.push((error_code, description));
                    }
                }
                crate::sender::SenderEvent::ResponseConnected {
                    response_correlation_id,
                } => {
                    // The image that owed this publication a response setup has
                    // now been answered — the publication has a live receiver,
                    // which is the end of the handshake — so it stops saying
                    // the session (`aeron_driver_conductor.c:7117-7131`).
                    //
                    // The reference sweeps every image and clears each match
                    // rather than stopping at the first, and clears whether or
                    // not the image's `SETUP` asked for a response channel: the
                    // lookup is by registration id alone. That is kept as it is,
                    // including its sharp edge — a *request* publication reports
                    // its subscription's registration id here, which is a
                    // different id space from an image's, so an id that names
                    // both would clear the wrong image's session.
                    if self.images.find(response_correlation_id).is_some() {
                        let _ = self.receiver.proxy().set_response_session_id(
                            response_correlation_id,
                            crate::publication_image::RESPONSE_NULL_SESSION_ID,
                        );
                    }
                }
                crate::sender::SenderEvent::EndpointRemoved { .. }
                | crate::sender::SenderEvent::PublicationRemoved { .. } => {
                    // The conductor's own bookkeeping for a removal arrives
                    // with the removal path (P1-4's last slice): an endpoint
                    // outlives its publications only until the reference
                    // count reaches zero, and that is the conductor's count.
                }
            }
        }

        work
    }

    /// Whether the driver should keep running.
    pub const fn is_running(&self) -> bool {
        self.running
    }

    /// The network publications this driver owns, for a caller that needs to
    /// look — a test, or a tool that wants the sessions it chose.
    pub fn network_publications(&self) -> &[crate::network_publications::NetworkPublicationRecord] {
        self.network_publications.publications()
    }

    /// The images this driver is reading, for a caller that needs to look.
    pub fn publication_images(&self) -> &[crate::publication_images::PublicationImageRecord] {
        self.images.images()
    }

    /// The counters this driver publishes. Read-only: only the conductor
    /// allocates and frees.
    pub const fn counters(&self) -> &CounterManager {
        &self.counters
    }

    /// The clients this driver knows about.
    pub const fn clients(&self) -> &Clients {
        &self.clients
    }

    /// The publications this driver owns.
    pub const fn publications(&self) -> &IpcPublications {
        &self.publications
    }

    /// The subscriptions reading this driver's publications.
    pub const fn subscriptions(&self) -> &IpcSubscriptions {
        &self.subscriptions
    }

    /// `ADD_PUBLICATION`s this driver could not serve.
    pub const fn publication_failures(&self) -> u64 {
        self.publication_failures
    }

    /// `ADD_SUBSCRIPTION`s this driver could not serve.
    pub const fn subscription_failures(&self) -> u64 {
        self.subscription_failures
    }

    /// Commands that are in the protocol and not implemented here yet.
    pub const fn unhandled_commands(&self) -> u64 {
        self.unhandled
    }

    /// Commands whose type id the protocol does not define.
    pub const fn unknown_commands(&self) -> u64 {
        self.unknown
    }

    /// Commands whose payload was shorter than their own lengths claimed.
    pub const fn malformed_commands(&self) -> u64 {
        self.malformed
    }

    /// `REMOVE_COUNTER`s for a counter this client does not own, including one
    /// from a client this driver has never seen.
    pub const fn unknown_counters(&self) -> u64 {
        self.unknown_counters
    }

    /// Broadcasts the to-clients ring refused. Never expected to be non-zero:
    /// every message this build sends is twelve bytes into a ring that holds
    /// far more.
    pub const fn broadcast_failures(&self) -> u64 {
        self.broadcast_failures
    }

    /// `ADD_COUNTER`s this driver could not serve: no client record, or no id
    /// left.
    pub const fn counter_failures(&self) -> u64 {
        self.counter_failures
    }

    /// The most recent command this driver could not serve, for a report.
    pub const fn last_unhandled(&self) -> Option<Command> {
        self.last_unhandled
    }

    /// Publish the shutdown signal and flush it.
    ///
    /// The heartbeat becomes `NULL_VALUE` rather than simply stopping: a
    /// client has to be able to tell "stopped on purpose" from "stopped being
    /// heard from" (`aeron-driver/src/main/c/aeron_driver_conductor.c:3493`,
    /// flushed at `:3494`). The reference does not reap its clients on the way
    /// out either — a client discovers the driver is gone from *this* field,
    /// not from a message — so neither does this.
    ///
    /// # Errors
    ///
    /// The error from flushing the mapping, if any.
    pub fn close(&mut self) -> std::io::Result<()> {
        // The counters the driver owns go first (`aeron_system_counters_close`,
        // called at `aeron_driver_conductor.c:3487`), and the heartbeat is
        // nulled after (`:3493`). It is why a driver that stopped on purpose
        // leaves forty-six *reclaimed* slots behind, and why a capture of a
        // stopped driver is a different file from one of a running driver —
        // the golden fixture is the running one.
        //
        // A client's counters are deliberately not freed, matching the
        // reference: it frees the client's link *arrays* on the way out and
        // leaves the counters themselves allocated.
        let Some(regions) = self.cnc.counter_regions() else {
            // A file this build created and validated does not lose its counter
            // regions; if it has, the shutdown is not the place to find out
            // quietly.
            self.running = false;
            return Err(std::io::Error::other(
                "the counter regions are unreachable at shutdown",
            ));
        };

        // The publications go first: each one gives its counters back and
        // hands its log buffer to the agent, which is the order the reference
        // closes in (`:3427-3436`) and the reason its shutdown leaves no
        // publication counters behind.
        self.publications
            .close(&mut self.counters, &regions, self.now_ms);
        // The network side: the thread first — it owns the sockets and the log
        // buffers, and a log buffer unmapped while a sender is reading it is
        // the one failure mode this ordering exists to prevent — then the
        // publications' own bookkeeping.
        let _ = self.sender.close();
        let _ = self.network_publications.close();
        // And the receive side, in the same order and for the same reason: the
        // thread that owns the sockets and the log buffers goes first.
        let _ = self.receiver.close();
        self.images.close();
        // The subscriptions own no counters: a reader's `sub-pos` is in the
        // publication's set, and the line above has already given it back.
        self.subscriptions.close();

        let released = self
            .system_counters
            .release_all(&mut self.counters, &regions, self.now_ms);
        debug_assert_eq!(
            system_counters::COUNT,
            released,
            "the driver releases exactly the counters it allocated"
        );

        // The distinct error log's *process* half needs no line here: the
        // observation table and the next offset are this build's own memory,
        // and dropping the conductor is what gives them back
        // (`aeron_distinct_error_log_close`, `aeron_driver_conductor.c:3489`).
        // The region is the file's, and the file is left as it stands — a
        // stopped driver's log is what a tool reads afterwards.
        self.running = false;
        self.write_heartbeat_value(layout::NULL_VALUE);
        self.cnc.sync()
    }

    /// Drain at most [`COMMAND_DRAIN_LIMIT`] commands and act on them.
    fn process_commands(&mut self, now_ns: i64) -> usize {
        // Borrows split by field rather than through `&mut self`, because the
        // read takes a window from the file while the handler writes state.
        let cnc = &self.cnc;
        let commands = &mut self.commands;
        let transmitter = &mut self.transmitter;
        let counters = &mut self.counters;
        let clients = &mut self.clients;
        let running = &mut self.running;
        let termination = self.termination;
        let config = &self.config;
        let publications = &mut self.publications;
        let network_publications = &mut self.network_publications;
        let send_endpoints = &mut self.send_endpoints;
        let sender = &self.sender;
        let receive_endpoints = &mut self.receive_endpoints;
        let images = &mut self.images;
        let receiver = &self.receiver;
        let publication_failures = &mut self.publication_failures;
        let subscriptions = &mut self.subscriptions;
        let subscription_failures = &mut self.subscription_failures;
        let now_ms = self.now_ms;
        let liveness_timeout_ns = self.liveness_timeout_ns;
        let pending_failures = &mut self.pending_broadcast_failures;
        let counter_failures = &mut self.counter_failures;
        let malformed = &mut self.malformed;
        let unknown_counters = &mut self.unknown_counters;
        let unhandled = &mut self.unhandled;
        let unknown = &mut self.unknown;
        let last_unhandled = &mut self.last_unhandled;
        let faults = &mut self.pending_log_errors;
        // A publication whose *client* link was released in this drain: the
        // release itself needs the sender and the endpoint registry, which the
        // drain's closure cannot reach, so the ids are collected here and the
        // work happens below it.
        let mut pending_publication_releases: Vec<i64> = Vec::new();
        // The destination commands, for the same reason: the *sender* is what
        // puts a destination on a tracker, and the drain's closure cannot reach
        // it. The payloads are kept verbatim, so that what is decoded below is
        // what the client wrote.
        let mut pending_destination_commands: Vec<(i32, Vec<u8>)> = Vec::new();

        let Some(region) = cnc.to_driver_region() else {
            return 0;
        };
        let Some(counter_regions) = cnc.counter_regions() else {
            return 0;
        };
        let Some(event_region) = cnc.to_clients_region_writable() else {
            return 0;
        };
        let mut transmit = Transmit {
            transmitter,
            region: &event_region,
            failures: pending_failures,
            faults,
        };

        let drained = commands.read(&region, COMMAND_DRAIN_LIMIT, |type_id, payload| {
            match Command::from_type_id(type_id) {
                Command::TerminateDriver => {
                    if TerminationPolicy::Allow == termination {
                        *running = false;
                    }
                }
                // A keepalive refreshes a client this driver knows and is
                // nothing at all for one it does not (`:5269-5280`). The
                // reference C client never sends it — it writes the counter —
                // so this is for other clients, and for a client that wants to
                // say it is alive without allocating anything.
                Command::ClientKeepalive => match decode_correlated(payload) {
                    Some(correlated) => {
                        clients.on_keepalive(
                            correlated.client_id,
                            now_ms,
                            counters,
                            &counter_regions,
                        );
                    }
                    None => malformed_command(type_id, payload.len(), malformed, &mut transmit),
                },
                // Nothing is freed here: the heartbeat is zeroed so the next
                // timeout tier collects the client, which is also what stops
                // that tier from announcing a timeout (`:6321-6331`).
                // A publication: parse the URI, resolve its parameters against
                // the driver's settings, and either share an existing
                // publication or ask the agent for a log buffer. The reply
                // follows from whichever of those happened
                // (`aeron_driver_conductor.c:3956-4080`).
                command @ (Command::AddPublication | Command::AddExclusivePublication) => {
                    let is_exclusive = matches!(command, Command::AddExclusivePublication);

                    match decode_add_publication(payload) {
                        Some(request) => {
                            let now = Now {
                                ms: now_ms,
                                ns: now_ns,
                                client_liveness_timeout_ns: liveness_timeout_ns,
                            };

                            // Which collection serves this depends on what
                            // the URI names, and *only* on that: the reference
                            // parses the channel and then follows one of two
                            // paths (`aeron_driver_conductor.c:4113-4135`), and
                            // a channel this build does not serve is refused by
                            // the collection that would have had it.
                            let result = match ChannelUri::parse(request.channel) {
                                Ok(uri) if uri.transport() == Transport::Udp => {
                                    network_publications.add_publication(
                                        &request,
                                        is_exclusive,
                                        config,
                                        counters,
                                        &counter_regions,
                                        clients,
                                        send_endpoints,
                                        sender.proxy(),
                                        subscriptions,
                                        images,
                                        receiver.proxy(),
                                        now,
                                        &mut transmit,
                                    )
                                }
                                _ => publications.add_publication(
                                    &request,
                                    is_exclusive,
                                    config,
                                    counters,
                                    &counter_regions,
                                    clients,
                                    subscriptions,
                                    now,
                                    &mut transmit,
                                ),
                            };

                            if let Err(error) = result {
                                // The error code is the reference's, derived
                                // from what failed rather than from where: a
                                // channel it cannot parse is a different
                                // answer from one whose parameters do not add
                                // up (`aeron_driver_conductor.c:2344-2352`).
                                *publication_failures += 1;
                                transmit.error(
                                    request.correlation_id,
                                    error.error_code(),
                                    error.to_string().as_bytes(),
                                );
                            }
                        }
                        None => malformed_command(type_id, payload.len(), malformed, &mut transmit),
                    }
                }
                // A publication's client letting go of it. The revocation
                // flag is set on the publication's own byte and acted on at
                // the next timeout tier (`aeron_driver_conductor.c:4705-4735`).
                Command::RemovePublication => match decode_remove_publication(payload) {
                    Some(request) => {
                        let link =
                            clients
                                .find_mut(request.correlated.client_id)
                                .and_then(|record| {
                                    let index =
                                        record.publication_links.iter().position(|link| {
                                            link.registration_id == request.registration_id
                                        })?;
                                    Some(record.publication_links.swap_remove(index))
                                });

                        match link {
                            Some(link) => {
                                if request.flags & REMOVE_PUBLICATION_FLAG_REVOKE != 0 {
                                    if let Some(publication) = publications
                                        .publications_mut()
                                        .iter_mut()
                                        .find(|publication| {
                                            publication.registration_id
                                                == link.publication_registration_id
                                        })
                                    {
                                        publication.set_revoked();
                                    }
                                }

                                publications.release_links(&[link], counters, &counter_regions);
                                pending_publication_releases.push(link.publication_registration_id);
                                transmit.operation_succeeded(request.correlated.correlation_id);
                            }
                            None => {
                                *publication_failures += 1;
                                // The reference's text names both ids
                                // (`aeron_driver_conductor.c:4734`), so a
                                // client — or an operator reading its logs —
                                // is told *whose* publication nobody had.
                                let unknown = format!(
                                    "unknown publication client_id={} registration_id={}",
                                    request.correlated.client_id, request.registration_id,
                                );
                                transmit.error(
                                    request.correlated.correlation_id,
                                    ERROR_CODE_UNKNOWN_PUBLICATION,
                                    unknown.as_bytes(),
                                );
                            }
                        }
                    }
                    None => malformed_command(type_id, payload.len(), malformed, &mut transmit),
                },
                // A subscription going away: its positions are detached and
                // its counters come back (`aeron_driver_conductor.c:5199-5267`)
                // — silently, which is the reference's answer in C and in
                // Java alike: neither announces the images a removal takes
                // away, so the acknowledgement is the whole event stream.
                Command::RemoveSubscription => match decode_remove_subscription(payload) {
                    Some(request) => {
                        if subscriptions.has(request.registration_id) {
                            // The detachment first, the acknowledgement after
                            // it: the reference unlinks every subscribable and
                            // only then answers, so a client that reads its
                            // events in order knows the removal has happened
                            // once the answer arrives.
                            subscriptions.remove(
                                request.registration_id,
                                counters,
                                &counter_regions,
                                publications,
                                sender.proxy(),
                                now_ms,
                            );

                            transmit.operation_succeeded(request.correlated.correlation_id);
                        } else {
                            *subscription_failures += 1;
                            // The same two ids as the publication's text
                            // (`aeron_driver_conductor.c:5258`).
                            let unknown = format!(
                                "unknown subscription client_id={} registration_id={}",
                                request.correlated.client_id, request.registration_id,
                            );
                            transmit.error(
                                request.correlated.correlation_id,
                                ERROR_CODE_UNKNOWN_SUBSCRIPTION,
                                unknown.as_bytes(),
                            );
                        }
                    }
                    None => malformed_command(type_id, payload.len(), malformed, &mut transmit),
                },
                // A subscription: parse the URI, register the client, answer
                // it, and then give it every publication it already matches
                // (`aeron_driver_conductor.c:4741-4824`).
                Command::AddSubscription => match decode_add_subscription(payload) {
                    Some(request) => {
                        let now = Now {
                            ms: now_ms,
                            ns: now_ns,
                            client_liveness_timeout_ns: liveness_timeout_ns,
                        };

                        // As with publications: what the URI names decides
                        // which half serves it, and nothing else. A spy is
                        // triaged **before** the transport test, because the
                        // prefix is not a transport — what follows it is an
                        // ordinary UDP channel, parsed as such
                        // (`aeron_driver_conductor_on_add_spy_subscription`,
                        // `aeron_driver_conductor.c:4926-4966`, whose own
                        // dispatch is a string comparison on the prefix).
                        let subscription_result = if is_spy_channel(request.channel) {
                            subscriptions.add_spy_subscription(
                                &request,
                                config,
                                counters,
                                &counter_regions,
                                clients,
                                network_publications,
                                sender.proxy(),
                                now,
                                &mut transmit,
                            )
                        } else {
                            match ChannelUri::parse(request.channel) {
                                Ok(uri) if uri.transport() == Transport::Udp => subscriptions
                                    .add_network_subscription(
                                        &request,
                                        config,
                                        counters,
                                        &counter_regions,
                                        clients,
                                        receive_endpoints,
                                        images,
                                        receiver.proxy(),
                                        now,
                                        &mut transmit,
                                    ),
                                _ => subscriptions.add_subscription(
                                    &request,
                                    config,
                                    counters,
                                    &counter_regions,
                                    clients,
                                    publications,
                                    now,
                                    &mut transmit,
                                ),
                            }
                        };

                        if let Err(error) = subscription_result {
                            *subscription_failures += 1;
                            transmit.error(
                                request.correlation_id,
                                error.error_code(),
                                error.to_string().as_bytes(),
                            );
                        }
                    }
                    None => malformed_command(type_id, payload.len(), malformed, &mut transmit),
                },
                Command::ClientClose => match decode_correlated(payload) {
                    Some(correlated) => {
                        clients.on_close(correlated.client_id, counters, &counter_regions);
                    }
                    None => malformed_command(type_id, payload.len(), malformed, &mut transmit),
                },
                Command::AddCounter => match decode_add_counter(payload) {
                    Some(command) => {
                        let client_id = command.correlated.client_id;
                        let registration_id = command.correlated.correlation_id;

                        // Registering the client is this command's first act,
                        // and it is what announces the client's heartbeat
                        // counter under the client id (`:1030`).
                        let Some(record) = clients.get_or_add(
                            client_id,
                            now_ms,
                            liveness_timeout_ns,
                            counters,
                            &counter_regions,
                            &mut transmit,
                        ) else {
                            *counter_failures += 1;
                            // The reference appends "Failed to add client" and
                            // returns -1, which the dispatcher turns into
                            // `ON_ERROR` (`:3226`, `:6171-6178`).
                            transmit.error(
                                registration_id,
                                ERROR_CODE_GENERIC_ERROR,
                                b"failed to add client",
                            );
                            return;
                        };

                        let allocated = counters
                            .allocate(
                                &counter_regions,
                                command.type_id,
                                command.key,
                                command.label,
                                now_ms,
                            )
                            .and_then(|counter_id| {
                                // Both writes, and the link only if they
                                // happened: a counter announced with a
                                // registration of zero is one nobody can ever
                                // find or remove again.
                                counters
                                    .set_registration_id(
                                        &counter_regions,
                                        counter_id,
                                        registration_id,
                                    )
                                    .and_then(|()| {
                                        counters.set_owner_id(
                                            &counter_regions,
                                            counter_id,
                                            client_id,
                                        )
                                    })
                                    .map(|()| counter_id)
                            });

                        match allocated {
                            Some(counter_id) => {
                                record.counter_links.push(CounterLink {
                                    registration_id,
                                    counter_id,
                                });
                                transmit.counter_ready(registration_id, counter_id);
                            }
                            None => {
                                *counter_failures += 1;
                                transmit.error(
                                    registration_id,
                                    ERROR_CODE_GENERIC_ERROR,
                                    b"failed to allocate counter",
                                );
                            }
                        }
                    }
                    None => malformed_command(type_id, payload.len(), malformed, &mut transmit),
                },
                Command::RemoveCounter => match decode_remove_counter(payload) {
                    Some(command) => {
                        let link =
                            clients
                                .find_mut(command.correlated.client_id)
                                .and_then(|record| {
                                    let index = record.counter_links.iter().position(|link| {
                                        link.registration_id == command.registration_id
                                    })?;
                                    Some(record.counter_links.swap_remove(index))
                                });

                        match link {
                            Some(link) => {
                                // Acknowledge, then announce, then free — the
                                // reference's order, and the reason its
                                // `removeCounter` unblocks while the counter is
                                // still being returned (`:6221-6235`).
                                transmit.operation_succeeded(command.correlated.correlation_id);
                                transmit.counter_unavailable(link.registration_id, link.counter_id);
                                counters.free(&counter_regions, link.counter_id, now_ms);
                            }
                            None => {
                                *unknown_counters += 1;
                                transmit.error(
                                    command.correlated.correlation_id,
                                    ERROR_CODE_UNKNOWN_COUNTER,
                                    b"unknown counter",
                                );
                            }
                        }
                    }
                    None => malformed_command(type_id, payload.len(), malformed, &mut transmit),
                },
                // A client refusing an image it was reading.
                //
                // Two targets, and the second is the one to miss: when no
                // **image** answers that registration id the driver looks among
                // its **IPC publications**, because for `aeron:ipc` what a
                // subscriber was handed *is* the publication
                // (`aeron_driver_conductor.c:6367-6398`). Both branches are
                // answered the same way afterwards — the rejections counter,
                // then `ON_OPERATION_SUCCEEDED` — and only a client that named
                // neither hears an error.
                Command::RejectImage => match decode_reject_image(payload) {
                    Err(RejectImageError::Malformed) => {
                        malformed_command(type_id, payload.len(), malformed, &mut transmit);
                    }
                    Err(RejectImageError::ReasonTooLong { correlated }) => {
                        // The handler's refusal, which the dispatch turns into
                        // an `ON_ERROR` on the command's own correlation id
                        // (`aeron_driver_conductor.c:6357-6364`, then its
                        // `result < 0` at `:3224-3227`).
                        transmit.error(
                            correlated.correlation_id,
                            ERROR_CODE_GENERIC_ERROR,
                            b"Invalidation reason_text must be 1023 bytes or less",
                        );
                    }
                    Ok(request) => {
                        let image_correlation_id = request.image_correlation_id;

                        let rejected = if images.find(image_correlation_id).is_some() {
                            // The image belongs to the receiver, so this is a
                            // command to that thread rather than a change here:
                            // all the conductor knows is *that* the image
                            // exists, and the receiver is what has the
                            // connections to send an `ERR` frame down
                            // (`aeron_driver_receiver_proxy_on_invalidate_image`,
                            // `aeron_driver_receiver_proxy.c:252-274`).
                            let _ =
                                receiver
                                    .proxy()
                                    .invalidate_image(image_correlation_id, request.reason.to_vec());

                            true
                        } else {
                            publications.reject(
                                image_correlation_id,
                                request.reason,
                                counters,
                                &counter_regions,
                                subscriptions,
                                &mut transmit,
                                now_ns,
                                now_ms,
                            )
                        };

                        if rejected {
                            let _ = system_counters::increment(
                                counters,
                                &counter_regions,
                                system_counters::id::IMAGES_REJECTED,
                            );

                            transmit.operation_succeeded(request.correlated.correlation_id);
                        } else {
                            transmit.error(
                                request.correlated.correlation_id,
                                ERROR_CODE_GENERIC_ERROR,
                                format!(
                                    "Unable to resolve image for correlationId={image_correlation_id}"
                                )
                                .as_bytes(),
                            );
                        }
                    }
                },
                // A client asking what session id to publish under.
                //
                // The driver's answer is a **hint**, and the only thing it
                // guarantees is that no publication it holds already uses it on
                // that stream — which is why the handler walks its own lists
                // rather than trusting the cursor: the cursor is where it would
                // look next, and a client may have published under an id it
                // made up in the meantime.
                // A counter the **driver** owns, at a client's request.
                //
                // The one thing that makes it static is the owner id: the
                // reference allocates it with `AERON_NULL_VALUE` and does *not*
                // put it in the client's list of counters
                // (`aeron_driver_conductor.c:6300-6312`), which is the whole of
                // why a client that dies does not take it with it. Pushing a
                // `CounterLink` here would look harmless and would free a
                // counter the driver is meant to keep — `Clients::reap` walks
                // that list for exactly that purpose.
                Command::AddStaticCounter => match decode_add_static_counter(payload) {
                    Some(command) => {
                        let client_id = command.correlated.client_id;
                        let correlation_id = command.correlated.correlation_id;

                        // Registering the client is this command's first act,
                        // the same as `ADD_COUNTER`'s (`:6259`).
                        let Some(_record) = clients.get_or_add(
                            client_id,
                            now_ms,
                            liveness_timeout_ns,
                            counters,
                            &counter_regions,
                            &mut transmit,
                        ) else {
                            *counter_failures += 1;
                            transmit.error(
                                correlation_id,
                                ERROR_CODE_GENERIC_ERROR,
                                b"failed to add client",
                            );
                            return;
                        };

                        // An id and a type id that already name a counter: the
                        // answer depends on who owns it (`:6265-6281`). One with
                        // an owner is somebody's, and a static counter may not be
                        // put in its place; one without is a static counter
                        // already, and this call is a client asking for the one
                        // it has.
                        let existing = counter_regions
                            .reader()
                            .find_by_type_and_registration(command.type_id, command.registration_id);

                        let counter_id = match existing {
                            Some(counter_id) => {
                                let owner_id = counter_regions
                                    .reader()
                                    .get(counter_id)
                                    .map_or(layout::NULL_VALUE, |descriptor| descriptor.owner_id);

                                if layout::NULL_VALUE != owner_id {
                                    *counter_failures += 1;
                                    transmit.error(
                                        correlation_id,
                                        ERROR_CODE_GENERIC_ERROR,
                                        format!(
                                            "cannot add static counter, because a non-static counter exists \
                                             (counterId={counter_id}) for typeId={} and registrationId={}",
                                            command.type_id, command.registration_id
                                        )
                                        .as_bytes(),
                                    );
                                    return;
                                }

                                counter_id
                            }
                            None => {
                                let Some(counter_id) = counters.allocate(
                                    &counter_regions,
                                    command.type_id,
                                    command.key,
                                    command.label,
                                    now_ms,
                                ) else {
                                    *counter_failures += 1;
                                    // The reference returns `-1` here with no
                                    // `AERON_SET_ERR` of its own
                                    // (`aeron_driver_conductor.c:6300-6304`),
                                    // so its client is answered with whatever
                                    // the thread's error slot last held — a
                                    // value it does not define and this build
                                    // cannot reproduce. The code is the generic
                                    // one and the words are this build's, which
                                    // is the divergence `docs/compat.md` already
                                    // records for every `ON_ERROR` that is not
                                    // one of the two quoted ones.
                                    transmit.error(
                                        correlation_id,
                                        ERROR_CODE_GENERIC_ERROR,
                                        b"failed to allocate static counter",
                                    );
                                    return;
                                };

                                let _ = counters.set_registration_id(
                                    &counter_regions,
                                    counter_id,
                                    command.registration_id,
                                );
                                let _ = counters.set_owner_id(
                                    &counter_regions,
                                    counter_id,
                                    layout::NULL_VALUE,
                                );

                                counter_id
                            }
                        };

                        transmit.send(
                            ON_STATIC_COUNTER_TYPE_ID,
                            &encode_static_counter(correlation_id, counter_id),
                        );
                    }
                    None => malformed_command(type_id, payload.len(), malformed, &mut transmit),
                },
                Command::GetNextAvailableSessionId => {
                    match decode_get_next_available_session_id(payload) {
                        Some(request) => {
                            let stream_id = request.stream_id;

                            // `outer: while (true)` (`aeron_driver_conductor.c:6422-6450`).
                            //
                            // It terminates, and the argument is worth having
                            // because the loop has no counter: every turn moves
                            // the cursor one id on, so the turns ask for
                            // *different* ids, and the only ones refused are
                            // those a publication **on this stream** already
                            // holds. There are finitely many of those and 2^32
                            // ids, so an id nobody holds arrives within
                            // `held + 1` turns.
                            let next_session_id = loop {
                                let candidate = publications.next_session_id();

                                let taken = publications.publications().iter().any(
                                    |publication| {
                                        publication.stream_id == stream_id
                                            && publication.session_id == candidate
                                    },
                                ) || network_publications.publications().iter().any(
                                    |publication| {
                                        publication.stream_id == stream_id
                                            && publication.session_id == candidate
                                    },
                                );

                                if !taken {
                                    break candidate;
                                }
                            };

                            transmit.send(
                                ON_NEXT_AVAILABLE_SESSION_ID_TYPE_ID,
                                &encode_next_available_session_id(
                                    request.correlated.correlation_id,
                                    next_session_id,
                                ),
                            );
                        }
                        None => malformed_command(type_id, payload.len(), malformed, &mut transmit),
                    }
                }
                Command::AddDestination
                | Command::RemoveDestination
                | Command::RemoveDestinationById
                | Command::AddReceiveDestination
                | Command::RemoveReceiveDestination => {
                    pending_destination_commands.push((type_id, payload.to_vec()));
                }
                command => {
                    if let Command::Unknown(unknown_type_id) = command {
                        *unknown += 1;
                        transmit.record_fault(
                            -ERROR_CODE_UNKNOWN_COMMAND_TYPE_ID,
                            compose_description(
                                -ERROR_CODE_UNKNOWN_COMMAND_TYPE_ID,
                                "aeron_driver_conductor_on_command",
                                "aeron_driver_conductor.c",
                                3219,
                                &format!("command={unknown_type_id} unknown"),
                            ),
                        );
                    } else {
                        *unhandled += 1;
                    }
                    *last_unhandled = Some(command);
                }
            }
        });

        // A publication whose link was released: the network half stops
        // sending it, gives its six counters back, and counts one less reader
        // on its endpoint. The IPC half did its own release inside the drain.
        let mut released = 0usize;

        for registration_id in pending_publication_releases {
            if let Some(record) = network_publications.remove(registration_id) {
                let _ = sender.proxy().remove_publication(registration_id);

                // The spies first, and for the same reason the other release
                // path gives: a client told its image is gone stops advancing
                // the position this publication's limit was computed from,
                // which is the state the publication is about to leave.
                subscriptions.unlink_spies_of(
                    registration_id,
                    counters,
                    &counter_regions,
                    now_ms,
                    &mut transmit,
                );

                for counter_id in [
                    record.counters.pub_pos,
                    record.counters.pub_lmt,
                    record.counters.snd_pos,
                    record.counters.snd_lmt,
                    record.counters.snd_bpe,
                    record.counters.snd_naks_received,
                ] {
                    let _ = counters.free(&counter_regions, counter_id, now_ms);
                }

                send_endpoints.detach_publication(record.endpoint_id);
                released += 1;
            }
        }

        // The destination commands.
        //
        // `REMOVE_DESTINATION_BY_ID` first, because it is the one command in the
        // family whose failures the reference answers **nothing** to: it calls
        // its handler without taking the result (`:3188-3200`), so the error for
        // a publication it cannot find (`:5562-5578`) never reaches the
        // `result < 0` that would send an `ON_ERROR` (`:3222-3225`). The client
        // waits and times out. The silence is reproduced rather than improved
        // on, so a lookup that finds nothing here answers nothing either, and
        // `docs/compat.md` carries the line for it.
        for (type_id, payload) in pending_destination_commands {
            let command = Command::from_type_id(type_id);

            // A receive destination is triaged by the prefix of the channel it
            // names (`aeron_driver_conductor.c:3051-3065`): `aeron:ipc` is one
            // kind of destination, `aeron-spy:` another, and everything else is
            // a network one.
            //
            // `aeron:ipc` is the one this build refuses **by name** — a client
            // told nothing waits out its timeout, and this is not a command
            // this driver is going to get to later. The refusal is recorded in
            // `docs/compat.md`.
            if Command::AddReceiveDestination == command
                || Command::RemoveReceiveDestination == command
            {
                let Some(request) = decode_destination_command(&payload) else {
                    malformed_command(type_id, payload.len(), malformed, &mut transmit);
                    continue;
                };

                if request.channel.starts_with(IPC_PREFIX.as_bytes()) {
                    transmit.error(
                        request.correlation_id,
                        ERROR_CODE_NOT_SUPPORTED,
                        b"aeron:ipc destinations are not served by this driver",
                    );
                    continue;
                }

                // A spy destination is a **source**, not a socket: it adds a
                // local read of a publication to a multi-destination
                // subscription, which is the third way a spy link is made
                // (`aeron_driver_conductor_execute_add_receive_spy_destination`,
                // `:5704-5806`, and its removal at `:6024-6065`).
                if is_spy_channel(request.channel) {
                    let now = Now {
                        ms: now_ms,
                        ns: now_ns,
                        client_liveness_timeout_ns: liveness_timeout_ns,
                    };

                    if Command::AddReceiveDestination == command {
                        let added = subscriptions.add_spy_destination(
                            &request,
                            config,
                            counters,
                            &counter_regions,
                            receive_endpoints,
                            network_publications,
                            sender.proxy(),
                            now,
                            &mut transmit,
                        );

                        if let Err(error) = added {
                            *subscription_failures += 1;
                            transmit.error(
                                request.correlation_id,
                                error.error_code(),
                                error.to_string().as_bytes(),
                            );
                        }
                    } else if subscriptions.remove_spy_destination(
                        request.registration_id,
                        request.channel,
                        counters,
                        &counter_regions,
                        sender.proxy(),
                        now_ms,
                        &mut transmit,
                    ) {
                        transmit.operation_succeeded(request.correlation_id);
                    } else {
                        *subscription_failures += 1;
                        let unknown = format!(
                            "unknown subscription client_id={} registration_id={}",
                            request.client_id, request.registration_id,
                        );
                        transmit.error(
                            request.correlation_id,
                            ERROR_CODE_UNKNOWN_SUBSCRIPTION,
                            unknown.as_bytes(),
                        );
                    }

                    continue;
                }

                // The network branch. A destination is added to the
                // **subscription** the client named — that is what its
                // registration id is — and through it to the endpoint that
                // subscription reads on (`aeron_driver_conductor.c:5879-5910`).
                let Some(link) = subscriptions.find_mds(request.registration_id) else {
                    transmit.error(
                        request.correlation_id,
                        ERROR_CODE_UNKNOWN_SUBSCRIPTION,
                        b"unknown subscription",
                    );
                    continue;
                };

                let Some(endpoint_id) = link.endpoint_id else {
                    // An IPC subscription has no socket, so there is nothing to
                    // add a destination to. The triage above catches an
                    // `aeron:ipc` destination before this; a link with no
                    // endpoint here is an IPC subscription named by a network
                    // destination, which names a channel it does not have.
                    transmit.error(
                        request.correlation_id,
                        ERROR_CODE_NOT_SUPPORTED,
                        b"an IPC subscription has no destination",
                    );
                    continue;
                };

                let Ok(uri) = ChannelUri::parse(request.channel) else {
                    transmit.error(
                        request.correlation_id,
                        ERROR_CODE_INVALID_CHANNEL,
                        b"incorrect URI format for destination",
                    );
                    continue;
                };

                // `:5889`: what a destination may not name, checked on the
                // parse and before the socket — `mtu`, `rcv-wnd`, either
                // buffer, the response correlation, and `control-mode=response`.
                // A destination is a place inside a subscription's channel,
                // not a channel of its own.
                if let Err(error) = validate_destination_uri_params(&uri, request.channel) {
                    transmit.error(
                        request.correlation_id,
                        error.error_code(),
                        error.to_string().as_bytes(),
                    );
                    continue;
                }

                let Ok(channel) = UdpChannel::resolve(request.channel, &uri) else {
                    transmit.error(
                        request.correlation_id,
                        ERROR_CODE_INVALID_CHANNEL,
                        b"incorrect URI format for destination",
                    );
                    continue;
                };

                if Command::AddReceiveDestination == command {
                    let Some(entry) = receive_endpoints.get(endpoint_id) else {
                        transmit.error(
                            request.correlation_id,
                            ERROR_CODE_UNKNOWN_SUBSCRIPTION,
                            b"the subscription's endpoint is gone",
                        );
                        continue;
                    };
                    let channel_status_counter_id = entry.channel_status_counter_id;

                    let params = ReceiveChannelEndpoints::transport_params(config, &channel);
                    let destination = match ReceiveDestination::open(
                        channel,
                        &params,
                        counters,
                        &counter_regions,
                        request.registration_id,
                        channel_status_counter_id,
                        now_ms,
                    ) {
                        Ok(destination) => destination,
                        Err(error) => {
                            transmit.error(
                                request.correlation_id,
                                ERROR_CODE_GENERIC_ERROR,
                                error.to_string().as_bytes(),
                            );
                            continue;
                        }
                    };

                    let _ = receiver
                        .proxy()
                        .add_destination(endpoint_id, Box::new(destination));
                    // The endpoint's destination list is on the receiver's
                    // thread; the agreement check that reads it is here, so
                    // the count is kept here too (`aeron_driver_conductor.c:2184`
                    // asks `1 == endpoint->destinations.length`).
                    receive_endpoints.attach_destination(endpoint_id);
                } else {
                    let _ = receiver
                        .proxy()
                        .remove_destination(endpoint_id, Box::new(channel));
                    receive_endpoints.detach_destination(endpoint_id);
                }

                transmit.operation_succeeded(request.correlation_id);
                continue;
            }

            if Command::RemoveDestinationById == command {
                let Some(request) = decode_destination_by_id_command(&payload) else {
                    malformed_command(type_id, payload.len(), malformed, &mut transmit);
                    continue;
                };

                let Some(record) = network_publications.find(request.resource_registration_id)
                else {
                    continue;
                };

                let _ = sender.proxy().remove_destination_by_id(
                    record.endpoint_id,
                    request.destination_registration_id,
                );

                transmit.operation_succeeded(request.correlation_id);
                continue;
            }

            let Some(request) = decode_destination_command(&payload) else {
                malformed_command(type_id, payload.len(), malformed, &mut transmit);
                continue;
            };

            let Some(record) = network_publications.find(request.registration_id) else {
                transmit.error(
                    request.correlation_id,
                    ERROR_CODE_UNKNOWN_PUBLICATION,
                    format!(
                        "unknown publication registration_id={}",
                        request.registration_id
                    )
                    .as_bytes(),
                );
                continue;
            };

            if let Err(error) = validate_destination_prefix(request.channel, "send") {
                transmit.error(
                    request.correlation_id,
                    error.error_code(),
                    error.to_string().as_bytes(),
                );
                continue;
            }

            // A destination whose name does not resolve is **kept** and the
            // command still succeeds: the reference sets the address to
            // `AF_UNSPEC` and falls through on purpose (`:5337-5343`), which is
            // `None` here. The consequence is in `docs/compat.md`'s
            // name-resolution row: this build has no re-resolution, so that
            // destination never recovers.
            let address = match validate_send_destination_uri(request.channel) {
                Ok(address) => Some(address),
                Err(UdpChannelError::Resolution(_)) => None,
                Err(error) => {
                    transmit.error(
                        request.correlation_id,
                        error.error_code(),
                        error.to_string().as_bytes(),
                    );
                    continue;
                }
            };

            let Ok(uri) = ChannelUri::parse(request.channel) else {
                transmit.error(
                    request.correlation_id,
                    ERROR_CODE_INVALID_CHANNEL,
                    b"incorrect URI format for destination",
                );
                continue;
            };

            let Ok(channel) = UdpChannel::resolve(request.channel, &uri) else {
                transmit.error(
                    request.correlation_id,
                    ERROR_CODE_INVALID_CHANNEL,
                    b"incorrect URI format for destination",
                );
                continue;
            };

            let registration_id = request.correlation_id;
            let outcome = if Command::AddDestination == command {
                sender.proxy().add_destination(
                    record.endpoint_id,
                    Box::new(channel),
                    address,
                    registration_id,
                )
            } else {
                // A removal names a destination by its channel, and the channel
                // that identifies one is the one it was added with — so the
                // address is what the tracker matches on (`:311-350`).
                address.map_or(Ok(()), |address| {
                    sender
                        .proxy()
                        .remove_destination(record.endpoint_id, address)
                })
            };

            if outcome.is_err() {
                // The sender has gone; the client is answered anyway, because
                // the reference answers before the sender applies anything
                // (`:5366-5367`) and a client that is told nothing hangs.
                transmit.record_fault(
                    ERROR_CODE_GENERIC_ERROR,
                    format!("destination command for publication {registration_id}"),
                );
            }

            transmit.operation_succeeded(request.correlation_id);
        }

        drained + released
    }

    /// Break a stall in the command ring, and count it when it breaks one.
    ///
    /// A producer that dies between claiming a record and committing it leaves
    /// a record that can never be read, and the consumer that stops at it never
    /// publishes a new head position — so the ring never moves again and the
    /// driver goes deaf while its heartbeat keeps telling every client it is
    /// fine. That is the whole failure mode, and time is the only thing that
    /// distinguishes it from a command being written right now.
    ///
    /// The reference asks the question on its timeout tier
    /// (`aeron_driver_conductor_on_check_for_blocked_driver_commands`,
    /// `:3246-3266`): if the consumer position has not moved while the
    /// producer's is ahead of it, and that has been true for a whole client
    /// liveness window, try to break it and count the break in system counter
    /// 20.
    fn check_for_blocked_commands(&mut self, now_ns: i64) -> bool {
        let Some(region) = self.cnc.to_driver_region() else {
            return false;
        };
        let (Some(consumer_position), Some(producer_position)) = (
            self.commands.consume_position(&region),
            self.commands.producer_position(&region),
        ) else {
            return false;
        };

        if consumer_position != self.last_command_consumer_position
            || producer_position <= consumer_position
        {
            self.time_of_last_position_change_ns = now_ns;
            self.last_command_consumer_position = consumer_position;
            return false;
        }

        let stalled_since_ns = self
            .time_of_last_position_change_ns
            .saturating_add(self.liveness_timeout_ns);
        if now_ns <= stalled_since_ns {
            return false;
        }

        if !self.commands.unblock(&region) {
            return false;
        }

        if let Some(regions) = self.cnc.counter_regions() {
            system_counters::increment(
                &self.counters,
                &regions,
                system_counters::id::UNBLOCKED_COMMANDS,
            );
        }

        true
    }

    /// The publications' turn: advance the ones on their way out, revoke the
    /// ones whose clients asked, and remove the ones that are done
    /// (`aeron_driver_conductor_on_check_managed_resources`, `:1691-1712`,
    /// which the reference runs on the same tier).
    fn check_publications(&mut self, now_ns: i64) -> usize {
        let Some(counter_regions) = self.cnc.counter_regions() else {
            return 0;
        };
        let Some(event_region) = self.cnc.to_clients_region_writable() else {
            return 0;
        };

        let mut transmit = Transmit {
            transmitter: &mut self.transmitter,
            region: &event_region,
            failures: &mut self.pending_broadcast_failures,
            faults: &mut self.pending_log_errors,
        };

        self.publications.on_time_event(
            &mut self.counters,
            &counter_regions,
            &mut self.subscriptions,
            &mut transmit,
            Now {
                ms: self.now_ms,
                ns: now_ns,
                client_liveness_timeout_ns: self.liveness_timeout_ns,
            },
        )
    }

    /// The client pool's turn: announce whoever has gone quiet, and then
    /// reclaim them.
    ///
    /// Two phases, in the reference's order (`aeron_driver_conductor.c:1038-1056`
    /// then `:1692-1712`): the announcements of *every* expired client precede
    /// the reclamation of any of them, which is visible on the ring whenever
    /// two clients expire in the same tick.
    fn check_clients(&mut self) -> usize {
        let Some(counter_regions) = self.cnc.counter_regions() else {
            return 0;
        };
        let Some(event_region) = self.cnc.to_clients_region_writable() else {
            return 0;
        };

        // Phase one reads the counters — each client's heartbeat, and system
        // counter 24 — so it borrows them immutably; phase two frees, so it
        // borrows them mutably. The two cannot be alive at once, and they
        // should not be: that is what keeps every announcement ahead of every
        // free.
        {
            let mut transmit = Transmit {
                transmitter: &mut self.transmitter,
                region: &event_region,
                failures: &mut self.pending_broadcast_failures,
                faults: &mut self.pending_log_errors,
            };
            self.clients.on_time_event(
                self.now_ms,
                &self.counters,
                &counter_regions,
                &mut transmit,
            );
        }

        let mut transmit = Transmit {
            transmitter: &mut self.transmitter,
            region: &event_region,
            failures: &mut self.pending_broadcast_failures,
            faults: &mut self.pending_log_errors,
        };
        let reaped = self.clients.reap_expired(
            self.now_ms,
            &mut self.counters,
            &counter_regions,
            &mut transmit,
            &mut self.publications,
            &mut self.subscriptions,
            self.sender.proxy(),
        );

        self.release_orphaned_network_publications() + reaped
    }

    /// Account for this pass's broadcast failures.
    ///
    /// Each one is an error the reference records: `client_transmit` appends
    /// "failed to transmit message" and logs it, and logging is what bumps the
    /// errors system counter (`aeron_driver_conductor.c:2233-2241` feeding
    /// `:1203-1215`). They are counted during the pass and recorded after it,
    /// because the pass holds the event region mutably; the order within a
    /// pass is not observable from outside it.
    ///
    /// The code recorded is zero, which is what the reference records too:
    /// `AERON_APPEND_ERR` appends text without setting a code
    /// (`util/aeron_error.c:380-388`, whose Windows twin's comment says the
    /// same at `:498-501`), and the last thing a recorded error did was clear
    /// the state (`:389-397`).
    ///
    /// This reverses the P1-1 review's D4, which read `:2233-2241` as "touches
    /// no counter" and removed the increment — the counter it leaves alone is
    /// a *different* one from the errors counter its own `log_error` raises.
    fn flush_broadcast_failures(&mut self) {
        if 0 == self.pending_broadcast_failures {
            return;
        }

        let pending = std::mem::take(&mut self.pending_broadcast_failures);
        self.broadcast_failures += pending;

        for _ in 0..pending {
            self.log_error(0, "failed to transmit message");
        }
    }

    /// Record a driver-side error: one entry in the distinct error log and one
    /// bump of the errors system counter — the bump **always**, even for an
    /// entry the log could not hold
    /// (`aeron_driver_conductor_log_explicit_error`,
    /// `aeron_driver_conductor.c:1203-1215`).
    fn log_error(&mut self, error_code: i32, description: &str) {
        self.record_entry(error_code, description);

        if let Some(regions) = self.cnc.counter_regions() {
            system_counters::increment(&self.counters, &regions, system_counters::id::ERRORS);
        }
    }

    /// Record what the pass left behind: every error it answered a client
    /// with, and the command adapter's own faults.
    ///
    /// An `ON_ERROR` is recorded under the **protocol** code the client was
    /// told, not the negated one a recording site set: the reference hands
    /// `log_explicit_error` the `code` it composed, not the `errcode`
    /// (`aeron_driver_conductor.c:2369`). One code the reference refuses to
    /// record is skipped — the transient "resource temporarily unavailable"
    /// is answered but kept out of the log, and so raises no counter either
    /// (`:2367`).
    fn record_pending_faults(&mut self) {
        for (error_code, description) in std::mem::take(&mut self.pending_log_errors) {
            if ERROR_CODE_RESOURCE_TEMPORARILY_UNAVAILABLE == error_code {
                continue;
            }

            self.log_error(error_code, &description);
        }
    }

    /// Write one entry, and say so on stderr when the region cannot hold it.
    ///
    /// The reference complains from **inside** its recorder
    /// (`aeron_distinct_error_log.c:183-191`), so every path that records
    /// reports a failure to record the same way — including the ones whose
    /// callers ignore the return value, such as the storage warning
    /// (`aeron_driver_context.c:1374`).
    fn record_entry(&mut self, error_code: i32, description: &str) {
        let Some(region) = self.cnc.error_log_writable() else {
            return;
        };

        if let Err(deepmsg_cnc::error_log::RecordError::Unrecordable { description }) = self
            .error_log
            .record(&region, self.now_ms, error_code, description)
        {
            // stderr, because the reference's recorder writes there itself —
            // this is the one line the driver prints on a path that is not a
            // start-up or shutdown failure, and it is faithful
            // (`aeron_distinct_error_log.c:189`, `AERON_FPRINTF(stderr, ...)`).
            // The reference formats a date; the epoch time says the same thing
            // and the stream, which is the part that is observable, matches.
            //
            // The reference's own system tests would be red on this too if they
            // could reach it, which is the point: it means an error log that
            // cannot hold an entry is visible instead of silent.
            eprintln!("{} - unrecordable error {}", self.now_ms, description);
        }
    }

    /// Take the agent's storage warnings and record them
    /// (`aeron_driver_context_run_storage_checks`'s second half,
    /// `aeron_driver_context.c:1368-1377`).
    ///
    /// The warning is the reference's own words, and the two numbers are
    /// printed the way its `PRId64` prints the `uint64_t` values it passes —
    /// as signed, which no real threshold or filesystem can tell apart.
    /// The description carries the reference's composition — the code's own
    /// line, then the recording site — which is what `ErrorStat` prints.
    fn record_storage_warnings(&mut self) {
        for warning in self.publications.poll_storage_warnings() {
            #[allow(clippy::cast_possible_wrap)] // printed as the reference prints it
            let message = format!(
                "WARNING: space is running low: threshold={} usable={} in {}",
                warning.threshold as i64,
                warning.usable as i64,
                warning.dir.display()
            );
            // Negated, because the reference's `AERON_SET_ERR` was given
            // `-AERON_ERROR_CODE_STORAGE_SPACE` and the log keeps what that
            // left (`:1370-1372`).
            let description = compose_description(
                -ERROR_CODE_STORAGE_SPACE,
                "aeron_driver_context_run_storage_checks",
                "aeron_driver_context.c",
                1370,
                &message,
            );
            self.record_distinct(-ERROR_CODE_STORAGE_SPACE, &description);
        }
    }

    /// Write one entry in the distinct error log, and nothing else.
    ///
    /// This is the reference's direct
    /// `aeron_distinct_error_log_record(context->error_log, …)` — the calls
    /// that do **not** go through `log_explicit_error`
    /// (`aeron_driver_conductor.c:1203-1215`), and so raise no errors
    /// counter. A caller that failed as well as warned uses
    /// [`Conductor::log_error`]; this is for the paths the reference
    /// recorded without deciding anything had failed.
    fn record_distinct(&mut self, error_code: i32, description: &str) {
        // What an entry that does not fit costs is the *return value*: this
        // path's caller does not look at it (`aeron_driver_context.c:1374`)
        // and no counter moves for a recorded error either. The complaint on
        // stderr still happens, because it is the recorder that makes it.
        self.record_entry(error_code, description);
    }

    /// Measure the pass that just ended, and count it if it ran long
    /// (`aeron_duty_cycle_tracker.h:50-62`).
    ///
    /// The measurement is the *gap* since the previous pass, which is what the
    /// reference measures: a tracker is updated at the start of a cycle and
    /// reports how long the last one took.
    fn track_cycle(&mut self, now_ns: i64) {
        let cycle_ns = now_ns.saturating_sub(self.last_cycle_ns);
        self.last_cycle_ns = now_ns;

        let Some(regions) = self.cnc.counter_regions() else {
            return;
        };

        system_counters::propose_max(
            &self.counters,
            &regions,
            system_counters::id::CONDUCTOR_MAX_CYCLE_TIME,
            cycle_ns,
        );
        if cycle_ns > system_counters::CONDUCTOR_CYCLE_THRESHOLD_NS {
            system_counters::increment(
                &self.counters,
                &regions,
                system_counters::id::CONDUCTOR_CYCLE_TIME_THRESHOLD_EXCEEDED,
            );
        }
    }

    fn write_heartbeat(&mut self) {
        self.write_heartbeat_value(self.now_ms);
    }

    fn write_heartbeat_value(&mut self, value: i64) {
        let Some(region) = self.cnc.to_driver_region() else {
            // Unreachable for a file this build created: the region is
            // validated at construction and the mapping is read-write.
            debug_assert!(false, "a created CnC file has no writable to-driver region");
            return;
        };

        let written = self.commands.write_consumer_heartbeat(&region, value);
        debug_assert!(
            written.is_some(),
            "the region was validated at construction"
        );
    }
}

/// The reference's conversion of the configured reuse timeout
/// (`aeron_driver_conductor.c:696-701`): milliseconds, and at least one when a
/// timeout was asked for at all. Zero means "reusable at once".
fn free_to_reuse_ms(nanoseconds: i64) -> i64 {
    if nanoseconds <= 0 {
        return 0;
    }

    (nanoseconds / 1_000_000).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use deepmsg_cnc::command::{
        ADD_EXCLUSIVE_PUBLICATION_TYPE_ID, ADD_PUBLICATION_TYPE_ID, ADD_STATIC_COUNTER_TYPE_ID,
        ADD_SUBSCRIPTION_TYPE_ID, ERROR_CODE_INVALID_CHANNEL, ERROR_CODE_NOT_SUPPORTED,
        ON_ERROR_TYPE_ID, ON_EXCLUSIVE_PUBLICATION_READY_TYPE_ID, ON_PUBLICATION_READY_TYPE_ID,
    };
    use deepmsg_cnc::create::COUNTERS_VALUES_BUFFER_LENGTH_MIN;
    use deepmsg_core::logbuffer::{descriptor, frame};

    use crate::config::{
        PUBLICATION_RESERVED_SESSION_ID_HIGH_DEFAULT, PUBLICATION_RESERVED_SESSION_ID_LOW_DEFAULT,
    };
    use deepmsg_client::counter::CounterEvent;
    use deepmsg_cnc::layout::NULL_VALUE;
    use deepmsg_cnc::{CncIdentity, CncLayout, TerminateDriver};
    use deepmsg_cnc::{Received, ToClientsReceiver};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// A directory of our own in the system temp directory, removed on drop.
    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            static COUNTER: AtomicU32 = AtomicU32::new(0);
            let n = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path =
                std::env::temp_dir().join(format!("deepmsg-conductor-{}-{n}", std::process::id()));
            std::fs::create_dir_all(&path).expect("create temp dir");
            Self(path)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn layout() -> CncLayout {
        CncLayout {
            counters_values_length: COUNTERS_VALUES_BUFFER_LENGTH_MIN,
            ..CncLayout::default()
        }
    }

    fn config(dir: &std::path::Path, termination: TerminationPolicy) -> DriverConfig {
        DriverConfig {
            aeron_dir: dir.to_owned(),
            termination,
            ..DriverConfig::default()
        }
    }

    fn create(dir: &std::path::Path) -> CncFile {
        CncFile::create(
            dir,
            &layout(),
            &CncIdentity {
                liveness_timeout_ns: 10_000_000_000,
                start_timestamp_ms: clock::epoch_millis(),
                pid: i64::from(std::process::id()),
            },
        )
        .expect("create")
    }

    /// A CnC file a conductor is running over, and its directory.
    fn running(termination: TerminationPolicy) -> (TempDir, Conductor) {
        let temp = TempDir::new();
        let cnc = create(&temp.0);
        let conductor = Conductor::new(cnc, &config(&temp.0, termination)).expect("conductor");
        (temp, conductor)
    }

    /// Send one command into the ring, the way a client does.
    fn send(conductor: &Conductor, type_id: i32, payload: &[u8]) {
        let ring = conductor.cnc.to_driver_ring().expect("producer view");
        ring.write(type_id, payload).expect("fits");
    }

    /// A `TERMINATE_DRIVER` payload, with a token a validator could look at.
    fn terminate_payload() -> Vec<u8> {
        let command = TerminateDriver {
            client_id: 1,
            correlation_id: 2,
            token: b"let me in",
        };
        let mut out = vec![0u8; command.encoded_length()];
        assert!(
            command.encode_into(&mut out),
            "the payload is its own length"
        );
        out
    }

    fn heartbeat(conductor: &Conductor) -> i64 {
        conductor
            .cnc
            .consumer_heartbeat_ms()
            .expect("the ring trailer is readable")
    }

    /// The conductor's counter regions, for a test that reads or writes one.
    fn counter_regions(conductor: &Conductor) -> deepmsg_cnc::CounterRegions<'_> {
        conductor
            .cnc
            .counter_regions()
            .expect("the counter regions")
    }

    /// A config with the two timeouts a test wants, rather than the defaults.
    fn config_with(dir: &std::path::Path, liveness_ns: i64, timer_ns: i64) -> DriverConfig {
        DriverConfig {
            aeron_dir: dir.to_owned(),
            client_liveness_timeout_ns: liveness_ns,
            timer_interval_ns: timer_ns,
            ..DriverConfig::default()
        }
    }

    /// A running conductor whose rings a second mapping can be read from.
    ///
    /// The second mapping is how a client sees this file, and it is also the
    /// only way a test can hold a reader across a `do_work` that wants
    /// `&mut` on the conductor.
    fn running_with(liveness_ns: i64, timer_ns: i64) -> (TempDir, Conductor) {
        let temp = TempDir::new();
        let cnc = create(&temp.0);
        let conductor =
            Conductor::new(cnc, &config_with(&temp.0, liveness_ns, timer_ns)).expect("conductor");
        (temp, conductor)
    }

    /// A reader over the conductor's to-clients ring, from a second mapping.
    fn events_reader(dir: &std::path::Path) -> (CncFile, ToClientsReceiver) {
        let cnc = CncFile::try_open(dir).expect("the file is published");
        let region = cnc.to_clients_region().expect("the event ring");
        let receiver = ToClientsReceiver::new(&region).expect("a broadcast ring");
        (cnc, receiver)
    }

    /// Everything on the ring, as `(type_id, payload)`.
    fn drain(cnc: &CncFile, receiver: &mut ToClientsReceiver) -> Vec<(i32, Vec<u8>)> {
        let region = cnc.to_clients_region().expect("the event ring");
        let mut out = Vec::new();
        while let Received::Message { type_id } = receiver.receive(&region) {
            out.push((type_id, receiver.message().to_vec()));
        }
        out
    }

    /// `ADD_COUNTER`'s wire form: the correlated head, the type id, and a key
    /// and label each with its own length and the key padded to four.
    fn add_counter_payload(
        client_id: i64,
        registration_id: i64,
        type_id: i32,
        key: &[u8],
        label: &[u8],
    ) -> Vec<u8> {
        #[allow(clippy::cast_possible_truncation)] // test-sized
        let mut out = Vec::new();
        out.extend_from_slice(&client_id.to_le_bytes());
        out.extend_from_slice(&registration_id.to_le_bytes());
        out.extend_from_slice(&type_id.to_le_bytes());
        out.extend_from_slice(&(key.len() as i32).to_le_bytes());
        out.extend_from_slice(key);
        out.resize(out.len() + (4 - key.len() % 4) % 4, 0);
        out.extend_from_slice(&(label.len() as i32).to_le_bytes());
        out.extend_from_slice(label);
        out
    }

    /// A directory a publication can be created in, and the settings to
    /// create it with.
    ///
    /// The term length is a test's rather than the driver's: the default is
    /// 64 MiB, and a log buffer is three terms and a metadata page — 192 MiB
    /// per publication, which is not what a unit test should write to a disk.
    ///
    /// The directory itself is made by [`crate::dir::prepare`], which is the
    /// driver's own start-up step and the thing that creates `publications/`:
    /// a test that only made the CnC file would be testing a driver whose
    /// agent cannot create anything.
    fn publication_config(dir: &std::path::Path) -> DriverConfig {
        let config = DriverConfig {
            aeron_dir: dir.to_owned(),
            ipc_term_buffer_length: 64 * 1024,
            ..DriverConfig::default()
        };
        crate::dir::prepare(&config, clock::epoch_millis()).expect("the directory is prepared");

        config
    }

    /// `ADD_PUBLICATION`'s wire form, written by the client-side encoder so
    /// that the two directions of the protocol are checked against each other
    /// in every test that sends one.
    fn add_publication_payload(
        client_id: i64,
        correlation_id: i64,
        stream_id: i32,
        channel: &str,
    ) -> Vec<u8> {
        let command = deepmsg_cnc::command::AddPublication {
            client_id,
            correlation_id,
            stream_id,
            channel,
        };
        let mut out = vec![0u8; command.encoded_length()];
        assert!(command.encode_into(&mut out));

        out
    }

    /// Drive the conductor until an event of `type_id` turns up, and return
    /// its payload.
    ///
    /// A publication's log buffer is created by the agent thread, so its reply
    /// lands a pass or two after the command: draining once would be asserting
    /// on the speed of a file system, and a deadline is what makes the
    /// assertion about the driver instead.
    ///
    /// Anything else that arrives is **kept** in `pending` rather than thrown
    /// away. A subscription's answer and its image are two events that arrive
    /// together, and a helper that dropped the second while waiting for the
    /// first would send its caller looking for an event it had already read.
    fn await_event(
        conductor: &mut Conductor,
        cnc: &CncFile,
        receiver: &mut ToClientsReceiver,
        pending: &mut Vec<(i32, Vec<u8>)>,
        type_id: i32,
    ) -> Vec<u8> {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);

        loop {
            if let Some(index) = pending.iter().position(|(id, _)| *id == type_id) {
                return pending.swap_remove(index).1;
            }

            pending.extend(drain(cnc, receiver));

            assert!(
                std::time::Instant::now() < deadline,
                "no event with type {type_id} arrived (pending: {:?})",
                pending.iter().map(|(id, _)| *id).collect::<Vec<_>>()
            );

            conductor.do_work();
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    /// `REMOVE_PUBLICATION`'s wire form, in either shape.
    ///
    /// Written here rather than in the `cnc` crate because the client's own
    /// removal API does not exist yet — the client's life cycle is the next
    /// commit — and a protocol encoder with no client to use it would be a
    /// promise rather than a contract.
    fn remove_publication_payload(
        client_id: i64,
        correlation_id: i64,
        registration_id: i64,
        flags: i64,
        with_flags: bool,
    ) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&client_id.to_le_bytes());
        out.extend_from_slice(&correlation_id.to_le_bytes());
        out.extend_from_slice(&registration_id.to_le_bytes());
        if with_flags {
            out.extend_from_slice(&flags.to_le_bytes());
        }

        out
    }

    /// `REMOVE_SUBSCRIPTION`'s wire form: the correlated head and the
    /// subscription's registration id.
    fn remove_subscription_payload(
        client_id: i64,
        correlation_id: i64,
        registration_id: i64,
    ) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&client_id.to_le_bytes());
        out.extend_from_slice(&correlation_id.to_le_bytes());
        out.extend_from_slice(&registration_id.to_le_bytes());

        out
    }

    /// A counter's value, with the regions taken and given back **inside** the
    /// call: they borrow the CnC file, so a test that held them across a pass
    /// could not drive the conductor at all.
    fn counter_value(conductor: &Conductor, counter_id: i32) -> Option<i64> {
        let regions = counter_regions(conductor);
        conductor.counters().value(&regions, counter_id)
    }

    /// Write a counter, for the same reason.
    fn set_counter(conductor: &Conductor, counter_id: i32, value: i64) {
        let regions = counter_regions(conductor);
        conductor.counters().set_value(&regions, counter_id, value);
    }

    /// Wait for the agent thread to remove a file.
    ///
    /// The conductor asks and the agent does it, so a test that asserted on the
    /// next line would be racing a thread it owns. The deadline is what keeps a
    /// removal that never happens from hanging the suite.
    fn await_removed(conductor: &mut Conductor, path: &std::path::Path) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);

        while path.exists() {
            assert!(
                std::time::Instant::now() < deadline,
                "{} was never removed",
                path.display()
            );
            conductor.do_work();
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    /// `ADD_SUBSCRIPTION`'s wire form, from the client-side encoder.
    fn add_subscription_payload(
        client_id: i64,
        correlation_id: i64,
        stream_id: i32,
        channel: &str,
    ) -> Vec<u8> {
        let command = deepmsg_cnc::command::AddSubscription {
            client_id,
            correlation_id,
            registration_correlation_id: -1,
            stream_id,
            channel,
        };
        let mut out = vec![0u8; command.encoded_length()];
        assert!(command.encode_into(&mut out));

        out
    }

    /// `REMOVE_COUNTER`'s wire form: the correlated head and the **counter's**
    /// registration id.
    fn remove_counter_payload(
        client_id: i64,
        correlation_id: i64,
        counter_registration_id: i64,
    ) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&client_id.to_le_bytes());
        out.extend_from_slice(&correlation_id.to_le_bytes());
        out.extend_from_slice(&counter_registration_id.to_le_bytes());
        out
    }

    #[test]
    fn taking_over_a_file_publishes_it_and_starts_the_heartbeat() {
        let (_temp, conductor) = running(TerminationPolicy::Deny);

        assert_eq!(
            deepmsg_core::version::CNC_VERSION,
            conductor.cnc.cnc_version(),
            "the conductor is what publishes the ready version"
        );
        assert!(
            conductor
                .cnc
                .driver_is_active(clock::epoch_millis(), 10_000),
            "and what sets the heartbeat that makes it look alive: {}",
            heartbeat(&conductor)
        );
    }

    #[test]
    fn the_first_client_id_is_one_because_the_driver_burns_zero() {
        let (_temp, conductor) = running(TerminationPolicy::Deny);

        let ring = conductor.cnc.to_driver_ring().expect("producer view");
        assert_eq!(
            Some(1),
            ring.next_correlation_id(),
            "the driver takes one at startup (`aeron_driver.c:970`)"
        );
    }

    #[test]
    fn the_timeout_tier_refreshes_the_heartbeat_and_counts_as_work() {
        let (_temp, mut conductor) = running(TerminationPolicy::Deny);

        // The deadline is seeded to now, so the first pass runs the tier.
        assert!(conductor.do_work() >= 1, "the timeout tier ran");
        let first = heartbeat(&conductor);

        // And not again until the timer interval has passed.
        assert_eq!(
            0,
            conductor.do_work(),
            "nothing to do: no commands, no tier"
        );
        assert_eq!(first, heartbeat(&conductor));
    }

    #[test]
    fn a_denied_termination_leaves_the_driver_running() {
        let (_temp, mut conductor) = running(TerminationPolicy::Deny);

        send(&conductor, 0x0E, &terminate_payload());
        conductor.do_work();

        assert!(conductor.is_running(), "the default policy refuses");
        assert_eq!(0, conductor.unhandled_commands());
    }

    #[test]
    fn an_allowed_termination_stops_the_driver() {
        let (_temp, mut conductor) = running(TerminationPolicy::Allow);

        send(&conductor, 0x0E, &terminate_payload());
        conductor.do_work();

        assert!(!conductor.is_running());
    }

    #[test]
    fn the_drain_limit_is_one_command_per_pass() {
        let (_temp, mut conductor) = running(TerminationPolicy::Deny);

        // Two commands that are both counted, so the counts say how many the
        // pass read without any byte arithmetic.
        //
        // A type id the protocol does not define, and there is no other kind
        // left to use: this test wants a command whose handling cannot start
        // happening, and as of `ADD_STATIC_COUNTER` every one of the eighteen
        // the protocol defines is served. The stand-in has run out — `0x07` was
        // one until ADD_DESTINATION, `0x10` until REJECT_IMAGE, `0x12` until
        // GET_NEXT_AVAILABLE_SESSION_ID, `0x0F` until this one — so what is
        // counted below is `unknown_commands` and not `unhandled_commands`.
        send(&conductor, 0x7F, b"first");
        send(&conductor, 0x7F, b"second");

        conductor.do_work();
        assert_eq!(1, conductor.unknown_commands(), "one command per pass");

        // And the ring is not stuck: the next pass finds the other one.
        conductor.do_work();
        assert_eq!(2, conductor.unknown_commands());

        // A third pass finds nothing, and the second pass left the ring empty.
        conductor.do_work();
        assert_eq!(2, conductor.unknown_commands());

        let ring = conductor.cnc.to_driver_ring().expect("producer view");
        let consumed =
            2 * layout::align_up(5 + layout::RECORD_HEADER_LENGTH, layout::RECORD_ALIGNMENT);
        assert_eq!(
            Some(consumed as i64),
            ring.consumer_position(),
            "both records were consumed, and only they"
        );
    }

    /// `ADD_DESTINATION` reaches a handler now, and what it answers is the
    /// reference's own unknown-publication error (`:5411-5420`).
    ///
    /// The publication named does not exist, which is the point: the command
    /// was decoded, looked up and answered, where before it was counted and
    /// named. A test that only asserted the counter would pass on a handler
    /// that answered nothing.
    #[test]
    fn an_add_destination_is_answered_rather_than_left_unhandled() {
        use deepmsg_cnc::command::{ADD_DESTINATION_TYPE_ID, DestinationCommand, ON_ERROR_TYPE_ID};

        let (temp, mut conductor) = running(TerminationPolicy::Deny);
        let (cnc, mut receiver) = events_reader(&temp.0);

        let command = DestinationCommand {
            client_id: 7,
            correlation_id: 9,
            registration_id: 4242,
            channel: "aeron:udp?endpoint=127.0.0.1:40456",
        };
        let mut payload = vec![0u8; command.encoded_length()];
        assert!(command.encode_into(&mut payload));

        send(&conductor, ADD_DESTINATION_TYPE_ID, &payload);
        conductor.do_work();

        assert_eq!(0, conductor.unhandled_commands(), "it is handled now");

        let responses = drain(&cnc, &mut receiver);
        assert_eq!(1, responses.len(), "one answer, and it is an error");
        assert_eq!(ON_ERROR_TYPE_ID, responses[0].0);
        assert_eq!(
            9i64.to_le_bytes(),
            responses[0].1[..8],
            "answered against the command that asked"
        );
    }

    /// `ADD_RCV_DESTINATION` is triaged by the prefix of the channel it names
    /// (`:3051-3065`): `aeron:ipc`, `aeron-spy:`, or a network channel.
    ///
    /// Only the first is refused by name — a client told nothing waits out its
    /// timeout, and this is not a command this driver will get to later. The
    /// other two are **served**: a spy destination is a local read added to a
    /// multi-destination subscription (`:5704-5806`) and a network one is a
    /// socket added to any network subscription (`:5879-5910`), and both reach
    /// the subscription first. The registration id here names no subscription,
    /// so what this covers is the triage — each prefix reaching its own branch,
    /// with each branch's own answer for a subscription that is not there.
    /// What a destination does once it is attached is `media::receive_endpoint`'s
    /// tests, and a spy destination against a real subscription is
    /// `tests/integration/spy_subscription.rs`.
    #[test]
    fn a_receive_destination_is_triaged_by_the_prefix_it_names() {
        use deepmsg_cnc::command::{
            ADD_RECEIVE_DESTINATION_TYPE_ID, DestinationCommand, ON_ERROR_TYPE_ID,
        };

        let channels = [
            (
                "aeron:ipc",
                "aeron:ipc destinations are not served by this driver",
            ),
            // A spy names a subscription it cannot find, which is the
            // reference's own unknown-subscription error (`:6053-6062`).
            (
                "aeron-spy:aeron:udp?endpoint=127.0.0.1:40456",
                "unknown subscription",
            ),
            ("aeron:udp?endpoint=127.0.0.1:40456", "unknown subscription"),
        ];

        for (channel, expected) in channels {
            let (temp, mut conductor) = running(TerminationPolicy::Deny);
            let (cnc, mut receiver) = events_reader(&temp.0);

            let command = DestinationCommand {
                client_id: 7,
                correlation_id: 9,
                registration_id: 4242,
                channel,
            };
            let mut payload = vec![0u8; command.encoded_length()];
            assert!(command.encode_into(&mut payload));

            send(&conductor, ADD_RECEIVE_DESTINATION_TYPE_ID, &payload);
            conductor.do_work();

            assert_eq!(0, conductor.unhandled_commands(), "{channel}");

            let responses = drain(&cnc, &mut receiver);
            assert_eq!(1, responses.len(), "{channel}: one answer, and an error");
            assert_eq!(ON_ERROR_TYPE_ID, responses[0].0, "{channel}");
            assert_eq!(
                9i64.to_le_bytes(),
                responses[0].1[..8],
                "{channel}: answered against the command that asked"
            );
            assert!(
                responses[0]
                    .1
                    .windows(expected.len())
                    .any(|window| window == expected.as_bytes()),
                "{channel}: the refusal names what it refused"
            );
        }
    }

    /// `REMOVE_DESTINATION_BY_ID` is the one command in the family whose
    /// failures the reference answers **nothing** to: it calls its handler
    /// without taking the result (`:3188-3200`), so the error for a publication
    /// it cannot find never reaches the `result < 0` that would send an
    /// `ON_ERROR` (`:3222-3225`). This build reproduces that rather than
    /// improving on it — `docs/compat.md` carries the line — so a client that
    /// names a publication the driver does not have waits, and times out.
    #[test]
    fn a_remove_destination_by_id_that_finds_nothing_answers_nothing() {
        use deepmsg_cnc::command::{DestinationByIdCommand, REMOVE_DESTINATION_BY_ID_TYPE_ID};

        let (temp, mut conductor) = running(TerminationPolicy::Deny);
        let (cnc, mut receiver) = events_reader(&temp.0);

        let command = DestinationByIdCommand {
            client_id: 7,
            correlation_id: 9,
            resource_registration_id: 4242,
            destination_registration_id: 43,
        };
        let mut payload = vec![0u8; DestinationByIdCommand::ENCODED_LENGTH];
        assert!(command.encode_into(&mut payload));

        send(&conductor, REMOVE_DESTINATION_BY_ID_TYPE_ID, &payload);
        conductor.do_work();

        assert_eq!(0, conductor.unhandled_commands(), "it is handled now");
        assert!(
            drain(&cnc, &mut receiver).is_empty(),
            "not even the error — the reference sends none, and the client waits"
        );
        assert!(conductor.is_running(), "and nothing else happened");
    }

    #[test]
    fn a_type_id_the_protocol_does_not_define_is_counted_separately() {
        let (_temp, mut conductor) = running(TerminationPolicy::Deny);

        send(&conductor, 0x7F, b"");
        conductor.do_work();

        assert_eq!(0, conductor.unhandled_commands());
        assert_eq!(1, conductor.unknown_commands());
        assert_eq!(Some(Command::Unknown(0x7F)), conductor.last_unhandled());

        // And the fault reaches the distinct error log in the adapter's
        // words — the composition the reference's `AERON_SET_ERR` builds —
        // with the errors counter bumped to match
        // (`aeron_driver_conductor.c:3218-3221`).
        let mut errors = Vec::new();
        let log = conductor.cnc.error_log().expect("the error log");
        assert_eq!(1, log.read(i64::MIN, &mut errors).entries);
        assert_eq!(
            "(-6) unknown command type id\n\
             [aeron_driver_conductor_on_command, aeron_driver_conductor.c:3219] \
             command=127 unknown\n",
            errors[0].text
        );
        assert_eq!(
            Some(1),
            counter_value(&conductor, system_counters::id::ERRORS)
        );
    }

    #[test]
    fn a_keepalive_from_a_client_this_driver_has_never_seen_is_not_an_error() {
        let (_temp, mut conductor) = running(TerminationPolicy::Deny);

        // The command's payload is a client id and a correlation id; both are
        // ignored here, exactly as the reference ignores an unknown client's.
        send(&conductor, 0x06, &[0u8; 16]);
        conductor.do_work();

        assert_eq!(0, conductor.unhandled_commands());
        assert_eq!(0, conductor.unknown_commands());
    }

    #[test]
    fn closing_reclaims_the_counters_the_driver_owns() {
        let (temp, mut conductor) = running_with(10_000_000_000, 1_000_000_000);

        conductor.close().expect("close");

        let reader = CncFile::try_open(&temp.0).expect("the file is published");
        let counters = reader.counters().expect("the counter regions");
        let scan = counters.for_each(|_| {});
        assert_eq!(0, scan.allocated, "a stopped driver publishes none");
        assert_eq!(46, scan.reclaimed, "and leaves forty-six reclaimed slots");
        assert!(
            !conductor.is_running(),
            "and a closed conductor is not a running one"
        );
    }

    #[test]
    fn closing_publishes_the_stop_signal_and_flushes_it() {
        let (_temp, mut conductor) = running(TerminationPolicy::Deny);

        conductor.close().expect("close");

        assert_eq!(
            Some(NULL_VALUE),
            conductor.cnc.consumer_heartbeat_ms(),
            "a clean stop is -1, not a timestamp that stops being refreshed"
        );
        assert!(
            !conductor
                .cnc
                .driver_is_active(clock::epoch_millis(), i64::MAX)
        );
    }

    #[test]
    fn a_command_is_consumed_exactly_once() {
        let (_temp, mut conductor) = running(TerminationPolicy::Deny);

        send(&conductor, 0x7F, b"channel");
        conductor.do_work();
        assert_eq!(1, conductor.unknown_commands());

        conductor.do_work();
        assert_eq!(
            1,
            conductor.unknown_commands(),
            "the second pass finds an empty ring"
        );

        let ring = conductor.cnc.to_driver_ring().expect("producer view");
        let consumed = layout::align_up(7 + layout::RECORD_HEADER_LENGTH, layout::RECORD_ALIGNMENT);
        assert_eq!(Some(consumed as i64), ring.consumer_position());
    }

    #[test]
    fn the_system_counters_are_published_with_the_file() {
        let (temp, conductor) = running_with(10_000_000_000, 1_000_000_000);

        let counters = conductor.counters();
        assert_eq!(45, counters.id_high_water_mark(), "all forty-six are there");

        // Read back through the client's own decoding, from a second mapping.
        let (cnc, _receiver) = events_reader(&temp.0);
        let observer = cnc.counters().expect("the counter regions");
        assert_eq!(46, observer.for_each(|_| {}).allocated);
        assert_eq!(
            Some(79_106),
            observer.value(34),
            "the Aeron version this build implements"
        );
    }

    #[test]
    fn an_add_counter_registers_the_client_and_announces_both_counters() {
        let (temp, mut conductor) = running(TerminationPolicy::Deny);
        let (cnc, mut receiver) = events_reader(&temp.0);

        send(
            &conductor,
            0x09,
            &add_counter_payload(7, 99, 100, b"hello", b"a counter"),
        );
        conductor.do_work();

        // The heartbeat counter is the first free id: the system counters took
        // 0..=45.
        assert_eq!(
            vec![
                (
                    deepmsg_cnc::command::ON_COUNTER_READY_TYPE_ID,
                    encode_counter_update(7, 46).to_vec()
                ),
                (
                    deepmsg_cnc::command::ON_COUNTER_READY_TYPE_ID,
                    encode_counter_update(99, 47).to_vec()
                ),
            ],
            drain(&cnc, &mut receiver),
            "the client's own heartbeat first, under its client id, then the counter it asked for"
        );

        let observer = cnc.counters().expect("the counter regions");
        assert_eq!(
            2,
            observer.for_each(|_| {}).allocated.saturating_sub(46),
            "the client's heartbeat and the counter it asked for"
        );
        let counter = observer
            .find_by_type_and_registration(100, 99)
            .expect("the application counter");
        assert_eq!(47, counter);
        assert_eq!(
            7,
            observer
                .find_by_type_id(11)
                .expect("a heartbeat")
                .registration_id
        );

        assert_eq!(1, conductor.clients().len());
        assert_eq!(0, conductor.counter_failures());
        assert_eq!(0, conductor.broadcast_failures());
    }

    #[test]
    fn a_silent_client_is_reaped_and_the_ring_says_so() {
        // Ten milliseconds of liveness and a one-millisecond tier: the client
        // registers, never writes its heartbeat again, and the next tier that
        // runs finds it gone.
        let (temp, mut conductor) = running_with(10_000_000, 1_000_000);
        let (cnc, mut receiver) = events_reader(&temp.0);

        send(
            &conductor,
            0x09,
            &add_counter_payload(7, 99, 100, &[], b"c"),
        );
        conductor.do_work();
        drain(&cnc, &mut receiver);

        std::thread::sleep(std::time::Duration::from_millis(20));
        // The tier runs on its own deadline; forcing it keeps the test to one
        // pass rather than a sleep long enough for a real timer.
        conductor.timeout_check_deadline_ns = 0;
        conductor.do_work();

        assert_eq!(
            vec![
                (
                    deepmsg_cnc::command::ON_CLIENT_TIMEOUT_TYPE_ID,
                    encode_client_timeout(7).to_vec()
                ),
                (
                    deepmsg_cnc::command::ON_UNAVAILABLE_COUNTER_TYPE_ID,
                    encode_counter_update(7, 46).to_vec()
                ),
                (
                    deepmsg_cnc::command::ON_UNAVAILABLE_COUNTER_TYPE_ID,
                    encode_counter_update(99, 47).to_vec()
                ),
            ],
            drain(&cnc, &mut receiver),
            "the timeout, the heartbeat, then the counters the client owned"
        );

        assert!(conductor.clients().is_empty());
        let observer = cnc.counters().expect("the counter regions");
        assert_eq!(
            2,
            observer.for_each(|_| {}).reclaimed,
            "both counters are back in the pool"
        );

        let reader = CncFile::try_open(&temp.0).expect("the file is published");
        assert_eq!(
            Some(1),
            reader
                .counters()
                .and_then(|c| c.value(system_counters::id::CLIENT_TIMEOUTS)),
            "system counter 24 counts it"
        );
    }

    #[test]
    fn a_client_that_closes_itself_is_collected_without_a_timeout() {
        let (temp, mut conductor) = running_with(10_000_000, 1_000_000);
        let (cnc, mut receiver) = events_reader(&temp.0);

        send(
            &conductor,
            0x09,
            &add_counter_payload(7, 99, 100, &[], b"c"),
        );
        conductor.do_work();
        drain(&cnc, &mut receiver);

        let mut close = Vec::new();
        close.extend_from_slice(&7i64.to_le_bytes());
        close.extend_from_slice(&1i64.to_le_bytes());
        send(&conductor, 0x0B, &close);
        conductor.do_work();

        // The close zeroed the heartbeat; the *next* tier is what collects it,
        // which is how the reference does it too.
        conductor.timeout_check_deadline_ns = 0;
        conductor.do_work();

        let events = drain(&cnc, &mut receiver);
        assert_eq!(2, events.len());
        assert!(
            events
                .iter()
                .all(|(type_id, _)| *type_id != deepmsg_cnc::command::ON_CLIENT_TIMEOUT_TYPE_ID),
            "a client that said goodbye is not announced as timed out"
        );
        assert!(conductor.clients().is_empty());
        assert_eq!(
            Some(0),
            cnc.counters()
                .and_then(|c| c.value(system_counters::id::CLIENT_TIMEOUTS)),
            "and it is not counted as one either"
        );
    }

    #[test]
    fn a_keepalive_refreshes_a_client_this_driver_knows() {
        let (_temp, mut conductor) = running(TerminationPolicy::Deny);

        send(
            &conductor,
            0x09,
            &add_counter_payload(7, 99, 100, &[], b"c"),
        );
        conductor.do_work();
        let registered = conductor.counters().value(&counter_regions(&conductor), 46);

        let mut keepalive = Vec::new();
        keepalive.extend_from_slice(&7i64.to_le_bytes());
        keepalive.extend_from_slice(&1i64.to_le_bytes());
        send(&conductor, 0x06, &keepalive);
        conductor.do_work();

        assert!(conductor.counters().value(&counter_regions(&conductor), 46) >= registered);
        assert_eq!(0, conductor.unhandled_commands(), "handled, not unhandled");

        // And one from a client it has never seen is neither an error nor a
        // registration.
        let mut stranger = Vec::new();
        stranger.extend_from_slice(&8i64.to_le_bytes());
        stranger.extend_from_slice(&2i64.to_le_bytes());
        send(&conductor, 0x06, &stranger);
        conductor.do_work();
        assert_eq!(1, conductor.clients().len());
    }

    #[test]
    fn a_remove_counter_frees_the_counter_and_announces_it() {
        let (temp, mut conductor) = running(TerminationPolicy::Deny);
        let (cnc, mut receiver) = events_reader(&temp.0);

        send(
            &conductor,
            0x09,
            &add_counter_payload(7, 99, 100, &[], b"c"),
        );
        conductor.do_work();
        drain(&cnc, &mut receiver);

        send(&conductor, 0x0A, &remove_counter_payload(7, 5, 99));
        conductor.do_work();

        assert_eq!(
            vec![
                (
                    deepmsg_cnc::command::ON_OPERATION_SUCCEEDED_TYPE_ID,
                    encode_operation_succeeded(5).to_vec()
                ),
                (
                    deepmsg_cnc::command::ON_UNAVAILABLE_COUNTER_TYPE_ID,
                    encode_counter_update(99, 47).to_vec()
                ),
            ],
            drain(&cnc, &mut receiver),
            "the command unblocks first, then the counter goes away — the reference's order"
        );
        assert_eq!(
            1,
            cnc.counters().expect("regions").for_each(|_| {}).reclaimed,
            "the counter is back in the pool"
        );
        assert!(
            conductor
                .clients()
                .find(7)
                .expect("the client is still here")
                .counter_links
                .is_empty()
        );

        // A second removal finds nothing, and so does one from a stranger —
        // and each is *answered*, so the client that asked is not left waiting
        // for a deadline (`aeron_driver_conductor.c:6244-6252`).
        send(&conductor, 0x0A, &remove_counter_payload(7, 6, 99));
        conductor.do_work();
        send(&conductor, 0x0A, &remove_counter_payload(8, 7, 99));
        conductor.do_work();
        assert_eq!(2, conductor.unknown_counters());

        let errors = drain(&cnc, &mut receiver);
        assert_eq!(2, errors.len());
        for (type_id, payload) in errors {
            assert_eq!(deepmsg_cnc::command::ON_ERROR_TYPE_ID, type_id);
            assert_eq!(
                deepmsg_cnc::command::ERROR_CODE_UNKNOWN_COUNTER,
                i32::from_le_bytes(payload[8..12].try_into().expect("four bytes")),
                "the code the reference uses for this failure"
            );
        }
    }

    #[test]
    fn a_malformed_command_is_counted_and_changes_nothing() {
        let (_temp, mut conductor) = running(TerminationPolicy::Deny);

        // An ADD_COUNTER whose label length runs past the payload: the field
        // sits four bytes before the label itself.
        let mut payload = add_counter_payload(7, 99, 100, &[], b"label");
        let label_length_offset = payload.len() - b"label".len() - 4;
        payload[label_length_offset..label_length_offset + 4]
            .copy_from_slice(&i32::MAX.to_le_bytes());

        send(&conductor, 0x09, &payload);
        conductor.do_work();

        assert_eq!(1, conductor.malformed_commands());
        assert_eq!(0, conductor.clients().len(), "no client was registered");
        assert_eq!(45, conductor.counters().id_high_water_mark());

        // And the fault reaches the distinct error log in the reference
        // adapter's words — composed, as its `AERON_SET_ERR` composes — with
        // the errors counter bumped to match (`aeron_driver_conductor.c:3231-3235`).
        let mut errors = Vec::new();
        let log = conductor.cnc.error_log().expect("the error log");
        assert_eq!(1, log.read(i64::MIN, &mut errors).entries);
        assert_eq!(
            format!(
                "(-7) malformed command\n\
                 [aeron_driver_conductor_on_command, aeron_driver_conductor.c:3232] \
                 command=9 too short: length={}\n",
                payload.len()
            ),
            errors[0].text
        );
        assert_eq!(
            Some(1),
            counter_value(&conductor, system_counters::id::ERRORS)
        );
    }

    #[test]
    fn a_broadcast_failure_is_recorded_and_counted() {
        // The reversal of the P1-1 review's D4: the reference's
        // `client_transmit` failure is logged like any driver error, and
        // logging is what bumps the errors counter — one bump per refused
        // message, with the distinct log collapsing them into one entry
        // (`aeron_driver_conductor.c:2233-2241` feeding `:1203-1215`).
        let (_temp, mut conductor) = running(TerminationPolicy::Deny);
        conductor.pending_broadcast_failures = 3;

        conductor.flush_broadcast_failures();

        assert_eq!(3, conductor.broadcast_failures());
        assert_eq!(
            Some(3),
            counter_value(&conductor, system_counters::id::ERRORS),
            "one bump per refused message"
        );

        let mut errors = Vec::new();
        let log = conductor.cnc.error_log().expect("the error log");
        assert_eq!(1, log.read(i64::MIN, &mut errors).entries);
        assert_eq!("failed to transmit message", errors[0].text);
        assert_eq!(3, errors[0].observation_count);
    }

    #[test]
    fn a_low_space_warning_is_recorded_without_counting_and_the_log_buffer_lands() {
        // The storage check's second half (`aeron_driver_context.c:1368-1377`):
        // a filesystem that can hold the log buffer but sits at or below the
        // threshold gets a warning in the distinct error log — written
        // directly, not through `log_explicit_error`, so the errors counter
        // stays where it was — and the create goes ahead. A threshold of
        // `i64::MAX` stands in for a nearly-full filesystem, and prints as
        // the reference's `PRId64` prints it: signed.
        let temp = TempDir::new();
        let config = DriverConfig {
            aeron_dir: temp.0.clone(),
            ipc_term_buffer_length: 64 * 1024,
            low_file_store_warning_threshold: i64::MAX as u64,
            ..DriverConfig::default()
        };
        crate::dir::prepare(&config, clock::epoch_millis()).expect("the directory is prepared");
        let cnc = create(&temp.0);
        let mut conductor = Conductor::new(cnc, &config).expect("conductor");
        let (cnc, mut receiver) = events_reader(&temp.0);
        let mut pending = Vec::new();

        send(
            &conductor,
            ADD_PUBLICATION_TYPE_ID,
            &add_publication_payload(7, 42, 1001, "aeron:ipc"),
        );
        await_event(
            &mut conductor,
            &cnc,
            &mut receiver,
            &mut pending,
            ON_PUBLICATION_READY_TYPE_ID,
        );

        assert_eq!(
            1,
            conductor.publications().publications().len(),
            "the warning never stopped the create"
        );

        let mut errors = Vec::new();
        let log = conductor.cnc.error_log().expect("the error log");
        assert_eq!(1, log.read(i64::MIN, &mut errors).entries);
        assert_eq!(1, errors[0].observation_count);
        assert!(
            errors[0].text.starts_with(
                "(-12) insufficient storage space\n\
                 [aeron_driver_context_run_storage_checks, aeron_driver_context.c:1370] \
                 WARNING: space is running low: threshold=9223372036854775807 usable="
            ),
            "the reference's composition and words, with the threshold it was given: {}",
            errors[0].text
        );
        assert!(
            errors[0]
                .text
                .ends_with(&format!(" in {}\n", temp.0.display())),
            "and the directory it asked about: {}",
            errors[0].text
        );
        assert_eq!(
            Some(0),
            counter_value(&conductor, system_counters::id::ERRORS),
            "a warning is not a counted error"
        );
    }

    #[test]
    fn a_refused_log_buffer_is_answered_and_recorded() {
        // The refusal half of the same check, end to end. A filesystem that
        // cannot hold the log refuses it (`aeron_driver_context.c:1360-1366`),
        // the agent reports the failure (`:432-436`), the publication's state
        // machine turns it into ERROR (`aeron_driver_conductor.c:4064-4067`)
        // and the conductor answers the client *and* records the error, in one
        // act (`:3304-3315` → `on_error`'s own `log_error:` label, `:2366-2370`).
        //
        // A directory that is gone is a filesystem that reports nothing
        // usable, which is how this build's `usable_fs_space` reads one — the
        // same stand-in the agent's own refusal test uses.
        let temp = TempDir::new();
        let config = publication_config(&temp.0);
        let cnc = create(&temp.0);
        let mut conductor = Conductor::new(cnc, &config).expect("conductor");
        let (cnc, mut receiver) = events_reader(&temp.0);

        std::fs::remove_dir_all(&temp.0).expect("the filesystem is gone");

        send(
            &conductor,
            ADD_PUBLICATION_TYPE_ID,
            &add_publication_payload(7, 42, 1001, "aeron:ipc"),
        );

        // The map is the agent's, so the answer arrives on a later pass than
        // the command did.
        let mut pending = Vec::new();
        let answer = await_event(
            &mut conductor,
            &cnc,
            &mut receiver,
            &mut pending,
            ON_ERROR_TYPE_ID,
        );
        assert_eq!(
            ERROR_CODE_STORAGE_SPACE,
            i32::from_le_bytes(answer[8..12].try_into().expect("four bytes")),
            "the refusal carries its own code, not the generic one"
        );

        // And the answer is an entry: same words, one sighting, one bump.
        let mut recorded = Vec::new();
        let log = conductor.cnc.error_log().expect("the error log");
        assert_eq!(1, log.read(i64::MIN, &mut recorded).entries);
        assert!(
            recorded[0]
                .text
                .starts_with("could not create the log buffer: "),
            "the entry holds what the client was told: {}",
            recorded[0].text
        );
        assert_eq!(1, recorded[0].observation_count);
        assert_eq!(
            Some(1),
            counter_value(&conductor, system_counters::id::ERRORS)
        );
    }

    #[test]
    fn a_ring_stalled_by_a_dead_producer_is_unblocked() {
        // What a client killed between claiming a command and committing it
        // leaves: a record whose length is the negative in-flight marker and a
        // tail already past it. Nothing can read it, so nothing can move the
        // consumer position, so the driver never sees another command — while
        // its heartbeat keeps telling every client it is alive.
        // A ten-second liveness window, because the window is what the test is
        // about: a stall is only a stall once it has outlasted it.
        let (temp, mut conductor) = running_with(10_000_000_000, 1_000_000);

        let claim_length = 24i64;
        let stalled = CncFile::try_open_writable(&temp.0).expect("the file is published");
        {
            let region = stalled.to_driver_region().expect("writable");
            region
                .store_i32_release(layout::RECORD_LENGTH_OFFSET, -(claim_length as i32))
                .expect("in range");
            region
                .store_i32_relaxed(layout::RECORD_MSG_TYPE_ID_OFFSET, 0x09)
                .expect("in range");

            let trailer = region.len() - layout::MPSC_RB_TRAILER_LENGTH;
            region
                .store_i64_release(trailer + layout::MPSC_TAIL_POSITION_OFFSET, claim_length)
                .expect("in range");
        }

        // A command the driver cannot read is not work, and the tier does
        // nothing until the stall has lasted a whole liveness window.
        conductor.timeout_check_deadline_ns = 0;
        conductor.do_work();
        assert_eq!(
            Some(0),
            conductor
                .cnc
                .to_driver_region()
                .and_then(|region| conductor.commands.consume_position(&region)),
            "the read stalls where it was"
        );

        // Age the stall past the window and run the tier again.
        conductor.time_of_last_position_change_ns -= 10_000_000_001;

        conductor.timeout_check_deadline_ns = 0;
        conductor.do_work();

        assert_eq!(
            Some(claim_length),
            conductor
                .cnc
                .to_driver_region()
                .and_then(|region| conductor.commands.consume_position(&region)),
            "the dead claim is stepped over and the ring moves again"
        );
        assert_eq!(
            Some(1),
            conductor.cnc.counter_regions().and_then(|regions| conductor
                .counters()
                .value(&regions, system_counters::id::UNBLOCKED_COMMANDS)),
            "and system counter 20 counts it"
        );

        // A live client's command still arrives normally afterwards.
        send(&conductor, 0x7F, b"after");
        conductor.do_work();
        assert_eq!(1, conductor.unknown_commands());
    }

    #[test]
    fn a_long_pass_is_measured_and_counted() {
        let (_temp, mut conductor) = running(TerminationPolicy::Deny);

        // The tracker measures the gap since the previous pass; moving that
        // mark back is how a test makes a pass look long without sleeping.
        conductor.last_cycle_ns -= 250_000_000;
        conductor.do_work();

        let regions = counter_regions(&conductor);
        let measured = conductor
            .counters()
            .value(&regions, system_counters::id::CONDUCTOR_MAX_CYCLE_TIME)
            .expect("the counter");
        assert!(
            measured >= 250_000_000,
            "the longest pass is kept: {measured}"
        );
        assert_eq!(
            Some(1),
            conductor.counters().value(
                &regions,
                system_counters::id::CONDUCTOR_CYCLE_TIME_THRESHOLD_EXCEEDED
            ),
            "and it was past the threshold"
        );
    }
    #[test]
    fn an_add_publication_creates_a_log_buffer_and_answers_the_client() {
        let temp = TempDir::new();
        let config = publication_config(&temp.0);
        let cnc = create(&temp.0);
        let mut conductor = Conductor::new(cnc, &config).expect("conductor");
        let (cnc, mut receiver) = events_reader(&temp.0);
        let mut pending = Vec::new();

        let registration_id = 42;
        send(
            &conductor,
            ADD_PUBLICATION_TYPE_ID,
            &add_publication_payload(7, registration_id, 1001, "aeron:ipc"),
        );

        let payload = await_event(
            &mut conductor,
            &cnc,
            &mut receiver,
            &mut pending,
            ON_PUBLICATION_READY_TYPE_ID,
        );

        // The fixed head, then the log buffer's path with no padding.
        let path = temp.0.join("publications").join("42.logbuffer");
        let path = path.to_str().expect("a path in the temp directory");
        assert_eq!(36 + path.len(), payload.len());
        assert_eq!(registration_id.to_le_bytes(), payload[0..8], "the request");
        assert_eq!(
            registration_id.to_le_bytes(),
            payload[8..16],
            "and the publication it made, which is what names the file"
        );
        assert_eq!(1001i32.to_le_bytes(), payload[20..24], "the stream");
        assert_eq!(
            (-1i32).to_le_bytes(),
            payload[28..32],
            "an IPC channel has no status counter"
        );
        assert_eq!(path.as_bytes(), &payload[36..]);
        assert_eq!(0, conductor.publication_failures());

        // The session id is one the driver speculated, so it is not one of the
        // ids it keeps for itself.
        let session_id = i32::from_le_bytes(payload[16..20].try_into().expect("four bytes"));
        // The session id is one the driver speculated, so it is outside the
        // range the driver keeps for itself — and the range is on *both* sides
        // of zero, because the cursor starts from a random `i32`.
        assert!(
            !(PUBLICATION_RESERVED_SESSION_ID_LOW_DEFAULT
                ..=PUBLICATION_RESERVED_SESSION_ID_HIGH_DEFAULT)
                .contains(&session_id),
            "session {session_id} is inside the reserved range"
        );

        // The reply names the limit counter, and it is a real one.
        let limit_counter_id = i32::from_le_bytes(payload[24..28].try_into().expect("four bytes"));
        let publication = &conductor.publications().publications()[0];
        assert_eq!(publication.pub_lmt_counter_id, limit_counter_id);
        assert_eq!(publication.session_id, session_id);

        let regions = counter_regions(&conductor);
        let limit = conductor
            .counters()
            .value(&regions, limit_counter_id)
            .expect("the counter is readable");
        assert_eq!(0, limit, "nothing has been published yet");

        // Both counters are in the file for `AeronStat`, under the labels the
        // reference gives them: `name: <registration> <session> <stream>
        // <channel>`, with the session the one the reply named.
        let reader = regions.reader();
        assert_eq!(
            format!("pub-pos (concurrent): 42 {session_id} 1001 aeron:ipc"),
            reader
                .find_by_type_id(crate::position::type_id::PUBLISHER_POSITION)
                .expect("pub-pos")
                .label
        );
        assert_eq!(
            format!("pub-lmt: 42 {session_id} 1001 aeron:ipc"),
            reader
                .find_by_type_id(crate::position::type_id::PUBLISHER_LIMIT)
                .expect("pub-lmt")
                .label
        );

        // And the log buffer is there, describing the same stream.
        let log = std::fs::read(path).expect("the file the reply named");
        let metadata = &log[log.len() - descriptor::METADATA_LENGTH..];

        // The socket buffer lengths in it are the kernel's answer, put there by
        // the driver's own probe rather than by a test: this is the end of the
        // chain that starts at `default_socket_buffers`, and a driver that
        // wrote zeroes would fail here on any machine with a kernel.
        assert_eq!(
            config.socket_buffers.rcvbuf.to_le_bytes(),
            metadata[descriptor::OS_DEFAULT_SOCKET_RCVBUF_LENGTH_OFFSET
                ..descriptor::OS_DEFAULT_SOCKET_RCVBUF_LENGTH_OFFSET + 4]
        );
        assert_eq!(
            config.socket_buffers.sndbuf.to_le_bytes(),
            metadata[descriptor::OS_DEFAULT_SOCKET_SNDBUF_LENGTH_OFFSET
                ..descriptor::OS_DEFAULT_SOCKET_SNDBUF_LENGTH_OFFSET + 4]
        );
        assert_eq!(
            (64 * 1024i32).to_le_bytes(),
            metadata[descriptor::TERM_LENGTH_OFFSET..descriptor::TERM_LENGTH_OFFSET + 4]
        );
        assert_eq!(
            1408i32.to_le_bytes(),
            metadata[descriptor::MTU_LENGTH_OFFSET..descriptor::MTU_LENGTH_OFFSET + 4]
        );
        assert_eq!(
            session_id.to_le_bytes(),
            metadata[descriptor::DEFAULT_FRAME_HEADER_OFFSET + frame::SESSION_ID_FIELD_OFFSET
                ..descriptor::DEFAULT_FRAME_HEADER_OFFSET + frame::SESSION_ID_FIELD_OFFSET + 4]
        );
    }

    #[test]
    fn a_second_publication_on_the_same_stream_shares_the_first_log_buffer() {
        let temp = TempDir::new();
        let config = publication_config(&temp.0);
        let cnc = create(&temp.0);
        let mut conductor = Conductor::new(cnc, &config).expect("conductor");
        let (cnc, mut receiver) = events_reader(&temp.0);
        let mut pending = Vec::new();

        send(
            &conductor,
            ADD_PUBLICATION_TYPE_ID,
            &add_publication_payload(7, 42, 1001, "aeron:ipc"),
        );
        await_event(
            &mut conductor,
            &cnc,
            &mut receiver,
            &mut pending,
            ON_PUBLICATION_READY_TYPE_ID,
        );

        // The same client asking again for the same stream gets the *existing*
        // publication: its correlation id comes back in the first field and the
        // publication's own — 42, not 43 — in the second.
        send(
            &conductor,
            ADD_PUBLICATION_TYPE_ID,
            &add_publication_payload(7, 43, 1001, "aeron:ipc"),
        );
        let payload = await_event(
            &mut conductor,
            &cnc,
            &mut receiver,
            &mut pending,
            ON_PUBLICATION_READY_TYPE_ID,
        );

        assert_eq!(43i64.to_le_bytes(), payload[0..8]);
        assert_eq!(
            42i64.to_le_bytes(),
            payload[8..16],
            "the shared publication"
        );
        assert_eq!(1, conductor.publications().publications().len());
        assert_eq!(
            2,
            conductor
                .clients()
                .find(7)
                .expect("the client")
                .publication_links
                .len(),
            "both requests are links the client holds"
        );

        // An *exclusive* publication is a different stream by contract, so it
        // gets its own log buffer and its own reply type.
        send(
            &conductor,
            ADD_EXCLUSIVE_PUBLICATION_TYPE_ID,
            &add_publication_payload(7, 44, 1001, "aeron:ipc"),
        );
        let payload = await_event(
            &mut conductor,
            &cnc,
            &mut receiver,
            &mut pending,
            ON_EXCLUSIVE_PUBLICATION_READY_TYPE_ID,
        );

        assert_eq!(44i64.to_le_bytes(), payload[0..8]);
        assert_eq!(44i64.to_le_bytes(), payload[8..16], "its own publication");
        assert_eq!(2, conductor.publications().publications().len());
        assert!(temp.0.join("publications").join("44.logbuffer").exists());
    }

    #[test]
    fn a_session_id_that_is_taken_by_a_stream_this_cannot_share_is_a_clash() {
        let temp = TempDir::new();
        let config = publication_config(&temp.0);
        let cnc = create(&temp.0);
        let mut conductor = Conductor::new(cnc, &config).expect("conductor");
        let (cnc, mut receiver) = events_reader(&temp.0);
        let mut pending = Vec::new();

        // An exclusive publication holds session 5000 on stream 1001. It is
        // not shareable, so the session id it holds cannot be handed out again
        // — and the check that says so runs *after* the sharing lookup, which
        // is what makes this a clash rather than a link.
        send(
            &conductor,
            ADD_EXCLUSIVE_PUBLICATION_TYPE_ID,
            &add_publication_payload(7, 42, 1001, "aeron:ipc?session-id=5000"),
        );
        let payload = await_event(
            &mut conductor,
            &cnc,
            &mut receiver,
            &mut pending,
            ON_EXCLUSIVE_PUBLICATION_READY_TYPE_ID,
        );
        assert_eq!(
            5000i32.to_le_bytes(),
            payload[16..20],
            "a session the URI named is the session it gets"
        );

        send(
            &conductor,
            ADD_PUBLICATION_TYPE_ID,
            &add_publication_payload(7, 43, 1001, "aeron:ipc?session-id=5000"),
        );
        let payload = await_event(
            &mut conductor,
            &cnc,
            &mut receiver,
            &mut pending,
            ON_ERROR_TYPE_ID,
        );

        assert_eq!(43i64.to_le_bytes(), payload[0..8]);
        assert_eq!(
            ERROR_CODE_INVALID_CHANNEL.to_le_bytes(),
            payload[8..12],
            "the reference's clash code"
        );
        assert_eq!(1, conductor.publication_failures());

        // The same stream under a session nobody is using is a new publication.
        send(
            &conductor,
            ADD_PUBLICATION_TYPE_ID,
            &add_publication_payload(7, 44, 1001, "aeron:ipc?session-id=5001"),
        );
        let payload = await_event(
            &mut conductor,
            &cnc,
            &mut receiver,
            &mut pending,
            ON_PUBLICATION_READY_TYPE_ID,
        );

        assert_eq!(5001i32.to_le_bytes(), payload[16..20]);
        assert_eq!(44i64.to_le_bytes(), payload[8..16], "its own publication");
        assert_eq!(2, conductor.publications().publications().len());
    }

    /// The receive path, driven by hand: a subscription binds a socket, a
    /// `SETUP` arrives on it, an image is built from the setup, and the client
    /// is told where the log buffer is.
    ///
    /// Every step here is one the reference's own publisher would take over the
    /// wire; a plain socket takes them instead so that a failure can name the
    /// step it failed at.
    #[test]
    fn a_udp_subscription_binds_a_socket_and_an_image_forms_on_a_setup() {
        use crate::protocol::{DataFrame, FrameHeader, SetupFrame};
        use crate::sys::AddressFamily;
        use crate::sys::socket::{DatagramSocket, Datagrams};

        let temp = TempDir::new();
        let config = publication_config(&temp.0);
        let cnc = create(&temp.0);
        let mut conductor = Conductor::new(cnc, &config).expect("conductor");
        let (cnc, mut receiver) = events_reader(&temp.0);
        let mut pending = Vec::new();

        // A port nobody holds: the test's own socket will send to it, and the
        // driver's receive endpoint will bind it.
        let port = {
            let probe = DatagramSocket::open(AddressFamily::Inet).expect("a socket");
            probe
                .bind("127.0.0.1:0".parse().expect("an address"))
                .expect("a bind");
            probe.local_address().expect("an address").port()
        };

        let channel = format!("aeron:udp?endpoint=127.0.0.1:{port}");
        send(
            &conductor,
            ADD_SUBSCRIPTION_TYPE_ID,
            &add_subscription_payload(7, 11, 1001, &channel),
        );

        // The reply carries the *endpoint's* channel status counter: a client
        // that reads it learns whether the socket is up.
        let payload = await_event(
            &mut conductor,
            &cnc,
            &mut receiver,
            &mut pending,
            ON_SUBSCRIPTION_READY_TYPE_ID,
        );
        assert_eq!(11i64.to_le_bytes(), payload[0..8]);

        let status_counter_id = i32::from_le_bytes(payload[8..12].try_into().expect("four bytes"));
        assert!(status_counter_id >= 0, "the endpoint has a channel status");

        // The `SETUP` a publisher sends when it finds a subscriber waiting.
        let session_id = 99;
        let setup = SetupFrame {
            term_offset: 0,
            session_id,
            stream_id: 1001,
            initial_term_id: 1_000,
            active_term_id: 1_000,
            term_length: 64 * 1024,
            mtu: 1408,
            ttl: 0,
        };
        let mut frame = [0u8; SetupFrame::LENGTH];
        assert!(setup.write_with_flags(&mut frame, 0).is_some());

        let publisher = DatagramSocket::open(AddressFamily::Inet).expect("a socket");
        publisher
            .bind("127.0.0.1:0".parse().expect("an address"))
            .expect("a bind");
        publisher.set_nonblocking().expect("non-blocking");

        // The driver has to be given the pass to process the command before the
        // socket exists.
        for _ in 0..3 {
            conductor.do_work();
        }

        publisher
            .send_batch(
                Some(format!("127.0.0.1:{port}").parse().expect("an address")),
                &[&frame],
            )
            .expect("a send");

        // The image is built off the conductor's thread (a log buffer to map),
        // so this waits for it rather than asserting after one pass.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let mut images = 0;

        while std::time::Instant::now() < deadline {
            conductor.do_work();

            if conductor.publication_images().len() > images {
                images = conductor.publication_images().len();
                break;
            }

            std::thread::sleep(std::time::Duration::from_millis(5));
        }

        assert_eq!(
            1, images,
            "a setup with a subscriber waiting builds an image"
        );

        let image = conductor.publication_images()[0].clone();
        assert_eq!(session_id, image.session_id);
        assert_eq!(1001, image.stream_id);
        assert_eq!(
            1, image.refcount,
            "the subscription that was waiting is linked to it"
        );

        // The reader is told where the image's log buffer is.
        let payload = await_event(
            &mut conductor,
            &cnc,
            &mut receiver,
            &mut pending,
            ON_AVAILABLE_IMAGE_TYPE_ID,
        );
        assert_eq!(image.registration_id.to_le_bytes(), payload[0..8]);
        assert!(
            payload.windows(9).any(|window| window == b"logbuffer"),
            "the message names the file"
        );

        // And a DATA frame now reaches the image: the high-water mark is what
        // says so.
        let data = DataFrame {
            term_offset: 0,
            session_id,
            stream_id: 1001,
            term_id: 1_000,
            reserved_value: 0,
        };
        let payload_bytes = b"the reference's bytes";
        let mut data_frame = vec![0u8; 32 + payload_bytes.len()];

        assert!(
            data.write_with_flags(&mut data_frame, crate::protocol::header_flags::UNFRAGMENTED)
                .is_some()
        );
        data_frame[32..].copy_from_slice(payload_bytes);

        let _ = publisher.send_batch(
            Some(format!("127.0.0.1:{port}").parse().expect("an address")),
            &[&data_frame],
        );

        let mut received_status = false;
        let mut buffers = vec![vec![0u8; 2048]];
        let mut datagrams = Datagrams::new();

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while std::time::Instant::now() < deadline {
            conductor.do_work();

            let received = publisher
                .receive_batch(&mut buffers, &mut datagrams)
                .unwrap_or(0);

            for (slot, datagram) in datagrams.as_slice()[..received].iter().enumerate() {
                let packet = &buffers[slot][..datagram.length];

                if let Some(header) = FrameHeader::read(packet) {
                    if header.frame_type == crate::protocol::frame_type::SM {
                        received_status = true;
                    }
                }
            }

            if received_status {
                break;
            }

            std::thread::sleep(std::time::Duration::from_millis(5));
        }

        assert!(
            received_status,
            "the subscriber answers with a status message: without one the \
             publisher stops after one window"
        );
    }

    /// An image that has finished its life is released, and every subscription
    /// reading it is told (`ON_UNAVAILABLE_IMAGE`) — the receiving side of the
    /// cleanup A9 asks about.
    #[test]
    fn an_image_that_is_done_is_released_and_its_readers_are_told() {
        use crate::protocol::SetupFrame;
        use crate::sys::AddressFamily;
        use crate::sys::socket::DatagramSocket;

        let temp = TempDir::new();
        let config = publication_config(&temp.0);
        let cnc = create(&temp.0);
        let mut conductor = Conductor::new(cnc, &config).expect("conductor");
        let (cnc, mut receiver) = events_reader(&temp.0);
        let mut pending = Vec::new();

        let port = {
            let probe = DatagramSocket::open(AddressFamily::Inet).expect("a socket");
            probe
                .bind("127.0.0.1:0".parse().expect("an address"))
                .expect("a bind");
            probe.local_address().expect("an address").port()
        };

        let channel = format!("aeron:udp?endpoint=127.0.0.1:{port}");
        send(
            &conductor,
            ADD_SUBSCRIPTION_TYPE_ID,
            &add_subscription_payload(7, 11, 1001, &channel),
        );
        let _ = await_event(
            &mut conductor,
            &cnc,
            &mut receiver,
            &mut pending,
            ON_SUBSCRIPTION_READY_TYPE_ID,
        );

        let setup = SetupFrame {
            term_offset: 0,
            session_id: 99,
            stream_id: 1001,
            initial_term_id: 1_000,
            active_term_id: 1_000,
            term_length: 64 * 1024,
            mtu: 1408,
            ttl: 0,
        };
        let mut frame = [0u8; SetupFrame::LENGTH];
        assert!(setup.write_with_flags(&mut frame, 0).is_some());

        let publisher = DatagramSocket::open(AddressFamily::Inet).expect("a socket");
        publisher
            .bind("127.0.0.1:0".parse().expect("an address"))
            .expect("a bind");

        for _ in 0..3 {
            conductor.do_work();
        }

        publisher
            .send_batch(
                Some(format!("127.0.0.1:{port}").parse().expect("an address")),
                &[&frame],
            )
            .expect("a send");

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);

        while std::time::Instant::now() < deadline {
            conductor.do_work();

            if !conductor.publication_images().is_empty() {
                break;
            }

            std::thread::sleep(std::time::Duration::from_millis(5));
        }

        let image = conductor.publication_images()[0].clone();
        assert_eq!(1, image.refcount);

        // The reader is told the log buffer is there before it is told it is
        // gone: two messages, in that order.
        let _ = await_event(
            &mut conductor,
            &cnc,
            &mut receiver,
            &mut pending,
            ON_AVAILABLE_IMAGE_TYPE_ID,
        );

        // The image has finished (the receiver said so) and the conductor
        // releases it.
        assert_eq!(1, conductor.release_image(image.registration_id));

        let payload = await_event(
            &mut conductor,
            &cnc,
            &mut receiver,
            &mut pending,
            ON_UNAVAILABLE_IMAGE_TYPE_ID,
        );
        assert_eq!(image.registration_id.to_le_bytes(), payload[0..8]);
        assert_eq!(11i64.to_le_bytes(), payload[8..16], "the reader is named");
        assert_eq!(1001i32.to_le_bytes(), payload[16..20], "and the stream");

        assert!(
            conductor.publication_images().is_empty(),
            "and the image is gone from the driver"
        );
    }

    /// A12: the session id a network publication runs under.
    ///
    /// Two publications on one channel and different streams get *different*
    /// sessions — the whole point of speculating rather than picking one — and
    /// a URI that names a session gets exactly that one. Both are what the
    /// reference does with the same `SessionIds` the IPC path uses
    /// (`aeron_driver_conductor.c:4455-4478`).
    #[test]
    fn network_publications_speculate_a_session_each_and_honour_a_named_one() {
        let temp = TempDir::new();
        let config = publication_config(&temp.0);
        let cnc = create(&temp.0);
        let mut conductor = Conductor::new(cnc, &config).expect("conductor");
        let (cnc, mut receiver) = events_reader(&temp.0);
        let mut pending = Vec::new();

        let channel = format!("aeron:udp?endpoint=127.0.0.1:{}", free_test_port());

        for (correlation_id, stream_id) in [(21i64, 1001i32), (22, 1002)] {
            send(
                &conductor,
                ADD_PUBLICATION_TYPE_ID,
                &add_publication_payload(7, correlation_id, stream_id, &channel),
            );
            let _ = await_event(
                &mut conductor,
                &cnc,
                &mut receiver,
                &mut pending,
                ON_PUBLICATION_READY_TYPE_ID,
            );
        }

        let sessions: Vec<i32> = conductor
            .network_publications()
            .iter()
            .map(|publication| publication.session_id)
            .collect();

        assert_eq!(2, sessions.len());
        assert_ne!(
            sessions[0], sessions[1],
            "two streams on one channel are two sessions"
        );

        // And one that names its session keeps it.
        send(
            &conductor,
            ADD_PUBLICATION_TYPE_ID,
            &add_publication_payload(7, 23, 1003, &format!("{channel}|session-id=77")),
        );
        let _ = await_event(
            &mut conductor,
            &cnc,
            &mut receiver,
            &mut pending,
            ON_PUBLICATION_READY_TYPE_ID,
        );

        let named = conductor
            .network_publications()
            .iter()
            .find(|publication| publication.registration_id == 23)
            .expect("the third publication");

        assert_eq!(
            77, named.session_id,
            "a session the URI named is the one used"
        );
    }

    /// A port nobody is listening on, for a test that only needs the shape of
    /// a channel.
    fn free_test_port() -> u16 {
        let socket = crate::sys::socket::DatagramSocket::open(crate::sys::AddressFamily::Inet)
            .expect("a socket");
        socket
            .bind("127.0.0.1:0".parse().expect("an address"))
            .expect("a bind");

        socket.local_address().expect("an address").port()
    }

    /// Removing a UDP publication stops the sender and gives its six counters
    /// back — the half of the removal path that was missing, and whose absence
    /// would have been silent: the client is answered, and the sender keeps
    /// sending.
    #[test]
    fn removing_a_udp_publication_stops_the_sender_and_frees_its_counters() {
        let temp = TempDir::new();
        let config = publication_config(&temp.0);
        let cnc = create(&temp.0);
        let mut conductor = Conductor::new(cnc, &config).expect("conductor");
        let (cnc, mut receiver) = events_reader(&temp.0);
        let mut pending = Vec::new();

        let channel = format!("aeron:udp?endpoint=127.0.0.1:{}", free_test_port());

        send(
            &conductor,
            ADD_PUBLICATION_TYPE_ID,
            &add_publication_payload(7, 31, 1001, &channel),
        );
        let _ = await_event(
            &mut conductor,
            &cnc,
            &mut receiver,
            &mut pending,
            ON_PUBLICATION_READY_TYPE_ID,
        );

        assert_eq!(1, conductor.network_publications().len());

        let counters_before = conductor.counters().free_list_len();

        send(
            &conductor,
            deepmsg_cnc::command::REMOVE_PUBLICATION_TYPE_ID,
            &remove_publication_payload(7, 32, 31, 0, false),
        );
        let payload = await_event(
            &mut conductor,
            &cnc,
            &mut receiver,
            &mut pending,
            deepmsg_cnc::command::ON_OPERATION_SUCCEEDED_TYPE_ID,
        );
        assert_eq!(32i64.to_le_bytes(), payload[0..8]);

        assert!(
            conductor.network_publications().is_empty(),
            "the publication is gone from the driver"
        );
        assert_eq!(
            counters_before + 6,
            conductor.counters().free_list_len(),
            "and its six counters came back"
        );
    }

    /// A7: two concurrent publications on one channel and stream are one
    /// publication — the same log buffer, the same counters, the same sender —
    /// and an explicit parameter that disagrees is a refusal rather than a
    /// second publication (`aeron_confirm_publication_match`,
    /// `aeron-driver/src/main/c/aeron_driver_conductor.c:1105-1178`).
    #[test]
    fn two_udp_publications_on_one_stream_share_and_a_named_mtu_must_agree() {
        let temp = TempDir::new();
        let config = publication_config(&temp.0);
        let cnc = create(&temp.0);
        let mut conductor = Conductor::new(cnc, &config).expect("conductor");
        let (cnc, mut receiver) = events_reader(&temp.0);
        let mut pending = Vec::new();

        let channel = format!("aeron:udp?endpoint=127.0.0.1:{}", free_test_port());

        send(
            &conductor,
            ADD_PUBLICATION_TYPE_ID,
            &add_publication_payload(7, 41, 1001, &channel),
        );
        let first = await_event(
            &mut conductor,
            &cnc,
            &mut receiver,
            &mut pending,
            ON_PUBLICATION_READY_TYPE_ID,
        );
        let first_registration_id =
            i64::from_le_bytes(first[8..16].try_into().expect("eight bytes"));

        // The same channel and stream again: the reply names the *first*
        // publication, because that is the one the client will write through.
        send(
            &conductor,
            ADD_PUBLICATION_TYPE_ID,
            &add_publication_payload(7, 42, 1001, &channel),
        );
        let second = await_event(
            &mut conductor,
            &cnc,
            &mut receiver,
            &mut pending,
            ON_PUBLICATION_READY_TYPE_ID,
        );
        assert_eq!(
            first_registration_id.to_le_bytes(),
            second[8..16],
            "a shared publication is the one that already exists"
        );
        assert_eq!(
            1,
            conductor.network_publications().len(),
            "one publication, one log buffer"
        );

        // A third that names an mtu the first did not: refused rather than
        // shared, and with the generic code the reference's `EINVAL` composes
        // to.
        send(
            &conductor,
            ADD_PUBLICATION_TYPE_ID,
            &add_publication_payload(7, 43, 1001, &format!("{channel}|mtu=1024")),
        );
        let error = await_event(
            &mut conductor,
            &cnc,
            &mut receiver,
            &mut pending,
            ON_ERROR_TYPE_ID,
        );
        assert_eq!(43i64.to_le_bytes(), error[0..8], "the failing command");
        assert_eq!(
            deepmsg_cnc::command::ERROR_CODE_GENERIC_ERROR.to_le_bytes(),
            error[8..12]
        );
        assert_eq!(1, conductor.network_publications().len());
    }

    #[test]
    fn a_channel_this_driver_cannot_serve_is_refused_with_a_code_and_an_answer() {
        let temp = TempDir::new();
        let config = publication_config(&temp.0);
        let cnc = create(&temp.0);
        let mut conductor = Conductor::new(cnc, &config).expect("conductor");
        let (cnc, mut receiver) = events_reader(&temp.0);
        let mut pending = Vec::new();

        // A channel that is not a channel: the reference raises
        // `-AERON_ERROR_CODE_INVALID_CHANNEL`, which reaches the client as the
        // positive code.
        send(
            &conductor,
            ADD_PUBLICATION_TYPE_ID,
            &add_publication_payload(7, 9, 1001, "aeron:tcp?endpoint=localhost:1"),
        );
        let payload = await_event(
            &mut conductor,
            &cnc,
            &mut receiver,
            &mut pending,
            ON_ERROR_TYPE_ID,
        );

        assert_eq!(9i64.to_le_bytes(), payload[0..8], "the failing command");
        assert_eq!(
            ERROR_CODE_INVALID_CHANNEL.to_le_bytes(),
            payload[8..12],
            "the reference's code for a channel it cannot read"
        );
        assert_eq!(1, conductor.publication_failures());

        // A channel the reference serves and this build does not — one that
        // asked for transport-level timestamps — is refused rather than left
        // waiting, and with the code the protocol has for exactly that. (A
        // unicast UDP channel used to be this test's example, then a multicast
        // group; P1-4 and G3-1 made both channels this driver *does* serve.)
        send(
            &conductor,
            ADD_PUBLICATION_TYPE_ID,
            &add_publication_payload(
                7,
                10,
                1001,
                "aeron:udp?endpoint=127.0.0.1:40123|media-rcv-ts-offset=0",
            ),
        );
        let payload = await_event(
            &mut conductor,
            &cnc,
            &mut receiver,
            &mut pending,
            ON_ERROR_TYPE_ID,
        );

        assert_eq!(10i64.to_le_bytes(), payload[0..8]);
        assert_eq!(ERROR_CODE_NOT_SUPPORTED.to_le_bytes(), payload[8..12]);
        assert_eq!(2, conductor.publication_failures());

        // A URI the client could not have written but a hostile one could: the
        // payload is short of the channel it claims.
        send(&conductor, ADD_PUBLICATION_TYPE_ID, &[0u8; 16]);
        conductor.do_work();
        assert_eq!(
            1,
            conductor.malformed_commands(),
            "and it is counted as malformed rather than as a failed publication"
        );
        assert_eq!(0, conductor.publications().publications().len());
    }

    #[test]
    fn a_parameter_value_the_reference_cannot_parse_is_generic() {
        // The URI's *structure* reads — the scheme, the transport, the shape
        // of the parameter — so the reference gets as far as parsing the
        // value, where its reader returns a bare `-1` and the conductor's
        // error composition turns that into the generic code
        // (`aeron_driver_conductor.c:2326-2341`). A driver that answered the
        // invalid-channel code here would move the goalposts for a client
        // that branches on the two codes differently.
        let temp = TempDir::new();
        let config = publication_config(&temp.0);
        let cnc = create(&temp.0);
        let mut conductor = Conductor::new(cnc, &config).expect("conductor");
        let (cnc, mut receiver) = events_reader(&temp.0);
        let mut pending = Vec::new();

        send(
            &conductor,
            ADD_PUBLICATION_TYPE_ID,
            &add_publication_payload(7, 9, 1001, "aeron:ipc?term-length=abc"),
        );
        let payload = await_event(
            &mut conductor,
            &cnc,
            &mut receiver,
            &mut pending,
            ON_ERROR_TYPE_ID,
        );

        assert_eq!(9i64.to_le_bytes(), payload[0..8], "the failing command");
        assert_eq!(
            ERROR_CODE_GENERIC_ERROR.to_le_bytes(),
            payload[8..12],
            "the code the reference's composition gives a value it cannot parse"
        );
        assert_eq!(1, conductor.publication_failures());
    }

    #[test]
    fn a_subscription_gets_an_image_and_the_producer_gets_a_window() {
        let temp = TempDir::new();
        let config = publication_config(&temp.0);
        let cnc = create(&temp.0);
        let mut conductor = Conductor::new(cnc, &config).expect("conductor");
        let (cnc, mut receiver) = events_reader(&temp.0);
        let mut pending = Vec::new();

        // A publication, created by the client's own encoder and answered with
        // the bytes the client decodes.
        send(
            &conductor,
            ADD_PUBLICATION_TYPE_ID,
            &add_publication_payload(7, 42, 1001, "aeron:ipc"),
        );
        let ready = await_event(
            &mut conductor,
            &cnc,
            &mut receiver,
            &mut pending,
            ON_PUBLICATION_READY_TYPE_ID,
        );
        let path = String::from_utf8(ready[36..].to_vec()).expect("a path");
        let session_id = i32::from_le_bytes(ready[16..20].try_into().expect("four"));
        let limit_counter_id = i32::from_le_bytes(ready[24..28].try_into().expect("four"));

        // The producer's end of the log, mapped the way a client maps it.
        let producer = deepmsg_client::publication::Publication::open(
            std::path::Path::new(&path),
            42,
            session_id,
            1001,
            limit_counter_id,
            -1,
        )
        .expect("the log the driver named");

        // With no reader there is no window at all, which is what a producer
        // with nobody listening should find.
        assert_eq!(
            Some(false),
            producer.is_connected(),
            "the driver has not connected anything yet"
        );
        assert_eq!(
            0,
            conductor
                .counters()
                .value(&counter_regions(&conductor), limit_counter_id)
                .expect("the limit")
        );

        // A subscription on the same stream.
        send(
            &conductor,
            ADD_SUBSCRIPTION_TYPE_ID,
            &add_subscription_payload(7, 9, 1001, "aeron:ipc"),
        );
        let subscription_ready = await_event(
            &mut conductor,
            &cnc,
            &mut receiver,
            &mut pending,
            ON_SUBSCRIPTION_READY_TYPE_ID,
        );
        assert_eq!(9i64.to_le_bytes(), subscription_ready[0..8]);
        assert_eq!(
            (-1i32).to_le_bytes(),
            subscription_ready[8..12],
            "an IPC subscription has no status counter"
        );

        // And its image, which names the *publication* first and the
        // subscription second.
        let image_ready = await_event(
            &mut conductor,
            &cnc,
            &mut receiver,
            &mut pending,
            ON_AVAILABLE_IMAGE_TYPE_ID,
        );
        let deepmsg_cnc::command::Response::AvailableImage {
            publication_registration_id,
            session_id: image_session_id,
            stream_id,
            subscriber_registration_id,
            subscriber_position_id,
            log_file,
            source_identity,
        } = deepmsg_cnc::command::decode_response(ON_AVAILABLE_IMAGE_TYPE_ID, &image_ready)
        else {
            panic!("an image");
        };

        assert_eq!(42, publication_registration_id, "the publication");
        assert_eq!(session_id, image_session_id);
        assert_eq!(1001, stream_id);
        assert_eq!(9, subscriber_registration_id, "the subscription");
        assert_eq!(path.as_bytes(), log_file);
        assert_eq!(b"aeron:ipc", source_identity, "the constant, not the URI");

        // The pass that follows is what gives the producer its window: the
        // limit becomes the join position plus one term window, and the log's
        // `is_connected` byte is already one.
        conductor.do_work();
        let regions = counter_regions(&conductor);
        let join_position = conductor
            .counters()
            .value(&regions, subscriber_position_id)
            .expect("the reader's position");
        assert_eq!(
            0, join_position,
            "a reader joining a publication nobody has written to joins at the start"
        );
        let limit = conductor
            .counters()
            .value(&regions, limit_counter_id)
            .expect("the limit");
        assert_eq!(
            32 * 1024,
            limit,
            "half a term of 64 KiB, from the reader's position"
        );
        assert_eq!(Some(true), producer.is_connected());

        // The producer writes, the driver reads the tail on its next pass, and
        // a reader sees the frame.
        let payload = b"hello driver";
        let deepmsg_core::logbuffer::append::Appended::Ok { .. } = producer.offer(limit, payload)
        else {
            panic!("the window allows one small frame");
        };
        conductor.do_work();
        assert_eq!(
            Some(64),
            producer_position(&conductor, 42),
            "32 bytes of header and 11 of payload, on a 32-byte frame grid"
        );

        // And the reader's end, mapped from the same reply.
        let mut image = deepmsg_client::image::Image::open(
            std::path::Path::new(&path),
            42,
            session_id,
            1001,
            subscriber_position_id,
            join_position,
        )
        .expect("the same log, read-only");

        let mut seen = Vec::new();
        image.poll(10, |fragment| {
            let mut out = vec![0u8; fragment.payload_length()];
            fragment.copy_payload(&mut out);
            seen.push(out);
        });

        assert_eq!(vec![payload.to_vec()], seen);
    }

    /// The producer's position, as the driver's `pub-pos` counter holds it.
    fn producer_position(conductor: &Conductor, registration_id: i64) -> Option<i64> {
        let regions = counter_regions(conductor);
        let reader = regions.reader();
        let counter = reader.find_by_type_id(crate::position::type_id::PUBLISHER_POSITION)?;

        assert_eq!(registration_id, counter.registration_id);

        conductor.counters().value(&regions, counter.counter_id)
    }

    #[test]
    fn a_subscription_that_arrives_first_is_told_when_the_publication_appears() {
        let temp = TempDir::new();
        let config = publication_config(&temp.0);
        let cnc = create(&temp.0);
        let mut conductor = Conductor::new(cnc, &config).expect("conductor");
        let (cnc, mut receiver) = events_reader(&temp.0);
        let mut pending = Vec::new();

        // Subscribing to a stream nobody publishes is legal, and the client is
        // answered as soon as the subscription exists.
        send(
            &conductor,
            ADD_SUBSCRIPTION_TYPE_ID,
            &add_subscription_payload(7, 9, 1001, "aeron:ipc"),
        );
        await_event(
            &mut conductor,
            &cnc,
            &mut receiver,
            &mut pending,
            ON_SUBSCRIPTION_READY_TYPE_ID,
        );
        assert!(
            drain(&cnc, &mut receiver).is_empty(),
            "no image without a publication"
        );

        // The publication arrives, and its reply is followed by the image the
        // subscription was waiting for.
        send(
            &conductor,
            ADD_PUBLICATION_TYPE_ID,
            &add_publication_payload(7, 42, 1001, "aeron:ipc"),
        );
        await_event(
            &mut conductor,
            &cnc,
            &mut receiver,
            &mut pending,
            ON_PUBLICATION_READY_TYPE_ID,
        );
        let image_ready = await_event(
            &mut conductor,
            &cnc,
            &mut receiver,
            &mut pending,
            ON_AVAILABLE_IMAGE_TYPE_ID,
        );

        assert_eq!(42i64.to_le_bytes(), image_ready[0..8], "the publication");
        assert_eq!(9i64.to_le_bytes(), image_ready[16..24], "the subscription");

        // And once more, from the other side: a second publication on another
        // stream is nobody's, so it produces no image.
        send(
            &conductor,
            ADD_PUBLICATION_TYPE_ID,
            &add_publication_payload(7, 43, 2002, "aeron:ipc"),
        );
        await_event(
            &mut conductor,
            &cnc,
            &mut receiver,
            &mut pending,
            ON_PUBLICATION_READY_TYPE_ID,
        );
        assert!(
            drain(&cnc, &mut receiver).is_empty(),
            "another stream is another stream"
        );
    }
    /// A conductor with one publication (client 7, correlation 42) and one
    /// subscription (client 7, correlation 9) on stream 1001, both served.
    #[allow(clippy::type_complexity)] // the whole fixture, returned whole
    fn publishing_and_subscribed() -> (
        TempDir,
        Conductor,
        CncFile,
        ToClientsReceiver,
        Vec<(i32, Vec<u8>)>,
    ) {
        let temp = TempDir::new();
        let config = publication_config(&temp.0);
        let cnc = create(&temp.0);
        let mut conductor = Conductor::new(cnc, &config).expect("conductor");
        let (cnc, mut receiver) = events_reader(&temp.0);
        let mut pending = Vec::new();

        send(
            &conductor,
            ADD_PUBLICATION_TYPE_ID,
            &add_publication_payload(7, 42, 1001, "aeron:ipc"),
        );
        await_event(
            &mut conductor,
            &cnc,
            &mut receiver,
            &mut pending,
            ON_PUBLICATION_READY_TYPE_ID,
        );

        send(
            &conductor,
            ADD_SUBSCRIPTION_TYPE_ID,
            &add_subscription_payload(7, 9, 1001, "aeron:ipc"),
        );
        await_event(
            &mut conductor,
            &cnc,
            &mut receiver,
            &mut pending,
            ON_SUBSCRIPTION_READY_TYPE_ID,
        );
        await_event(
            &mut conductor,
            &cnc,
            &mut receiver,
            &mut pending,
            ON_AVAILABLE_IMAGE_TYPE_ID,
        );

        (temp, conductor, cnc, receiver, pending)
    }

    #[test]
    fn removing_a_publication_ends_its_stream_and_then_removes_it() {
        let (temp, mut conductor, cnc, mut receiver, _pending) = publishing_and_subscribed();

        // One pass for the limit, so there is a window to take back.
        conductor.do_work();
        let limit_counter_id = conductor.publications().publications()[0].pub_lmt_counter_id;
        let pos_counter_id = conductor.publications().publications()[0].pub_pos_counter_id;
        let subscriber_position_id =
            conductor.subscriptions().links()[0].subscribables[0].counter_id;
        assert_eq!(Some(32 * 1024), counter_value(&conductor, limit_counter_id));

        // The client lets go.
        send(
            &conductor,
            deepmsg_cnc::command::REMOVE_PUBLICATION_TYPE_ID,
            &remove_publication_payload(7, 50, 42, 0, true),
        );
        conductor.do_work();

        let (type_id, payload) = drain(&cnc, &mut receiver)
            .into_iter()
            .next()
            .expect("an answer");
        assert_eq!(
            deepmsg_cnc::command::ON_OPERATION_SUCCEEDED_TYPE_ID,
            type_id
        );
        assert_eq!(50i64.to_le_bytes(), payload[0..8]);

        // The link is gone, and the publication is on its way out: its limit
        // has been pulled back to where the producer got to and its log says
        // the stream ended there.
        assert!(
            conductor
                .clients()
                .find(7)
                .expect("the client")
                .publication_links
                .is_empty()
        );
        assert_eq!(
            Some(0),
            counter_value(&conductor, limit_counter_id),
            "the limit is the producer's position, which is zero"
        );
        assert_eq!(
            Some(0),
            conductor.publications().publications()[0].end_of_stream_position(),
            "and the log says so"
        );

        // The reader catches up — there was nothing to read, so it already has
        // — and the publication drains, lingers and goes. Two tiers: one to
        // drain, one to be done.
        set_counter(&conductor, subscriber_position_id, 0);
        for _ in 0..2 {
            conductor.timeout_check_deadline_ns = 0;
            conductor.do_work();
        }

        // And the reader was told, at the moment the publication finished
        // draining: it holds a mapping of a log buffer that is about to be
        // deleted (`aeron_ipc_publication.c:561-577`). The channel in that
        // message is the *constant*, not the one the client subscribed with.
        let events = drain(&cnc, &mut receiver);
        let unavailable = events
            .iter()
            .find(|(type_id, _)| *type_id == ON_UNAVAILABLE_IMAGE_TYPE_ID)
            .expect("the reader has to hear the stream is over");
        assert_eq!(42i64.to_le_bytes(), unavailable.1[0..8], "the publication");
        assert_eq!(9i64.to_le_bytes(), unavailable.1[8..16], "the subscription");
        assert_eq!(b"aeron:ipc", &unavailable.1[24..]);

        assert!(conductor.publications().publications().is_empty());

        // The log buffer goes with it, on the agent thread.
        await_removed(
            &mut conductor,
            &temp.0.join("publications").join("42.logbuffer"),
        );

        // And the counters: the two the publication owned and the reader's.
        let observer = cnc.counters().expect("the counter regions");
        assert!(
            observer
                .find_by_type_id(crate::position::type_id::PUBLISHER_POSITION)
                .is_none(),
            "pub-pos is reclaimed"
        );
        assert!(
            observer
                .find_by_type_id(crate::position::type_id::SUBSCRIPTION_POSITION)
                .is_none(),
            "and the reader's position"
        );
        // `pub-pos`'s *value* is still readable — the values region is indexed
        // by counter id and a reclaimed slot keeps what it held until something
        // else takes the id (`CounterManager::value` is the reference's address
        // arithmetic). What says it is gone is the state, and the region reader
        // is what skips a reclaimed counter.
        assert!(
            counter_value(&conductor, pos_counter_id).is_some(),
            "a reclaimed counter's slot is still there"
        );
    }

    #[test]
    fn removing_a_subscription_detaches_its_reader_and_frees_its_counter() {
        let (_temp, mut conductor, cnc, mut receiver, _pending) = publishing_and_subscribed();
        send(
            &conductor,
            deepmsg_cnc::command::REMOVE_SUBSCRIPTION_TYPE_ID,
            &remove_subscription_payload(7, 51, 9),
        );
        conductor.do_work();

        // The removal is silent in the reference, in C and in Java alike:
        // no image is announced as gone, and the acknowledgement — which the
        // reference sends only after the unlinking — is the whole event
        // stream (`aeron_driver_conductor.c:5199-5267`).
        let events = drain(&cnc, &mut receiver);
        assert_eq!(1, events.len(), "the acknowledgement, and nothing else");
        assert_eq!(
            deepmsg_cnc::command::ON_OPERATION_SUCCEEDED_TYPE_ID,
            events[0].0
        );
        assert_eq!(51i64.to_le_bytes(), events[0].1[0..8]);

        assert!(conductor.subscriptions().links().is_empty());
        assert_eq!(
            0,
            conductor.publications().publications()[0].subscribers.len(),
            "the publication has no readers again"
        );
        assert!(
            conductor.publications().publications()[0]
                .is_drained(conductor.counters(), &counter_regions(&conductor))
        );
        assert!(
            cnc.counters()
                .expect("the counter regions")
                .find_by_type_id(crate::position::type_id::SUBSCRIPTION_POSITION)
                .is_none(),
            "and the reader's counter is reclaimed"
        );
    }

    #[test]
    fn an_unknown_removal_is_answered_with_the_references_code() {
        let (_temp, mut conductor, cnc, mut receiver, _pending) = publishing_and_subscribed();

        // A publication this client does not hold, and a subscription nobody
        // made.
        send(
            &conductor,
            deepmsg_cnc::command::REMOVE_PUBLICATION_TYPE_ID,
            &remove_publication_payload(7, 60, 99, 0, true),
        );
        conductor.do_work();
        send(
            &conductor,
            deepmsg_cnc::command::REMOVE_SUBSCRIPTION_TYPE_ID,
            &remove_subscription_payload(7, 61, 99),
        );
        conductor.do_work();

        let errors = drain(&cnc, &mut receiver);
        assert_eq!(2, errors.len());
        assert_eq!(
            deepmsg_cnc::command::ERROR_CODE_UNKNOWN_PUBLICATION,
            i32::from_le_bytes(errors[0].1[8..12].try_into().expect("four bytes"))
        );
        assert_eq!(
            &b"unknown publication client_id=7 registration_id=99"[..],
            &errors[0].1[16..],
            "the reference's text names both ids (aeron_driver_conductor.c:4734)"
        );
        assert_eq!(
            deepmsg_cnc::command::ERROR_CODE_UNKNOWN_SUBSCRIPTION,
            i32::from_le_bytes(errors[1].1[8..12].try_into().expect("four bytes"))
        );
        assert_eq!(
            &b"unknown subscription client_id=7 registration_id=99"[..],
            &errors[1].1[16..],
            "and the subscription's text does the same (:5258)"
        );
        assert_eq!(1, conductor.publication_failures());
        assert_eq!(1, conductor.subscription_failures());

        // Answering with an `ON_ERROR` is also recording it: the reference's
        // `on_error` falls through to its own `log_error:` label, so the two
        // halves are one act (`aeron_driver_conductor.c:2366-2370`). Two
        // answers, two entries, two bumps of the errors counter — and the
        // entry holds the same words the client was handed, because the
        // reference hands `log_explicit_error` the message it just sent.
        let mut recorded = Vec::new();
        let log = conductor.cnc.error_log().expect("the error log");
        assert_eq!(2, log.read(i64::MIN, &mut recorded).entries);
        assert_eq!(
            "unknown publication client_id=7 registration_id=99",
            recorded[0].text
        );
        assert_eq!(
            "unknown subscription client_id=7 registration_id=99",
            recorded[1].text
        );
        assert_eq!(1, recorded[0].observation_count, "one sighting each");
        assert_eq!(1, recorded[1].observation_count);
        assert_eq!(
            Some(2),
            counter_value(&conductor, system_counters::id::ERRORS),
            "one bump per answer, not one per pass"
        );

        // The older 24-byte removal — a client built before the flags word —
        // still finds its link and is still answered.
        send(
            &conductor,
            deepmsg_cnc::command::REMOVE_PUBLICATION_TYPE_ID,
            &remove_publication_payload(7, 62, 42, 0, false),
        );
        conductor.do_work();

        let events = drain(&cnc, &mut receiver);
        assert_eq!(1, events.len());
        assert_eq!(
            deepmsg_cnc::command::ON_OPERATION_SUCCEEDED_TYPE_ID,
            events[0].0,
            "no flags means no revocation, not a refusal"
        );
        assert_eq!(
            Some(2),
            counter_value(&conductor, system_counters::id::ERRORS),
            "and an answer that is not an error raises nothing"
        );
    }

    #[test]
    fn the_one_error_the_reference_keeps_out_of_the_log_is_answered_but_not_recorded() {
        // `aeron_driver_conductor_on_error` skips its own `log_error:` label
        // for `RESOURCE_TEMPORARILY_UNAVAILABLE` — a transient condition is
        // answered but not recorded, and so raises no counter either
        // (`aeron_driver_conductor.c:2367-2370`). No command this build
        // serves reaches that code yet, so the decision is exercised where it
        // is made.
        let (_temp, mut conductor) = running(TerminationPolicy::Deny);

        conductor.pending_log_errors.push((
            ERROR_CODE_RESOURCE_TEMPORARILY_UNAVAILABLE,
            "the to-clients ring is busy".to_string(),
        ));
        conductor.do_work();

        let mut recorded = Vec::new();
        let log = conductor.cnc.error_log().expect("the error log");
        assert_eq!(0, log.read(i64::MIN, &mut recorded).entries);
        assert_eq!(
            Some(0),
            counter_value(&conductor, system_counters::id::ERRORS)
        );
    }

    #[test]
    fn a_revoked_publication_tells_its_readers_and_is_counted() {
        let (_temp, mut conductor, cnc, mut receiver, _pending) = publishing_and_subscribed();

        send(
            &conductor,
            deepmsg_cnc::command::REMOVE_PUBLICATION_TYPE_ID,
            &remove_publication_payload(
                7,
                52,
                42,
                deepmsg_cnc::command::REMOVE_PUBLICATION_FLAG_REVOKE,
                true,
            ),
        );
        conductor.do_work();
        drain(&cnc, &mut receiver);

        assert!(conductor.publications().publications()[0].is_revoked());

        // The revocation is acted on at the next timeout tier: the stream is
        // cut off where the producer got to, and the reader is told.
        conductor.timeout_check_deadline_ns = 0;
        conductor.do_work();

        let events = drain(&cnc, &mut receiver);
        assert_eq!(
            deepmsg_cnc::command::ON_UNAVAILABLE_IMAGE_TYPE_ID,
            events[0].0
        );
        assert_eq!(42i64.to_le_bytes(), events[0].1[0..8]);
        assert_eq!(
            b"aeron:ipc",
            &events[0].1[24..],
            "a revoke names the constant channel, not the subscription's"
        );
        assert_eq!(
            Some(1),
            counter_value(&conductor, crate::system_counters::id::PUBLICATIONS_REVOKED),
            "and the driver counts it"
        );
        assert_eq!(
            Some(0),
            conductor.publications().publications()[0].end_of_stream_position()
        );
    }

    /// `ADD_STATIC_COUNTER`'s wire form, built by the **client's** encoder so
    /// that the two directions of the protocol are checked against each other
    /// — the same reason `add_counter_payload` exists.
    fn add_static_counter_payload(
        client_id: i64,
        correlation_id: i64,
        registration_id: i64,
        type_id: i32,
        key: &[u8],
        label: &[u8],
    ) -> Vec<u8> {
        deepmsg_cnc::command::encode_add_static_counter(
            client_id,
            correlation_id,
            registration_id,
            type_id,
            key,
            label,
        )
    }

    /// The `counter_id` an `ON_STATIC_COUNTER` carried, and the heartbeat
    /// counter the driver announced for the client on the way past.
    fn static_counter_from(events: &[(i32, Vec<u8>)], correlation_id: i64) -> i32 {
        let payload = events
            .iter()
            .find(|(type_id, _)| *type_id == ON_STATIC_COUNTER_TYPE_ID)
            .map(|(_, payload)| payload)
            .expect("the driver answers with a static counter");

        assert_eq!(
            correlation_id.to_le_bytes(),
            payload[0..8],
            "on the correlation id of the request, with no correlated head"
        );

        i32::from_le_bytes(payload[8..12].try_into().expect("four bytes"))
    }

    #[test]
    fn a_static_counter_belongs_to_the_driver_and_not_to_the_client() {
        let (_temp, mut conductor, cnc, mut receiver, _pending) = publishing_and_subscribed();
        drain(&cnc, &mut receiver);

        send(
            &conductor,
            ADD_STATIC_COUNTER_TYPE_ID,
            &add_static_counter_payload(7, 50, 42, 99, b"stat", b"a static counter"),
        );
        conductor.do_work();

        let events = drain(&cnc, &mut receiver);
        let counter_id = static_counter_from(&events, 50);

        let regions = counter_regions(&conductor);
        let descriptor = regions
            .reader()
            .get(counter_id)
            .expect("the counter exists");

        assert_eq!(99, descriptor.type_id);
        assert_eq!(42, descriptor.registration_id, "the id it is found by");
        assert_eq!(
            NULL_VALUE, descriptor.owner_id,
            "which is the whole of what makes it static: an owner id of nothing"
        );
        assert_eq!("a static counter", descriptor.label);

        // And the client is told about it the *other* way. `ADD_COUNTER`
        // announces its counter with `ON_COUNTER_READY`, which is a message
        // about a counter the client now owns; this one is answered with
        // `ON_STATIC_COUNTER` and no announcement, which is the difference that
        // matters — there is nothing for this client to give back, and nothing
        // will be taken away from it.
        assert!(
            !events.iter().any(|(type_id, payload)| {
                *type_id == ON_COUNTER_READY_TYPE_ID
                    && i32::from_le_bytes(payload[8..12].try_into().expect("four bytes"))
                        == counter_id
            }),
            "a static counter is not announced as one of the client's"
        );
    }

    #[test]
    fn a_static_counter_outlives_the_client_that_asked_for_it() {
        // The trap this slice exists to avoid, and the reason it is a test of
        // its own: a static counter is not in the client's list of counters, so
        // the tier that reaps a dead client does not take it. Pushing a
        // `CounterLink` for one would look harmless and would free a counter
        // the driver is meant to keep.
        let temp = TempDir::new();
        let config = DriverConfig {
            aeron_dir: temp.0.clone(),
            ipc_term_buffer_length: 64 * 1024,
            client_liveness_timeout_ns: 100_000_000,
            timer_interval_ns: 10_000_000,
            ..DriverConfig::default()
        };
        let cnc = create(&temp.0);
        let mut conductor = Conductor::new(cnc, &config).expect("conductor");
        let (cnc, mut receiver) = events_reader(&temp.0);

        send(
            &conductor,
            ADD_STATIC_COUNTER_TYPE_ID,
            &add_static_counter_payload(7, 50, 42, 99, b"stat", b"a static counter"),
        );
        conductor.do_work();

        let events = drain(&cnc, &mut receiver);
        let counter_id = static_counter_from(&events, 50);

        // The client's heartbeat, which the driver announced on the way past:
        // its correlation id is the client id, which is the one id no command
        // can ever carry again.
        let heartbeat_counter_id = events
            .iter()
            .find(|(type_id, payload)| {
                *type_id == ON_COUNTER_READY_TYPE_ID && payload[0..8] == 7i64.to_le_bytes()
            })
            .map(|(_, payload)| i32::from_le_bytes(payload[8..12].try_into().expect("four bytes")))
            .expect("the client's heartbeat was announced");

        // Stop saying it is alive, and let the driver decide.
        set_counter(&conductor, heartbeat_counter_id, 0);

        for _ in 0..4 {
            conductor.timeout_check_deadline_ns = 0;
            conductor.do_work();
        }

        let events = drain(&cnc, &mut receiver);

        // A positive observation in the negative control: the client really was
        // reaped, so the counter's survival is not the survival of everything.
        assert!(
            events.iter().any(|(type_id, payload)| *type_id
                == deepmsg_cnc::command::ON_CLIENT_TIMEOUT_TYPE_ID
                && payload[0..8] == 7i64.to_le_bytes()),
            "the client timed out"
        );
        assert!(
            !events.iter().any(|(type_id, payload)| {
                *type_id == ON_UNAVAILABLE_COUNTER_TYPE_ID && payload[0..8] == 42i64.to_le_bytes()
            }),
            "and its static counter was not announced as going with it"
        );

        let regions = counter_regions(&conductor);
        let descriptor = regions
            .reader()
            .get(counter_id)
            .unwrap_or_else(|| panic!("counter {counter_id} is still the driver's"));

        assert_eq!(NULL_VALUE, descriptor.owner_id);
        assert_eq!(42, descriptor.registration_id);
    }

    #[test]
    fn a_static_counter_is_found_again_by_its_type_and_registration_id() {
        // How two processes agree on one counter: the second ask names the same
        // pair and is answered with what the first one made, rather than with a
        // second counter nobody would find.
        let (_temp, mut conductor, cnc, mut receiver, _pending) = publishing_and_subscribed();
        drain(&cnc, &mut receiver);

        let payload = add_static_counter_payload(7, 50, 42, 99, b"stat", b"a static counter");

        send(&conductor, ADD_STATIC_COUNTER_TYPE_ID, &payload);
        conductor.do_work();
        let first = static_counter_from(&drain(&cnc, &mut receiver), 50);

        send(&conductor, ADD_STATIC_COUNTER_TYPE_ID, &payload);
        conductor.do_work();
        let second = static_counter_from(&drain(&cnc, &mut receiver), 50);

        assert_eq!(first, second);
    }

    #[test]
    fn a_static_counter_may_not_take_a_live_counters_place() {
        // A pair that already names a counter **somebody owns** is refused
        // rather than handed over: a static counter has no owner, so taking a
        // live counter's id would leave two clients believing different things
        // about one slot (`aeron_driver_conductor.c:6270-6281`).
        let (_temp, mut conductor, cnc, mut receiver, _pending) = publishing_and_subscribed();
        drain(&cnc, &mut receiver);

        send(
            &conductor,
            deepmsg_cnc::command::ADD_COUNTER_TYPE_ID,
            &add_counter_payload(7, 42, 99, b"stat", b"a counter a client owns"),
        );
        conductor.do_work();
        drain(&cnc, &mut receiver);

        send(
            &conductor,
            ADD_STATIC_COUNTER_TYPE_ID,
            &add_static_counter_payload(7, 50, 42, 99, b"stat", b"a static counter"),
        );
        conductor.do_work();

        let events = drain(&cnc, &mut receiver);
        let payload = events
            .iter()
            .find(|(type_id, _)| *type_id == ON_ERROR_TYPE_ID)
            .map(|(_, payload)| payload.clone())
            .expect("the client is answered");

        assert_eq!(50i64.to_le_bytes(), payload[0..8]);
        assert_eq!(
            ERROR_CODE_GENERIC_ERROR,
            i32::from_le_bytes(payload[8..12].try_into().expect("four bytes"))
        );
        assert!(
            String::from_utf8_lossy(&payload[16..]).contains("cannot add static counter"),
            "the reference's words: {}",
            String::from_utf8_lossy(&payload[16..])
        );
    }

    #[test]
    fn a_channel_parameter_this_driver_cannot_serve_is_answered_rather_than_ignored() {
        // G1-4's whole point, at the client's end: `cc=` and `nak-delay=` used
        // to reach the parser's generic list and change nothing, so a client
        // that named one got a subscription behaving like a different one.
        let (_temp, mut conductor, cnc, mut receiver, _pending) = publishing_and_subscribed();
        drain(&cnc, &mut receiver);

        // A strategy this build does not carry: refused, by name, on the
        // correlation id that asked — where the reference's supplier fails with
        // no error set at all and the client is told nothing.
        let port = {
            use crate::sys::AddressFamily;
            use crate::sys::socket::DatagramSocket;

            let probe = DatagramSocket::open(AddressFamily::Inet).expect("a socket");
            probe
                .bind("127.0.0.1:0".parse().expect("an address"))
                .expect("a bind");
            probe.local_address().expect("an address").port()
        };

        send(
            &conductor,
            ADD_SUBSCRIPTION_TYPE_ID,
            &add_subscription_payload(
                7,
                11,
                1001,
                &format!("aeron:udp?endpoint=127.0.0.1:{port}|cc=cubic"),
            ),
        );
        conductor.do_work();

        let events = drain(&cnc, &mut receiver);
        let payload = events
            .iter()
            .find(|(type_id, _)| *type_id == ON_ERROR_TYPE_ID)
            .map(|(_, payload)| payload.clone())
            .expect("the client is answered rather than left waiting");

        assert_eq!(11i64.to_le_bytes(), payload[0..8]);
        assert!(
            String::from_utf8_lossy(&payload[16..]).contains("cc=cubic"),
            "and told which parameter: {}",
            String::from_utf8_lossy(&payload[16..])
        );
        assert!(
            !events
                .iter()
                .any(|(type_id, _)| *type_id == ON_SUBSCRIPTION_READY_TYPE_ID),
            "no subscription was created"
        );

        // And the half that *is* served: a named `nak-delay` is read, and the
        // subscription is created with it.
        send(
            &conductor,
            ADD_SUBSCRIPTION_TYPE_ID,
            &add_subscription_payload(
                7,
                12,
                1001,
                &format!("aeron:udp?endpoint=127.0.0.1:{port}|nak-delay=2ms"),
            ),
        );
        conductor.do_work();

        let events = drain(&cnc, &mut receiver);
        assert!(
            events
                .iter()
                .any(|(type_id, _)| *type_id == ON_SUBSCRIPTION_READY_TYPE_ID),
            "a channel this build can serve is served"
        );
    }

    /// Two subscriptions that one image would serve have to agree about
    /// `reliable`, and the reference refuses the second rather than serving it
    /// the first one's answer
    /// (`aeron_driver_conductor_has_clashing_subscription`, `:320-332`).
    #[test]
    fn two_subscriptions_that_disagree_about_reliability_cannot_share_a_channel() {
        let (_temp, mut conductor, cnc, mut receiver, _pending) = publishing_and_subscribed();
        drain(&cnc, &mut receiver);

        // A channel of this test's own: the fixtures are shared, and a
        // subscription on theirs would be a clash with theirs.
        let port = {
            use crate::sys::AddressFamily;
            use crate::sys::socket::DatagramSocket;

            let probe = DatagramSocket::open(AddressFamily::Inet).expect("a socket");
            probe
                .bind("127.0.0.1:0".parse().expect("an address"))
                .expect("a bind");
            probe.local_address().expect("an address").port()
        };
        let channel = format!("aeron:udp?endpoint=127.0.0.1:{port}");
        let unreliable = format!("{channel}|reliable=false");

        // The first one names it, and is served: nothing about the value itself
        // is refused.
        send(
            &conductor,
            ADD_SUBSCRIPTION_TYPE_ID,
            &add_subscription_payload(7, 21, 1002, &unreliable),
        );
        conductor.do_work();

        let events = drain(&cnc, &mut receiver);
        assert!(
            events
                .iter()
                .any(|(type_id, _)| *type_id == ON_SUBSCRIPTION_READY_TYPE_ID),
            "the first subscription is served"
        );

        // The second asks for the default on the same endpoint, stream and
        // session. The image they would share can only behave one way, so the
        // reference answers with an error rather than letting one client's
        // choice silently become the other's.
        send(
            &conductor,
            ADD_SUBSCRIPTION_TYPE_ID,
            &add_subscription_payload(7, 22, 1002, &channel),
        );
        conductor.do_work();

        let events = drain(&cnc, &mut receiver);
        let payload = events
            .iter()
            .find(|(type_id, _)| *type_id == ON_ERROR_TYPE_ID)
            .map(|(_, payload)| payload.clone())
            .expect("the client is answered rather than left waiting");

        assert_eq!(22i64.to_le_bytes(), payload[0..8], "on the id that asked");
        assert_eq!(
            22i32.to_le_bytes(),
            payload[8..12],
            "and under the reference's own code for it, which is the platform's \
             `EINVAL` and not one of `AERON_ERROR_CODE_*` (`:322-330`)"
        );

        let text = String::from_utf8_lossy(&payload[16..]).to_string();
        assert!(
            text.contains("option conflicts with existing subscription: reliable=true"),
            "the option and the value the caller gave: {text}"
        );
        assert!(
            text.contains(&format!("existingChannel={unreliable}")),
            "and the channel already there: {text}"
        );
        assert!(
            text.contains(&format!("channel={channel}")),
            "and the one that arrived: {text}"
        );
        assert!(
            !events
                .iter()
                .any(|(type_id, _)| *type_id == ON_SUBSCRIPTION_READY_TYPE_ID),
            "no second subscription was created"
        );

        // The control, so that the refusal cannot pass by refusing everything:
        // the same option twice is not a clash, whichever way round.
        send(
            &conductor,
            ADD_SUBSCRIPTION_TYPE_ID,
            &add_subscription_payload(7, 23, 1002, &unreliable),
        );
        conductor.do_work();

        let events = drain(&cnc, &mut receiver);
        assert!(
            events
                .iter()
                .any(|(type_id, _)| *type_id == ON_SUBSCRIPTION_READY_TYPE_ID),
            "a subscription that agrees about the option is served"
        );
    }

    /// `GET_NEXT_AVAILABLE_SESSION_ID`'s wire form: the correlated head and a
    /// stream id (`aeron_control_protocol.h:247-253`).
    fn next_session_id_payload(client_id: i64, correlation_id: i64, stream_id: i32) -> Vec<u8> {
        let command = deepmsg_cnc::command::GetNextAvailableSessionId {
            correlated: deepmsg_cnc::command::Correlated {
                client_id,
                correlation_id,
            },
            stream_id,
        };

        let mut out = vec![0u8; command.encoded_length()];
        assert!(command.encode_into(&mut out));

        out
    }

    /// Ask for a session id and read the answer off the ring.
    fn ask_for_a_session_id(
        conductor: &mut Conductor,
        cnc: &CncFile,
        receiver: &mut ToClientsReceiver,
        pending: &mut Vec<(i32, Vec<u8>)>,
        stream_id: i32,
    ) -> i32 {
        send(
            conductor,
            deepmsg_cnc::command::GET_NEXT_AVAILABLE_SESSION_ID_TYPE_ID,
            &next_session_id_payload(7, 8, stream_id),
        );

        let payload = await_event(
            conductor,
            cnc,
            receiver,
            pending,
            ON_NEXT_AVAILABLE_SESSION_ID_TYPE_ID,
        );

        i32::from_le_bytes(payload[8..12].try_into().expect("four bytes"))
    }

    #[test]
    fn the_session_id_answering_skips_one_a_publication_already_holds() {
        let (_temp, mut conductor, cnc, mut receiver, mut pending) = publishing_and_subscribed();
        drain(&cnc, &mut receiver);

        // The cursor is a random id the driver drew at startup, so a clash is
        // not something a test can wait for — it is something the test has to
        // *make*. What makes it possible is that the URI may name its own
        // session, so a publication can be put exactly where the cursor is.
        let cursor = conductor.publications().session_ids().cursor();

        send(
            &conductor,
            ADD_PUBLICATION_TYPE_ID,
            &add_publication_payload(7, 43, 1002, &format!("aeron:ipc?session-id={cursor}")),
        );
        await_event(
            &mut conductor,
            &cnc,
            &mut receiver,
            &mut pending,
            ON_PUBLICATION_READY_TYPE_ID,
        );

        assert_eq!(
            cursor,
            conductor
                .publications()
                .publications()
                .iter()
                .find(|publication| publication.registration_id == 43)
                .expect("the publication that named its session")
                .session_id
        );

        // Naming a session is not an allocation, so it must not have moved the
        // cursor onto it (`aeron_driver_conductor.c:1071-1075`, whose advance is
        // for speculated ids only) — otherwise the id below would be skipped by
        // accident and this test would prove nothing.
        assert_eq!(
            cursor,
            conductor.publications().session_ids().cursor(),
            "a URI that named its own session did not move the cursor"
        );

        // The clash, and it has to be asked **first**: every ask moves the
        // cursor, so an ask about anything else beforehand would leave this one
        // testing nothing — which is what an earlier version of this test did.
        send(
            &conductor,
            deepmsg_cnc::command::GET_NEXT_AVAILABLE_SESSION_ID_TYPE_ID,
            &next_session_id_payload(7, 8, 1002),
        );
        conductor.do_work();

        let events = drain(&cnc, &mut receiver);
        let payload = events
            .iter()
            .find(|(type_id, _)| *type_id == ON_NEXT_AVAILABLE_SESSION_ID_TYPE_ID)
            .map(|(_, payload)| payload.clone())
            .expect("the driver answers");

        assert_eq!(
            8i64.to_le_bytes(),
            payload[0..8],
            "on its own correlation id"
        );
        assert_eq!(
            cursor.wrapping_add(1),
            i32::from_le_bytes(payload[8..12].try_into().expect("four bytes")),
            "the id the publication holds is skipped, not answered with"
        );

        // The cursor moved past what was handed out, so the next ask does not
        // repeat the last answer — which is all the driver promises, and why
        // the reference advances *before* it checks for a clash
        // (`aeron_driver_conductor.c:6425-6426`).
        assert_eq!(
            cursor.wrapping_add(2),
            conductor.publications().session_ids().cursor()
        );

        // And somebody else's stream holds nothing, so there the cursor's own
        // id is free: a session id is only unique per stream.
        assert_eq!(
            cursor.wrapping_add(2),
            ask_for_a_session_id(&mut conductor, &cnc, &mut receiver, &mut pending, 9999),
            "another stream does not hold it"
        );
    }

    #[test]
    fn the_session_id_answering_skips_a_network_publication_too() {
        // The reference walks **both** lists, IPC first and then network
        // (`aeron_driver_conductor.c:6429-6444`), and the second walk is the one
        // easy to leave out: a driver that only checked its IPC publications
        // would hand a client a session id a network publication on that stream
        // already holds — and the client would then be refused by the create's
        // own clash check, having done exactly what it was told.
        use crate::sys::AddressFamily;
        use crate::sys::socket::DatagramSocket;

        let temp = TempDir::new();
        let config = publication_config(&temp.0);
        let cnc = create(&temp.0);
        let mut conductor = Conductor::new(cnc, &config).expect("conductor");
        let (cnc, mut receiver) = events_reader(&temp.0);
        let mut pending = Vec::new();

        let first = ask_for_a_session_id(&mut conductor, &cnc, &mut receiver, &mut pending, 1002);

        let socket = DatagramSocket::open(AddressFamily::Inet).expect("a socket");
        socket
            .bind("127.0.0.1:0".parse().expect("an address"))
            .expect("a bind");
        let endpoint = socket.local_address().expect("an address");

        send(
            &conductor,
            ADD_PUBLICATION_TYPE_ID,
            &add_publication_payload(
                7,
                42,
                1002,
                &format!(
                    "aeron:udp?endpoint={endpoint}|session-id={}",
                    first.wrapping_add(1)
                ),
            ),
        );
        await_event(
            &mut conductor,
            &cnc,
            &mut receiver,
            &mut pending,
            ON_PUBLICATION_READY_TYPE_ID,
        );

        let second = ask_for_a_session_id(&mut conductor, &cnc, &mut receiver, &mut pending, 1002);

        assert_eq!(
            first.wrapping_add(2),
            second,
            "the id the network publication holds is skipped as well"
        );
    }

    /// A `REJECT_IMAGE` payload (`aeron_control_protocol.h:220-227`): the
    /// correlated head, the target's registration id, where the rejecting
    /// client had read to, and the reason with its own length.
    ///
    /// The record's shape is checked before anything else, and a reason of four
    /// characters is the shortest one that makes the record
    /// `sizeof(aeron_reject_image_command_t)` — 40 bytes
    /// (`aeron_driver_conductor.c:3177-3180`).
    fn reject_image_payload(
        client_id: i64,
        correlation_id: i64,
        image_correlation_id: i64,
        position: i64,
        reason: &[u8],
    ) -> Vec<u8> {
        assert!(
            40 <= 36 + reason.len(),
            "a record shorter than the struct is malformed, not a short command"
        );

        let mut out = vec![0u8; 36 + reason.len()];
        out[0..8].copy_from_slice(&client_id.to_le_bytes());
        out[8..16].copy_from_slice(&correlation_id.to_le_bytes());
        out[16..24].copy_from_slice(&image_correlation_id.to_le_bytes());
        out[24..32].copy_from_slice(&position.to_le_bytes());
        out[32..36].copy_from_slice(
            &i32::try_from(reason.len())
                .expect("a reason far below i32::MAX")
                .to_le_bytes(),
        );
        out[36..].copy_from_slice(reason);

        out
    }

    #[test]
    fn a_rejected_ipc_publication_tells_its_publisher_and_takes_its_readers_away() {
        let (_temp, mut conductor, cnc, mut receiver, _pending) = publishing_and_subscribed();

        let reader_counter_id = conductor.subscriptions().links()[0].subscribables[0].counter_id;
        let free_before = conductor.counters().free_list_len();

        // The id a client would have been handed for an IPC image is the
        // publication's own registration id — the correlation id of its
        // `ADD_PUBLICATION` — which is why one command has two targets.
        send(
            &conductor,
            deepmsg_cnc::command::REJECT_IMAGE_TYPE_ID,
            &reject_image_payload(7, 8, 42, 0, b"Needs to be closed"),
        );
        conductor.do_work();

        let events = drain(&cnc, &mut receiver);

        let payload = events
            .iter()
            .find(|(type_id, _)| *type_id == ON_PUBLICATION_ERROR_TYPE_ID)
            .map(|(_, payload)| payload.clone())
            .expect("the publisher is told");

        assert_eq!(42i64.to_le_bytes(), payload[0..8], "the publication named");
        assert_eq!(
            NULL_VALUE.to_le_bytes(),
            payload[8..16],
            "an IPC publication answers no destination"
        );
        assert_eq!(
            conductor.publications().publications()[0].session_id,
            i32::from_le_bytes(payload[16..20].try_into().expect("four bytes"))
        );
        assert_eq!(
            1001,
            i32::from_le_bytes(payload[20..24].try_into().expect("four bytes"))
        );
        assert_eq!(
            NULL_VALUE.to_le_bytes(),
            payload[24..32],
            "…and hears from no receiver"
        );
        assert_eq!(NULL_VALUE.to_le_bytes(), payload[32..40], "nor a group");
        assert_eq!(
            1,
            i16::from_le_bytes(payload[40..42].try_into().expect("two bytes")),
            "the loopback source is sent as an ordinary IPv4 address"
        );
        assert_eq!(
            [1, 0, 0, 127],
            payload[44..48],
            "the reference's own bytes: `INADDR_LOOPBACK` written without an `htonl`"
        );
        assert_eq!(
            deepmsg_cnc::command::ERROR_CODE_IMAGE_REJECTED,
            i32::from_le_bytes(payload[60..64].try_into().expect("four bytes"))
        );
        assert_eq!(
            18,
            i32::from_le_bytes(payload[64..68].try_into().expect("four bytes"))
        );
        assert_eq!(
            b"Needs to be closed",
            &payload[68..],
            "the client's own words"
        );

        // The reader is told the image is gone and loses its position.
        let unavailable = events
            .iter()
            .find(|(type_id, _)| *type_id == ON_UNAVAILABLE_IMAGE_TYPE_ID)
            .expect("the reader is told");
        assert_eq!(42i64.to_le_bytes(), unavailable.1[0..8]);
        assert_eq!(9i64.to_le_bytes(), unavailable.1[8..16]);
        assert!(
            conductor.subscriptions().links()[0]
                .subscribables
                .is_empty(),
            "the subscription holds no reader for it any more"
        );
        assert_eq!(
            free_before + 1,
            conductor.counters().free_list_len(),
            "and the reader's counter came back"
        );
        assert!(
            counter_value(&conductor, reader_counter_id).is_some(),
            "the id is still a readable slot — `free` reclaims it, it does not erase it"
        );

        // Both halves of the answer, in the reference's order: the counter,
        // then the completion.
        assert_eq!(
            Some(1),
            counter_value(&conductor, crate::system_counters::id::IMAGES_REJECTED)
        );
        assert!(
            events
                .iter()
                .any(|(type_id, _)| *type_id == ON_OPERATION_SUCCEEDED_TYPE_ID)
        );
        assert_eq!(0, conductor.unhandled_commands());
    }

    #[test]
    fn a_reader_that_arrives_while_a_publication_is_rejected_is_not_linked() {
        let (_temp, mut conductor, cnc, mut receiver, _pending) = publishing_and_subscribed();

        send(
            &conductor,
            deepmsg_cnc::command::REJECT_IMAGE_TYPE_ID,
            &reject_image_payload(7, 8, 42, 0, b"Needs to be closed"),
        );
        conductor.do_work();
        drain(&cnc, &mut receiver);

        assert!(conductor.publications().publications()[0].is_in_cool_down());

        // A second reader, arriving inside the cool down. It is told its
        // subscription exists — that is a fact about the subscription — and it
        // is *not* given an image, which is what refusing readers means.
        send(
            &conductor,
            ADD_SUBSCRIPTION_TYPE_ID,
            &add_subscription_payload(7, 10, 1001, "aeron:ipc"),
        );

        for _ in 0..3 {
            conductor.do_work();
        }

        let events = drain(&cnc, &mut receiver);

        assert!(
            events
                .iter()
                .any(|(type_id, _)| *type_id == ON_SUBSCRIPTION_READY_TYPE_ID),
            "the subscription itself is fine"
        );
        assert!(
            !events
                .iter()
                .any(|(type_id, _)| *type_id == ON_AVAILABLE_IMAGE_TYPE_ID),
            "but nothing links it to a publication that is refusing readers"
        );
    }

    #[test]
    fn a_reject_image_that_names_nothing_is_answered_with_the_references_error() {
        let (_temp, mut conductor, cnc, mut receiver, _pending) = publishing_and_subscribed();

        drain(&cnc, &mut receiver);

        send(
            &conductor,
            deepmsg_cnc::command::REJECT_IMAGE_TYPE_ID,
            &reject_image_payload(7, 8, 12_345, 0, b"nowhere"),
        );
        conductor.do_work();

        let events = drain(&cnc, &mut receiver);

        let payload = events
            .iter()
            .find(|(type_id, _)| *type_id == ON_ERROR_TYPE_ID)
            .map(|(_, payload)| payload.clone())
            .expect("the client is answered");

        assert_eq!(
            8i64.to_le_bytes(),
            payload[0..8],
            "on its own correlation id"
        );
        assert_eq!(
            ERROR_CODE_GENERIC_ERROR,
            i32::from_le_bytes(payload[8..12].try_into().expect("four bytes"))
        );
        assert_eq!(
            b"Unable to resolve image for correlationId=12345",
            &payload[16..],
            "the reference's words (aeron_driver_conductor.c:6389-6392)"
        );

        assert_eq!(
            Some(0),
            counter_value(&conductor, crate::system_counters::id::IMAGES_REJECTED),
            "nothing was rejected"
        );
        assert!(
            !events
                .iter()
                .any(|(type_id, _)| *type_id == ON_OPERATION_SUCCEEDED_TYPE_ID),
            "and nothing succeeded"
        );
    }

    #[test]
    fn an_invalidation_reason_longer_than_the_reference_allows_is_answered_with_an_error() {
        let (_temp, mut conductor, cnc, mut receiver, _pending) = publishing_and_subscribed();

        drain(&cnc, &mut receiver);

        let reason = vec![b'x'; 1024];
        send(
            &conductor,
            deepmsg_cnc::command::REJECT_IMAGE_TYPE_ID,
            &reject_image_payload(7, 8, 42, 0, &reason),
        );
        conductor.do_work();

        let events = drain(&cnc, &mut receiver);

        let payload = events
            .iter()
            .find(|(type_id, _)| *type_id == ON_ERROR_TYPE_ID)
            .map(|(_, payload)| payload.clone())
            .expect("the client is answered");

        assert_eq!(8i64.to_le_bytes(), payload[0..8]);
        assert_eq!(
            ERROR_CODE_GENERIC_ERROR,
            i32::from_le_bytes(payload[8..12].try_into().expect("four bytes"))
        );
        assert_eq!(
            b"Invalidation reason_text must be 1023 bytes or less",
            &payload[16..],
            "the reference's words (aeron_driver_conductor.c:6357-6364)"
        );

        // The publication is untouched: the bound was refused before the
        // registration id in the command was ever looked up.
        assert!(!conductor.publications().publications()[0].is_in_cool_down());
        assert_eq!(
            Some(0),
            counter_value(&conductor, crate::system_counters::id::IMAGES_REJECTED)
        );
    }

    #[test]
    fn a_reject_image_shorter_than_its_own_struct_is_malformed() {
        let (_temp, mut conductor, cnc, mut receiver, _pending) = publishing_and_subscribed();

        drain(&cnc, &mut receiver);

        // 36 bytes is `offsetof(reason_text)` and not `sizeof` — the record the
        // reference's own Java client would send for an empty reason, and one
        // its dispatch refuses before any handler sees it
        // (`aeron_driver_conductor.c:3177-3180`).
        send(
            &conductor,
            deepmsg_cnc::command::REJECT_IMAGE_TYPE_ID,
            &[0u8; 36],
        );
        conductor.do_work();

        assert_eq!(1, conductor.malformed_commands());
        assert!(
            drain(&cnc, &mut receiver).is_empty(),
            "a malformed command is recorded, not answered"
        );
    }

    /// Read this test's own socket until an `ERR` frame turns up, or the
    /// deadline passes.
    ///
    /// An image sends its status messages on the same socket, so what arrives
    /// first is whatever it said before it was rejected; the frame is picked
    /// out by type rather than by being the next datagram.
    fn await_error_frame(
        socket: &crate::sys::socket::DatagramSocket,
        within: std::time::Duration,
    ) -> Option<Vec<u8>> {
        use crate::protocol::{FrameHeader, frame_type};
        use crate::sys::socket::Datagrams;

        let mut buffers: Vec<Vec<u8>> = (0..4).map(|_| vec![0u8; 2048]).collect();
        let mut datagrams = Datagrams::new();
        let until = std::time::Instant::now() + within;

        while std::time::Instant::now() < until {
            if let Ok(_count) = socket.receive_batch(&mut buffers, &mut datagrams) {
                for (slot, datagram) in datagrams.as_slice().iter().enumerate() {
                    let packet = &buffers[slot][..datagram.length];

                    if FrameHeader::read(packet)
                        .is_some_and(|header| header.frame_type == frame_type::ERR)
                    {
                        return Some(packet.to_vec());
                    }
                }
            }

            std::thread::sleep(std::time::Duration::from_millis(5));
        }

        None
    }

    /// A publication sending to a socket this test owns, the driver's own send
    /// address, and the session the publication runs under.
    ///
    /// The address is learned the way a receiver learns it: the publication
    /// says `SETUP` to its endpoint on a timer, and where that datagram came
    /// from is where to answer it.
    fn publishing_to_a_socket() -> (
        TempDir,
        Conductor,
        CncFile,
        ToClientsReceiver,
        crate::sys::socket::DatagramSocket,
        std::net::SocketAddr,
        i32,
    ) {
        use crate::sys::AddressFamily;
        use crate::sys::socket::DatagramSocket;

        let temp = TempDir::new();
        let config = publication_config(&temp.0);
        let cnc = create(&temp.0);
        let mut conductor = Conductor::new(cnc, &config).expect("conductor");
        let (cnc, mut receiver) = events_reader(&temp.0);
        let mut pending = Vec::new();

        let socket = DatagramSocket::open(AddressFamily::Inet).expect("a socket");
        socket
            .bind("127.0.0.1:0".parse().expect("an address"))
            .expect("a bind");
        socket.set_nonblocking().expect("non-blocking");
        let endpoint = socket.local_address().expect("an address");

        send(
            &conductor,
            ADD_PUBLICATION_TYPE_ID,
            &add_publication_payload(7, 42, 1001, &format!("aeron:udp?endpoint={endpoint}")),
        );
        let ready = await_event(
            &mut conductor,
            &cnc,
            &mut receiver,
            &mut pending,
            ON_PUBLICATION_READY_TYPE_ID,
        );
        let session_id = i32::from_le_bytes(ready[16..20].try_into().expect("four bytes"));

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let mut driver_address = None;

        while std::time::Instant::now() < deadline && driver_address.is_none() {
            conductor.do_work();
            driver_address = next_datagram(&socket).map(|(_, source)| source);
            std::thread::sleep(std::time::Duration::from_millis(5));
        }

        let driver_address = driver_address.expect("the publication says SETUP to its endpoint");

        (
            temp,
            conductor,
            cnc,
            receiver,
            socket,
            driver_address,
            session_id,
        )
    }

    /// Read this test's own socket once, with where the datagram came from.
    fn next_datagram(
        socket: &crate::sys::socket::DatagramSocket,
    ) -> Option<(Vec<u8>, std::net::SocketAddr)> {
        use crate::sys::socket::Datagrams;

        let mut buffers: Vec<Vec<u8>> = (0..4).map(|_| vec![0u8; 2048]).collect();
        let mut datagrams = Datagrams::new();

        if let Ok(_count) = socket.receive_batch(&mut buffers, &mut datagrams) {
            for (slot, datagram) in datagrams.as_slice().iter().enumerate() {
                if let Some(source) = datagram.source {
                    return Some((buffers[slot][..datagram.length].to_vec(), source));
                }
            }
        }

        None
    }

    /// The other end of the same chain: an `ERR` frame that arrives at **this**
    /// driver's own publication has to reach that publication's client.
    ///
    /// This is the half a publisher actually sees, and the two ends are in one
    /// process whenever a client publishes and subscribes to one stream — which
    /// is what every one of the reference's own rejection tests does. Without
    /// it the frame leaves and nothing tells the client, so `RejectImageTest`
    /// waits out its deadline with the whole mechanism working underneath.
    #[test]
    fn an_error_frame_from_a_live_receiver_reaches_the_publications_client() {
        use crate::protocol::{ErrorFrame, StatusMessageFrame};

        let (_temp, mut conductor, cnc, mut receiver, socket, driver_address, session_id) =
            publishing_to_a_socket();
        let mut pending = Vec::new();

        // A status message makes the publication record this receiver, which is
        // what an `ERR` from it is checked against a moment later.
        // The publication's own first term — which is a value the driver made
        // up, not one this test can guess — because a status message a term and
        // a half outside it is refused before it reaches the publication at all
        // (`aeron_network_publication_is_valid_status_message`, `:841-856`).
        let initial_term_id = conductor.network_publications()[0].params.initial_term_id;
        let status = StatusMessageFrame {
            session_id,
            stream_id: 1001,
            consumption_term_id: initial_term_id,
            consumption_term_offset: 0,
            receiver_window: 0,
            receiver_id: 99,
        };
        let mut frame = [0u8; StatusMessageFrame::LENGTH];
        assert!(status.write_with_flags(&mut frame, 0).is_some());
        socket
            .send_batch(Some(driver_address), &[&frame])
            .expect("a send");

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);

        while std::time::Instant::now() < deadline
            && counter_value(
                &conductor,
                crate::system_counters::id::STATUS_MESSAGES_RECEIVED,
            )
            .unwrap_or(0)
                < 1
        {
            conductor.do_work();
            std::thread::sleep(std::time::Duration::from_millis(5));
        }

        assert_eq!(
            Some(1),
            counter_value(
                &conductor,
                crate::system_counters::id::STATUS_MESSAGES_RECEIVED
            ),
            "the sender has the receiver now"
        );

        // And the refusal.
        let reason = b"Needs to be closed";
        let refusal = ErrorFrame {
            session_id,
            stream_id: 1001,
            receiver_id: 99,
            group_tag: 0,
            error_code: deepmsg_cnc::command::ERROR_CODE_IMAGE_REJECTED,
            error_length: i32::try_from(reason.len()).expect("a short reason"),
        };
        let mut frame = [0u8; ErrorFrame::LENGTH + 18];
        assert!(refusal.write(&mut frame[..ErrorFrame::LENGTH]).is_some());
        frame[ErrorFrame::LENGTH..].copy_from_slice(reason);
        socket
            .send_batch(Some(driver_address), &[&frame])
            .expect("a send");

        let payload = await_event(
            &mut conductor,
            &cnc,
            &mut receiver,
            &mut pending,
            ON_PUBLICATION_ERROR_TYPE_ID,
        );

        assert_eq!(42i64.to_le_bytes(), payload[0..8], "the publication named");
        assert_eq!(
            NULL_VALUE.to_le_bytes(),
            payload[8..16],
            "this channel has no destination tracker to name one from"
        );
        assert_eq!(
            session_id,
            i32::from_le_bytes(payload[16..20].try_into().expect("four bytes"))
        );
        assert_eq!(
            1001,
            i32::from_le_bytes(payload[20..24].try_into().expect("four bytes"))
        );
        assert_eq!(99i64.to_le_bytes(), payload[24..32], "who refused it");
        assert_eq!(
            NULL_VALUE.to_le_bytes(),
            payload[32..40],
            "the frame carried no group tag, so the field is ignored"
        );
        assert_eq!(
            1,
            i16::from_le_bytes(payload[40..42].try_into().expect("two bytes")),
            "and the datagram's source is sent as an ordinary IPv4 address"
        );
        assert_eq!(
            socket.local_address().expect("an address").port(),
            u16::from_le_bytes(payload[42..44].try_into().expect("two bytes"))
        );
        assert_eq!([127, 0, 0, 1], payload[44..48]);
        assert_eq!(
            deepmsg_cnc::command::ERROR_CODE_IMAGE_REJECTED,
            i32::from_le_bytes(payload[60..64].try_into().expect("four bytes"))
        );
        assert_eq!(b"Needs to be closed", &payload[68..]);
    }

    #[test]
    fn a_rejected_network_image_is_refused_by_the_receiver_that_owns_it() {
        use crate::protocol::{FrameHeader, SetupFrame, frame_type};
        use crate::sys::AddressFamily;
        use crate::sys::socket::DatagramSocket;

        let temp = TempDir::new();
        let config = publication_config(&temp.0);
        let cnc = create(&temp.0);
        let mut conductor = Conductor::new(cnc, &config).expect("conductor");
        let (cnc, mut receiver) = events_reader(&temp.0);
        let mut pending = Vec::new();

        // A port nobody holds: the driver's receive endpoint will bind it, and
        // this test's socket is the publisher that sends to it.
        let port = {
            let probe = DatagramSocket::open(AddressFamily::Inet).expect("a socket");
            probe
                .bind("127.0.0.1:0".parse().expect("an address"))
                .expect("a bind");
            probe.local_address().expect("an address").port()
        };

        send(
            &conductor,
            ADD_SUBSCRIPTION_TYPE_ID,
            &add_subscription_payload(7, 11, 1001, &format!("aeron:udp?endpoint=127.0.0.1:{port}")),
        );
        await_event(
            &mut conductor,
            &cnc,
            &mut receiver,
            &mut pending,
            ON_SUBSCRIPTION_READY_TYPE_ID,
        );

        let session_id = 99;
        let setup = SetupFrame {
            term_offset: 0,
            session_id,
            stream_id: 1001,
            initial_term_id: 1_000,
            active_term_id: 1_000,
            term_length: 64 * 1024,
            mtu: 1408,
            ttl: 0,
        };
        let mut frame = [0u8; SetupFrame::LENGTH];
        assert!(setup.write_with_flags(&mut frame, 0).is_some());

        let publisher = DatagramSocket::open(AddressFamily::Inet).expect("a socket");
        publisher
            .bind("127.0.0.1:0".parse().expect("an address"))
            .expect("a bind");
        publisher.set_nonblocking().expect("non-blocking");

        for _ in 0..3 {
            conductor.do_work();
        }

        publisher
            .send_batch(
                Some(format!("127.0.0.1:{port}").parse().expect("an address")),
                &[&frame],
            )
            .expect("a send");

        // The image is built off the conductor's thread — there is a log buffer
        // to map — so this waits for it rather than asserting after one pass.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);

        while std::time::Instant::now() < deadline && conductor.publication_images().is_empty() {
            conductor.do_work();
            std::thread::sleep(std::time::Duration::from_millis(5));
        }

        let image_registration_id = conductor.publication_images()[0].registration_id;
        assert_eq!(session_id, conductor.publication_images()[0].session_id);

        drain(&cnc, &mut receiver);

        send(
            &conductor,
            deepmsg_cnc::command::REJECT_IMAGE_TYPE_ID,
            &reject_image_payload(7, 8, image_registration_id, 0, b"Needs to be closed"),
        );
        conductor.do_work();

        let events = drain(&cnc, &mut receiver);

        // The other branch of the handler, and the reason the two are one
        // command: an id that names an image is *not* looked for among the IPC
        // publications, so no publisher is told anything here.
        assert!(
            !events
                .iter()
                .any(|(type_id, _)| *type_id == ON_PUBLICATION_ERROR_TYPE_ID),
            "an image is not an IPC publication, and nothing here publishes"
        );
        assert_eq!(
            Some(1),
            counter_value(&conductor, crate::system_counters::id::IMAGES_REJECTED)
        );
        assert!(
            events
                .iter()
                .any(|(type_id, _)| *type_id == ON_OPERATION_SUCCEEDED_TYPE_ID)
        );

        // And the publisher is told — on the wire, by the receiver, which is the
        // only half of this that a client on the other end can see.
        let frame = await_error_frame(&publisher, std::time::Duration::from_secs(5))
            .expect("an ERR frame reaches the publisher");
        let error = crate::protocol::ErrorFrame::read(&frame).expect("an ERR frame");

        assert_eq!(
            frame_type::ERR,
            FrameHeader::read(&frame).expect("a header").frame_type
        );
        assert_eq!(session_id, error.session_id);
        assert_eq!(1001, error.stream_id);
        assert_eq!(
            deepmsg_cnc::command::ERROR_CODE_IMAGE_REJECTED,
            error.error_code
        );
        assert_eq!(
            b"Needs to be closed",
            error.text(&frame).expect("the reason it was given")
        );
        assert_eq!(
            Some(1),
            counter_value(&conductor, crate::system_counters::id::ERROR_FRAMES_SENT)
        );
    }

    #[test]
    fn a_fragmented_message_from_our_client_arrives_whole() {
        let temp = TempDir::new();
        let config = publication_config(&temp.0);
        let cnc = create(&temp.0);
        let mut conductor = Conductor::new(cnc, &config).expect("conductor");
        let (cnc, mut receiver) = events_reader(&temp.0);
        let mut pending = Vec::new();

        // A publication and a reader for it, the way every client does it.
        send(
            &conductor,
            ADD_PUBLICATION_TYPE_ID,
            &add_publication_payload(7, 42, 1001, "aeron:ipc"),
        );
        let ready = await_event(
            &mut conductor,
            &cnc,
            &mut receiver,
            &mut pending,
            ON_PUBLICATION_READY_TYPE_ID,
        );
        let path = String::from_utf8(ready[36..].to_vec()).expect("a path");
        let session_id = i32::from_le_bytes(ready[16..20].try_into().expect("four"));
        let limit_counter_id = i32::from_le_bytes(ready[24..28].try_into().expect("four"));

        send(
            &conductor,
            ADD_SUBSCRIPTION_TYPE_ID,
            &add_subscription_payload(7, 9, 1001, "aeron:ipc"),
        );
        await_event(
            &mut conductor,
            &cnc,
            &mut receiver,
            &mut pending,
            ON_SUBSCRIPTION_READY_TYPE_ID,
        );
        let image_ready = await_event(
            &mut conductor,
            &cnc,
            &mut receiver,
            &mut pending,
            ON_AVAILABLE_IMAGE_TYPE_ID,
        );
        let deepmsg_cnc::command::Response::AvailableImage {
            subscriber_position_id,
            ..
        } = deepmsg_cnc::command::decode_response(ON_AVAILABLE_IMAGE_TYPE_ID, &image_ready)
        else {
            panic!("an image");
        };

        let producer = deepmsg_client::publication::Publication::open(
            std::path::Path::new(&path),
            42,
            session_id,
            1001,
            limit_counter_id,
            -1,
        )
        .expect("the log the driver named");

        // The window has to be open before a payload this size can be offered.
        conductor.do_work();
        let limit = counter_value(&conductor, limit_counter_id).expect("the limit");

        // Three frames' worth: one frame holds 1376 bytes of payload on this
        // channel, so the client splits this into three and the driver carries
        // frames it knows nothing about the shape of.
        let payload: Vec<u8> = (0..1376 * 2 + 10)
            .map(|index| (index % 251) as u8)
            .collect();
        let deepmsg_core::logbuffer::append::Appended::Ok { .. } = producer.offer(limit, &payload)
        else {
            panic!("the window allows this");
        };

        conductor.do_work();
        let regions = counter_regions(&conductor);
        let join_position = conductor
            .counters()
            .value(&regions, subscriber_position_id)
            .expect("the reader's position");
        assert_eq!(0, join_position);

        // Read the frames back through the reader's mapping, and reassemble.
        let mut image = deepmsg_client::image::Image::open(
            std::path::Path::new(&path),
            42,
            session_id,
            1001,
            subscriber_position_id,
            join_position,
        )
        .expect("the same log, read-only");

        let mut assembler = deepmsg_client::fragment_assembler::FragmentAssembler::new();
        let mut delivered: Vec<(i32, i32, Vec<u8>)> = Vec::new();
        let mut frames = 0;

        // The fragment's type is spelled out because an unannotated closure
        // gets one lifetime inferred from its first use, and this one is handed
        // fragments of every lifetime the image reads.
        frames += image.poll(64, &mut |fragment: &deepmsg_client::image::Fragment<'_>| {
            let mut handler = |message: deepmsg_client::fragment_assembler::Message<'_>| {
                delivered.push((
                    message.header.session_id,
                    message.header.stream_id,
                    message.payload.to_vec(),
                ));
            };

            assembler.push(fragment, &mut handler);
        });

        assert_eq!(3, frames, "the appender split it into three frames");
        assert_eq!(
            vec![(session_id, 1001, payload)],
            delivered,
            "and the reader gets one message, not three fragments"
        );
        assert_eq!(0, assembler.abandoned());
    }

    /// A conductor driven on its own thread.
    ///
    /// The client's API *waits* for replies, and a reply needs the conductor to
    /// run: in one thread the two deadlock. This is the smallest thing that
    /// makes the pair behave like the two processes they would be, and it is
    /// what the lifeline tests need — they are about a client noticing that its
    /// driver is not there.
    struct DrivenDriver {
        stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
        thread: Option<std::thread::JoinHandle<()>>,
    }

    impl DrivenDriver {
        fn start(cnc: CncFile, config: DriverConfig) -> Self {
            let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            let flag = std::sync::Arc::clone(&stop);

            let thread = std::thread::spawn(move || {
                let mut conductor = Conductor::new(cnc, &config).expect("conductor");

                while !flag.load(std::sync::atomic::Ordering::Relaxed) && conductor.is_running() {
                    conductor.do_work();
                    std::thread::sleep(std::time::Duration::from_micros(200));
                }

                // A clean stop, so the ring carries the sentinel a client
                // reads as "the driver meant to go" rather than letting its
                // heartbeat go stale.
                let _ = conductor.close();
            });

            Self {
                stop,
                thread: Some(thread),
            }
        }

        /// Stop it the way a `kill -9` would: no close, so no sentinel and no
        /// fresh heartbeat — the file keeps whatever it last held.
        fn kill(mut self) {
            self.stop.store(true, std::sync::atomic::Ordering::Relaxed);

            if let Some(thread) = self.thread.take() {
                let _ = thread.join();
            }
        }
    }

    impl Drop for DrivenDriver {
        fn drop(&mut self) {
            self.stop.store(true, std::sync::atomic::Ordering::Relaxed);

            if let Some(thread) = self.thread.take() {
                let _ = thread.join();
            }
        }
    }

    /// A client connected to a directory, with the driver beneath it running on
    /// its own thread.
    fn connected(
        temp: &TempDir,
        config: &DriverConfig,
    ) -> (DrivenDriver, deepmsg_client::client::Client) {
        let cnc = create(&temp.0);
        let driver = DrivenDriver::start(cnc, config.clone());
        let client = deepmsg_client::client::Client::connect(&temp.0).expect("the client connects");

        (driver, client)
    }

    /// The images a subscription holds, as the *client* sees them: an
    /// `ON_UNAVAILABLE_IMAGE` that has been polled takes one away, and an
    /// `ON_AVAILABLE_IMAGE` puts one back.
    fn images_of(client: &deepmsg_client::client::Client, registration_id: i64) -> usize {
        client
            .subscription(registration_id)
            .map_or(0, |subscription| subscription.images().len())
    }

    /// Wait for the client to see something, polling events as it waits — an
    /// `ON_UNAVAILABLE_IMAGE` does not arrive by itself.
    fn wait_for<F>(
        client: &mut deepmsg_client::client::Client,
        within: std::time::Duration,
        mut predicate: F,
    ) -> bool
    where
        F: FnMut(&mut deepmsg_client::client::Client) -> bool,
    {
        let deadline = std::time::Instant::now() + within;

        loop {
            client.poll();

            if predicate(client) {
                return true;
            }
            if std::time::Instant::now() >= deadline {
                return false;
            }

            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    }

    /// The untethered machine, end to end and through a client: a reader that
    /// stops reading is put aside, and then either woken or closed.
    ///
    /// The IPC half of what `tests/interop/udp_transport.rs` does to an image,
    /// and it lives here — and in the ordinary test run — because IPC needs no
    /// socket and no second process. What it covers that the machine's own unit
    /// tests cannot is everything that leaves the driver: `ON_UNAVAILABLE_IMAGE`
    /// on a client's ring, an image that comes back at the join position, and a
    /// counter that goes back to the manager.
    ///
    /// Three subscriptions on one publication, because all three outcomes have
    /// the same precondition: **"behind" is relative to the fastest reader**
    /// (`aeron_ipc_publication.c:357-360`), so a lone reader is never put aside
    /// however slowly it reads. One reader that keeps up is what makes the
    /// other two late.
    #[test]
    fn an_untethered_ipc_reader_is_put_aside_woken_or_closed() {
        let temp = TempDir::new();
        let config = DriverConfig {
            // The machine runs on the conductor's timeout tier, and the three
            // stages below are tens of milliseconds: at the default one-second
            // tier each stage would cost a second and the test would be
            // measuring the clock rather than the machine.
            timer_interval_ns: 10_000_000,
            ..publication_config(&temp.0)
        };
        let (_driver, mut client) = connected(&temp, &config);
        let timeout = std::time::Duration::from_secs(5);

        // The publication's channel carries the window the limit is measured
        // against and the three timeouts. For a publication's own readers both
        // come from *its* URI (`aeron_driver_uri.c:420-442`), not from the
        // readers' — which is what makes this one channel a configuration for
        // all of them.
        // The three stages are long enough to be *observed*: a reader that is
        // put aside and woken again is unavailable for the linger and resting
        // timeouts and no longer, so a stage of tens of milliseconds would make
        // the window this test looks through narrower than its own polling.
        let channel = "aeron:ipc?pub-wnd=8192\
                       |untethered-window-limit-timeout=300ms\
                       |untethered-linger-timeout=300ms\
                       |untethered-resting-timeout=300ms";

        let reader = client
            .add_subscription(channel, 1001, timeout)
            .expect("the reader subscribes");
        let rejoining = client
            .add_subscription("aeron:ipc?tether=false|rejoin=true", 1001, timeout)
            .expect("the rejoining reader subscribes");
        let leaving = client
            .add_subscription("aeron:ipc?tether=false|rejoin=false", 1001, timeout)
            .expect("the leaving reader subscribes");

        let publication = client
            .add_publication(channel, 1001, timeout)
            .expect("the publication");

        assert!(
            wait_for(&mut client, timeout, |client| {
                images_of(client, reader) > 0
                    && images_of(client, rejoining) > 0
                    && images_of(client, leaving) > 0
            }),
            "all three readers hold the publication's log buffer: {} {} {}\n\
             client error: {:?}\nsubscriptions: {:?}\npublication: {:?}",
            images_of(&client, reader),
            images_of(&client, rejoining),
            images_of(&client, leaving),
            client.error(),
            client
                .subscriptions()
                .iter()
                .map(|s| (
                    s.registration_id(),
                    s.images().len(),
                    s.channel().to_owned()
                ))
                .collect::<Vec<_>>(),
            client
                .publications()
                .iter()
                .map(|p| p.registration_id())
                .collect::<Vec<_>>()
        );

        // Less than the window, because the window is what holds the producer
        // back: the slowest reader is still at its join position, so the
        // producer may write one window past it and no further.
        for index in 0..6 {
            let mut payload = format!("untethered-{index}").into_bytes();
            payload.resize(1200, b'.');

            let deadline = std::time::Instant::now() + timeout;
            let mut offered = false;

            while !offered && std::time::Instant::now() < deadline {
                client.poll();
                read_the_reader(&mut client, reader);

                offered = matches!(
                    client.offer(publication, &payload),
                    Some(deepmsg_core::logbuffer::append::Appended::Ok { .. })
                );

                std::thread::sleep(std::time::Duration::from_millis(2));
            }

            assert!(offered, "the producer must be able to write its window");
        }

        // First outcome: both late readers are told their image is gone, and
        // the reader that kept up is untouched. The reading happens here rather
        // than in a loop before it because the two are the same window: being
        // put aside lasts exactly two stages, and the machine is already
        // running by the time the last message is written.
        assert!(
            wait_for(&mut client, timeout, |client| {
                read_the_reader(client, reader);

                images_of(client, rejoining) == 0 && images_of(client, leaving) == 0
            }),
            "both late readers are put aside: rejoining {}, leaving {}, reader {}",
            images_of(&client, rejoining),
            images_of(&client, leaving),
            images_of(&client, reader)
        );
        assert_eq!(
            1,
            images_of(&client, reader),
            "the machine moved who was late, not the publication"
        );

        // Second and third: the rejoining one is woken at the join position,
        // and the one that is not rejoining is never told anything again.
        assert!(
            wait_for(&mut client, timeout, |client| images_of(client, rejoining)
                > 0),
            "the rejoining reader is woken with its image"
        );
        assert_eq!(
            0,
            images_of(&client, leaving),
            "a reader that is not rejoining is closed, and closure is silent"
        );
    }

    /// Read whatever the reader has, which is what moves its position.
    fn read_the_reader(client: &mut deepmsg_client::client::Client, subscription_id: i64) {
        use deepmsg_client::client::FRAGMENT_LIMIT;
        use deepmsg_client::fragment_assembler::Message;

        client.poll_subscription(subscription_id, FRAGMENT_LIMIT, |_message: Message<'_>| {});
    }

    #[test]
    fn a_client_notices_a_driver_that_stopped_on_purpose() {
        let temp = TempDir::new();
        let config = publication_config(&temp.0);
        let (driver, mut client) = connected(&temp, &config);

        // A command, so this driver knows the client and has given it a
        // heartbeat counter to keep alive.
        client
            .add_subscription("aeron:ipc", 1001, std::time::Duration::from_secs(5))
            .expect("subscribed");
        assert!(client.poll());
        assert_eq!(None, client.error());

        let cnc = CncFile::open_writable(&temp.0, std::time::Duration::from_secs(5))
            .expect("the file, from the test's side");

        // A clean stop writes the sentinel (`aeron_driver_conductor.c:3493`):
        // a client that reads it knows the driver left on purpose rather than
        // being killed, and those are different errors.
        drop(driver);
        std::thread::sleep(std::time::Duration::from_millis(50));

        let ring = cnc.to_driver_ring().expect("the ring");
        assert_eq!(
            Some(deepmsg_cnc::layout::NULL_VALUE),
            ring.consumer_heartbeat(),
            "the driver wrote the sentinel as it went"
        );

        client.poll();
        assert_eq!(
            Some(deepmsg_client::client::ClientError::DriverShutdown),
            client.error(),
            "and the client reads it as one"
        );
        assert!(client.is_terminated());
    }

    #[test]
    fn a_client_notices_a_driver_that_was_killed() {
        let temp = TempDir::new();
        let config = publication_config(&temp.0);
        let (driver, mut client) = connected(&temp, &config);

        client
            .add_subscription("aeron:ipc", 1001, std::time::Duration::from_secs(5))
            .expect("subscribed");
        assert!(client.poll());

        // Killed: no sentinel, and nothing refreshing the heartbeat any more.
        // The test writes one that is already older than the client's timeout,
        // because waiting ten seconds for the real thing is not a unit test.
        driver.kill();

        let cnc = CncFile::open_writable(&temp.0, std::time::Duration::from_secs(5))
            .expect("the file, from the test's side");
        let region = cnc.to_driver_region().expect("the ring");
        let consumer =
            deepmsg_cnc::ToDriverRingConsumer::new(&region.as_read_only()).expect("a command ring");
        let stale = clock::epoch_millis() - (deepmsg_client::client::DRIVER_TIMEOUT_MS + 1_000);
        consumer
            .write_consumer_heartbeat(&region, stale)
            .expect("in range");

        client.poll();
        let Some(deepmsg_client::client::ClientError::DriverTimeout { age_ms, timeout_ms }) =
            client.error()
        else {
            panic!(
                "a heartbeat past the timeout is a driver that is gone, not one that is \
                 slow: {:?}",
                client.error()
            );
        };

        // The timeout is the contract. The age is *when the client looked*: the
        // heartbeat above was written a second past the timeout, so what comes
        // back is that second plus however long the poll itself took — the same
        // millisecond in a quiet run, and one more whenever the clock ticks
        // over between the two, which a loaded machine does about a run in
        // three. So the age is a window, and the window is narrow: a client
        // measuring from any other moment lands outside it by orders of
        // magnitude, which is what it is here to catch.
        assert_eq!(deepmsg_client::client::DRIVER_TIMEOUT_MS, timeout_ms);
        assert!(
            (deepmsg_client::client::DRIVER_TIMEOUT_MS + 1_000
                ..=deepmsg_client::client::DRIVER_TIMEOUT_MS + 5_000)
                .contains(&age_ms),
            "the heartbeat was written 1000ms past the timeout, so the age is that plus \
             the poll's own delay, not {age_ms}"
        );
    }

    #[test]
    fn a_client_can_add_hold_and_remove_a_counter() {
        let temp = TempDir::new();
        let config = publication_config(&temp.0);
        let (_driver, mut client) = connected(&temp, &config);

        // Add: the command, the driver's reply, and the handle the reply
        // named — whose registration id is the correlation id the request
        // used, the same binding the reference's `on_counter_ready` makes
        // (`aeron_client_conductor.c:850-895`).
        let counter = client
            .add_counter(
                100,
                b"a key",
                "a counter of ours",
                std::time::Duration::from_secs(5),
            )
            .expect("the counter is allocated");

        assert_eq!(1, client.counters().len());
        assert_eq!(
            Some(&counter),
            client.counter(counter.registration_id()),
            "the client holds what it asked for"
        );

        // The file agrees — the same find an `AeronStat` does — and the label
        // and key arrived whole.
        let cnc = CncFile::open_writable(&temp.0, std::time::Duration::from_secs(5))
            .expect("the file, from the test's side");
        let counters = cnc.counters().expect("the counter regions");
        assert_eq!(
            Some(counter.counter_id()),
            counters.find_by_type_and_registration(100, counter.registration_id())
        );
        let descriptor = counter.descriptor(&counters).expect("allocated");
        assert_eq!("a counter of ours", descriptor.label);
        assert_eq!(0, descriptor.value, "a counter starts at zero");

        // The value is this client's to write, and the write is the whole of
        // the counter's life as far as the driver is concerned.
        {
            let writable = cnc.counters_writable().expect("writable regions");
            assert!(counter.set_value(&writable, 41));
        }
        assert_eq!(Some(41), counter.value(&counters));

        // Remove: the acknowledgement unblocks the call, and the counter goes
        // back to the pool.
        client
            .remove_counter(&counter, std::time::Duration::from_secs(5))
            .expect("removed");

        assert!(client.counters().is_empty());
        assert!(
            counters
                .find_by_type_and_registration(100, counter.registration_id())
                .is_none(),
            "the slot is reclaimed"
        );
    }

    #[test]
    fn counter_announcements_arrive_as_events_for_the_watchers() {
        let temp = TempDir::new();
        let config = publication_config(&temp.0);
        let (_driver, mut client) = connected(&temp, &config);

        let counter = client
            .add_counter(
                100,
                b"a key",
                "a counter of ours",
                std::time::Duration::from_secs(5),
            )
            .expect("the counter is allocated");

        // Two announcements crossed the broadcast while the add was waiting:
        // this client's heartbeat — keyed by the client id — and the counter
        // it asked for. Both are events for the watchers; only the second was
        // also the reply the add was waiting on.
        let events = client.counter_events();
        assert!(
            events.iter().any(|event| matches!(
                event,
                CounterEvent::Ready { correlation_id, .. } if *correlation_id == client.client_id()
            )),
            "the heartbeat's announcement is an event, keyed by the client id"
        );
        assert!(
            events.iter().any(|event| matches!(
                event,
                CounterEvent::Ready { correlation_id, counter_id }
                    if *correlation_id == counter.registration_id()
                        && *counter_id == counter.counter_id()
            )),
            "and so is the counter the add asked for"
        );

        client
            .remove_counter(&counter, std::time::Duration::from_secs(5))
            .expect("removed");

        // The acknowledgement is what unblocks the call; the announcement
        // follows it on the broadcast and only a later poll reads it
        // (`:6221-6235` — the reference's order too).
        let mut unavailable = false;
        for _ in 0..200 {
            client.poll();
            if client.counter_events().iter().any(|event| {
                matches!(
                    event,
                    CounterEvent::Unavailable { correlation_id, .. }
                        if *correlation_id == counter.registration_id()
                )
            }) {
                unavailable = true;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        assert!(unavailable, "the removal is an event for the watchers too");
    }

    #[test]
    fn another_clients_counters_arrive_as_events_too() {
        let temp = TempDir::new();
        let config = publication_config(&temp.0);
        let (_driver, mut watcher) = connected(&temp, &config);

        // A second client on the same directory. The broadcast is one ring
        // every client reads in full, so the watcher sees the other client's
        // heartbeat and counter appear, though nothing of the watcher's was
        // waiting for either.
        let mut other =
            deepmsg_client::client::Client::connect(&temp.0).expect("the other client connects");
        let counter = other
            .add_counter(
                100,
                b"a key",
                "a counter of theirs",
                std::time::Duration::from_secs(5),
            )
            .expect("the counter is allocated");

        let mut saw_heartbeat = false;
        let mut saw_counter = false;
        for _ in 0..200 {
            watcher.poll();
            for event in watcher.counter_events() {
                match event {
                    CounterEvent::Ready { correlation_id, .. }
                        if correlation_id == other.client_id() =>
                    {
                        saw_heartbeat = true;
                    }
                    CounterEvent::Ready { correlation_id, .. }
                        if correlation_id == counter.registration_id() =>
                    {
                        saw_counter = true;
                    }
                    _ => {}
                }
            }

            if saw_heartbeat && saw_counter {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }

        assert!(
            saw_heartbeat,
            "the other client's heartbeat is an event here"
        );
        assert!(saw_counter, "and so is its counter");
    }

    #[test]
    fn a_client_reads_the_counters_through_its_own_mapping() {
        let temp = TempDir::new();
        let config = publication_config(&temp.0);
        let (_driver, mut client) = connected(&temp, &config);

        client
            .add_subscription("aeron:ipc", 1001, std::time::Duration::from_secs(5))
            .expect("subscribed");

        // The reader is over the client's own mapping of the file — the same
        // one another process would map — and finds the heartbeat the driver
        // allocated for it, holding a value this client's polls have been
        // writing. This is the `aeron_counters_reader(client)` an
        // `AeronStat`-shaped tool would walk (`aeron_client.c:276-286`).
        let reader = client.counters_reader().expect("the counter regions");
        let heartbeat = reader
            .find_by_type_and_registration(
                deepmsg_cnc::counters::CLIENT_HEARTBEAT_TYPE_ID,
                client.client_id(),
            )
            .expect("the heartbeat is in the file");
        assert!(
            reader.value(heartbeat).unwrap_or(0) > 0,
            "a living client has been writing its heartbeat"
        );
    }

    #[test]
    fn a_client_whose_heartbeat_counter_is_reclaimed_gives_up() {
        let temp = TempDir::new();
        // A liveness window of ten milliseconds and a tier of one: the driver
        // reaps this client as soon as it stops saying it is alive, which also
        // takes its heartbeat counter away — the exact state that made the
        // cached id dangerous.
        let config = DriverConfig {
            aeron_dir: temp.0.clone(),
            ipc_term_buffer_length: 64 * 1024,
            client_liveness_timeout_ns: 100_000_000,
            timer_interval_ns: 10_000_000,
            ..DriverConfig::default()
        };
        let (_driver, mut client) = connected(&temp, &config);

        client
            .add_subscription("aeron:ipc", 1001, std::time::Duration::from_secs(5))
            .expect("subscribed");
        assert!(client.poll(), "the heartbeat counter is found and written");

        // Stop saying anything, and let the driver decide. Long enough that
        // the window has certainly passed and a tier has certainly run.
        std::thread::sleep(std::time::Duration::from_millis(400));

        client.poll();
        assert_eq!(
            Some(deepmsg_client::client::ClientError::HeartbeatCounterClosed),
            client.error(),
            "the counter this client was writing is somebody else's now, or nobody's"
        );
    }

    #[test]
    fn a_client_that_falls_a_ring_behind_counts_the_lap() {
        let temp = TempDir::new();
        let config = publication_config(&temp.0);
        let (_driver, mut client) = connected(&temp, &config);

        // One poll, so the client's cursor is planted where the ring begins.
        client.poll();
        assert_eq!(0, client.laps());

        // Now write more than a ring's worth of events through the driver's own
        // transmitter, with nobody reading them. The writer wraps rather than
        // refusing, so the events the client has not read yet are overwritten —
        // which is what a lap is, and what the reference calls a system error.
        let cnc = CncFile::open_writable(&temp.0, std::time::Duration::from_secs(5))
            .expect("the file, from the test's side");
        let region = cnc.to_clients_region_writable().expect("the event ring");
        let mut transmitter = deepmsg_cnc::ToClientsTransmitter::new(&region).expect("a ring");

        let payload = vec![0x5Au8; 512];
        let record = layout::align_up(
            payload.len() + layout::RECORD_HEADER_LENGTH,
            layout::RECORD_ALIGNMENT,
        );
        let writes = config.layout.to_clients_length / record + 2;

        for _ in 0..writes {
            transmitter
                .transmit(&region, ON_ERROR_TYPE_ID, &payload)
                .expect("the ring has room for a wrap");
        }

        client.poll();

        assert_eq!(
            1,
            client.laps(),
            "the client resynchronised past the overwritten span"
        );
        // The lap was caught *before* a read, so no message was lost mid-read
        // — which is what `discarded` counts, and why it is zero here while the
        // lap count is not.
        assert_eq!(0, client.discarded());

        // A command that timed out afterwards says so, rather than leaving the
        // caller to wonder whether the driver ignored it.
        let error = deepmsg_client::client::CommandError::TimedOut {
            correlation_id: 7,
            laps: client.laps(),
            discarded: client.discarded(),
        };
        assert!(
            error.to_string().contains("resynchronised past the ring"),
            "a timeout carries the reason it might have happened: {error}"
        );
    }

    #[test]
    fn a_poll_reads_one_term_at_a_time() {
        let temp = TempDir::new();
        let config = publication_config(&temp.0);
        let cnc = create(&temp.0);
        let mut conductor = Conductor::new(cnc, &config).expect("conductor");
        let (cnc, mut receiver) = events_reader(&temp.0);
        let mut pending = Vec::new();

        send(
            &conductor,
            ADD_PUBLICATION_TYPE_ID,
            &add_publication_payload(7, 42, 1001, "aeron:ipc"),
        );
        let ready = await_event(
            &mut conductor,
            &cnc,
            &mut receiver,
            &mut pending,
            ON_PUBLICATION_READY_TYPE_ID,
        );
        let path = String::from_utf8(ready[36..].to_vec()).expect("a path");
        let session_id = i32::from_le_bytes(ready[16..20].try_into().expect("four"));
        let limit_counter_id = i32::from_le_bytes(ready[24..28].try_into().expect("four"));

        send(
            &conductor,
            ADD_SUBSCRIPTION_TYPE_ID,
            &add_subscription_payload(7, 9, 1001, "aeron:ipc"),
        );
        await_event(
            &mut conductor,
            &cnc,
            &mut receiver,
            &mut pending,
            ON_SUBSCRIPTION_READY_TYPE_ID,
        );
        let image_ready = await_event(
            &mut conductor,
            &cnc,
            &mut receiver,
            &mut pending,
            ON_AVAILABLE_IMAGE_TYPE_ID,
        );
        let deepmsg_cnc::command::Response::AvailableImage {
            subscriber_position_id,
            ..
        } = deepmsg_cnc::command::decode_response(ON_AVAILABLE_IMAGE_TYPE_ID, &image_ready)
        else {
            panic!("an image");
        };

        let producer = deepmsg_client::publication::Publication::open(
            std::path::Path::new(&path),
            42,
            session_id,
            1001,
            limit_counter_id,
            -1,
        )
        .expect("the log the driver named");

        let mut image = deepmsg_client::image::Image::open(
            std::path::Path::new(&path),
            42,
            session_id,
            1001,
            subscriber_position_id,
            0,
        )
        .expect("the same log, read-only");

        // Fill the first term. Each message is small enough for the window a
        // single reader opens, and the reader reports the position it has read
        // — which is what lets the publisher get past one window at all
        // (`aeron_ipc_publication.c:296-313` computes the limit from it).
        let small = vec![b'a'; 1024];
        let mut written = 0;

        loop {
            conductor.do_work();
            let limit = counter_value(&conductor, limit_counter_id).expect("the limit");

            match producer.offer(limit, &small) {
                deepmsg_core::logbuffer::append::Appended::Ok { .. } => written += small.len(),
                // The reader has to catch up for the window to reopen — but
                // only as far as it has to: reporting the position it has read
                // is what lets the publisher carry on, and holding it back to
                // the point where the offer is refused leaves unread frames at
                // the end of the term, which is what this test is about.
                deepmsg_core::logbuffer::append::Appended::BackPressured => {
                    image.poll(64, |_| {});
                    set_counter(&conductor, subscriber_position_id, image.position());
                    continue;
                }
                // The message did not fit the term's remainder: a padding frame
                // covers it and the log is now in its next term.
                deepmsg_core::logbuffer::append::Appended::EndOfLog => {
                    conductor.do_work();
                    let limit = counter_value(&conductor, limit_counter_id).expect("the limit");

                    assert!(
                        matches!(
                            producer.offer(limit, &small),
                            deepmsg_core::logbuffer::append::Appended::Ok { .. }
                        ),
                        "the term has rotated and the next message lands in the new one"
                    );
                    written += small.len();
                    break;
                }
                other => panic!("{other:?}"),
            }
        }

        assert!(
            written > 32 * 1024,
            "more than one window's worth had to be written to fill the term: {written}"
        );

        // One poll with a fragment limit far above what two terms hold
        // together. What it delivers must all lie in **one term**: the
        // reference fixes the partition at the poll's entry and stops the scan
        // at that term's end (`aeron_image.c:266-273`), and a caller that
        // throttles by counting fragments is entitled to the same numbers.
        let mut positions = Vec::new();
        let read = image.poll(100_000, |fragment| positions.push(fragment.position()));

        assert_eq!(read, positions.len());
        assert!(read > 0, "there is unread data in this term");

        let term_length = 64 * 1024;
        let term = positions[0] / term_length;
        assert!(
            positions
                .iter()
                .all(|position| position / term_length == term),
            "a poll delivered fragments from more than one term: {positions:?}"
        );

        // And the message that rotated into the next term is still there for
        // the next poll — a poll that stopped at the term's end did not lose
        // it.
        let mut next_term = 0;
        image.poll(100_000, |_| next_term += 1);
        assert!(
            next_term > 0,
            "the message in the next term is read by the next poll"
        );
    }

    #[test]
    fn a_publication_stops_at_its_window_and_goes_again_when_the_reader_reads() {
        let temp = TempDir::new();
        let config = publication_config(&temp.0);
        let cnc = create(&temp.0);
        let mut conductor = Conductor::new(cnc, &config).expect("conductor");
        let (cnc, mut receiver) = events_reader(&temp.0);
        let mut pending = Vec::new();

        send(
            &conductor,
            ADD_PUBLICATION_TYPE_ID,
            &add_publication_payload(7, 42, 1001, "aeron:ipc"),
        );
        let ready = await_event(
            &mut conductor,
            &cnc,
            &mut receiver,
            &mut pending,
            ON_PUBLICATION_READY_TYPE_ID,
        );
        let path = String::from_utf8(ready[36..].to_vec()).expect("a path");
        let session_id = i32::from_le_bytes(ready[16..20].try_into().expect("four"));
        let limit_counter_id = i32::from_le_bytes(ready[24..28].try_into().expect("four"));

        send(
            &conductor,
            ADD_SUBSCRIPTION_TYPE_ID,
            &add_subscription_payload(7, 9, 1001, "aeron:ipc"),
        );
        await_event(
            &mut conductor,
            &cnc,
            &mut receiver,
            &mut pending,
            ON_SUBSCRIPTION_READY_TYPE_ID,
        );
        let image_ready = await_event(
            &mut conductor,
            &cnc,
            &mut receiver,
            &mut pending,
            ON_AVAILABLE_IMAGE_TYPE_ID,
        );
        let deepmsg_cnc::command::Response::AvailableImage {
            subscriber_position_id,
            ..
        } = deepmsg_cnc::command::decode_response(ON_AVAILABLE_IMAGE_TYPE_ID, &image_ready)
        else {
            panic!("an image");
        };

        let producer = deepmsg_client::publication::Publication::open(
            std::path::Path::new(&path),
            42,
            session_id,
            1001,
            limit_counter_id,
            -1,
        )
        .expect("the log the driver named");
        let mut image = deepmsg_client::image::Image::open(
            std::path::Path::new(&path),
            42,
            session_id,
            1001,
            subscriber_position_id,
            0,
        )
        .expect("the same log, read-only");

        // The window is half a term (32 KiB) and each message is 4 KiB, so the
        // eighth cannot be written until the reader reports something: this is
        // the whole of backpressure, and it is what stops a producer from
        // outrunning a reader on a log buffer that only has three terms.
        let message = vec![b'x'; 4 * 1024];
        let mut offered = 0;

        loop {
            conductor.do_work();
            let limit = counter_value(&conductor, limit_counter_id).expect("the limit");

            match producer.offer(limit, &message) {
                deepmsg_core::logbuffer::append::Appended::Ok { .. } => offered += 1,
                deepmsg_core::logbuffer::append::Appended::BackPressured => break,
                other => panic!("{other:?}"),
            }

            assert!(
                offered < 32,
                "the window has to close before a whole term is written"
            );
        }

        assert_eq!(
            8, offered,
            "half a term of 64 KiB holds exactly eight of these"
        );

        // The limit stopped where the reader's position plus one window is, and
        // it does not move while the reader does not.
        let stopped_at = counter_value(&conductor, limit_counter_id).expect("the limit");
        assert_eq!(32 * 1024, stopped_at);

        for _ in 0..4 {
            conductor.do_work();
        }
        assert_eq!(
            Some(stopped_at),
            counter_value(&conductor, limit_counter_id),
            "a limit that moves with nobody reading is a window that is not one"
        );
        assert!(matches!(
            producer.offer(stopped_at, &message),
            deepmsg_core::logbuffer::append::Appended::BackPressured
        ));

        // The reader catches up and reports, which is the only thing that can
        // open the window again.
        let mut read = 0;
        image.poll(64, |_| read += 1);
        assert_eq!(
            24, read,
            "eight messages of 4 KiB, and each of those is three frames"
        );
        set_counter(&conductor, subscriber_position_id, image.position());

        conductor.do_work();
        let reopened = counter_value(&conductor, limit_counter_id).expect("the limit");
        assert!(
            reopened > stopped_at,
            "the window follows the reader: {reopened} against {stopped_at}"
        );

        assert!(matches!(
            producer.offer(reopened, &message),
            deepmsg_core::logbuffer::append::Appended::Ok { .. }
        ));
    }
}
