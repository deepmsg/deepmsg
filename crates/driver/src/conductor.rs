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
    ERROR_CODE_GENERIC_ERROR, ERROR_CODE_MALFORMED_COMMAND,
    ERROR_CODE_RESOURCE_TEMPORARILY_UNAVAILABLE, ERROR_CODE_STORAGE_SPACE,
    ERROR_CODE_UNKNOWN_COMMAND_TYPE_ID, ERROR_CODE_UNKNOWN_COUNTER, ERROR_CODE_UNKNOWN_PUBLICATION,
    ERROR_CODE_UNKNOWN_SUBSCRIPTION, ImageBuffersReady, ON_AVAILABLE_IMAGE_TYPE_ID,
    ON_CLIENT_TIMEOUT_TYPE_ID, ON_COUNTER_READY_TYPE_ID, ON_ERROR_TYPE_ID,
    ON_OPERATION_SUCCEEDED_TYPE_ID, ON_SUBSCRIPTION_READY_TYPE_ID, ON_UNAVAILABLE_COUNTER_TYPE_ID,
    ON_UNAVAILABLE_IMAGE_TYPE_ID, PublicationBuffersReady, REMOVE_PUBLICATION_FLAG_REVOKE,
    decode_add_counter, decode_add_publication, decode_add_subscription, decode_correlated,
    decode_remove_counter, decode_remove_publication, decode_remove_subscription,
    encode_client_timeout, encode_counter_update, encode_error, encode_operation_succeeded,
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
use crate::native_resource_agent::StorageChecks;
use crate::network_publications::NetworkPublications;
use crate::send_endpoints::SendChannelEndpoints;
use crate::sender::Sender;
use crate::system_counters::{self, SystemCounterError, SystemCounters};

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

        let now_ns = clock::epoch_nano_time();
        let mut clock = CachedClock::new();
        let now_ms = clock.update(now_ns);

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
            send_endpoints: SendChannelEndpoints::new(),
            network_publications,
            sender,
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
        };

        Ok(conductor)
    }

    /// One duty cycle. The return value is the reference's `work_count`.
    pub fn do_work(&mut self) -> usize {
        let now_ns = clock::epoch_nano_time();
        let mut work_count = 0;

        self.track_cycle(now_ns);

        if now_ns > self.clock_update_deadline_ns {
            self.now_ms = self.clock.update(now_ns);
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
            + self.update_publication_limits();
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
        let mut work = self.poll_sender_events();

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
            ms: self.now_ms,
            ns: clock::epoch_nano_time(),
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
            self.sender.proxy(),
            now,
            &mut transmit,
        );

        work
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
                crate::sender::SenderEvent::Fault {
                    error_code,
                    description,
                } => {
                    self.pending_log_errors.push((error_code, description));
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

        commands.read(&region, COMMAND_DRAIN_LIMIT, |type_id, payload| {
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

                        if let Err(error) = subscriptions.add_subscription(
                            &request,
                            config,
                            counters,
                            &counter_regions,
                            clients,
                            publications,
                            now,
                            &mut transmit,
                        ) {
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
        })
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
            now_ns,
            self.now_ms,
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
        self.clients.reap_expired(
            self.now_ms,
            &mut self.counters,
            &counter_regions,
            &mut transmit,
            &mut self.publications,
            &mut self.subscriptions,
        )
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
            // The reference prints a formatted date here; stderr is a
            // diagnostic and not a contract, and the epoch time says the same
            // thing (`aeron_distinct_error_log.c:183-191`).
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
        ADD_EXCLUSIVE_PUBLICATION_TYPE_ID, ADD_PUBLICATION_TYPE_ID, ADD_SUBSCRIPTION_TYPE_ID,
        ERROR_CODE_INVALID_CHANNEL, ERROR_CODE_NOT_SUPPORTED, ON_ERROR_TYPE_ID,
        ON_EXCLUSIVE_PUBLICATION_READY_TYPE_ID, ON_PUBLICATION_READY_TYPE_ID,
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
        // pass read without any byte arithmetic. `0x07` is ADD_DESTINATION:
        // still unimplemented (the network transport is P1-4), which is what a
        // test wants from a stand-in — a command whose handling cannot start
        // happening.
        send(&conductor, 0x07, b"first");
        send(&conductor, 0x07, b"second");

        conductor.do_work();
        assert_eq!(1, conductor.unhandled_commands(), "one command per pass");

        // And the ring is not stuck: the next pass finds the other one.
        conductor.do_work();
        assert_eq!(2, conductor.unhandled_commands());

        // A third pass finds nothing, and the second pass left the ring empty.
        conductor.do_work();
        assert_eq!(2, conductor.unhandled_commands());

        let ring = conductor.cnc.to_driver_ring().expect("producer view");
        let consumed =
            2 * layout::align_up(5 + layout::RECORD_HEADER_LENGTH, layout::RECORD_ALIGNMENT);
        assert_eq!(
            Some(consumed as i64),
            ring.consumer_position(),
            "both records were consumed, and only they"
        );
    }

    #[test]
    fn an_unimplemented_command_is_counted_and_named() {
        let (_temp, mut conductor) = running(TerminationPolicy::Deny);

        send(&conductor, 0x07, b"aeron:ipc|1"); // ADD_DESTINATION
        conductor.do_work();

        assert_eq!(1, conductor.unhandled_commands());
        assert_eq!(0, conductor.unknown_commands());
        assert_eq!(Some(Command::AddDestination), conductor.last_unhandled());
        assert!(conductor.is_running(), "and nothing else happened");
    }

    #[test]
    fn a_static_counter_request_is_recognised_and_not_served() {
        // ADD_STATIC_COUNTER is the one unimplemented command whose absence is
        // a recorded divergence rather than an unbuilt transport feature: the
        // reference's driver allocates the counter
        // (`aeron_driver_conductor.c:3153-3166`, the handler at `:6255`) and
        // answers `ON_STATIC_COUNTER`, which its client pairs with the ready
        // handler (`aeron_client_conductor.c:1147`). deepmsg recognises the
        // command — it is in the protocol's table — but serves nothing, so a
        // client that asks for one waits for a reply that never comes
        // (docs/compat.md, "The counter a client asks for").
        let (_temp, mut conductor) = running(TerminationPolicy::Deny);

        send(&conductor, 0x0F, &[0u8; 16]);
        conductor.do_work();

        assert_eq!(1, conductor.unhandled_commands());
        assert_eq!(0, conductor.unknown_commands());
        assert_eq!(Some(Command::AddStaticCounter), conductor.last_unhandled());

        // Counted, not recorded: the protocol defines this command, so it is
        // no error to receive one — merely a thing this driver cannot do.
        let mut errors = Vec::new();
        let log = conductor.cnc.error_log().expect("the error log");
        assert_eq!(0, log.read(i64::MIN, &mut errors).entries);
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

        send(&conductor, 0x07, b"channel");
        conductor.do_work();
        assert_eq!(1, conductor.unhandled_commands());

        conductor.do_work();
        assert_eq!(
            1,
            conductor.unhandled_commands(),
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

        // A channel the reference serves and this build does not — a multicast
        // group — is refused rather than left waiting, and with the code the
        // protocol has for exactly that. (A unicast UDP channel used to be this
        // test's example; P1-4 made it a channel this driver *does* serve.)
        send(
            &conductor,
            ADD_PUBLICATION_TYPE_ID,
            &add_publication_payload(7, 10, 1001, "aeron:udp?endpoint=224.0.1.1:40123"),
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
