//! A poll-driven client.
//!
//! A client maps the CnC file, writes commands into the to-driver ring, and
//! reads the driver's replies off the to-clients broadcast. This type is the
//! state for that loop; the caller drives it, which is also how the reference
//! works — `aeron_main_do_work` is pushed by whoever owns the thread, and the
//! agent thread is only one way to push it.
//!
//! That is deliberate for now: [`Client::poll`] is the whole duty cycle, so
//! adding a thread later means calling it from a thread, not rewriting it.
//!
//! # Staying alive
//!
//! The driver allocates a heartbeat counter when it first sees a `client_id`
//! and reaps the client — destroying **every subscription it owns** — once that
//! counter goes stale (`aeron.client.liveness.timeout`, 10 s by default). So
//! [`Client::poll`] refreshes it, and a caller who stops polling loses
//! everything the client created. There is no separate keepalive command to
//! send: the reference writes the counter directly, in
//! `aeron_client_conductor_check_liveness`
//! (`aeron-client/src/main/c/aeron_client_conductor.c:1305-1375`), and that is
//! what this does.
//!
//! # One response per poll, and why that is load-bearing
//!
//! [`Client::poll`] takes at most one message off the broadcast per call. That
//! is not a simplification: the driver sends `ON_SUBSCRIPTION_READY` and then,
//! if a matching publication already exists, `ON_AVAILABLE_IMAGE` for it. The
//! image is matched against a subscription this type only registers *after* the
//! ready response is seen, so draining both in one call would drop the image.

use std::ffi::OsStr;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use deepmsg_cnc::command::{
    ADD_COUNTER_TYPE_ID, ADD_DESTINATION_TYPE_ID, ADD_EXCLUSIVE_PUBLICATION_TYPE_ID,
    ADD_PUBLICATION_TYPE_ID, ADD_RECEIVE_DESTINATION_TYPE_ID, ADD_STATIC_COUNTER_TYPE_ID,
    ADD_SUBSCRIPTION_TYPE_ID, AddCounter, AddPublication, AddSubscription, Correlated,
    DestinationByIdCommand, DestinationCommand, GET_NEXT_AVAILABLE_SESSION_ID_TYPE_ID,
    GetNextAvailableSessionId, REJECT_IMAGE_TYPE_ID, REMOVE_COUNTER_TYPE_ID,
    REMOVE_DESTINATION_BY_ID_TYPE_ID, REMOVE_DESTINATION_TYPE_ID,
    REMOVE_RECEIVE_DESTINATION_TYPE_ID, RemoveCounter, Response, decode_response,
    encode_add_static_counter, encode_reject_image,
};
use deepmsg_cnc::counters::{CLIENT_HEARTBEAT_TYPE_ID, CountersReader};
use deepmsg_cnc::layout::NULL_VALUE;
use deepmsg_cnc::{ClaimError, CncFile, CncOpenError, Received, ToClientsReceiver};

use crate::counter::{Counter, CounterEvent, StaticCounter};
use crate::fragment_assembler::Message;
use crate::image::{Fragment, Image};
use crate::publication::{ExclusivePublication, Publication};
use crate::publication_error::PublicationErrorEvent;
use crate::subscription::Subscription;

/// How long to wait between polls while a command is outstanding.
///
/// The reference's own test helper yields in a tight loop and the conductor
/// sleeps 16 ms between retries; this is the latter, which does not spin a
/// core.
pub const POLL_INTERVAL: Duration = Duration::from_millis(16);

/// The wait between the first polls of [`Client::wait`], doubling up to
/// [`POLL_INTERVAL`]: short enough that a fast reply is not paid for with a
/// frame of latency, and the same 16 ms ceiling as before for one that is slow.
const MIN_POLL_INTERVAL: Duration = Duration::from_micros(50);

/// Default deadline for a command's reply, matching
/// `AERON_CONTEXT_DRIVER_TIMEOUT_MS_DEFAULT`
/// (`aeron-client/src/main/c/aeron_context.c:35`).
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

/// How long [`Client::connect`] waits for a driver that is starting.
///
/// The same ten seconds as [`DEFAULT_TIMEOUT`], and the same constant in the
/// reference: the client's `driver_timeout_ms` is what bounds its wait for the
/// file, the version and the heartbeat at connect
/// (`aeron-client/src/main/c/aeron_context.c:35`, used at `aeronc.c:74`).
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Default fragment budget for one poll, matching
/// `AERON_IMAGE_FRAGMENT_LIMIT_DEFAULT`.
pub const FRAGMENT_LIMIT: usize = 10;

/// Why a client could not connect.
#[derive(Debug)]
pub enum ConnectError {
    /// The CnC file could not be opened read-write, or describes no usable
    /// layout.
    Cnc(CncOpenError),
    /// The to-clients region cannot be read as a broadcast ring.
    NoEventRing,
    /// The to-driver region cannot be written as a command ring.
    NoCommandRing,
}

impl std::fmt::Display for ConnectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Cnc(error) => write!(f, "could not open the CnC file: {error}"),
            Self::NoEventRing => f.write_str("the to-clients region is not a readable ring"),
            Self::NoCommandRing => f.write_str("the to-driver region is not a writable ring"),
        }
    }
}

impl std::error::Error for ConnectError {}

/// Why a command did not complete.
#[derive(Debug)]
pub enum CommandError {
    /// The ring had no room, or refused the message.
    Claim(ClaimError),
    /// The command could not be encoded, which the caller caused by passing
    /// something the format cannot express.
    Encoding,
    /// No reply arrived before the deadline.
    ///
    /// The reference leaves the driver-side resource in place when this
    /// happens — it sets a timed-out flag and sends nothing — so a subscription
    /// can exist that the client believes failed. It is recorded here rather
    /// than reproduced silently: the error carries the correlation id, which is
    /// the subscription's registration id, so a caller *can* clean up.
    TimedOut {
        /// The id the request used, and the subscription's registration id if
        /// the driver did create it.
        correlation_id: i64,
        /// How many times this client has resynchronised past a lap of the
        /// to-clients ring, which is one place a reply can be lost.
        laps: u64,
        /// And how many messages those laps cost.
        discarded: u64,
    },
    /// The client stopped being able to work — see [`ClientError`]. Every
    /// command still waiting failed with it, because there is nothing left to
    /// wait for.
    Terminated(ClientError),
    /// The driver refused the command.
    Driver {
        /// The reference's error code.
        code: i32,
        /// Its description, lossily decoded.
        message: String,
    },
    /// The driver accepted the command but the log buffer it named could not be
    /// mapped.
    ///
    /// A *local* failure, distinct from the driver refusing: the driver has
    /// created the resource, so the caller owns something it cannot use. In
    /// practice this means the path did not exist or held no usable metadata.
    LogBuffer {
        /// The path the driver sent.
        path: PathBuf,
        /// Why the mapping failed.
        source: io::Error,
    },
}

impl std::fmt::Display for CommandError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Claim(error) => write!(f, "the command ring refused the command: {error:?}"),
            Self::Encoding => f.write_str("the command does not fit its own encoding"),
            Self::TimedOut {
                correlation_id,
                laps,
                discarded,
            } => {
                write!(
                    f,
                    "no reply for correlation id {correlation_id}; the driver may still have \
                     created the resource"
                )?;

                // A lap of the to-clients ring is a *plausible* reason a reply
                // never arrived, and it is reported as one rather than left as
                // a mystery: the reference turns a lap into a client-level
                // error instead, and this build records the difference
                // (`docs/compat.md`).
                if *laps > 0 {
                    write!(
                        f,
                        " (this client has resynchronised past the ring {laps} time(s), \
                         losing {discarded} message(s) with them, which may be where the \
                         reply went)"
                    )?;
                }

                Ok(())
            }
            Self::Driver { code, message } => write!(f, "the driver refused it: {code} {message}"),
            Self::LogBuffer { path, source } => {
                write!(
                    f,
                    "the log buffer {} could not be mapped: {source}",
                    path.display()
                )
            }
            Self::Terminated(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for CommandError {}

/// Why a client stopped being able to work.
///
/// The reference reports these through an error handler and then force-closes
/// everything the client holds (`aeron_client_conductor_check_liveness`,
/// `aeron-client/src/main/c/aeron_client_conductor.c:1305-1394`). A `Client`
/// that has one of these has no driver behind it any more, and every call that
/// would wait for one says so instead of waiting for a deadline.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClientError {
    /// The driver stopped on purpose: it wrote the null sentinel into the ring
    /// as it shut down (`aeron-driver/src/main/c/aeron_driver_conductor.c:3493`).
    DriverShutdown,
    /// The driver has not refreshed the ring's heartbeat for longer than the
    /// driver timeout, so it is gone rather than slow — killed, or a machine
    /// that lost power. The reference's `MediaDriver keepalive` error.
    DriverTimeout {
        /// How old the heartbeat was, in milliseconds.
        age_ms: i64,
        /// The timeout it was measured against.
        timeout_ms: i64,
    },
    /// The heartbeat counter this client writes is no longer its own: the
    /// driver went away and another one took the directory, or this client was
    /// reaped. Writing into it would keep somebody else alive.
    HeartbeatCounterClosed,
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DriverShutdown => f.write_str("the media driver has been shut down"),
            Self::DriverTimeout { age_ms, timeout_ms } => write!(
                f,
                "the media driver's heartbeat is {age_ms}ms old, past the {timeout_ms}ms timeout"
            ),
            Self::HeartbeatCounterClosed => {
                f.write_str("the heartbeat counter was closed by somebody else")
            }
        }
    }
}

impl std::error::Error for ClientError {}

/// How stale the driver's heartbeat may be before this client treats it as
/// gone: `AERON_DRIVER_TIMEOUT_MS_DEFAULT (10 * 1000)`
/// (`aeron-driver/src/main/c/aeron_driver_context.c:217`), which is what the
/// reference's client context uses too (`aeron.driver.timeout`).
pub const DRIVER_TIMEOUT_MS: i64 = 10 * 1000;

/// What a completed command handed back.
#[derive(Debug)]
enum Ready {
    /// A subscription exists.
    Subscription { channel_status_indicator_id: i32 },
    /// A publication exists, and its log buffer can now be mapped.
    Publication {
        registration_id: i64,
        session_id: i32,
        stream_id: i32,
        position_limit_counter_id: i32,
        channel_status_indicator_id: i32,
        log_file: PathBuf,
    },
    /// The same, for a publication with one producer
    /// (`ON_EXCLUSIVE_PUBLICATION_READY`, whose payload is the concurrent
    /// one's — `aeron_driver_conductor_on_exclusive_publication_ready` sends
    /// the same fields under a different id).
    ///
    /// A separate arm rather than a flag on the one above: the two are
    /// different handles, and a client that could not tell them apart would
    /// have to guess which `add_*` call it was answering.
    ExclusivePublication {
        registration_id: i64,
        session_id: i32,
        stream_id: i32,
        position_limit_counter_id: i32,
        channel_status_indicator_id: i32,
        log_file: PathBuf,
    },
    /// A counter exists, and its slot in the values region is the handle.
    Counter { counter_id: i32 },
    /// The driver answered what session id to publish under.
    NextSessionId { next_session_id: i32 },
    /// The driver allocated a counter it owns, at this client's request.
    StaticCounter { counter_id: i32 },
    /// The command's work is done, and there is nothing to hand back — a
    /// removal's acknowledgement.
    OperationSucceeded,
}

/// A pending command, waiting for the response that completes it.
struct Pending {
    correlation_id: i64,
    deadline: Instant,
    outcome: Option<Result<Ready, CommandError>>,
}

/// A client connected to a running driver.
pub struct Client {
    cnc: CncFile,
    /// Written into every command. The driver creates a client record on first
    /// sight and never validates it — there is no registration handshake in
    /// this protocol — but it must be *stable*, because the heartbeat counter
    /// the driver allocates is keyed on it.
    client_id: i64,
    receiver: ToClientsReceiver,
    pending: Vec<Pending>,
    subscriptions: Vec<Subscription>,
    publications: Vec<Publication>,
    exclusive_publications: Vec<ExclusivePublication>,
    /// The counters this client asked for. The driver reclaims them with the
    /// client when it goes, so this list is the client's whole counter life.
    counters: Vec<Counter>,
    /// Counter announcements read off the broadcast and not yet drained:
    /// every counter that appeared or went away, whoever owns it.
    counter_events: Vec<CounterEvent>,
    /// The counters this client asked the driver to allocate **for the
    /// driver** — kept apart from `counters` on purpose, because everything
    /// that walks `counters` is about counters this client owns and can give
    /// back, and none of it is true of these.
    static_counters: Vec<StaticCounter>,
    /// Publications the driver reported as failed and nobody has drained yet.
    /// Unlike the counter events these are about resources this client owns,
    /// which is why they are kept rather than dropped: a publisher that never
    /// looks loses the only notice it gets.
    publication_errors: Vec<PublicationErrorEvent>,
    /// Found lazily: the driver allocates it when it first sees `client_id`,
    /// which is during the first command, so it may not exist yet.
    heartbeat_counter: Option<i32>,
    unknown_responses: u64,
    /// Images that arrived for a subscription this client did not have yet.
    orphan_images: u64,
    /// Set once when this client discovers the driver is gone, and never
    /// cleared: everything it would do afterwards is work for a driver that is
    /// not there.
    terminated: Option<ClientError>,
}

impl Client {
    /// Connect to the driver owning `aeron_dir`.
    ///
    /// # Errors
    ///
    /// [`ConnectError`] if the CnC file cannot be opened read-write or either
    /// ring is unusable.
    pub fn connect(aeron_dir: &Path) -> Result<Self, ConnectError> {
        Self::connect_with_timeout(aeron_dir, CONNECT_TIMEOUT)
    }

    /// Connect, waiting at most `timeout` for a driver that is starting.
    ///
    /// The window is a setting in the reference — `driver_timeout_ms`,
    /// `AERON_DRIVER_TIMEOUT`, ten seconds by default — and this is the same
    /// knob.
    ///
    /// # Errors
    ///
    /// As [`Client::connect`], including the timeout expiring.
    pub fn connect_with_timeout(aeron_dir: &Path, timeout: Duration) -> Result<Self, ConnectError> {
        // A client that arrives while the driver is starting has to wait for
        // it, not fail: the CnC file is created first and published a moment
        // later, and for a 46 MB file that moment is not small. The reference
        // waits out exactly this window, in four steps — file, mapping,
        // version, heartbeat — for up to `driver_timeout_ms`
        // (`aeron_client_connect_to_driver`, `aeronc.c:70-125`).
        //
        // What this does *not* wait for is a heartbeat: a driver that published
        // a version and then stopped is one this client connects to and then
        // notices, which is P0's behaviour and the client conductor's job
        // (`crates/client/src/conductor.rs`), not the connect path's.
        let cnc = CncFile::open_writable(aeron_dir, timeout).map_err(ConnectError::Cnc)?;

        // The client id comes from the ring's shared counter, exactly as the
        // reference's does. Two consecutive values are not needed here — that
        // is the terminate path's habit — but one is, and this is where the
        // reference takes it from.
        let client_id = {
            let ring = cnc.to_driver_ring().ok_or(ConnectError::NoCommandRing)?;
            ring.next_correlation_id().unwrap_or(0)
        };

        let event_region = cnc.to_clients_region().ok_or(ConnectError::NoEventRing)?;
        let receiver = ToClientsReceiver::new(&event_region).ok_or(ConnectError::NoEventRing)?;

        // The window is rebuilt on each poll from the mapping, so the receiver
        // holds no borrow of it — which is what lets this type own both.
        let _ = event_region;

        Ok(Self {
            cnc,
            client_id,
            receiver,
            pending: Vec::new(),
            subscriptions: Vec::new(),
            publications: Vec::new(),
            exclusive_publications: Vec::new(),
            counters: Vec::new(),
            counter_events: Vec::new(),
            static_counters: Vec::new(),
            publication_errors: Vec::new(),
            heartbeat_counter: None,
            unknown_responses: 0,
            orphan_images: 0,
            terminated: None,
        })
    }

    /// This client's id, as the driver knows it.
    pub const fn client_id(&self) -> i64 {
        self.client_id
    }

    /// Responses that arrived for something this client does not model.
    ///
    /// The to-clients ring is a single broadcast, so a client sees every
    /// response to every other client too. ADR-0003 says skip and count; this
    /// is the count, and it is deliberately observable rather than silent.
    pub const fn unknown_responses(&self) -> u64 {
        self.unknown_responses
    }

    /// Images that named a subscription this client does not have.
    ///
    /// Not an error and not necessarily a bug: the driver can link a
    /// subscription and send the image in the same breath as the ready
    /// response, and a client that has not yet registered the subscription —
    /// or that has since dropped it — has nowhere to put it.
    pub const fn orphan_images(&self) -> u64 {
        self.orphan_images
    }

    /// Why this client stopped being able to work, if it has.
    ///
    /// Once this is set, every command that waits for a reply fails with it
    /// rather than waiting out its deadline, and nothing is written to the
    /// driver any more — the reference's client does the same, and calls it
    /// being terminating (`aeron_client_conductor.c:1305-1336`).
    pub const fn error(&self) -> Option<ClientError> {
        self.terminated
    }

    /// Whether this client has given up on its driver.
    pub const fn is_terminated(&self) -> bool {
        self.terminated.is_some()
    }

    /// How many times this client has resynchronised past a lap of the
    /// to-clients ring.
    ///
    /// A lap means the driver wrote more than a ring's worth of events while
    /// this client was not reading, and whatever was in the overwritten span is
    /// gone — possibly this client's own reply. The reference treats it as a
    /// system error and force-closes the client
    /// (`aeron_client_conductor.c:2728-2734`); this build resynchronises and
    /// counts, and the count is here so that a caller can assert it and find it
    /// in a timeout's message (`docs/compat.md` records the divergence).
    pub const fn laps(&self) -> u64 {
        self.receiver.lapped()
    }

    /// Messages discarded because the driver overwrote them mid-read.
    pub const fn discarded(&self) -> u64 {
        self.receiver.discarded()
    }

    /// The subscriptions this client holds.
    pub fn subscriptions(&self) -> &[Subscription] {
        &self.subscriptions
    }

    /// The publications this client holds.
    pub fn publications(&self) -> &[Publication] {
        &self.publications
    }

    /// A subscription by registration id.
    pub fn subscription(&self, registration_id: i64) -> Option<&Subscription> {
        self.subscriptions
            .iter()
            .find(|subscription| subscription.registration_id() == registration_id)
    }

    /// A publication by registration id.
    pub fn publication(&self, registration_id: i64) -> Option<&Publication> {
        self.publications
            .iter()
            .find(|publication| publication.registration_id() == registration_id)
    }

    /// The counters this client holds.
    pub fn counters(&self) -> &[Counter] {
        &self.counters
    }

    /// A counter by registration id.
    pub fn counter(&self, registration_id: i64) -> Option<&Counter> {
        self.counters
            .iter()
            .find(|counter| counter.registration_id() == registration_id)
    }

    /// The counters reader over this client's own mapping of the CnC file.
    ///
    /// The same reader the reference hands its applications, for the whole
    /// life of the client (`aeron_counters_reader`,
    /// `aeron-client/src/main/c/aeron_client.c:276-286`) — every counter in
    /// the file, not only this client's, because the values region is one
    /// shared namespace every process on the host reads the same way.
    ///
    /// `None` when the file describes no counter regions, which a driver that
    /// is running does not produce.
    pub fn counters_reader(&self) -> Option<CountersReader<'_>> {
        self.cnc.counters()
    }

    /// Counter announcements since the last drain, and take them.
    ///
    /// Every counter that appeared or went away on the broadcast, whoever
    /// owns it — see [`CounterEvent`] for what the reference does with these
    /// and how this shape differs. Draining is the delivery: an announcement
    /// left in the queue is one nobody has been told about yet, so a caller
    /// that wants the watchers kept current drains every duty cycle.
    ///
    /// Nothing bounds the queue. A caller that never drains it keeps every
    /// announcement it has been sent, so a client that polls without reading
    /// grows by one entry per counter event on the host — which is a
    /// consequence of the shape rather than of a bug, and the reason the
    /// method is named for the drain it performs.
    pub fn counter_events(&mut self) -> Vec<CounterEvent> {
        std::mem::take(&mut self.counter_events)
    }

    /// Publications the driver has reported as failed, and nothing else — the
    /// events that have arrived since the last call.
    ///
    /// Each names a publication this client holds, so a caller can match one
    /// against its own list by
    /// [`PublicationErrorEvent::registration_id`]. The queue is **not** cleared
    /// when the driver goes: an error nobody has read is news that outlives the
    /// connection, where a released publication does not.
    pub fn publication_errors(&mut self) -> Vec<PublicationErrorEvent> {
        std::mem::take(&mut self.publication_errors)
    }

    /// Run one duty cycle: refresh the heartbeat, then take at most one
    /// response.
    ///
    /// Returns whether anything happened, matching the reference's `work_count`
    /// so a caller can drive an idle strategy from it.
    pub fn poll(&mut self) -> bool {
        let keepalive = self.check_liveness();

        let mut worked = false;
        if let Some(region) = self.cnc.to_clients_region() {
            if let Received::Message { type_id } = self.receiver.receive(&region) {
                // The payload is a scratch copy, valid only until the next
                // receive, so it is copied out before anything else touches the
                // receiver — and the response is decoded from the copy, which
                // is also where a log path's bytes come from.
                let payload = self.receiver.message().to_vec();
                self.handle(type_id, &payload);
                worked = true;
            }
        }

        self.expire_pending();

        worked || keepalive
    }

    /// Add a subscription and wait for the driver to confirm it.
    ///
    /// Returns the subscription's registration id, which is the correlation id
    /// the request used — the driver echoes it in the ready response and uses
    /// it as the registration id thereafter.
    ///
    /// The subscription is registered with this client only once the ready
    /// response has been seen, so a caller must keep polling afterwards for
    /// `ON_AVAILABLE_IMAGE` to be attached — see [`Client::poll`].
    ///
    /// # Errors
    ///
    /// [`CommandError`] if the command could not be sent, the driver refused
    /// it, or no reply arrived within `timeout`.
    pub fn add_subscription(
        &mut self,
        channel: &str,
        stream_id: i32,
        timeout: Duration,
    ) -> Result<i64, CommandError> {
        let correlation_id = self.next_correlation_id()?;

        let command = AddSubscription {
            client_id: self.client_id,
            correlation_id,
            // A field the driver never reads; the reference sends -1
            // (`aeron_client_conductor.c:2000-2004` writes the other four
            // fields and leaves these eight as whatever the ring held).
            registration_correlation_id: -1,
            stream_id,
            channel,
        };

        let mut payload = vec![0u8; command.encoded_length()];
        if !command.encode_into(&mut payload) {
            return Err(CommandError::Encoding);
        }

        self.send(ADD_SUBSCRIPTION_TYPE_ID, &payload, correlation_id, timeout)?;

        let ready = self.wait(correlation_id)?;
        let Ready::Subscription {
            channel_status_indicator_id,
        } = ready
        else {
            return Err(CommandError::Encoding);
        };

        self.subscriptions.push(Subscription::new(
            correlation_id,
            channel.to_owned(),
            stream_id,
            channel_status_indicator_id,
        ));

        Ok(correlation_id)
    }

    /// Add a publication and wait for the driver to confirm it.
    ///
    /// Returns the publication's registration id — the correlation id the
    /// request used, and the id the driver names the log file after
    /// (`aeron_fileutil.c:1208-1214`). The log buffer is mapped before this
    /// returns, so [`Client::offer`] is usable immediately.
    ///
    /// # Errors
    ///
    /// [`CommandError`] if the command could not be sent, the driver refused
    /// it, no reply arrived within `timeout`, or the log buffer it named could
    /// not be mapped.
    pub fn add_publication(
        &mut self,
        channel: &str,
        stream_id: i32,
        timeout: Duration,
    ) -> Result<i64, CommandError> {
        let correlation_id = self.next_correlation_id()?;

        let command = AddPublication {
            client_id: self.client_id,
            correlation_id,
            stream_id,
            channel,
        };

        let mut payload = vec![0u8; command.encoded_length()];
        if !command.encode_into(&mut payload) {
            return Err(CommandError::Encoding);
        }

        self.send(ADD_PUBLICATION_TYPE_ID, &payload, correlation_id, timeout)?;

        let ready = self.wait(correlation_id)?;
        let Ready::Publication {
            registration_id,
            session_id,
            stream_id,
            position_limit_counter_id,
            channel_status_indicator_id,
            log_file,
        } = ready
        else {
            return Err(CommandError::Encoding);
        };

        let publication = Publication::open(
            &log_file,
            registration_id,
            session_id,
            stream_id,
            position_limit_counter_id,
            channel_status_indicator_id,
        )
        .map_err(|source| CommandError::LogBuffer {
            path: log_file,
            source,
        })?;

        self.publications.push(publication);

        Ok(registration_id)
    }

    /// Add a publication only this client will write to, and wait for the
    /// driver to make it.
    ///
    /// `aeron_async_add_exclusive_publication` (`aeronc.c:473-497`). The
    /// command is the concurrent one's under a different id, and so is the
    /// reply — what differs is which handle comes back and, from then on, that
    /// the append never claims.
    ///
    /// # Errors
    ///
    /// [`CommandError`] for a command that could not be sent or a driver that
    /// refused it.
    pub fn add_exclusive_publication(
        &mut self,
        channel: &str,
        stream_id: i32,
        timeout: Duration,
    ) -> Result<i64, CommandError> {
        let correlation_id = self.next_correlation_id()?;

        let command = AddPublication {
            client_id: self.client_id,
            correlation_id,
            stream_id,
            channel,
        };

        let mut payload = vec![0u8; command.encoded_length()];
        if !command.encode_into(&mut payload) {
            return Err(CommandError::Encoding);
        }

        self.send(
            ADD_EXCLUSIVE_PUBLICATION_TYPE_ID,
            &payload,
            correlation_id,
            timeout,
        )?;

        let ready = self.wait(correlation_id)?;
        let Ready::ExclusivePublication {
            registration_id,
            session_id,
            stream_id,
            position_limit_counter_id,
            channel_status_indicator_id,
            log_file,
        } = ready
        else {
            return Err(CommandError::Encoding);
        };

        let publication = ExclusivePublication::open(
            &log_file,
            registration_id,
            session_id,
            stream_id,
            position_limit_counter_id,
            channel_status_indicator_id,
        )
        .map_err(|source| CommandError::LogBuffer {
            path: log_file,
            source,
        })?;

        self.exclusive_publications.push(publication);

        Ok(registration_id)
    }

    /// Offer to an exclusive publication.
    ///
    /// # Errors
    ///
    /// [`None`] when no such publication is held; see [`Appended`] for the
    /// rest.
    pub fn offer_exclusive(
        &self,
        registration_id: i64,
        payload: &[u8],
    ) -> Option<deepmsg_core::logbuffer::append::Appended> {
        let publication = self.exclusive_publication(registration_id)?;

        let limit = self
            .cnc
            .counters()
            .and_then(|counters| counters.value(publication.position_limit_counter_id()))
            .unwrap_or(0);

        Some(publication.offer(limit, payload))
    }

    /// An exclusive publication this client holds.
    pub fn exclusive_publication(&self, registration_id: i64) -> Option<&ExclusivePublication> {
        self.exclusive_publications
            .iter()
            .find(|publication| publication.registration_id() == registration_id)
    }

    /// Add a counter and wait for the driver to allocate it.
    ///
    /// The type id is the contract a reader keys on — it says how to read the
    /// key — and the key and label are free-form. The value starts at zero
    /// and is this client's to write through the CnC file; nothing about it
    /// ever reaches the driver (`aeron_async_add_counter`,
    /// `aeron-client/src/main/c/aeronc.c:534-561`, and the waiting is this
    /// crate's shape rather than the reference's poll triple).
    ///
    /// Returns the handle, which names the slot in the values region; the
    /// client keeps it too, and reclaims it with this client when the driver
    /// reaps the client.
    ///
    /// # Errors
    ///
    /// [`CommandError`] if the command could not be sent, the driver refused
    /// it, or no reply arrived within `timeout`.
    pub fn add_counter(
        &mut self,
        type_id: i32,
        key: &[u8],
        label: &str,
        timeout: Duration,
    ) -> Result<Counter, CommandError> {
        let correlation_id = self.next_correlation_id()?;

        let command = AddCounter {
            correlated: Correlated {
                client_id: self.client_id,
                correlation_id,
            },
            type_id,
            key,
            label: label.as_bytes(),
        };

        let mut payload = vec![0u8; command.encoded_length()];
        if !command.encode_into(&mut payload) {
            return Err(CommandError::Encoding);
        }

        self.send(ADD_COUNTER_TYPE_ID, &payload, correlation_id, timeout)?;

        let Ready::Counter { counter_id } = self.wait(correlation_id)? else {
            return Err(CommandError::Encoding);
        };

        // One command, one registration: the id the driver allocated under is
        // the correlation id this request used, which is what `ON_COUNTER_READY`
        // echoed (`aeron_client_conductor.c:850-895`).
        let counter = Counter::new(correlation_id, correlation_id, counter_id);
        self.counters.push(counter);

        Ok(counter)
    }

    /// Remove a counter and wait for the driver to acknowledge.
    ///
    /// The acknowledgement is the whole reply — `ON_UNAVAILABLE_COUNTER`
    /// follows it on the broadcast as an event for whoever was watching, and
    /// is not part of this command's answer
    /// (`aeron_driver_conductor.c:6221-6235`).
    ///
    /// # Errors
    ///
    /// [`CommandError`] if the command could not be sent, the driver refused
    /// it — a counter another client owns, or one already gone — or no reply
    /// arrived within `timeout`.
    pub fn remove_counter(
        &mut self,
        counter: &Counter,
        timeout: Duration,
    ) -> Result<(), CommandError> {
        let correlation_id = self.next_correlation_id()?;

        let command = RemoveCounter {
            correlated: Correlated {
                client_id: self.client_id,
                correlation_id,
            },
            registration_id: counter.registration_id(),
        };

        let mut payload = vec![0u8; command.encoded_length()];
        if !command.encode_into(&mut payload) {
            return Err(CommandError::Encoding);
        }

        self.send(REMOVE_COUNTER_TYPE_ID, &payload, correlation_id, timeout)?;

        let Ready::OperationSucceeded = self.wait(correlation_id)? else {
            return Err(CommandError::Encoding);
        };

        self.counters
            .retain(|held| held.registration_id() != counter.registration_id());

        Ok(())
    }

    /// Add a destination to a publication and wait for the driver to confirm
    /// it.
    ///
    /// Returns the destination's **registration id**, and that is the
    /// correlation id this request used. There is no separate allocation for a
    /// destination in the reference either: its client completes a registering
    /// resource when the reply's correlation id matches the id it is waiting on
    /// (`aeron_client_conductor.c:975-991`), and
    /// `aeron_async_destination_get_registration_id` documents its return value
    /// as "correlation_id sent to driver" (`aeronc.h:2697-2705`).
    ///
    /// The URI is **not** parsed here. The reference's client only null-checks
    /// it (`aeron_client.c:635-652`); what a destination URI may be is the
    /// driver's question (`aeron_driver_conductor_validate_send_destination_uri`,
    /// `aeron_driver_conductor.c:5369-5410`). A bad URI therefore comes back as
    /// the driver's own `ON_ERROR`, in the driver's words, rather than as a
    /// second refusal worded here that no other client would recognise.
    ///
    /// # Errors
    ///
    /// [`CommandError`] if the command could not be sent, the driver refused
    /// it, or no reply arrived within `timeout`.
    pub fn add_destination(
        &mut self,
        publication_id: i64,
        uri: &str,
        timeout: Duration,
    ) -> Result<i64, CommandError> {
        self.destination_command(ADD_DESTINATION_TYPE_ID, publication_id, uri, timeout)
    }

    /// Remove a destination from a publication by its URI, and wait for the
    /// driver to confirm it.
    ///
    /// # Errors
    ///
    /// [`CommandError`] as [`Client::add_destination`], including for a URI the
    /// publication does not have a destination on.
    pub fn remove_destination(
        &mut self,
        publication_id: i64,
        uri: &str,
        timeout: Duration,
    ) -> Result<(), CommandError> {
        self.destination_command(REMOVE_DESTINATION_TYPE_ID, publication_id, uri, timeout)
            .map(|_| ())
    }

    /// Remove a publication's destination by the id [`Client::add_destination`]
    /// returned, and wait for the driver to confirm it.
    ///
    /// # Errors
    ///
    /// [`CommandError`] — but read
    /// [`CommandError::TimedOut`] before treating one as a transport failure.
    /// This is the one command in the family whose failures the reference
    /// answers **nothing** to: `REMOVE_DESTINATION_BY_ID` calls its handler
    /// without taking the result
    /// (`aeron_driver_conductor.c:3188-3200`), so the error the handler returns
    /// for a publication it cannot find (`:5562-5578`) never reaches the
    /// `result < 0` that would send an `ON_ERROR`. Every other destination
    /// command assigns it (`:3020`, `:3035`, `:3055-3062`, `:3083-3090`). A
    /// caller that passes an unknown publication waits for an answer that is
    /// not coming, and the reference's own clients do the same — so a timeout
    /// here is the reference's behaviour, not a broken connection.
    pub fn remove_destination_by_id(
        &mut self,
        publication_id: i64,
        destination_id: i64,
        timeout: Duration,
    ) -> Result<(), CommandError> {
        let correlation_id = self.next_correlation_id()?;

        let command = DestinationByIdCommand {
            client_id: self.client_id,
            correlation_id,
            resource_registration_id: publication_id,
            destination_registration_id: destination_id,
        };

        let mut payload = vec![0u8; DestinationByIdCommand::ENCODED_LENGTH];
        if !command.encode_into(&mut payload) {
            return Err(CommandError::Encoding);
        }

        self.send(
            REMOVE_DESTINATION_BY_ID_TYPE_ID,
            &payload,
            correlation_id,
            timeout,
        )?;

        let Ready::OperationSucceeded = self.wait(correlation_id)? else {
            return Err(CommandError::Encoding);
        };

        Ok(())
    }

    /// Add a destination to a subscription — a source it will also accept the
    /// stream from — and wait for the driver to confirm it.
    ///
    /// Returns the destination's registration id, as
    /// [`Client::add_destination`] does, and for the same reason.
    ///
    /// # Errors
    ///
    /// [`CommandError`] if the command could not be sent, the driver refused
    /// it, or no reply arrived within `timeout`.
    pub fn add_rcv_destination(
        &mut self,
        subscription_id: i64,
        uri: &str,
        timeout: Duration,
    ) -> Result<i64, CommandError> {
        self.destination_command(
            ADD_RECEIVE_DESTINATION_TYPE_ID,
            subscription_id,
            uri,
            timeout,
        )
    }

    /// Remove a source from a subscription, and wait for the driver to confirm
    /// it.
    ///
    /// # Errors
    ///
    /// [`CommandError`] as [`Client::add_rcv_destination`].
    pub fn remove_rcv_destination(
        &mut self,
        subscription_id: i64,
        uri: &str,
        timeout: Duration,
    ) -> Result<(), CommandError> {
        self.destination_command(
            REMOVE_RECEIVE_DESTINATION_TYPE_ID,
            subscription_id,
            uri,
            timeout,
        )
        .map(|_| ())
    }

    /// Write one of the four destination commands that carry a URI, and wait
    /// for its answer.
    ///
    /// All four answer with the same eight-byte `aeron_operation_succeeded_t` —
    /// a bare correlation id (`aeron_control_protocol.h:117-121`) — so one
    /// helper serves them, and the caller decides what the id means. Which of
    /// the four was sent lives in the record's type id and nowhere in the
    /// payload, which is why `type_id` is a parameter here and not a field of
    /// the record.
    ///
    /// `registration_id` is the publication or subscription the destination
    /// belongs to; the driver tells the two apart by the type id.
    fn destination_command(
        &mut self,
        type_id: i32,
        registration_id: i64,
        uri: &str,
        timeout: Duration,
    ) -> Result<i64, CommandError> {
        let correlation_id = self.next_correlation_id()?;

        let command = DestinationCommand {
            client_id: self.client_id,
            correlation_id,
            registration_id,
            channel: uri,
        };

        let mut payload = vec![0u8; command.encoded_length()];
        if !command.encode_into(&mut payload) {
            return Err(CommandError::Encoding);
        }

        self.send(type_id, &payload, correlation_id, timeout)?;

        let Ready::OperationSucceeded = self.wait(correlation_id)? else {
            return Err(CommandError::Encoding);
        };

        Ok(correlation_id)
    }

    /// Give a publication back to the driver
    /// (`aeron_publication_close`, `aeron_publication.c:91-112`, through
    /// `aeron_client_conductor_async_close_publication`).
    ///
    /// The publication goes away on this call: the driver answers when the link
    /// is gone, and until that answer arrives the publication is still this
    /// client's. A caller that wants the stream cut off with a message to its
    /// readers wants [`Self::revoke_publication`] — the difference is one flag
    /// (`REMOVE_PUBLICATION_FLAG_REVOKE`), and everything the flag sets off.
    ///
    /// # Errors
    ///
    /// [`CommandError`] for a driver that fell silent, or one that answers with
    /// something other than an acknowledgement.
    pub fn remove_publication(
        &mut self,
        registration_id: i64,
        timeout: Duration,
    ) -> Result<(), CommandError> {
        self.remove_publication_with_flags(registration_id, 0, timeout)
    }

    /// Cut a publication's stream off and give it back
    /// (`Publication.revokeOnClose` / `Publication.revoke`, which is the same
    /// command with `REMOVE_PUBLICATION_FLAG_REVOKE`).
    ///
    /// Every reader of the stream is told: their images are revoked, drained,
    /// and reported unavailable with `Image::is_publication_revoked` true.
    ///
    /// # Errors
    ///
    /// As [`Self::remove_publication`].
    pub fn revoke_publication(
        &mut self,
        registration_id: i64,
        timeout: Duration,
    ) -> Result<(), CommandError> {
        self.remove_publication_with_flags(
            registration_id,
            deepmsg_cnc::command::REMOVE_PUBLICATION_FLAG_REVOKE,
            timeout,
        )
    }

    fn remove_publication_with_flags(
        &mut self,
        registration_id: i64,
        flags: i64,
        timeout: Duration,
    ) -> Result<(), CommandError> {
        let correlation_id = self.next_correlation_id()?;

        let command = deepmsg_cnc::command::RemovePublication {
            correlated: deepmsg_cnc::command::Correlated {
                client_id: self.client_id,
                correlation_id,
            },
            registration_id,
            flags,
        };

        let mut payload = vec![0u8; deepmsg_cnc::command::RemovePublication::encoded_length()];
        if !command.encode_into(&mut payload) {
            return Err(CommandError::Encoding);
        }

        self.send(
            deepmsg_cnc::command::REMOVE_PUBLICATION_TYPE_ID,
            &payload,
            correlation_id,
            timeout,
        )?;

        let Ready::OperationSucceeded = self.wait(correlation_id)? else {
            return Err(CommandError::Encoding);
        };

        self.forget_publication(registration_id);

        Ok(())
    }

    /// Give a subscription back to the driver
    /// (`aeron_subscription_close`, through
    /// `aeron_client_conductor_async_close_subscription`).
    ///
    /// The driver answers when its link is gone; every image it held goes with
    /// it, and a receiver that has no readers left stops feeding the stream.
    ///
    /// # Errors
    ///
    /// As [`Self::remove_publication`].
    pub fn remove_subscription(
        &mut self,
        registration_id: i64,
        timeout: Duration,
    ) -> Result<(), CommandError> {
        let correlation_id = self.next_correlation_id()?;

        let command = deepmsg_cnc::command::RemoveSubscription {
            correlated: deepmsg_cnc::command::Correlated {
                client_id: self.client_id,
                correlation_id,
            },
            registration_id,
        };

        let mut payload = vec![0u8; deepmsg_cnc::command::RemoveSubscription::encoded_length()];
        if !command.encode_into(&mut payload) {
            return Err(CommandError::Encoding);
        }

        self.send(
            deepmsg_cnc::command::REMOVE_SUBSCRIPTION_TYPE_ID,
            &payload,
            correlation_id,
            timeout,
        )?;

        let Ready::OperationSucceeded = self.wait(correlation_id)? else {
            return Err(CommandError::Encoding);
        };

        self.forget_subscription(registration_id);

        Ok(())
    }

    /// Forget a publication without telling the driver
    /// (`Publication.forceClose`).
    ///
    /// The link the driver holds is left behind on purpose: it is what a client
    /// does when it is about to die, and the cost of it is the driver holding a
    /// resource until the client's own liveness timeout reaps it. Returns
    /// whether there was one to forget.
    pub fn force_remove_publication(&mut self, registration_id: i64) -> bool {
        self.forget_publication(registration_id)
    }

    /// The same for a subscription.
    pub fn force_remove_subscription(&mut self, registration_id: i64) -> bool {
        self.forget_subscription(registration_id)
    }

    /// Drop a publication from this client's list, and close it.
    fn forget_publication(&mut self, registration_id: i64) -> bool {
        let Some(index) = self
            .publications
            .iter()
            .position(|publication| publication.registration_id() == registration_id)
        else {
            return false;
        };

        // Dropped rather than closed in place: this client hands resources
        // out **borrowed**, so a caller that still held one would not be able
        // to call this at all — and the observable that says a publication is
        // closed is that [`Self::publication`] no longer answers with it.
        let _ = self.publications.swap_remove(index);

        true
    }

    /// The same for a subscription.
    fn forget_subscription(&mut self, registration_id: i64) -> bool {
        let Some(index) = self
            .subscriptions
            .iter()
            .position(|subscription| subscription.registration_id() == registration_id)
        else {
            return false;
        };

        let _ = self.subscriptions.swap_remove(index);

        true
    }

    /// Ask the driver to allocate a counter it will keep, and wait for it.
    ///
    /// The counter belongs to the **driver**, not to this client: it is
    /// allocated with an owner id of `NULL_VALUE` and is not added to the list
    /// this client's counters are in, so it survives this client and is never
    /// announced as unavailable (`aeron_driver_conductor.c:6300-6312`). That is
    /// what makes it useful — a value two processes want to watch without
    /// either of them owning it — and it is why the handle that comes back is a
    /// [`StaticCounter`] rather than a [`Counter`]: there is no removal to make.
    ///
    /// Asking twice for the same `(type_id, registration_id)` answers with the
    /// counter that already exists, which is how two processes agree on one.
    /// An id and type id that already name a counter **somebody owns** is
    /// refused instead, because a static counter may not take a live counter's
    /// place.
    ///
    /// # Errors
    ///
    /// [`CommandError`] if the command could not be sent, the driver refused it
    /// — the owner conflict above, or no room for the counter — or no answer
    /// arrived within `timeout`.
    pub fn add_static_counter(
        &mut self,
        type_id: i32,
        registration_id: i64,
        key: &[u8],
        label: &[u8],
        timeout: Duration,
    ) -> Result<StaticCounter, CommandError> {
        let correlation_id = self.next_correlation_id()?;

        let payload = encode_add_static_counter(
            self.client_id,
            correlation_id,
            registration_id,
            type_id,
            key,
            label,
        );

        self.send(
            ADD_STATIC_COUNTER_TYPE_ID,
            &payload,
            correlation_id,
            timeout,
        )?;

        let Ready::StaticCounter { counter_id } = self.wait(correlation_id)? else {
            return Err(CommandError::Encoding);
        };

        let counter = StaticCounter::new(registration_id, counter_id);
        self.static_counters.push(counter);

        Ok(counter)
    }

    /// The counters this client asked the driver for, which the driver owns.
    pub fn static_counters(&self) -> &[StaticCounter] {
        &self.static_counters
    }

    /// Ask the driver what session id to publish under on `stream_id`, and
    /// wait for the answer.
    ///
    /// The reference's clients block on this the same way
    /// (`ClientConductor.nextSessionId`, `ClientConductor.java:501-527`, which
    /// waits for the response before returning it), and so does this one.
    ///
    /// The id is a **hint**, not a reservation. The driver moves its cursor
    /// past whatever it answers, so two callers — and two calls — get different
    /// ids; but nothing reserves what it hands out, so an id can go unused for
    /// ever and a client may publish under one it made up. What is guaranteed
    /// is narrower and is the whole point: no publication the driver **already
    /// holds** uses that session id on that stream, which is what makes the
    /// answer worth asking for when a client is about to create one.
    ///
    /// A client can do that itself — the id space is 2^32 and a collision needs
    /// the same stream — which is why the reference falls back to a random id
    /// when the driver is too old to serve the command
    /// (`ClientConductor.java:523-526`). Asking is how a *cluster* gets its
    /// session ids to line up across nodes, and how a client avoids retrying a
    /// create that clashed.
    ///
    /// # Errors
    ///
    /// [`CommandError`] if the command could not be sent, or no answer arrived
    /// within `timeout`.
    pub fn next_session_id(
        &mut self,
        stream_id: i32,
        timeout: Duration,
    ) -> Result<i32, CommandError> {
        let correlation_id = self.next_correlation_id()?;

        let command = GetNextAvailableSessionId {
            correlated: Correlated {
                client_id: self.client_id,
                correlation_id,
            },
            stream_id,
        };

        let mut payload = vec![0u8; command.encoded_length()];
        if !command.encode_into(&mut payload) {
            return Err(CommandError::Encoding);
        }

        self.send(
            GET_NEXT_AVAILABLE_SESSION_ID_TYPE_ID,
            &payload,
            correlation_id,
            timeout,
        )?;

        let Ready::NextSessionId { next_session_id } = self.wait(correlation_id)? else {
            return Err(CommandError::Encoding);
        };

        Ok(next_session_id)
    }

    /// Reject an image this client is reading, and wait for the driver to
    /// acknowledge the command.
    ///
    /// `image_registration_id` is the **publication's** registration id, which
    /// is what [`crate::image::Image`] carries and what an `ON_AVAILABLE_IMAGE`
    /// named; `position` is how far this reader had got, which only travels so
    /// that the far end's frame says where the stream was cut off.
    ///
    /// What this starts is a chain that ends somewhere else: the driver refuses
    /// the image, its receiver sends an `ERR` frame to the publisher, and the
    /// **publisher's** client is handed an
    /// [`PublicationErrorEvent`](crate::publication_error::PublicationErrorEvent)
    /// with code 13. The acknowledgement this waits for says only that the
    /// driver acted on the command — the reference's own clients do not wait at
    /// all, which is why a caller that wants the outcome wants the other
    /// client's queue.
    ///
    /// # Errors
    ///
    /// [`CommandError`] if the command could not be sent, the driver refused it
    /// — an id that names no image and no IPC publication is answered with
    /// `GENERIC_ERROR` — or no reply arrived within `timeout`.
    pub fn reject_image(
        &mut self,
        image_registration_id: i64,
        position: i64,
        reason: &str,
        timeout: Duration,
    ) -> Result<(), CommandError> {
        let correlation_id = self.next_correlation_id()?;

        let payload = encode_reject_image(
            self.client_id,
            correlation_id,
            image_registration_id,
            position,
            reason.as_bytes(),
        );

        self.send(REJECT_IMAGE_TYPE_ID, &payload, correlation_id, timeout)?;

        let Ready::OperationSucceeded = self.wait(correlation_id)? else {
            return Err(CommandError::Encoding);
        };

        Ok(())
    }

    /// Offer a payload on a publication.
    ///
    /// Returns `None` if there is no such publication. Otherwise the typed
    /// outcome — see [`deepmsg_core::logbuffer::append::Appended`], whose
    /// `EndOfLog` and `MidRotation` both mean "retry", not that anything
    /// failed: one is a rotation that happened, the other a rotation happening
    /// right now.
    ///
    /// The window limit is read from the counter the driver maintains, here and
    /// per call, because that counter is the only thing standing between a
    /// producer and overwriting data a subscriber has not read. Reading it once
    /// and caching it is how a producer silently outruns its consumer.
    pub fn offer(
        &self,
        registration_id: i64,
        payload: &[u8],
    ) -> Option<deepmsg_core::logbuffer::append::Appended> {
        let publication = self.publication(registration_id)?;

        // The limit counter is in the CnC file; the log buffer is the
        // publication's own mapping. Disjoint, so both borrows are shared.
        let limit = self
            .cnc
            .counters()
            .and_then(|counters| counters.value(publication.position_limit_counter_id()))
            .unwrap_or(0);

        Some(publication.offer(limit, payload))
    }

    /// Read up to `fragment_limit` fragments from one image.
    ///
    /// Returns how many were delivered, or `None` if there is no such
    /// subscription or image.
    ///
    /// The image's reader position is published to its counter **after** the
    /// handler has returned, never before — and that publication is not
    /// optional. The driver computes the publisher's window limit from the
    /// minimum of these counters (`aeron_ipc_publication.c:296-313`), so an
    /// image that is read but not reported eventually blocks the publisher.
    pub fn poll_image<F>(
        &mut self,
        subscription_id: i64,
        image_registration_id: i64,
        fragment_limit: usize,
        handler: F,
    ) -> Option<usize>
    where
        F: FnMut(&Fragment<'_>),
    {
        let (counter_id, position, read) = {
            let subscription = self
                .subscriptions
                .iter_mut()
                .find(|s| s.registration_id() == subscription_id)?;
            let image = subscription
                .images_mut()
                .iter_mut()
                .find(|i| i.registration_id() == image_registration_id)?;

            let read = image.poll(fragment_limit, handler);

            (image.subscriber_position_id(), image.position(), read)
        };

        if let Some(counters) = self.cnc.counters_writable() {
            counters.set_value(counter_id, position);
        }

        Some(read)
    }

    /// Read up to `fragment_limit` fragments from every image of a
    /// subscription, and deliver **whole messages**.
    ///
    /// This is the subscription's default delivery, and the reference's: a
    /// client that polls a subscription reads messages, and the frames they
    /// arrived in are an implementation detail of the transport
    /// (`aeron_subscription_poll` with a fragment assembler, which is what
    /// every reference sample does). The fragments are reassembled per session
    /// by the subscription's own assembler, so a message split across two
    /// polls — or across two images — is still delivered once, whole.
    ///
    /// [`Client::poll_image`] is the fragment-level view, for a caller that
    /// wants the frames themselves. Mixing the two on one subscription is legal
    /// but worth thinking about: the fragments the raw poll takes are fragments
    /// the assembler never sees, and a message missing a piece is a message
    /// abandoned.
    ///
    /// Returns how many messages were delivered.
    pub fn poll_subscription<F>(
        &mut self,
        subscription_id: i64,
        fragment_limit: usize,
        mut handler: F,
    ) -> usize
    where
        F: FnMut(Message<'_>),
    {
        let Some(subscription) = self
            .subscriptions
            .iter_mut()
            .find(|s| s.registration_id() == subscription_id)
        else {
            return 0;
        };

        let (messages, counter_writes) = subscription.poll_messages(fragment_limit, &mut handler);

        // Published after the poll rather than during it: the counters live in
        // the CnC file, which the images do not borrow, and the reference
        // publishes a reader's position for the driver to compute the
        // publisher's window from — an image that is read but not reported
        // eventually blocks the publisher (`aeron_ipc_publication.c:296-313`).
        if let Some(counters) = self.cnc.counters_writable() {
            for (counter_id, position) in counter_writes {
                counters.set_value(counter_id, position);
            }
        }

        messages
    }

    /// Send a command and register it as pending with a deadline.
    fn send(
        &mut self,
        type_id: i32,
        payload: &[u8],
        correlation_id: i64,
        timeout: Duration,
    ) -> Result<(), CommandError> {
        {
            let ring = self
                .cnc
                .to_driver_ring()
                .ok_or(CommandError::Claim(ClaimError::Invalid))?;
            ring.write(type_id, payload).map_err(CommandError::Claim)?;
        }

        self.pending.push(Pending {
            correlation_id,
            deadline: Instant::now() + timeout,
            outcome: None,
        });

        Ok(())
    }

    /// Drive the loop until `correlation_id` completes or its deadline passes.
    ///
    /// This is what the reference's own test helper does
    /// (`aeron_test_base.h:116-131`) and what the C++ wrapper's blocking
    /// `addSubscription` is.
    fn wait(&mut self, correlation_id: i64) -> Result<Ready, CommandError> {
        // The first wait is short and doubles to [`POLL_INTERVAL`]. A driver
        // answers in well under a millisecond — its command tier runs every
        // pass — so a fixed frame-length sleep after every poll made each
        // command cost the caller a frame of its latency budget. A reply that
        // is genuinely late still backs off to the same cadence as before.
        let mut wait = MIN_POLL_INTERVAL;

        loop {
            self.poll();

            // Ahead of the lookup below, and before the deadline: when the
            // client has given up, the commands it was waiting on are gone
            // *because* of that, and reporting them as timed out would bury the
            // reason.
            if let Some(error) = self.terminated {
                return Err(CommandError::Terminated(error));
            }

            let Some(index) = self
                .pending
                .iter()
                .position(|p| p.correlation_id == correlation_id)
            else {
                // Vanished, which only `expire_pending` does, and it records
                // the reason.
                return Err(CommandError::TimedOut {
                    correlation_id,
                    laps: self.receiver.lapped(),
                    discarded: self.receiver.discarded(),
                });
            };

            if let Some(outcome) = self.pending[index].outcome.take() {
                self.pending.swap_remove(index);
                return outcome;
            }

            if Instant::now() >= self.pending[index].deadline {
                self.pending.swap_remove(index);
                return Err(CommandError::TimedOut {
                    correlation_id,
                    laps: self.receiver.lapped(),
                    discarded: self.receiver.discarded(),
                });
            }

            std::thread::sleep(wait);
            wait = (wait * 2).min(POLL_INTERVAL);
        }
    }

    /// Refresh this client's heartbeat counter, if the driver has made one yet.
    fn check_liveness(&mut self) -> bool {
        if self.terminated.is_some() {
            return false;
        }

        let now = now_ms();
        let heartbeat = self
            .cnc
            .to_driver_ring()
            .and_then(|ring| ring.consumer_heartbeat());

        // The reference's two failure branches, in its order, and the order is
        // what makes them distinguishable: a driver that *stopped* leaves the
        // null sentinel behind (`aeron_driver_conductor.c:3493` writes it), and
        // a driver that was killed leaves its last heartbeat — so the first
        // test is exact and the second is a timeout
        // (`aeron_client_conductor.c:1308-1334`).
        match heartbeat {
            Some(NULL_VALUE) => {
                self.terminate(ClientError::DriverShutdown);
                return false;
            }
            Some(last) => {
                let age_ms = now.saturating_sub(last);

                if age_ms > DRIVER_TIMEOUT_MS {
                    self.terminate(ClientError::DriverTimeout {
                        age_ms,
                        timeout_ms: DRIVER_TIMEOUT_MS,
                    });
                    return false;
                }
            }
            None => return false,
        }

        let Some(counters) = self.cnc.counters_writable() else {
            return false;
        };

        let Some(counter_id) = self.heartbeat_counter else {
            // Not there yet is normal: the driver allocates it while handling
            // the first command, so the first poll may run before it exists.
            // Treating that as an error would make a healthy client look
            // broken. The announcement usually adopts it first (in `handle`);
            // this scan is what saves a client that never saw the
            // announcement — a lap, or polls that stopped — and is also the
            // reference's only mechanism, run every liveness check until it
            // hits (`aeron_client_conductor.c:1338-1341`).
            self.heartbeat_counter =
                counters.find_by_type_and_registration(CLIENT_HEARTBEAT_TYPE_ID, self.client_id);

            return false;
        };

        // Re-verified every cycle rather than remembered, because a counter id
        // outlives the client that owned it: the driver reclaims ids and hands
        // them out again, so the id this client cached can be *somebody else's*
        // counter by now — after the driver went away and another took the
        // directory, or after this client was reaped. Writing `now` into it
        // would keep a stranger alive and keep this client looking healthy
        // while it is not (`aeron_client_conductor.c:1338-1390`).
        //
        // Read **by id**, in constant time, as the reference does
        // (`aeron_counter_heartbeat_timestamp_is_active`, `:1234-1249`). Asking
        // the same question by scanning the catalogue costs the whole catalogue
        // — every counter in the file, each one built into a descriptor with
        // its label — and this is on the poll path, which is the loop every
        // client runs: a scan here measured 6.6 us per poll, against 25 ns for
        // the message poll beside it.
        let still_ours = counters.is_active(counter_id, CLIENT_HEARTBEAT_TYPE_ID, self.client_id);

        if !still_ours {
            self.terminate(ClientError::HeartbeatCounterClosed);
            return false;
        }

        counters.set_value(counter_id, now).is_some()
    }

    /// Give up: nothing this client holds can be used any more.
    ///
    /// The reference force-closes every resource it holds, which unmaps every
    /// image it had mapped (`aeron_client_conductor_force_close_resources`,
    /// `:1290-1303`). Dropping them here does the same, and a caller that
    /// reaches for one afterwards finds nothing rather than a mapping of a log
    /// buffer whose driver is gone.
    fn terminate(&mut self, error: ClientError) {
        self.terminated = Some(error);
        self.subscriptions.clear();
        self.publications.clear();
        self.counters.clear();
        self.pending.clear();
    }

    /// Take one decoded response and act on it.
    fn handle(&mut self, type_id: i32, payload: &[u8]) {
        match decode_response(type_id, payload) {
            Response::SubscriptionReady {
                correlation_id,
                channel_status_indicator_id,
            } => {
                self.complete(
                    correlation_id,
                    Ok(Ready::Subscription {
                        channel_status_indicator_id,
                    }),
                );
            }
            Response::PublicationReady {
                correlation_id,
                registration_id,
                session_id,
                stream_id,
                position_limit_counter_id,
                channel_status_indicator_id,
                log_file,
            } => {
                // The decoder answers both ids with this one shape, because the
                // payload is the same; which command it was is the *id*, and it
                // is the only thing that says which handle to build.
                //
                // The handle is the **correlation id**, which is this client's
                // own id for this `ADD_PUBLICATION` and the one the driver
                // matches a removal by — the reference's Java client does the
                // same (`ClientConductor` keeps the correlation id as the
                // registration id and the message's `registration_id` as the
                // publication's own, `AERON_PUBLICATION_BUFFERS_READY`'s two
                // ids, `aeron_control_protocol.h`). Taking the message's field
                // instead would give two publications on one channel the *same*
                // handle, and closing one would close the other's channel.
                let _ = registration_id;
                let log_file = path_from_bytes(log_file);
                let fields = (
                    correlation_id,
                    session_id,
                    stream_id,
                    position_limit_counter_id,
                    channel_status_indicator_id,
                );

                let ready =
                    if type_id == deepmsg_cnc::command::ON_EXCLUSIVE_PUBLICATION_READY_TYPE_ID {
                        Ready::ExclusivePublication {
                            registration_id: fields.0,
                            session_id: fields.1,
                            stream_id: fields.2,
                            position_limit_counter_id: fields.3,
                            channel_status_indicator_id: fields.4,
                            log_file,
                        }
                    } else {
                        Ready::Publication {
                            registration_id: fields.0,
                            session_id: fields.1,
                            stream_id: fields.2,
                            position_limit_counter_id: fields.3,
                            channel_status_indicator_id: fields.4,
                            log_file,
                        }
                    };

                self.complete(correlation_id, Ok(ready));
            }
            Response::AvailableImage {
                publication_registration_id,
                session_id,
                stream_id,
                subscriber_registration_id,
                subscriber_position_id,
                log_file,
                ..
            } => {
                self.attach_image(
                    subscriber_registration_id,
                    publication_registration_id,
                    session_id,
                    stream_id,
                    subscriber_position_id,
                    &path_from_bytes(log_file),
                );
            }
            Response::UnavailableImage {
                publication_registration_id,
                subscription_registration_id,
                ..
            } => {
                if let Some(subscription) = self
                    .subscriptions
                    .iter_mut()
                    .find(|s| s.registration_id() == subscription_registration_id)
                {
                    subscription.remove_image(publication_registration_id);
                }
            }
            // Not an answer to a command: the driver is telling a client that
            // one of its publications has failed. The to-clients ring is a
            // **broadcast**, so every client decodes this and only the one
            // holding that publication is owed anything — which is the
            // reference's match (`ClientConductor.onPublicationError`,
            // `aeron-client/src/main/java/io/aeron/ClientConductor.java:312-326`:
            // it walks its own publications and fires the handler only for the
            // one whose `originalRegistrationId` matches). A client that
            // skipped that would report another client's failure and, worse,
            // tell its `reject_image` caller the rejection had come home.
            //
            // The publication is not closed either: a rejected image comes back
            // after a liveness timeout, so what to do about it is the
            // application's.
            Response::PublicationError {
                registration_id,
                destination_registration_id,
                session_id,
                stream_id,
                receiver_id,
                group_tag,
                source,
                error_code,
                message,
            } => {
                if !self
                    .publications
                    .iter()
                    .any(|publication| publication.registration_id() == registration_id)
                {
                    return;
                }

                self.publication_errors.push(PublicationErrorEvent {
                    registration_id,
                    destination_registration_id,
                    session_id,
                    stream_id,
                    receiver_id,
                    group_tag,
                    source,
                    error_code,
                    message: message.to_vec(),
                });
            }
            // A counter the driver owns rather than this client. It is
            // completed like any other reply — the client asked for it — but
            // what it hands back is deliberately *not* a `Counter`: this
            // client does not own it, nothing will announce it as unavailable
            // when this client goes, and there is no `REMOVE_COUNTER` that
            // could take it away again.
            Response::StaticCounter {
                correlation_id,
                counter_id,
            } => {
                self.complete(correlation_id, Ok(Ready::StaticCounter { counter_id }));
            }
            // The answer to `GET_NEXT_AVAILABLE_SESSION_ID`, matched by its
            // correlation id like any other reply. The driver broadcasts it to
            // every client, so this client's own `next_session_id` call is the
            // only thing that finds a pending entry for it.
            Response::NextAvailableSessionId {
                correlation_id,
                next_session_id,
            } => {
                self.complete(correlation_id, Ok(Ready::NextSessionId { next_session_id }));
            }
            Response::Error {
                offending_command_correlation_id,
                error_code,
                message,
            } => {
                self.complete(
                    offending_command_correlation_id,
                    Err(CommandError::Driver {
                        code: error_code,
                        message: String::from_utf8_lossy(message).into_owned(),
                    }),
                );
            }
            // The driver's reply to an `ADD_COUNTER` this client sent, matched
            // by the echoed correlation id — the same match the reference's
            // `on_counter_ready` makes against its awaiting resources before
            // it fires the watchers' handlers unconditionally, for every
            // client's counters (`aeron_client_conductor.c:850-895`). The
            // messages that match nothing pending are broadcasts — another
            // client's counter, or this client's own heartbeat — and are not
            // a reply to any command, so they are neither completed nor
            // counted as unknown.
            Response::CounterReady {
                correlation_id,
                counter_id,
            } => {
                self.counter_events.push(CounterEvent::Ready {
                    correlation_id,
                    counter_id,
                });

                // The heartbeat's correlation is the client id, and no command
                // can ever carry that id again: client ids and correlation ids
                // come from the one ring counter, and this client's id was
                // drawn at connect, so the value is spent and will not be
                // handed out as a correlation. The reference instead finds its
                // heartbeat by scanning the file on the liveness path
                // (`aeron_client_conductor.c:1338-1341`); adopting it here
                // saves that scan in the normal case, and the scan remains
                // below for a client that never saw the announcement.
                if self.heartbeat_counter.is_none() && correlation_id == self.client_id {
                    self.heartbeat_counter = Some(counter_id);
                }

                if self
                    .pending
                    .iter()
                    .any(|pending| pending.correlation_id == correlation_id)
                {
                    self.complete(correlation_id, Ok(Ready::Counter { counter_id }));
                }
            }
            // A counter went away. The reference's handler for this fires the
            // unavailable callbacks and does nothing else — no lookup, no
            // close (`aeron_client_conductor.c:898-906`) — because a handle
            // is only a name and a reclaimed slot already answers every
            // question about itself with "gone". So does this: the event is
            // the whole of the client's side.
            Response::CounterUnavailable {
                correlation_id,
                counter_id,
            } => {
                self.counter_events.push(CounterEvent::Unavailable {
                    correlation_id,
                    counter_id,
                });
            }
            Response::OperationSucceeded { correlation_id } => {
                self.complete(correlation_id, Ok(Ready::OperationSucceeded));
            }
            // Someone else's client was reaped. Every client sees it.
            Response::ClientTimeout { .. } => {}
            // Not modelled, or too short to trust. Counted, never fatal —
            // ADR-0003 — and a deliberate divergence from the reference, whose
            // default error handler exits.
            Response::Other { .. } => self.unknown_responses += 1,
        }
    }

    /// Map an image's log buffer and attach it to its subscription.
    fn attach_image(
        &mut self,
        subscription_id: i64,
        publication_registration_id: i64,
        session_id: i32,
        stream_id: i32,
        subscriber_position_id: i32,
        path: &Path,
    ) {
        // Where to start reading. The driver writes the join position into the
        // counter as part of linking the subscription
        // (`aeron_driver_conductor.c:3547-3575`), and it does so *before*
        // sending this message — so the counter, not the message, is the
        // authority on where this subscriber starts.
        let join_position = self
            .cnc
            .counters()
            .and_then(|counters| counters.value(subscriber_position_id))
            .unwrap_or(0);

        let image = match Image::open(
            path,
            publication_registration_id,
            session_id,
            stream_id,
            subscriber_position_id,
            join_position,
        ) {
            Ok(image) => image,
            Err(_) => {
                self.orphan_images += 1;
                return;
            }
        };

        let Some(subscription) = self
            .subscriptions
            .iter_mut()
            .find(|s| s.registration_id() == subscription_id)
        else {
            self.orphan_images += 1;
            return;
        };

        subscription.add_image(image);
    }

    fn complete(&mut self, correlation_id: i64, outcome: Result<Ready, CommandError>) {
        if let Some(pending) = self
            .pending
            .iter_mut()
            .find(|p| p.correlation_id == correlation_id)
        {
            pending.outcome = Some(outcome);
        } else {
            // A response to a command this client never sent, or one whose
            // deadline already passed. The reference drops both silently, and
            // so does this — the id is not a key to anything that still exists.
            self.unknown_responses += 1;
        }
    }

    /// Drop pending commands whose deadline has passed, recording why.
    ///
    /// A command that times out here might not have been ignored: it might have
    /// been *lost*, and one of the ways a reply is lost is a lap of the
    /// to-clients ring (`crate::Client::laps`). The counts travel with the
    /// error so that a caller who reports a timeout can say which it was.
    fn expire_pending(&mut self) {
        let now = Instant::now();
        let laps = self.receiver.lapped();
        let discarded = self.receiver.discarded();

        for pending in &mut self.pending {
            if pending.outcome.is_none() && now >= pending.deadline {
                pending.outcome = Some(Err(CommandError::TimedOut {
                    correlation_id: pending.correlation_id,
                    laps,
                    discarded,
                }));
            }
        }
    }

    fn next_correlation_id(&mut self) -> Result<i64, CommandError> {
        let ring = self
            .cnc
            .to_driver_ring()
            .ok_or(CommandError::Claim(ClaimError::Invalid))?;
        ring.next_correlation_id()
            .ok_or(CommandError::Claim(ClaimError::Invalid))
    }
}

/// A path from the raw bytes the driver sent.
///
/// Lossy conversion would corrupt a path that is not valid UTF-8, and the
/// driver sends whatever it was given; on Linux a path *is* bytes, so take them
/// as they are.
fn path_from_bytes(bytes: &[u8]) -> PathBuf {
    PathBuf::from(OsStr::from_bytes(bytes))
}

fn now_ms() -> i64 {
    let elapsed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();

    #[allow(clippy::cast_possible_truncation)]
    let millis = elapsed.as_millis() as i64;
    millis
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("client_id", &self.client_id)
            .field("pending", &self.pending.len())
            .field("subscriptions", &self.subscriptions.len())
            .field("publications", &self.publications.len())
            .field("counters", &self.counters.len())
            .field("counter_events", &self.counter_events.len())
            .field("unknown_responses", &self.unknown_responses)
            .finish()
    }
}
