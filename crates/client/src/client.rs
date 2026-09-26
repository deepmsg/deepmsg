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
//! send: the reference writes the counter directly
//! (`aeron-client/src/main/c/aeron_client_conductor.c:1345-1387`), and that is
//! what this does.

use std::path::Path;
use std::time::{Duration, Instant};

use deepmsg_cnc::command::{ADD_SUBSCRIPTION_TYPE_ID, AddSubscription, Response, decode_response};
use deepmsg_cnc::counters::CLIENT_HEARTBEAT_TYPE_ID;
use deepmsg_cnc::{ClaimError, CncFile, CncOpenError, Received, ToClientsReceiver};

/// How long to wait between polls while a command is outstanding.
///
/// The reference's own test helper yields in a tight loop and the conductor
/// sleeps 16 ms between retries; this is the latter, which does not spin a
/// core.
pub const POLL_INTERVAL: Duration = Duration::from_millis(16);

/// Default deadline for a command's reply, matching
/// `AERON_CONTEXT_DRIVER_TIMEOUT_MS_DEFAULT`
/// (`aeron-client/src/main/c/aeron_context.c:35`).
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

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
    },
    /// The driver refused the command.
    Driver {
        /// The reference's error code.
        code: i32,
        /// Its description, lossily decoded.
        message: String,
    },
}

impl std::fmt::Display for CommandError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Claim(error) => write!(f, "the command ring refused the command: {error:?}"),
            Self::Encoding => f.write_str("the command does not fit its own encoding"),
            Self::TimedOut { correlation_id } => write!(
                f,
                "no reply for correlation id {correlation_id}; the driver may still have \
                 created the resource"
            ),
            Self::Driver { code, message } => write!(f, "the driver refused it: {code} {message}"),
        }
    }
}

impl std::error::Error for CommandError {}

/// A pending command, waiting for the response that completes it.
struct Pending {
    correlation_id: i64,
    deadline: Instant,
    outcome: Option<Result<SubscriptionReady, CommandError>>,
}

/// The fields of `ON_SUBSCRIPTION_READY` this client uses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SubscriptionReady {
    channel_status_indicator_id: i32,
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
    /// Found lazily: the driver allocates it when it first sees `client_id`,
    /// which is during the first command, so it may not exist yet.
    heartbeat_counter: Option<i32>,
    unknown_responses: u64,
}

impl Client {
    /// Connect to the driver owning `aeron_dir`.
    ///
    /// # Errors
    ///
    /// [`ConnectError`] if the CnC file cannot be opened read-write or either
    /// ring is unusable.
    pub fn connect(aeron_dir: &Path) -> Result<Self, ConnectError> {
        let cnc = CncFile::try_open_writable(aeron_dir).map_err(ConnectError::Cnc)?;

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
            heartbeat_counter: None,
            unknown_responses: 0,
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

    /// Messages discarded because the driver overwrote them mid-read.
    pub const fn discarded(&self) -> u64 {
        self.receiver.discarded()
    }

    /// Run one duty cycle: refresh the heartbeat, then take at most one
    /// response.
    ///
    /// Returns whether anything happened, matching the reference's `work_count`
    /// so a caller can drive an idle strategy from it.
    pub fn poll(&mut self) -> bool {
        let keepalive = self.keepalive();

        let mut worked = false;
        if let Some(region) = self.cnc.to_clients_region() {
            if let Received::Message { type_id } = self.receiver.receive(&region) {
                // The payload is a scratch copy, valid only until the next
                // receive, so it is consumed here rather than handed out.
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
            // A field the driver never reads; the reference sends -1.
            registration_correlation_id: -1,
            stream_id,
            channel,
        };

        let mut payload = vec![0u8; command.encoded_length()];
        if !command.encode_into(&mut payload) {
            return Err(CommandError::Encoding);
        }

        {
            let ring = self
                .cnc
                .to_driver_ring()
                .ok_or(CommandError::Claim(ClaimError::Invalid))?;
            ring.write(ADD_SUBSCRIPTION_TYPE_ID, &payload)
                .map_err(CommandError::Claim)?;
        }

        self.pending.push(Pending {
            correlation_id,
            deadline: Instant::now() + timeout,
            outcome: None,
        });

        // Drive the loop the caller would otherwise have to write. This is what
        // the reference's own test helper does (aeron_test_base.h:116-131) and
        // what the C++ wrapper's blocking addSubscription is.
        loop {
            self.poll();

            let Some(index) = self
                .pending
                .iter()
                .position(|p| p.correlation_id == correlation_id)
            else {
                // Vanished, which only `expire_pending` does, and it records
                // the reason.
                return Err(CommandError::TimedOut { correlation_id });
            };

            if let Some(outcome) = self.pending[index].outcome.take() {
                self.pending.swap_remove(index);
                return outcome.map(|_ready| correlation_id);
            }

            if Instant::now() >= self.pending[index].deadline {
                self.pending.swap_remove(index);
                return Err(CommandError::TimedOut { correlation_id });
            }

            std::thread::sleep(POLL_INTERVAL);
        }
    }

    /// Refresh this client's heartbeat counter, if the driver has made one yet.
    fn keepalive(&mut self) -> bool {
        let Some(counters) = self.cnc.counters_writable() else {
            return false;
        };

        if self.heartbeat_counter.is_none() {
            // Not there yet is normal: the driver allocates it while handling
            // the first command, so the first poll may run before it exists.
            // Treating that as an error would make a healthy client look broken.
            self.heartbeat_counter =
                counters.find_by_type_and_registration(CLIENT_HEARTBEAT_TYPE_ID, self.client_id);
        }

        let Some(counter_id) = self.heartbeat_counter else {
            return false;
        };

        counters.set_value(counter_id, now_ms()).is_some()
    }

    /// Take one decoded response and match it to a pending command.
    fn handle(&mut self, type_id: i32, payload: &[u8]) {
        match decode_response(type_id, payload) {
            Response::SubscriptionReady {
                correlation_id,
                channel_status_indicator_id,
            } => {
                self.complete(
                    correlation_id,
                    Ok(SubscriptionReady {
                        channel_status_indicator_id,
                    }),
                );
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
            // A fresh client's *first* response is its own heartbeat counter,
            // with the client id in the correlation field — not a reply to
            // anything. Also how a later `ADD_COUNTER` would arrive.
            Response::CounterReady { .. } => {}
            // Someone else's client was reaped. Every client sees it.
            Response::ClientTimeout { .. } => {}
            // Not modelled, or too short to trust. Counted, never fatal —
            // ADR-0003 — and a deliberate divergence from the reference, whose
            // default error handler exits.
            Response::Other { .. } => self.unknown_responses += 1,
        }
    }

    fn complete(&mut self, correlation_id: i64, outcome: Result<SubscriptionReady, CommandError>) {
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
    fn expire_pending(&mut self) {
        let now = Instant::now();
        for pending in &mut self.pending {
            if pending.outcome.is_none() && now >= pending.deadline {
                pending.outcome = Some(Err(CommandError::TimedOut {
                    correlation_id: pending.correlation_id,
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
            .field("unknown_responses", &self.unknown_responses)
            .finish()
    }
}
