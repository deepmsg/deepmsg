//! The conductor: the duty cycle that owns the CnC file.
//!
//! One call to [`Conductor::do_work`] is one pass of the reference's loop
//! (`aeron-driver/src/main/c/aeron_driver_conductor.c:3369-3407`), which runs
//! at two rates on purpose:
//!
//! | tier | every | what it does here |
//! |---|---|---|
//! | timeout | `timer_interval_ns`, 1 s by default | publish the driver's liveness |
//! | commands | every pass | drain the to-driver ring |
//!
//! The drain limit is **one** command per pass
//! (`AERON_COMMAND_DRAIN_LIMIT`, `aeron-driver/src/main/c/aeron_driver_context.h:53`).
//! That is not a tuning knob: it is the control plane's latency floor, because
//! a pass is also where every other thing that has to happen this cycle
//! happens, and batching commands would let a burst push the timer tier out.
//!
//! # What this conductor does not do yet
//!
//! The reference's timeout tier checks seven pools of managed resources —
//! clients, publications, images, endpoints, lingering resources — and
//! reclaims the ones that have reached end of life
//! (`aeron_driver_conductor.c:1692-1709`). P1-0 has no resources to check, so
//! what the tier does is the one thing a *client* depends on: it refreshes the
//! to-driver ring's consumer heartbeat, which is the only liveness signal this
//! protocol has.
//!
//! It also means a command this build does not implement yet gets no reply.
//! The reference would answer `ON_ERROR`, but responses go on the to-clients
//! broadcast and there is no transmitter in this build; the client that sent
//! the command will reach its own deadline and report a timeout, which is the
//! same shape as the reference's own timeout leak (`docs/protocol/cnc-layout.md`,
//! "A leak in the reference"). Unimplemented and unknown commands are counted
//! rather than logged per command, so a driver under a client it cannot serve
//! is noisy once at shutdown instead of once per command.

use deepmsg_cnc::layout;
use deepmsg_cnc::{CncCreateError, CncFile, ToDriverRingConsumer};
use deepmsg_core::clock::{self, CachedClock};

use crate::config::{DriverConfig, TerminationPolicy};

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
    /// The ready version could not be stored.
    Publish(CncCreateError),
    /// The to-driver region is not a ring this build can consume. Validation
    /// accepts only lengths that describe one, so reaching this means the file
    /// was not created by this build.
    NoCommandRing,
}

impl std::fmt::Display for ConductorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Publish(error) => write!(f, "the CnC file could not be published: {error}"),
            Self::NoCommandRing => f.write_str("the to-driver region is not a command ring"),
        }
    }
}

impl std::error::Error for ConductorError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Publish(error) => Some(error),
            Self::NoCommandRing => None,
        }
    }
}

/// The driver's control plane.
pub struct Conductor {
    cnc: CncFile,
    commands: ToDriverRingConsumer,
    termination: TerminationPolicy,
    timer_interval_ns: i64,
    clock: CachedClock,
    clock_update_deadline_ns: i64,
    timeout_check_deadline_ns: i64,
    now_ms: i64,
    running: bool,
    unhandled: u64,
    unknown: u64,
    last_unhandled: Option<Command>,
}

impl Conductor {
    /// Take over a freshly created CnC file and publish it as ready.
    ///
    /// The order here is the reference's, and it is the reason
    /// [`CncFile::publish`] is a separate step: burn a correlation id
    /// (`aeron-driver/src/main/c/aeron_driver.c:970` — which is why the first
    /// client sees id 1 rather than 0), publish the driver's liveness on the
    /// to-driver ring (`:971`), and only then store the ready version
    /// (`:972`) and flush it (`:973`).
    ///
    /// # Errors
    ///
    /// [`ConductorError`] if the file cannot be published or does not carry a
    /// usable command ring.
    pub fn new(cnc: CncFile, config: &DriverConfig) -> Result<Self, ConductorError> {
        let commands = {
            let region = cnc
                .to_driver_region()
                .ok_or(ConductorError::NoCommandRing)?;
            ToDriverRingConsumer::new(&region.as_read_only())
                .ok_or(ConductorError::NoCommandRing)?
        };

        // A producer's view, taken for one call and dropped: the id the driver
        // burns at startup belongs to the same counter a client takes its
        // client id from.
        if let Some(ring) = cnc.to_driver_ring() {
            let _ = ring.next_correlation_id();
        }

        let now_ns = clock::epoch_nano_time();
        let mut clock = CachedClock::new();
        let now_ms = clock.update(now_ns);

        let mut conductor = Self {
            cnc,
            commands,
            termination: config.termination,
            timer_interval_ns: config.timer_interval_ns,
            clock,
            clock_update_deadline_ns: now_ns + CLOCK_UPDATE_INTERVAL_NS,
            // Seeded to now, so the first pass runs the timeout tier: the
            // reference does the same (`aeron_driver_conductor.c:824`), and it
            // means the heartbeat is set before anything can read the version.
            timeout_check_deadline_ns: now_ns,
            now_ms,
            running: true,
            unhandled: 0,
            unknown: 0,
            last_unhandled: None,
        };

        conductor.write_heartbeat();
        conductor.cnc.publish().map_err(ConductorError::Publish)?;

        Ok(conductor)
    }

    /// One duty cycle. The return value is the reference's `work_count`.
    pub fn do_work(&mut self) -> usize {
        let now_ns = clock::epoch_nano_time();
        let mut work_count = 0;

        if now_ns > self.clock_update_deadline_ns {
            self.now_ms = self.clock.update(now_ns);
            self.clock_update_deadline_ns = now_ns + CLOCK_UPDATE_INTERVAL_NS;
        }

        if now_ns > self.timeout_check_deadline_ns {
            self.write_heartbeat();
            self.timeout_check_deadline_ns = now_ns + self.timer_interval_ns;
            work_count += 1;
        }

        work_count + self.process_commands()
    }

    /// Whether the driver should keep running.
    pub const fn is_running(&self) -> bool {
        self.running
    }

    /// Commands that are in the protocol and not implemented here yet.
    pub const fn unhandled_commands(&self) -> u64 {
        self.unhandled
    }

    /// Commands whose type id the protocol does not define.
    pub const fn unknown_commands(&self) -> u64 {
        self.unknown
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
    /// flushed at `:3494`). Everything else the reference tears down here —
    /// counters, the error log, the managed resources — has nothing to close
    /// yet.
    ///
    /// # Errors
    ///
    /// The error from flushing the mapping, if any.
    pub fn close(&mut self) -> std::io::Result<()> {
        self.write_heartbeat_value(layout::NULL_VALUE);
        self.cnc.sync()
    }

    /// Drain at most [`COMMAND_DRAIN_LIMIT`] commands and act on them.
    fn process_commands(&mut self) -> usize {
        // Borrows split by field rather than through `&mut self`, because the
        // read takes a window from the file while the handler writes state.
        let cnc = &self.cnc;
        let commands = &mut self.commands;
        let running = &mut self.running;
        let termination = self.termination;
        let unhandled = &mut self.unhandled;
        let unknown = &mut self.unknown;
        let last_unhandled = &mut self.last_unhandled;

        let Some(region) = cnc.to_driver_region() else {
            return 0;
        };

        commands.read(&region, COMMAND_DRAIN_LIMIT, |type_id, _payload| {
            match Command::from_type_id(type_id) {
                Command::TerminateDriver => {
                    if TerminationPolicy::Allow == termination {
                        *running = false;
                    }
                }
                // Both are no-ops for a client this driver has not seen, and
                // in P1-0 it has seen none: the reference's keepalive looks the
                // client up and does nothing when it is missing
                // (`aeron_driver_conductor.c:5269-5279`), and so does its close
                // (`:6321-6331`). They are handled, not unimplemented.
                Command::ClientKeepalive | Command::ClientClose => {}
                command => {
                    if matches!(command, Command::Unknown(_)) {
                        *unknown += 1;
                    } else {
                        *unhandled += 1;
                    }
                    *last_unhandled = Some(command);
                }
            }
        })
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

#[cfg(test)]
mod tests {
    use super::*;
    use deepmsg_cnc::create::COUNTERS_VALUES_BUFFER_LENGTH_MIN;
    use deepmsg_cnc::layout::NULL_VALUE;
    use deepmsg_cnc::{CncIdentity, CncLayout, TerminateDriver};
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
        send(&conductor, 0x04, b"first");
        send(&conductor, 0x04, b"second");

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

        send(&conductor, 0x04, b"aeron:ipc|1"); // ADD_SUBSCRIPTION
        conductor.do_work();

        assert_eq!(1, conductor.unhandled_commands());
        assert_eq!(0, conductor.unknown_commands());
        assert_eq!(Some(Command::AddSubscription), conductor.last_unhandled());
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

        send(&conductor, 0x04, b"channel");
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
}
