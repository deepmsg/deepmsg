//! The rig's own logic, against the reference's own tests.
//!
//! `benchmarks-api/src/test/java/io/aeron/benchmarks/LoadTestRigTest.java` is
//! where "the cadence was ported correctly" is decided, and three of its cases
//! decide it: they pin the *sequence* of sends, the exact timestamps those sends
//! carry, and how many times the clock was read to get there. Everything else in
//! the port can be argued about; those three cannot, which is why they are here
//! rather than left to a number that looks about right.
//!
//! The reference uses Mockito for four things — a spy transceiver, a scripted
//! clock, a counted idle strategy and a recording progress reporter, plus an
//! `InOrder` over all of them. This does the same with hand-written doubles that
//! share one event log, which is what makes `InOrder`'s job a single comparison
//! rather than four sets of call counts.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::io::{self, Write};
use std::rc::Rc;

use deepmsg_bench::loadtest::config::{
    Builder, Configuration, IdleStrategy, TimeUnit, Transceiver,
};
use deepmsg_bench::loadtest::progress::ProgressReporter;
use deepmsg_bench::loadtest::recorder::Recorder;
use deepmsg_bench::loadtest::result::{self, Status};
use deepmsg_bench::loadtest::rig::{LoadTestRig, RigError, SendResult};
use deepmsg_bench::loadtest::transceiver::{Clock, Idle, MessageTransceiver, TransceiverError};

/// What a run did, in the order it did it.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Event {
    Init,
    Destroy,
    BenchmarkStart(bool),
    BenchmarkComplete(bool),
    Send {
        number_of_messages: usize,
        message_length: usize,
        timestamp: i64,
    },
    Receive,
    Idle,
    IdleReset,
    Report {
        start_time_ns: i64,
        now_ns: i64,
        sent_messages: i64,
        iterations: u32,
    },
    ReportReset,
}

/// Everything a run touches, and where its output went.
#[derive(Default)]
struct Run {
    events: Vec<Event>,
    /// How many complete lines had been written when each event happened, which
    /// is how the interleaving of output and work is checked.
    lines_at: Vec<usize>,
    lines: Vec<String>,
    pending: String,
    clock_reads: usize,
}

impl Run {
    fn record(&mut self, event: Event) {
        self.events.push(event);
        self.lines_at.push(self.lines.len());
    }

    /// The events, without where they fell in the output.
    fn order(&self) -> Vec<Event> {
        self.events.clone()
    }

    fn text(&self) -> String {
        self.lines.join("\n")
    }

    /// How many full lines had been written when the `n`th event happened.
    fn lines_before(&self, nth: usize) -> usize {
        self.lines_at[nth]
    }
}

/// A handle to the shared state, which every double holds a clone of.
#[derive(Clone, Default)]
struct Shared(Rc<RefCell<Run>>);

impl Shared {
    fn new() -> Self {
        Self::default()
    }

    fn record(&self, event: Event) {
        self.0.borrow_mut().record(event);
    }

    fn order(&self) -> Vec<Event> {
        self.0.borrow().order()
    }

    fn text(&self) -> String {
        self.0.borrow().text()
    }

    fn lines_before(&self, nth: usize) -> usize {
        self.0.borrow().lines_before(nth)
    }

    fn clock_reads(&self) -> usize {
        self.0.borrow().clock_reads
    }
}

/// Where the rig's output goes, one line at a time.
impl Write for Shared {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let mut run = self.0.borrow_mut();
        let text = String::from_utf8_lossy(buffer).into_owned();
        run.pending.push_str(&text);

        while let Some(at) = run.pending.find('\n') {
            let line = run.pending[..at].to_owned();
            run.pending.drain(..=at);
            run.lines.push(line);
        }

        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// A clock that hands out a scripted sequence, repeats its last reading, and
/// counts every read.
///
/// The reference reads its mock's call count back to decide whether the rig read
/// the clock where it should have; the count is the assertion, so the counter
/// has to outlive the move into the recorder.
#[derive(Clone, Default)]
struct ScriptedClock {
    readings: Rc<RefCell<VecDeque<i64>>>,
    last: Rc<Cell<i64>>,
    shared: Shared,
}

impl ScriptedClock {
    fn new(shared: &Shared, readings: impl IntoIterator<Item = i64>) -> Self {
        Self {
            readings: Rc::new(RefCell::new(readings.into_iter().collect())),
            last: Rc::new(Cell::new(0)),
            shared: shared.clone(),
        }
    }
}

impl Clock for ScriptedClock {
    fn nano_time(&self) -> i64 {
        self.shared.0.borrow_mut().clock_reads += 1;

        match self.readings.borrow_mut().pop_front() {
            Some(reading) => {
                self.last.set(reading);
                reading
            }
            None => self.last.get(),
        }
    }
}

/// What `send` returns, and what a `receive` finds.
///
/// `number_of_messages` unless a script says otherwise, and the script's last
/// value repeats once it runs out — which is what Mockito's `thenReturn` does
/// and what the reference's tests assume.
struct Scripted {
    send_results: VecDeque<usize>,
    last_send_result: Option<usize>,
    messages_per_receive: usize,
    fail_init: bool,
}

impl Default for Scripted {
    /// The reference's spy reports one message for every look, which is also
    /// what keeps the drain loop at the end of a send from never finishing.
    fn default() -> Self {
        Self {
            send_results: VecDeque::new(),
            last_send_result: None,
            messages_per_receive: 1,
            fail_init: false,
        }
    }
}

/// The transceiver under the rig: a spy that does nothing but record.
#[derive(Clone)]
struct SpyTransceiver {
    shared: Shared,
    script: Rc<RefCell<Scripted>>,
}

impl SpyTransceiver {
    fn new(shared: &Shared, script: Scripted) -> Self {
        Self {
            shared: shared.clone(),
            script: Rc::new(RefCell::new(script)),
        }
    }
}

impl<C: Clock> MessageTransceiver<C> for SpyTransceiver {
    fn init(&mut self, _configuration: &Configuration) -> Result<(), TransceiverError> {
        self.shared.record(Event::Init);

        if self.script.borrow().fail_init {
            return Err(TransceiverError::Failed {
                action: "init",
                message: "the reference's own test fails the start on purpose".to_owned(),
            });
        }

        Ok(())
    }

    fn destroy(&mut self) -> Result<(), TransceiverError> {
        self.shared.record(Event::Destroy);
        Ok(())
    }

    fn send(
        &mut self,
        number_of_messages: usize,
        message_length: usize,
        timestamp: i64,
        _checksum: i64,
        _recorder: &mut Recorder<C>,
    ) -> usize {
        self.shared.record(Event::Send {
            number_of_messages,
            message_length,
            timestamp,
        });

        let mut script = self.script.borrow_mut();

        match script.send_results.pop_front() {
            Some(sent) => {
                script.last_send_result = Some(sent);
                sent
            }
            None => script.last_send_result.unwrap_or(number_of_messages),
        }
    }

    fn receive(&mut self, recorder: &mut Recorder<C>) {
        self.shared.record(Event::Receive);

        let checksum = recorder.checksum();
        for _ in 0..self.script.borrow().messages_per_receive {
            // The reference's spy reports a timestamp of one, so a round trip is
            // the clock's reading less one.
            recorder.on_message_received(1, checksum);
        }
    }

    fn on_benchmark_start(&mut self, warmup: bool) {
        self.shared.record(Event::BenchmarkStart(warmup));
    }

    fn on_benchmark_complete(&mut self, warmup: bool) {
        self.shared.record(Event::BenchmarkComplete(warmup));
    }
}

/// An idle strategy that counts what the rig asked of it.
#[derive(Clone)]
struct CountedIdle(Shared);

impl Idle for CountedIdle {
    fn idle(&mut self) {
        self.0.record(Event::Idle);
    }

    fn reset(&mut self) {
        self.0.record(Event::IdleReset);
    }
}

/// A progress reporter that records instead of printing.
#[derive(Clone)]
struct CountingProgress(Shared);

impl ProgressReporter for CountingProgress {
    fn report_progress(
        &mut self,
        start_time_ns: i64,
        now_ns: i64,
        sent_messages: i64,
        iterations: u32,
    ) {
        self.0.record(Event::Report {
            start_time_ns,
            now_ns,
            sent_messages,
            iterations,
        });
    }

    fn reset(&mut self) {
        self.0.record(Event::ReportReset);
    }
}

/// The pieces a test needs to build a rig and then look at what it did.
struct Fixture {
    shared: Shared,
    clock: ScriptedClock,
    configuration: Configuration,
}

impl Fixture {
    /// A configuration the reference's own `@BeforeEach` would accept, with the
    /// output directory under the temporary directory.
    fn new(name: &str) -> Self {
        let shared = Shared::new();
        let directory =
            std::env::temp_dir().join(format!("deepmsg-bench-rig-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&directory);

        let configuration = Builder::new()
            .warmup_iterations(0)
            .iterations(1)
            .message_rate(1)
            .transceiver(Transceiver::InMemory)
            .output_directory(directory)
            .output_file_name_prefix("test")
            .build()
            .expect("the reference's own test configuration is valid");

        let clock = ScriptedClock::new(&shared, []);

        Self {
            shared,
            clock,
            configuration,
        }
    }

    fn with_clock(mut self, readings: impl IntoIterator<Item = i64>) -> Self {
        self.clock = ScriptedClock::new(&self.shared, readings);
        self
    }

    fn with_configuration(mut self, configuration: Configuration) -> Self {
        self.configuration = configuration;
        self
    }

    /// Run with the given script and hand back the shared state, so the test can
    /// assert on what happened.
    fn run(self, script: Scripted) -> (Shared, Result<Status, RigError>) {
        let mut rig = self.rig(script);
        let status = rig.run();

        (self.shared, status)
    }
}

// ---------------------------------------------------------------------------
// The tests
// ---------------------------------------------------------------------------

impl Fixture {
    fn rig(
        &self,
        script: Scripted,
    ) -> LoadTestRig<SpyTransceiver, ScriptedClock, CountedIdle, CountingProgress, Shared> {
        LoadTestRig::new(
            self.configuration.clone(),
            SpyTransceiver::new(&self.shared, script),
            Recorder::new(result::histogram(), 7, self.clock.clone()),
            CountedIdle(self.shared.clone()),
            CountingProgress(self.shared.clone()),
            self.shared.clone(),
        )
    }

    /// `send` on its own, which is how the reference's timeline tests drive it.
    fn send_only(
        self,
        iterations: u32,
        number_of_messages: u32,
        script: Scripted,
    ) -> (Shared, SendResult) {
        let mut rig = self.rig(script);
        let result = rig.send(iterations, number_of_messages);

        (self.shared, result)
    }
}

fn send(number_of_messages: usize, message_length: usize, timestamp: i64) -> Event {
    Event::Send {
        number_of_messages,
        message_length,
        timestamp,
    }
}

const RECEIVE: Event = Event::Receive;
const IDLE_RESET: Event = Event::IdleReset;

/// The reference's `runPerformsWarmupBeforeMeasurement`.
///
/// A run with one warmup iteration and one measurement iteration, at a rate of
/// one message a second, against a clock that never moves. Everything that
/// happens, in the order it happens — which is what the reference checks with an
/// `InOrder`, and what this checks with one comparison.
///
/// The reference's own trailing `verifyNoMoreInteractions()` is not ported. Its
/// `InOrder` consumes earlier invocations on the same mock as it walks forward,
/// which is why the drain loop's `receive` does not trip it; an exact sequence
/// says the same thing and more, without depending on that.
#[test]
fn the_warmup_runs_before_the_measurement() {
    let fixed = 123_000_000_000;
    let fixture = Fixture::new("warmup-order").with_clock([fixed]);
    let configuration = Builder::new()
        .warmup_iterations(1)
        .warmup_message_rate(1)
        .iterations(1)
        .message_rate(1)
        .output_time_unit(TimeUnit::Nanoseconds)
        .output_directory(std::env::temp_dir().join("deepmsg-bench-rig-warmup-order-out"))
        .output_file_name_prefix("test")
        .build()
        .expect("valid");
    let (shared, status) = fixture.with_configuration(configuration).run(Scripted {
        messages_per_receive: 1,
        ..Scripted::default()
    });

    assert_eq!(status.expect("the run goes through"), Status::Ok);

    // The configuration is printed before the transceiver is started, which is
    // the one thing an event list alone cannot say.
    assert!(
        shared.lines_before(0) > 0,
        "the configuration is on the way out before the transceiver is started"
    );

    assert_eq!(
        shared.order(),
        vec![
            Event::Init,
            Event::BenchmarkStart(true),
            send(1, 16, fixed),
            Event::Report {
                start_time_ns: fixed,
                now_ns: fixed,
                sent_messages: 1,
                iterations: 1,
            },
            IDLE_RESET,
            RECEIVE,
            IDLE_RESET,
            Event::BenchmarkComplete(true),
            Event::ReportReset,
            Event::BenchmarkStart(false),
            send(1, 16, fixed),
            Event::Report {
                start_time_ns: fixed,
                now_ns: fixed,
                sent_messages: 1,
                iterations: 1,
            },
            IDLE_RESET,
            RECEIVE,
            IDLE_RESET,
            Event::BenchmarkComplete(false),
            Event::ReportReset,
            Event::Destroy,
        ]
    );

    // The configuration is printed before anything is started, and the two
    // headings before the work they name.
    let text = shared.text();
    assert!(
        text.contains("Starting latency benchmark using the following configuration"),
        "{text}"
    );
    assert!(text.contains("messageTransceiver=in-memory"), "{text}");
    assert!(
        text.contains("Running warmup for 1 iterations of 1 messages each"),
        "{text}"
    );
    assert!(
        text.contains("Running measurement for 1 iterations of 1 messages each"),
        "{text}"
    );
    assert!(
        text.contains("Histogram of RTT latencies in NANOSECONDS."),
        "{text}"
    );
    assert!(
        !text.contains("WARNING"),
        "a run that sent and received everything warns about nothing"
    );
}

/// The reference's `runWarnsAboutMissedTargetRate`.
///
/// Three iterations at five messages a second is fifteen expected; the clock
/// jumps to nine seconds on its second reading, so the run is out of time after
/// two messages and says so.
#[test]
fn a_run_that_missed_its_target_says_so() {
    let fixture = Fixture::new("warn-rate").with_clock([1, 9_000_000_000]);
    let configuration = Builder::new()
        .warmup_iterations(0)
        .iterations(3)
        .message_rate(5)
        .batch_size(2)
        .output_directory(std::env::temp_dir().join("deepmsg-bench-rig-warn-out"))
        .output_file_name_prefix("test")
        .build()
        .expect("valid");
    let (shared, status) = fixture
        .with_configuration(configuration)
        .run(Scripted::default());

    assert_eq!(status.expect("the run goes through"), Status::Fail);

    let text = shared.text();
    assert!(
        text.contains("Histogram of RTT latencies in MICROSECONDS."),
        "{text}"
    );
    assert!(
        text.contains(
            "*** WARNING: Target message rate not achieved: expected to send 15 messages in total but \
             managed to send only 2 messages (loss 86.6667%)!"
        ),
        "{text}"
    );
}

/// The reference's `receiveShouldKeepReceivingMessagesUpToTheSentMessagesLimit`.
#[test]
fn receiving_stops_at_the_number_of_messages_sent() {
    // The clock has to move at least once. The reference's histogram is a mock,
    // so a round trip of zero — every message answered by a clock that never
    // moved off the run's own start — goes into it unremarked; a real histogram
    // has one as its lowest value and refuses it.
    let fixture = Fixture::new("receive-limit").with_clock([0, 10]);
    let configuration = Builder::new()
        .warmup_iterations(0)
        .iterations(1)
        .message_rate(3)
        .batch_size(200)
        .output_time_unit(TimeUnit::Minutes)
        .output_directory(std::env::temp_dir().join("deepmsg-bench-rig-limit-out"))
        .output_file_name_prefix("test")
        .build()
        .expect("valid");

    let (shared, result) = fixture.with_configuration(configuration).send_only(
        1,
        3,
        Scripted {
            messages_per_receive: 1,
            ..Scripted::default()
        },
    );

    assert_eq!(result.sent_messages, 3);
    assert_eq!(result.received_messages, 3);

    let events = shared.order();
    assert_eq!(events.iter().filter(|event| **event == RECEIVE).count(), 3);
    assert_eq!(
        events.iter().filter(|event| **event == IDLE_RESET).count(),
        4
    );
}

/// The reference's `sendStopsWhenTotalNumberOfMessagesIsReached`.
///
/// The whole point of the port, in one test: five batches go out at five exact
/// timestamps, the clock is read exactly twenty-four times, and the idle
/// strategy is reset exactly ten. Any of those being different is the cadence
/// being different.
#[test]
fn sending_stops_when_the_total_number_of_messages_is_reached() {
    let millis = |value: i64| value * 1_000_000;
    let fixture = Fixture::new("total-reached").with_clock([
        millis(1000),
        millis(1750),
        millis(2400),
        millis(2950),
    ]);
    let configuration = Builder::new()
        .warmup_iterations(0)
        .iterations(2)
        .message_rate(1)
        .batch_size(4)
        .message_length(24)
        .output_directory(std::env::temp_dir().join("deepmsg-bench-rig-total-out"))
        .output_file_name_prefix("test")
        .build()
        .expect("valid");

    let (shared, result) = fixture.with_configuration(configuration).send_only(
        2,
        9,
        Scripted {
            // Two messages come back per look, as the reference's spy does.
            messages_per_receive: 2,
            ..Scripted::default()
        },
    );

    assert_eq!(result.sent_messages, 18);
    assert_eq!(result.received_messages, 18);

    let mut expected = vec![
        send(4, 24, 1_000_000_000),
        send(4, 24, 1_444_444_444),
        Event::Report {
            start_time_ns: 1_000_000_000,
            now_ns: 2_400_000_000,
            sent_messages: 8,
            iterations: 2,
        },
        send(4, 24, 1_888_888_888),
        send(4, 24, 2_333_333_332),
        send(2, 24, 2_777_777_776),
        Event::Report {
            start_time_ns: 1_000_000_000,
            now_ns: 2_950_000_000,
            sent_messages: 18,
            iterations: 2,
        },
        IDLE_RESET,
    ];
    for _ in 0..9 {
        expected.push(RECEIVE);
        expected.push(IDLE_RESET);
    }

    assert_eq!(shared.order(), expected);
    assert_eq!(shared.clock_reads(), 24);
}

/// The reference's `sendStopsIfTimeElapsesBeforeTargetNumberOfMessagesIsReached`.
///
/// A transceiver that sends less than it is asked to, and a clock that outruns
/// the schedule: the run keeps going until the deadline, not until the count.
#[test]
fn sending_stops_when_the_time_runs_out() {
    let millis = |value: i64| value * 1_000_000;
    let fixture = Fixture::new("time-elapsed").with_clock([
        millis(500),
        millis(501),
        millis(777),
        millis(778),
        millis(6750),
        millis(6751),
        millis(9200),
        millis(9201),
        millis(12_000),
        millis(12_001),
    ]);
    let configuration = Builder::new()
        .warmup_iterations(0)
        .iterations(10)
        .message_rate(1)
        .batch_size(30)
        .message_length(100)
        .output_directory(std::env::temp_dir().join("deepmsg-bench-rig-elapsed-out"))
        .output_file_name_prefix("test")
        .build()
        .expect("valid");

    let (shared, result) = fixture.with_configuration(configuration).send_only(
        10,
        100,
        Scripted {
            // `thenReturn(15, 10, 5, 30)`, and thirty from then on.
            send_results: [15_usize, 10, 5, 30].into_iter().collect(),
            messages_per_receive: 1,
            ..Scripted::default()
        },
    );

    assert_eq!(result.sent_messages, 120);
    assert_eq!(result.received_messages, 120);

    let events = shared.order();
    let sends: Vec<&Event> = events
        .iter()
        .filter(|event| matches!(event, Event::Send { .. }))
        .collect();
    assert_eq!(
        sends,
        vec![
            &send(30, 100, 500_000_000),
            &send(15, 100, 500_000_000),
            &send(5, 100, 500_000_000),
            &send(30, 100, 800_000_000),
            &send(30, 100, 1_100_000_000),
            &send(30, 100, 1_400_000_000),
        ]
    );
    assert_eq!(
        events.iter().filter(|event| **event == RECEIVE).count(),
        120
    );
    assert_eq!(
        events.iter().filter(|event| **event == IDLE_RESET).count(),
        119
    );
    assert_eq!(shared.clock_reads(), 128);

    let reports: Vec<&Event> = events
        .iter()
        .filter(|event| matches!(event, Event::Report { .. }))
        .collect();
    assert_eq!(
        reports,
        vec![
            &Event::Report {
                start_time_ns: 500_000_000,
                now_ns: 6_751_000_000,
                sent_messages: 30,
                iterations: 10,
            },
            &Event::Report {
                start_time_ns: 500_000_000,
                now_ns: 9_200_000_000,
                sent_messages: 60,
                iterations: 10,
            },
            &Event::Report {
                start_time_ns: 500_000_000,
                now_ns: 9_201_000_000,
                sent_messages: 90,
                iterations: 10,
            },
        ]
    );
}

/// The reference's `sendUsesGracePeriodToFlushOutstandingMessagesAfterNominalDuration`.
///
/// The nominal second is up at t=1s and the send deadline, with the default
/// hundred milliseconds of grace, is at 1.1s. At 1.05s the run is past the one
/// and inside the other, so the last message goes out instead of being clipped —
/// and the reply to the first is drained there rather than after the sending.
#[test]
fn the_grace_period_flushes_what_is_still_outstanding() {
    let fixture = Fixture::new("grace").with_clock([0, 1_050_000_000]);
    let configuration = Builder::new()
        .warmup_iterations(0)
        .iterations(1)
        .message_rate(2)
        .batch_size(1)
        .message_length(24)
        .output_directory(std::env::temp_dir().join("deepmsg-bench-rig-grace-out"))
        .output_file_name_prefix("test")
        .build()
        .expect("valid");

    let (shared, result) = fixture.with_configuration(configuration).send_only(
        1,
        2,
        Scripted {
            messages_per_receive: 1,
            ..Scripted::default()
        },
    );

    assert_eq!(result.sent_messages, 2);
    assert_eq!(result.received_messages, 2);

    let sends_and_receives: Vec<Event> = shared
        .order()
        .into_iter()
        .filter(|event| matches!(event, Event::Send { .. } | Event::Receive))
        .collect();
    assert_eq!(
        sends_and_receives[..3],
        [send(1, 24, 0), RECEIVE, send(1, 24, 500_000_000)],
        "the reply to the first message is drained inside the grace window, not after the last send"
    );
}

/// The reference's `shouldCallDestroyOnMessageTransceiverIfInitFails`, which is
/// what its `finally` is for.
#[test]
fn the_transceiver_is_torn_down_even_when_it_will_not_start() {
    let fixture = Fixture::new("init-fails");
    let (shared, status) = fixture.run(Scripted {
        fail_init: true,
        ..Scripted::default()
    });

    assert!(matches!(status, Err(RigError::Transceiver(_))));
    assert_eq!(shared.order(), vec![Event::Init, Event::Destroy]);
}

/// The reference's `endToEndTest`: the whole thing, against a real clock and the
/// in-memory transceiver, with the file it produces on disk.
#[test]
fn a_run_end_to_end_produces_a_result_file() {
    let directory =
        std::env::temp_dir().join(format!("deepmsg-bench-rig-e2e-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&directory);

    let configuration = Builder::new()
        .warmup_iterations(2)
        .warmup_message_rate(100)
        .iterations(5)
        .message_rate(777)
        .message_length(32)
        .batch_size(9)
        .transceiver(Transceiver::InMemory)
        .output_directory(&directory)
        .output_file_name_prefix("test")
        .build()
        .expect("valid");

    let checksum = deepmsg_bench::loadtest::recorder::checksum();
    let mut rig = LoadTestRig::new(
        configuration.clone(),
        deepmsg_bench::loadtest::in_memory::InMemoryTransceiver::new(),
        Recorder::new(
            result::histogram(),
            checksum,
            deepmsg_bench::loadtest::transceiver::SystemClock,
        ),
        IdleStrategy::BusySpin,
        deepmsg_bench::loadtest::progress::NullProgressReporter,
        Vec::new(),
    );

    let started = std::time::Instant::now();
    let status = rig.run();
    let elapsed = started.elapsed();

    assert_eq!(status.expect("the run goes through"), Status::Ok);
    assert!(
        elapsed.as_secs() <= 2 + 5 + 1,
        "a two-second warmup and a five-second measurement took {elapsed:?}"
    );

    let files: Vec<String> = std::fs::read_dir(&directory)
        .expect("the output directory was made")
        .map(|entry| {
            entry
                .expect("readable")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect();

    assert_eq!(
        files.iter().filter(|name| name.ends_with(".hdr")).count(),
        1,
        "{files:?}"
    );
    assert!(files.iter().any(|name| name == "logs"), "{files:?}");

    let _ = std::fs::remove_dir_all(&directory);
}
