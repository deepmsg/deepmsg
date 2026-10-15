//! Generators that drop what a **receive** endpoint is handed, on purpose.
//!
//! The reference keeps this family apart from the driver's own generators: they
//! live in `aeron-test-support` and are linked into the one test binary that
//! needs them (`aeron-archive/src/test/c/CMakeLists.txt:67-72`). This module is
//! the same thing — **test-only decision rules**, built from the driver's
//! settings rather than from a caller's supplier, because a driver here is a
//! process and the reference's is started in-process by its test.
//!
//! # Why a test needs them at all
//!
//! Loopback does not lose anything, so a test that waits for the network to
//! drop a frame waits forever, and every path that exists to *recover* from
//! loss is unreachable: the gap is never there to be detected, the NAK never
//! goes out, and the replay that fills it never happens. The reference's own
//! answer is this family, and each one is a different way to make the loss
//! **deterministic** rather than merely likely.
//!
//! # What each one is for, in one line
//!
//! * [`StreamId`] — a whole stream's data, so a subscriber's recovery is
//!   exercised at all;
//! * [`StreamIdEveryNth`] and [`EveryNthFrame`] — a *fraction* of it, which is
//!   what "moderate" and "heavy" loss mean in the reference's cases;
//! * [`SetupAtPosition`] — one handshake, so that a subscriber attaches to a
//!   stream at a later position than another subscriber on the same channel;
//! * [`DataInRange`] — one contiguous run of one term, so that a gap is punched
//!   at a position the test chose instead of wherever a rate happened to land.
//!
//! # Two of them are per-endpoint, and that is the whole trick
//!
//! [`SetupAtPosition`] and [`DataInRange`] carry a **skip substring**, and an
//! endpoint whose URI contains it is left alone. That is how the reference's
//! cases build "the recording stays whole while the reader's copy is missing a
//! range" — the archive's recording subscription names `init-term-id=` in its
//! channel and a plain application subscription does not
//! (`aeron_data_in_range_loss_generator.h:29-33`) — and it is why the generator
//! has to be made *per endpoint* rather than one per driver. Here the skip is
//! decided where the endpoint is made, so an exempt endpoint is given **no
//! generator at all**: same decision, one allocation less.
//!
//! ★ **What this build does not have**, and the reason is the one above: the
//! reference's generators are enabled and disabled *while a test runs*
//! (`…_loss_generator_enable`, from the test's own thread). A driver that is a
//! process can be told what to drop before it starts and not afterwards, so
//! every rule here is **fixed from the settings** and a case that wants loss to
//! begin at a chosen moment has to say so as a **position window** instead.
//! That deviation is recorded in `docs/compat.md`.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use crate::media::loss_generator::LossGenerator;
use crate::protocol::{DataFrame, FrameHeader, SetupFrame};

/// Drop every `DATA` frame of one stream
/// (`aeron_stream_id_loss_generator.c`).
///
/// The bluntest of the five, and the one the reference's resilience binary uses
/// for "this stream loses everything" — which for a *replay* stream means the
/// subscriber only ever sees what the replay sends again.
#[derive(Debug, Clone, Copy)]
pub struct StreamId {
    stream_id: i32,
}

impl StreamId {
    /// Drop `DATA` on `stream_id`, and nothing else.
    #[must_use]
    pub const fn new(stream_id: i32) -> Self {
        Self { stream_id }
    }
}

impl LossGenerator for StreamId {
    fn should_drop(&mut self, _address: SocketAddr, _buffer: &[u8], _length: usize) -> bool {
        false
    }

    fn should_drop_detailed(
        &mut self,
        _address: SocketAddr,
        _buffer: &[u8],
        stream_id: i32,
        _session_id: i32,
        _term_id: i32,
        _term_offset: i32,
        _length: usize,
    ) -> bool {
        stream_id == self.stream_id
    }
}

/// Drop one `DATA` frame of one stream in every `every`
/// (`aeron_stream_id_frame_data_loss_generator.c`).
///
/// The reference reaches this shape through a **predicate** the test supplies
/// (`aeron_stream_id_frame_data_loss_generator_enable` takes a function
/// pointer); a rate is the only predicate a setting can spell, and it is the one
/// the reference's cases actually pass. What is counted is frames the rule
/// *matched*, not frames seen, so the rate is over the stream it names.
#[derive(Debug, Clone, Copy)]
pub struct StreamIdEveryNth {
    stream_id: i32,
    every: u64,
    matched: u64,
}

impl StreamIdEveryNth {
    /// Drop one matching frame in every `every`. A value below two drops
    /// nothing, which is what the setting's own refusal of one leaves.
    #[must_use]
    pub const fn new(stream_id: i32, every: u64) -> Self {
        Self {
            stream_id,
            every,
            matched: 0,
        }
    }

    /// How many frames this rule matched, dropped or not.
    #[must_use]
    pub const fn matched(&self) -> u64 {
        self.matched
    }
}

impl LossGenerator for StreamIdEveryNth {
    fn should_drop(&mut self, _address: SocketAddr, _buffer: &[u8], _length: usize) -> bool {
        false
    }

    fn should_drop_detailed(
        &mut self,
        _address: SocketAddr,
        _buffer: &[u8],
        stream_id: i32,
        _session_id: i32,
        _term_id: i32,
        _term_offset: i32,
        _length: usize,
    ) -> bool {
        if stream_id != self.stream_id || self.every < 2 {
            return false;
        }

        self.matched += 1;

        self.matched % self.every == 0
    }
}

/// Drop one `DATA` frame in every `every`, whichever stream it is on
/// (`aeron_frame_data_loss_generator.c`).
///
/// The same predicate without a stream filter — for a channel that carries one
/// stream, or a test that means "this endpoint loses a third of what it is
/// sent" and does not care which stream that is.
#[derive(Debug, Clone, Copy)]
pub struct EveryNthFrame {
    every: u64,
    seen: u64,
}

impl EveryNthFrame {
    /// Drop one frame in every `every`. Below two, nothing.
    #[must_use]
    pub const fn new(every: u64) -> Self {
        Self { every, seen: 0 }
    }

    /// How many frames this rule was shown.
    #[must_use]
    pub const fn seen(&self) -> u64 {
        self.seen
    }
}

impl LossGenerator for EveryNthFrame {
    fn should_drop(&mut self, _address: SocketAddr, _buffer: &[u8], _length: usize) -> bool {
        false
    }

    fn should_drop_detailed(
        &mut self,
        _address: SocketAddr,
        _buffer: &[u8],
        _stream_id: i32,
        _session_id: i32,
        _term_id: i32,
        _term_offset: i32,
        _length: usize,
    ) -> bool {
        if self.every < 2 {
            return false;
        }

        self.seen += 1;

        self.seen % self.every == 0
    }
}

/// Drop the `SETUP` that names one position
/// (`aeron_setup_at_position_loss_generator.c`).
///
/// A `SETUP` is how a subscriber learns a stream exists and where it starts, so
/// dropping *this* one and letting a later one through is how a test makes a
/// subscriber attach **late** — which is the only way to reach the alignment a
/// persistent subscription does when the live stream is already ahead of its
/// replay.
///
/// ★ It answers the **simple** question, and the reference's own vtable leaves
/// the detailed slot null beside it (`…_loss_generator.c:161`): this generator
/// has no opinion about data, and a build that routed `DATA` through it would
/// drop a term's worth of frames.
#[derive(Debug, Clone, Copy)]
pub struct SetupAtPosition {
    stream_id: i32,
    initial_term_id: i32,
    active_term_id: i32,
    term_offset: i32,
    dropped: u64,
}

impl SetupAtPosition {
    /// Drop the `SETUP` whose `(stream_id, initial_term_id, active_term_id,
    /// term_offset)` is exactly this one.
    #[must_use]
    pub const fn new(
        stream_id: i32,
        initial_term_id: i32,
        active_term_id: i32,
        term_offset: i32,
    ) -> Self {
        Self {
            stream_id,
            initial_term_id,
            active_term_id,
            term_offset,
            dropped: 0,
        }
    }

    /// How many `SETUP`s this rule withheld.
    #[must_use]
    pub const fn dropped(&self) -> u64 {
        self.dropped
    }
}

impl LossGenerator for SetupAtPosition {
    fn should_drop(&mut self, _address: SocketAddr, buffer: &[u8], length: usize) -> bool {
        if length < SetupFrame::LENGTH {
            return false;
        }

        let Some(setup) = SetupFrame::read(buffer) else {
            return false;
        };

        if setup.stream_id != self.stream_id
            || setup.initial_term_id != self.initial_term_id
            || setup.active_term_id != self.active_term_id
            || setup.term_offset != self.term_offset
        {
            return false;
        }

        self.dropped += 1;

        true
    }
}

/// Drop the `DATA` of one term whose offset falls in one range
/// (`aeron_data_in_range_loss_generator.c`).
///
/// The deterministic version of loss: instead of a rate that lands wherever it
/// lands, a **contiguous run** at a position the test chose. That is what makes
/// "the reader is missing exactly this, and the recording is missing nothing"
/// a thing a case can assert.
///
/// ★ **Heartbeats pass.** A `DATA` frame that is nothing but its header, with a
/// zero frame length, is how a publication carries `EOS` and `REVOKE` — and a
/// subscriber whose last frame is dropped never learns its publisher is gone
/// (`…_loss_generator.c:107-114`).
#[derive(Debug, Clone, Copy)]
pub struct DataInRange {
    stream_id: i32,
    active_term_id: i32,
    min_inclusive: i32,
    max_exclusive: i32,
    dropped: u64,
}

impl DataInRange {
    /// Drop `DATA` on `stream_id` in `active_term_id` at offsets in
    /// `[min_inclusive, max_exclusive)`.
    #[must_use]
    pub const fn new(
        stream_id: i32,
        active_term_id: i32,
        min_inclusive: i32,
        max_exclusive: i32,
    ) -> Self {
        Self {
            stream_id,
            active_term_id,
            min_inclusive,
            max_exclusive,
            dropped: 0,
        }
    }

    /// How many frames this rule withheld.
    #[must_use]
    pub const fn dropped(&self) -> u64 {
        self.dropped
    }
}

impl LossGenerator for DataInRange {
    fn should_drop(&mut self, _address: SocketAddr, _buffer: &[u8], _length: usize) -> bool {
        false
    }

    fn should_drop_detailed(
        &mut self,
        _address: SocketAddr,
        buffer: &[u8],
        stream_id: i32,
        _session_id: i32,
        term_id: i32,
        term_offset: i32,
        length: usize,
    ) -> bool {
        if length == DataFrame::LENGTH {
            // A heartbeat: the header and nothing else, so the only thing it can
            // be carrying is a flag.
            if let Some(header) = FrameHeader::read(buffer) {
                if header.frame_length == 0 {
                    return false;
                }
            }
        }

        if stream_id != self.stream_id
            || term_id != self.active_term_id
            || term_offset < self.min_inclusive
            || term_offset >= self.max_exclusive
        {
            return false;
        }

        self.dropped += 1;

        true
    }
}

/// Drop one stream's frames **for a while**, and then stop
/// (`drop_payloads_after_threshold`,
/// `aeron-archive/src/test/c/client/aeron_archive_persistent_subscription_resilience_test.cpp:1926-1939`;
/// the time-bounded predicate beside it is `:1952`).
///
/// The first `pass_first` frames of the stream go through, everything after them
/// is withheld for `until`, and after that everything goes through again. That is
/// what makes a rule **self-healing**, and it is the reason this exists: the
/// reference's cases that drop *responses* pass their handshake, block for a
/// while, and then have to let the machine recover — which its own generators do
/// by being disabled from the test's thread (`…_stream_id_loss_generator_disable`).
/// A driver whose rules are fixed before it starts has no such moment, so the
/// bound has to be **in the rule**.
///
/// ★★ **The bound is a time, and measuring is what said so.** A first version
/// bounded it by a **count of frames**, and the log showed it failing twice over.
/// First the count was burned through in milliseconds, because a withheld frame
/// is *retransmitted* — the receiver notices the gap and asks — so what advanced
/// the counter was the transport's own recovery rather than anything the machine
/// under test did; counting **distinct frames** fixed that. And then, with the
/// count honest, it still never reached its end: the count stalled after about
/// ten frames and the machine sat in `AWAIT_LIST_RECORDING_RESPONSE` until its
/// own deadline, because **frames stop arriving long before a frame budget is
/// spent** — withholding everything tears the channel down rather than delaying
/// it. A time is immune to both: it does not care how many times a frame is sent,
/// and it ends whether or not the channel is still carrying anything.
///
/// The reference reached for a time in the same place: its composite predicate
/// drops `SETUP`s "until `g_setup_drop_until_ns`" (`:1948-1950`).
///
/// # What counts as a frame
///
/// ★ **Heartbeats pass and are not counted.** A `DATA` frame that is nothing but
/// its header is how a publication carries `EOS` and `REVOKE`, and — as
/// important here — how a session says it is still alive: a blackout that
/// swallowed them would drop the connection rather than a response. The
/// reference's own predicate draws the line in the same place (`:1928-1937`).
#[derive(Debug, Clone)]
pub struct Blackout {
    /// Which stream's frames this counts, or `None` for any.
    stream_id: Option<i32>,
    /// How many frames of the stream go through before the blackout starts.
    pass_first: u64,
    /// How long after the stream's first frame the blackout **opens**. What
    /// `pass_first` cannot say, and the reason this is here: a case that wants
    /// "the handshake and the listing get through, then one response is held
    /// back" is naming a **moment**, and counting frames names it only if the
    /// handshake's frame count is known — measured at three *and* four on this
    /// machine, so counting cannot name it at all.
    ///
    /// Zero is the whole of what was here before: the blackout opens with the
    /// first frame that has passed `pass_first`.
    from: Duration,
    /// How long it withholds once it has.
    until: Duration,
    /// When the stream's first frame arrived, which is what `until` is measured
    /// from — `None` until one has.
    started: Option<Instant>,
    /// The frames it has been shown, **by the position that names them**, so
    /// that `pass_first` counts frames of the stream and not transmissions of
    /// them.
    seen: Vec<(i32, i32)>,
}

impl Blackout {
    /// Pass the first `pass_first` frames of `stream_id`, withhold what follows
    /// **between `from` and `until`** — both measured from the stream's first
    /// frame — and let the rest through.
    ///
    /// `from` of zero is a blackout that opens as soon as `pass_first` frames
    /// have gone through, which is what this rule was before it had one.
    #[must_use]
    pub const fn new(
        stream_id: Option<i32>,
        pass_first: u64,
        from: Duration,
        until: Duration,
    ) -> Self {
        Self {
            stream_id,
            pass_first,
            from,
            until,
            started: None,
            seen: Vec::new(),
        }
    }

    /// How many **distinct** frames of the stream it has been shown.
    #[must_use]
    pub fn matched(&self) -> u64 {
        self.seen.len() as u64
    }

    /// Whether the blackout is in force, from the stream's first frame: it opens
    /// at `from` and shuts at `until`.
    fn holding(&self) -> bool {
        self.started
            .is_some_and(|started| self.from <= started.elapsed() && started.elapsed() < self.until)
    }
}

impl LossGenerator for Blackout {
    fn should_drop(&mut self, _address: SocketAddr, _buffer: &[u8], _length: usize) -> bool {
        false
    }

    fn should_drop_detailed(
        &mut self,
        _address: SocketAddr,
        buffer: &[u8],
        stream_id: i32,
        _session_id: i32,
        term_id: i32,
        term_offset: i32,
        length: usize,
    ) -> bool {
        if self.stream_id.is_some_and(|wanted| wanted != stream_id) {
            return false;
        }

        if length == DataFrame::LENGTH {
            // A frame that is only its header is a heartbeat: it passes, and it
            // is not one of the frames being counted.
            if let Some(header) = FrameHeader::read(buffer) {
                if header.frame_length == 0 {
                    return false;
                }
            }
        }

        // Which frame of the stream this is — the same answer for a frame that
        // arrives twice, so that retransmission cannot spend the
        // `pass_first` budget.
        let position = (term_id, term_offset);

        let ordinal = match self.seen.iter().position(|seen| *seen == position) {
            Some(index) => index as u64 + 1,
            None => {
                self.seen.push(position);

                self.seen.len() as u64
            }
        };

        if self.started.is_none() {
            self.started = Some(Instant::now());
        }

        ordinal > self.pass_first && self.holding()
    }
}

/// Where a `SETUP` is named (`…_setup_at_position_loss_generator.h:31-40`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SetupPosition {
    /// The stream the handshake is for.
    pub stream_id: i32,
    /// The term the publisher began in.
    pub initial_term_id: i32,
    /// The term the handshake is being sent in.
    pub active_term_id: i32,
    /// And where in it.
    pub term_offset: i32,
}

/// Which run of a term is named
/// (`…_data_in_range_loss_generator.h:27-32`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DataRange {
    /// The stream the run is on.
    pub stream_id: i32,
    /// The term it is in.
    pub active_term_id: i32,
    /// The first offset in it.
    pub min_inclusive: i32,
    /// One past the last.
    pub max_exclusive: i32,
}

/// What the driver's settings ask the receive side to drop.
///
/// **One rule per driver**, because that is what the reference's supplier gives
/// an endpoint: one generator, whatever it was built from. Which rule it is
/// depends on which settings were named — see [`Spec::generator_for`] — and two
/// rules that would both apply are **refused where the settings are read**
/// rather than silently ordered here.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Spec {
    /// A stream's `DATA`, or `None` for any stream.
    pub stream_id: Option<i32>,
    /// One frame in every this many, or `None` for all of them.
    pub every: Option<u64>,
    /// How many matching frames a **blackout** lets through first.
    pub pass_first: Option<u64>,
    /// How long after the stream's first frame a **blackout** opens, in
    /// milliseconds, or `None` for "as soon as `pass_first` frames have gone".
    pub from_ms: Option<u64>,
    /// How long a **blackout** withholds once it has started, in milliseconds,
    /// or `None` for no blackout.
    pub until_ms: Option<u64>,
    /// The one `SETUP` to drop.
    pub setup_at_position: Option<SetupPosition>,
    /// The one run of `DATA` to drop.
    pub data_in_range: Option<DataRange>,
    /// Endpoints whose channel URI contains this are **left alone**.
    pub endpoint_skip: Option<String>,
}

impl Spec {
    /// Whether these settings ask for nothing.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.stream_id.is_none()
            && self.every.is_none()
            && self.until_ms.is_none()
            && self.setup_at_position.is_none()
            && self.data_in_range.is_none()
    }

    /// The generator an endpoint on `original_uri` gets, or `None` if this
    /// endpoint is to keep everything.
    ///
    /// The skip is decided **here**, once, rather than inside the generator on
    /// every frame: an endpoint whose URI names the exempted channel is given no
    /// generator at all, which is the same answer the reference reaches by
    /// checking the URI at frame-check time and always saying no.
    #[must_use]
    pub fn generator_for(&self, original_uri: &[u8]) -> Option<Box<dyn LossGenerator>> {
        if let Some(skip) = self.endpoint_skip.as_deref() {
            if contains(original_uri, skip.as_bytes()) {
                return None;
            }
        }

        if let Some(range) = self.data_in_range {
            return Some(Box::new(DataInRange::new(
                range.stream_id,
                range.active_term_id,
                range.min_inclusive,
                range.max_exclusive,
            )));
        }

        if let Some(position) = self.setup_at_position {
            return Some(Box::new(SetupAtPosition::new(
                position.stream_id,
                position.initial_term_id,
                position.active_term_id,
                position.term_offset,
            )));
        }

        // A blackout comes before the unbounded rules, and is the only one that
        // ends by itself — see [`Blackout`].
        if let Some(until_ms) = self.until_ms {
            return Some(Box::new(Blackout::new(
                self.stream_id,
                self.pass_first.unwrap_or(0),
                Duration::from_millis(self.from_ms.unwrap_or(0)),
                Duration::from_millis(until_ms),
            )));
        }

        match (self.stream_id, self.every) {
            (Some(stream_id), Some(every)) => {
                Some(Box::new(StreamIdEveryNth::new(stream_id, every)))
            }
            (Some(stream_id), None) => Some(Box::new(StreamId::new(stream_id))),
            (None, Some(every)) => Some(Box::new(EveryNthFrame::new(every))),
            (None, None) => None,
        }
    }
}

/// Whether `haystack` contains `needle` — `strstr`, for a URI that is bytes.
fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    !needle.is_empty()
        && haystack
            .windows(needle.len())
            .any(|window| window == needle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn loopback() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 40456)
    }

    /// A `DATA` frame of `payload` bytes on `stream_id`, at `term_id`/`offset`.
    ///
    /// Written through the protocol's own writer rather than by hand, for the
    /// reason a hand-rolled frame is a liability: the byte a test got wrong is
    /// the byte the code under test reads, and both then agree on something the
    /// wire never says.
    fn data_frame(stream_id: i32, term_id: i32, term_offset: i32, payload: usize) -> Vec<u8> {
        let mut frame = vec![0u8; DataFrame::LENGTH + payload];

        DataFrame {
            term_offset,
            session_id: 7,
            stream_id,
            term_id,
            reserved_value: 0,
        }
        .write(&mut frame)
        .expect("the frame fits");

        // `write` sets the header's length to the frame's own size, which for a
        // payload-carrying frame is the whole datagram here.
        let length = i32::try_from(frame.len())
            .expect("a small frame")
            .to_le_bytes();
        frame[0..4].copy_from_slice(&length);

        frame
    }

    /// A `SETUP` naming one stream at one position.
    fn setup_frame(
        stream_id: i32,
        initial_term_id: i32,
        active_term_id: i32,
        term_offset: i32,
    ) -> Vec<u8> {
        let mut frame = vec![0u8; SetupFrame::LENGTH];

        SetupFrame {
            term_offset,
            session_id: 7,
            stream_id,
            initial_term_id,
            active_term_id,
            term_length: 64 * 1024,
            mtu: 1408,
            ttl: 0,
        }
        .write(&mut frame)
        .expect("the frame fits");

        frame
    }

    /// Ask the detailed question about one frame.
    fn detailed(generator: &mut dyn LossGenerator, frame: &[u8]) -> bool {
        let data = DataFrame::read(frame).expect("a data frame");

        generator.should_drop_detailed(
            loopback(),
            frame,
            data.stream_id,
            data.session_id,
            data.term_id,
            data.term_offset,
            frame.len(),
        )
    }

    #[test]
    fn a_stream_id_rule_drops_that_stream_and_no_other() {
        let mut generator = StreamId::new(4218);

        assert!(detailed(&mut generator, &data_frame(4218, 1, 0, 32)));
        assert!(!detailed(&mut generator, &data_frame(4219, 1, 0, 32)));

        // And it has no opinion about a SETUP, which is the reference's null
        // detailed slot beside it.
        assert!(!generator.should_drop(
            loopback(),
            &setup_frame(4218, 1, 1, 0),
            SetupFrame::LENGTH
        ));
    }

    #[test]
    fn a_rate_over_one_stream_counts_only_the_frames_it_matched() {
        let mut generator = StreamIdEveryNth::new(4218, 3);

        // Three of another stream's frames go by without being counted.
        for _ in 0..3 {
            assert!(!detailed(&mut generator, &data_frame(4219, 1, 0, 32)));
        }

        assert_eq!(0, generator.matched());

        let dropped: Vec<bool> = (0..6)
            .map(|_| detailed(&mut generator, &data_frame(4218, 1, 0, 32)))
            .collect();

        assert_eq!(vec![false, false, true, false, false, true], dropped);
        assert_eq!(6, generator.matched());
    }

    #[test]
    fn a_rate_with_no_stream_drops_across_streams() {
        let mut generator = EveryNthFrame::new(2);

        assert!(!detailed(&mut generator, &data_frame(1, 1, 0, 32)));
        assert!(detailed(&mut generator, &data_frame(2, 1, 0, 32)));
        assert!(!detailed(&mut generator, &data_frame(1, 1, 0, 32)));
        assert!(detailed(&mut generator, &data_frame(2, 1, 0, 32)));
        assert_eq!(4, generator.seen());
    }

    /// A rate of one would empty the wire, so it is read as no loss at all —
    /// the same reading the setting's own refusal gives.
    #[test]
    fn a_rate_below_two_drops_nothing() {
        for every in [0, 1] {
            let mut stream = StreamIdEveryNth::new(4218, every);
            let mut any = EveryNthFrame::new(every);

            for _ in 0..4 {
                assert!(!detailed(&mut stream, &data_frame(4218, 1, 0, 32)));
                assert!(!detailed(&mut any, &data_frame(4218, 1, 0, 32)));
            }
        }
    }

    #[test]
    fn a_setup_rule_drops_the_position_it_names_and_lets_others_through() {
        let mut generator = SetupAtPosition::new(4218, 100, 101, 4096);

        assert!(generator.should_drop(
            loopback(),
            &setup_frame(4218, 100, 101, 4096),
            SetupFrame::LENGTH
        ));

        // Each field is part of the name: a SETUP a term later, or one offset
        // later, is a different handshake and passes.
        for other in [
            setup_frame(4219, 100, 101, 4096),
            setup_frame(4218, 99, 101, 4096),
            setup_frame(4218, 100, 102, 4096),
            setup_frame(4218, 100, 101, 8192),
        ] {
            assert!(!generator.should_drop(loopback(), &other, SetupFrame::LENGTH));
        }

        assert_eq!(1, generator.dropped());

        // And it never answers the detailed question — data is not its business.
        assert!(!detailed(&mut generator, &data_frame(4218, 1, 0, 32)));
    }

    #[test]
    fn a_range_rule_drops_the_half_open_range_of_one_term() {
        let mut generator = DataInRange::new(4218, 5, 1024, 2048);

        assert!(!detailed(&mut generator, &data_frame(4218, 5, 1023, 32)));
        assert!(detailed(&mut generator, &data_frame(4218, 5, 1024, 32)));
        assert!(detailed(&mut generator, &data_frame(4218, 5, 2047, 32)));
        assert!(!detailed(&mut generator, &data_frame(4218, 5, 2048, 32)));

        // Another term, or another stream, is outside it whatever the offset.
        assert!(!detailed(&mut generator, &data_frame(4218, 6, 1500, 32)));
        assert!(!detailed(&mut generator, &data_frame(4219, 5, 1500, 32)));

        assert_eq!(2, generator.dropped());
    }

    /// ★ A blackout is **bounded**: it ends by itself, and that is the whole of
    /// why it exists — a machine that is retrying drives the count up and the
    /// frames start arriving again with nothing having to be switched off.
    /// A frame of the stream at a position of its own.
    fn nth_frame(stream_id: i32, index: i32) -> Vec<u8> {
        data_frame(stream_id, 1, index * 64, 32)
    }

    #[test]
    fn a_blackout_passes_its_first_frames_and_then_holds() {
        let mut generator = Blackout::new(Some(4218), 2, Duration::ZERO, Duration::from_secs(60));

        // Two through, and then withheld for as long as the window lasts.
        assert!(!detailed(&mut generator, &nth_frame(4218, 0)));
        assert!(!detailed(&mut generator, &nth_frame(4218, 1)));
        assert!(detailed(&mut generator, &nth_frame(4218, 2)));
        assert!(detailed(&mut generator, &nth_frame(4218, 3)));
        assert_eq!(4, generator.matched());

        // Another stream is not what it counts.
        let mut generator = Blackout::new(Some(4218), 0, Duration::ZERO, Duration::from_secs(60));

        assert!(!detailed(&mut generator, &data_frame(4219, 1, 0, 32)));
        assert!(detailed(&mut generator, &nth_frame(4218, 0)));
        assert_eq!(1, generator.matched());
    }

    /// ★★ **The window ends by itself**, and it ends by *time* — which is the
    /// correction measuring forced. A bound counted in frames was spent twice
    /// over: first by the transport's own retransmissions, and then never
    /// reached at all, because withholding everything stops the frames arriving
    /// long before a frame budget is used up.
    #[test]
    fn a_blackout_ends_when_its_window_does() {
        let mut generator = Blackout::new(Some(4218), 0, Duration::ZERO, Duration::from_millis(1));

        assert!(detailed(&mut generator, &nth_frame(4218, 0)));

        std::thread::sleep(Duration::from_millis(5));

        // The window is over: this frame and every one after it go through.
        assert!(!detailed(&mut generator, &nth_frame(4218, 1)));
        assert!(!detailed(&mut generator, &nth_frame(4218, 2)));
    }

    /// ★ **The window is a span, not "everything after the first frames".**
    ///
    /// This is what `pass.first` cannot say and the reason `from` is here: the
    /// reference's response-dropping cases let an archive's handshake and
    /// listing through and then hold **one response** back, and "after the
    /// handshake" is a *moment*. Counting frames names it only if the
    /// handshake's frame count is known, and measured on this machine it is
    /// three *and* four — so a rule that counted could not name it at all.
    ///
    /// A `from` of zero is the rule as it was before it had one, which is what
    /// the other cases here assert with `Duration::ZERO`.
    #[test]
    fn a_blackout_can_open_later_and_lets_what_came_before_it_through() {
        let mut generator = Blackout::new(
            Some(4218),
            0,
            Duration::from_millis(50),
            Duration::from_millis(150),
        );

        // Before the window opens: through, and counted as frames of the stream.
        assert!(!detailed(&mut generator, &nth_frame(4218, 0)));
        assert!(!detailed(&mut generator, &nth_frame(4218, 1)));
        assert_eq!(2, generator.matched());

        std::thread::sleep(Duration::from_millis(80));

        // Inside it: withheld.
        assert!(detailed(&mut generator, &nth_frame(4218, 2)));

        std::thread::sleep(Duration::from_millis(120));

        // And past it: through again, the window having ended by itself.
        assert!(!detailed(&mut generator, &nth_frame(4218, 3)));
    }

    /// ★ A heartbeat inside a blackout passes **and does not count** — a session
    /// that is told nothing while its responses are withheld still has to look
    /// alive, and a bounded drop that swallowed the heartbeats would drop the
    /// connection rather than the responses.
    #[test]
    fn a_heartbeat_is_neither_dropped_nor_counted_by_a_blackout() {
        let mut generator = Blackout::new(Some(4218), 0, Duration::ZERO, Duration::from_secs(60));

        let mut heartbeat = data_frame(4218, 1, 0, 0);
        heartbeat[0..4].copy_from_slice(&0i32.to_le_bytes());

        assert!(!detailed(&mut generator, &heartbeat));
        assert_eq!(0, generator.matched());

        // What the blackout counts is the first *payload* frame, not the first
        // frame — and while its window is open, every payload frame is withheld.
        assert!(detailed(&mut generator, &nth_frame(4218, 0)));
        assert!(!detailed(&mut generator, &heartbeat));
        assert_eq!(1, generator.matched());
        assert!(detailed(&mut generator, &nth_frame(4218, 1)));
        assert_eq!(2, generator.matched());
    }

    /// ★ **A retransmission is the same frame.** The counter that made this real:
    /// a withheld frame is asked for again, and a blackout that counted every
    /// arrival was burned through by those retransmissions long before the
    /// machine it was aimed at retried anything.
    #[test]
    fn a_retransmission_neither_advances_nor_escapes_a_blackout() {
        let mut generator = Blackout::new(Some(4218), 0, Duration::ZERO, Duration::from_secs(60));

        let first = data_frame(4218, 1, 0, 32);

        assert!(detailed(&mut generator, &first));
        assert_eq!(1, generator.matched());

        // The same frame again, ten times: still withheld, and still one frame.
        for _ in 0..10 {
            assert!(detailed(&mut generator, &first));
        }

        assert_eq!(1, generator.matched());

        // And `pass_first` still counts **frames of the stream**: a second
        // distinct frame is the second frame, whatever the transport did with
        // the first.
        let mut generator = Blackout::new(Some(4218), 1, Duration::ZERO, Duration::from_secs(60));

        assert!(!detailed(&mut generator, &nth_frame(4218, 0)));

        for _ in 0..10 {
            assert!(!detailed(&mut generator, &nth_frame(4218, 0)));
        }

        assert!(detailed(&mut generator, &nth_frame(4218, 1)));
        assert_eq!(2, generator.matched());
    }

    /// ★ The skip is what makes "the recording stays whole while one reader's
    /// copy is missing a range" a thing a case can build: the archive's
    /// recording subscription names `init-term-id=` in its channel and a plain
    /// application subscription does not
    /// (`…_data_in_range_loss_generator.h:29-33`).
    #[test]
    fn an_endpoint_the_skip_names_is_given_no_generator_at_all() {
        let spec = Spec {
            data_in_range: Some(DataRange {
                stream_id: 4218,
                active_term_id: 5,
                min_inclusive: 0,
                max_exclusive: 4096,
            }),
            endpoint_skip: Some("init-term-id=".to_owned()),
            ..Spec::default()
        };

        assert!(spec.generator_for(b"aeron:ipc?term-length=64k").is_some());
        assert!(
            spec.generator_for(b"aeron:ipc?term-length=64k|init-term-id=7")
                .is_none()
        );
    }

    /// Which of the five a `Spec` builds, asked through the trait rather than
    /// by type: what a caller depends on is which frames come back dropped.
    #[test]
    fn the_rule_a_spec_builds_is_the_one_its_settings_name() {
        let data = data_frame(4218, 5, 1024, 32);
        let other = data_frame(4219, 5, 1024, 32);
        let setup = setup_frame(4218, 100, 101, 4096);

        let cases: [(Spec, bool, bool, bool); 5] = [
            // A stream, all of it.
            (
                Spec {
                    stream_id: Some(4218),
                    ..Spec::default()
                },
                true,
                false,
                false,
            ),
            // A stream, at a rate.
            (
                Spec {
                    stream_id: Some(4218),
                    every: Some(1),
                    ..Spec::default()
                },
                false,
                false,
                false,
            ),
            // Any stream, at a rate — two frames asked, the second of them
            // dropped, so the first answer below is about `data` only.
            (
                Spec {
                    every: Some(3),
                    ..Spec::default()
                },
                false,
                false,
                false,
            ),
            // One handshake, and no opinion about data.
            (
                Spec {
                    setup_at_position: Some(SetupPosition {
                        stream_id: 4218,
                        initial_term_id: 100,
                        active_term_id: 101,
                        term_offset: 4096,
                    }),
                    ..Spec::default()
                },
                false,
                true,
                false,
            ),
            // One run of one term, and no opinion about handshakes.
            (
                Spec {
                    data_in_range: Some(DataRange {
                        stream_id: 4218,
                        active_term_id: 5,
                        min_inclusive: 1024,
                        max_exclusive: 2048,
                    }),
                    ..Spec::default()
                },
                true,
                false,
                false,
            ),
        ];

        for (spec, drops_data, drops_setup, drops_other_stream) in cases {
            let mut generator = spec
                .generator_for(b"aeron:ipc")
                .expect("every case names a rule");

            assert_eq!(
                drops_data,
                detailed(&mut *generator, &data),
                "data, for {spec:?}"
            );
            assert_eq!(
                drops_other_stream,
                detailed(&mut *generator, &other),
                "another stream, for {spec:?}"
            );
            assert_eq!(
                drops_setup,
                generator.should_drop(loopback(), &setup, SetupFrame::LENGTH),
                "setup, for {spec:?}"
            );
        }

        // And settings that name no rule build nothing.
        assert!(Spec::default().generator_for(b"aeron:ipc").is_none());
    }

    /// ★ A heartbeat carries `EOS` and `REVOKE`, so a subscriber whose last one
    /// is dropped never learns its publisher is gone. The reference lets them
    /// pass and so does this.
    #[test]
    fn a_heartbeat_inside_the_range_still_passes() {
        let mut generator = DataInRange::new(4218, 5, 0, 4096);

        // A header with a **zero** frame length: that, and not merely an empty
        // payload, is what the reference recognises
        // (`…_loss_generator.c:111`).
        let mut heartbeat = data_frame(4218, 5, 1024, 0);
        assert_eq!(DataFrame::LENGTH, heartbeat.len());
        heartbeat[0..4].copy_from_slice(&0i32.to_le_bytes());

        assert!(!detailed(&mut generator, &heartbeat));
        assert_eq!(0, generator.dropped());

        // One byte of payload makes it an ordinary frame again.
        assert!(detailed(&mut generator, &data_frame(4218, 5, 1024, 1)));
        assert_eq!(1, generator.dropped());
    }
}
