//! Congestion control: the strategy an image runs, named by the channel's
//! `cc=` (`aeron_congestion_control.c`, 491 lines, and its header's 114).
//!
//! Two strategies, which is all the reference has: `static` is the identity — a
//! window that never moves, and what every image in this build ran before this
//! module existed — and `cubic` is the one that measures a round trip and moves
//! the window with it. A third entry in the reference's name table, `default`,
//! is the *chooser*: it reads the channel's `cc=` and builds one of the two
//! (`:165-205`), and [`Strategy::from_name`] is that chooser's first half.
//!
//! The seven functions of `aeron_congestion_control.h:58-68` are methods here,
//! in the reference's order, and the state each one keeps is the reference's
//! state: a window for `static`, and for `cubic` the same eleven numbers —
//! `cwnd`, `w_max`, `k`, the three timestamps, the RTT estimate — which is why
//! the arithmetic below reads like the C. CUBIC is a pure function of them:
//! no I/O, no concurrency, and the only two things it writes outside itself are
//! the two per-image counters a client reads (`rcv-cc-cubic-rtt` and
//! `rcv-cc-cubic-wnd`).
//!
//! Three settings go with it, and the reference reads all three with `getenv`
//! *inside the supplier* rather than off its context (`:392-402`) — the names
//! are the authority here, and the properties below are their exact inverse
//! (`AERON_CUBICCONGESTIONCONTROL_MEASURERTT` ⇐ `aeron.cubiccongestioncontrol.measurertt`).

use deepmsg_cnc::{CounterManager, CounterRegions, layout};

use crate::config::DriverConfig;
use crate::flowcontrol::receiver_window_length;
use crate::position::{allocate_stream_counter, type_id};

/// The value `cc=` takes when it names the static window
/// (`AERON_STATICWINDOWCONGESTIONCONTROL_CC_PARAM_VALUE`).
pub const STATIC: &str = "static";

/// And when it names CUBIC (`AERON_CUBICCONGESTIONCONTROL_CC_PARAM_VALUE`).
pub const CUBIC: &str = "cubic";

/// CUBIC's constants (`aeron_congestion_control.c:36-42`).
const INITIAL_CWND: i32 = 10;
const C: f64 = 0.4;
const B: f64 = 0.2;
const RTT_TIMEOUT_MULTIPLE: i64 = 4;
pub const INITIAL_RTT_NS_DEFAULT: i64 = 100 * 1000;

/// One second in nanoseconds, as a `double`: every duration in the cubic
/// arithmetic is divided by this before it is cubed (`:36`).
const SECOND_IN_NS: f64 = 1_000_000_000.0;

/// The counter a CUBIC image publishes its last measured round trip in
/// (`AERON_CUBICCONGESTIONCONTROL_RTT_INDICATOR_COUNTER_NAME`).
pub const RTT_COUNTER_NAME: &str = "rcv-cc-cubic-rtt";

/// And the one its current window is read from.
pub const WINDOW_COUNTER_NAME: &str = "rcv-cc-cubic-wnd";

/// Which supplier a **driver** names for its images
/// (`aeron_congestion_control_strategy_supplier_load`, `:45-62`, loaded from
/// the context at `aeron_driver_context.c:579-585`).
///
/// The three names are the reference's table, and the first is the default: a
/// driver that names none gets the *chooser*, which is what makes a channel's
/// `cc=` mean anything. Naming `static` or `cubic` here is naming the supplier
/// **instead** — every image runs that one, whatever its channel says — and a
/// name the table does not carry fails the driver at start-up, which is what
/// the reference's `goto error` does with an unfindable symbol.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Supplier {
    /// `aeron_congestion_control_default_strategy_supplier`: the channel's
    /// `cc=` decides, per image.
    #[default]
    Default,
    /// `aeron_static_window_congestion_control_strategy_supplier`.
    Static,
    /// `aeron_cubic_congestion_control_strategy_supplier`.
    Cubic,
}

impl Supplier {
    /// The supplier a name names, or `None` for one this build cannot load —
    /// the reference's `NULL` from its symbol table.
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "default" => Some(Self::Default),
            STATIC => Some(Self::Static),
            CUBIC => Some(Self::Cubic),
            _ => None,
        }
    }

    /// The strategy this supplier builds for a channel
    /// (`aeron_congestion_control_default_strategy_supplier`, `:165-205`): the
    /// chooser reads `cc=`, and either of the other two already knows.
    ///
    /// `None` for a channel the chooser cannot serve, which is the reference's
    /// `result` left at `-1` and the caller's to report.
    pub fn strategy(self, congestion_control: Option<&str>) -> Option<Strategy> {
        match self {
            Self::Default => Strategy::from_name(congestion_control),
            Self::Static => Some(Strategy::Static),
            Self::Cubic => Some(Strategy::Cubic),
        }
    }
}

/// Which strategy a channel's `cc=` names
/// (`aeron_congestion_control_default_strategy_supplier`, `:165-205`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Strategy {
    /// The channel named nothing, or named `static`.
    Static,
    /// The channel named `cubic`.
    Cubic,
}

impl Strategy {
    /// The reference's chooser: a channel that names nothing gets the static
    /// window, `static` and `cubic` get themselves, and **any other name is
    /// `None`** — which is the reference's `result` left at `-1`, with no error
    /// set anywhere (`:200-204`), so what a client hears about a name this
    /// build cannot serve is decided by the caller at image creation and not
    /// here.
    ///
    /// The comparison is the whole string, not a prefix: the reference compares
    /// `sizeof("static")` bytes, NUL included, so `cc=static1234` matches
    /// nothing (`:172`, and `defaultStrategySupplierShouldReturnNegativeResultWhenCcParamIsUnknown`).
    pub fn from_name(name: Option<&str>) -> Option<Self> {
        match name {
            None | Some(STATIC) => Some(Self::Static),
            Some(CUBIC) => Some(Self::Cubic),
            Some(_) => None,
        }
    }
}

/// What one rebuild decided
/// (`aeron_congestion_control_strategy_on_track_rebuild_func_t`'s two outputs).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Rebuild {
    /// The window the status message about this rebuild carries.
    pub window_length: i32,
    /// Whether the status message has to go **now** rather than on the
    /// quarter-window rule (`*should_force_sm`).
    pub should_force_sm: bool,
}

/// The strategy an image runs — the seven functions of
/// `aeron_congestion_control.h:58-68`, as methods.
#[derive(Debug)]
pub enum CongestionControl {
    /// A window that never moves.
    Static(StaticWindow),
    /// The one that measures.
    Cubic(Cubic),
}

impl CongestionControl {
    /// Build the strategy for a channel (`aeron_static_window_…_supplier`,
    /// `:128-160`, and `aeron_cubic_…_supplier`, `:361-491`).
    ///
    /// `channel_window_length` is the **endpoint channel's** receiver window —
    /// its own `rcv-wnd=` or the driver's default
    /// (`aeron_udp_channel_receiver_window`, read by the reference at
    /// `aeron_driver_conductor.c:6631`, which hands the supplier
    /// `endpoint->conductor_fields.udp_channel` and not the subscription's).
    ///
    /// # Errors
    ///
    /// `None` when the strategy cannot be built: CUBIC's initial RTT is not a
    /// duration, or a counter cannot be allocated
    /// (`cubicCongestionControlSupplierReturnsNegativeValueIfInitialRttIsInvalid`).
    /// The reference answers the same way — `result < 0` from the supplier,
    /// which its conductor turns into a failed image create — and the caller
    /// here decides what the client is told.
    #[allow(clippy::too_many_arguments)] // the supplier's arguments, one per field
    pub fn create(
        strategy: Strategy,
        config: &DriverConfig,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        registration_id: i64,
        session_id: i32,
        stream_id: i32,
        channel: &[u8],
        mtu_length: i32,
        term_length: i32,
        channel_window_length: i32,
        now_ms: i64,
        now_ns: i64,
    ) -> Option<Self> {
        let window = window_for_channel(channel_window_length, term_length);

        match strategy {
            Strategy::Static => Some(Self::Static(StaticWindow {
                window_length: window,
            })),
            Strategy::Cubic => Cubic::new(
                config,
                counters,
                regions,
                registration_id,
                session_id,
                stream_id,
                channel,
                mtu_length,
                window,
                now_ms,
                now_ns,
            )
            .map(Self::Cubic),
        }
    }

    /// The identity, built from the two lengths alone — `create`'s own
    /// `Strategy::Static` arm, for a caller that has a channel's window and its
    /// term and nothing else to read (a test, or a construction that has
    /// already decided the strategy).
    pub fn static_window(channel_window_length: i32, term_length: i32) -> Self {
        Self::Static(StaticWindow {
            window_length: window_for_channel(channel_window_length, term_length),
        })
    }

    /// Whether a round trip is worth measuring right now
    /// (`should_measure_rtt`).
    pub fn should_measure_rtt(&self, now_ns: i64) -> bool {
        match self {
            Self::Static(_) => false,
            Self::Cubic(cubic) => cubic.should_measure_rtt(now_ns),
        }
    }

    /// A measurement went out (`on_rttm_sent`).
    pub fn on_rttm_sent(&mut self, now_ns: i64) {
        if let Self::Cubic(cubic) = self {
            cubic.on_rttm_sent(now_ns);
        }
    }

    /// One came back (`on_rttm`): `rtt_ns` is what the frame's echo timestamp
    /// and reception delta worked out to
    /// (`aeron_publication_image.c:849-857`).
    pub fn on_rttm(
        &mut self,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
        now_ns: i64,
        rtt_ns: i64,
    ) {
        if let Self::Cubic(cubic) = self {
            cubic.on_rttm(counters, regions, now_ns, rtt_ns);
        }
    }

    /// A rebuild, and what window it leaves behind (`on_track_rebuild`).
    #[allow(clippy::too_many_arguments)] // the reference's own list of inputs
    pub fn on_track_rebuild(
        &mut self,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
        now_ns: i64,
        new_consumption_position: i64,
        last_sm_position: i64,
        hwm_position: i64,
        starting_rebuild_position: i64,
        ending_rebuild_position: i64,
        loss_occurred: bool,
    ) -> Rebuild {
        match self {
            Self::Static(window) => Rebuild {
                window_length: window.window_length,
                should_force_sm: false,
            },
            Self::Cubic(cubic) => cubic.on_track_rebuild(
                counters,
                regions,
                now_ns,
                new_consumption_position,
                last_sm_position,
                hwm_position,
                starting_rebuild_position,
                ending_rebuild_position,
                loss_occurred,
            ),
        }
    }

    /// The window an image's first status message carries
    /// (`initial_window_length`).
    pub const fn initial_window_length(&self) -> i32 {
        match self {
            Self::Static(window) => window.window_length,
            Self::Cubic(cubic) => cubic.initial_window_length,
        }
    }

    /// The largest window the strategy will ever ask for
    /// (`max_window_length`).
    pub const fn max_window_length(&self) -> i32 {
        match self {
            Self::Static(window) => window.window_length,
            Self::Cubic(cubic) => cubic.max_window_length,
        }
    }

    /// The per-image counters this strategy took, for the caller that gives
    /// them back (`aeron_cubic_…_fini`, `:342-352`). The static strategy takes
    /// none — the reference allocates none for it.
    pub fn counter_ids(&self) -> &[i32] {
        match self {
            Self::Static(_) => &[],
            Self::Cubic(cubic) => &cubic.counters,
        }
    }
}

/// The window a channel resolves to
/// (`aeron_udp_channel_receiver_window` + `aeron_receiver_window_length`,
/// `:155-157`): the channel's own, capped at half a term — the cap is what
/// makes a small term the thing that limits a window rather than the buffer.
fn window_for_channel(channel_window_length: i32, term_length: i32) -> i32 {
    #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
    let window = receiver_window_length(
        channel_window_length.unsigned_abs() as usize,
        term_length.unsigned_abs() as usize,
    ) as i32;

    window
}

/// `static`'s whole state: the window (`:65-68`).
#[derive(Clone, Copy, Debug)]
pub struct StaticWindow {
    window_length: i32,
}

/// CUBIC's state (`aeron_cubic_congestion_control_strategy_state_stct`,
/// `:210-240`), field for field.
#[derive(Debug)]
pub struct Cubic {
    tcp_mode: bool,
    measure_rtt: bool,

    initial_window_length: i32,
    max_window_length: i32,
    mtu: i32,
    max_cwnd: i32,
    cwnd: i32,
    w_max: i32,
    k: f64,

    initial_rtt_ns: i64,
    rtt_ns: i64,
    rtt_timeout_ns: i64,
    window_update_timeout_ns: i64,
    last_loss_timestamp_ns: i64,
    last_update_timestamp_ns: i64,
    last_rtt_timestamp_ns: i64,

    /// `rcv-cc-cubic-rtt` and `rcv-cc-cubic-wnd`, in the order the reference
    /// allocates them (`:432-462`).
    counters: [i32; 2],
}

impl Cubic {
    /// The supplier (`aeron_cubic_congestion_control_strategy_supplier`,
    /// `:361-491`): read the three settings, take the two lengths off the
    /// channel, seed the window, allocate the two counters.
    #[allow(clippy::too_many_arguments)] // the supplier's arguments, one per field
    fn new(
        config: &DriverConfig,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        registration_id: i64,
        session_id: i32,
        stream_id: i32,
        channel: &[u8],
        mtu_length: i32,
        max_window_length: i32,
        now_ms: i64,
        now_ns: i64,
    ) -> Option<Self> {
        // The three settings, read where the reference reads them (`:392-402`).
        let initial_rtt_ns = config.cubic_initial_rtt_ns()?;

        let mtu = mtu_length.max(1);
        let max_cwnd = max_window_length / mtu;
        let cwnd = INITIAL_CWND.min(max_cwnd);

        // The two counters are per **image**, with no client as their owner —
        // `AERON_NULL_VALUE` in the reference's `aeron_stream_counter_allocate`
        // (`:437`, `:455`).
        let allocate = |counters: &mut CounterManager, name: &str| {
            allocate_stream_counter(
                counters,
                regions,
                name,
                type_id::PER_IMAGE,
                layout::NULL_VALUE,
                registration_id,
                session_id,
                stream_id,
                channel,
                "",
                now_ms,
            )
        };

        let rtt_counter = allocate(counters, RTT_COUNTER_NAME)?;
        let Some(window_counter) = allocate(counters, WINDOW_COUNTER_NAME) else {
            let _ = counters.free(regions, rtt_counter, now_ms);
            return None;
        };

        let initial_window_length = cwnd * mtu;

        let _ = counters.set_value(regions, rtt_counter, 0);
        let _ = counters.set_value(regions, window_counter, i64::from(initial_window_length));

        Some(Self {
            tcp_mode: config.cubic_tcp_mode,
            measure_rtt: config.cubic_measure_rtt,
            initial_window_length,
            max_window_length,
            mtu,
            max_cwnd,
            cwnd,
            // Initially the maximum and acting in the TCP and concave region
            // (`:425-427`).
            w_max: max_cwnd,
            k: cbrt(f64::from(max_cwnd) * B / C),
            initial_rtt_ns,
            rtt_ns: initial_rtt_ns,
            window_update_timeout_ns: initial_rtt_ns,
            rtt_timeout_ns: initial_rtt_ns * RTT_TIMEOUT_MULTIPLE,
            // The two timestamps are the receiver clock's reading at creation
            // (`:464-466`), which the caller passes in.
            last_loss_timestamp_ns: now_ns,
            last_update_timestamp_ns: now_ns,
            last_rtt_timestamp_ns: 0,
            counters: [rtt_counter, window_counter],
        })
    }

    /// `(last_rtt_timestamp_ns + rtt_timeout_ns) - now_ns < 0` (`:243-247`):
    /// due when the timeout has **passed**, so the same instant is not yet due.
    ///
    /// The arithmetic wraps as the C's does — these are clock readings added to
    /// timeouts, and a build that panicked on the sum would be a driver that
    /// stops on a clock it did not choose.
    fn should_measure_rtt(&self, now_ns: i64) -> bool {
        self.measure_rtt
            && self
                .last_rtt_timestamp_ns
                .wrapping_add(self.rtt_timeout_ns)
                .wrapping_sub(now_ns)
                < 0
    }

    /// `on_rttm_sent` (`:249-256`).
    fn on_rttm_sent(&mut self, now_ns: i64) {
        self.last_rtt_timestamp_ns = now_ns;
    }

    /// `on_rttm` (`:258-278`): the estimate, the counter, and a timeout that
    /// is never shorter than the initial one.
    fn on_rttm(
        &mut self,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
        now_ns: i64,
        rtt_ns: i64,
    ) {
        self.last_rtt_timestamp_ns = now_ns;
        self.rtt_ns = rtt_ns;
        let _ = counters.set_value(regions, self.counters[0], rtt_ns);
        self.rtt_timeout_ns = rtt_ns.max(self.initial_rtt_ns) * RTT_TIMEOUT_MULTIPLE;
    }

    /// `on_track_rebuild` (`:280-334`).
    #[allow(clippy::too_many_arguments)] // the reference's own list of inputs
    fn on_track_rebuild(
        &mut self,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
        now_ns: i64,
        new_consumption_position: i64,
        last_sm_position: i64,
        _hwm_position: i64,
        _starting_rebuild_position: i64,
        _ending_rebuild_position: i64,
        loss_occurred: bool,
    ) -> Rebuild {
        let mut should_force_sm = false;

        if loss_occurred {
            // Multiplicative decrease, and the window this loss came from is
            // where the curve grows back towards (`:294-301`).
            should_force_sm = true;
            self.w_max = self.cwnd;
            self.k = cbrt(f64::from(self.w_max) * B / C);

            #[allow(clippy::cast_possible_truncation)] // the C truncates too
            let cwnd = (f64::from(self.cwnd) * (1.0 - B)) as i32;
            self.cwnd = if cwnd > 1 { cwnd } else { 1 };
            self.last_loss_timestamp_ns = now_ns;
        } else if self.cwnd < self.max_cwnd
            && self
                .last_update_timestamp_ns
                .wrapping_add(self.window_update_timeout_ns)
                .wrapping_sub(now_ns)
                < 0
        {
            // `W_cubic = C(T - K)^3 + w_max` (`:304-312`).
            let duration_since_decr =
                (now_ns.wrapping_sub(self.last_loss_timestamp_ns) as f64) / SECOND_IN_NS;
            let diff_to_k = duration_since_decr - self.k;
            let incr = C * diff_to_k * diff_to_k * diff_to_k;

            #[allow(clippy::cast_possible_truncation)] // the C truncates too
            let cwnd = self.w_max + incr as i32;
            self.cwnd = if cwnd < self.max_cwnd {
                cwnd
            } else {
                self.max_cwnd
            };

            // The TCP-friendly region, which is what `tcp_mode` buys: while the
            // cubic window is still below where it started, a window that grows
            // at the AIMD rate is taken instead if it is larger (`:315-324`).
            if self.tcp_mode && self.cwnd < self.w_max {
                let rtt_in_seconds = (self.rtt_ns as f64) / SECOND_IN_NS;
                let w_tcp = f64::from(self.w_max) * (1.0 - B)
                    + ((3.0 * B / (2.0 - B)) * (duration_since_decr / rtt_in_seconds));

                #[allow(clippy::cast_possible_truncation)] // the C truncates too
                let new_cwnd = w_tcp as i32;
                self.cwnd = if new_cwnd > self.cwnd {
                    new_cwnd
                } else {
                    self.cwnd
                };
            }

            self.last_update_timestamp_ns = now_ns;
        } else if 1 == self.cwnd && new_consumption_position > last_sm_position {
            // A window of one MTU is a window the sender is waiting on: the
            // reader moving means another status message is owed now (`:326-330`).
            should_force_sm = true;
        }

        let window_length = self.cwnd * self.mtu;
        let _ = counters.set_value(regions, self.counters[1], i64::from(window_length));

        Rebuild {
            window_length,
            should_force_sm,
        }
    }
}

/// C's `cbrt`, named so the arithmetic above reads like the C.
fn cbrt(value: f64) -> f64 {
    value.cbrt()
}

#[cfg(test)]
mod tests {
    use super::*;
    use deepmsg_core::buffer::AtomicBuffer;

    /// A counter manager over buffers that live as long as the fixture, as the
    /// other modules' tests have it.
    struct Counters {
        metadata: Vec<u8>,
        values: Vec<u8>,
    }

    impl Counters {
        fn new() -> Self {
            Self {
                metadata: vec![0u8; 64 * 1024 * 4],
                values: vec![0u8; 64 * 1024],
            }
        }

        fn open(&mut self) -> CounterRegions<'_> {
            CounterRegions::new(
                AtomicBuffer::from_slice_mut(&mut self.metadata).expect("aligned"),
                AtomicBuffer::from_slice_mut(&mut self.values).expect("aligned"),
            )
            .expect("four-to-one")
        }
    }

    /// The reference's own channel in the cubic golden test
    /// (`defaultStrategySupplierShouldChooseCubicCongestionControlWhenCcParamIsCubic`):
    /// `cc=cubic|rcv-wnd=65536`, mtu 1408, term 65536 << 2.
    const GOLDEN_CHANNEL: &[u8] = b"aeron:udp?endpoint=192.168.0.1:9999|cc=cubic|rcv-wnd=65536";
    const GOLDEN_MTU: i32 = 1408;
    const GOLDEN_TERM: i32 = 65536 << 2;
    const GOLDEN_WINDOW: i32 = 65536;

    fn config() -> DriverConfig {
        DriverConfig::default()
    }

    struct Fixture {
        holder: Counters,
        manager: CounterManager,
    }

    impl Fixture {
        fn new() -> Self {
            Self {
                holder: Counters::new(),
                manager: CounterManager::new(64 * 1024, 1_000).expect("room"),
            }
        }

        #[allow(clippy::too_many_arguments)]
        fn create(
            &mut self,
            strategy: Strategy,
            config: &DriverConfig,
            mtu: i32,
            term: i32,
            window: i32,
        ) -> Option<CongestionControl> {
            let regions = self.holder.open();

            CongestionControl::create(
                strategy,
                config,
                &mut self.manager,
                &regions,
                11,
                5,
                42,
                GOLDEN_CHANNEL,
                mtu,
                term,
                window,
                0,
                1_000,
            )
        }
    }

    /// The context's supplier overrides the channel: naming `static` or
    /// `cubic` as the **supplier** means every image runs that one, and only
    /// `default` — the driver's own default — reads `cc=` per channel
    /// (`aeron_congestion_control.c:45-62`, `:165-205`).
    #[test]
    fn a_named_supplier_decides_for_every_image_and_the_default_reads_the_channel() {
        assert_eq!(
            Some(Strategy::Cubic),
            Supplier::Default.strategy(Some("cubic"))
        );
        assert_eq!(None, Supplier::Default.strategy(Some("nonsense")));
        assert_eq!(Some(Strategy::Static), Supplier::Default.strategy(None));

        assert_eq!(
            Some(Strategy::Static),
            Supplier::Static.strategy(Some("cubic")),
            "a channel cannot argue with the driver's supplier"
        );
        assert_eq!(
            Some(Strategy::Cubic),
            Supplier::Cubic.strategy(None),
            "and a channel that names nothing gets it all the same"
        );

        assert_eq!(Some(Supplier::Default), Supplier::from_name("default"));
        assert_eq!(Some(Supplier::Static), Supplier::from_name("static"));
        assert_eq!(Some(Supplier::Cubic), Supplier::from_name("cubic"));
        assert_eq!(
            None,
            Supplier::from_name("aeron_cubic_congestion_control_strategy_supplier"),
            "a symbol this build cannot load is a name it cannot serve"
        );
    }

    /// `cc=` names the strategy by its **whole** string: nothing and `static`
    /// are the static window, `cubic` is CUBIC, and anything else is a name
    /// this build cannot serve (`aeron_congestion_control.c:165-205`;
    /// `defaultStrategySupplierShouldReturnNegativeResultWhenCcParamIsUnknown`
    /// is the reference's own case for the third).
    #[test]
    fn a_channel_names_the_strategy_it_wants() {
        assert_eq!(Some(Strategy::Static), Strategy::from_name(None));
        assert_eq!(Some(Strategy::Static), Strategy::from_name(Some("static")));
        assert_eq!(Some(Strategy::Cubic), Strategy::from_name(Some("cubic")));
        assert_eq!(None, Strategy::from_name(Some("static1234")));
        assert_eq!(None, Strategy::from_name(Some("CUBIC")));
        assert_eq!(None, Strategy::from_name(Some("")));
    }

    /// The static window is the channel's receiver window capped at half a
    /// term, and it never moves — the identity every image in this build ran
    /// before this module existed.
    #[test]
    fn the_static_window_is_the_channels_and_never_moves() {
        let mut fixture = Fixture::new();
        let mut strategy = fixture
            .create(
                Strategy::Static,
                &config(),
                GOLDEN_MTU,
                64 * 1024,
                32 * 1024,
            )
            .expect("a strategy");

        assert_eq!(32 * 1024, strategy.initial_window_length());
        assert_eq!(32 * 1024, strategy.max_window_length());
        assert!(!strategy.should_measure_rtt(100));
        assert!(
            strategy.counter_ids().is_empty(),
            "static takes no counters"
        );

        let regions = fixture.holder.open();
        let rebuilt =
            strategy.on_track_rebuild(&fixture.manager, &regions, 1_000, 1, 0, 10_000, 0, 1, true);

        assert_eq!(
            Rebuild {
                window_length: 32 * 1024,
                should_force_sm: false,
            },
            rebuilt,
            "a loss changes nothing about a window that cannot move"
        );

        // Half a term is the cap, and it is a cap rather than an average:
        // asking for a term's worth gets half of it (`aeron_receiver_window_length`).
        let capped = fixture
            .create(Strategy::Static, &config(), GOLDEN_MTU, 8_096, 131_072)
            .expect("a strategy");
        assert_eq!(4_048, capped.initial_window_length());
    }

    /// The cubic golden case, value for value
    /// (`defaultStrategySupplierShouldChooseCubicCongestionControlWhenCcParamIsCubic`):
    /// `max_cwnd = rcv-wnd / mtu = 46`, the first window is `10 * mtu`, the two
    /// per-image counters exist — `rcv-cc-cubic-wnd` at that first window,
    /// `rcv-cc-cubic-rtt` at zero — and **nothing is measured**: `measure_rtt`
    /// is off unless a setting turns it on.
    #[test]
    fn cubic_starts_at_ten_mtus_and_measures_nothing_by_default() {
        let mut fixture = Fixture::new();
        let strategy = fixture
            .create(
                Strategy::Cubic,
                &config(),
                GOLDEN_MTU,
                GOLDEN_TERM,
                GOLDEN_WINDOW,
            )
            .expect("a strategy");

        assert!(
            !strategy.should_measure_rtt(777),
            "the reference's own assertion, at its own instant"
        );
        assert_eq!(GOLDEN_MTU * 10, strategy.initial_window_length());
        assert_eq!(
            GOLDEN_WINDOW,
            strategy.max_window_length(),
            "the channel's rcv-wnd, under half a term"
        );

        let regions = fixture.holder.open();
        let ids = strategy.counter_ids();
        assert_eq!(2, ids.len(), "cubic takes the two per-image counters");

        let window_counter = fixture
            .manager
            .value(&regions, ids[1])
            .expect("an allocated counter");
        let rtt_counter = fixture
            .manager
            .value(&regions, ids[0])
            .expect("an allocated counter");

        assert_eq!(i64::from(GOLDEN_MTU * 10), window_counter);
        assert_eq!(0, rtt_counter);

        // `aeron_cubic_congestion_control_strategy_get_max_cwnd`'s own answer.
        assert_eq!(GOLDEN_WINDOW / GOLDEN_MTU, 46);
    }

    /// The three settings, on
    /// (`cubicCongestionControlStrategyConfiguration`): `initial_rtt = 1s`,
    /// `measure_rtt` and `tcp_mode` true. The window starts at two MTUs here
    /// rather than ten, because a term of 8096 caps the channel's window at
    /// 4048 — and the measurement cycle is the reference's, instant for
    /// instant.
    #[test]
    fn the_settings_turn_measurement_and_the_tcp_region_on() {
        let mut config = config();
        config.cubic_initial_rtt = Some("1s".to_owned());
        config.cubic_measure_rtt = true;
        config.cubic_tcp_mode = true;

        let mut fixture = Fixture::new();
        let mut strategy = fixture
            .create(Strategy::Cubic, &config, GOLDEN_MTU, 8_096, 131_072)
            .expect("a strategy");

        let regions = fixture.holder.open();
        let ids = strategy.counter_ids().to_vec();

        assert_eq!(
            i64::from(GOLDEN_MTU * 2),
            fixture.manager.value(&regions, ids[1]).expect("a counter"),
            "4048 / 1408 == 2 congestion windows"
        );

        // Due at ten seconds: the timeout is the initial RTT times four, so
        // four seconds after a timestamp of zero.
        assert!(strategy.should_measure_rtt(10_000_000_000));

        strategy.on_rttm_sent(10_000_000_000);
        assert!(
            !strategy.should_measure_rtt(10_000_000_000),
            "the same instant is not yet due again"
        );

        strategy.on_rttm(&fixture.manager, &regions, 20_000_000_000, 555);
        assert_eq!(
            555,
            fixture.manager.value(&regions, ids[0]).expect("a counter"),
            "the estimate is what the counter carries"
        );

        assert!(
            strategy.should_measure_rtt(30_000_000_000),
            "555ns is below the initial RTT, so the timeout stays at four seconds"
        );
    }

    /// An initial RTT that is not a duration fails the supplier
    /// (`cubicCongestionControlSupplierReturnsNegativeValueIfInitialRttIsInvalid`).
    #[test]
    fn an_initial_rtt_that_is_not_a_duration_fails_the_supplier() {
        let mut config = config();
        config.cubic_initial_rtt = Some("initial_rtt wrong value".to_owned());

        let mut fixture = Fixture::new();

        assert!(
            fixture
                .create(
                    Strategy::Cubic,
                    &config,
                    GOLDEN_MTU,
                    GOLDEN_TERM,
                    GOLDEN_WINDOW
                )
                .is_none(),
            "the reference's `result < 0`, which is also its `strategy == NULL`"
        );
    }

    /// A loss halves the window and points the curve at where it was, and the
    /// status message is forced: the reader has to be told the window shrank
    /// (`aeron_congestion_control.c:286-301`).
    #[test]
    fn a_loss_shrinks_the_window_and_forces_a_status_message() {
        let mut fixture = Fixture::new();
        let mut strategy = fixture
            .create(
                Strategy::Cubic,
                &config(),
                GOLDEN_MTU,
                GOLDEN_TERM,
                GOLDEN_WINDOW,
            )
            .expect("a strategy");

        let regions = fixture.holder.open();
        let ids = strategy.counter_ids().to_vec();

        // Ten MTUs in, so the decrease is visible: 10 * 0.8 = 8.
        let rebuilt =
            strategy.on_track_rebuild(&fixture.manager, &regions, 5_000, 0, 0, 0, 0, 0, true);

        assert!(rebuilt.should_force_sm, "a loss is news");
        assert_eq!(8 * GOLDEN_MTU, rebuilt.window_length);
        assert_eq!(
            i64::from(8 * GOLDEN_MTU),
            fixture.manager.value(&regions, ids[1]).expect("a counter"),
            "and the window counter a client reads says the same"
        );

        // A rebuild with no loss and no timeout does not move it: the window
        // update interval is the initial RTT, and no time has passed.
        let quiet =
            strategy.on_track_rebuild(&fixture.manager, &regions, 5_000, 0, 0, 0, 0, 0, false);

        assert!(!quiet.should_force_sm);
        assert_eq!(8 * GOLDEN_MTU, quiet.window_length);
    }

    /// A window of one MTU is a window the sender is stuck on, so a reader that
    /// moves forces a status message even though nothing else changed
    /// (`aeron_congestion_control.c:326-330`).
    #[test]
    fn a_one_mtu_window_lets_a_reader_move_it() {
        let mut fixture = Fixture::new();
        // A term small enough that the window can be one MTU: 2 * 1408 = 2816,
        // so `max_cwnd` is 2 and the first loss takes it to 1.
        let mut strategy = fixture
            .create(Strategy::Cubic, &config(), GOLDEN_MTU, 8_096, 8_096)
            .expect("a strategy");

        let regions = fixture.holder.open();

        assert_eq!(2 * GOLDEN_MTU, strategy.initial_window_length());

        let shrunk = strategy.on_track_rebuild(&fixture.manager, &regions, 1, 0, 0, 0, 0, 0, true);
        assert_eq!(
            GOLDEN_MTU, shrunk.window_length,
            "cwnd 2 * 0.8 is 1.6, and the C truncates it to 1"
        );
        assert!(shrunk.should_force_sm);

        let at_the_floor =
            strategy.on_track_rebuild(&fixture.manager, &regions, 1, 0, 0, 0, 0, 0, true);
        assert_eq!(
            GOLDEN_MTU, at_the_floor.window_length,
            "and 1 * 0.8 truncates to 0, which the `cwnd > 1 ? cwnd : 1` keeps at one"
        );

        let moved = strategy.on_track_rebuild(&fixture.manager, &regions, 2, 10, 5, 0, 0, 0, false);

        assert!(
            moved.should_force_sm,
            "a reader at the only window there is has to be answered"
        );

        let still = strategy.on_track_rebuild(&fixture.manager, &regions, 2, 5, 5, 0, 0, 0, false);

        assert!(!still.should_force_sm, "and one that has not moved is not");
    }

    /// The counters a cubic strategy took are the caller's to give back, and
    /// they are per-image: `PER_IMAGE` is the type id the reference uses, and
    /// the owner is nobody (`AERON_NULL_VALUE`).
    #[test]
    fn the_two_counters_are_per_image_and_owned_by_nobody() {
        let mut fixture = Fixture::new();
        let strategy = fixture
            .create(
                Strategy::Cubic,
                &config(),
                GOLDEN_MTU,
                GOLDEN_TERM,
                GOLDEN_WINDOW,
            )
            .expect("a strategy");

        let regions = fixture.holder.open();
        let reader = regions.reader();

        for id in strategy.counter_ids() {
            let descriptor = reader.get(*id).expect("an allocated record");

            assert!(
                descriptor.label.starts_with(RTT_COUNTER_NAME)
                    || descriptor.label.starts_with(WINDOW_COUNTER_NAME),
                "a cubic counter is named for what it carries: {}",
                descriptor.label
            );
            assert_eq!(
                layout::NULL_VALUE,
                descriptor.owner_id,
                "nobody owns a per-image counter"
            );
            assert_eq!(type_id::PER_IMAGE, descriptor.type_id);
            assert_eq!(11, descriptor.registration_id);
        }
    }

    /// `PER_IMAGE` is the reference's own type id, and it is not one of the
    /// four this build already carried (`aeron_counters.h:95`).
    #[test]
    fn the_per_image_type_id_is_the_references() {
        assert_eq!(10, type_id::PER_IMAGE);
        assert_ne!(type_id::RECEIVER_HWM, type_id::PER_IMAGE);
    }
}
