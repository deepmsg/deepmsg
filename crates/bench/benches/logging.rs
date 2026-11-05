//! The log facade's caller-side cost, measured on one machine by one harness.
//!
//! This is a manual harness rather than a criterion group, for two reasons:
//! the criterion iteration count is chosen for the timer, not for the disk, and
//! an enabled group would write an unbounded number of lines; and the budget in
//! the L0a plan is a percentile, which this prints directly.
//!
//! The unit is one hot-loop iteration. Six groups run the same loop shape:
//!
//! - `baseline` — no logger at all;
//! - `disabled` — the facade with no global logger installed (a no-op);
//! - `filtered_1_in_1000` — an instance whose ceiling is `off`;
//! - `enabled_1_in_1000` / `enabled_1_in_100` — a real writer, at two rates;
//! - `enabled_1_in_1` — one event per operation, which is the per-event cost
//!   and the tail the amortised groups cannot show.
//!
//! The rates are per 1000 loop iterations, so a run of 200 samples times a
//! 1000-iteration batch writes 200 and 2000 lines respectively. That is enough
//! to exercise the caller path and small enough not to turn the measurement
//! into a disk benchmark.
//!
//! Run with `cargo bench -p deepmsg-bench --bench logging`. The budgets from
//! `analysis/observability/deepmsg-l0a-plan-log-crate.md` C14 are: the disabled
//! path within noise of the baseline, the 1-in-1000 p50 within 1 ns/op of it,
//! and one event's own p50/p99 within 1 µs / 5 µs.

use std::hint::black_box;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Instant;

use deepmsg_log::{Component, Config, Event, Level, Roll, open};

/// Operations per timed sample.
const BATCH: usize = 1000;
/// Samples thrown away before the measured ones.
const WARM_UP: usize = 20;
/// Measured samples; 200 × 1000 = 200 000 operations per group.
const SAMPLES: usize = 200;
/// The per-event group emits on every operation, so it runs shorter: 55 000
/// events is enough for a tail and small enough that the queue never fills.
const EVENT_WARM_UP: usize = 5;
const EVENT_SAMPLES: usize = 50;

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let suffix = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("deepmsg-log-bench-{}-{suffix}", std::process::id()));
        std::fs::create_dir_all(&path).expect("temp directory");
        Self(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn config(directory: &TempDir, level: Level, node_id: &str) -> Config {
    Config {
        dir: directory.0.clone(),
        node_id: node_id.to_owned(),
        component: Component::Driver,
        level,
        component_levels: Vec::new(),
        roll: Roll::default(),
        queue_capacity: 65_536,
        cpu: None,
        pinner: None,
        clock: deepmsg_core::clock::epoch_nano_time,
    }
}

struct Stats {
    p50: f64,
    p99: f64,
}

fn measure(operation: impl FnMut(usize)) -> Stats {
    measure_with(operation, WARM_UP, SAMPLES)
}

fn measure_with(mut operation: impl FnMut(usize), warm_up: usize, samples: usize) -> Stats {
    for index in 0..(warm_up * BATCH) {
        operation(index);
    }

    let mut readings = Vec::with_capacity(samples);
    for sample in 0..samples {
        let start = Instant::now();
        for offset in 0..BATCH {
            operation(sample * BATCH + offset);
        }
        readings.push(start.elapsed().as_nanos() as f64 / BATCH as f64);
    }

    readings.sort_by(f64::total_cmp);
    Stats {
        p50: readings[readings.len() / 2],
        p99: readings[(readings.len() * 99) / 100],
    }
}

fn report(name: &str, stats: Stats) {
    println!(
        "logging {name:<22} p50 {:6.2} ns   p99 {:6.2} ns",
        stats.p50, stats.p99
    );
}

fn main() {
    let directory = TempDir::new();
    let filtered = open(config(&directory, Level::Off, "filtered")).expect("filtered logger");
    let enabled = open(config(&directory, Level::Info, "enabled")).expect("enabled logger");

    report(
        "baseline",
        measure(|index| {
            let value = black_box(index);
            if index % 1000 == 0 {
                black_box(value.wrapping_mul(3));
            }
        }),
    );

    report(
        "disabled",
        measure(|index| {
            if index % 1000 == 0 {
                deepmsg_log::event(Level::Info, Component::Driver, Event::RoleChange)
                    .field_u64("index", index as u64)
                    .emit();
            }
        }),
    );

    report(
        "filtered_1_in_1000",
        measure(|index| {
            if index % 1000 == 0 {
                filtered
                    .event(Level::Info, Component::Driver, Event::RoleChange)
                    .field_u64("index", index as u64)
                    .emit();
            }
        }),
    );

    report(
        "enabled_1_in_1000",
        measure(|index| {
            if index % 1000 == 0 {
                enabled
                    .event(Level::Info, Component::Driver, Event::RoleChange)
                    .field_u64("index", index as u64)
                    .emit();
            }
        }),
    );

    report(
        "enabled_1_in_100",
        measure(|index| {
            if index % 100 == 0 {
                enabled
                    .event(Level::Info, Component::Driver, Event::RoleChange)
                    .field_u64("index", index as u64)
                    .emit();
            }
        }),
    );

    // One event per operation: this is the per-event cost and its tail, not an
    // amortised per-loop figure. The queue is sized so every send takes the
    // normal path; a non-zero drop count would mean the reading measured the
    // full-queue path instead, and is printed rather than hidden.
    report(
        "enabled_1_in_1",
        measure_with(
            |index| {
                enabled
                    .event(Level::Info, Component::Driver, Event::RoleChange)
                    .field_u64("index", index as u64)
                    .emit();
            },
            EVENT_WARM_UP,
            EVENT_SAMPLES,
        ),
    );
    println!("logging enabled_1_in_1 dropped {}", enabled.dropped_lines());

    enabled.shutdown();
    filtered.shutdown();
}
