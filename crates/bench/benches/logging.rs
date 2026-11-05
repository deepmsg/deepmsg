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
//! - `enabled_1_in_1000` / `enabled_1_in_100` — a real writer, at two rates.
//!
//! The rates are per 1000 loop iterations, so a run of 200 samples times a
//! 1000-iteration batch writes 200 and 2000 lines respectively. That is enough
//! to exercise the caller path and small enough not to turn the measurement
//! into a disk benchmark.
//!
//! Run with `cargo bench -p deepmsg-bench --bench logging`. The budgets from
//! `analysis/observability/deepmsg-l0a-plan-log-crate.md` C14 are: the disabled
//! path within noise of the baseline, and the low-rate enabled path's p99
//! within 2% of it.

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
        queue_capacity: 8192,
        cpu: None,
        pinner: None,
        clock: deepmsg_core::clock::epoch_nano_time,
    }
}

struct Stats {
    p50: f64,
    p99: f64,
}

fn measure(mut operation: impl FnMut(usize)) -> Stats {
    for index in 0..(WARM_UP * BATCH) {
        operation(index);
    }

    let mut samples = Vec::with_capacity(SAMPLES);
    for sample in 0..SAMPLES {
        let start = Instant::now();
        for offset in 0..BATCH {
            operation(sample * BATCH + offset);
        }
        samples.push(start.elapsed().as_nanos() as f64 / BATCH as f64);
    }

    samples.sort_by(f64::total_cmp);
    Stats {
        p50: samples[samples.len() / 2],
        p99: samples[(samples.len() * 99) / 100],
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

    enabled.shutdown();
    filtered.shutdown();
}
