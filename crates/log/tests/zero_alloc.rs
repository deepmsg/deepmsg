//! The calling thread must not allocate: ADR-0003, and the L0a criterion C4.
//!
//! This is its own test binary because a `#[global_allocator]` is per binary,
//! and because the reading must be this thread's. The counting allocator is
//! therefore thread-local: the writer thread allocates its batch buffer by
//! design, and that must not count against the callers.
//!
//! The run has two parts. A warm-up lets every lazily created structure settle
//! (the channel is allocated by `open`, but the writer's first batch is not),
//! then the counter is reset, then 100 000 events go through the same public
//! builder a component will use. The assertion is **zero**.
//!
//! If this test ever fails, the queue is the first suspect: `Queue` documents
//! the assumption that `sync_channel` allocates its buffer at creation and
//! never per send. The fallback named in the design is a preallocated
//! `Mutex<VecDeque<Record>>` behind the same interface.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};

use deepmsg_log::{Component, Config, Event, Level, Roll, open};

thread_local! {
    /// One thread's allocation count. `const` so reading it cannot itself
    /// allocate before the counter exists.
    static ALLOCATIONS: Cell<u64> = const { Cell::new(0) };
}

struct Counting;

impl Counting {
    fn count_one() {
        // `try_with`: an allocation during thread teardown (or before this
        // thread's TLS exists) must not panic inside the allocator.
        let _ = ALLOCATIONS.try_with(|cell| cell.set(cell.get() + 1));
    }
}

// SAFETY: every method forwards to `System`, the allocator this wrapper
// replaces; the only added work is a thread-local counter.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        Self::count_one();
        // SAFETY: `System.alloc` has the same contract as this method's, and
        // the layout is the caller's.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        Self::count_one();
        // SAFETY: as in `alloc`.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        Self::count_one();
        // SAFETY: as in `alloc`; the pointer and sizes are the caller's.
        unsafe { System.realloc(ptr, layout, new_size) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: as in `alloc`; deallocation has no counter.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

fn allocations() -> u64 {
    ALLOCATIONS.with(Cell::get)
}

fn frozen_clock() -> i64 {
    0
}

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let suffix = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "deepmsg-log-zero-alloc-{}-{suffix}",
            std::process::id()
        ));
        fs::create_dir_all(&path).expect("temp directory");
        Self(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn emitting_does_not_allocate_on_the_calling_thread() {
    let directory = TempDir::new();
    let config = Config {
        dir: directory.0.clone(),
        node_id: "node-1".to_owned(),
        component: Component::Driver,
        level: Level::Info,
        component_levels: Vec::new(),
        roll: Roll::default(),
        queue_capacity: 8192,
        cpu: None,
        pinner: None,
        clock: frozen_clock,
    };
    let handle = open(config).expect("the logger opens");

    // Warm-up: every lazily created structure settles, including the writer's
    // first batch. These allocations are not the ones under test.
    for index in 0..10_000_u64 {
        handle
            .event(Level::Info, Component::Driver, Event::RoleChange)
            .message("warm-up")
            .field_u64("index", index)
            .emit();
    }

    ALLOCATIONS.with(|cell| cell.set(0));
    for index in 0..100_000_u64 {
        handle
            .event(Level::Info, Component::Driver, Event::RoleChange)
            .message("steady")
            .field_u64("index", index)
            .emit();
    }
    let allocated = allocations();

    handle.shutdown();

    assert_eq!(0, allocated, "the calling thread allocated on the log path");
}
