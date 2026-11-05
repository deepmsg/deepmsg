//! The bounded hand-off between event callers and the writer thread.
//!
//! A full queue is a **drop**, never a block: a busy-spin driver must not wait
//! for a log file. The drop is counted, and the writer thread reports the count
//! as its own `log.dropped` event (L0a plan §2.8), so the loss is visible in
//! the same place as everything else.
//!
//! `std::sync::mpsc::sync_channel` allocates its buffer once, at creation. The
//! zero-allocation test in `tests/zero_alloc.rs` is what keeps that assumption
//! honest; if it ever stops holding, this module is the one to change.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TrySendError, sync_channel};

use crate::record::Record;

/// A bounded queue of encoded records and the count of what it refused.
#[derive(Clone)]
pub(crate) struct Queue {
    sender: SyncSender<Record>,
    dropped: Arc<AtomicU64>,
}

impl Queue {
    /// Create a queue with room for `capacity` records (at least one).
    pub(crate) fn new(capacity: usize) -> (Self, Receiver<Record>) {
        let (sender, receiver) = sync_channel(capacity.max(1));
        (
            Self {
                sender,
                dropped: Arc::new(AtomicU64::new(0)),
            },
            receiver,
        )
    }

    /// Hand a record to the writer, or count it as dropped.
    pub(crate) fn push(&self, record: Record) {
        match self.sender.try_send(record) {
            Ok(()) => {}
            Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Records this queue refused because it was full or the writer was gone.
    pub(crate) fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::Queue;
    use crate::event::{Component, Event};
    use crate::level::Level;
    use crate::record::Encoder;

    fn record() -> crate::record::Record {
        Encoder::begin(0, Level::Info, Component::Driver, "n", Event::RoleChange).finish()
    }

    #[test]
    fn a_full_queue_counts_the_record_instead_of_blocking() {
        let (queue, receiver) = Queue::new(1);
        queue.push(record());
        assert_eq!(0, queue.dropped());

        queue.push(record());
        assert_eq!(1, queue.dropped());

        // The first record is still there for the writer.
        assert!(receiver.try_recv().is_ok());
        queue.push(record());
        assert_eq!(1, queue.dropped());
    }

    #[test]
    fn a_disconnected_queue_counts_too() {
        let (queue, receiver) = Queue::new(1);
        drop(receiver);
        queue.push(record());
        assert_eq!(1, queue.dropped());
    }
}
