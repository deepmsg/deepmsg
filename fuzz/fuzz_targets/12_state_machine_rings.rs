#![no_main]

//! Target 12: the lock-free rings, as structured operation sequences.
//!
//! Raw bytes cannot say "claim, abort, claim, commit"; this target derives a
//! `Vec<RingOp>` instead and drives the real types. Two rings keep the checks
//! honest:
//!
//! - ring 1 is written only through `write`, so what comes out must equal what
//!   went in, in order (the FIFO invariant);
//! - ring 2 is the claim/abort/commit surface, where a half-written record is
//!   the point and no payload is expected.
//!
//! The broadcast ring is written and read the same way. Every region is 4 KiB
//! plus its trailer, so the capacity checks the constructors make are real and
//! the trailer is where the counters would corrupt if a cursor ran past it.

use std::collections::VecDeque;

use arbitrary::Arbitrary;
use deepmsg_cnc::broadcast::{Received, ToClientsReceiver, ToClientsTransmitter};
use deepmsg_cnc::layout::{BROADCAST_TRAILER_LENGTH, MPSC_RB_TRAILER_LENGTH};
use deepmsg_cnc::ring::{ToDriverRing, ToDriverRingConsumer};
use deepmsg_core::buffer::AtomicBuffer;
use libfuzzer_sys::fuzz_target;

const DRIVER_RING: usize = 4096 + MPSC_RB_TRAILER_LENGTH;
const BROADCAST: usize = 4096 + BROADCAST_TRAILER_LENGTH;

#[repr(align(64))]
struct Aligned<const N: usize>([u8; N]);

#[derive(Arbitrary, Debug)]
enum RingOp {
    Write {
        msg_type: i32,
        payload: Vec<u8>,
    },
    Read {
        limit: u8,
    },
    Claim {
        msg_type: i32,
        length: u16,
        abort: bool,
    },
    BroadcastTransmit {
        type_id: i32,
        payload: Vec<u8>,
    },
    BroadcastReceive {
        limit: u8,
    },
}

fn bounded(payload: Vec<u8>) -> Vec<u8> {
    let mut payload = payload;
    payload.truncate(128);
    payload
}

fn type_id(value: i32) -> i32 {
    (value.unsigned_abs() % 1024) as i32 + 1
}

fuzz_target!(|ops: Vec<RingOp>| {
    let mut fifo = Aligned([0_u8; DRIVER_RING]);
    let mut abuse = Aligned([0_u8; DRIVER_RING]);
    let mut broadcast = Aligned([0_u8; BROADCAST]);

    let mut expected: VecDeque<Vec<u8>> = VecDeque::new();
    let mut expected_broadcast: VecDeque<Vec<u8>> = VecDeque::new();

    let mut consumer = {
        let region = AtomicBuffer::from_slice(&fifo.0).expect("aligned");
        ToDriverRingConsumer::new(&region).expect("a 4 KiB ring is a ring")
    };
    let mut receiver = {
        let region = AtomicBuffer::from_slice(&broadcast.0).expect("aligned");
        ToClientsReceiver::new(&region).expect("a 4 KiB broadcast is one")
    };

    for op in ops.into_iter().take(256) {
        match op {
            RingOp::Write { msg_type, payload } => {
                let payload = bounded(payload);
                let region = AtomicBuffer::from_slice_mut(&mut fifo.0).expect("aligned");
                if let Some(ring) = ToDriverRing::new(region) {
                    if ring.write(type_id(msg_type), &payload).is_ok() {
                        expected.push_back(payload);
                    }
                }
            }
            RingOp::Read { limit } => {
                let region = AtomicBuffer::from_slice_mut(&mut fifo.0).expect("aligned");
                consumer.read(&region, usize::from(limit).max(1), |_type_id, payload| {
                    if let Some(want) = expected.pop_front() {
                        assert_eq!(want, payload, "the FIFO order changed");
                    }
                });
            }
            RingOp::Claim {
                msg_type,
                length,
                abort,
            } => {
                let length = usize::from(length % 256);
                let region = AtomicBuffer::from_slice_mut(&mut abuse.0).expect("aligned");
                if let Some(ring) = ToDriverRing::new(region) {
                    if let Ok(offset) = ring.claim(type_id(msg_type), length) {
                        if abort {
                            let _ = ring.abort(offset);
                        } else {
                            let _ = ring.commit(offset);
                        }
                    }
                }
            }
            RingOp::BroadcastTransmit { type_id, payload } => {
                let payload = bounded(payload);
                let region = AtomicBuffer::from_slice_mut(&mut broadcast.0).expect("aligned");
                if let Some(mut transmitter) = ToClientsTransmitter::new(&region) {
                    if transmitter
                        .transmit(&region, type_id.max(1), &payload)
                        .is_ok()
                    {
                        expected_broadcast.push_back(payload);
                    }
                }
            }
            RingOp::BroadcastReceive { limit } => {
                let region = AtomicBuffer::from_slice(&broadcast.0).expect("aligned");
                for _ in 0..usize::from(limit).max(1) {
                    match receiver.receive(&region) {
                        Received::Message { .. } => {
                            if let Some(want) = expected_broadcast.pop_front() {
                                assert_eq!(want, receiver.message(), "broadcast order changed");
                            }
                        }
                        Received::Empty | Received::Discarded => break,
                    }
                }
            }
        }
    }

    // Drain what is left; 256 is the most this target can have written.
    {
        let region = AtomicBuffer::from_slice_mut(&mut fifo.0).expect("aligned");
        consumer.read(&region, 256, |_type_id, payload| {
            if let Some(want) = expected.pop_front() {
                assert_eq!(want, payload, "the FIFO order changed while draining");
            }
        });
        assert!(expected.is_empty(), "a written message never came back");
    }
});
