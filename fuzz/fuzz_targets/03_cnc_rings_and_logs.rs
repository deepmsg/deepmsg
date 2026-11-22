#![no_main]

//! Target 03: the CnC shared-memory regions — rings, counters, error log and
//! loss report.
//!
//! These regions are what a crashed or hostile process leaves behind: the
//! driver trusts the trailer, the counters reader trusts the metadata, and the
//! error log trusts its own length fields. The assertions are the region
//! bounds: reads stop at the end of their record, scans terminate, and a
//! corrupted cursor is a zero answer rather than an index.

use deepmsg_cnc::broadcast::{ToClientsReceiver, ToClientsTransmitter};
use deepmsg_cnc::counters::CountersReader;
use deepmsg_cnc::error_log::ErrorLogReader;
use deepmsg_cnc::layout::{BROADCAST_TRAILER_LENGTH, MPSC_RB_TRAILER_LENGTH};
use deepmsg_cnc::loss_report::{self, LossReportEntry};
use deepmsg_cnc::metadata::{CncMetadata, RegionLayout};
use deepmsg_cnc::ring::{ToDriverRing, ToDriverRingConsumer};
use deepmsg_core::buffer::AtomicBuffer;
use deepmsg_fuzz::{with_read_only, with_read_write, with_two_read_only};
use libfuzzer_sys::fuzz_target;

#[repr(align(64))]
struct Aligned<const N: usize>([u8; N]);

const DRIVER_RING: usize = 4096 + MPSC_RB_TRAILER_LENGTH;
const BROADCAST: usize = 4096 + BROADCAST_TRAILER_LENGTH;

fn fill(bytes: &mut [u8], data: &[u8]) {
    let len = data.len().min(bytes.len());
    bytes[..len].copy_from_slice(&data[..len]);
}

fn ring_regions(data: &[u8]) {
    let mut bytes = Aligned([0_u8; DRIVER_RING]);
    fill(&mut bytes.0, data);

    // A producer writing what the fuzzer said, then the consumer over the
    // bytes that produced. Either side alone is a fair test of the other's
    // assumptions.
    {
        let Some(region) = AtomicBuffer::from_slice_mut(&mut bytes.0) else {
            return;
        };
        if let Some(ring) = ToDriverRing::new(region) {
            let payload = &data[..data.len().min(64)];
            let _ = ring.write(1, payload);
            let _ = ring.write(7, payload);
        }
    }

    let mut consumer = {
        let Some(region) = AtomicBuffer::from_slice(&bytes.0) else {
            return;
        };
        ToDriverRingConsumer::new(&region)
    };
    if let Some(consumer) = consumer.as_mut() {
        let Some(region) = AtomicBuffer::from_slice_mut(&mut bytes.0) else {
            return;
        };
        let mut messages = 0_usize;
        let read = consumer.read(&region, 16, |type_id, payload| {
            assert!(payload.len() <= 4096);
            let _ = type_id;
            messages += 1;
        });
        assert!(messages <= read);
    }
}

fn broadcast_region(data: &[u8]) {
    let mut bytes = Aligned([0_u8; BROADCAST]);
    fill(&mut bytes.0, data);

    {
        let Some(region) = AtomicBuffer::from_slice_mut(&mut bytes.0) else {
            return;
        };
        if let Some(mut transmitter) = ToClientsTransmitter::new(&region) {
            let payload = &data[..data.len().min(64)];
            let _ = transmitter.transmit(&region, 1, payload);
            let _ = transmitter.transmit(&region, i32::MAX, payload);
        }
    }

    let Some(region) = AtomicBuffer::from_slice(&bytes.0) else {
        return;
    };
    if let Some(mut receiver) = ToClientsReceiver::new(&region) {
        for _ in 0..16 {
            let _ = receiver.receive(&region);
        }
    }
}

fuzz_target!(|data: &[u8]| {
    if let Ok(metadata) = CncMetadata::decode(data) {
        let mut length = [0_u8; core::mem::size_of::<usize>()];
        let take = data.len().min(length.len());
        length[..take].copy_from_slice(&data[..take]);
        let _ = RegionLayout::compute(&metadata, usize::from_le_bytes(length));
    }

    ring_regions(data);
    broadcast_region(data);

    with_two_read_only(data, data, |metadata, values| {
        let reader = CountersReader::new(metadata, values);
        let mut counters = 0_usize;
        let _ = reader.for_each(|_descriptor| {
            counters += 1;
        });
        assert!(counters <= data.len());
    });

    with_read_only(data, |region, len| {
        let mut entries = Vec::new();
        let _ = ErrorLogReader::new(region).read(i64::MIN, &mut entries);
        assert!(entries.len() <= len);
    });

    with_read_only(data, |region, _| {
        let _ = loss_report::read_all(&region);
        let _ = LossReportEntry::decode(&region, 0);
    });

    let _ = with_write_check(data);
});

/// A write view of the same bytes, for the writers that need one.
fn with_write_check(data: &[u8]) -> usize {
    with_read_write(data, |region, len| {
        let _ = region.load_i32_acquire(0);
        len
    })
}
