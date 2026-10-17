//! Agrona's `ExpandableRingBuffer`, as the pending-service-message tracker uses
//! it.
//!
//! A byte ring of power-of-two capacity, with each message framed by an 8-byte
//! header: a `u32` length (header included) and a `u32` type (`0` padding, `1`
//! data). Frames are 8-byte aligned, and a frame that would wrap is preceded by
//! a padding frame that fills the end of the buffer.
//!
//! The behaviour here was read out of `agrona-2.6.1.jar` with `javap -p -c`
//! and is pinned against a run of the jar by
//! `tests/fixtures/regen/RingGen.java` → `tests/fixtures/ring-ops.txt`.
//!
//! # What the tracker depends on
//!
//! * `append` returning `false` is the capacity signal; it grows the buffer
//!   otherwise (`PendingServiceMessageTracker.java:99-103`).
//! * `for_each(head_offset, …)` resumes **after** the message whose end offset
//!   is `head_offset`; the consumer is handed the offset just past the message
//!   it is looking at, which is what the tracker stores between polls.
//! * `consume` advances the head by the bytes it really consumed and returns
//!   that number; a consumer that answers `false` leaves its message in place
//!   and backs the head up.

/// `ExpandableRingBuffer.HEADER_LENGTH`.
const HEADER_LENGTH: usize = 8;

/// `ExpandableRingBuffer.HEADER_ALIGNMENT`.
const HEADER_ALIGNMENT: usize = 8;

/// `ExpandableRingBuffer.MAX_CAPACITY`.
pub const MAX_CAPACITY: usize = 1 << 30;

/// The header's type field: padding (`MESSAGE_TYPE_PADDING`).
const MESSAGE_TYPE_PADDING: u32 = 0;

/// The header's type field: data (`MESSAGE_TYPE_DATA`).
const MESSAGE_TYPE_DATA: u32 = 1;

/// Answers whether to keep walking (`MessageConsumer.onMessage`).
pub trait MessageConsumer {
    /// One message: its bytes and the offset just past it, relative to the
    /// ring's head.
    fn on_message(&mut self, message: &[u8], head_offset: usize) -> bool;
}

impl<F> MessageConsumer for F
where
    F: FnMut(&[u8], usize) -> bool,
{
    fn on_message(&mut self, message: &[u8], head_offset: usize) -> bool {
        self(message, head_offset)
    }
}

/// The ring (`org.agrona.ExpandableRingBuffer`).
#[derive(Clone, Debug)]
pub struct ExpandableRingBuffer {
    buffer: Vec<u8>,
    capacity: usize,
    mask: usize,
    max_capacity: usize,
    head: usize,
    tail: usize,
}

impl Default for ExpandableRingBuffer {
    fn default() -> Self {
        Self::new()
    }
}

impl ExpandableRingBuffer {
    /// `new ExpandableRingBuffer()` — unbounded up to [`MAX_CAPACITY`].
    pub fn new() -> Self {
        Self {
            buffer: Vec::new(),
            capacity: 0,
            mask: 0,
            max_capacity: MAX_CAPACITY,
            head: 0,
            tail: 0,
        }
    }

    /// `new ExpandableRingBuffer(maxCapacity, initialCapacity, isDirect)`.
    ///
    /// # Panics
    ///
    /// If either capacity is not a power of two or `initial_capacity` exceeds
    /// `max_capacity`, matching Agrona's own validation.
    pub fn with_capacity(max_capacity: usize, initial_capacity: usize) -> Self {
        assert!(initial_capacity.is_power_of_two() || initial_capacity == 0);
        assert!(max_capacity.is_power_of_two());
        assert!(initial_capacity <= max_capacity);
        Self {
            buffer: vec![0_u8; initial_capacity],
            capacity: initial_capacity,
            mask: initial_capacity.saturating_sub(1),
            max_capacity,
            head: 0,
            tail: 0,
        }
    }

    /// `capacity()`: the bytes allocated.
    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    /// `size()`: `tail - head`, the bytes of framed messages currently stored.
    pub const fn size(&self) -> usize {
        self.tail - self.head
    }

    /// `isEmpty()`.
    pub const fn is_empty(&self) -> bool {
        self.size() == 0
    }

    /// `head()` and `tail()` as logical offsets.
    pub const fn head(&self) -> usize {
        self.head
    }

    /// The logical end of the stored data.
    pub const fn tail(&self) -> usize {
        self.tail
    }

    /// `reset(capacity)`: a fresh buffer with a fixed capacity.
    ///
    /// # Panics
    ///
    /// If the next power of two above `capacity` exceeds the maximum.
    pub fn reset(&mut self, capacity: usize) {
        let capacity = next_power_of_two_at_least_one(capacity);
        assert!(
            capacity <= self.max_capacity,
            "capacity {capacity} exceeds maxCapacity {}",
            self.max_capacity
        );
        if capacity != self.capacity {
            self.capacity = capacity;
            self.mask = capacity - 1;
            self.buffer = vec![0_u8; capacity];
        }
        self.head = 0;
        self.tail = 0;
    }

    /// `append(buffer, offset, length)`.
    ///
    /// `false` means the frame does not fit and the buffer could not grow.
    pub fn append(&mut self, payload: &[u8]) -> bool {
        let head_index = self.head & self.mask;
        let tail_index = self.tail & self.mask;
        let length = align(HEADER_LENGTH + payload.len(), HEADER_ALIGNMENT);
        let mut remaining = self.capacity.saturating_sub(self.tail - self.head);

        if length > remaining {
            if !self.resize(length) {
                return false;
            }
        } else if tail_index >= head_index {
            // A frame that would wrap is preceded by a padding frame, unless
            // resizing is cheaper.
            let to_end = self.capacity - tail_index;
            if length > to_end {
                if length <= remaining - to_end {
                    self.put_u32(tail_index, to_end as u32);
                    self.put_u32(tail_index + 4, MESSAGE_TYPE_PADDING);
                    self.tail += to_end;
                } else if !self.resize(length) {
                    return false;
                }
            }
        }

        remaining = self.capacity.saturating_sub(self.tail - self.head);
        if length > remaining {
            return false;
        }

        self.write_message(payload);
        self.tail += length;
        true
    }

    /// `forEach(headOffset, consumer, limit)` — walk without consuming.
    ///
    /// The consumer is given the offset just past each message. Returns how
    /// many messages were handed over.
    ///
    /// # Panics
    ///
    /// If `head_offset` is beyond `size()`, matching Agrona's
    /// `IllegalArgumentException`.
    pub fn for_each<C: MessageConsumer>(
        &self,
        head_offset: usize,
        consumer: &mut C,
        limit: usize,
    ) -> usize {
        assert!(
            head_offset <= self.size(),
            "headOffset={head_offset} > size={}",
            self.size()
        );
        assert_eq!(0, head_offset % HEADER_ALIGNMENT, "headOffset not aligned");

        let mut offset = self.head + head_offset;
        let mut count = 0;

        while count < limit && offset < self.tail {
            let index = offset & self.mask;
            let length = self.get_u32(index) as usize;
            let message_type = self.get_u32(index + 4);
            let aligned = align(length, HEADER_ALIGNMENT);
            offset += aligned;

            if message_type != MESSAGE_TYPE_PADDING {
                let message = &self.buffer[index + HEADER_LENGTH..index + length];
                if !consumer.on_message(message, offset - self.head) {
                    break;
                }
                count += 1;
            }
        }

        count
    }

    /// `consume(consumer, limit)` — walk and advance the head.
    ///
    /// Returns the bytes consumed. A consumer that answers `false` leaves its
    /// message and everything after it in place.
    pub fn consume<C: MessageConsumer>(&mut self, consumer: &mut C, limit: usize) -> usize {
        let mut offset = self.head;
        let mut count = 0;
        let head = self.head;

        while count < limit && offset < self.tail {
            let index = offset & self.mask;
            let length = self.get_u32(index) as usize;
            let message_type = self.get_u32(index + 4);
            let aligned = align(length, HEADER_ALIGNMENT);
            offset += aligned;

            if message_type != MESSAGE_TYPE_PADDING {
                let message = &self.buffer[index + HEADER_LENGTH..index + length];
                if !consumer.on_message(message, offset - head) {
                    offset -= aligned;
                    break;
                }
                count += 1;
            }
        }

        let consumed = offset - head;
        self.head = offset;
        consumed
    }

    /// `writeMessage(buffer, offset, length)`.
    fn write_message(&mut self, payload: &[u8]) {
        let index = self.tail & self.mask;
        let length = HEADER_LENGTH + payload.len();
        self.put_u32(index, length as u32);
        self.put_u32(index + 4, MESSAGE_TYPE_DATA);
        self.buffer[index + HEADER_LENGTH..index + length].copy_from_slice(payload);
    }

    /// `resize(requiredCapacity)`: `nextPow2(capacity + required)`, grown and
    /// compacted to head 0.
    fn resize(&mut self, required_capacity: usize) -> bool {
        let new_capacity = next_power_of_two_at_least_one(self.capacity + required_capacity);
        if new_capacity < self.capacity || new_capacity > self.max_capacity {
            return false;
        }

        let head_index = self.head & self.mask;
        let size = self.size();
        let first = size.min(self.capacity.saturating_sub(head_index));
        let mut new_buffer = vec![0_u8; new_capacity];
        new_buffer[..first].copy_from_slice(&self.buffer[head_index..head_index + first]);
        if first < size {
            let second = size - first;
            new_buffer[first..first + second].copy_from_slice(&self.buffer[..second]);
        }

        self.buffer = new_buffer;
        self.capacity = new_capacity;
        self.mask = new_capacity - 1;
        self.head = 0;
        self.tail = size;
        true
    }

    fn get_u32(&self, index: usize) -> u32 {
        u32::from_le_bytes(
            self.buffer[index..index + 4]
                .try_into()
                .expect("four bytes"),
        )
    }

    fn put_u32(&mut self, index: usize, value: u32) {
        self.buffer[index..index + 4].copy_from_slice(&value.to_le_bytes());
    }
}

const fn align(value: usize, alignment: usize) -> usize {
    (value + alignment - 1) & !(alignment - 1)
}

/// Agrona's `findNextPositivePowerOfTwo`, with 0 mapped to 1 (a `reset(0)` must
/// still leave an appendable buffer).
fn next_power_of_two_at_least_one(value: usize) -> usize {
    let mut power = 1;
    while power < value {
        power <<= 1;
    }
    power
}

#[cfg(test)]
mod tests {
    use super::*;

    fn payload(start: u8, length: usize) -> Vec<u8> {
        (0..length).map(|i| start.wrapping_add(i as u8)).collect()
    }

    #[test]
    fn append_and_each_hand_over_the_offsets_the_jar_hands_over() {
        // `tests/fixtures/ring-ops.txt`: payloads of 3, 8 and 9 bytes at
        // offsets 16, 32 and 56, capacity 64, size 56.
        let mut ring = ExpandableRingBuffer::new();
        assert!(ring.append(&payload(0xA0, 3)));
        assert!(ring.append(&payload(0xA0, 8)));
        assert!(ring.append(&payload(0xA0, 9)));
        assert_eq!(56, ring.size());
        assert_eq!(64, ring.capacity());

        let mut seen = Vec::new();
        ring.for_each(
            0,
            &mut |message: &[u8], head: usize| {
                seen.push((message.len(), head));
                true
            },
            10,
        );
        assert_eq!(vec![(3, 16), (8, 32), (9, 56)], seen);
    }

    #[test]
    fn each_from_a_message_boundary_resumes_after_it() {
        let mut ring = ExpandableRingBuffer::new();
        assert!(ring.append(&payload(0xA0, 3)));
        assert!(ring.append(&payload(0xA0, 8)));
        assert!(ring.append(&payload(0xA0, 9)));

        let mut seen = Vec::new();
        ring.for_each(
            16,
            &mut |message: &[u8], head: usize| {
                seen.push((message.len(), head));
                true
            },
            10,
        );
        assert_eq!(vec![(8, 32), (9, 56)], seen);
    }

    #[test]
    fn consume_backs_up_when_the_consumer_says_no() {
        // `consume-return=0`, and the message is still there afterwards.
        let mut ring = ExpandableRingBuffer::new();
        assert!(ring.append(&payload(0xA0, 3)));
        assert!(ring.append(&payload(0xA0, 8)));

        let consumed = ring.consume(&mut |_: &[u8], _: usize| false, 10);
        assert_eq!(0, consumed);
        assert_eq!(32, ring.size());

        let consumed = ring.consume(&mut |_: &[u8], _: usize| true, 1);
        assert_eq!(16, consumed);
        assert_eq!(16, ring.size());
    }

    #[test]
    fn a_fixed_capacity_refuses_the_frame_it_cannot_hold() {
        // `tests/fixtures/ring-ops.txt`: two 24-byte payloads fill 64 bytes and
        // the third append answers false.
        let mut ring = ExpandableRingBuffer::with_capacity(64, 64);
        assert!(ring.append(&payload(0xA0, 24)));
        assert!(ring.append(&payload(0xA0, 24)));
        assert!(!ring.append(&payload(0xA0, 24)));
        assert_eq!(64, ring.size());
        assert_eq!(64, ring.capacity());
    }

    #[test]
    fn a_frame_that_would_wrap_is_preceded_by_padding() {
        // Capacity 64. Two frames (32 + 16 bytes) fill 48; consuming the first
        // leaves head 32 and tail 48 with 16 bytes of data. A 24-byte frame
        // does not fit the 16 bytes to the end, but it does fit before head —
        // so the ring writes a padding frame over [48, 64) and the message at 0.
        let mut ring = ExpandableRingBuffer::with_capacity(64, 64);
        assert!(ring.append(&payload(0, 24)));
        assert!(ring.append(&payload(0, 8)));
        let consumed = ring.consume(&mut |_: &[u8], _: usize| true, 1);
        assert_eq!(32, consumed);
        assert_eq!(48, ring.tail());
        assert!(ring.append(&payload(0, 16)));
        assert_eq!(88, ring.tail(), "logical tail: 48 + 16 padding + 24 frame");

        let mut lengths = Vec::new();
        ring.for_each(
            0,
            &mut |message: &[u8], _: usize| {
                lengths.push(message.len());
                true
            },
            10,
        );
        assert_eq!(vec![8, 16], lengths);
    }

    #[test]
    fn reset_zero_still_accepts_an_append() {
        let mut ring = ExpandableRingBuffer::new();
        ring.reset(0);
        assert_eq!(1, ring.capacity());
        assert!(ring.append(b"x"));
        assert_eq!(16, ring.size());
    }
}
