//! The position counters a driver publishes, and the key they all carry.
//!
//! Mirrors `aeron-driver/src/main/c/aeron_position.c:23-111`. Three counters
//! come from one allocator there — `pub-pos`, `pub-lmt` and `sub-pos` differ
//! only in their type id, their name and a suffix — because they are one shape:
//! a position belonging to a `(registration, session, stream, channel)` tuple,
//! with the channel in the **key** so a reader holding an id can tell what it
//! measures.
//!
//! # The channel appears twice, and differently
//!
//! In the **label** it is written whole, up to 380 bytes
//! (`aeron_position.c:35-39`), because the label is for a human reading
//! `AeronStat`. In the **key** it is truncated to the 92 bytes the struct leaves
//! for it, and the key's length field records *that* length
//! (`:44-47`) — so a 200-byte URI produces a label that shows all of it and a
//! key that shows the first 92 and says so. Copying one length into the other is
//! a bug that no test of either alone would catch.

use deepmsg_cnc::{CounterManager, CounterRegions};

/// The counter **type ids** the position counters carry
/// (`aeron-client/src/main/c/aeron_counters.h`).
///
/// A counter's type id is what tells a reader how to interpret its key, so
/// these are byte-level contract and are named rather than written inline at
/// the three call sites below.
pub mod type_id {
    /// `AERON_COUNTER_PUBLISHER_LIMIT_TYPE_ID` (`:71-72`), label `"pub-lmt"`.
    pub const PUBLISHER_LIMIT: i32 = 1;
    /// `AERON_COUNTER_SUBSCRIPTION_POSITION_TYPE_ID` (`:80-81`), label
    /// `"sub-pos"`.
    pub const SUBSCRIPTION_POSITION: i32 = 4;
    /// `AERON_COUNTER_PUBLISHER_POSITION_TYPE_ID` (`:100-102`), label
    /// `"pub-pos (concurrent)"` or `"pub-pos (exclusive)"`.
    pub const PUBLISHER_POSITION: i32 = 12;
    /// `AERON_COUNTER_RECEIVER_HWM_TYPE_ID` (`:74-76`), label `"rcv-hwm"`.
    pub const RECEIVER_HWM: i32 = 3;
    /// `AERON_COUNTER_RECEIVER_POSITION_TYPE_ID` (`:86-88`), label `"rcv-pos"`.
    pub const RECEIVER_POSITION: i32 = 5;
    /// `AERON_COUNTER_RECEIVER_NAKS_SENT_TYPE_ID` (`:124`), label
    /// `"rcv-naks-sent"`.
    pub const RECEIVER_NAKS_SENT: i32 = 20;
    /// `AERON_COUNTER_SENDER_POSITION_TYPE_ID` (`:104-106`), label
    /// `"snd-pos"`.
    pub const SENDER_POSITION: i32 = 2;
    /// `AERON_COUNTER_SENDER_LIMIT_TYPE_ID` (`:108-110`), label `"snd-lmt"`.
    pub const SENDER_LIMIT: i32 = 9;
    /// `AERON_COUNTER_FC_NUM_RECEIVERS_TYPE_ID` (`:115`), label
    /// `"fc-receivers"` (`aeron_flow_control.h:28`).
    ///
    /// One per publication whose strategy keeps a group, and none for a
    /// publication whose does not: the reference allocates it in the group
    /// supplier (`aeron_min_flow_control.c:589-594`).
    pub const FC_NUM_RECEIVERS: i32 = 17;
}

/// The key a stream-position counter carries
/// (`aeron-client/src/main/c/concurrent/aeron_counters_manager.h:37-47`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StreamPositionKey<'a> {
    /// The registration id the counter belongs to — a publication's, or a
    /// subscription's.
    pub registration_id: i64,
    /// The session it measures.
    pub session_id: i32,
    /// The stream it measures.
    pub stream_id: i32,
    /// The channel, as the client sent it, including any parameters.
    pub channel: &'a [u8],
}

impl StreamPositionKey<'_> {
    /// The key's width, and the one the counter manager is told about.
    pub const LENGTH: usize = 112;

    /// Where the channel field starts: after the two ids and the two lengths.
    const CHANNEL_OFFSET: usize = 20;

    /// How much of a channel the key can hold.
    pub const CHANNEL_LENGTH_MAX: usize = Self::LENGTH - Self::CHANNEL_OFFSET;

    /// The key as the bytes a reader would find.
    ///
    /// The reference builds this as a designated initialiser
    /// (`aeron_position.c:41-42`), so everything it does not name is **zero** —
    /// including the channel's tail past what fits. Writing a shorter array and
    /// leaving the rest of the buffer alone would put whatever was on the stack
    /// into a file other processes read.
    pub fn encode(&self) -> [u8; StreamPositionKey::LENGTH] {
        let mut out = [0u8; Self::LENGTH];
        let channel_length = self.channel.len().min(Self::CHANNEL_LENGTH_MAX);

        out[..8].copy_from_slice(&self.registration_id.to_le_bytes());
        out[8..12].copy_from_slice(&self.session_id.to_le_bytes());
        out[12..16].copy_from_slice(&self.stream_id.to_le_bytes());
        #[allow(clippy::cast_possible_truncation)] // bounded by CHANNEL_LENGTH_MAX
        out[16..20].copy_from_slice(&(channel_length as i32).to_le_bytes());
        out[Self::CHANNEL_OFFSET..Self::CHANNEL_OFFSET + channel_length]
            .copy_from_slice(&self.channel[..channel_length]);

        out
    }
}

/// The label every one of these counters carries, formatted as the reference
/// formats it (`aeron_position.c:35-39`): `name: <registration> <session>
/// <stream> <channel><suffix>`.
pub fn stream_counter_label(
    name: &str,
    registration_id: i64,
    session_id: i32,
    stream_id: i32,
    channel: &[u8],
    suffix: &str,
) -> Vec<u8> {
    let mut label = format!("{name}: {registration_id} {session_id} {stream_id} ").into_bytes();
    label.extend_from_slice(channel);
    label.extend_from_slice(suffix.as_bytes());

    label
}

/// Allocate a counter of the shape every stream position uses
/// (`aeron_position.c:23-62`).
///
/// Returns the id, or `None` if the manager is full — and leaves nothing behind
/// when it fails, because the manager's own allocation is the only step that can
/// fail.
#[allow(clippy::too_many_arguments)]
pub fn allocate_stream_counter(
    manager: &mut CounterManager,
    regions: &CounterRegions<'_>,
    name: &str,
    type_id: i32,
    client_id: i64,
    registration_id: i64,
    session_id: i32,
    stream_id: i32,
    channel: &[u8],
    suffix: &str,
    now_ms: i64,
) -> Option<i32> {
    let label = stream_counter_label(
        name,
        registration_id,
        session_id,
        stream_id,
        channel,
        suffix,
    );
    let key = StreamPositionKey {
        registration_id,
        session_id,
        stream_id,
        channel,
    }
    .encode();

    let counter_id = manager.allocate(regions, type_id, &key, &label, now_ms)?;

    manager
        .set_registration_id(regions, counter_id, registration_id)
        .and_then(|()| manager.set_owner_id(regions, counter_id, client_id))
        .map(|()| counter_id)
}

/// `pub-pos`: how far the producer has written
/// (`AERON_COUNTER_PUBLISHER_POSITION_TYPE_ID`, `aeron_counters.h:100-102`).
///
/// The name carries the publication's kind because the two are different
/// streams with different rules — a concurrent publication can have several
/// producers, an exclusive one has exactly one — and `AeronStat` is where that
/// distinction is read.
#[allow(clippy::too_many_arguments)] // one per field of the key and the label
pub fn allocate_publisher_position(
    manager: &mut CounterManager,
    regions: &CounterRegions<'_>,
    client_id: i64,
    registration_id: i64,
    session_id: i32,
    stream_id: i32,
    channel: &[u8],
    is_exclusive: bool,
    now_ms: i64,
) -> Option<i32> {
    let name = if is_exclusive {
        "pub-pos (exclusive)"
    } else {
        "pub-pos (concurrent)"
    };

    allocate_stream_counter(
        manager,
        regions,
        name,
        type_id::PUBLISHER_POSITION,
        client_id,
        registration_id,
        session_id,
        stream_id,
        channel,
        "",
        now_ms,
    )
}

/// `pub-lmt`: the highest position the producer may write
/// (`AERON_COUNTER_PUBLISHER_LIMIT_TYPE_ID`, `aeron_counters.h:71-72`).
///
/// The driver is its only writer, and it is the whole of backpressure.
#[allow(clippy::too_many_arguments)]
pub fn allocate_publisher_limit(
    manager: &mut CounterManager,
    regions: &CounterRegions<'_>,
    client_id: i64,
    registration_id: i64,
    session_id: i32,
    stream_id: i32,
    channel: &[u8],
    now_ms: i64,
) -> Option<i32> {
    allocate_stream_counter(
        manager,
        regions,
        "pub-lmt",
        type_id::PUBLISHER_LIMIT,
        client_id,
        registration_id,
        session_id,
        stream_id,
        channel,
        "",
        now_ms,
    )
}

/// `sub-pos`: how far a subscriber has read
/// (`AERON_COUNTER_SUBSCRIPTION_POSITION_TYPE_ID`, `aeron_counters.h:80-81`).
///
/// One per (subscription, publication) pair, and the join position is in the
/// **label** (`aeron_position.c:86-105`) because it is what a reader needs to
/// interpret the value: a subscriber that reads 0 is caught up if it joined at
/// 0 and an eternity behind if it joined at a million.
#[allow(clippy::too_many_arguments)] // one per field of the key and the label
pub fn allocate_subscription_position(
    manager: &mut CounterManager,
    regions: &CounterRegions<'_>,
    client_id: i64,
    registration_id: i64,
    session_id: i32,
    stream_id: i32,
    channel: &[u8],
    joining_position: i64,
    now_ms: i64,
) -> Option<i32> {
    allocate_stream_counter(
        manager,
        regions,
        "sub-pos",
        type_id::SUBSCRIPTION_POSITION,
        client_id,
        registration_id,
        session_id,
        stream_id,
        channel,
        &format!(" @{joining_position}"),
        now_ms,
    )
}

/// The names the channel-status counters carry
/// (`aeron-client/src/main/c/aeron_counters.h:86-90`).
pub const SEND_CHANNEL_STATUS_NAME: &str = "snd-channel";
/// The receive endpoint's name.
pub const RECEIVE_CHANNEL_STATUS_NAME: &str = "rcv-channel";
/// The name a multi-destination send endpoint's destination count carries
/// (`AERON_COUNTER_CHANNEL_MDC_NUM_DESTINATIONS_NAME`,
/// `aeron-client/src/main/c/aeron_counters.h:117`).
pub const MDC_NUM_DESTINATIONS_NAME: &str = "mdc-num-dest";

/// The type ids the channel-status counters carry
/// (`aeron-client/src/main/c/aeron_counters.h:86-90`).
pub mod channel_type_id {
    /// `AERON_COUNTER_SEND_CHANNEL_STATUS_TYPE_ID`, label `"snd-channel"`.
    pub const SEND_CHANNEL_STATUS: i32 = 6;
    /// `AERON_COUNTER_RECEIVE_CHANNEL_STATUS_TYPE_ID`, label `"rcv-channel"`.
    pub const RECEIVE_CHANNEL_STATUS: i32 = 7;
    /// `AERON_COUNTER_CHANNEL_NUM_DESTINATIONS_TYPE_ID`, label
    /// `"mdc-num-dest"` — how many destinations a multi-destination send
    /// endpoint currently has (`aeron_counters.h:118`).
    pub const MDC_NUM_DESTINATIONS: i32 = 18;
}

/// What a channel-status counter holds
/// (`aeron-client/src/main/c/concurrent/aeron_counters_manager.h:30-34`).
pub mod channel_status {
    /// Just allocated, before the endpoint has its socket
    /// (`AERON_COUNTER_CHANNEL_ENDPOINT_STATUS_INITIALIZING`).
    pub const INITIALIZING: i64 = 0;
    /// Open and usable (`AERON_COUNTER_CHANNEL_ENDPOINT_STATUS_ACTIVE`).
    pub const ACTIVE: i64 = 1;
    /// Being torn down (`AERON_COUNTER_CHANNEL_ENDPOINT_STATUS_CLOSING`).
    pub const CLOSING: i64 = 2;
    /// The endpoint failed (`AERON_COUNTER_CHANNEL_ENDPOINT_STATUS_ERRORED`).
    pub const ERRORED: i64 = -1;
}

/// The key a channel-status counter carries
/// (`aeron_channel_endpoint_status_key_layout_t`,
/// `aeron-client/src/main/c/concurrent/aeron_counters_manager.h:44-49`):
/// the channel's length and the channel.
///
/// The reference builds this with a `memcpy` into an uninitialized struct
/// (`aeron_position.c:220-222`), so everything past the copied bytes is
/// whatever was on its stack. Here it is zero — the same bytes for the same
/// channel every time, which is the point of a key.
pub struct ChannelStatusKey<'a> {
    /// The channel, as the client sent it.
    pub channel: &'a [u8],
}

impl ChannelStatusKey<'_> {
    /// The key's width, and the one the counter manager is told about.
    pub const LENGTH: usize = StreamPositionKey::LENGTH;

    /// Where the channel field starts, after the length that describes it.
    const CHANNEL_OFFSET: usize = 4;

    /// How much of a channel the key can hold.
    pub const CHANNEL_LENGTH_MAX: usize = Self::LENGTH - Self::CHANNEL_OFFSET;

    /// The key as the bytes a reader would find.
    pub fn encode(&self) -> [u8; ChannelStatusKey::LENGTH] {
        let mut out = [0u8; Self::LENGTH];
        let channel_length = self.channel.len().min(Self::CHANNEL_LENGTH_MAX);

        #[allow(clippy::cast_possible_truncation)] // bounded by CHANNEL_LENGTH_MAX
        out[..4].copy_from_slice(&(channel_length as i32).to_le_bytes());
        out[Self::CHANNEL_OFFSET..Self::CHANNEL_OFFSET + channel_length]
            .copy_from_slice(&self.channel[..channel_length]);

        out
    }
}

/// The names a local-address counter carries
/// (`aeron-client/src/main/c/aeron_counters.h:107-108`).
///
/// Each names the endpoint or destination whose *actual* bound address it
/// holds, which is what a client reads to answer "which port did `:0` become".
pub const SEND_LOCAL_SOCKADDR_NAME: &str = "snd-local-sockaddr";
/// The receive side's.
pub const RECEIVE_LOCAL_SOCKADDR_NAME: &str = "rcv-local-sockaddr";

/// `AERON_COUNTER_LOCAL_SOCKADDR_TYPE_ID` (`aeron_counters.h:109`).
pub const LOCAL_SOCKADDR_TYPE_ID: i32 = 14;

/// The key a local-address counter carries
/// (`aeron_local_sockaddr_key_layout_t`,
/// `aeron-client/src/main/c/concurrent/aeron_counters_manager.h:62-68`): the
/// channel-status counter this address belongs to, the length of the address,
/// and the address itself.
///
/// The channel-status id is in the key because a counter is found by it: a
/// client that knows which channel it is asking about reads the status counter
/// and then the address beside it.
pub struct LocalSockaddrKey<'a> {
    /// The `snd-channel` or `rcv-channel` counter of the endpoint this address
    /// belongs to.
    pub channel_status_id: i32,
    /// The bound address as text.
    pub local_sockaddr: &'a str,
}

impl LocalSockaddrKey<'_> {
    /// The key's width, and the one the counter manager is told about.
    pub const LENGTH: usize = StreamPositionKey::LENGTH;

    /// Where the address starts, after the two `int32`s.
    const ADDRESS_OFFSET: usize = 8;

    /// How much of an address the key can hold.
    pub const ADDRESS_LENGTH_MAX: usize = Self::LENGTH - Self::ADDRESS_OFFSET;

    /// The key as the bytes a reader would find.
    pub fn encode(&self) -> [u8; LocalSockaddrKey::LENGTH] {
        let mut out = [0u8; Self::LENGTH];
        let length = self.local_sockaddr.len().min(Self::ADDRESS_LENGTH_MAX);

        out[..4].copy_from_slice(&self.channel_status_id.to_le_bytes());
        #[allow(clippy::cast_possible_truncation)] // bounded by ADDRESS_LENGTH_MAX
        out[4..8].copy_from_slice(&(length as i32).to_le_bytes());
        out[Self::ADDRESS_OFFSET..Self::ADDRESS_OFFSET + length]
            .copy_from_slice(&self.local_sockaddr.as_bytes()[..length]);

        out
    }
}

/// Allocate the counter that says where an endpoint or destination is actually
/// bound (`aeron_counter_local_sockaddr_indicator_allocate`,
/// `aeron-driver/src/main/c/aeron_position.c:276-309`).
///
/// Its **value** is the endpoint's state rather than a position — the caller
/// writes `ACTIVE` once the socket is up — and its label names both the channel
/// status it belongs to and the address, so that a reader of `AeronStat` can
/// see which is which.
///
/// The address is the reason this exists. A destination may name port zero
/// (a multi-destination one does), and then the counter's **key** is the only
/// place the port the kernel chose can be read from — which is what
/// `ReplayMerge` does when it has to resolve a replay port
/// (`LocalSocketAddressStatus.findAddress`).
///
/// # Errors
///
/// `None` when the counter manager has no room; nothing is left behind.
pub fn allocate_local_sockaddr_counter(
    manager: &mut CounterManager,
    regions: &CounterRegions<'_>,
    name: &str,
    registration_id: i64,
    channel_status_counter_id: i32,
    local_sockaddr: &str,
    now_ms: i64,
) -> Option<i32> {
    let key = LocalSockaddrKey {
        channel_status_id: channel_status_counter_id,
        local_sockaddr,
    }
    .encode();

    // `"%s: %d %s"` — the name, the channel status, and the address
    // (`aeron_position.c:295-297`).
    let label = format!("{name}: {channel_status_counter_id} {local_sockaddr}");

    let counter_id = manager.allocate(
        regions,
        LOCAL_SOCKADDR_TYPE_ID,
        &key,
        label.as_bytes(),
        now_ms,
    )?;

    manager
        .set_registration_id(regions, counter_id, registration_id)
        .map(|()| counter_id)
}

/// Allocate a channel-status counter for an endpoint
/// (`aeron_channel_endpoint_status_allocate`, `aeron_position.c:204-229`).
///
/// Its **value** is the endpoint's state rather than a position, and its
/// registration id is the endpoint's own — which is what a client reads to
/// answer "is this channel's socket up yet".
///
/// # Errors
///
/// `None` when the counter manager has no room; nothing is left behind.
pub fn allocate_channel_status_counter(
    manager: &mut CounterManager,
    regions: &CounterRegions<'_>,
    name: &str,
    type_id: i32,
    registration_id: i64,
    channel: &[u8],
    now_ms: i64,
) -> Option<i32> {
    let mut label = format!("{name}: ").into_bytes();
    label.extend_from_slice(channel);

    let key = ChannelStatusKey { channel }.encode();
    let counter_id = manager.allocate(regions, type_id, &key, &label, now_ms)?;

    manager
        .set_registration_id(regions, counter_id, registration_id)
        .map(|()| counter_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    use deepmsg_core::buffer::{AtomicBuffer, ReadWrite};

    #[repr(align(64))]
    struct Region(Vec<u8>);

    impl Region {
        fn zeroed(len: usize) -> Self {
            Self(vec![0u8; len])
        }

        fn writable(&mut self) -> AtomicBuffer<'_, ReadWrite> {
            AtomicBuffer::from_slice_mut(&mut self.0).expect("aligned")
        }
    }

    const VALUES_LENGTH: usize = 64 * 1024;

    struct Fixture {
        metadata: Region,
        values: Region,
    }

    impl Fixture {
        fn new() -> Self {
            Self {
                metadata: Region::zeroed(VALUES_LENGTH * 4),
                values: Region::zeroed(VALUES_LENGTH),
            }
        }

        fn open(&mut self) -> (CounterManager, CounterRegions<'_>) {
            let regions = CounterRegions::new(self.metadata.writable(), self.values.writable())
                .expect("four-to-one");
            let manager = CounterManager::new(VALUES_LENGTH, 1_000).expect("room");

            (manager, regions)
        }
    }

    /// The key a client finds a local address by
    /// (`aeron_local_sockaddr_key_layout_t`, `aeron_counters_manager.h:62-68`).
    #[test]
    fn the_local_address_key_holds_the_channel_status_and_the_address() {
        let key = LocalSockaddrKey {
            channel_status_id: 42,
            local_sockaddr: "127.0.0.1:40456",
        }
        .encode();

        assert_eq!(112, key.len());
        assert_eq!(
            42i32.to_le_bytes(),
            key[..4],
            "the channel status this address belongs to"
        );
        assert_eq!(15i32.to_le_bytes(), key[4..8], "the address's length");
        assert_eq!(b"127.0.0.1:40456", &key[8..23]);
        assert!(
            key[23..].iter().all(|byte| 0 == *byte),
            "everything past the address is zero"
        );
    }

    /// The counter itself: type 14, the address in its label beside the channel
    /// status it belongs to (`aeron_position.c:295-297`), and the client that
    /// asked as its registration id.
    #[test]
    fn a_local_address_counter_names_the_channel_status_and_the_address() {
        let mut fixture = Fixture::new();

        let counter_id = {
            let (mut counters, regions) = fixture.open();

            allocate_local_sockaddr_counter(
                &mut counters,
                &regions,
                RECEIVE_LOCAL_SOCKADDR_NAME,
                7,
                42,
                "127.0.0.1:40456",
                1_000,
            )
            .expect("a counter")
        };

        let reader = deepmsg_cnc::counters::CountersReader::new(
            fixture.metadata.writable(),
            fixture.values.writable(),
        );
        let mut found = None;
        reader.for_each(|descriptor| {
            if descriptor.counter_id == counter_id {
                found = Some((
                    descriptor.type_id,
                    descriptor.registration_id,
                    descriptor.label.clone(),
                ));
            }
        });

        assert_eq!(
            Some((
                LOCAL_SOCKADDR_TYPE_ID,
                7,
                "rcv-local-sockaddr: 42 127.0.0.1:40456".to_owned()
            )),
            found
        );
    }

    #[test]
    fn the_key_puts_the_ids_first_and_zeroes_what_is_left() {
        let key = StreamPositionKey {
            registration_id: 42,
            session_id: 7,
            stream_id: 9,
            channel: b"aeron:ipc",
        }
        .encode();

        assert_eq!(112, key.len());
        assert_eq!(42i64.to_le_bytes(), key[..8]);
        assert_eq!(7i32.to_le_bytes(), key[8..12]);
        assert_eq!(9i32.to_le_bytes(), key[12..16]);
        assert_eq!(9i32.to_le_bytes(), key[16..20], "the channel's length");
        assert_eq!(b"aeron:ipc", &key[20..29]);
        assert!(
            key[29..].iter().all(|byte| 0 == *byte),
            "everything the reference's designated initialiser leaves is zero"
        );
    }

    #[test]
    fn a_channel_too_long_for_the_key_is_truncated_and_says_so() {
        // The key holds 92 bytes; the label holds 380. Writing the label's
        // length into the key would describe bytes that are not there.
        let channel = vec![b'x'; 200];
        let key = StreamPositionKey {
            registration_id: 1,
            session_id: 2,
            stream_id: 3,
            channel: &channel,
        }
        .encode();

        assert_eq!(
            (StreamPositionKey::CHANNEL_LENGTH_MAX as i32).to_le_bytes(),
            key[16..20]
        );
        assert_eq!(
            vec![b'x'; StreamPositionKey::CHANNEL_LENGTH_MAX],
            key[20..].to_vec()
        );
    }

    #[test]
    fn the_three_counters_carry_the_type_ids_and_labels_the_reference_gives_them() {
        let mut fixture = Fixture::new();
        let (mut manager, regions) = fixture.open();

        let publish_id = allocate_publisher_position(
            &mut manager,
            &regions,
            7,
            99,
            100,
            1001,
            b"aeron:ipc",
            false,
            0,
        )
        .expect("an id");
        let limit_id =
            allocate_publisher_limit(&mut manager, &regions, 7, 99, 100, 1001, b"aeron:ipc", 0)
                .expect("an id");
        let subscribe_id = allocate_subscription_position(
            &mut manager,
            &regions,
            7,
            100,
            100,
            1001,
            b"aeron:ipc",
            4096,
            0,
        )
        .expect("an id");

        let reader = regions.reader();
        let find = |type_id: i32| {
            reader
                .find_by_type_id(type_id)
                .unwrap_or_else(|| panic!("type {type_id} is allocated"))
        };

        let position = find(type_id::PUBLISHER_POSITION);
        assert_eq!(12, position.type_id);
        assert_eq!(
            "pub-pos (concurrent): 99 100 1001 aeron:ipc",
            position.label
        );

        let limit = find(type_id::PUBLISHER_LIMIT);
        assert_eq!(1, limit.type_id);
        assert_eq!("pub-lmt: 99 100 1001 aeron:ipc", limit.label);

        let subscriber = find(type_id::SUBSCRIPTION_POSITION);
        assert_eq!(4, subscriber.type_id);
        assert_eq!("sub-pos: 100 100 1001 aeron:ipc @4096", subscriber.label);

        // Registration and owner are the ids the caller passed, and the value
        // starts where the counter manager leaves it.
        assert_eq!(99, position.registration_id);
        assert_eq!(7, position.owner_id);
        assert_eq!(99, limit.registration_id);
        assert_eq!(100, subscriber.registration_id);

        assert!(
            publish_id < limit_id && limit_id < subscribe_id,
            "ids are handed out in allocation order"
        );
    }

    #[test]
    fn an_exclusive_publication_says_so_in_its_label() {
        let mut fixture = Fixture::new();
        let (mut manager, regions) = fixture.open();

        allocate_publisher_position(
            &mut manager,
            &regions,
            7,
            99,
            100,
            1001,
            b"aeron:ipc",
            true,
            0,
        )
        .expect("an id");

        assert_eq!(
            "pub-pos (exclusive): 99 100 1001 aeron:ipc",
            regions
                .reader()
                .find_by_type_id(type_id::PUBLISHER_POSITION)
                .expect("allocated")
                .label
        );
    }
}
