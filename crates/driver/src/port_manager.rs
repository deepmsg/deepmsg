//! The wildcard port manager: which port a channel that named none gets
//! (`aeron_port_manager.c`, 211 lines, and `aeron_port_manager.h`).
//!
//! A channel is allowed to write port zero, which means "somewhere". Two
//! different somewheres are wanted, and the manager is the thing that tells
//! them apart:
//!
//! - **the kernel's** (`0 0`, the default): the port is left at zero and the
//!   `bind` syscall picks one. The manager only *records* what was picked, so
//!   that a second channel asking for a port in the same run does not collide
//!   with the first by accident;
//! - **the driver's** (any other range): the manager hands out the next free
//!   port in `[low, high]`, rotating, and refuses when the range is full.
//!
//! It exists because of what the client has to be able to learn: a channel that
//! named port zero is a channel whose real port is knowable from nowhere else,
//! and both sides publish it in the `send-local-sockaddr` /
//! `receive-local-sockaddr` counter their endpoint writes
//! (`media/aeron_send_channel_endpoint.c:211-228`,
//! `media/aeron_receive_destination.c:88-101`).
//!
//! The reference hangs two of these on the driver context — one for each
//! direction, each with its own range
//! (`aeron_driver_context.c:428-437`, set from
//! `AERON_SENDER_WILDCARD_PORT_RANGE` / `AERON_RECEIVER_WILDCARD_PORT_RANGE`,
//! `:1058-1082`). This build keeps each beside the registry that owns the
//! endpoints it hands ports to; see [`crate::send_endpoints`] and
//! [`crate::receive_endpoints`].
//!
//! # What the range string looks like
//!
//! **Two numbers separated by a space**, `"20700 20701"` — not `low-high`.
//! The reference parses it with two `strtoll` calls and no separator handling
//! at all (`aeron_port_manager.c:176-211`): the first stops at the space, and
//! the second skips the leading whitespace itself. The Java side writes it the
//! same way (`WildcardPortManagerSystemTest.java:68-69`,
//! `.receiverWildcardPortRange("20700 20701")`), and the error a full range
//! produces repeats it space-separated — which is what a client's
//! `RegistrationException` carries (`:99-102`,
//! `"no available ports in range %" PRIu16 " %" PRIu16`).

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};

use crate::udp_channel::UdpChannel;

/// The range the manager hands ports out of, and the two ends of it.
///
/// `0 0` — the default — is not "the range of nothing": it is the flag that
/// says the **kernel** picks (`is_os_wildcard`,
/// `aeron_wildcard_port_manager_set_range`, `aeron_port_manager.c:58-65`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PortRange {
    /// The low end, inclusive.
    pub low: u16,
    /// The high end, inclusive.
    pub high: u16,
}

impl PortRange {
    /// What a driver that named no range uses: let the kernel choose
    /// (`AERON_DRIVER_*_WILDCARD_PORT_RANGE` unset, `aeron_port_manager.c:49-52`).
    pub const OS_WILDCARD: Self = Self { low: 0, high: 0 };

    /// Whether the kernel, rather than the manager, picks the port
    /// (`is_os_wildcard`, `:64`).
    pub const fn is_os_wildcard(&self) -> bool {
        self.low == 0 && self.high == 0
    }

    /// Read a range the way the reference does
    /// (`aeron_parse_port_range`, `:176-211`).
    ///
    /// # Errors
    ///
    /// [`PortRangeError`] for either half, and for a low end above the high
    /// one — the three ways the reference's parse returns `-1`.
    pub fn parse(range: &str) -> Result<Self, PortRangeError> {
        // `strtoll` skips leading whitespace itself, which is the whole of the
        // separator handling: the first parse stops at the space, and the
        // second starts at it.
        let (first, rest) = split_number(range).ok_or(PortRangeError::FirstPart)?;
        let low = u16::try_from(first).map_err(|_| PortRangeError::FirstPart)?;

        let (second, _) = split_number(rest).ok_or(PortRangeError::SecondPart)?;
        let high = u16::try_from(second).map_err(|_| PortRangeError::SecondPart)?;

        if low > high {
            return Err(PortRangeError::LowAboveHigh);
        }

        Ok(Self { low, high })
    }
}

/// Read the number a `strtoll` would, and answer it with the text it stopped
/// at. `None` when it read nothing at all.
fn split_number(text: &str) -> Option<(i64, &str)> {
    let bytes = text.as_bytes();
    let mut index = 0;

    while index < bytes.len() && bytes[index].is_ascii_whitespace() {
        index += 1;
    }

    let start = index;
    if index < bytes.len() && (bytes[index] == b'+' || bytes[index] == b'-') {
        index += 1;
    }
    let digits = index;

    while index < bytes.len() && bytes[index].is_ascii_digit() {
        index += 1;
    }

    if index == digits {
        return None;
    }

    let value = text[start..index].parse::<i64>().ok()?;

    Some((value, &text[index..]))
}

/// Why a range string was not one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PortRangeError {
    /// The first number was missing, negative or above 65535
    /// (`"failed to parse first part of port range"`, `:183-186`).
    FirstPart,
    /// The second, likewise (`:195-198`).
    SecondPart,
    /// `"low port should be less than or equal to high port"` (`:202-206`).
    LowAboveHigh,
}

impl std::fmt::Display for PortRangeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::FirstPart => "failed to parse first part of port range",
            Self::SecondPart => "failed to parse second part of port range",
            Self::LowAboveHigh => "low port should be less than or equal to high port",
        })
    }
}

impl std::error::Error for PortRangeError {}

/// Why a port could not be had.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PortError {
    /// Every port in the range is spoken for
    /// (`"no available ports in range %" PRIu16 " %" PRIu16`, `:99-102`).
    ///
    /// The words are the reference's, and they matter: a client that has run a
    /// driver out of ports reads them in its `RegistrationException`
    /// (`WildcardPortManagerSystemTest.java:90`).
    NoAvailablePorts {
        /// The low end of the range that is full.
        low: u16,
        /// The high end.
        high: u16,
    },
    /// The manager had no room to record the port
    /// (`"could not add to wildcard port manager map"`, `:114` and `:136`).
    ///
    /// A `HashMap` that has already grown once does not fail to grow again the
    /// way the reference's counter map can, so this is here for the shape of
    /// the contract rather than for a case this build can reach.
    NoRoom,
}

impl std::fmt::Display for PortError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoAvailablePorts { low, high } => {
                write!(f, "no available ports in range {low} {high}")
            }
            Self::NoRoom => f.write_str("could not add to wildcard port manager map"),
        }
    }
}

impl std::error::Error for PortError {}

/// The ports a driver has handed out, and the range it hands them out of
/// (`aeron_wildcard_port_manager_t`, `aeron_port_manager.h:41-50`).
///
/// The table counts rather than flags: a port a channel **named** is recorded
/// too (`:132-139`), so that a channel that named none is never handed a port
/// some other channel is already using. The count is what makes that safe when
/// two channels name the same port — the first `free` does not open it up
/// (`aeron_int64_counter_map_remove`, `:172`, which drops the entry outright,
/// so the last free is the one that matters).
#[derive(Debug)]
pub struct WildcardPortManager {
    /// Port to how many channels hold it (`port_table`).
    port_table: HashMap<u16, i64>,
    /// The low end of the range, and where [`Self::next_port`] wraps back to.
    low_port: u16,
    /// The high end of the range, inclusive.
    high_port: u16,
    /// Where the next search starts, which is what makes the handing out
    /// rotate rather than always restart at the bottom (`:106-110`).
    next_port: u16,
    /// Which direction this manager serves. It changes one decision: a
    /// **sender** whose channel named no `control=` is not given a managed
    /// port at all, because for a sender the port in `endpoint=` is the
    /// *remote* one and the local port is the kernel's business (`:142-159`).
    is_sender: bool,
    /// Whether the kernel picks, which is the default and the state of any
    /// driver that named no range (`:52`, `:64`).
    is_os_wildcard: bool,
}

impl WildcardPortManager {
    /// A manager for one direction, letting the kernel pick
    /// (`aeron_wildcard_port_manager_init`, `:36-56`).
    pub fn new(is_sender: bool) -> Self {
        Self {
            port_table: HashMap::new(),
            low_port: 0,
            high_port: 0,
            next_port: 0,
            is_sender,
            is_os_wildcard: true,
        }
    }

    /// A sender's manager, which is what a test that does not care about ports
    /// wants: with no range set it never hands one out.
    pub fn sender() -> Self {
        Self::new(true)
    }

    /// A receiver's, likewise.
    pub fn receiver() -> Self {
        Self::new(false)
    }

    /// Set the range the manager hands ports out of
    /// (`aeron_wildcard_port_manager_set_range`, `:58-65`).
    ///
    /// `0 0` turns the kernel's wildcard back on rather than describing an
    /// empty range, and the rotation restarts at the low end.
    pub fn set_range(&mut self, range: PortRange) {
        self.low_port = range.low;
        self.high_port = range.high;
        self.next_port = range.low;
        self.is_os_wildcard = range.is_os_wildcard();
    }

    /// The range in force, which is what a driver prints at start-up
    /// (`aeron_driver.c:658-660`).
    pub const fn range(&self) -> PortRange {
        PortRange {
            low: self.low_port,
            high: self.high_port,
        }
    }

    /// The port a channel's bind address should use, given the address the
    /// channel itself named
    /// (`aeron_wildcard_port_manager_get_managed_port`, `:121-163`).
    ///
    /// Three cases, in the reference's order:
    ///
    /// 1. **the channel named a port** — it keeps it, and the manager records
    ///    it so nothing else is handed the same one;
    /// 2. **the channel named none and the kernel is picking** — it keeps the
    ///    zero, and the kernel answers. Nothing is recorded, because the
    ///    manager never learns what was picked: the caller reads the bound
    ///    address off the socket for that (`bind_addr_and_port_func`);
    /// 3. **the channel named none and a range is set** — the next free port
    ///    in it, or [`PortError::NoAvailablePorts`] when there is none.
    ///
    /// The sender exception lives in the third case: a sender's zero is left
    /// alone unless the channel named a `control=`
    /// (`!is_sender || udp_channel->has_explicit_control`, `:142`).
    ///
    /// # Errors
    ///
    /// [`PortError`] when the range is full.
    pub fn get_managed_port(
        &mut self,
        channel: &UdpChannel,
        bind_addr: SocketAddr,
    ) -> Result<SocketAddr, PortError> {
        let named = bind_addr.port();

        if 0 != named {
            self.record(named)?;
            return Ok(bind_addr);
        }

        if self.is_os_wildcard {
            return Ok(bind_addr);
        }

        if self.is_sender && !channel.has_explicit_control {
            return Ok(bind_addr);
        }

        let port = self.allocate_open_port()?;

        Ok(SocketAddr::new(bind_addr.ip(), port))
    }

    /// Give a port back (`aeron_wildcard_port_manager_free_managed_port`,
    /// `:165-174`).
    ///
    /// Port zero is not a port: a channel the kernel picked for holds nothing
    /// in the table, and saying so is the whole of this function.
    pub fn free_managed_port(&mut self, port: u16) {
        if 0 != port {
            self.port_table.remove(&port);
        }
    }

    /// Put a port in the table, or add a holder to one already there
    /// (`aeron_int64_counter_map_add_and_get(..., 1, NULL)`).
    fn record(&mut self, port: u16) -> Result<(), PortError> {
        let entry = self.port_table.entry(port).or_insert(0);
        *entry = entry.checked_add(1).ok_or(PortError::NoRoom)?;

        Ok(())
    }

    /// The next free port, and take it
    /// (`aeron_wildcard_port_manager_allocate_open_port`, `:93-119`).
    fn allocate_open_port(&mut self) -> Result<u16, PortError> {
        let port = self.find_open_port().ok_or(PortError::NoAvailablePorts {
            low: self.low_port,
            high: self.high_port,
        })?;

        self.next_port = if port == self.high_port {
            self.low_port
        } else {
            port + 1
        };

        self.record(port)?;

        Ok(port)
    }

    /// The first port in the range nobody holds, starting the search where the
    /// last one left off and wrapping once (`:72-91`).
    ///
    /// The two passes are the reference's, and the wrap is why the low end is
    /// searched twice: a range whose free ports are all below `next_port` is
    /// found by the second pass, not missed by the first.
    ///
    /// Both loops widen to `u32`; the reference's `uint16_t` counter would wrap
    /// forever on a range ending at 65535, which is the one range it cannot
    /// finish (`:74-88`).
    fn find_open_port(&self) -> Option<u16> {
        let free = |port: &u16| !self.port_table.contains_key(port);

        (u32::from(self.next_port)..=u32::from(self.high_port))
            .find(|port| free(&(*port as u16)))
            .or_else(|| {
                (u32::from(self.low_port)..u32::from(self.next_port))
                    .find(|port| free(&(*port as u16)))
            })
            .map(|port| port as u16)
    }
}

/// The port an address names, and zero for one that names none
/// (`aeron_wildcard_port_manager_get_port`, `:22-34`).
///
/// The address's family is not read: both families carry their port as a
/// sixteen-bit value, and a caller that has a [`SocketAddr`] has already had to
/// choose one.
pub const fn port_of(addr: SocketAddr) -> u16 {
    addr.port()
}

/// Whether an address is one with nothing in it — the shape a channel writes
/// when it names no endpoint at all.
pub const fn is_unspecified(addr: SocketAddr) -> bool {
    match addr.ip() {
        IpAddr::V4(ip) => ip.is_unspecified(),
        IpAddr::V6(ip) => ip.is_unspecified(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SENDER_RANGE: PortRange = PortRange {
        low: 20702,
        high: 20702,
    };

    const RECEIVER_RANGE: PortRange = PortRange {
        low: 20700,
        high: 20701,
    };

    fn channel(uri: &str) -> UdpChannel {
        let parsed = crate::channel_uri::ChannelUri::parse(uri.as_bytes()).expect("a URI");

        UdpChannel::resolve(uri.as_bytes(), &parsed).expect("a channel")
    }

    fn receiver_channel(port: u16) -> UdpChannel {
        channel(&format!("aeron:udp?endpoint=127.0.0.1:{port}"))
    }

    fn sender_channel(port: u16) -> UdpChannel {
        channel(&format!("aeron:udp?endpoint=127.0.0.1:{port}"))
    }

    /// A dynamic sender names a `control=`, which is the only shape of sender
    /// the manager gives a port to.
    fn dynamic_sender_channel(port: u16) -> UdpChannel {
        channel(&format!(
            "aeron:udp?control=127.0.0.1:{port}|control-mode=dynamic"
        ))
    }

    fn wildcard(port: u16) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], port))
    }

    #[test]
    fn a_range_is_two_numbers_with_a_space_between_them() {
        assert_eq!(
            PortRange {
                low: 20700,
                high: 20701
            },
            PortRange::parse("20700 20701").expect("a range")
        );
        assert_eq!(
            PortRange { low: 0, high: 0 },
            PortRange::parse("0 0").expect("a range")
        );
    }

    /// The separator is the space and nothing else: a hyphen is what a reader
    /// of the *setting name* would expect, and what the reference would read
    /// as a negative second number.
    #[test]
    fn a_hyphen_is_not_a_separator() {
        assert_eq!(
            Err(PortRangeError::SecondPart),
            PortRange::parse("20700-20701")
        );
    }

    #[test]
    fn a_range_that_is_not_one_is_refused() {
        for (text, expected) in [
            ("", PortRangeError::FirstPart),
            ("  ", PortRangeError::FirstPart),
            ("20700", PortRangeError::SecondPart),
            ("20700 ", PortRangeError::SecondPart),
            ("20701 20700", PortRangeError::LowAboveHigh),
            ("-1 10", PortRangeError::FirstPart),
            ("1 -10", PortRangeError::SecondPart),
            ("65536 65536", PortRangeError::FirstPart),
        ] {
            assert_eq!(Err(expected), PortRange::parse(text), "parsing {text:?}");
        }
    }

    /// The three unit tests the plan asks for, first of three: **the range
    /// rotates**. Two channels on a two-port range get the two ports in order,
    /// and a third is refused rather than handed one of them.
    #[test]
    fn the_ports_come_out_of_the_range_in_turn() {
        let mut manager = WildcardPortManager::receiver();
        manager.set_range(RECEIVER_RANGE);

        let channel = receiver_channel(0);

        assert_eq!(
            Ok(wildcard(20700)),
            manager.get_managed_port(&channel, wildcard(0))
        );
        assert_eq!(
            Ok(wildcard(20701)),
            manager.get_managed_port(&channel, wildcard(0))
        );
        assert_eq!(
            Err(PortError::NoAvailablePorts {
                low: 20700,
                high: 20701
            }),
            manager.get_managed_port(&channel, wildcard(0))
        );
    }

    /// Second: **a range that is full says so, in the reference's words** —
    /// which is what a client reads back in its `RegistrationException`
    /// (`WildcardPortManagerSystemTest.java:90`).
    #[test]
    fn a_full_range_names_the_range_it_could_not_fit_into() {
        let mut manager = WildcardPortManager::sender();
        manager.set_range(SENDER_RANGE);

        let channel = dynamic_sender_channel(0);

        assert_eq!(
            Ok(SocketAddr::from(([127, 0, 0, 1], 20702))),
            manager.get_managed_port(&channel, wildcard(0))
        );

        let error = manager
            .get_managed_port(&channel, wildcard(0))
            .expect_err("a full range");

        assert_eq!("no available ports in range 20702 20702", error.to_string());
    }

    /// Third: **`0 0` leaves the port to the kernel** — the address comes back
    /// untouched, and nothing is recorded, because a port the manager did not
    /// pick is not one it can hand out again.
    #[test]
    fn the_os_wildcard_leaves_the_choice_to_the_kernel() {
        let mut manager = WildcardPortManager::receiver();

        assert_eq!(
            Ok(wildcard(0)),
            manager.get_managed_port(&receiver_channel(0), wildcard(0))
        );
        assert!(manager.port_table.is_empty());
    }

    /// A port a channel **named** is recorded, so the manager does not hand it
    /// to a channel that named none.
    #[test]
    fn a_port_a_channel_named_is_not_handed_to_another() {
        let mut manager = WildcardPortManager::receiver();
        manager.set_range(PortRange {
            low: 20700,
            high: 20701,
        });

        let named = receiver_channel(0);

        assert_eq!(
            Ok(wildcard(20700)),
            manager.get_managed_port(&named, wildcard(20700))
        );
        assert_eq!(
            Ok(wildcard(20701)),
            manager.get_managed_port(&named, wildcard(0))
        );
        assert_eq!(Some(&1), manager.port_table.get(&20700));
    }

    /// A sender's zero is the *remote* port's business unless the channel
    /// named a `control=`: for a plain sender the local port is the kernel's,
    /// and a managed one would be a port nothing ever binds.
    #[test]
    fn a_sender_is_given_a_port_only_when_it_named_a_control() {
        let mut manager = WildcardPortManager::sender();
        manager.set_range(SENDER_RANGE);

        assert_eq!(
            Ok(wildcard(0)),
            manager.get_managed_port(&sender_channel(0), wildcard(0))
        );
        assert_eq!(
            Ok(wildcard(20702)),
            manager.get_managed_port(&dynamic_sender_channel(0), wildcard(0))
        );
    }

    /// A port given back comes round again, and the rotation picks up where it
    /// left off rather than restarting — which is what lets the oracle's third
    /// subscription find the port the second one gave up
    /// (`WildcardPortManagerSystemTest.java:92-96`).
    #[test]
    fn a_port_comes_back_round_when_it_is_given_up() {
        let mut manager = WildcardPortManager::receiver();
        manager.set_range(RECEIVER_RANGE);

        let channel = receiver_channel(0);
        let first = manager
            .get_managed_port(&channel, wildcard(0))
            .expect("a port");
        let second = manager
            .get_managed_port(&channel, wildcard(0))
            .expect("a port");

        assert_eq!(wildcard(20700), first);
        assert_eq!(wildcard(20701), second);

        // The rotation is at the low end again, so the first free port it
        // finds is the one just given back.
        manager.free_managed_port(second.port());

        assert_eq!(
            Ok(wildcard(20701)),
            manager.get_managed_port(&channel, wildcard(0))
        );
    }

    /// Giving back port zero is nothing to do: no channel the kernel chose for
    /// is in the table, and the manager must not decide one is.
    #[test]
    fn giving_port_zero_back_is_nothing() {
        let mut manager = WildcardPortManager::receiver();
        manager.set_range(RECEIVER_RANGE);

        manager.free_managed_port(0);

        assert_eq!(
            Ok(wildcard(20700)),
            manager.get_managed_port(&receiver_channel(0), wildcard(0))
        );
    }

    /// The manager is told the address's port, not its family
    /// (`aeron_wildcard_port_manager_get_port`, `:22-34`).
    #[test]
    fn the_port_is_read_from_the_address_whatever_its_family() {
        assert_eq!(20700, port_of("[::1]:20700".parse().expect("an address")));
        assert_eq!(20700, port_of(wildcard(20700)));
        assert_eq!(0, port_of(wildcard(0)));
        assert!(is_unspecified("0.0.0.0:40456".parse().expect("an address")));
        assert!(!is_unspecified(wildcard(40456)));
    }
}
