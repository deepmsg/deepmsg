//! The port, its queues, and the frames coming off them.

use std::ffi::{CStr, CString, c_char, c_uint, c_void};
use std::marker::PhantomData;
use std::ptr::NonNull;
use std::sync::Arc;

use crate::Error;
use crate::mempool::{Mempool, ParkedFrame};
use crate::sys::{self, SHIM_BURST_MAX, SHIM_CKSUM_BAD, SHIM_CKSUM_GOOD};

/// How a port is configured before it is started.
#[derive(Clone, Copy, Debug)]
pub struct PortSettings {
    /// Receive queues to set up. The driver's own mapping of queues to threads
    /// decides how many, and this crate only hands them out.
    pub rx_queues: u16,
    /// Transmit queues to set up.
    pub tx_queues: u16,
    /// Descriptors per receive queue.
    pub rx_descriptors: u16,
    /// Descriptors per transmit queue.
    pub tx_descriptors: u16,
    /// Receive offloads to ask for — `RTE_ETH_RX_OFFLOAD_*` bits, which this
    /// build only ever asks for on a hardware port (D0: the virtual devices
    /// have none beyond scatter, so the software path is the one CI exercises).
    pub rx_offloads: u64,
    /// Transmit offloads to ask for.
    pub tx_offloads: u64,
}

impl Default for PortSettings {
    fn default() -> Self {
        Self {
            rx_queues: 1,
            tx_queues: 1,
            rx_descriptors: 1024,
            tx_descriptors: 1024,
            rx_offloads: 0,
            tx_offloads: 0,
        }
    }
}

/// What a port says it is, read once when it is started.
#[derive(Clone, Debug)]
pub struct PortInfo {
    /// The name EAL knows it by: a PCI address, or a virtual device's name.
    pub name: String,
    /// The PMD's name.
    pub driver: String,
    pub max_rx_queues: u16,
    pub max_tx_queues: u16,
    pub min_mtu: u16,
    pub max_mtu: u16,
    /// The socket the device is on, `None` where it reports `-1` — which is
    /// every port this build has measured, virtual and real alike (D0).
    pub socket_id: Option<u32>,
    pub rx_offloads: u64,
    pub tx_offloads: u64,
}

/// A started port.
///
/// Held in an [`Arc`]: the queues a thread was handed keep it alive, and the
/// last of them dropping is what stops and closes the device — so a port cannot
/// be closed while a thread is still receiving on it.
pub struct Port {
    id: u16,
    info: PortInfo,
}

impl Port {
    /// The names of every port EAL has.
    pub fn names() -> Vec<String> {
        // SAFETY: a plain count of the devices EAL probed.
        let count = unsafe { sys::shim_port_count() };
        (0..count).map(|id| name_of(id as u16)).collect()
    }

    /// What this port is.
    pub fn info(&self) -> &PortInfo {
        &self.info
    }

    /// This port's hardware address.
    pub fn mac(&self) -> [u8; 6] {
        let mut mac = [0u8; 6];
        // SAFETY: the buffer is six bytes, which is what the call writes.
        let rc = unsafe { sys::shim_port_mac(c_uint::from(self.id), mac.as_mut_ptr()) };
        debug_assert_eq!(rc, 0, "a started port has a MAC address");
        mac
    }

    /// The port's MTU.
    pub fn mtu(&self) -> u16 {
        let mut mtu = 0u16;
        // SAFETY: the buffer is a `u16`, which is what the call writes.
        let rc = unsafe { sys::shim_port_mtu(c_uint::from(self.id), &mut mtu) };
        debug_assert_eq!(rc, 0, "a started port reports its MTU");
        mtu
    }

    /// Set the port's MTU.
    pub fn set_mtu(&self, mtu: u16) -> Result<(), Error> {
        // SAFETY: a port this handle owns, and a number the caller chose; the
        // PMD refuses one it cannot carry.
        let rc = unsafe { sys::shim_port_set_mtu(c_uint::from(self.id), mtu) };
        if rc != 0 {
            return Err(Error::from_rte_errno("the port refused that MTU"));
        }
        Ok(())
    }

    /// Configure and start the port with this name, and hand out its queues.
    ///
    /// One queue pair per index, up to `settings.rx_queues` (and as many
    /// transmit queues): which thread gets which pair is the driver's mapping,
    /// not this crate's. A failure after the port was configured closes it
    /// again, so a caller that gives up does not leave a half-configured device
    /// behind.
    pub fn start(
        name: &str,
        settings: &PortSettings,
        pool: &Mempool,
    ) -> Result<(Arc<Self>, Vec<QueuePair>), Error> {
        let id = id_of(name)?;
        let info = info_of(id)?;

        let configured = (|| -> Result<(), Error> {
            // SAFETY: every call below takes this port's id, which came from
            // `rte_eth_dev_get_port_by_name`, and numbers that came from the
            // caller's settings. DPDK's order is configure, then the queues,
            // then start.
            unsafe {
                let rc = sys::shim_port_configure(
                    c_uint::from(id),
                    c_uint::from(settings.rx_queues),
                    c_uint::from(settings.tx_queues),
                    settings.rx_offloads,
                    settings.tx_offloads,
                );
                if rc != 0 {
                    return Err(Error::from_rte_errno("the port refused that configuration"));
                }

                for queue in 0..settings.rx_queues {
                    let rc = sys::shim_rx_queue_setup(
                        c_uint::from(id),
                        c_uint::from(queue),
                        c_uint::from(settings.rx_descriptors),
                        pool.handle().as_ptr(),
                    );
                    if rc != 0 {
                        return Err(Error::from_rte_errno("a receive queue was refused"));
                    }
                }

                for queue in 0..settings.tx_queues {
                    let rc = sys::shim_tx_queue_setup(
                        c_uint::from(id),
                        c_uint::from(queue),
                        c_uint::from(settings.tx_descriptors),
                    );
                    if rc != 0 {
                        return Err(Error::from_rte_errno("a transmit queue was refused"));
                    }
                }

                let rc = sys::shim_port_start(c_uint::from(id));
                if rc != 0 {
                    return Err(Error::from_rte_errno("the port would not start"));
                }
            }
            Ok(())
        })();

        if let Err(error) = configured {
            // SAFETY: the port was configured and is not started, which is
            // exactly what `rte_eth_dev_close` is documented to accept.
            unsafe { sys::shim_port_close(c_uint::from(id)) };
            return Err(error);
        }

        let port = Arc::new(Self { id, info });
        let pairs = (0..settings.rx_queues)
            .map(|queue| QueuePair {
                port: Arc::clone(&port),
                rx_queue: queue,
                tx_queue: queue,
                _one_thread: PhantomData,
            })
            .collect();

        Ok((port, pairs))
    }
}

impl Drop for Port {
    fn drop(&mut self) {
        // SAFETY: no queue can be alive — each holds an `Arc` of this port —
        // so no thread is receiving or transmitting on it, which is what
        // stopping requires. Closing releases the device.
        unsafe {
            sys::shim_port_stop(c_uint::from(self.id));
            sys::shim_port_close(c_uint::from(self.id));
        }
    }
}

impl std::fmt::Debug for Port {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Port")
            .field("name", &self.info.name)
            .field("driver", &self.info.driver)
            .finish()
    }
}

/// One receive queue and one transmit queue, and the thread they belong to.
///
/// `Send` and neither `Sync` nor `Clone`, which is the invariant written as a
/// type: `rte_eth_rx_burst` on one queue from two threads at once is not a
/// thing DPDK offers, and a queue pair crossing to the thread that will use it
/// is exactly what the driver does with it.
pub struct QueuePair {
    port: Arc<Port>,
    rx_queue: u16,
    tx_queue: u16,
    _one_thread: PhantomData<*mut ()>,
}

// SAFETY: the pair is moved to the thread that owns it and never shared;
// nothing in it is bound to the thread that created it (the port's id is a
// number and DPDK's queues are not thread-affine, only single-threaded).
unsafe impl Send for QueuePair {}

impl QueuePair {
    /// The port this pair belongs to.
    pub fn port(&self) -> &Arc<Port> {
        &self.port
    }

    /// Take up to `max` frames off the receive queue, handing each to `f`.
    ///
    /// A frame's bytes are borrowed for the length of the call; `f` answers
    /// [`Disposition::Keep`] to keep the frame — it is then pushed onto `kept`
    /// as a [`ParkedFrame`] — and [`Disposition::Release`] to give it straight
    /// back to the pool. Everything released goes back in **one** call, so a
    /// pass pays a constant number of crossings whatever the burst carries.
    ///
    /// `pool` is the pool the queue was set up with: the frames it hands out
    /// borrow it, so they cannot outlive it. A callback that panics leaks the
    /// frames the burst had not reached yet — a panic on this path is already
    /// the end of the process's media, and the alternative is catching it.
    pub fn rx_burst<'pool>(
        &mut self,
        pool: &'pool Mempool,
        max: usize,
        f: &mut dyn FnMut(RxFrame<'_>) -> Disposition,
        kept: &mut Vec<ParkedFrame<'pool>>,
    ) -> usize {
        let _ = pool;
        let max = max.min(SHIM_BURST_MAX);
        if max == 0 {
            return 0;
        }

        let mut descs = [sys::ShimRxDesc {
            mbuf: std::ptr::null_mut(),
            data: std::ptr::null_mut(),
            len: 0,
            checksum: sys::SHIM_CKSUM_NONE,
        }; SHIM_BURST_MAX];

        // SAFETY: the descriptor array holds `SHIM_BURST_MAX` writable
        // descriptors and `max` is at most that; the queue index is one this
        // pair was set up with, and the pair's `Send + !Sync` means no other
        // thread is in this queue.
        let count = unsafe {
            sys::shim_rx_burst(
                c_uint::from(self.port.id),
                c_uint::from(self.rx_queue),
                descs.as_mut_ptr(),
                max as c_uint,
            )
        } as usize;

        let mut released = [std::ptr::null_mut::<c_void>(); SHIM_BURST_MAX];
        let mut released_count = 0usize;

        for desc in &descs[..count] {
            // SAFETY: the shim filled this descriptor from a frame the NIC
            // handed over: `data` runs for `len` bytes, and this code is the
            // only owner until either branch below takes it.
            let bytes = unsafe { std::slice::from_raw_parts(desc.data, usize::from(desc.len)) };
            let frame = RxFrame {
                bytes,
                checksum: Checksum::of(desc.checksum),
            };

            match f(frame) {
                Disposition::Keep => {
                    let handle = NonNull::new(desc.mbuf).expect("a received frame has a handle");
                    kept.push(ParkedFrame::own(handle, desc.data, desc.len));
                }
                Disposition::Release => {
                    released[released_count] = desc.mbuf;
                    released_count += 1;
                }
            }
        }

        if released_count > 0 {
            // SAFETY: the first `released_count` entries are live mbufs this
            // code owns and is giving back exactly once.
            unsafe { sys::shim_mbuf_free_bulk(released.as_mut_ptr(), released_count as c_uint) };
        }

        count
    }
}

impl std::fmt::Debug for QueuePair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueuePair")
            .field("port", &self.port.info.name)
            .field("rx_queue", &self.rx_queue)
            .field("tx_queue", &self.tx_queue)
            .finish()
    }
}

/// A frame on its way in, borrowed for the length of the callback.
pub struct RxFrame<'a> {
    bytes: &'a [u8],
    checksum: Checksum,
}

impl<'a> RxFrame<'a> {
    /// The frame's bytes, headers and all.
    pub fn bytes(&self) -> &'a [u8] {
        self.bytes
    }

    /// The NIC's verdict on the frame's checksums.
    pub fn checksum(&self) -> Checksum {
        self.checksum
    }

    /// How long the frame is.
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Whether the frame is empty.
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

impl std::fmt::Debug for RxFrame<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RxFrame")
            .field("len", &self.bytes.len())
            .field("checksum", &self.checksum)
            .finish()
    }
}

/// What the callback wants done with the frame it was handed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Disposition {
    /// Give it back to the pool when the callback returns.
    Release,
    /// Keep it: it comes out of the burst as a [`ParkedFrame`].
    Keep,
}

/// What the NIC said about a frame's checksums.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Checksum {
    /// The port has no checksum offload, so nothing judged it. Every virtual
    /// device this build's tests run on answers this (D0).
    NotJudged,
    /// The NIC checked it and it was right.
    Good,
    /// The NIC checked it and it was wrong.
    Bad,
}

impl Checksum {
    fn of(shim_verdict: u32) -> Self {
        match shim_verdict {
            SHIM_CKSUM_GOOD => Self::Good,
            SHIM_CKSUM_BAD => Self::Bad,
            _ => Self::NotJudged,
        }
    }
}

/// The port with this name, as EAL knows it.
fn id_of(name: &str) -> Result<u16, Error> {
    let name = CString::new(name)
        .map_err(|_| Error::refused(format!("a port name contains a NUL byte: {name:?}")))?;

    // SAFETY: a NUL-terminated name that outlives the call; the answer is a
    // port id or `-1`, which is checked here.
    let id = unsafe { sys::shim_port_by_name(name.as_ptr()) };
    u16::try_from(id)
        .map_err(|_| Error::refused(format!("no port is named {}", name.to_string_lossy())))
}

/// A port's name, from its id.
fn name_of(id: u16) -> String {
    let mut buffer = [0 as c_char; 64];
    // SAFETY: the buffer is 64 bytes, which is what the shim is told.
    unsafe { sys::shim_port_name(c_uint::from(id), buffer.as_mut_ptr(), 64) };
    // SAFETY: the shim NUL-terminates what it writes.
    unsafe { CStr::from_ptr(buffer.as_ptr()) }
        .to_string_lossy()
        .into_owned()
}

/// What the port is, from its id.
fn info_of(id: u16) -> Result<PortInfo, Error> {
    let mut raw = sys::ShimPortInfo {
        driver: [0; 64],
        max_rx_queues: 0,
        max_tx_queues: 0,
        max_mtu: 0,
        min_mtu: 0,
        socket_id: -1,
        rx_offloads: 0,
        tx_offloads: 0,
    };

    // SAFETY: the struct is this crate's own layout, mirrored field for field
    // in the shim, and the port id is one EAL answered with.
    let rc = unsafe { sys::shim_port_info(c_uint::from(id), &mut raw) };
    if rc != 0 {
        return Err(Error::from_rte_errno("the port would not describe itself"));
    }

    // SAFETY: the shim NUL-terminates the driver name it copies in.
    let driver = unsafe { CStr::from_ptr(raw.driver.as_ptr()) }
        .to_string_lossy()
        .into_owned();

    Ok(PortInfo {
        name: name_of(id),
        driver,
        max_rx_queues: raw.max_rx_queues,
        max_tx_queues: raw.max_tx_queues,
        min_mtu: raw.min_mtu,
        max_mtu: raw.max_mtu,
        socket_id: u32::try_from(raw.socket_id).ok(),
        rx_offloads: raw.rx_offloads,
        tx_offloads: raw.tx_offloads,
    })
}
