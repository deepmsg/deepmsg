//! The DPDK media: the EAL, the pool and the port, in the order they open.
//!
//! One value, [`DpdkMedia`], is everything the driver has to hold for its data
//! paths to run on a NIC it drives itself — and it is a value rather than a set
//! of process globals because what it holds has to go away in an order. The
//! design's §4.13 names the order, and this module gets it from the types
//! instead of from a call sequence: a queue pair holds the port, the port holds
//! its queues' frames, and the last thing dropped is the media itself. Whoever
//! takes the pairs out takes that with them, so dropping the driver's stack is
//! what stops the port, and nothing here has to remember to.
//!
//! # What this opens, and what it leaves to the threading slice
//!
//! It opens **queue pairs** and hands them out; it does not decide which thread
//! drives which. That is §4.7's table, and the table's counts are not one
//! number: `DEDICATED` wants one receive queue and two transmit queues,
//! `SHARED_NETWORK` and `SHARED` one of each — so how many pairs a media asks
//! for is a question about the threading mode, which is not known here.
//!
//! It matters for more than tidiness. A pair is a receive queue and a transmit
//! queue at the same index, so "one receive and two transmit" is *three* pairs,
//! and a device has to have three of each to carry that. Every virtual device
//! this build's tests run on reports **one** (`net_pcap`: `max_rx_queues: 1,
//! max_tx_queues: 1`), so a media that insisted on three could not be opened
//! without a real NIC — and the close path, which is what this module is for,
//! would be the one thing that only ever ran on hardware.
//!
//! # What is *not* a value
//!
//! The EAL and the pool are process-wide, and neither can be undone:
//! `rte_eal_init` may be called once in a process and `rte_eal_cleanup` does
//! not make a second one possible (D0). So the pool is a [`OnceLock`] — the
//! same shape `Eal` has in the crate — and that is not a convenience, it is
//! what closes the D2 owed item: the crate's `ParkedFrame<'pool>` borrows the
//! pool it came from, so a frame with no lifetime parameter in its type
//! (`ReadyFrame::Mbuf`) needs a pool with the process's lifetime, and a
//! `OnceLock` is how a value gets one (P-D4-1).
//!
//! The price is written down here because it is real: a pool in a `OnceLock`
//! is never dropped on the ordinary exit path, so "every frame went back to it"
//! is not something `Drop` guarantees. It is something a test asserts, which is
//! why [`DpdkMedia::pool_size`] and [`DpdkMedia::pool`] are public — the whole
//! close path can be checked against the number the pool was made with, and the
//! check catches both a port that was not closed and a frame that leaked.
//!
//! # Order of opening
//!
//! ```text
//!   the process's cpuset         (§4.7: the lcore set is the process's own)
//!   EAL                          (before the CnC file, so a failure leaves no
//!                                 half a driver directory — §4.13)
//!   the port, by name            (found, then asked what it is)
//!   the pool                     (on that port's socket; one per process)
//!   the port, started            (with what it said it could do)
//!   the queue pairs, to be handed to whatever drives them
//! ```
//!
//! The pool comes after the port is found and before it is started, and that is
//! not an arbitrary middle: `rte_eth_rx_queue_setup` takes the pool the receive
//! queue will draw its frames from, so the pool exists before the queues do —
//! and the port's NUMA socket, which the pool is allocated on, is only known
//! once the port has been found.

use std::sync::{Arc, Mutex, OnceLock};

use deepmsg_dpdk::{
    Eal, Error as PortError, Mempool, MempoolSettings, Port, PortInfo, PortSettings, QueuePair,
    RegisteredThread,
};

use super::dpdk_port::{DpdkRxPort, DpdkTxPort};
use super::eal::{self, EalArgumentError};
use super::packet_io::{Offloads, RxPort};
use super::stack::{Clock, Stack, StackParams, TransmitQueues};
use crate::config::{
    DpdkSettings, MTU_LENGTH_DEFAULT, RCV_INITIAL_WINDOW_LENGTH_DEFAULT, ThreadingMode,
};
use crate::sys;

/// The process's frame pool, made once.
static POOL: OnceLock<Mempool> = OnceLock::new();

/// Set by the first media that finishes opening.
static OPENED: OnceLock<()> = OnceLock::new();

/// Why a DPDK media could not be opened.
#[derive(Debug)]
pub enum DpdkError {
    /// A setting the media cannot run without is not there.
    Missing {
        /// The setting's name, as an operator would look for it.
        setting: &'static str,
    },
    /// The process's own CPUs could not be read.
    Affinity(std::io::Error),
    /// The EAL's arguments could not be built.
    Arguments(EalArgumentError),
    /// The EAL would not start.
    Eal(PortError),
    /// No port with that name — or no port at all, when none was named.
    NoPort {
        /// The name asked for, or `None` for "the first one".
        name: Option<String>,
        /// What there was, for a message a reader can act on.
        found: Vec<String>,
    },
    /// The port cannot do what the configuration asks of it.
    Capability {
        /// The port's name.
        name: String,
        /// What was asked, and what the device answers.
        asked: String,
    },
    /// The pool would not be made.
    Pool(PortError),
    /// The port would not start.
    Port(PortError),
    /// A media is already open in this process.
    AlreadyOpen,
}

impl std::fmt::Display for DpdkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Missing { setting } => {
                write!(f, "the dpdk media needs `{setting}`, and it is not set")
            }
            Self::Affinity(error) => write!(
                f,
                "the CPUs this process may use could not be read: {error}"
            ),
            Self::Arguments(error) => write!(f, "{error}"),
            Self::Eal(error) => write!(f, "the EAL would not start: {error}"),
            Self::NoPort { name, found } => match name {
                Some(name) => write!(
                    f,
                    "no port is named `{name}`; the EAL found {found:?} \
                     (`deepmsg.dpdk.port`)"
                ),
                None => write!(f, "the EAL found no port to use (`deepmsg.dpdk.port`)"),
            },
            Self::Capability { name, asked } => write!(
                f,
                "port `{name}` cannot do what the configuration asks: {asked}"
            ),
            Self::Pool(error) => write!(f, "the frame pool could not be made: {error}"),
            Self::Port(error) => write!(f, "the port would not start: {error}"),
            Self::AlreadyOpen => f.write_str(
                "a dpdk media is already open in this process: the EAL and the pool are \
                 process-wide, and neither can be made twice",
            ),
        }
    }
}

impl std::error::Error for DpdkError {}

/// How many queue pairs a threading mode's map needs (§4.7).
///
/// A pair is a receive queue and a transmit queue at one index, so a mode that
/// wants one receive queue and two transmit queues asks for **three** pairs and
/// the receive port takes the first; a mode whose two roles share a thread
/// wants one of each, which is one pair.
///
/// `INVOKER` is in the second group because the map puts everything on the
/// calling thread — the mode a *media driver process* refuses to start with,
/// and one a caller driving the stack itself still has to configure for.
pub const fn queue_pairs(mode: ThreadingMode) -> u16 {
    match mode {
        ThreadingMode::Dedicated => 3,
        ThreadingMode::SharedNetwork | ThreadingMode::Shared | ThreadingMode::Invoker => 1,
    }
}

/// The three roles a media's queue pairs are handed to: one receive port and
/// the transmit queues the mode gives the two agents ([`TransmitQueues`]).
pub struct DpdkPorts {
    /// The receive queue, which the machine has one of (§4.7, 方案 A).
    pub rx: Box<dyn RxPort>,
    /// The transmit queues, indexed by
    /// [`Affinity`](crate::media::Affinity): the sender's and the receiver's.
    pub tx: TransmitQueues,
}

/// A DPDK port, its pool, and the queue pairs a driver's threads will drive.
pub struct DpdkMedia {
    settings: DpdkSettings,
    mode: ThreadingMode,
    info: PortInfo,
    pool: &'static Mempool,
    /// How many frames the pool was made with, which is what a close has to
    /// give back (see the module note).
    pool_size: u32,
    /// The queues, taken one at a time by whoever drives them. The port itself
    /// is not held here: every pair holds it, and the last one to go is what
    /// stops and closes the device (§4.13's close order by type, not by call).
    pairs: Vec<QueuePair>,
}

impl DpdkMedia {
    /// Open the media the settings describe, for the threading mode `mode`.
    ///
    /// The mode is what says how many queue pairs to set up and what each role
    /// gets ([`queue_pairs`], [`DpdkMedia::into_ports`]), which is why it is
    /// here rather than at the point the pairs are handed out: a media opened
    /// for one mode and driven as another is a mistake this shape cannot make.
    ///
    /// One call per process: the EAL starts once and the pool is made once, so
    /// a second call is a second `rte_eal_init`, which DPDK does not offer
    /// (it answers the first EAL, which is a different port than the one being
    /// asked for). What a caller gets for its trouble is an error naming that.
    ///
    /// # Errors
    ///
    /// [`DpdkError`], whose variants name the step that answered: a missing
    /// setting, an EAL that would not start, a port that is not there, a port
    /// that cannot carry `pairs` queues, or a pool that could not be made.
    pub fn open(settings: &DpdkSettings, mode: ThreadingMode) -> Result<Self, DpdkError> {
        if OPENED.get().is_some() {
            return Err(DpdkError::AlreadyOpen);
        }

        let address = settings.address.ok_or(DpdkError::Missing {
            setting: "deepmsg.dpdk.ipv4.address",
        })?;
        let _ = address;

        // The CPUs the process has *now*, before the EAL moves anything: what
        // it is told is a set it may not exceed (§4.7).
        let cpus = sys::current_thread_affinity().map_err(DpdkError::Affinity)?;
        let arguments = eal::arguments(settings, &cpus).map_err(DpdkError::Arguments)?;
        Eal::init(&arguments).map_err(DpdkError::Eal)?;

        let found = Port::names();
        let name = match &settings.port {
            Some(name) => name.clone(),
            None => found.first().cloned().ok_or(DpdkError::NoPort {
                name: None,
                found: found.clone(),
            })?,
        };

        let info = Port::describe(&name).map_err(|_| DpdkError::NoPort {
            name: Some(name.clone()),
            found,
        })?;
        let pairs = queue_pairs(mode);
        check_capability(&info, pairs)?;

        // The two offloads this build reads back are asked for exactly when the
        // port says it judges them, so what `offloads()` reports below is what
        // the frames will say (`asking_for_checksum_offloads`).
        let port_settings = PortSettings {
            rx_queues: pairs,
            tx_queues: pairs,
            rx_descriptors: settings.rx_descriptors,
            tx_descriptors: settings.tx_descriptors,
            ..PortSettings::default()
        }
        .asking_for_checksum_offloads(&info);

        let pool = pool(settings, &info, pairs)?;
        // The pool's whole count, taken before the receive queues fill
        // themselves from it: what a close has to give back is everything a
        // start took.
        let pool_size = pool.available();

        let (_port, pairs) = Port::start(&name, &port_settings, pool).map_err(DpdkError::Port)?;

        let _ = OPENED.set(());

        Ok(Self {
            settings: settings.clone(),
            mode,
            info,
            pool,
            pool_size,
            pairs,
        })
    }

    /// What the port is, as it described itself before it was configured.
    pub const fn info(&self) -> &PortInfo {
        &self.info
    }

    /// The pool every frame of this media's comes from, and goes back to.
    ///
    /// Exposed because it is the only way to see that everything went back: a
    /// pool in a `OnceLock` is never dropped, so its free count is the account
    /// of the whole close path (see the module note).
    pub const fn pool(&self) -> &'static Mempool {
        self.pool
    }

    /// How many frames the pool was made with.
    pub const fn pool_size(&self) -> u32 {
        self.pool_size
    }

    /// What the port judges, which is what its frames' verdicts will mean
    /// ([`Offloads::checksums`]).
    ///
    /// The port was asked for exactly these when it was configured
    /// (`asking_for_checksum_offloads`), so this is what a frame of it will
    /// say — and what the startup that reports what the media runs on reads.
    pub fn offloads(&self) -> Offloads {
        Offloads {
            ipv4_checksum: self.info.can_judge_ipv4_checksum(),
            udp_checksum: self.info.can_judge_udp_checksum(),
        }
    }

    /// The three roles of §4.7's map, from the pairs this media opened.
    ///
    /// What a pair *is* decides the shape of this, and it is the crate's shape
    /// rather than this build's: one receive queue and one transmit queue at
    /// the same index, driven together. So
    ///
    /// * `DEDICATED` — a thread per role — takes a pair each, and the receive
    ///   port's own transmit queue goes unused (the map asks the machine for
    ///   one receive queue and two transmit ones, and a pair cannot be asked
    ///   for one of its two halves);
    /// * the modes with both agents on one thread want **one** receive queue
    ///   and **one** transmit queue, which is one pair — and the two roles
    ///   share it, the receive port driving one direction and the transmit
    ///   port the other.
    ///
    /// # Errors
    ///
    /// [`DpdkError::Capability`] if the media does not hold the pairs its
    /// threading mode needs, which cannot happen through [`DpdkMedia::open`]
    /// and is checked rather than assumed.
    pub fn into_ports(self) -> Result<DpdkPorts, DpdkError> {
        let mode = self.mode;
        let short = |opened: usize| DpdkError::Capability {
            name: self.info.name.clone(),
            asked: format!(
                "{} queue pairs for {mode:?}, and it was opened with {opened}",
                queue_pairs(mode)
            ),
        };
        let opened = self.pairs.len();

        let mut pairs = self.pairs.into_iter();
        let mut take = || pairs.next();

        let Some(first) = take() else {
            return Err(short(opened));
        };

        let (rx, tx) = match mode {
            ThreadingMode::Dedicated => {
                let (Some(sender), Some(receiver)) = (take(), take()) else {
                    return Err(short(opened));
                };

                (
                    Box::new(DpdkRxPort::new(first, self.pool)) as Box<dyn RxPort>,
                    TransmitQueues::PerRole(
                        Box::new(DpdkTxPort::new(sender, self.pool)),
                        Box::new(DpdkTxPort::new(receiver, self.pool)),
                    ),
                )
            }
            ThreadingMode::SharedNetwork | ThreadingMode::Shared | ThreadingMode::Invoker => {
                let shared = Arc::new(Mutex::new(first));

                (
                    Box::new(DpdkRxPort::sharing(Arc::clone(&shared), self.pool))
                        as Box<dyn RxPort>,
                    TransmitQueues::Shared(Box::new(DpdkTxPort::sharing(shared, self.pool))),
                )
            }
        };

        Ok(DpdkPorts { rx, tx })
    }

    /// The stack these three roles belong to, configured from the settings the
    /// media was opened with.
    ///
    /// The stack owns the ports, and the ports own the queue pairs, so dropping
    /// the stack is what stops the port — the close order §4.13 asks for, held
    /// by the types rather than by a call sequence.
    ///
    /// # Errors
    ///
    /// [`DpdkError::Missing`] for a setting the stack has to have — the port's
    /// own address and prefix length, which nothing else can supply — and
    /// whatever [`DpdkMedia::into_ports`] answers.
    pub fn into_stack(self, now: Clock) -> Result<Arc<Stack>, DpdkError> {
        let mac = match self.pairs.first() {
            Some(pair) => pair.port().mac(),
            None => {
                return Err(DpdkError::Capability {
                    name: self.info.name.clone(),
                    asked: "a stack, from a media with no queue pairs".to_owned(),
                });
            }
        };
        let params = stack_params(&self.settings, mac)?;

        // What the counters will be labelled with, and how the timer reads the
        // numbers only the device has (design §4.10) — taken **before** the
        // pairs go into the ports, because that is the last moment anything
        // here holds a port.
        //
        // The two handles are what makes the media's own counters reachable at
        // all: a `Port` is otherwise buried in the queue pair that the endpoint
        // holding it drives from another thread, and the pool is a process
        // static nothing else can reach. They are what a device read needs and
        // nothing else does.
        let device_port = self.pairs.first().map(|pair| Arc::clone(pair.port()));
        let pool = self.pool();
        let device = device_port.map(|port| crate::media::userspace::stack::DeviceParts {
            name: self.info.name.clone(),
            driver: self.info.driver.clone(),
            read: Box::new(move || {
                let stats = port.stats().map_err(|error| error.to_string())?;
                let link = port.link().map_err(|error| error.to_string())?;

                Ok(crate::media::userspace::dpdk_counters::Device {
                    up: link.up,
                    no_buffers: stats.no_buffers,
                    received: stats.received,
                    transmitted: stats.transmitted,
                    received_bytes: stats.received_bytes,
                    transmitted_bytes: stats.transmitted_bytes,
                    missed: stats.missed,
                    mempool_available: pool.available(),
                })
            }),
        });

        let ports = self.into_ports()?;

        Ok(Stack::new(params, now, ports.rx, ports.tx, device))
    }
}

impl std::fmt::Debug for DpdkMedia {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DpdkMedia")
            .field("port", &self.info.name)
            .field("driver", &self.info.driver)
            .field("pool_size", &self.pool_size)
            .field("pairs_left", &self.pairs.len())
            .field("offloads", &self.offloads())
            .finish()
    }
}

/// The process's pool, made if it is not there yet.
///
/// One per process, which is one per port: the design's §4.8 has a pool per
/// NIC, and §4.13's "one EAL per process" is the same shape one level down.
/// Its socket is the port's, which is why this cannot be a setting — and why
/// it is `SOCKET_ID_ANY` for every device this build has measured, since they
/// all answer `-1` (D0).
fn pool(
    settings: &DpdkSettings,
    info: &PortInfo,
    pairs: u16,
) -> Result<&'static Mempool, DpdkError> {
    if let Some(pool) = POOL.get() {
        return Ok(pool);
    }

    let settings = MempoolSettings {
        count: pool_frames(settings, pairs),
        cache: settings.mempool_cache,
        socket_id: info.socket_id.map_or(-1, |socket| socket as i32),
        ..MempoolSettings::default()
    };

    let made = Mempool::create("deepmsg_dpdk_pool", &settings).map_err(DpdkError::Pool)?;

    // A second opener loses the race and answers the pool the winner made,
    // which is the only pool this process will ever have.
    let _ = POOL.set(made);

    Ok(POOL
        .get()
        .expect("set just above, or by the thread that won"))
}

/// How many frames the pool starts with (§4.8), when the configuration does not
/// say.
///
/// `pairs` is what the media is being opened with: every queue of every pair
/// fills itself from the pool as it is set up.
///
/// The design's arithmetic for its own defaults — a thousand descriptors each
/// way and a cache of 256 — lands on about eight thousand, which is the number
/// this build takes (`2^13 − 1`, the size DPDK's own ring prefers). What the
/// formula adds for the frames in flight between a receive and a send is not
/// pinned down until the ready rings and the transport limit are
/// (the threading slice), so what moves the number today is the descriptors: a
/// configuration asking for more of them than the default gets a pool with room
/// for them, and one that knows its own number says so with
/// `deepmsg.dpdk.mempool.size`.
fn pool_frames(settings: &DpdkSettings, pairs: u16) -> u32 {
    if let Some(size) = settings.mempool_size {
        return size;
    }

    // Every queue fills itself from the pool as it is set up, receive and
    // transmit alike, so the descriptors are the floor.
    let descriptors = u32::from(settings.rx_descriptors)
        .saturating_add(u32::from(settings.tx_descriptors))
        .saturating_mul(u32::from(pairs));

    let wanted = MempoolSettings::default().count.max(descriptors);

    // `2^k − 1`, which is what a ring of that many entries costs and what DPDK
    // rounds to anyway.
    wanted
        .saturating_add(1)
        .next_power_of_two()
        .saturating_sub(1)
}

/// The stack's settings, from the media's.
///
/// The address and the prefix length are the media's own: a stack answers ARP
/// for one address and hears a wildcard bind on it, so there is nothing to
/// guess and nothing to default to.
///
/// The MTU and the receive window are the driver's, not the NIC's: they are the
/// Aeron MTU the channels are configured with and the window a subscription's
/// terms come from, and the media's own MTU is the frame size the port carries
/// ([`RxPort::mtu`]).
///
/// **Static neighbours are not here yet**: `StackParams` has no field for them
/// and the neighbour table has no seeding path, so `deepmsg.dpdk.static.
/// neighbours` is read and unused until the slice that adds one.
fn stack_params(settings: &DpdkSettings, mac: [u8; 6]) -> Result<StackParams, DpdkError> {
    let (address, prefix_length) = settings.address.ok_or(DpdkError::Missing {
        setting: "deepmsg.dpdk.ipv4.address",
    })?;

    Ok(StackParams {
        address,
        prefix_length,
        mac,
        gateway: settings.gateway,
        mtu: MTU_LENGTH_DEFAULT as usize,
        receive_window: RCV_INITIAL_WINDOW_LENGTH_DEFAULT as usize,
        ephemeral_ports: settings.ephemeral_ports.clone(),
        arp_interval: settings.arp_retry,
        arp_timeout: settings.arp_timeout,
    })
}

/// Register the calling thread with the EAL, if this process has one.
///
/// `None` when no media is open, which is not a failure: a driver whose data
/// paths run on the kernel's sockets has no EAL, and nothing to register with.
///
/// The answer is a guard that unregisters on drop, so a caller holds it for as
/// long as its thread runs ([`crate::driver::run_agent`] binds it to a name for
/// exactly that reason).
///
/// A registration that fails is reported and not carried further: the thread
/// runs either way, and what is lost is the pool's per-lcore cache for it —
/// every allocation it makes goes to the shared ring instead (R7). On stderr,
/// because stderr is where this driver reports failures; the alternative is a
/// media running slower than it was configured to and saying nothing.
pub fn register_thread() -> Option<RegisteredThread> {
    OPENED.get()?;

    match RegisteredThread::register() {
        Ok(registered) => Some(registered),
        Err(error) => {
            eprintln!(
                "deepmsg-driver: this thread could not register with the EAL, so its \
                 frames go through the pool's shared ring: {error}"
            );
            None
        }
    }
}

/// Refuse a port that cannot carry the queues it is about to be asked for.
///
/// `rte_eth_dev_configure` would refuse it too, in a code with no sentence
/// attached; what this buys is a message that names the port and both numbers.
fn check_capability(info: &PortInfo, pairs: u16) -> Result<(), DpdkError> {
    if info.max_rx_queues >= pairs && info.max_tx_queues >= pairs {
        return Ok(());
    }

    Err(DpdkError::Capability {
        name: info.name.clone(),
        asked: format!(
            "{pairs} queue pairs, and it has {} receive and {} transmit",
            info.max_rx_queues, info.max_tx_queues
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings() -> DpdkSettings {
        DpdkSettings {
            address: Some((std::net::Ipv4Addr::new(10, 10, 0, 1), 24)),
            ..DpdkSettings::default()
        }
    }

    fn described(max_queues: u16) -> PortInfo {
        PortInfo {
            name: "a-port".to_owned(),
            driver: "a-pmd".to_owned(),
            max_rx_queues: max_queues,
            max_tx_queues: max_queues,
            min_mtu: 68,
            max_mtu: 1500,
            socket_id: None,
            rx_offloads: 0,
            tx_offloads: 0,
        }
    }

    /// §4.8's count, for the design's own defaults and for a configuration
    /// that moves the descriptors.
    #[test]
    fn the_pool_is_sized_for_the_descriptors_and_no_smaller() {
        assert_eq!(
            8191,
            pool_frames(&settings(), 3),
            "the design's defaults land on about eight thousand, and a ring of \
             two to the thirteenth less one is what DPDK prefers"
        );

        let bigger = DpdkSettings {
            rx_descriptors: 4096,
            tx_descriptors: 4096,
            ..settings()
        };
        assert!(
            pool_frames(&bigger, 3) >= 4096 * 2 * 3,
            "a pool smaller than the queues fill themselves from would starve \
             them: {}",
            pool_frames(&bigger, 3)
        );

        let said_so = DpdkSettings {
            mempool_size: Some(65_536),
            ..settings()
        };
        assert_eq!(
            65_536,
            pool_frames(&said_so, 3),
            "a configuration that knows its own number gets it, unfolded"
        );
    }

    /// §4.7's table, as the number of queue pairs a device is asked for.
    ///
    /// A pair is one receive queue and one transmit queue, so the two counts
    /// the table gives are not the count this asks for: `DEDICATED` wants one
    /// receive queue and **two** transmit queues, which is three pairs, and
    /// the modes with both agents on one thread want one of each, which is one.
    #[test]
    fn a_mode_asks_for_the_pairs_its_map_needs() {
        assert_eq!(3, queue_pairs(ThreadingMode::Dedicated));
        for mode in [
            ThreadingMode::SharedNetwork,
            ThreadingMode::Shared,
            ThreadingMode::Invoker,
        ] {
            assert_eq!(1, queue_pairs(mode), "{mode:?}");
        }

        assert_eq!(
            3,
            queue_pairs(ThreadingMode::DEFAULT),
            "and the driver's default mode is the three-pair one"
        );
    }

    /// A port with fewer queues than were asked for is refused with a message
    /// that names the port and both numbers, rather than by
    /// `rte_eth_dev_configure` answering a code.
    #[test]
    fn a_port_with_too_few_queues_is_refused_by_name() {
        let refused = check_capability(&described(1), 3).expect_err("one queue short");
        assert!(matches!(refused, DpdkError::Capability { .. }));
        assert!(
            refused.to_string().contains("a-port") && refused.to_string().contains("3"),
            "the message names the port and what was asked: {refused}"
        );

        assert!(check_capability(&described(1), 1).is_ok());
        assert!(check_capability(&described(3), 3).is_ok());
    }
}
