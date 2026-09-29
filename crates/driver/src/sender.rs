//! The sender agent: the thread that owns the send sockets and the
//! publications.
//!
//! Mirrors `aeron-driver/src/main/c/aeron_driver_sender.c`. It is the first
//! thing in this build that is a *thread of its own*, and what it exists to
//! keep apart is the conductor's control plane from the data plane's two
//! costs: a `sendmmsg` on a socket whose buffer is full, and a `recvmmsg` that
//! walks every endpoint's queue.
//!
//! # It sends *and* receives
//!
//! A publication is not one-way: status messages (the receivers' flow
//! control), NAKs, RTTMs and ERR frames arrive on the *same* socket the data
//! leaves by, and they are read here — by the sender's own poll
//! (`aeron_driver_sender.c:154-171`). That is why the reference's sender has a
//! poller at all, and why a data plane without this half cannot be told what
//! the far end has room for. `send.to.sm.poll.ratio` decides how often the
//! second half runs; this build polls every pass, which is that ratio at one
//! and costs only the syscalls.
//!
//! # Who owns what
//!
//! The conductor decides — it parses the channel, allocates the counters, and
//! creates the endpoints and the publications — and then *moves* them here
//! through [`SenderCommand`]. That is the reference's arrangement too (its
//! `aeron_driver_sender_proxy_on_add_endpoint` is called from
//! `aeron_driver_conductor.c:2015`), and it is what keeps each data structure
//! with one owner: after the handover an endpoint's socket and a publication's
//! term buffer have exactly one, and every other party addresses them by id.
//!
//! # Counters, without owning them
//!
//! Both threads need the counters, and neither may own the mapping. The
//! conductor holds the [`CncFile`] behind an `Arc`, and each agent derives its
//! own [`CounterRegions`] view per pass — the same memory, seen through the
//! same atomics, which is what the whole CnC contract is. The agent's
//! [`CounterManager`] is its own and is used only to *read and write values*:
//! allocation stays on the conductor, where the ownership rules are.

use std::io;
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, Sender as Channel};
use std::thread::JoinHandle;

use deepmsg_cnc::{CncFile, CounterManager, CounterRegions};

use crate::idle::Backoff;
use crate::media::send_endpoint::SendChannelEndpoint;
use crate::network_publication::NetworkPublication;
use crate::protocol::{
    ErrorFrame, FrameHeader, NakFrame, StatusMessageFrame, frame_type, header_flags, is_frame_valid,
};
use crate::sys::socket::Datagrams;
use crate::system_counters::{self, System};

/// How many datagrams one poll may read
/// (`AERON_DRIVER_SENDER_IO_VECTOR_LENGTH_MAX`,
/// `aeron-driver/src/main/c/aeron_driver_context.h:55`).
const RECEIVE_SLOTS: usize = 16;

/// What the conductor asks the sender to do.
pub enum SenderCommand {
    /// Take ownership of an endpoint and its socket.
    AddEndpoint {
        /// The id the conductor knows it by.
        id: u64,
        /// The endpoint itself.
        endpoint: Box<SendChannelEndpoint>,
    },
    /// Take ownership of a publication and start sending it.
    AddPublication {
        /// The publication itself.
        publication: Box<NetworkPublication>,
    },
    /// Stop sending a publication.
    RemovePublication {
        /// Which one.
        registration_id: i64,
    },
    /// Close an endpoint's socket and give the endpoint up.
    RemoveEndpoint {
        /// Which one.
        id: u64,
    },
    /// Stop the thread.
    Stop,
}

/// What the sender tells the conductor.
#[derive(Debug)]
pub enum SenderEvent {
    /// An endpoint was closed: its channel-status counter can be freed, which
    /// only the conductor may do.
    EndpointRemoved {
        /// Which endpoint.
        id: u64,
    },
    /// A publication is no longer being sent.
    PublicationRemoved {
        /// Which publication.
        registration_id: i64,
    },
    /// Something for the conductor to record: a socket that refused a send, a
    /// frame that could not be believed.
    Fault {
        /// The protocol error code to record it under.
        error_code: i32,
        /// The words.
        description: String,
    },
}

/// The conductor's end of the conversation.
pub struct SenderProxy {
    commands: Channel<SenderCommand>,
    events: Receiver<SenderEvent>,
}

impl SenderProxy {
    /// Ask the sender to take an endpoint.
    ///
    /// # Errors
    ///
    /// [`io::Error`] when the thread is gone.
    pub fn add_endpoint(&self, id: u64, endpoint: Box<SendChannelEndpoint>) -> io::Result<()> {
        self.commands
            .send(SenderCommand::AddEndpoint { id, endpoint })
            .map_err(|_| stopped())
    }

    /// Ask the sender to take a publication.
    ///
    /// # Errors
    ///
    /// [`io::Error`] when the thread is gone.
    pub fn add_publication(&self, publication: Box<NetworkPublication>) -> io::Result<()> {
        self.commands
            .send(SenderCommand::AddPublication { publication })
            .map_err(|_| stopped())
    }

    /// Ask the sender to stop sending a publication.
    ///
    /// # Errors
    ///
    /// [`io::Error`] when the thread is gone.
    pub fn remove_publication(&self, registration_id: i64) -> io::Result<()> {
        self.commands
            .send(SenderCommand::RemovePublication { registration_id })
            .map_err(|_| stopped())
    }

    /// Ask the sender to close an endpoint.
    ///
    /// # Errors
    ///
    /// [`io::Error`] when the thread is gone.
    pub fn remove_endpoint(&self, id: u64) -> io::Result<()> {
        self.commands
            .send(SenderCommand::RemoveEndpoint { id })
            .map_err(|_| stopped())
    }

    /// Take everything the sender has said since the last call.
    pub fn poll(&self) -> Vec<SenderEvent> {
        self.events.try_iter().collect()
    }

    /// Ask the thread to stop.
    ///
    /// # Errors
    ///
    /// [`io::Error`] when the thread is gone.
    pub fn stop(&self) -> io::Result<()> {
        self.commands
            .send(SenderCommand::Stop)
            .map_err(|_| stopped())
    }
}

/// The thread itself, with its proxy.
pub struct Sender {
    proxy: SenderProxy,
    thread: Option<JoinHandle<()>>,
}

impl Sender {
    /// Start the sender thread.
    ///
    /// `values_length` is the counters region's length, which is what fixes a
    /// counter manager's id space; the agent's own manager is built from it so
    /// that a counter id means the same thing on both sides.
    ///
    /// # Errors
    ///
    /// [`io::Error`] if the thread cannot be spawned.
    pub fn start(
        cnc: Arc<CncFile>,
        values_length: usize,
        free_to_reuse_timeout_ms: i64,
        mtu_length: usize,
        cycle_threshold_ns: i64,
    ) -> io::Result<Self> {
        let (command_tx, command_rx) = mpsc::channel::<SenderCommand>();
        let (event_tx, event_rx) = mpsc::channel::<SenderEvent>();

        let thread = std::thread::Builder::new()
            .name("deepmsg-sender".to_owned())
            .spawn(move || {
                let Some(counters) = CounterManager::new(values_length, free_to_reuse_timeout_ms)
                else {
                    return;
                };

                let mut sender =
                    SenderThread::new(cnc, counters, mtu_length, cycle_threshold_ns, event_tx);
                sender.run(&command_rx);
            })?;

        Ok(Self {
            proxy: SenderProxy {
                commands: command_tx,
                events: event_rx,
            },
            thread: Some(thread),
        })
    }

    /// The conductor's end.
    pub const fn proxy(&self) -> &SenderProxy {
        &self.proxy
    }

    /// Ask the thread to stop and wait for it.
    ///
    /// # Errors
    ///
    /// [`io::Error`] when the thread is already gone or panicked.
    pub fn close(&mut self) -> io::Result<()> {
        let _ = self.proxy.stop();

        if let Some(thread) = self.thread.take() {
            thread
                .join()
                .map_err(|_| io::Error::other("the sender thread panicked"))?;
        }

        Ok(())
    }
}

/// An error for "the thread is not there", which every proxy method reports.
fn stopped() -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, "the sender thread has stopped")
}

/// What the thread owns.
struct SenderThread {
    cnc: Arc<CncFile>,
    counters: CounterManager,
    cycle_threshold_ns: i64,
    events: Channel<SenderEvent>,
    endpoints: Vec<(u64, Box<SendChannelEndpoint>)>,
    publications: Vec<NetworkPublication>,
    /// One buffer per receive slot, allocated once.
    buffers: Vec<Vec<u8>>,
    datagrams: Datagrams,
    last_cycle_ns: i64,
    idle: Backoff,
}

impl SenderThread {
    fn new(
        cnc: Arc<CncFile>,
        counters: CounterManager,
        mtu_length: usize,
        cycle_threshold_ns: i64,
        events: Channel<SenderEvent>,
    ) -> Self {
        Self {
            cnc,
            counters,
            cycle_threshold_ns,
            events,
            endpoints: Vec::new(),
            publications: Vec::new(),
            buffers: (0..RECEIVE_SLOTS).map(|_| vec![0u8; mtu_length]).collect(),
            datagrams: Datagrams::new(),
            last_cycle_ns: deepmsg_core::clock::epoch_nano_time(),
            idle: Backoff::new(),
        }
    }

    /// The loop: commands, then a pass as long as there is work.
    fn run(&mut self, commands: &Receiver<SenderCommand>) {
        loop {
            let mut stop = false;

            for command in commands.try_iter() {
                match command {
                    SenderCommand::AddEndpoint { id, endpoint } => {
                        self.endpoints.push((id, endpoint));
                    }
                    SenderCommand::AddPublication { publication } => {
                        self.publications.push(*publication);
                    }
                    SenderCommand::RemovePublication { registration_id } => {
                        self.publications
                            .retain(|publication| publication.registration_id != registration_id);
                        let _ = self
                            .events
                            .send(SenderEvent::PublicationRemoved { registration_id });
                    }
                    SenderCommand::RemoveEndpoint { id } => {
                        self.endpoints.retain(|(endpoint_id, _)| *endpoint_id != id);
                        let _ = self.events.send(SenderEvent::EndpointRemoved { id });
                    }
                    SenderCommand::Stop => stop = true,
                }
            }

            let work = if stop { 0 } else { self.do_work() };
            self.idle.idle(work);

            if stop {
                break;
            }
        }
    }

    /// One pass: read the control frames, then send each publication's share
    /// (`aeron_driver_sender_do_send`,
    /// `aeron-driver/src/main/c/aeron_driver_sender.c:132-260`).
    ///
    /// The body is written against the fields rather than through methods so
    /// that the counter view's borrow and the mutable borrows of the two lists
    /// are visibly disjoint — which is what the borrow checker asks for, and
    /// what makes it obvious that this pass touches nothing else.
    fn do_work(&mut self) -> usize {
        // The Arc is cloned rather than borrowed so that the regions' borrow is
        // of this local: a region view holds the mapping, and holding `self`
        // borrowed for its lifetime would forbid every mutation below.
        let cnc = Arc::clone(&self.cnc);
        let Some(regions) = cnc.counter_regions() else {
            return 0;
        };
        let system = System::new(&self.counters, &regions);

        let mut work = Self::receive_control_frames(
            &mut self.endpoints,
            &mut self.publications,
            &mut self.buffers,
            &mut self.datagrams,
            &system,
            &self.counters,
            &regions,
            &self.events,
        );

        work += Self::send_publications(
            &mut self.endpoints,
            &mut self.publications,
            &system,
            &self.counters,
            &regions,
            &self.events,
        );

        Self::track_cycle(
            &self.counters,
            &regions,
            self.cycle_threshold_ns,
            &mut self.last_cycle_ns,
        );
        work
    }

    /// Read everything the endpoints' sockets hold and hand each frame to the
    /// publication it names (`aeron_send_channel_endpoint_dispatch`,
    /// `media/aeron_send_channel_endpoint.c:466-513`).
    #[allow(clippy::too_many_arguments)] // the fields of one pass, made explicit
    fn receive_control_frames(
        endpoints: &mut [(u64, Box<SendChannelEndpoint>)],
        publications: &mut [NetworkPublication],
        buffers: &mut [Vec<u8>],
        datagrams: &mut Datagrams,
        system: &System<'_>,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
        events: &Channel<SenderEvent>,
    ) -> usize {
        let mut work = 0;

        for index in 0..endpoints.len() {
            // The borrow of this endpoint ends before the datagrams are
            // dispatched, because a dispatch may need to *send* through any of
            // them (a resend answers a NAK on the endpoint it arrived at).
            let received = endpoints[index]
                .1
                .transport_mut()
                .receive(buffers, datagrams);

            let received = match received {
                Ok(received) => received,
                Err(error) => {
                    let _ = events.send(SenderEvent::Fault {
                        error_code: deepmsg_cnc::command::ERROR_CODE_GENERIC_ERROR,
                        description: format!("could not receive on a send endpoint: {error}"),
                    });
                    continue;
                }
            };

            if received == 0 {
                continue;
            }

            let batch = *datagrams;
            let mut bytes_received = 0i64;

            for (slot, datagram) in batch.as_slice().iter().enumerate() {
                bytes_received += i64::try_from(datagram.length).unwrap_or(0);
                work += 1;

                Self::dispatch(
                    counters,
                    regions,
                    publications,
                    endpoints,
                    &buffers[slot][..datagram.length],
                );
            }

            system.add(system_counters::id::BYTES_RECEIVED, bytes_received);
        }

        work
    }

    /// One frame, to the publication that names it
    /// (`aeron_send_channel_endpoint_dispatch`, `:466-513`).
    ///
    /// An associated function rather than a method so that the borrow of the
    /// datagram buffers and the borrow of the publication list are visibly
    /// disjoint.
    fn dispatch(
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
        publications: &mut [NetworkPublication],
        endpoints: &mut [(u64, Box<SendChannelEndpoint>)],
        bytes: &[u8],
    ) {
        let system = System::new(counters, regions);

        let Some(header) = FrameHeader::read(bytes) else {
            return;
        };

        if !is_frame_valid(&header, bytes.len()) {
            system.increment(system_counters::id::INVALID_PACKETS);
            return;
        }

        let now_ns = deepmsg_core::clock::epoch_nano_time();

        match header.frame_type {
            frame_type::SM => {
                if let Some(frame) = StatusMessageFrame::read(bytes) {
                    if let Some(publication) =
                        find_publication(publications, frame.stream_id, frame.session_id)
                    {
                        // `SEND_SETUP` is not a position report: it is a
                        // receiver saying it has no image and wants the stream
                        // described (`aeron_send_channel_endpoint.c:649-656`).
                        if header.flags & header_flags::SM_SEND_SETUP != 0 {
                            publication.trigger_send_setup_frame();
                        } else {
                            publication.on_status_message(
                                &frame,
                                header.flags,
                                &system,
                                counters,
                                regions,
                                now_ns,
                            );
                        }
                    }
                }
            }
            frame_type::NAK => {
                if let Some(frame) = NakFrame::read(bytes) {
                    let Some(index) =
                        index_of_publication(publications, frame.stream_id, frame.session_id)
                    else {
                        return;
                    };

                    let endpoint_id = publications[index].endpoint_id;
                    let Some(position) = endpoints.iter().position(|(id, _)| *id == endpoint_id)
                    else {
                        return;
                    };

                    let (_, endpoint) = &mut endpoints[position];
                    let publication = &mut publications[index];

                    // The resend happens *inside* `on_nak` when the channel's
                    // delay is zero — the same place the reference's callback
                    // fires (`aeron_retransmit_handler.c:110-124`).
                    let _ =
                        publication.on_nak(&frame, &system, endpoint, counters, regions, now_ns);
                }
            }
            frame_type::ERR => {
                if let Some(frame) = ErrorFrame::read(bytes) {
                    if let Some(publication) =
                        find_publication(publications, frame.stream_id, frame.session_id)
                    {
                        publication.on_error(&frame, &system, counters, regions);
                    }
                }
            }
            frame_type::RTTM => {
                // A measurement reply. The `max` strategy asks for none and has
                // nothing to do with the answer — the reference's `max` is a
                // no-op for it too (`aeron_flow_control.c:87-105`).
            }
            _ => {}
        }
    }

    /// Send every publication's share. The reference rotates one publication
    /// per call of its flywheel; this sends each of them every pass, which is
    /// the same set of datagrams with a different interleaving.
    #[allow(clippy::too_many_arguments)] // the fields of one pass, made explicit
    fn send_publications(
        endpoints: &mut [(u64, Box<SendChannelEndpoint>)],
        publications: &mut [NetworkPublication],
        system: &System<'_>,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
        events: &Channel<SenderEvent>,
    ) -> usize {
        let now_ns = deepmsg_core::clock::epoch_nano_time();
        let mut work = 0;

        for publication in publications.iter_mut() {
            let endpoint_id = publication.endpoint_id;

            let Some(position) = endpoints.iter().position(|(id, _)| *id == endpoint_id) else {
                continue;
            };

            let endpoint = &mut endpoints[position].1;
            let registration_id = publication.registration_id;

            match publication.send(endpoint, system, counters, regions, now_ns) {
                Ok(bytes) => {
                    if bytes > 0 {
                        system.add(
                            system_counters::id::BYTES_SENT,
                            i64::try_from(bytes).unwrap_or(i64::MAX),
                        );
                        work += 1;
                    }
                }
                Err(error) => {
                    let _ = events.send(SenderEvent::Fault {
                        error_code: deepmsg_cnc::command::ERROR_CODE_GENERIC_ERROR,
                        description: format!(
                            "could not send on the channel of publication {registration_id}: {error}"
                        ),
                    });
                }
            }
        }

        work
    }

    /// Measure the pass that just ended, and count it if it ran long — the
    /// same duty-cycle tracker the conductor keeps, on the sender's two
    /// counters (`aeron-driver/src/main/c/aeron_driver_sender.c:262-276`).
    fn track_cycle(
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
        cycle_threshold_ns: i64,
        last_cycle_ns: &mut i64,
    ) {
        let now_ns = deepmsg_core::clock::epoch_nano_time();
        let cycle_ns = now_ns.saturating_sub(*last_cycle_ns);
        *last_cycle_ns = now_ns;

        system_counters::propose_max(
            counters,
            regions,
            system_counters::id::SENDER_MAX_CYCLE_TIME,
            cycle_ns,
        );

        if cycle_ns > cycle_threshold_ns {
            system_counters::increment(
                counters,
                regions,
                system_counters::id::SENDER_CYCLE_TIME_THRESHOLD_EXCEEDED,
            );
        }
    }
}

/// The publication a control frame names, as an index — for a caller that
/// needs another field beside it and so cannot hold the borrow.
fn index_of_publication(
    publications: &[NetworkPublication],
    stream_id: i32,
    session_id: i32,
) -> Option<usize> {
    publications.iter().position(|publication| {
        publication.stream_id == stream_id && publication.session_id == session_id
    })
}

/// The publication a control frame names
/// (`aeron_int64_to_ptr_hash_map_get(&endpoint->publication_dispatch_map, aeron_map_compound_key(stream_id, session_id))`).
fn find_publication(
    publications: &mut [NetworkPublication],
    stream_id: i32,
    session_id: i32,
) -> Option<&mut NetworkPublication> {
    publications.iter_mut().find(|publication| {
        publication.stream_id == stream_id && publication.session_id == session_id
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::channel_uri::ChannelUri;
    use crate::flowcontrol::MaxStrategy;
    use crate::media::TransportParams;
    use crate::network_publication::{NetworkPublication, PublicationCounters};
    use crate::publication_params::PublicationParams;
    use crate::retransmit_handler::RetransmitHandler;
    use crate::sys::AddressFamily;
    use crate::sys::socket::DatagramSocket;
    use crate::udp_channel::UdpChannel;
    use deepmsg_cnc::create::COUNTERS_VALUES_BUFFER_LENGTH_MIN;
    use deepmsg_cnc::{CncIdentity, CncLayout};
    use deepmsg_core::buffer::AtomicBuffer;
    use deepmsg_core::logbuffer::descriptor;
    use deepmsg_core::logbuffer::frame::{FLAG_UNFRAGMENTED, Frame, TYPE_DATA};
    use deepmsg_core::logbuffer::logfile::LogFile;
    use deepmsg_core::logbuffer::position::RawTail;

    struct TempDir(std::path::PathBuf);

    impl TempDir {
        fn new() -> Self {
            static NEXT: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
            let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let dir =
                std::env::temp_dir().join(format!("deepmsg-sender-{}-{n}", std::process::id()));
            std::fs::create_dir_all(&dir).expect("a temp directory");

            Self(dir)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    const TERM_LENGTH: i32 = 64 * 1024;

    /// A sender thread and a publication producing to a socket this test owns.
    ///
    /// The whole path is exercised: the conductor-side create, the handover
    /// through the command channel, the sender's own poll of its socket, and
    /// the bytes that leave.
    #[test]
    fn a_publication_handed_to_the_sender_is_sent() {
        let dir = TempDir::new();
        let cnc = CncFile::create(
            &dir.0,
            &CncLayout {
                counters_values_length: COUNTERS_VALUES_BUFFER_LENGTH_MIN,
                ..CncLayout::default()
            },
            &CncIdentity {
                liveness_timeout_ns: 10_000_000_000,
                start_timestamp_ms: deepmsg_core::clock::epoch_millis(),
                pid: i64::from(std::process::id()),
            },
        )
        .expect("a CnC file");

        let cnc = Arc::new(cnc);

        let listener = DatagramSocket::open(AddressFamily::Inet).expect("a socket");
        listener
            .bind("127.0.0.1:0".parse().expect("an address"))
            .expect("a bind");
        listener.set_nonblocking().expect("non-blocking");
        let bound = listener.local_address().expect("a bound address");

        let uri = format!("aeron:udp?endpoint={bound}");
        let parsed = ChannelUri::parse(uri.as_bytes()).expect("a URI");
        let channel = UdpChannel::resolve(uri.as_bytes(), &parsed).expect("a channel");

        let endpoint = channel.clone();
        let mut metadata = vec![0u8; COUNTERS_VALUES_BUFFER_LENGTH_MIN * 4];
        let mut values = vec![0u8; COUNTERS_VALUES_BUFFER_LENGTH_MIN];
        let endpoint_regions = CounterRegions::new(
            AtomicBuffer::from_slice_mut(&mut metadata).expect("aligned"),
            AtomicBuffer::from_slice_mut(&mut values).expect("aligned"),
        )
        .expect("four-to-one");
        let mut endpoint_manager =
            CounterManager::new(COUNTERS_VALUES_BUFFER_LENGTH_MIN, 1_000).expect("room");

        let endpoint = SendChannelEndpoint::create(
            endpoint,
            &TransportParams::default(),
            &mut endpoint_manager,
            &endpoint_regions,
            7,
            1,
        )
        .expect("an endpoint");

        // The publication's six counters, allocated on the conductor's side of
        // the handover.
        let (counters, publication) = {
            let regions = cnc.counter_regions().expect("regions");
            let mut manager =
                CounterManager::new(COUNTERS_VALUES_BUFFER_LENGTH_MIN, 1_000).expect("room");

            let allocate = |manager: &mut CounterManager, name: &[u8]| {
                manager
                    .allocate(&regions, 12, &[], name, 1)
                    .expect("a counter")
            };

            let ids = PublicationCounters {
                pub_pos: allocate(&mut manager, b"pub-pos"),
                pub_lmt: allocate(&mut manager, b"pub-lmt"),
                snd_pos: allocate(&mut manager, b"snd-pos"),
                snd_lmt: allocate(&mut manager, b"snd-lmt"),
                snd_bpe: allocate(&mut manager, b"snd-bpe"),
                snd_naks_received: allocate(&mut manager, b"snd-naks"),
            };

            // The window is open and the producer has published a frame.
            let _ = manager.set_value(&regions, ids.snd_lmt, 1024);
            let _ = manager.set_value(&regions, ids.pub_lmt, 1024);

            let log = LogFile::create(
                &dir.0.join("publication.logbuffer"),
                TERM_LENGTH,
                4096,
                false,
            )
            .expect("a log buffer");
            let term = log.term(0).expect("a term");
            let frame = Frame::new(&term, 0);
            frame
                .begin(132, FLAG_UNFRAGMENTED, TYPE_DATA, 0, 42, 1001, 1_000)
                .expect("in range");
            frame.write_payload(&[9u8; 100]).expect("in range");
            frame.publish(132).expect("in range");
            log.metadata()
                .expect("metadata")
                .store_i64_release(
                    descriptor::TERM_TAIL_COUNTERS_OFFSET,
                    RawTail::new(1_000, 160).raw(),
                )
                .expect("in range");

            let params = PublicationParams {
                term_length: TERM_LENGTH,
                term_length_named: false,
                mtu_length: 1408,
                mtu_length_named: false,
                publication_window_length: 32 * 1024,
                max_resend: 0,
                entity_tag: -1,
                response_correlation_id: -1,
                session_id: Some(42),
                linger_timeout_ns: 5_000_000_000,
                untethered_window_limit_timeout_ns: 5_000_000_000,
                untethered_linger_timeout_ns: 5_000_000_000,
                untethered_resting_timeout_ns: 10_000_000_000,
                is_sparse: true,
                signal_eos: true,
                spies_simulate_connection: false,
                starting_position: None,
                initial_term_id: 1_000,
            };

            let publication = NetworkPublication::create(
                7,
                9,
                42,
                1001,
                1,
                uri.as_bytes(),
                Box::new(log),
                &params,
                false,
                ids,
                4,
                MaxStrategy::default(),
                RetransmitHandler::new(1_000, 5_000_000, false, 1),
                4096,
                crate::sys::SocketBufferLengths {
                    rcvbuf: 0,
                    sndbuf: 0,
                },
                0,
                0,
                0,
            )
            .expect("a publication");

            (manager, publication)
        };

        let mut sender = Sender::start(
            Arc::clone(&cnc),
            COUNTERS_VALUES_BUFFER_LENGTH_MIN,
            1_000,
            1408,
            100_000_000,
        )
        .expect("a sender");

        sender
            .proxy()
            .add_endpoint(1, Box::new(endpoint))
            .expect("an endpoint");
        sender
            .proxy()
            .add_publication(Box::new(publication))
            .expect("a publication");

        // The thread runs on its own schedule, so poll for what it sent. It
        // says SETUP while nothing has answered (`has_initial_connection` is a
        // status message away) and the frame itself is what this waits for.
        let mut buffers = vec![vec![0u8; 2048]];
        let mut datagrams = Datagrams::new();
        let mut setup_frames = 0;
        let mut data_frame: Option<Vec<u8>> = None;

        for _ in 0..200 {
            let received = listener
                .receive_batch(&mut buffers, &mut datagrams)
                .unwrap_or(0);

            for (slot, datagram) in datagrams.as_slice()[..received].iter().enumerate() {
                let bytes = &buffers[slot][..datagram.length];
                let header = FrameHeader::read(bytes).expect("every datagram is a frame");

                match header.frame_type {
                    frame_type::SETUP => setup_frames += 1,
                    frame_type::DATA => data_frame = Some(bytes.to_vec()),
                    other => panic!("a sender sent a {other:#x} frame"),
                }
            }

            if data_frame.is_some() {
                break;
            }

            std::thread::sleep(std::time::Duration::from_millis(5));
        }

        sender.close().expect("a clean stop");

        let data_frame = data_frame.expect("the sender thread put the frame on the wire");
        let header = FrameHeader::read(&data_frame).expect("a header");
        assert_eq!(frame_type::DATA, header.frame_type);
        assert_eq!(132, header.frame_length);
        assert!(data_frame[32..132].iter().all(|byte| *byte == 9));
        assert!(
            setup_frames > 0,
            "and it said SETUP first: nothing had answered it"
        );

        // And the sender position moved on the conductor's own view of the
        // counters, which is the shared-memory contract this test is really
        // about: two threads, one region.
        let regions = cnc.counter_regions().expect("regions");
        assert_eq!(
            Some(160),
            counters.value(&regions, publication_id(&counters, &regions))
        );
    }

    /// The `snd-pos` counter's id, recovered by its label — the test allocated
    /// the ids itself, so a label search is as good as remembering them.
    fn publication_id(counters: &CounterManager, regions: &CounterRegions<'_>) -> i32 {
        let mut found = -1;
        regions.reader().for_each(|descriptor| {
            if descriptor.label == "snd-pos" {
                found = descriptor.counter_id;
            }
        });
        let _ = counters;

        found
    }
}
