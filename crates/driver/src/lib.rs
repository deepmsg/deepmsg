//! The deepmsg media driver: the process that owns the CnC file, the term
//! buffers and the UDP data plane.
//!
//! Thread model (P1), mirroring the reference C driver (M08):
//!
//! - a conductor thread: command drain and duty cycle — the
//!   single-threaded control-plane core (M09),
//! - a sender and a receiver thread for the data plane (M10 / M11),
//! - name-resolution offload and asynchronous resource reclamation.
//!
//! How many threads that is, and which piece is on which, is
//! `aeron.threading.mode`'s to say — see [`driver`], which is the reference's
//! `aeron_driver_t` and the runner table it builds from the setting.
//!
//! Unsafe is denied at crate root, and exactly one module carries the
//! documented allow ADR-0002 zone 4 provides for: [`sys`], which holds the
//! kernel seam — signals, the socket probe a log buffer's metadata is written
//! from, randomness — and grows into the media syscall shim (`sendmmsg`,
//! `recvmmsg`) when the data plane arrives.

#![deny(unsafe_code)]

pub mod channel_uri;
pub mod channel_validation;
pub mod clients;
pub mod conductor;
pub mod config;
pub mod congestion_control;
pub mod cpuset;
pub mod dir;
pub mod driver;
pub mod flowcontrol;
pub mod idle;
pub mod ipc_publication;
pub mod ipc_publications;
pub mod ipc_subscriptions;
pub mod loss;
pub mod loss_detector;
pub mod media;
pub mod name_resolver;
pub mod native_resource_agent;
pub mod network_publication;
pub mod network_publications;
pub mod port_manager;
pub mod position;
pub mod protocol;
pub mod publication_image;
pub mod publication_images;
pub mod publication_params;
pub mod receive_endpoints;
pub mod receiver;
pub mod retransmit_handler;
pub mod send_endpoints;
pub mod sender;
pub mod stage_timing;
pub mod subscribable;
/// The kernel seam: the one `unsafe` in this crate, and the only place the
/// allow appears. ADR-0002 zone 4.
#[allow(unsafe_code)]
pub mod sys;
pub mod system_counters;
pub mod topology;
pub mod udp_channel;
