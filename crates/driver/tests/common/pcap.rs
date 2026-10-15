//! Captures a `net_pcap` port reads and writes, and the child process a case
//! that wants an EAL has to run in.
//!
//! `net_pcap` is the PMD this build's DPDK cases run on: it reads a capture
//! this side writes and writes one this side reads, so a frame's whole journey
//! through a port is judged by bytes on disk — with real descriptors and real
//! mbufs, and with **no privileges**: no veth pair, no `CAP_NET_RAW`, no huge
//! pages (D3's plan, P-D3-2).
//!
//! Every case that wants an EAL runs in a child process of its own binary,
//! because `rte_eal_init` is once per process and `rte_eal_cleanup` does not
//! undo it (D0). The child is re-entered with `--ignored --exact <helper>`, and
//! a filtered test binary exits 0 with nothing run — which is why the parent
//! insists on seeing its child report exactly one passing test.

// Cargo compiles this file into every test binary that asks for it, and each of
// them uses a different part of it: a file that only receives never reads a
// `tx_pcap` back. So "unused here" is not "unused", and the warning that says
// otherwise is one every such file would have to carry.
#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::process::Command;

/// Run one case in a child process of this same test binary.
pub fn in_a_child(helper: &str, env: &[(&str, &str)]) {
    let exe = std::env::current_exe().expect("the test binary's own path");
    let mut command = Command::new(&exe);
    command.args([
        "--exact",
        helper,
        "--ignored",
        "--nocapture",
        "--test-threads=1",
    ]);
    for (key, value) in env {
        command.env(key, value);
    }

    let output = command
        .output()
        .unwrap_or_else(|error| panic!("running {}: {error}", exe.display()));
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "the child case `{helper}` failed ({}):\n{stdout}\n{stderr}",
        output.status
    );
    assert!(
        stdout.contains("1 passed"),
        "the child case `{helper}` did not run. A filtered test binary exits 0 \
         with nothing run, so this is the one way a criterion could pass by not \
         being looked at:\n{stdout}\n{stderr}"
    );
}

/// Write frames as a libpcap capture: the 24-byte file header, then one record
/// header and the frame per packet. `net_pcap` reads exactly this shape and its
/// `tx_pcap` writes it back.
pub fn write_capture(path: &Path, frames: &[Vec<u8>]) {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&0xa1b2_c3d4u32.to_le_bytes()); // magic
    bytes.extend_from_slice(&2u16.to_le_bytes()); // major
    bytes.extend_from_slice(&4u16.to_le_bytes()); // minor
    bytes.extend_from_slice(&0u32.to_le_bytes()); // thiszone
    bytes.extend_from_slice(&0u32.to_le_bytes()); // sigfigs
    bytes.extend_from_slice(&65535u32.to_le_bytes()); // snaplen
    bytes.extend_from_slice(&1u32.to_le_bytes()); // link type: Ethernet

    for frame in frames {
        bytes.extend_from_slice(&0u32.to_le_bytes()); // ts_sec
        bytes.extend_from_slice(&0u32.to_le_bytes()); // ts_usec
        bytes.extend_from_slice(&(frame.len() as u32).to_le_bytes()); // incl_len
        bytes.extend_from_slice(&(frame.len() as u32).to_le_bytes()); // orig_len
        bytes.extend_from_slice(frame);
    }

    std::fs::write(path, bytes).expect("writing a capture");
}

/// Read a capture back: the frames in it, in order.
pub fn read_capture(path: &Path) -> Vec<Vec<u8>> {
    let bytes = std::fs::read(path).unwrap_or_else(|error| {
        panic!("reading {}: {error}", path.display());
    });
    assert!(bytes.len() >= 24, "a capture has a file header");

    let mut frames = Vec::new();
    let mut cursor = 24; // the file header
    while cursor + 16 <= bytes.len() {
        let lengths = &bytes[cursor..cursor + 16];
        let incl_len = u32::from_le_bytes(lengths[8..12].try_into().expect("four bytes"));
        cursor += 16;
        let end = cursor + incl_len as usize;
        assert!(end <= bytes.len(), "a record's frame is in the file");
        frames.push(bytes[cursor..end].to_vec());
        cursor = end;
    }

    frames
}

/// A file of a test's own, removed when the test that made it returns.
pub struct Capture(PathBuf);

impl Capture {
    /// A capture holding `frames`, for a `net_pcap` port to read.
    pub fn of(name: &str, frames: &[Vec<u8>]) -> Self {
        let path = Self::path_for(name);
        write_capture(&path, frames);
        Self(path)
    }

    /// A path with nothing at it: what a `tx_pcap` port is told to write to.
    pub fn for_writing(name: &str) -> Self {
        let path = Self::path_for(name);
        let _ = std::fs::remove_file(&path);
        Self(path)
    }

    fn path_for(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "deepmsg-dpdk-{}-{}-{name}.pcap",
            std::process::id(),
            std::env::current_exe()
                .ok()
                .and_then(|exe| exe
                    .file_stem()
                    .map(|stem| stem.to_string_lossy().into_owned()))
                .unwrap_or_else(|| "test".to_owned())
        ))
    }

    pub fn path(&self) -> &str {
        self.0.to_str().expect("a UTF-8 path")
    }
}

impl Drop for Capture {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}
