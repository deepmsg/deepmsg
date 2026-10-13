# ADR-0006: Kernel-bypass media (DPDK)

- Status: accepted (2026-10-14)

## Context

The media layer is the one part of this system defined by the operating
system's interface rather than by Aeron's. A driver that sends and receives
through `sendmmsg` and `recvmmsg` pays for the kernel's socket layer on every
datagram, and its ceiling is the kernel's, not the NIC's. `README.md`
principle 2 already anticipates the alternative: FFI is not a red line, "a
kernel-bypass transport would be FFI throughout", and each use names what it
buys. This is that argument made once, for the media layer.

The reference offers a seam and a set of counter ids to build on, and nothing
else:

- `aeron_udp_channel_transport_bindings_t`
  (`media/aeron_udp_channel_transport_bindings.h:113-134`) — twelve function
  slots plus meta info, written so that the socket implementation can be
  replaced; `init_func` and `poller_init_func` take an `affinity` (`:26-32`)
  the socket implementation ignores, and which only a media that knows each
  thread owns its queues can use;
- `AERON_COUNTER_DPDK_*`, type ids 75–95
  (`aeron-client/src/main/c/aeron_counters.h:155-173`) — ids with no labels and
  no semantics attached, and 80/81 left unused.

The implementation itself is Aeron Premium's and is closed. The open tree's
only prose is a marketing line (`README.md:37`), and no file under
`aeron-driver/src/main/c/media/` mentions DPDK. So this is **not** a
line-by-line port with an oracle to diff against: it is an implementation on a
seam the reference published, and everything the reference kept to itself —
parameter names, counter labels, ARP policy — is this repo's own choice,
recorded as such in `docs/compat.md`.

Three existing decisions bear on it.

**ADR-0002** holds `unsafe` to five named zones and says new `unsafe` needs an
amendment; this ADR is that amendment, and a C library reached through FFI
throughout fits none of the five.
Zone 4 is the *kernel's* interface to this process — syscalls, signals, and the
media syscalls the kernel path sends and receives through — while this is a
*library's* interface: its own headers, its own build step, its own version,
its own initialisation lifetime.

**`docs/compat.md`** refuses settings that name code to load, because this
build has no `dlopen` and no `dlsym` — and it also records the case that is
*not* a refusal: the UDP channel interceptor names, which the reference
resolves against a table it compiles in, with no `dlopen` fallback
(`media/aeron_udp_channel_transport_bindings.c:71-93`). The media setting sits
between those two cases and lands on the interceptor side of the line. The
reference scans a table it compiles in (`:59-69`) and, failing that, calls
`aeron_dlsym(RTLD_DEFAULT, name)`
(`aeron-client/src/main/c/util/aeron_symbol_table.c:58-92`) — the hook a
Premium deployment injects its binding through. This build keeps the scan and
drops the fallback: `dpdk` is a second entry in this build's own table,
compiled in with the crate, and a name in neither table stops the driver, the
same outcome the reference has when nothing answers to the name. The refusal
rule's subject is *loading code into this process*; a binding this binary was
compiled with is not that.

**The kernel path is not going away.** A DPDK port serves the addresses bound
to it; `localhost`, the other NICs and the conductor's name resolver keep the
kernel implementation. The interop suite and the reference's system tests run
against a DPDK build at all only because that fallback exists.

## Decision

1. **`dpdk` is a second media binding, compiled in and off by default.** The
   setting keeps the reference's own name,
   `AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_MEDIA` (`aeronmd.h:731`), and its
   default stays `default` — the kernel's sockets. `dpdk` is served only by a
   binary built with the `dpdk` cargo feature; a build without it stops the
   driver with a reason that says so, as any other name it does not serve does.
   Nothing is resolved at run time that was not compiled in.

2. **The sixth `unsafe` zone is `deepmsg-dpdk`** (`crates/dpdk`): the C shim
   that turns DPDK's `static inline` data path into real functions, the
   hand-written `extern "C"` declarations of the shim's stable signatures, and
   the safe wrappers over them. It is a zone of its own rather than an
   extension of zone 4 because ADR-0002's axis is *whose boundary it is*, and
   this boundary is a library's, not the kernel's.

3. **The zone's edge is the crate.** The driver's DPDK media is safe code over
   `deepmsg-dpdk`'s API and stays under `forbid(unsafe_code)`; no other crate
   sees an `rte_mbuf` pointer or a queue's raw state, and the shim is shaped so
   that no DPDK structure's layout reaches Rust at all. If wiring the media
   turns out to need raw access in the driver, that is a further amendment —
   not a quiet widening.

4. **Where Rust can do it, Rust does it.** What the FFI buys is the NIC
   boundary: EAL, port and queue setup, mbuf and pool management, and the two
   burst calls. Ethernet and IPv4/UDP framing, checksums, ARP, the port and
   neighbour tables and demultiplexing are safe Rust in the driver, tested
   against an in-memory `PacketIo` with no DPDK and no NIC. Principle 2's
   "never a substitute for a Rust implementation that would do" is honoured by
   how thin the unsafe side is, not by how it is spelled.

5. **Shared library, installed headers, one DPDK per binary.** The crate links
   the system DPDK through `pkg-config` and compiles the shim against the
   installed headers; a static link would drop the PMDs that register through
   constructors in plugin objects. 23.11 LTS is the baseline, and the version
   is whatever the build machine has — the shim's own signatures are what keeps
   a `rte_mbuf` layout change from reaching the Rust side.

6. **Compatibility in a DPDK build is the same where it can be, and says so
   where it cannot.** The wire bytes, the CnC file, the counter *type ids* and
   the client protocol are unchanged, and the existing suites cover them. The
   configuration parameters under the media setting, the counter labels and the
   ARP policy are this repo's own, because the reference's are not visible; v1
   refuses multicast channels outright, so the compatibility promise for that
   build is the non-multicast subset of the reference's behaviour. Each of
   these is a `docs/compat.md` row **with the test that pins it**, added by the
   slice that owns the test and never ahead of it.

7. **Licensing and dependency policy.** DPDK is BSD-3-Clause, compatible with
   this repo's Apache-2.0, and it is a system library rather than a crate:
   `deny.toml` never sees it, and the dependency is declared in one crate's
   build script. It is not vendored, and does not become a vendored dependency.

## Consequences

- The `unsafe` review surface is six named modules, and the new one is small on
  purpose: a shim of some twenty-five functions, batched so that the hot path
  crosses the language boundary a constant number of times per pass rather than
  once per frame, and a hand-written declaration file beside it. A reviewer
  reads it in one sitting, which is the whole point of a named zone.
- The default build, the default CI and the interop suite never touch DPDK. The
  feature is off, one crate's dependency is optional, and the `dpdk` job is
  additive: a machine without `libdpdk-dev` cannot fail a default build.
- One EAL initialisation per process, and cleanup at the end of that process.
  The crate holds that in a type rather than in a comment, and a test that
  needs a media driver spawns one.
- A DPDK build is not the reference's DPDK build, and cannot be diffed against
  it. The counter ids line up; the labels do not, and no test can make them.
  The measurement that decides whether this was worth doing is this build
  against this build, on the same port, over the same kernel.
- A DPDK build is a driver with one more media, not a replacement for the
  kernel's, so this decision is withdrawable: the feature, the crate and the
  zone can go together, and nothing outside that feature depends on them.
