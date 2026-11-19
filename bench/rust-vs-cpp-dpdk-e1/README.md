# E1 — the Rust media against the C++ media, one cable

Two arms, both driven by **this tree's** `loadtest-rig` and `echo-node`; the
only thing that differs is which driver binary owns the two ports:

| arm | driver on both ports | media |
|---|---|---|
| **R** | `target/release/deepmsg-driver` (`--features dpdk`) | `crates/driver/src/media/userspace` |
| **C** | `deepmsg-cpp`'s `deepmsg-media-driver` (`AXC_WITH_DPDK=ON`) | `src/core/media/dpdk` (the Rust track transcribed) |

Two drivers per cell, one per side: a media takes one port per driver, and the
two ends of a cable cannot share one. Both ports are bound to `vfio-pci`; the
clients are CnC clients of their side's driver, so there is no namespace and no
kernel interface — the cable is the only path between the two ends.

## What each script is

    p0-smoke.sh     does this tree's client drive the C++ driver at all?
                    One C++ driver on the kernel path, one aeron.dir, both
                    clients on loopback. Verdict: no `WARNING:`, one `.hdr`,
                    the count identity, and the counters readable by
                    `cnc-dump` (the two CnC readers agree).

    run-batch.sh    the A/B. `--list` prints the plan and the prerequisites;
                    `--quick` runs one cell per arm; the default runs the D6
                    grid (25K:1344 / 101K:1344 / 301K:288 / 501K:32) in four
                    rounds, **R C C R**, so each arm has two and its floor is
                    itself. 60 s + 30 s warmup per cell. `--out DIR` moves the
                    results; `E1_PERF=1` attaches `perf stat`/`perf record` to
                    the two driver processes (read them with perf-report.sh).

    perf-report.sh  cycles per message and the top symbols, per cell, from an
                    `E1_PERF=1` run.

    analyse.sh      the table. Each arm's two rounds are averaged; the ratio is
                    read only when it is outside both arms' floors (each arm's
                    two rounds against each other).

## What is held equal

Same machine, same two ports of one X540 and the same cable, the same six of
eight cores on both arms, `SHARED` threading and `spin` idle on every agent,
the same MTU and initial window, and the same clients (this tree's, both arms).
**Both drivers are Release**: the C++ `dpdk` preset is Debug, and the preflight
refuses a C arm whose `file` says `with debug_info` — the first E1 session
measured the build type and said nothing about the media.
The media's environment is the same for both drivers — `AERON_UDP_CHANNEL_
TRANSPORT_BINDINGS_MEDIA=dpdk` plus `DEEPMSG_DPDK_PORT/IPV4_ADDRESS/EAL_ARGS`;
both drivers read `AERON_` and `DEEPMSG_` (`apps/deepmsg-media-driver/main.cpp`).

The link on a port DPDK has just started takes about twenty seconds; each cell
waits 25 s after the drivers answer, and the wait is not in the window.

## What a cell must show to count

- `dpdk media on …` in **both** drivers' logs — a cell that fell back to the
  kernel is a failed cell, not a fast one;
- the rig's own identity: `rate × iterations` messages, no `WARNING:`, one
  `.hdr` and no `.FAIL`;
- the media's drop counters (77 nobufs + 84 missed + 88 queue drop) at zero,
  read with `cnc-dump` from both drivers' CnC files.

## What the numbers are not

The arms differ in the driver as well as the media: arm R runs this tree's
driver, arm C the reference C driver with its bindings. So a ratio here is
**(this driver + this media) against (that driver + that media)** — the client
and the node are constant, so the client is not in it, but the driver is. A
statement about the media alone needs the media driven without a driver
(planned as E2 in `analysis/dpdk/plan-2026-11-18-rust-vs-cpp-dpdk-media-bench.md`).

Nothing here is comparable with a cell from another session: the reading rules
are the bench batches' own (`bench/README.md`), and the floor above is the only
noise measurement that applies.
