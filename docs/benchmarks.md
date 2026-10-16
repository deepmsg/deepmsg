# The numbers this build runs at

A snapshot, not a contract: `docs/compat.md` holds the contracts, and every row
there names a test. What follows names a machine and a date, and the point of it
is to be *re-runnable* — each table carries the command that produced it, and a
number without the fingerprint below is not a baseline, it is an anecdote.

**Measured** 2026-09-30 on:

- Intel(R) Core(TM) i7-10875H (8 cores / 16 threads), 2.30 GHz base
- Linux 6.6.87.2-microsoft-standard-WSL2 (the whole run is inside WSL2)
- rustc 1.95.0 (59807616e 2026-04-14), `cargo bench` (release)
- driver `target/release/deepmsg-driver`, built from this tree
- reference: `aeronmd` and the C++ samples from the Aeron 1.53.2 checkout
  (`../aeron/cppbuild/Release/binaries`, built 2026-09-25)

Nothing here runs in CI. A number from a shared machine is not a gate, so what
CI does with the harness is compile and lint it.

## Round-trip latency (µs)

Two processes and a driver in between: one publishes a message stamped with
`monotonic_nano_time()`, the other echoes it back unchanged, and the caller
times the whole loop. That is what the reference's own `Ping`/`Pong` measure,
and they are one of the three rows — so a row produced by our client sits in a
table with rows produced by code that shares nothing with it.

| client | driver | channel | length | p50 | p90 | p99 | p99.9 | max | n |
|---|---|---|---|---|---|---|---|---|---|
| ours | ours | `aeron:ipc` | 32 B | 0.9 | 1.2 | 9.6 | 38.0 | 166.9 | 50000 |
| ours | ours | `aeron:ipc` | 1024 B | 1.8 | 11.7 | 19.4 | 44.0 | 89.0 | 50000 |
| ours | ours | `aeron:udp` | 32 B | 11.7 | 13.7 | 38.1 | 55.5 | 87.4 | 50000 |
| ours | ours | `aeron:udp` | 1024 B | 17.4 | 42.9 | 104.6 | 231.8 | 321.3 | 50000 |
| reference `Ping`/`Pong` | ours | `aeron:ipc` | 32 B | 0.3 | 0.4 | 1.0 | 1.2 | 61.0 | 50000 |
| reference `Ping`/`Pong` | ours | `aeron:ipc` | 1024 B | 0.7 | 1.4 | 1.7 | 27.1 | 179.6 | 50000 |
| reference `Ping`/`Pong` | ours | `aeron:udp` | 32 B | 12.1 | 13.0 | 38.8 | 51.7 | 225.2 | 50000 |
| reference `Ping`/`Pong` | ours | `aeron:udp` | 1024 B | 14.2 | 22.1 | 46.5 | 65.1 | 133.9 | 50000 |
| reference `Ping`/`Pong` | reference | `aeron:ipc` | 32 B | 0.3 | 0.3 | 1.0 | 1.2 | 54.5 | 50000 |
| reference `Ping`/`Pong` | reference | `aeron:ipc` | 1024 B | 0.7 | 1.4 | 1.6 | 24.4 | 100.9 | 50000 |
| reference `Ping`/`Pong` | reference | `aeron:udp` | 32 B | 9.2 | 13.5 | 37.4 | 82.9 | 2285.6 | 50000 |
| reference `Ping`/`Pong` | reference | `aeron:udp` | 1024 B | 18.9 | 27.1 | 54.0 | 73.9 | 457.7 | 50000 |

    cargo build --release -p deepmsg-driver
    cargo bench -p deepmsg-bench --bench latency -- --messages 50000 --warmup 5000 --md
    cargo bench -p deepmsg-bench --bench latency -- --peer reference --md
    cargo bench -p deepmsg-bench --bench latency -- --peer reference --driver reference --md

### The reference's Java pair

The same instrument written in the other language the reference ships: its Java
`Ping` and `Pong` against its Java media driver (`io.aeron.driver.MediaDriver`).
Both ends are the reference's, and neither is a combination with anything of
ours — the Java client has never been run against this build's driver, and that
is a compatibility question rather than a performance one.

| client | driver | channel | length | p50 | p90 | p99 | p99.9 | max | n |
|---|---|---|---|---|---|---|---|---|---|
| reference `Ping`/`Pong` (Java) | reference (Java) | `aeron:ipc` | 32 B | 0.5 | 0.7 | 4.4 | 33.8 | 1253.4 | 50000 |
| reference `Ping`/`Pong` (Java) | reference (Java) | `aeron:udp` | 32 B | 9.0 | 13.7 | 177.9 | 414.5 | 1488.9 | 50000 |
| reference `Ping`/`Pong` (Java) | reference (Java) | `aeron:udp` | 1024 B | 13.5 | 47.3 | 319.2 | 433.4 | 1959.9 | 50000 |

    cargo bench -p deepmsg-bench --bench latency -- \
        --peer reference-java --driver reference-java --messages 50000 --warmup 5000 --md

Three things to read it with:

- **The medians are at parity with the C++ pair** on the same scenarios (0.5
  against 0.3 µs on IPC; 9.0 against 9.2 and 13.5 against 18.9 on UDP), which is
  what a mature implementation of the same protocol on a loopback socket looks
  like. The **tails are not**: p99 is 178 µs against 37 µs on UDP at 32 bytes,
  and the maximum is milliseconds rather than hundreds of microseconds. That is
  the JVM (safepoints, GC, JIT) showing up in the percentiles the C++ rows do
  not have it in.
- **The warm-up is 5,000 messages, not the sample's own default** — that is ten
  iterations of 10,000, so 100,000. The counts are kept equal to the other rows
  on purpose, which means these rows carry more JIT compilation inside the
  measurement than the Java sample would run with on its own. The medians are
  past the worst of it; the tails are where the difference would show.
- `aeron.sample.exclusive.publications=true` is set explicitly: the C++ `Ping`
  publishes through an exclusive publication and the Java sample's own default
  is a plain one, so setting it leaves the language as the only difference
  between those two rows. `--add-opens java.base/jdk.internal.misc=ALL-UNNAMED`
  is the other setting both ends need, and the reference's own Gradle passes it
  to every task it runs (`build.gradle:225-226`).

## Throughput (one direction)

One publisher, one counting process, and no reply: what arrived, and what it
took to publish it. `attempts/msg` is the number of `offer` calls per delivered
message — 1.00 is a publisher that was never once blocked, and a larger number
is a publisher waiting on the window, which is the difference between "the
system is fast" and "the consumer is keeping up".

| client | driver | channel | length | messages/s | MB/s | attempts/msg | n |
|---|---|---|---|---|---|---|---|
| ours | ours | `aeron:ipc` | 32 B | 2,821,662 | 86 | 1.00 | 200000 |
| ours | ours | `aeron:ipc` | 1024 B | 850,632 | 831 | 1.00 | 200000 |
| ours | ours | `aeron:udp` | 32 B | 2,155,244 | 66 | 1.00 | 200000 |
| ours | ours | `aeron:udp` | 1024 B | 250,710 | 245 | 12.99 | 200000 |

    cargo bench -p deepmsg-bench --bench latency -- --mode throughput --messages 200000 --md

The reference's own throughput samples are not in this table: they publish from
a dedicated thread inside one process (`Throughput.cpp`), and this build's
client is one thread with a `&mut Client`, so the shapes are not the same one.
Comparing them would compare the thread models.

## Micro-benchmarks (criterion)

In this process, no driver, no socket. What these are for is the other
direction: a change that makes one of them twice as slow is visible here and
nowhere else, because at the harness's scale it is noise.

| benchmark | time |
|---|---|
| `logbuffer_append/32` | 47.3 ns |
| `logbuffer_append/1024` | 73.6 ns |
| `term_scan/one_datagram` | 146.8 ns |
| `term_scan/whole_term` | 8.13 µs |
| `buffer/load_i64` | 444 ps |
| `buffer/store_i64` | 3.12 ns |
| `buffer/compare_exchange_i64` | 13.41 ns |
| `buffer/copy_in_32` | 2.95 ns |
| `buffer/copy_in_1k` | 22.95 ns |
| `buffer/copy_out_1k` | 22.10 ns |

    cargo bench -p deepmsg-bench --bench micro

## What differs from the reference's own numbers

- **IPC is ~3× the reference's** (0.9 µs against 0.3 µs at 32 bytes), and the
  difference is the clients, not the driver: rows 5-6 and 9-10 are the same
  number on our driver and on the reference's, and the reference's `Ping` uses
  an *exclusive publication* — a client-side promise that one thread publishes
  — whose append path is `logbuffer_append` and nothing else. Our client's
  `offer` is below that in the same table (47 ns) and its poll path is a
  `poll_subscription` over an assembler; the round trip pays both twice.
- **UDP is at parity** (11.7 µs against 12.1 at 32 bytes on our driver), which
  is where the kernel, not the client, sets the floor.
- **`aeron:ipc` still crosses a process boundary**: a client and a driver, with
  a shared log buffer between them. It is not a function call, and the numbers
  above are not "the cost of a queue".
- **Throughput at 1024 bytes over UDP is back-pressured** (12.99 attempts per
  message): the consumer cannot keep up with the producer, and the window says
  so. The 250,710/s is the rate the consumer sustained, not the rate the
  publisher could reach.

## One number worth its own paragraph

The first version of this harness measured 8.8 µs at `aeron:ipc`, 32 bytes —
31× the reference's 0.3 µs — and the cause was in `Client::poll`, not in the
driver: its liveness check asked "is the heartbeat counter I cached still mine?"
with `CountersReader::get`, which is `for_each` with a filter and built a
descriptor for **every counter in the file** to answer a question about one id.
It cost 6.6 µs per poll, on the loop every client runs. The reference asks the
same question of the slot the id names, in constant time
(`aeron_client_conductor.c:1234-1249`), and so does this build now.

Removing it took the round trip from 8.8 µs to 0.9 µs and throughput from
116,544/s to 2,758,084/s — with `attempts/msg` at 1.00 before *and* after, which
is what says the publisher was never the limit: the consumer's poll loop was,
and the poll loop was mostly this.

## Against the reference's own benchmark harness

The tables above come from this tree's own harness and from the reference's
`Ping`/`Pong` samples: a process pair, one message in flight, no rate control.
`bench/` measures something else — two batches that drive the reference's
benchmark repository (`aeron-io/benchmarks`) instead, where the load is a stated
rate for a stated number of iterations, the far end is the harness's echo node,
and both sides write the same interval-log files.

| batch | what changes between the rows | what a reading can say |
|---|---|---|
| `bench/upstream-c-drivers` | the media driver, three ways: Java, a C control built here, a C build of the changes sent upstream in [aeron-io/aeron#2188](https://github.com/aeron-io/aeron/pull/2188) | which layer, because exactly one layer moves |
| `bench/rust-vs-java` | nothing inside either side — this build's whole stack against the reference's | only how far apart the two systems are, never which layer |

    bench/build.sh
    bench/upstream-c-drivers/build-c-driver.sh c-control upstream/master
    bench/upstream-c-drivers/build-c-driver.sh c-patched driver/c-driver-receive-path
    bench/upstream-c-drivers/patches/install.sh --apply
    bench/upstream-c-drivers/run-batch.sh                  # about 80 minutes
    bench/rust-vs-java/run-batch.sh                        # about 40 minutes

Both write one directory of histograms per cell under `bench/out/` (ignored by
git) with a `fingerprint.txt` and a `RUN-INFO.txt` beside them, and both read
them with `bench/tools/quantiles.sh` — the harness's own HdrHistogram decoder,
which reads this build's files because `crates/bench/src/loadtest/result.rs`
writes the format the harness's `PersistedHistogram` writes. Each batch's
`read.py` prints the pair round by round, with the floor and with whether the
sign survived the reversal.

Every rule the tables above are read by applies here too, and two more: a ratio
is read only when both rounds agree in sign, and a difference inside the floor
(each row against itself across the two rounds) is not a difference. Both
batches have been run, and the second one's table is at the end of this
section.

### The C driver's receive path, against the unpatched driver and the Java one

    bench/build.sh
    bench/upstream-c-drivers/build-c-driver.sh c-control upstream/master
    bench/upstream-c-drivers/build-c-driver.sh c-patched driver/c-driver-receive-path
    bench/upstream-c-drivers/patches/install.sh --apply
    bench/upstream-c-drivers/run-batch.sh

`E[X | X < p99]`, nanoseconds, `round 1 / round 2`; three runs of 60 s per cell
(30 s warmup at 25K), the second round with the drivers in reverse order. The
first three rows are what each driver measured; the last three are the ratios
between them, so that the sizes can be read against each other:

| driver | 101K:1344 | 301K:288 | 501K:32 |
|---|---|---|---|
| `java` — the harness's own Java driver | 6690 / 6739 | 8170 / 8134 | 8369 / 8374 |
| `c`, unpatched — built here | 7124 / 7216 | 8377 / 8324 | 8774 / 8779 |
| `c`, patched — the three changes | **6442 / 6440** | **7850 / 7902** | **8112 / 8103** |
| ratio, **patched / unpatched** | 0.904 / 0.892 | 0.937 / 0.949 | 0.925 / 0.923 |
| ratio, **patched / java** | 0.963 / 0.956 | 0.961 / 0.972 | 0.969 / 0.968 |
| ratio, unpatched / java | 1.065 / 1.071 | 1.025 / 1.023 | 1.048 / 1.048 |

All nine ratios agree in sign across the reversal, and the floor — each driver
against itself between the two rounds — is at most 1.3%, and under 0.7% in seven
of the nine cells. Every effect below is at least twice the floor it sits on.
Read on the ratios:

- before these changes, this driver's receive path was **2.3–7.1% slower than the
  Java driver's** at these tiers;
- with them it is **2.9–4.4% faster than the Java driver's**, in every tier and
  both rounds;
- against the unpatched driver built from the same source, by the same script,
  with the same compiler, the three commits are worth **5.1–10.8%**.

The unpatched driver is a build of the revision the changes apply to (`dd7c56c7`),
made by the same script with the same cmake and the same compiler as the patched
one (`da361c17`): the only difference between those two rows is the three
commits. It is deliberately **not** the C binaries the harness checks in, which
are a build of some other source at some other time — comparing against those
would compare against a compiler and an era as well.

**The fingerprint.**

- **Machine** Intel i7-9700K, 8 cores, no SMT, one socket, 31 GiB; Ubuntu 24.04.4;
  kernel 7.0.0-38-generic; governor `performance`, turbo enabled.
- **Toolchain** gcc 13.3.0; cmake 4.3.4 (the one the upstream work area's own
  `cppbuild` installs); OpenJDK 21.0.12.1; the harness built by its own Gradle
  build.
- **Harness** `aeron-io/benchmarks` at `1b11bfc` (its `master`), a clean
  checkout, with exactly four paths changed by this tree and nothing else:
  `scripts/remote-benchmarks-runner` and
  `scripts/aeron/remote-echo-single-host-benchmarks`, which carry the two
  patches, and the two launchers `scripts/aeron/c-media-driver-c-control` and
  `…-c-patched`. Its Java side is the harness's own dependency, Aeron 1.53.3.
- **Drivers** `c-control` from `dd7c56c7` (`upstream/master`,
  `libaeron_driver.so` `6059b663…`), `c-patched` from `da361c17`
  (`driver/c-driver-receive-path`, `a7d70356…`); both were rebuilt from those
  revisions afterwards and came out bit-identical, which is what says the builds
  that ran are the builds those revisions produce. The Java one is the harness's
  own driver.
- **Grid and placement** 501K↔32, 301K↔288, 101K↔1344 at mtu 1408; the driver's
  conductor, sender and receiver on cores 1, 2 and 3, the echo node on 4, the
  client on 5, cores 6 and 7 for the JVMs' own threads, core 0 left to the kernel
  and the network softirq; UDP on 127.0.0.1:13000 and :13100.
- **Driver settings** the harness writes these per run, and all three are
  handed the same file: `max.messages.per.send=2`,
  `receiver.io.vector.capacity=2`, `sender.io.vector.capacity=2`. Those are the
  harness's own single-host defaults, and being identical across them is the
  only thing that matters here — this batch compares drivers, not
  configurations. The other batch, the two whole stacks, is the one that has to
  pin those three knobs to what this build hard-codes.

The histograms, the fingerprint file and the run's own record are in
`bench/out/upstream-c-drivers/<date>-<host>/` (ignored by git).

### This build's whole stack, against the reference's

    bench/build.sh
    bench/rust-vs-java/run-batch.sh

Two arms, neither of them a swap inside itself. The Java arm is the harness's
own `media-driver`, its `echo-client` (the `LoadTestRig` over
`EchoMessageTransceiver`) and its `echo-server` (the reference's `EchoNode`).
The Rust arm is this tree's `deepmsg-driver`, its `loadtest-rig` and its
`echo-node`, built by `bench/build.sh` at the profile they are measured under.

`E[X | X < p99]`, nanoseconds, `round 1 / round 2`; one run of 60 s per cell
(30 s warmup at 25K), the second round with the two stacks in reversed order:

| tier | `java` | `rust` | `rust / java` | verdict |
|---|---|---|---|---|
| 25K:32 | 5999 / 5982 | 5895 / 5794 | 0.983 / 0.969 | same sign |
| 101K:1344 | 6738 / 6835 | 6812 / 6747 | 1.011 / 0.987 | **sign flips** |
| 301K:288 | 8186 / 8167 | 8032 / 8021 | 0.981 / 0.982 | same sign |
| 501K:32 | 8398 / 8418 | 8364 / 8371 | 0.996 / 0.995 | same sign |

The floor — each stack against itself between the two rounds — is 1.7% for the
Rust arm at 25K, 1.4% and 0.95% for the Java and Rust arms at 101K, and under
0.3% in the five other cells. Read each cell against its own floor, and then
against the worst floor in the table, which is 1.7%:

- **301K:288** is 1.8% faster in both rounds, against a cell floor of 0.23% or
  less. This is the one tier where the difference is larger than any floor in
  the table.
- **501K:32** is 0.4% and 0.6%, the same sign, against a cell floor of 0.23%:
  outside its own cell's floor, inside the table's worst.
- **25K:32** agrees in sign (1.7% and 3.2%) against the Rust arm's own 1.7% drift
  at that tier — one to two times its own floor.
- **101K:1344** flips sign between the two rounds, so nothing is read there at
  all.

So this batch reads a difference of about 1.8% at one tier, two tiers small
enough to be argued either way, and nothing at the fourth — which is what a
whole-stack comparison at this resolution is able to say, and no more.

**The two sides are not the same Aeron version, and this batch does not separate
that from the implementations.** This build targets byte-level compatibility
with Aeron 1.53.2 (`docs/adr/0001`); the Java arm is the harness's own
dependency, Aeron 1.53.3. Nothing breaks — the two arms never talk to each
other, each being a driver, a client and a node from one side — but the ratio
carries a version difference that was not measured on its own, and it should be
read as the one known unknown in the table.

**The fingerprint.** The machine, toolchain, core placement and driver settings
are the ones in the section above, with one difference: this batch is the one
that has to pin the three throughput knobs, because it compares whole stacks
rather than one layer, so both sides are handed
`bench/lib/conf/benchmark.properties` (the aligned values) on top of the
harness's own. The harness is the same checkout at `1b11bfc` with the same four
paths changed. This build's three binaries, by digest:

| binary | digest |
|---|---|
| `deepmsg-driver` | `d0cfdb4a…` |
| `loadtest-rig` | `adc4b5e9…` |
| `echo-node` | `577ff8fc…` |

They are this tree at `b7d8d1f`, which is `main` plus this section's own commits
— a different node and rig from any earlier session's, so this table may not be
read against one of those even by direction alone. The histograms and the
record are in `bench/out/rust-vs-java/<date>-<host>/` (ignored by git).

### The media, against the kernel path

    analysis/dpdk/d6-abba/run.sh
    analysis/dpdk/d6-abba/analyse.sh

**This is the one table here whose two arms are both this build.** It asks what
the media is worth, not how this build compares with the reference: the same
three binaries — `deepmsg-driver`, `loadtest-rig`, `echo-node` — on both arms,
one cable between two ports of one X540, one address pair (`10.10.0.1` ↔
`10.10.0.2`), four rounds in ABBA order (K M M K) so that each arm has two and
its floor is itself. **The only thing that differs is which media owns a port**:
`ixgbe` on arm K, this build's DPDK media (`--features dpdk`, the port bound to
`vfio-pci`) on arm M. A difference here is the difference between a kernel UDP
path and this build's userspace one, and nothing else.

**Both ends of the cable are this machine, and each is put in a network
namespace of its own.** Without the split the kernel takes its local shortcut
and the cable carries nothing — measured on the batch's first cell as 58
packets on the wire against 1.5 million messages offered, the rig reporting
87.8% loss. Both drivers are started as root on both arms, because entering a
namespace needs `CAP_SYS_ADMIN` and a driver that ran as its owner on one arm
and as root on the other would put the user into the comparison. The rig, the
node and both drivers are given the same cores on both arms.

Round trip, on the wire, microseconds; one 60 s run per cell with a 30 s warmup
at 25K, each arm's two rounds averaged. There is no p99.9 column: the reader
these batches share does not emit one, and says why — "the gaps these batches
look for live in the body of the distribution" (`bench/tools/HdrQuantiles.java`).
`max` is given instead, and taken with the salt it deserves: the media arm's
worst single round trip in a cell is 308–953 µs at every tier, which is one
scheduling decision, not a path — while the kernel arm's is 64 ms at 501K.

| tier | n | K p50 | K p90 | K p99 | K max | M p50 | M p90 | M p99 | M max |
|---|---|---|---|---|---|---|---|---|---|
| 25K:1344 | 1,500,000 | 86.75 | 114.56 | 135.62 | 611.0 | 30.41 | 32.33 | 34.50 | 953.3 |
| 101K:1344 | 6,060,000 | 155.84 | 194.37 | 215.42 | 323.1 | 25.98 | 27.02 | 32.46 | 318.1 |
| 301K:288 | 18,060,000 | 216.83 | 234.05 | 244.41 | 423.3 | 21.14 | 21.55 | 27.35 | 308.1 |
| 501K:32 | 30,060,000 | 2875.39 | 34816.00 | 52379.65 | 64110.6 | 18.14 | 18.98 | 23.62 | 331.4 |

and the same cells as ratios, with each arm's floor read as its own two rounds
against each other (`analyse.sh` prints this table directly):

| tier | stat | M/K | floor | verdict |
|---|---|---|---|---|
| 25K | p50 | 0.3506 | 6.71% | M better |
| 25K | p99 | 0.2544 | 3.15% | M better |
| 25K | `E[X\|X<p99]` | 0.3564 | 7.75% | M better |
| 101K | p50 | 0.1667 | 1.07% | M better |
| 101K | p99 | 0.1507 | 0.71% | M better |
| 101K | `E[X\|X<p99]` | 0.1655 | 1.37% | M better |
| 301K | p50 | 0.0975 | 1.53% | M better |
| 301K | p99 | 0.1119 | 10.59% | M better |
| 301K | `E[X\|X<p99]` | 0.0974 | 0.21% | M better |
| 501K | p50 | 0.0063 | 15.95% | M better |
| 501K | p99 | 0.0005 | 4.57% | M better |
| 501K | `E[X\|X<p99]` | 0.0020 | 45.61% | M better |

**The shape is the finding.** The media arm's p50 *falls* with the offered rate
— 30.4 µs at 25K, 18.1 µs at 501K — while the kernel arm's rises from 86.8 to
2875 µs, and at 501K its p99 is 52.4 ms against the media arm's 23.6 µs. A
busier media driver sits closer to the steady state of a bounded queue; the
kernel path, once its socket buffers are full, is describing a queue that is
not draining.

**Read the 501K row for its stability, not its ratio.** The floor there is
15.95% on p50 and 45.61% on `E[X|X<p99]`, and it is the *kernel* arm that
provides it: its two rounds are 2646 and 3105 µs, against the media arm's 18.14
and 18.14. An arm at its limit does not have a number, it has a range, so the
ratio in that row is read as "one arm is at its limit and the other is not"
rather than as a multiple. The other three tiers have floors of 0.21%–7.75%,
and every ratio there is far outside them.

**Both arms carried the offered load**, which is what makes the ratios readable
at all: every cell reports the same `n` (the counts in the table above), the rig
complained about its target rate in no cell, and the two sides' port counters
agree to within a sampling tick. This section does **not** measure a throughput
ceiling — the rig offers a rate and measures the round trip, so what it can say
about throughput is that the media arm carries the offered load at 18 µs while
the kernel arm carries it at 3 ms. Neither tier is anywhere near the cable: at
501K:32 the offered load is well under a gigabit on a 10G link.

**What makes the arms comparable, checked rather than assumed** (this build has
been wrong about "configured for the media" more than once):

- **Both drivers of a media cell published the media's own counters** — 21 of
  them each, `dpdk port: 0000:03:00.0 …, driver=net_ixgbe` — and **neither
  driver of a kernel cell published any** (`dpdk` count 0). The arms are what
  they say they are. (These cells ran before this slice added 92, the extended
  statistics; a media driver publishes 22 more of them now, which the note on
  the counters' cost below accounts for.)
- **No media cell logged the idle warning** the driver prints when its thread
  has nothing to wait on. Both arms run `SHARED` with `aeron.shared.idle.strategy=spin`,
  so the one thread a driver has keeps reading the port — and the cells' own
  properties files are what make the other two settings agree: the warning is
  computed from the **sender's** and **receiver's** strategies whatever the
  threading mode is, and a `SHARED` driver whose shared strategy spins but whose
  two are the default `backoff` prints it (measured, on the open-loop arm
  afterwards, which passes no properties files at all).
- The cell's own deadline was never reached on either arm; every cell took 93 s
  (kernel) or 119 s (media, the extra 26 s being a fixed wait for the link to
  come up — a 10GBASE-T port DPDK has just started takes about twenty seconds,
  and nothing crosses before it does).

**The fingerprint.** Measured 2026-10-15 on the bench machine, which is **not**
the machine the tables at the top of this page were measured on: Intel(R)
Core(TM) i7-9700K (8 cores), Linux 7.0.0-38-generic, DPDK 23.11.4, rustc 1.95.0
(59807616e 2026-04-14) with LLVM 22.1.2, clang 18.1.3. Those two version
numbers are why this build does not attempt cross-language LTO: `-Clinker-plugin-lto`
needs one LLVM on both sides of the boundary, and four major versions separate
them. The three binaries, by digest:

| binary | digest |
|---|---|
| `deepmsg-driver` | `0b5a8dcd…` |
| `loadtest-rig` | `adc4b5e9…` |
| `echo-node` | `577ff8fc…` |

They are this tree at `207c11d`, the driver built with `--features dpdk` — the
plain `cargo build --release -p deepmsg-driver --features dpdk`, which is worth
writing down because **it reproduces `0b5a8dcd…` byte for byte** from that
commit (checked while building the pair for the measurement below). The batch,
its two rounds of histograms and every cell's `RUN-INFO.txt` — which records the
media environment variables the drivers actually received, the core placement
and the digests above — are in `analysis/dpdk/d6-abba/out-round3/`.
Both rounds of a second full grid agree with this one: the same ratios came out
at 0.3456 / 0.1640 / 0.0987 / 0.0058 on p50.

**The three drop counters, and what a zero is worth.** Every cell keeps what its
two drivers published, read out of the CnC before the next cell deletes it
(`keep_counters` in the batch; `analysis/dpdk/d6-abba/counters.sh` prints the
table). Each media cell's two drivers published 21 of the media's counters, and
in every one of them the three that would say a frame was lost stood at zero:

| tier | 82 received (rig / node) | 91 free frames (rig / node) | 77 | 84 | 88 |
|---|---|---|---|---|---|
| 25K | 2,249,114 / 2,251,598 | 6,227 / 6,218 | 0 | 0 | 0 |
| 101K | 6,800,477 / 6,810,838 | 6,227 / 6,217 | 0 | 0 | 0 |
| 301K | 18,676,322 / 18,709,780 | 6,224 / 6,184 | 0 | 0 | 0 |
| 501K | 30,524,477 / 30,566,364 | 6,197 / 6,208 | 0 | 0 | 0 |

The kernel arm's eight cells publish none of these at all — a `dpdk` count of 0
where the media arm has 21 — which is the same statement one level down: the
counters are the media's own. The two sides' port counts differ by about a tenth
of a second of traffic, which is the sampling period and not loss: those
counters are written by a timer, while the per-transport counter (type 76) is
written by the thread that reads the datagrams.

**A zero is worth something only if the counter can move**, so each of the three
was attacked on purpose before being quoted (one cell at 501K each;
`analysis/dpdk/d6-negative-arms.md` holds the predictions, written before the
runs, and the two that were wrong):

| arm | what it did | predicted | what happened |
|---|---|---|---|
| `mempool.size=2048` | a pool a quarter of the 8,192 frames the descriptors ask for | 77 > 0 | **77 = 262,624** (node side), and 91 fell from ~6,200 free frames to 38 |
| `rx.burst=1` | one frame per pass off the port | 84 > 0 | 84 = 0 — the arm carried the whole load (30.4M datagrams, no rate complaint) |
| `rx.burst=1024` | a pass that could take 1024 frames | 88 > 0 | 88 = 0 — a burst is what the port happens to hold, and it is drained every pass |
| a busy loop on the driver's core | the driver's own thread losing half its CPU | 84 > 0 | 84 = 88 = 0 — the rig still completed all 30.06M round trips (p40 19.1 µs, max 5.08 ms) |

**The one knob the design itself pointed at has since been swept across its whole
range, in one session.** `deepmsg.dpdk.rx.burst` is what one pass may take off
the port, and the plan called the receive path's selectivity the most likely of
its tuning items to have something in it — 32 frames can arrive at once while
each pass consumes one datagram (`DATAGRAMS_PER_POLL = 1`, `receiver.rs:66`).
Six cells at 501K on the media arm, only the knob differing, with the two ends of
the session given the same setting so the drift is the floor
(`analysis/dpdk/d6-abba/sweep-run.sh`, the table in `sweep-analyse.sh`):

| `rx.burst` | 1 | 8 | 32 | 128 | 1024 |
|---|---|---|---|---|---|
| p50 (µs) | 18.18 | 18.18 | 18.22 / 18.13 | 18.16 | 18.14 |
| `E[X\|X<p99]` (µs) | 18.35 | 18.36 | 18.41 / 18.26 | 18.34 | 18.24 |

The floor is 0.53% on p50 and 0.78% on `E[X|X<p99]`; burst 8 against 128 moves
them by **0.09%** and **0.10%**, so the criterion is not met and the selectivity
is not built. A thousandfold change in what a pass may take, and the media arm
sits at 18.1–18.2 µs throughout — which is the negative arms' mechanism read
from the other side: a burst is what the port happens to hold, not a batch it
waits to accumulate.

**So 77's zero is evidence, and 84's now is too:**

- **84 `missed-packets`** (`rte_eth_stats.imissed`) is the device's own counter,
  and the batch above cannot overrun a port because its load is closed-loop: a
  driver that falls behind throttles the offer instead of overflowing the NIC —
  which is what the starvation arm measured, with the offered datagram count
  halved and not one frame dropped. It takes an **open-loop** flood to move it,
  and that was built and run afterwards: this build's media on one port of the
  card, `dpdk-testpmd --forward-mode=txonly --txpkts=64` on the other at ~10
  Mpps, and a client holding one subscription so that there is a transport for
  the frames to be kept by (`analysis/dpdk/d6-flood-arm.md` has the shape, the
  predictions and the three arms). Two arms, two runs, **84 = 260,271,331** and
  **264,777,277** against 82 = 281.9M and 280.7M received — with a control that
  holds the same wall clock and no generator at 0/0/0.
- **88 `queue-drop` needed a knob and a plain flood, and it has both now.** The
  knob is the media's receive window, which this slice wired up (the second point
  below), and the flood is the kernel's own socket on the other port — the one
  that sends datagrams this stack accepts. **Same session, same sender, same hundred
  thousand datagrams, one setting between the two arms:**

  | receive window | 82 received | 76 transport | 88 queue-drop | 76 + 88 |
  |---|---|---|---|---|
  | 128 KiB (the default; a 256-frame ring) | 100,005 | 100,000 | 0 | 100,000 |
  | 1408 B (one frame; a **two**-frame ring) | 100,005 | 99,735 | **265** | 100,000 |

  Two counters dividing the offered load exactly — 99,735 + 265 = 100,000, not
  one datagram in neither — with 84, 86, 89 and 95 all at zero. Shrinking the
  ring from 256 frames to 2 is what makes the counter move, which is also why
  it was unreachable before the window was wired: **a counter whose threshold
  cannot be configured is a counter nobody can test.** What the attempts to
  move it before that found instead is a mechanism and a defect:

  1. **A closed-loop load cannot fill the ring**, and never will: it holds two
     receive windows while Aeron's flow control promises at most one in flight
     (`stack.rs:165-169`).
  2. **It could not be sized either, until this slice**: the media's receive
     window was a hard-coded constant (`dpdk.rs:550`) while
     `aeron.rcv.initial.window.length` is a real setting the **kernel** path
     honours (`receive_endpoints.rs:589-594`) — so a driver configured above
     128 KiB had every subscription on the media refused, and no setting could
     shrink the ring. That is fixed: the driver's window is what the media's
     stack is built with, so the ring follows it.
  3. **Under the flood the ring is drained every pass, so it cannot fill.** The
     flooded frames are kept, pushed and popped — and then dropped by the
     transport, which re-reads every frame with `Checksums::Software`
     (`transport.rs:263-292`) after the demux has taken the *port's* word for
     it. The generator's frames carry a **zero UDP checksum** (measured, by
     decoding a capture of what it puts on the wire), which hardware reads as
     "not computed" and calls good while the software sum is not; and the
     `continue` that follows **counts nothing and does not spend the pass's
     datagram budget**, so one pass empties the whole ring. 76 stayed at zero
     with it, and 88 could not rise. Under a flood to a port **nothing** is
     bound to — where the same frames are unclaimed and counted — the stack
     reports 85M of them, so this is not a stack that failed to run.

Read the table for what it is: **the media dropped nothing under the load this
section measures, and all three counters that would have said otherwise have
since been shown able to move** — 77 by a pool too small, 84 by an open-loop
flood, 88 by a ring small enough to fill. The third one's answer is not about
this load at all, and it is worth having on its own: a frame the
transport's software re-read rejects is dropped in silence, with no counter
anywhere — which is the one thing the demux's own module note says must not
happen ("a driver whose counters say nothing arrived while its transports say
otherwise is a driver nobody can debug"). It is not fixed here: it is in the
driver's transport rather than in what this slice was allowed to touch, and it
wants a slice of its own with a criterion.

**One caveat found on the way, for anyone reading port counters later:** 75, 82,
83, 84 and 91 are written by a timer that shares the driver's thread, so a
driver whose thread is starved reports *stale* port numbers — the starvation arm
read 16.9M datagrams where the traffic implies 30.5M. The per-transport counter
(type 76) is written by the reading thread itself and does not have that problem.

**What reading those counters costs, since the same thread does both.** `rte_eth_stats`
and the link are cheap and are read every second; a port's **extended** statistics
(92) are ~150 hardware registers on the X540, and the answer to the design's own
open question about reading them against live traffic is *bounded rather than
resolved*: read once a second, a two-binary ABBA at 501K puts them at **+0.22 µs
on p99** (0.85%, against that session's floor of 0.10–0.49%) with p50, p90 and
`E[X|X<p99]` inside theirs (0.08%); read **once a minute** — which is where they
are now (`EXTENDED_STATS_EVERY`) — the next session's ABBA came out 1.0% *below*
the unpatched binary, with the unpatched binary's own two cells 0.90% apart. So
the effect is real in mechanism and at the edge of what p99 can resolve on this
machine, and it is bounded at 0.22 µs at 1 Hz and unmeasurable at 1/minute.
Nothing in the tables above turns on it: they are p50/p90 with a p99 column,
and their floors are wider than that
(`analysis/dpdk/d6-92-cost.md` has both runs).
