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
| `bench/upstream-c-drivers` | the media driver, three ways: Java, a C control built here, a C build of the changes this tree is asking upstream for | which layer, because exactly one layer moves |
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
(each row against itself across the two rounds) is not a difference. The second
batch has not been run yet; the first one has.

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
