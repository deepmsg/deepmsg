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
