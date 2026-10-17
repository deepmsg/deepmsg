# `archive-vs-java/`

This build's archive against the reference's, with the reference's own benchmark
harness as the instrument and the reference's own clients as the load.

## The arms

| arm | driver | archive | client / far end |
|---|---|---|---|
| `j` | the reference's Java driver | the reference's archive | the reference's clients |
| `r-attach` | the reference's Java driver | **ours** | the reference's clients |
| `r-java` | **ours** | **ours** | the reference's clients |
| `r-rust` | **ours** | **ours** | **our** Rust rig and `ArchiveNode` |

★ **Only `r-attach` against `j` says anything about the archive**, because those
two differ by exactly that. `r-java` against `r-attach` is the driver's
contribution, and `r-rust` against `j` differs by all three layers and is the
product-level number rather than an attribution.

`j` and `r-rust` run today. `r-attach` and `r-java` need
`patches/external-archive.patch` — an `ArchiveNode` that connects to an archive
it does not own — and are **refused by name** until it lands, rather than run as
something they are not.

## One machine or two

The batch runs both shapes and **defaults to one machine**:

```
bench/archive-vs-java/run-batch.sh --quick                    # one machine, ~2 minutes
bench/archive-vs-java/run-batch.sh                            # both arms, both rounds
bench/archive-vs-java/run-batch.sh --two-host --peer gavin@host ...
```

On one machine the server half (driver + archive + node) and the client half
(driver + rig) are separate processes joined by loopback. On two the client half
moves to the peer, and `lib/lan.sh` proves a UDP datagram makes the round trip
**in both directions** before the first cell.

★ **Two machines is not the default because of what the wire here costs.** A cell
was run both ways on 2026-10-17 with the same parameters: one machine sent
125,000 of 125,000 messages at a 4.25 ms mean, the peer shape sent 56,165 (55%
lost) at a 55.7 ms median with a 1.86 s mean. The link carried 2.4 Gb/s but its
**round trip was milliseconds** — WSL2's virtual path — while the archive's own
cost is microseconds. The full readings are in
`analysis/bench/deepmsg-bench-plan-archive-vs-java.md` §6.5.

## What is here

| file | what it is |
|---|---|
| `run-batch.sh` | the batch: `--list`, `--quick`, `--out`, `--arms`, `--fsync`, `--two-host` |
| `read.py` | the readings: latency quantiles from the cells' histograms, the three ratios, the drift floor, the archive's own counters and disk |
| `self-check.sh` | builds a result tree whose every number is known and checks the table `read.py` prints from it |
| `../lib/conf/archive.properties` | the archive settings a batch runs on, each with what this build does with it |

The measurement classes the reference does not have (`external-archive.patch`, the
S1/S3/S4/S5 scenarios) are the next slices.

## The tree a batch writes

```
<out>/round-<name>/
    <scenario>_<arm>_fsync=<0|2>_mtu=<M>_length=<L>_rate=<R>/
        run-<N>/<*.hdr>  counters.txt  archive-du.txt  client.log
```

`counters.txt` is `<label>\t<value>` per line, with the ` - archiveId=` suffix
every archive appends (`ArchiveCounters.ARCHIVE_ID_LABEL_SUFFIX`) normalised
away: the labels are the reference's own, which is what makes a column read from
the `j` arm and the same column from `r-attach` the same counter.

## Running the reader

```
bench/archive-vs-java/read.py <results-dir> [--stat e_lt_p99|p50|p40]
bench/archive-vs-java/self-check.sh          # needs a HdrHistogram to read with
```

## The rules a cell keeps

They are the driver batches' rules, unchanged, and they are worth restating
because a green table hides both:

- **A cell that did not finish is not a reading.** The batch gates on the
  client's own `WARNING:` line and on the histogram holding what the rate and the
  iterations asked for; a cell that failed is `.FAIL` rather than a row.
- **Nothing is measured on a busy machine**, and nothing is read from one round.
  Every ratio is printed per round, and a ratio is read only when the two rounds
  agree in sign — the floor table is the drift that has to be smaller than the
  difference being claimed.
