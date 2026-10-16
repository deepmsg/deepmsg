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

## What is here so far

| file | what it is |
|---|---|
| `read.py` | the readings: latency quantiles from the cells' histograms, the three ratios, the drift floor, and the archive's own counters and disk |
| `self-check.sh` | builds a result tree whose every number is known and checks the table `read.py` prints from it |
| `../lib/conf/archive.properties` | the archive settings a batch runs on, each with what this build does with it |

The batch itself (`run-batch.sh`, the cells, the patches that let the reference's
harness be pointed at an archive it does not own) is **not here yet** — it is the
next slice, and `read.py`'s module note is the contract it has to satisfy.

## Running the reader

```
bench/archive-vs-java/read.py <results-dir> [--stat e_lt_p99|p50|p40]
bench/archive-vs-java/self-check.sh          # needs a HdrHistogram to read with
```

`self-check.sh` wants the same thing `tools/quantiles.sh` does — the harness's
jar, or `HdrHistogram` and `agrona` in the Gradle cache — and prints `SKIPPED`
without it rather than failing.

## The two rules a cell keeps

They are the driver batches' rules, unchanged, and they are worth restating
because a green table hides both:

- **A cell that did not finish is not a reading.** The batch gates on the
  client's own `WARNING:` line and on the histogram holding what the rate and the
  iterations asked for; a cell that failed is `.FAIL` rather than a row.
- **Nothing is measured on a busy machine**, and nothing is read from one round.
  Every ratio is printed per round, and a ratio is read only when the two rounds
  agree in sign — the floor table is the drift that has to be smaller than the
  difference being claimed.
