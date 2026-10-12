# The run batches

Two batches live here. Both measure this build against the reference's, both
drive the reference's own benchmark harness (`aeron-io/benchmarks`), and both
write their results into `bench/out/` — which is ignored, because what belongs in
the tree is the table and the command that produced it, not the histogram files.

| batch | question | can it say which layer? |
|---|---|---|
| `upstream-c-drivers/` | what are the C driver changes in this tree's upstream work worth? | yes — one layer is swapped, three times over |
| `rust-vs-java/` | two whole stacks: how far apart are they? | **no** — nothing is swapped inside either arm |

That distinction is the whole reason the two are separate, and it is the one
thing to hold on to when reading either: a number from `rust-vs-java` is a
statement about two systems, and cannot be used as if it were a statement about
this build's driver, client or node.

## Running one

```
bench/build.sh                                   # the driver, the rig, the node

# the C driver arms
bench/upstream-c-drivers/build-c-driver.sh c-control upstream/master
bench/upstream-c-drivers/build-c-driver.sh c-patched driver/c-driver-receive-path
bench/upstream-c-drivers/patches/install.sh --apply
bench/upstream-c-drivers/run-batch.sh            # ~80 minutes with the defaults

# the two stacks
bench/rust-vs-java/run-batch.sh                  # ~40 minutes with the defaults
```

Every entry script takes `--list` (print the plan, the digests and whether the
prerequisites are met — it runs nothing and exits non-zero when something is
missing), `--quick` (the same shape in about a minute, for checking the plumbing
rather than for numbers), and `--out`. Each one writes three things beside its
results:

- `fingerprint.txt` — the machine, the toolchain, every checkout by revision, the
  binaries by digest (`lib/fingerprint.sh`). A number without it is an anecdote.
- `RUN-INFO.txt` — what this particular batch was: the arms, the grid, the cores,
  the channels, the settings the drivers were actually given.
- the histograms themselves, one per cell, in the harness's own layout.

## Why a batch needs three directories

Only one of the two stacks being compared is in this repository. The harness is a
checkout of `aeron-io/benchmarks`; the C driver is built out of a checkout of
`aeron-io/aeron`, and the tree's upstream work lives in a clone of a fork of it.
None of them is vendored, so each is an environment variable with a sibling
directory as its default — `BENCHMARKS_DIR`, `AERON_REF_DIR`, `AERON_FORK_DIR`,
`DEEPMSG_DIR` — and `lib/checkouts.sh` is where those are named and checked.

Two of those defaults are worth saying out loud:

- **`BENCHMARKS_DIR` is `../benchmarks-deepmsg`, and it has to be a clean
  checkout.** That one carries nothing but this tree's two patches. The other
  checkout on this machine, `../benchmarks`, is the one the earlier sessions were
  measured with: it has the `leg1` and answer-curve probes in its Java sources,
  a send-delta histogram, and a local pin of the harness to Aeron 1.53.2. Those
  are instruments, not settings, and a number taken with them is a number about a
  modified harness. Point `BENCHMARKS_DIR` at it and the batch will run, and the
  result will not be evidence. `lib/checkouts.sh` says the same thing again where
  someone setting the variable will see it.
- **The harness has to have been built once**, because its Java client, its Java
  node and the Java driver all come out of one shadow jar:

      cd "$BENCHMARKS_DIR" && ./gradlew :benchmarks-all:shadowJar

  A clean checkout does not have it. Both batches check for it and stop with that
  command rather than failing later on a missing class.

`AERON_REF_DIR` (`../aeron`) is the frozen 1.53.2 reference. It is in the
fingerprint because it is part of what the machine was, and neither batch reads
anything out of it: a batch touches the harness (two patches) and the fork, and
leaves the reference alone.

## The rules every cell keeps

Four, all of them paid for once already (`lib/cell.sh`):

1. **A run starts from nothing.** `/dev/shm/aeron` is removed before the driver
   starts; a leftover directory is a leftover window.
2. **The driver is taken down, not asked down** at the end of a cell. SIGTERM
   runs a shutdown that stamps every system counter `RECLAIMED`, and a reader
   that prints only `ALLOCATED` then finds `Bytes sent` and the cycle-time
   counters nowhere. The reference's own runner uses `kill -9` for the same
   reason.
3. **A cell that did not finish is not a reading.** The check is the client's own
   `WARNING:` line, a histogram that is present rather than `.FAIL`-suffixed, and
   at least as many samples in it as the rate and the iterations asked for.
4. **Nothing is measured on a busy machine.** Two of these processes are
   full-core spinners, and a stray one from the previous cell is a second
   variable.

And the reading rules, which are in `docs/benchmarks.md` and are not negotiable:

- **Two rounds, the second with the order reversed.** A ratio whose sign survives
  the reversal is the thing being measured; one that does not was the session.
- **`E[X | X < p99]` and p40/p50 decide; the mean does not.** The reference's node
  stops for ~8 ms in every cell — a whole microsecond of mean and nothing of a
  p50 — and cross-curve percentile subtraction is not a distribution either.
- **The floor is printed with every table.** An arm against itself across the two
  rounds is the drift, and a difference inside it is not a difference.
- **Nothing is compared across sessions bit for bit.** Direction and magnitude,
  or it is a different machine.

## The readers

`tools/HdrQuantiles.java` prints one row of quantiles per histogram file, in
nanoseconds; `tools/quantiles.sh` runs it against the harness's own HdrHistogram
classes. Both batches' `read.py` build their tables out of that one tool, which
is what makes a file written by the Java client and a file written by the Rust
rig readable side by side at all — they are the same format, `result.rs` writes
the interval log the harness's `PersistedHistogram` writes.

## What is not here

The instruments. The probes that made the layered readings possible — the
`leg1`/`leg1.actual` histograms, the answer curve, the send-delta histogram, the
per-turn gates — are kept beside the sessions that used them rather than in the
tree, because they cost ~1.5% of the round trip when they are on and change the
link-time layout even when they are off. A batch here measures what ships.
