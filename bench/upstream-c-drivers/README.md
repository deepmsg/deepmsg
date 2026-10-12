# The C driver arms

One batch, one question: **what are this tree's three C driver changes worth,
measured on the reference's own benchmark?**

```
bench/build.sh                                   # this build's binaries (only the `c` arm needs nothing)
bench/upstream-c-drivers/build-c-driver.sh c-control upstream/master
bench/upstream-c-drivers/build-c-driver.sh c-patched driver/c-driver-receive-path
bench/upstream-c-drivers/patches/install.sh --apply
bench/upstream-c-drivers/run-batch.sh            # ~40 minutes a round, two rounds
```

`BENCHMARKS_DIR` must be a **clean** checkout of the harness
(`../benchmarks-deepmsg` by default), it must have been built once
(`./gradlew :benchmarks-all:shadowJar`), and the arms are built out of
`AERON_FORK_DIR` (`../aeron-deepmsg`). `bench/README.md` says why the other
checkout on the machine — the one with the probes in it — is not the default.

`run-batch.sh --list` prints the plan and the digests without running anything,
and exits non-zero if a prerequisite is missing. `--quick` is the same shape in
about a minute, for checking the plumbing.

## The arms

Each one is a driver the run swaps in, and *arm* is the word the scripts use for
it (`--arms`, `AERON_C_ARM_DIR`, one launcher per arm). `docs/benchmarks.md`
calls the same things drivers, so that a table of them is not read as if a ratio
were a fourth one.

| arm | what it is | why it is there |
|---|---|---|
| `java` | the harness's own `media-driver` — the Java driver | the scoreboard: whether the result is interesting to anyone who is not us |
| `c` | the C driver binaries checked into the harness | a witness only. They are some other build of some other source at some other time |
| `c-control` | the unpatched revision, built here | **the control** |
| `c-patched` | the same revision with the three driver commits | the candidate |

Both C arms come out of `build-c-driver.sh`, from the same repository, with the
same cmake and the same compiler, taken as a `git worktree` at a revision so
nothing in the work area moves. That is the whole reason `c-control` exists
rather than reusing the harness's `c` arm: comparing a patch against a
prebuilt binary compares it against a compiler, an era and a set of flags too.
The ratio to read is **`c-patched / c-control`**; `c-patched / java` says whether
the result is worth anyone's attention, and `c-control / java` says how much of
the gap the source already had.

## The rounds

The harness's outer loop is over drivers, so the batch calls it twice — once
with the arms in order, once reversed — and prints both. A ratio whose sign
survives the reversal is the driver; one that does not was the session.

## What it changes in the harness, and how to take it back

`patches/install.sh --apply` puts four things into the harness checkout:

1. **`local-launch-shim.patch`** — the harness launches every process through
   `execute_remote_command`, which is `ssh -i <key> user@node` even when the node
   is this one. The patch makes an empty node mean "this host" (`bash -c`, and
   `cp` instead of `scp`), and lets the three throughput knobs be set
   independently of each other. Skipped when the checkout already has it.
   Setting up passwordless `ssh localhost` is the other way to run this, and the
   reason the check tolerates either.
2. **`supported-drivers.patch`** — two more entries in the single-host script's
   `supported_drivers`. The harness picks a driver by name and a name is a
   launcher; there is no variable that says which binary to run, so two more C
   drivers have to be two more names.
3. **`scripts/aeron/c-media-driver-c-control` and `…-c-patched`** — one launcher
   file, installed twice. It reads the arm out of its own name and execs
   `aeronmd` out of `$AERON_C_ARM_DIR/<arm>`, which `run-batch.sh` exports as
   this tree's `bench/out/arms`.
4. Nothing else. The two patches are the whole of it, and
   `patches/install.sh --revert` reverses them and removes the launchers.

Two things the harness does to itself that a reader should know about:

- It **rewrites `scripts/aeron/benchmark.properties`** on every run: that file is
  the driver settings for the run, not a checked-in setting. `git -C
  "$BENCHMARKS_DIR" checkout scripts/aeron/benchmark.properties` puts back
  whatever was there.
- It leaves its result tree under `scripts/results/` and an archive in the
  download directory. `run-batch.sh` extracts the archive into its own output
  tree and reads that; the harness's copy is left where it is.

## Reading the output

`read.py` prints, per round, each arm's `E[X | X < p99]` and the ratios, then the
floor — each arm against itself across the two rounds — and whether each ratio's
sign survived the reversal. `--stat p50` (or `p40`) reads the same tree in the
other column.

Nothing in the table is a verdict on its own. The rules the rest of this tree's
measurements are read by apply unchanged:

- a ratio is read only when both rounds agree in sign;
- a difference inside the floor is not a difference;
- `p99` is printed for shape only — the columns that decide are
  `E[X | X < p99]` and p40/p50;
- every result set goes with the `fingerprint.txt` and `RUN-INFO.txt` written
  beside it, or the numbers are anecdotes about a machine nobody can identify.

## The revisions

`run-batch.sh` records the revision of every arm's `aeronmd` and
`libaeron_driver.so` by digest, and the upstream work area's `HEAD`, in
`RUN-INFO.txt`. A table without those is not a comparison: it is two numbers
from two trees nobody can name.
