# A whole reference cluster, three members on three hosts

`run-cluster-4host.sh` drives the reference's own cluster orchestration
(`scripts/aeron/remote-cluster-benchmarks` from the harness checkout) with three
members on three machines and the client, with its own driver, on a fourth.
Every node is the reference's — consensus module, archive, clustered service —
and the only thing that changes between the two scenarios of a round is the
**media driver** under it:

| arm | media driver |
|---|---|
| `java` | the reference's Java driver |
| `deepmsg` | this build's driver, through the harness's `deepmsg-media-driver` |

A reading here says what this build's driver is worth when a whole reference
cluster runs on top of it. It says **nothing** about this build's consensus
module, archive or container: none of them is in the cell. Those are
`bench/cluster-arms/`, which runs four arms (`j` / `j-d` / `r-a` / `r`) over
three members on three hosts with the client on a fourth.

## What it needs

- **Four machines of one type**, on one VPC/subnet. The 2026-11-02 run used
  `ecs.c7.8xlarge` (16 cores, no SMT, 64 GiB). The addresses live in
  `hosts.env` and nowhere else; `cluster_require_hosts` refuses a missing or
  duplicated host rather than guessing one.
- **`provision-host.sh --check <user@host>`**, then again without `--check`, on
  every machine that is not the controller. It copies the harness, this build's
  driver and an offline JDK (with the `/etc` trees the JDK's absolute symlinks
  point at), and sets `rmem_max` — the Java driver only warns about the image's
  ~416 KiB, the C driver exits.
- **The harness patched to know this build's driver**:
  `bench/cluster-arms/patches/install.sh --apply`, which adds
  `scripts/aeron/deepmsg-media-driver`, the `drivers['deepmsg']` row in
  `remote-cluster-benchmarks`, and the two name tables the harness's wait and
  kill paths read.
- **The receive interrupts on cores that do not sleep**, on all four:
  `bench/tools/irq-steer.sh apply`. Without it a cluster cell measures C-state
  wakeups; see `bench/README.md`, "The interrupt layout".

## Running it

```
bench/cluster-4host/run-cluster-4host.sh --list      # the plan, paths and preflight; runs nothing
bench/cluster-4host/run-cluster-4host.sh --quick     # 5 s per arm, plumbing only
bench/cluster-4host/run-cluster-4host.sh             # four tiers, 60 s + 30 s warmup, two rounds
```

`--rounds 1,2` and `--fsync 0,2` narrow the grid. The second round reverses the
scenario order, so a difference that follows the machine rather than the driver
flips sign. Results land under `bench/out/cluster-4host/` (`OUT_DIR` overrides),
one log and one downloaded directory per round.

Every `CLUSTER_*` and `*_CPU` setting in `hosts.env` is a default that the
environment can override before the file is sourced — the ports come out of one
block per member, above the ephemeral range, and the cores are the layout the
2026-11-02 run used.

## What a reading needs

The same rules as every other batch (`bench/README.md`, "The rules every cell
keeps"): two rounds that agree in sign, a floor measured in the same session, no
mean at this scale. One more belongs to this topology: **a cell is only a
three-member cell if all three members were alive at the end**. Two of the three
are a quorum, so a cell that sends every message can still have lost a member —
`bench/cluster-arms` records each member's counters and election count for that
reason. This runner leaves that evidence to the harness's own logs, and a cell
whose members are not all named in them is not a reading.

## Where the numbers are

The readings this runner produced are in the maintainer's analysis notes, not in
this repository. The default addresses in `hosts.env` are the machines of the
2026-11-02 run; cloud instances get new addresses when they are stopped, so ask
before reusing them.
