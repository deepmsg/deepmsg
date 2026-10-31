# PREREG — the cluster arms batch (P3-10 / LC-7)

Written **before** the batch has been run for a reading, and not changed after.
The work package is `analysis/cluster/plan-2026-10-29-p3-10-cluster-arms.md`; the
criteria are LC-7a/b/c in the P3 performance spec. What follows pins the six
things the plan's §4.3 says a pre-registration must pin.

Everything in this file is **relative**. There is not a single microsecond or a
single messages-per-second figure here, on purpose: a number whose units are ns
belongs to one session on one machine, and this file has to outlive it.

## 1. Scope

**Single-member clusters only.** LC-7's three criteria are read on the
single-member tier (the work package's Q6 decision). The three-member tier is a
functional check — it starts, it echoes, one `--quick` cell — and produces **no
latency or throughput conclusion**. Nothing in this file applies to it.

The batch runs on **two machines**: the node side (driver, archive, consensus
module, container) on the bench machine, and the client **and its media driver**
on the peer. `--two-host --peer <user@host>` is the only shape whose numbers are
readings; a one-host run is plumbing, not evidence, and may not be quoted as an
LC-7 result.

The client is the reference's in every arm, and so is the client's media driver
(the reference's `media-driver`, started on the peer). No arm changes the
instrument. The peer's harness checkout is part of the fingerprint: the batch
refuses two different reference revisions on the two machines.

## 1a. The client link, and the tiers it can carry

The client leg is a **1GbE direct link**, and that is a hard ceiling on the grid,
not a matter of tuning. One request and one answer per message, `batch.size=1`,
with Aeron + UDP/IP + Ethernet headers:

| tier | payload per direction | with ~98 B of headers | verdict |
|---|---|---|---|
| `25K:32` | 25K x 32 B = 6.4 Mbps | ~26 Mbps | runnable |
| `501K:32` | 501K x 32 B = 128 Mbps | ~520 Mbps, ~500K pps | runnable; the packet rate is the risk |
| `301K:288` | 301K x 288 B = 693 Mbps | ~930 Mbps (~93% of line rate) | **not runnable**: no margin |
| `101K:1344` | 101K x 1344 B = **1.086 Gbps** | ~1.16 Gbps | **not runnable**: above the link's one-direction rate before any header |

`101K:1344` is excluded by arithmetic, not by a measurement: no header budget
brings 1.086 Gbps under 1 Gbps. `301K:288` is excluded because 93% of line rate
leaves nothing for the client's own UDP stack and the USB adapter. If the client
leg is replaced with a 10GBASE-T adapter, a **new PREREG** is written for the new
topology; this one is not edited after a reading.

## 2. The arms, and what each ratio may say

| arm | driver | archive | consensus module + container | client |
|---|---|---|---|---|
| `j` | reference (Java) | reference (Java, in the node JVM) | reference (Java, in the node JVM) | reference |
| `j-d` | **ours** | reference (in the node JVM) | reference (in the node JVM) | reference |
| `r-a` | **ours** | **reference** `io.aeron.archive.Archive`, own JVM | **ours**, two processes | reference |
| `r` | **ours** | **ours**, own process | **ours**, two processes | reference |

| ratio | the two arms differ by | what it is allowed to say |
|---|---|---|
| `r-a / j-d` | archive is the reference's in both; driver is ours in both | **the consensus module and the container** |
| `j-d / j` | node is the reference's in both | **the driver** |
| `r-a / r` | driver and consensus module are ours in both | **the archive** |
| `r / j` | nothing is swapped inside either | the whole stack; attributes nothing |

★ The client is the reference's in all four arms, and so is the client's media
driver (the reference's `media-driver`, on the peer). No ratio here is a ratio
between instruments.

## 3. The grid

- **Tiers** (rate : message length): `25K:32` and `501K:32`. The tree's earlier
  work is read at four tiers; the other two (`101K:1344`, `301K:288`) are
  excluded by §1a's link budget **before** any reading, not dropped after one.
- **Sync levels**: archive `{file,catalog}.sync.level` × cluster
  `file.sync.level`, **each 0 or 2, all four combinations**:
  `0:0`, `0:2`, `2:0`, `2:2` (`archive:cluster`).
- 2 tiers × 4 sync combinations × 4 arms × 2 rounds = **64 cells**, one run
  each. `--quick` exists to check the plumbing and is not a reading.

Every cell uses the same channels, the same cores and the same client. The only
thing that changes between arms is the table in §2.

## 4. The processes, the threads and the cores

The node side is **five busy-spin threads** in every arm, and the driver is
three. The reference's node puts five of them in one JVM; the `r-a`/`r` arms put
them on four processes. The thread count and the core count are the same, which
is what makes "one process fewer, one process more" not a variable.

| arm | process | threads | cores |
|---|---|---|---|
| all | node-side media driver | conductor, sender, receiver | 1, 2, 3 (its own properties file) |
| `j`, `j-d` | `ClusterNode` (Java) | archive ×3, consensus module, echo service | `0,4,5,6,7` |
| `r-a`, `r` | archive | ×3 | `4,5,6` |
| `r-a`, `r` | consensus module | 1 | `7` |
| `r-a`, `r` | container | 1 | `0` |
| all | **client's media driver** (reference `media-driver`, on the peer) | conductor, sender, receiver | the peer's physical cores, no SMT siblings; read off its `lscpu`, written into RUN-INFO |
| all | client (reference `cluster-client`, on the peer) | 1 | the peer's physical core, no SMT sibling of the driver's |

The client's driver has its own `aeron.dir` on the peer's `/dev/shm`
(`/dev/shm/cluster-arms-client/aeron`), not the node's. A term buffer on a
disk-backed file would put a page fault in the measured path; the peer's `/tmp`
is its ext4 image, so scripts and histograms live there and the ring does not.
The peer's driver is given its own `aeron.{conductor,sender,receiver}.cpu.affinity`
in an override file that comes after the harness's two property files.

Idle strategy is `spin` everywhere. The reference's `ClusterNode` hard-codes
`BusySpinIdleStrategy` and ignores `-Daeron.cluster.idle.strategy`, so the arms
this build owns are given `-Daeron.cluster.idle.strategy=spin` explicitly rather
than left on their default: a difference between the arms may not be a
difference in idle strategy.

## 5. The rounds

Two rounds, ABBA: the first visits the arms in the order given on the command
line, the second in reverse. A ratio is **read** only when both rounds agree in
sign; one that flips is reported as `flips — not a reading`.

The floor is printed with every table: an arm against itself across the two
rounds. **A difference inside the floor of its own cell is not a difference**,
and the cell is marked void rather than read.

The **path** is part of the instrument. Before the first cell, the batch sends
1000 64-byte datagrams from the bench to the peer and back over the same link and
writes the p50 / p99 / p99.9 into RUN-INFO. The probe is Python at both ends, so
its number is an upper bound on the wire-plus-stack cost, not a correction to
subtract from a cell. It exists to be read beside the table: a cell whose effect
is smaller than the path's own spread is a cell the topology cannot resolve.

The statistic is `E[X | X < p99]`, with p40 / p50 / p99 / p99.9 reported beside
it. **The mean is not used** — the reference's node stops for ~8 ms in every
cell, which is a whole microsecond of mean and nothing of a p50.

## 6. The criteria, and what happens when one fails

| # | criterion | pass |
|---|---|---|
| LC-7a | no regression | per cell: `r-a / j-d` ≤ 1 + that cell's floor, **and** the two rounds agree in sign (or both lie inside the floor) |
| LC-7b | the tail | `r-a / j-d` < 1 at p99.9 in **most** cells, both rounds agreeing in sign |
| LC-7c | throughput | the highest runnable tier (`501K:32`): `expected == sent == received` — the rig's own check; the histogram may not be `.FAIL` |

**When a criterion fails:**

1. The cell is reported as it read. It is not dropped, not re-run until it looks
   right, and not explained away in prose.
2. `j-d / j` is read first: if the driver alone moves the cell by more than the
   floor, the difference is the driver's and the criterion is not a statement
   about the cluster layers.
3. The gate in §4.1 of the plan holds regardless of what the readings say: **a
   reference node that cannot run on this build's driver in one member stops the
   card** — it is not degraded to "we only compared `r` with `j`".
4. A cell that did not finish is `.FAIL`, not a row and not a silent absence.

**What this PREREG may not be used for:** a comparison across sessions. The
machine has drifted before; a ratio is read within one batch, from its own two
rounds and its own floor.
