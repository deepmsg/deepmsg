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

The batch runs on one machine in this slice. A cell whose client runs on the
second machine is what a *read* number needs and is not implemented in
`run-batch.sh` yet; `--two-host` is refused by name rather than quietly run on
one host. **A number taken with the client on the same machine is plumbing, not
evidence**, and may not be quoted as an LC-7 result.

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

★ The client is the reference's in all four arms. No ratio here is a ratio
between instruments.

## 3. The grid

- **Tiers** (rate : message length): `25K:32`, `101K:1344`, `301K:288`, `501K:32`
  — the four the tree's earlier work is read at (`rust-vs-java`'s default).
- **Sync levels**: archive `{file,catalog}.sync.level` × cluster
  `file.sync.level`, **each 0 or 2, all four combinations**:
  `0:0`, `0:2`, `2:0`, `2:2` (`archive:cluster`).
- 4 tiers × 4 sync combinations × 4 arms × 2 rounds = **128 cells**, one run
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
| all | media driver | conductor, sender, receiver | 1, 2, 3 (its own properties file) |
| `j`, `j-d` | `ClusterNode` (Java) | archive ×3, consensus module, echo service | `0,4,5,6,7` |
| `r-a`, `r` | archive | ×3 | `4,5,6` |
| `r-a`, `r` | consensus module | 1 | `7` |
| `r-a`, `r` | container | 1 | `0` |
| all | client | 1 | the peer machine |

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

The statistic is `E[X | X < p99]`, with p40 / p50 / p99 / p99.9 reported beside
it. **The mean is not used** — the reference's node stops for ~8 ms in every
cell, which is a whole microsecond of mean and nothing of a p50.

## 6. The criteria, and what happens when one fails

| # | criterion | pass |
|---|---|---|
| LC-7a | no regression | per cell: `r-a / j-d` ≤ 1 + that cell's floor, **and** the two rounds agree in sign (or both lie inside the floor) |
| LC-7b | the tail | `r-a / j-d` < 1 at p99.9 in **most** cells, both rounds agreeing in sign |
| LC-7c | throughput | highest tier: `expected == sent == received` — the rig's own check; the histogram may not be `.FAIL` |

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
