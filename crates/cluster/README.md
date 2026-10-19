# deepmsg cluster (P3)

Five crates: one per seam the reference draws, plus the operator's tool
across them. The design is `analysis/cluster/README.md`; this file is the
map of the code.

| Crate | Reference counterpart | What it is |
|---|---|---|
| `common/` → `deepmsg-cluster-common` | the pieces `aeron-cluster` shares | `aeron.cluster.*` properties and their disposition table, the collections whose layout is observable, guarded SBE decoding |
| `consensus/` → `deepmsg-cluster` | `io.aeron.cluster` | the consensus module, the election state machine, `recording.log`, `node-state.dat`, `ClusterBackup`, `ClusterTool` |
| `service/` → `deepmsg-cluster-service` | `io.aeron.cluster.service` | the service container and the `ClusteredService` API — the S2 seam's container side |
| `client/` → `deepmsg-cluster-client` | `io.aeron.cluster.client` | `AsyncConnect`, the egress poller, leader change — the S1 seam's client side |
| `tool/` → `deepmsg-cluster-tool` | `io.aeron.cluster.ClusterTool`, `ClusterToolOperator` | the operator's tool: the reference's eighteen commands, over a cluster's mark files, recording log, counters and archive |

Dependency direction, and the rules it encodes:

```
consensus ─┐
service ───┼─▶ common
client ────┘
tool ──▶ consensus, service, client, archive
    service ──▶ deepmsg-client      (the container is a client of the driver)
    service ──▶ deepmsg-driver      (idle strategies only — the reference's
                                     `aeron-cluster` depends on `aeron-driver`
                                     for the same names)
```

- **`client` does not depend on `consensus` or `service`.** An application
  talking to a cluster must not link the consensus code, or LC-1 stops being a
  test of anything.
- **`service` does not depend on `consensus`.** The container has to be able to
  run against a *Java* consensus module (LC-4a); the direction keeps that true
  by construction rather than by discipline.
- Every crate forbids `unsafe` and denies `clippy::disallowed_types`; the list
  of banned types is `clippy.toml` in each, and it is about *observable
  behaviour*, not taste (no locks or channels between agents, and no map with a
  randomised hasher).

## Seams to preserve

Carried over from the placeholder that stood here before P3. These are the
premium features' seams, kept even though the features are out of scope; a
later build must be able to add them without changing a byte of what is here.

- `isStandby` identity and the `JoinLog.isStandby` field (schema 111
  sinceVersion 16),
- the two flags gates for the STANDBY_SNAPSHOT cluster action (consensus
  module side and container side),
- the StandbySnapshot(81) notification message and its persistence ledger
  semantics,
- the mark-file takeover rule: the only tolerated component-type migration
  on restart is BACKUP -> CONSENSUS_MODULE (reference
  `ClusterMarkFile.java:186-193`), with candidate term id inherited and the
  error log rescued,
- reserved counter type ids 220..=232 (already encoded in
  `deepmsg-core::counters`),
- the cluster schemas carried under `schemas/`.

## Tests that are tables, not code

- `tests/reference-cases-{consensus,other}.tsv` — one row per reference case
  (574: 178 system + 396 unit), with what each case asserts and what we do
  about it. Both lanes share the same rules; each lane owns one file.
- `common/tests/fixtures/` — two fixtures produced by *running* agrona-2.6.1,
  not by reading it: the iteration order of `Long2LongHashMap` (which decides
  snapshot bytes) and the accepted duration/size syntax. The `regen/` scripts
  that made them are kept beside them.
