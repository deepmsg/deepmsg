# deepmsg-cluster (placeholder)

The cluster track (replicated state machine over the archive, elections,
snapshots) and the premium-style standby features are **out of scope for
now**. This directory is a placeholder so the seams stay visible; it is not
a workspace member yet.

Seams to preserve while building everything else:

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

Primary inputs when this track starts: the reference cluster implementation
(`aeron-cluster/src/main/java/`) and the cluster schemas carried under
`schemas/`.
