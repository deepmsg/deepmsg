# ADR-0007: Compatibility baseline moves to Aeron 1.54.2

- Status: accepted (2026-11-26), supersedes [ADR-0001](0001-compat-baseline.md)

## Context

ADR-0001 fixed the baseline at Aeron 1.53.2 (commit `664f58e705`) and made an
upstream bump a deliberate act with a checklist: update `schemas/`, commit the
diff, extend `docs/compat.md`, extend the interop suite. This is that act,
performed for Aeron 1.54.2 (commit `481d4271ad`, 2026-11-11), the last release
in the 1.54 line whose contracts this build can follow without also adopting
1.54.3.

The 288 commits between the two tags change four public contracts and repair
several behaviours; everything else is unchanged. The full analysis, the
per-contract evidence and the delivery split are in
`analysis/aeron/deepmsg-aeron-1.54.2-upgrade-plan.md` (outside the repository).

## Decision

Target byte-level compatibility with Aeron **1.54.2** (commit `481d4271ad`).

The contract changes adopted with this baseline:

- **Cluster SBE `111/17` → `111/18`**: `ClusterActionRequest` (id 6) and
  `SnapshotMarker` (id 8) gain `termBaseLogPosition`, threaded through the
  consensus module's append and snapshot paths. CnC `0.2.0`, archive SBE
  `101/14`, archive mark `3.1.0`, archive protocol `1.12.0`, cluster mark
  `110/2`, node-state `112/10` and both cluster protocols are unchanged.
- **Driver settings**: `aeron.driver.cpuset.affinity` became
  `aeron.thread.affinity.use.cpuset.offsets`, and the validation-errors switch
  became `aeron.thread.affinity.fail.on.validation.errors`. Four settings are
  new and implemented: `aeron.socket.tos`,
  `aeron.receiver.poll.to.slow.work.ratio`,
  `aeron.udp.channel.transport.poller.iteration.threshold` and
  `aeron.udp.channel.transport.poller.socket.poll.limit`. The io vector maxima
  and the network publication message maximum are 64, not 16.
- **Channel URI**: `so-tos` is accepted, resolved and applied as `IP_TOS` /
  `IPV6_TCLASS` on both sockets.
- **Response subscriptions**: an explicit `session-id` must match the response
  publication's session; the driver refuses the mismatch.

The archive's recovery guards (short frames and out-of-range writes) and the
concurrent-recording bound (two map sizes) are adopted with the same baseline,
as are the upstream renames (`*EventLogger` → `*Tracer`, the
`AERON_COUNTER_ARCHIVE_*` counter macros).

## Consequences

- Every `file:line` citation in the repository was re-homed against 1.54.2.
  A citation means "this line at 1.54.2"; the reference checkout expected at
  `../aeron` is that tag.
- The interop, archive and cluster ledgers were regenerated against the 1.54.2
  surfaces: archive C suite 317 cases, cluster 175 system + 404 unit rows.
- 1.54.3 (2026-11-24) has no schema change and is not adopted here; a later
  bump is a new ADR, not an edit to this one.
- `deepmsg-cpp` stays on 1.54.0 and its own upgrade is a separate decision.
- Behavioural gaps that remain are recorded in `docs/compat.md`, not silently
  inherited: `AERON_HDR_TYPE_ATS_SM` is not implemented, the receiver drains
  the command queue every pass rather than on the slow tick, and
  `PROTOCOL_SEMANTIC_VERSION` stays this build's own 17.
