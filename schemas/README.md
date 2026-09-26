# SBE schemas

Single source of truth for the SBE-encoded contracts. The wire protocol,
archive/cluster control protocols, mark files and the node-state file all
share SBE encoding; the only deliberately non-SBE on-disk format in the
system is the archive `recording.log` (hand-rolled inside `deepmsg-archive`).

Forked verbatim from the [aeron-io/aeron](https://github.com/aeron-io/aeron)
**1.53.2** tree (commit `664f58e705`) on 2026-09-25. Upstream filenames are
kept for painless diffing.

Copyright 2014-2025 Real Logic Limited, licensed under the Apache License 2.0.
Aeron is owned and operated by Adaptive Financial Consulting.

| File | Schema id / version | Upstream path |
|---|---|---|
| `aeron-archive-codecs.xml` | 101 / 14 | `aeron-archive/src/main/resources/archive/` |
| `aeron-archive-mark-codecs.xml` | 100 / 2 | `aeron-archive/src/main/resources/archive/` |
| `aeron-cluster-codecs.xml` | 111 / 17 | `aeron-cluster/src/main/resources/cluster/` |
| `aeron-cluster-mark-codecs.xml` | 110 / 2 | `aeron-cluster/src/main/resources/cluster/` |
| `aeron-cluster-node-state-codecs.xml` | 112 / see file | `aeron-cluster/src/main/resources/cluster/` |

Rules:

- No semantic edits. If a deviation is ever required, record it in
  `docs/compat.md` first.
- Upstream bumps: copy the new file in, commit the diff, update
  `docs/compat.md`, extend the interop/golden tests.
- The cluster schemas are carried for the future cluster track; nothing in
  the workspace depends on their contents yet (`deepmsg-codec` only validates
  their presence).
