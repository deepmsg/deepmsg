# Known-bad upstream paths

Paths that look plausible but do **not** exist in the Aeron 1.53.2 checkout
(commit `664f58e705`). Every one of them has been cited in this repository at
some point. Resolve a path with `ls` before trusting it.

If an upstream bump makes a row here obsolete, update the row — do not delete
it silently, since the shape of the mistake is the reusable part.

| Cited path | Where it actually is at 1.53.2 |
|---|---|
| `aeron-driver/src/main/c/aeron_sender.c` | `aeron_driver_sender.c` — the `_driver_` infix |
| `aeron-driver/src/main/c/aeron_receiver.c` | `aeron_driver_receiver.c` |
| `aeron-driver/src/main/c/concurrent/aeron_driver_conductor.c` | `aeron-driver/src/main/c/aeron_driver_conductor.c` — no `concurrent/` in the driver root |
| `aeron-driver/src/main/c/flow_control/` | no such directory; the file is `aeron-driver/src/main/c/aeron_flow_control.c` |
| `aeron-client/src/main/c/logbuffer/` | no such directory; `aeron-client/src/main/c/concurrent/aeron_logbuffer_descriptor.{c,h}` |
| `aeron-client/src/main/c/media/` | `media/` is a **driver** directory: `aeron-driver/src/main/c/media/` |
| `aeron-client/src/main/c/media/aeron_udp_channel.c` | `aeron-driver/src/main/c/media/aeron_udp_channel.c` — wrong tree entirely |
| `aeron-client/src/main/c/util/aeron_uri.c` | `aeron-client/src/main/c/uri/aeron_uri.c` — `uri/` and `util/` are different directories |
| `aeron-client/src/main/c/concurrent/aeron_atomics.{c,h}` | `aeron-client/src/main/c/concurrent/aeron_atomic.{c,h}` — singular |
| `aeron-client/src/main/c/concurrent/aeron_dist/counter.{c,h}` | no `aeron_dist/` exists anywhere; the counter is `aeron-client/src/main/c/aeron_counter.c` |
| `aeron-archive/src/main/java/io/aeron/archive/RecordingLog.java` | `aeron-cluster/src/main/java/io/aeron/cluster/RecordingLog.java` — wrong module |

## Traps worth remembering

- **`uri/` vs `util/`** in the client tree. Both exist. URI parsing lives in
  `uri/`; general helpers live in `util/`.
- **`media/` and `concurrent/`** exist in both the client and the driver tree
  but hold different files. The UDP channel and transport code is driver-side.
- **`aeron_atomic` is singular** at `aeron-client/src/main/c/concurrent/`,
  alongside `aeron_atomic64_c11.h` and `aeron_atomic64_gcc_x86_64.h`.
- **`RecordingLog.java` is a cluster file**, not an archive file. The archive's
  on-disk formats live in `Catalog.java` and `ArchiveMarkFile.java`; the
  cluster's recording log is where `ENTRY_TYPE_INVALID_FLAG = 1 << 31` and
  `RECORD_ALIGNMENT = 64` are defined.
- **There is no `aeron_dist/` directory** at 1.53.2. The distributed-log
  structures sit directly under `concurrent/` — ring buffers, concurrent
  queues, term scanners and rebuilder.
