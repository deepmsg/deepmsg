# Protocol notes

Byte-contract documentation, rewritten from the reference implementation
(`docs/reference.md` documents the expected checkout layout). Planned files:

| File | Contents | Reference source |
|---|---|---|
| `cnc-layout.md` **(written)** | CnC metadata block, version and acceptance, regions, ring trailers, counters, error log, ordering | `aeron-client/src/main/c/aeron_cnc_file_descriptor.h`, `CncFileDescriptor.java` |
| `error-log-layout.md` **(written)** | the distinct error log's writer half: the composed text, de-duplication, publish ordering, the unrecordable boundary | `aeron-client/src/main/c/concurrent/aeron_distinct_error_log.{c,h}`, `util/aeron_error.c` |
| `term-layout.md` **(written)** | log-buffer metadata, frame descriptor, position math | `aeron-client/src/main/c/concurrent/aeron_logbuffer_descriptor.h` |
| `wire-frames.md` | data/setup/SM/NAK/RTTM frame layouts | `aeron-client/src/main/c/protocol/aeron_udp_protocol.h` |
| `recording-log.md` | 48-byte entries, INVALID bit, 64-byte alignment, append/restore | `aeron-cluster/src/main/java/io/aeron/cluster/RecordingLog.java` |
| `catalog.md` | archive catalog header, mark files, takeover rules | `aeron-archive/src/main/java/io/aeron/archive/Catalog.java`, `aeron-cluster/src/main/java/io/aeron/cluster/service/ClusterMarkFile.java` |

Each file must cite reference sources as upstream `file:line`.
