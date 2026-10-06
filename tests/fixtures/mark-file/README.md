# Archive mark file golden

One archive mark file, `archive-mark.dat`, written by the **reference's own**
`ArchiveMarkFile`, with the **reference's own** reading of every field beside
it in `mark-file.tsv`.

The SBE golden fixtures next door pin the mark file's *header message* (schema
100, template 200) field by field. What they cannot say is anything about the
**file** around it: where the error buffer begins, how long the file is, and
that a reader which finds those two by different arithmetic than the writer's
ends up somewhere else. That is what this golden is for, and it is a whole file
rather than a message because that is the only shape in which those facts exist.

| | |
|---|---|
| Written by | `GenerateMarkFile.java`, from `aeron-all-1.53.2.jar` |
| Fixtures | `archive-mark.dat` (1 056 768 bytes), `mark-file.tsv` (19 readings) |
| Captured | 2026-10-06 |
| Read by | `crates/archive/tests/mark_file.rs` (no reference needed) and `tests/interop/mark_file_reference.rs` (both readers, live) |

## The files

`archive-mark.dat` — what `io.aeron.archive.ArchiveMarkFile` wrote, with no
edit of any kind. Its length is the reference's own
`align(HEADER_LENGTH + errorBufferLength, pageSize)` = 8192 + 1 MiB.

`mark-file.tsv` — the reference's reading of it: `name<TAB>value`, one per
line, comment lines starting with `#`. Every value is what the reference's
`ArchiveMarkFile` **answers** for that file, taken through the same decoder a
live archive uses — not what the generator intended to write. That is the
difference between a golden and a hope, and it is the same rule
`fixtures/sbe/README.md` states for the SBE readings.

## Regenerating

    JAR=../../aeron/aeron-all/build/libs/aeron-all-1.53.2.jar
    javac -proc:none -cp "$JAR" -d /tmp/markgen GenerateMarkFile.java
    java --add-exports java.base/jdk.internal.misc=ALL-UNNAMED \
         --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
         -cp "$JAR:/tmp/markgen" io.aeron.archive.GenerateMarkFile .

The two `--add-*` flags are Agrona's on a modern JDK, not this program's; the
same two are passed by `deepmsg_tests::driver::AGRONA_JVM_ARGS`, which is what
the interop test runs `ArchiveTool` with.

**The bytes are not identical across runs, and cannot be.** The reference
stamps the pid from `SystemUtil.getPid()` when it creates the file
(`ArchiveMarkFile.java:196`), and there is no parameter for it. So a
regeneration changes the pid — and therefore the file's digest — which is why
`mark-file.tsv` records the pid the run had rather than the generator pretending
to a fixed one. Everything else in the file is fixed: the timestamps, the two
ids, the channels, and the two distinct errors written into the error buffer
through the reference's own `DistinctErrorLog`.

`GenerateMarkFile.java` is in `package io.aeron.archive` for one reason: the
constructor that **creates** a mark file is package-private (`:110`; the public
ones open an existing file), and package access is by name — the classpath does
not seal it.
