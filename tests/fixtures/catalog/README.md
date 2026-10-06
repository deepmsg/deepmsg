# Archive catalog golden

One archive catalog, `archive.catalog`, written by the **reference's own**
`Catalog`, with the **reference's own** reading of every field in it beside it in
`catalog.tsv`.

The SBE golden fixtures next door pin the catalog's three messages
(`CatalogHeader` 20, `RecordingDescriptorHeader` 21, `RecordingDescriptor` 22)
field by field. What they cannot say is anything about the **file**: where the
first record is, how a record's `length` relates to the frame the alignment
produces, and that the records carry no SBE message header at all — the record
header *is* the framing. Those facts only exist in a whole file.

| | |
|---|---|
| Written by | `GenerateCatalog.java`, from `aeron-all-1.53.2.jar` |
| Fixtures | `archive.catalog` (1 MiB, 3 recordings), `catalog.tsv` (66 readings) |
| Captured | 2026-10-06 |
| Read by | `crates/archive/tests/catalog.rs` (no reference needed) and, next, `tests/interop/catalog_reference.rs` |

## The files

`archive.catalog` — what `io.aeron.archive.Catalog` wrote, with no edit of any
kind. Its header says version 3.1.0, length 32, `nextRecordingId` 3 and
**alignment 64**, and its three records begin at 32, 288 and 480.

`catalog.tsv` — the reference's reading of it, `key<TAB>field<TAB>value`, where
`key` is `header`, `file` or `recordN`. Every value is what the reference's own
decoders **answer** for that file — not what the generator intended to write.
Two of the numbers are the ones a reader cannot compute: each record's
`location`, and the `length` in its header.

## Regenerating

    JAR=../../aeron/aeron-all/build/libs/aeron-all-1.53.2.jar
    javac -proc:none -cp "$JAR" -d /tmp/catgen GenerateCatalog.java
    java --add-exports java.base/jdk.internal.misc=ALL-UNNAMED \
         --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
         -cp "$JAR:/tmp/catgen" io.aeron.archive.GenerateCatalog .

The two `--add-*` flags are Agrona's on a modern JDK, not this program's; the
same two are `deepmsg_tests::driver::AGRONA_JVM_ARGS`, which is what the interop
test runs `ArchiveTool` with.

**Unlike the mark file's golden, this one is byte-identical across runs**: no
pid, no clock, nothing that depends on the process. `git status` after a re-run
shows nothing, which is the cheapest check that the generator is deterministic.

## Two choices, both deliberate

**Every recording is finished** — a real stop position and stop timestamp rather
than `NULL_POSITION`. A record that is `VALID` with a null stop position is one
the reference tries to *repair* on open, by reading the recording's **segment
files** (`refreshAndFixDescriptor`, `Catalog.java:1066`), and this fixture has no
segments: it would fail for a reason that has nothing to do with the catalog's
layout. What a catalog of *live* recordings looks like is the archive's question,
not a format fixture's.

**The three recordings are different lengths** — the descriptor's tail is three
variable strings — so a reader that stepped by one record's frame for all of them
would land somewhere that is not a record. The frame lengths are 256, 192 and
320 bytes; the recorded `location`s are what say so.

`GenerateCatalog.java` is in `package io.aeron.archive` for the same reason
`GenerateMarkFile.java` is: the constructor that creates a catalog is
package-private (`Catalog.java:152`), as is `addNewRecording` (`:407`).
