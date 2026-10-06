/*
 * Writes the archive catalog golden: one `archive.catalog`, produced by the
 * reference's own `Catalog`, with the **reference's own** reading of every field
 * in it beside it.
 *
 * The SBE goldens next door already pin the catalog's three messages
 * (`CatalogHeader` 20, `RecordingDescriptorHeader` 21, `RecordingDescriptor` 22)
 * field by field. What they cannot say is anything about the **file**: that a
 * new catalog's first record is at offset 32 and not at the deprecated 1024, how
 * a record's `length` relates to the frame the alignment produces, and that the
 * records carry no SBE message header at all — the record header *is* the
 * framing. Those facts only exist in a whole file.
 *
 * This class is in `io.aeron.archive` on purpose: the constructor that creates a
 * catalog is package-private (`Catalog.java:152`), as is `addNewRecording`
 * (`:407`), and package access is by name — the classpath does not seal it.
 *
 * Run as:
 *
 *     javac -proc:none -cp <aeron-all-1.53.2.jar> -d <tmp> GenerateCatalog.java
 *     java --add-exports java.base/jdk.internal.misc=ALL-UNNAMED \
 *          --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
 *          -cp <aeron-all-1.53.2.jar>:<tmp> io.aeron.archive.GenerateCatalog <out-dir>
 *
 * and it writes `archive.catalog` and `catalog.tsv` into `<out-dir>`.
 *
 * Two choices worth stating:
 *
 *   - every recording is **finished** — a real stop position and stop timestamp
 *     rather than `NULL_POSITION`. A record that is VALID with a null stop
 *     position is one the reference tries to *repair* by reading the recording's
 *     segment files (`refreshAndFixDescriptor`, `:1066`), and this fixture has no
 *     segments: it would fail for a reason that has nothing to do with the
 *     catalog's layout. What a catalog of *live* recordings looks like is a
 *     question for the archive that is writing them, not for a format fixture;
 *   - the three recordings are deliberately different lengths — the descriptor's
 *     tail is three variable strings — so a reader that steps by one record's
 *     frame for all of them lands somewhere that is not a record.
 */

package io.aeron.archive;

import io.aeron.archive.codecs.RecordingDescriptorDecoder;
import io.aeron.archive.codecs.RecordingDescriptorHeaderDecoder;
import io.aeron.archive.codecs.mark.MessageHeaderDecoder;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public final class GenerateCatalog
{
    /** Fixed, so two runs are byte-identical. */
    private static final long START_TIMESTAMP = 1_700_000_000_000L;
    private static final EpochClock CLOCK = () -> START_TIMESTAMP;

    private static final long CAPACITY = 1024 * 1024;
    private static final int FILE_SYNC_LEVEL = 1;

    private GenerateCatalog()
    {
    }

    public static void main(final String[] args) throws Exception
    {
        if (1 != args.length)
        {
            System.err.println("usage: GenerateCatalog <out-dir>");
            System.exit(1);
        }

        final Path out = Paths.get(args[0]);
        Files.createDirectories(out);
        Files.deleteIfExists(out.resolve(Archive.Configuration.CATALOG_FILE_NAME));

        final File archiveDir = out.toFile();
        final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(4096));

        final Catalog catalog = new Catalog(
            archiveDir,
            null,
            FILE_SYNC_LEVEL,
            CAPACITY,
            CLOCK,
            null,
            buffer);

        try
        {
            // Three finished recordings. The strings differ in length on
            // purpose: a reader that steps by one record's frame for all of them
            // lands somewhere that is not a record.
            catalog.addNewRecording(
                0, 4096, START_TIMESTAMP, START_TIMESTAMP + 1000, 7,
                64 * 1024 * 1024, 64 * 1024, 1408, 42, 1001,
                "aeron:udp?endpoint=localhost:9000",
                "aeron:udp?endpoint=localhost:9000|sparse=true",
                "aeron:udp?endpoint=localhost:8000");

            catalog.addNewRecording(
                4096, 8192, START_TIMESTAMP + 2000, START_TIMESTAMP + 2500, 7,
                64 * 1024 * 1024, 64 * 1024, 1408, 43, 1002,
                "aeron:ipc",
                "aeron:ipc|sparse=true",
                "aeron:ipc");

            catalog.addNewRecording(
                8192, 900_000, START_TIMESTAMP + 3000, START_TIMESTAMP + 9999, 11,
                16 * 1024 * 1024, 128 * 1024, 1408, 44, 1003,
                "aeron:udp?endpoint=localhost:9002|reliable=false",
                "aeron:udp?endpoint=localhost:9002|reliable=false|sparse=true",
                "aeron:udp?endpoint=localhost:8002");
        }
        finally
        {
            catalog.close();
        }

        // Read back with plain file IO and the reference's own decoders: opening
        // the catalog again would take it through `refreshCatalog`, and this is
        // about what is in the bytes.
        final byte[] bytes = Files.readAllBytes(out.resolve(Archive.Configuration.CATALOG_FILE_NAME));
        final UnsafeBuffer file = new UnsafeBuffer(bytes);

        try (PrintWriter tsv = new PrintWriter(
            Files.newBufferedWriter(out.resolve("catalog.tsv"), StandardCharsets.UTF_8)))
        {
            tsv.println("# the reference's own reading of archive.catalog");
            tsv.println("# written by GenerateCatalog; `location` is the byte offset of the record");

            final CatalogHeaderReader header = new CatalogHeaderReader();
            tsv.println("header\tversion\t" + header.version(file));
            tsv.println("header\tlength\t" + header.length(file));
            tsv.println("header\tnextRecordingId\t" + header.nextRecordingId(file));
            tsv.println("header\talignment\t" + header.alignment(file));

            final int headerLength = header.blockLength();
            int offset = headerLength;
            int records = 0;

            while (offset + RecordingDescriptorHeaderDecoder.BLOCK_LENGTH <= bytes.length)
            {
                final RecordingDescriptorHeaderDecoder recordHeader =
                    new RecordingDescriptorHeaderDecoder();
                recordHeader.wrap(file, offset,
                    RecordingDescriptorHeaderDecoder.BLOCK_LENGTH, RecordingDescriptorHeaderDecoder.SCHEMA_VERSION);

                final int length = recordHeader.length();
                if (0 == length)
                {
                    break;
                }

                final int bodyOffset = offset + RecordingDescriptorHeaderDecoder.BLOCK_LENGTH;
                final RecordingDescriptorDecoder descriptor = new RecordingDescriptorDecoder();
                descriptor.wrap(file, bodyOffset,
                    RecordingDescriptorDecoder.BLOCK_LENGTH, RecordingDescriptorDecoder.SCHEMA_VERSION);

                final String where = "record" + records;
                tsv.println(where + "\tlocation\t" + offset);
                tsv.println(where + "\tlength\t" + length);
                tsv.println(where + "\tstate\t" + recordHeader.state());
                tsv.println(where + "\tchecksum\t" + recordHeader.checksum());
                tsv.println(where + "\trecordingId\t" + descriptor.recordingId());
                tsv.println(where + "\tcontrolSessionId\t" + descriptor.controlSessionId());
                tsv.println(where + "\tcorrelationId\t" + descriptor.correlationId());
                tsv.println(where + "\tstartTimestamp\t" + descriptor.startTimestamp());
                tsv.println(where + "\tstopTimestamp\t" + descriptor.stopTimestamp());
                tsv.println(where + "\tstartPosition\t" + descriptor.startPosition());
                tsv.println(where + "\tstopPosition\t" + descriptor.stopPosition());
                tsv.println(where + "\tinitialTermId\t" + descriptor.initialTermId());
                tsv.println(where + "\tsegmentFileLength\t" + descriptor.segmentFileLength());
                tsv.println(where + "\ttermBufferLength\t" + descriptor.termBufferLength());
                tsv.println(where + "\tmtuLength\t" + descriptor.mtuLength());
                tsv.println(where + "\tsessionId\t" + descriptor.sessionId());
                tsv.println(where + "\tstreamId\t" + descriptor.streamId());
                tsv.println(where + "\tstrippedChannel\t" + descriptor.strippedChannel());
                tsv.println(where + "\toriginalChannel\t" + descriptor.originalChannel());
                tsv.println(where + "\tsourceIdentity\t" + descriptor.sourceIdentity());

                offset = bodyOffset + length;
                records++;
            }

            tsv.println("file\trecords\t" + records);
            tsv.println("file\tlength\t" + bytes.length);
        }

        System.out.println("wrote " + out.resolve(Archive.Configuration.CATALOG_FILE_NAME) +
            " (" + Files.size(out.resolve(Archive.Configuration.CATALOG_FILE_NAME)) + " bytes)");
    }

    /**
     * The catalog header, read through the reference's own decoder. A tiny class
     * rather than four inline calls because the header is not an SBE *message*
     * on the wire — it has no message header — so it is wrapped at offset 0 with
     * the block length the generated codec declares.
     */
    private static final class CatalogHeaderReader
    {
        private static final io.aeron.archive.codecs.CatalogHeaderDecoder DECODER =
            new io.aeron.archive.codecs.CatalogHeaderDecoder();

        static int blockLength()
        {
            return io.aeron.archive.codecs.CatalogHeaderDecoder.BLOCK_LENGTH;
        }

        private io.aeron.archive.codecs.CatalogHeaderDecoder wrap(final UnsafeBuffer buffer)
        {
            return DECODER.wrap(
                buffer, 0,
                io.aeron.archive.codecs.CatalogHeaderDecoder.BLOCK_LENGTH,
                io.aeron.archive.codecs.CatalogHeaderDecoder.SCHEMA_VERSION);
        }

        int version(final UnsafeBuffer buffer)
        {
            return wrap(buffer).version();
        }

        int length(final UnsafeBuffer buffer)
        {
            return wrap(buffer).length();
        }

        long nextRecordingId(final UnsafeBuffer buffer)
        {
            return wrap(buffer).nextRecordingId();
        }

        int alignment(final UnsafeBuffer buffer)
        {
            return wrap(buffer).alignment();
        }
    }
}
