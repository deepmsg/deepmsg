/*
 * Opens an archive catalog the way an archive does, and prints what it recovered.
 *
 * The repair this exists to observe happens **inside** `Catalog`'s constructor:
 * it builds the index and then refreshes every record, and a record that is
 * VALID with a null stop position is one whose stop is recovered by reading the
 * recording's segment files (`Catalog.java:228-231` → `refreshAndFixDescriptor`,
 * `:1066-1097`). So "what would the reference recover from this directory" is
 * not a question a reader can answer without writing — it is answered by opening
 * the catalog and then looking at the file.
 *
 * This class is in `io.aeron.archive` because that constructor is package-private
 * (`:152`), as `GenerateCatalog.java` explains at more length.
 *
 * Run as:
 *
 *     javac -proc:none -cp <aeron-all-1.53.2.jar> -d <tmp> RecoverCatalog.java
 *     java --add-exports java.base/jdk.internal.misc=ALL-UNNAMED \
 *          --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
 *          -cp <aeron-all-1.53.2.jar>:<tmp> io.aeron.archive.RecoverCatalog <archive-dir>
 *
 * and it prints one line per record — `<recordingId> <stopPosition>` — after
 * repairing the file in place.
 */

package io.aeron.archive;

import io.aeron.archive.codecs.CatalogHeaderDecoder;
import io.aeron.archive.codecs.RecordingDescriptorDecoder;
import io.aeron.archive.codecs.RecordingDescriptorHeaderDecoder;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

public final class RecoverCatalog
{
    /**
     * The clock the repair reads. Only the stop **timestamp** comes from it —
     * the stop position comes from the segments — so a fixed reading keeps this
     * program's output about positions.
     */
    private static final long NOW = 1_700_000_000_000L;

    private static final long CAPACITY = 1024 * 1024;
    private static final int FILE_SYNC_LEVEL = 1;

    private RecoverCatalog()
    {
    }

    public static void main(final String[] args) throws Exception
    {
        if (1 != args.length)
        {
            System.err.println("usage: RecoverCatalog <archive-dir>");
            System.exit(1);
        }

        final File archiveDir = new File(args[0]);
        final EpochClock clock = () -> NOW;
        final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(4096));

        // Opening *is* the repair. `archiveDirChannel` is null because a test
        // directory has no reason to be forced; a null checksum is "this archive
        // records no checksums", which is the default (`Archive.java:640`).
        try (Catalog catalog = new Catalog(
            archiveDir, null, FILE_SYNC_LEVEL, CAPACITY, clock, null, buffer))
        {
            if (0 == catalog.entryCount())
            {
                // Nothing to print, but the open still happened — and an empty
                // catalog is a legitimate one.
                return;
            }
        }

        final Path path = archiveDir.toPath().resolve(Archive.Configuration.CATALOG_FILE_NAME);
        final byte[] bytes = Files.readAllBytes(path);
        final UnsafeBuffer file = new UnsafeBuffer(bytes);

        int offset = CatalogHeaderDecoder.BLOCK_LENGTH;

        while (offset + RecordingDescriptorHeaderDecoder.BLOCK_LENGTH < bytes.length)
        {
            final RecordingDescriptorHeaderDecoder header = new RecordingDescriptorHeaderDecoder();
            header.wrap(file, offset,
                RecordingDescriptorHeaderDecoder.BLOCK_LENGTH, RecordingDescriptorHeaderDecoder.SCHEMA_VERSION);

            final int length = header.length();
            if (0 == length)
            {
                break;
            }

            final int body = offset + RecordingDescriptorHeaderDecoder.BLOCK_LENGTH;
            final RecordingDescriptorDecoder descriptor = new RecordingDescriptorDecoder();
            descriptor.wrap(file, body,
                RecordingDescriptorDecoder.BLOCK_LENGTH, RecordingDescriptorDecoder.SCHEMA_VERSION);

            System.out.println(descriptor.recordingId() + " " + descriptor.stopPosition());

            offset = body + length;
        }
    }
}
