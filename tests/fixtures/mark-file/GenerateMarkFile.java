/*
 * Writes the archive mark file golden: one `archive-mark.dat`, produced by the
 * reference's own `ArchiveMarkFile`, with the **reference's own** reading of
 * every field beside it.
 *
 * The SBE golden fixtures next door already pin the header *message* (schema
 * 100, template 200) field by field. What they cannot say is anything about the
 * file around it: where the error buffer begins, how long the file is, and that
 * a reader which finds those two by different arithmetic than the writer's ends
 * up somewhere else. That is what this golden is for, and it is a whole file
 * rather than a message because that is the only shape in which those facts
 * exist.
 *
 * This class is in `io.aeron.archive` on purpose. The constructor that *creates*
 * a mark file is package-private (`ArchiveMarkFile.java:110`; the public ones
 * open an existing file), so a program that wants the reference to write one
 * has to be in the same package — package access is by name, and the classpath
 * does not seal it.
 *
 * Run as:
 *
 *     javac -cp <aeron-all-1.53.2.jar> -d <tmp> GenerateMarkFile.java
 *     java -cp <aeron-all-1.53.2.jar>:<tmp> io.aeron.archive.GenerateMarkFile <out-dir>
 *
 * and it writes `archive-mark.dat` and `mark-file.tsv` into `<out-dir>`.
 *
 * Which values are chosen, and why they are those:
 *
 *   - every field the archive can be asked to write is given a value a reader
 *     can tell from the field next to it (the archive id is not the control
 *     stream id, the two channels differ in length as well as in text), so a
 *     reader that lands on the wrong offset disagrees rather than agreeing by
 *     accident;
 *   - the timestamps are fixed, so two runs produce the same bytes except for
 *     the one thing that cannot be fixed from here: the pid, which the
 *     reference stamps from `SystemUtil.getPid()`. The tsv records the pid the
 *     run had, which is what makes it readable rather than a mystery;
 *   - the error buffer gets two distinct errors through the reference's own
 *     `DistinctErrorLog`, which is the same class `Archive` wires to the mark
 *     file's buffer — so `ArchiveTool errors` reading this file has something
 *     to read, and the format on both sides is the reference's.
 */

package io.aeron.archive;

import org.agrona.BitUtil;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.errors.DistinctErrorLog;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public final class GenerateMarkFile
{
    private static final int ERROR_BUFFER_LENGTH = 1024 * 1024;
    private static final int PAGE_SIZE = 4096;

    /** Fixed, so that two runs differ only in the pid. */
    private static final long START_TIMESTAMP = 1_700_000_000_000L;
    private static final long ACTIVITY_TIMESTAMP = 1_700_000_001_000L;

    private static final long ARCHIVE_ID = 0x1122_3344_5566_7788L;
    private static final int CONTROL_STREAM_ID = 1001;
    private static final int LOCAL_CONTROL_STREAM_ID = 1002;
    private static final int EVENTS_STREAM_ID = 1003;
    private static final String CONTROL_CHANNEL = "aeron:udp?endpoint=localhost:8010";
    private static final String LOCAL_CONTROL_CHANNEL = "aeron:ipc";
    private static final String EVENTS_CHANNEL = "aeron:udp?endpoint=localhost:8011";
    private static final String AERON_DIRECTORY = "/dev/shm/aeron-mark-golden";

    private GenerateMarkFile()
    {
    }

    public static void main(final String[] args) throws Exception
    {
        if (1 != args.length)
        {
            System.err.println("usage: GenerateMarkFile <out-dir>");
            System.exit(1);
        }

        final Path out = Paths.get(args[0]);
        Files.createDirectories(out);

        // Agrona's `mapNewOrExistingMarkFile` refuses a file that is **active**
        // — a live archive is not something a second process may recreate — so
        // a re-run has to start from nothing. That refusal is a feature of the
        // reference, not an obstacle here: this generator *is* the archive as
        // far as the file is concerned.
        Files.deleteIfExists(out.resolve(ArchiveMarkFile.FILENAME));

        final String version = GenerateMarkFile.class.getPackage().getImplementationVersion();
        final EpochClock clock = () -> ACTIVITY_TIMESTAMP;
        final File file = out.resolve(ArchiveMarkFile.FILENAME).toFile();

        // The same arithmetic `ArchiveMarkFile.alignedTotalFileLength` does
        // (`:450-471`), which is private to a `Context` the generator has no
        // reason to build. It is the one number here that is computed rather
        // than read back, and it is computed the same way.
        final int totalFileLength =
            BitUtil.align(ArchiveMarkFile.HEADER_LENGTH + ERROR_BUFFER_LENGTH, PAGE_SIZE);

        final ArchiveMarkFile markFile = new ArchiveMarkFile(
            file, totalFileLength, ERROR_BUFFER_LENGTH, clock, 10_000);

        try
        {
            // The context the creating constructor does not take is the one the
            // `Context` constructor would have encoded from; the fields it
            // would have set are set here through the same encoder, with values
            // a reader can tell apart.
            final UnsafeBuffer buffer = markFile.buffer();
            final io.aeron.archive.codecs.mark.MarkFileHeaderEncoder encoder =
                new io.aeron.archive.codecs.mark.MarkFileHeaderEncoder();
            encoder.wrapAndApplyHeader(buffer, 0, new io.aeron.archive.codecs.mark.MessageHeaderEncoder())
                .startTimestamp(START_TIMESTAMP)
                .controlStreamId(CONTROL_STREAM_ID)
                .localControlStreamId(LOCAL_CONTROL_STREAM_ID)
                .eventsStreamId(EVENTS_STREAM_ID)
                .headerLength(ArchiveMarkFile.HEADER_LENGTH)
                .errorBufferLength(ERROR_BUFFER_LENGTH)
                .archiveId(ARCHIVE_ID)
                .controlChannel(CONTROL_CHANNEL)
                .localControlChannel(LOCAL_CONTROL_CHANNEL)
                .eventsChannel(EVENTS_CHANNEL)
                .aeronDirectory(AERON_DIRECTORY);

            // Two distinct errors, through the class `Archive` wires to this
            // very buffer (`Archive.java:1331-1332`).
            final DistinctErrorLog errorLog =
                new DistinctErrorLog(markFile.errorBuffer(), clock, StandardCharsets.US_ASCII);
            errorLog.record(new RuntimeException("the first distinct error"));
            errorLog.record(new RuntimeException("the second distinct error"));
            errorLog.record(new RuntimeException("the first distinct error"));

            markFile.signalReady(ACTIVITY_TIMESTAMP);

            try (PrintWriter tsv = new PrintWriter(
                Files.newBufferedWriter(out.resolve("mark-file.tsv"), StandardCharsets.UTF_8)))
            {
                tsv.println("# the reference's own reading of archive-mark.dat");
                tsv.println("# written by GenerateMarkFile, from " +
                    (null == version ? "the classpath's aeron-archive" : version));
                tsv.println();

                // The **opening** constructor, which is the public one and the
                // one `ArchiveTool` uses (`:212`, over `openExistingMarkFile`):
                // the constructor above creates, and refuses a file that is
                // already there and active — which this one now is.
                final ArchiveMarkFile reader = new ArchiveMarkFile(
                    out.toFile(), ArchiveMarkFile.FILENAME, clock, 10_000, (ignored) -> { });

                try
                {
                    line(tsv, "pid", reader.decoder().pid());
                    line(tsv, "version", reader.decoder().version());
                    line(tsv, "activityTimestamp", reader.decoder().activityTimestamp());
                    line(tsv, "startTimestamp", reader.decoder().startTimestamp());
                    line(tsv, "controlStreamId", reader.decoder().controlStreamId());
                    line(tsv, "localControlStreamId", reader.decoder().localControlStreamId());
                    line(tsv, "eventsStreamId", reader.decoder().eventsStreamId());
                    line(tsv, "headerLength", reader.decoder().headerLength());
                    line(tsv, "errorBufferLength", reader.decoder().errorBufferLength());
                    line(tsv, "archiveId", reader.decoder().archiveId());
                    line(tsv, "semanticVersion", ArchiveMarkFile.SEMANTIC_VERSION);
                    line(tsv, "majorVersion", ArchiveMarkFile.MAJOR_VERSION);
                    line(tsv, "fileName", ArchiveMarkFile.FILENAME);
                    line(tsv, "linkFileName", ArchiveMarkFile.LINK_FILENAME);
                    line(tsv, "fileLength", out.resolve(ArchiveMarkFile.FILENAME).toFile().length());
                    line(tsv, "controlChannel", reader.decoder().controlChannel());
                    line(tsv, "localControlChannel", reader.decoder().localControlChannel());
                    line(tsv, "eventsChannel", reader.decoder().eventsChannel());
                    line(tsv, "aeronDirectory", reader.decoder().aeronDirectory());
                }
                finally
                {
                    reader.close();
                }
            }
        }
        finally
        {
            markFile.close();
        }

        System.out.println("wrote " + out.resolve(ArchiveMarkFile.FILENAME) +
            " (" + Files.size(out.resolve(ArchiveMarkFile.FILENAME)) + " bytes)");

        // Printed rather than written into the tsv, which the reading above
        // owns: this is the file's digest, and a reader of this repository
        // wants it next to the file it describes.
        System.out.println("sha256 " + sha256(out.resolve(ArchiveMarkFile.FILENAME)));
    }

    private static void line(final PrintWriter tsv, final String name, final Object value)
    {
        tsv.println(name + "\t" + value);
    }

    private static String sha256(final Path path) throws IOException
    {
        try
        {
            final java.security.MessageDigest digest = java.security.MessageDigest.getInstance("SHA-256");
            final byte[] bytes = digest.digest(Files.readAllBytes(path));
            final StringBuilder builder = new StringBuilder();

            for (final byte b : bytes)
            {
                builder.append(String.format("%02x", b));
            }

            return builder.toString();
        }
        catch (final Exception ex)
        {
            throw new IOException(ex);
        }
    }

}
