/*
 * The smallest thing that fails: a real archive, a real client, and one
 * connect — over IPC, with the response channel a `control-mode=response`.
 *
 * P2-0c measured this failing in two independent suites and both languages. The
 * driver's own response-channel layer was then measured and found
 * indistinguishable from the reference's across seven shapes, so the difference
 * has to be in the one ingredient those shapes left out: the archive. This is
 * the archive, minus everything that is not the connect.
 *
 * It is `ArchiveResponseClientTest.shouldConnectUsingLocalIpcResponseChannels`
 * with the test framework taken off: the archive is launched in this process
 * against whatever driver the aeron directory belongs to, the client is built
 * the same way that test builds it (`localControlChannel` /
 * `localControlStreamId` are the archive's own, which is what makes this the
 * pair the client is meant to reach), and the only thing asked is whether the
 * connect comes back.
 *
 * The readings are on stdout, one per line, because the answer is a comparison
 * between two runs of this program against two drivers and not any single
 * number in it. It exits 0 either way: a failed connect is the measurement.
 *
 *     ARCHIVE localControlChannel=<channel> localControlStreamId=<id>
 *     CONNECT ok
 *     REQUEST_CHANNEL <the request channel, which must carry response-correlation-id>
 *     CONNECT failed:<exception>
 *
 * Usage: <aeron dir> <archive dir> [control request channel] [control request stream id]
 */

import java.io.File;

import io.aeron.Aeron;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;

public class ArchiveConnectProbe
{
    public static void main(final String[] args) throws Exception
    {
        if (args.length < 2)
        {
            System.err.println("Usage: ArchiveConnectProbe <aeron dir> <archive dir> " +
                "[control request channel] [control request stream id]");
            System.exit(2);
        }

        final String aeronDir = args[0];
        final File archiveDir = new File(args[1]);

        final Archive.Context archiveCtx = new Archive.Context()
            .aeronDirectoryName(aeronDir)
            .archiveDir(archiveDir)
            // `aeron:ipc` is refused outright — "must be UDP media" — so the
            // pair this test is about is a **UDP** control request channel and an
            // **IPC** control response channel. That mix is the shape, and it is
            // the one thing none of the driver-only shapes tried.
            .controlChannel("aeron:udp?endpoint=localhost:0")
            .replicationChannel("aeron:udp?endpoint=localhost:0")
            .deleteArchiveOnStart(true)
            .threadingMode(ArchiveThreadingMode.SHARED);

        try (Archive archive = Archive.launch(archiveCtx))
        {
            final String requestChannel = args.length > 2 ?
                args[2] : archive.context().localControlChannel();
            final int requestStreamId = args.length > 3 ?
                Integer.parseInt(args[3]) : archive.context().localControlStreamId();

            System.out.println(
                "ARCHIVE localControlChannel=" + archive.context().localControlChannel() +
                " localControlStreamId=" + archive.context().localControlStreamId());
            System.out.flush();

            final AeronArchive.Context clientCtx = new AeronArchive.Context()
                .aeronDirectoryName(aeronDir)
                .controlRequestChannel(requestChannel)
                .controlRequestStreamId(requestStreamId)
                .controlResponseChannel("aeron:ipc?control-mode=response");

            try (AeronArchive ignored = AeronArchive.connect(clientCtx))
            {
                System.out.println("CONNECT ok");
                System.out.println("REQUEST_CHANNEL " + clientCtx.controlRequestChannel());
            }
            catch (final Throwable ex)
            {
                System.out.println("CONNECT failed:" + ex);
            }

            System.out.flush();
        }
    }
}
