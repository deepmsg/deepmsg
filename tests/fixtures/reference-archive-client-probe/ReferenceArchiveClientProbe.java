/*
 * The reference's own archive client, talking to an archive that is **not** the
 * reference's — and to a driver that **is**.
 *
 * That pairing is the whole of the archive-vs-Java benchmark's `R-attach` arm, and
 * of its `B0′` gate: the driver is the reference's Java media driver, the archive is
 * deepmsg's, and the client is `io.aeron.archive.client.AeronArchive` out of
 * `aeron-all-1.53.2.jar`. Every other test of our archive has our client on both
 * ends of it, and a protocol two copies of one implementation agree about is not yet
 * a protocol.
 *
 * # The fifth thing, which is why this exists at all
 *
 * Four control operations are the obvious half: `listRecordings`, `startRecording`,
 * `stopRecording`, `startReplay`. The half that matters is what the reference's
 * benchmark harness does **before it sends a single message**:
 *
 *     io.aeron.archive.status.RecordingPos.findCounterIdBySession(counters, sessionId, archiveId)
 *
 * polled in a loop with **no timeout** (`AeronUtil.awaitRecordingStart:277-288`, called
 * from `ArchiveNode.run():97`). That counter is allocated by the *archive* into the
 * *driver's* counters region — ours, into theirs — and it has to carry a type id and a
 * key layout the reference's reader searches for. A build that got either wrong would
 * record perfectly and publish nothing, and the harness would **hang** rather than
 * fail. So this prints the counter it found, and the test asserts on it.
 *
 * # Readings
 *
 * One per line on stdout, and they are the assertion:
 *
 *     ARCHIVE_ID <id>
 *     LISTED <recordingId> <sessionId> <streamId> <originalChannel> <startPosition> <stopPosition>
 *     LISTED_COUNT <n>
 *     SUBSCRIPTION <subscriptionId>
 *     SESSION <publicationSessionId>
 *     PUBLISHED <messages> <position>
 *     COUNTER <counterId> <recordingId>
 *     LIVE <publicationSessionId>
 *     STOP_POSITION <position>
 *     REPLAY_SESSION <replaySessionId>
 *     REPLAYED <fragments> <lastPosition>
 *     DONE
 *
 * `LIVE` is a **handshake**, and it is the whole reason this probe is spawned rather
 * than run: a recording position counter lives only as long as its recording, so a
 * second reader that arrives after the stop is a reader of a freed slot. The probe
 * prints `LIVE` and waits, the test reads the same slot with *its* reader, and then
 * says `go` on stdin.
 *
 * A wait that runs out prints `TIMEOUT <what>` on **stderr** and exits 1, so that the
 * readings stay on stdout and the reason is in the failure.
 *
 * Usage: ReferenceArchiveClientProbe <aeron dir> <control channel> <response channel>
 *            <record channel> <record stream id> <replay channel> <replay stream id>
 *            <deadline ms>
 */

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.status.RecordingPos;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;

import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.archive.codecs.SourceLocation.LOCAL;
import static org.agrona.concurrent.status.CountersReader.NULL_COUNTER_ID;

public class ReferenceArchiveClientProbe
{
    /** How many messages are published into the recording before it is stopped. */
    private static final int MESSAGE_COUNT = 8;

    /** The read limit of a poll, as the harness uses (`AeronUtil.FRAGMENT_LIMIT`). */
    private static final int FRAGMENT_LIMIT = 10;

    private static long deadlineMs;

    public static void main(final String[] args) throws Exception
    {
        if (args.length < 8)
        {
            System.err.println("Usage: ReferenceArchiveClientProbe <aeron dir> <control channel> " +
                "<response channel> <record channel> <record stream id> <replay channel> " +
                "<replay stream id> <deadline ms>");
            System.exit(2);
        }

        final String aeronDir = args[0];
        final String recordChannel = args[3];
        final int recordStreamId = Integer.parseInt(args[4]);
        final String replayChannel = args[5];
        final int replayStreamId = Integer.parseInt(args[6]);
        deadlineMs = System.currentTimeMillis() + Long.parseLong(args[7]);

        // The two channels are arguments and not defaults: the reference's own default
        // response channel is a *dynamic* one (`AeronArchive.java:2779`), and our
        // archive is reached at a port the test picked. Both have to be UDP with a
        // concrete port — a publication on `endpoint=localhost:0` is refused by the
        // driver outright.
        final AeronArchive.Context context = new AeronArchive.Context()
            .aeronDirectoryName(aeronDir)
            .controlRequestChannel(args[1])
            .controlResponseChannel(args[2]);

        try (AeronArchive archive = AeronArchive.connect(context))
        {
            final long archiveId = archive.archiveId();
            System.out.println("ARCHIVE_ID " + archiveId);
            System.out.flush();

            // 1. A listing, decoded by the reference's own recording-descriptor codec.
            final int listed = archive.listRecordings(0, 10, (
                controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp,
                startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength,
                mtuLength, sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) ->
                System.out.println("LISTED " + recordingId + " " + sessionId + " " + streamId + " " +
                    originalChannel + " " + startPosition + " " + stopPosition));
            System.out.println("LISTED_COUNT " + listed);
            System.out.flush();

            final Aeron aeron = archive.context().aeron();
            final ExclusivePublication publication =
                aeron.addExclusivePublication(recordChannel, recordStreamId);
            final int sessionId = publication.sessionId();
            System.out.println("SESSION " + sessionId);
            System.out.flush();

            // ★ The channel carries the publication's **session id**, which is what the
            // reference's own archive node asks for (`ArchiveNode.java:86`): a local
            // source is recorded by naming exactly the publication that is to be
            // recorded, not by subscribing to the channel and taking what turns up.
            final long subscriptionId = archive.startRecording(
                ChannelUri.addSessionId(recordChannel, sessionId), recordStreamId, LOCAL, true);
            System.out.println("SUBSCRIPTION " + subscriptionId);
            System.out.flush();

            final long position = publishAndPoll(publication);

            // 2. ★ The counter, found the way the benchmark harness finds it.
            final CountersReader counters = aeron.countersReader();
            int counterId;
            do
            {
                counterId = RecordingPos.findCounterIdBySession(counters, sessionId, archiveId);
                checkDeadline("the recording position counter");
            }
            while (NULL_COUNTER_ID == counterId);

            final long recordingId = RecordingPos.getRecordingId(counters, counterId);
            System.out.println("COUNTER " + counterId + " " + recordingId);
            System.out.println("LIVE " + sessionId);
            System.out.flush();

            awaitGo();

            archive.stopRecording(subscriptionId);

            // 3. Where it stopped, which the catalog writes when the session is closed
            // — so it is a value that appears rather than one that is there.
            long stopPosition;
            do
            {
                stopPosition = archive.getStopPosition(recordingId);
                checkDeadline("the catalog's stop position for " + recordingId);
            }
            while (NULL_POSITION == stopPosition);

            System.out.println("STOP_POSITION " + stopPosition);
            System.out.flush();

            // 4. A replay, read back by this process on the channel it named.
            final long replaySessionId =
                archive.startReplay(recordingId, 0, Long.MAX_VALUE, replayChannel, replayStreamId);
            System.out.println("REPLAY_SESSION " + replaySessionId);
            System.out.flush();

            final Subscription replay = aeron.addSubscription(
                ChannelUri.addSessionId(replayChannel, (int)replaySessionId), replayStreamId);

            final int[] fragments = { 0 };
            final long[] lastPosition = { NULL_POSITION };

            while (fragments[0] < MESSAGE_COUNT)
            {
                replay.poll((buffer, offset, length, header) ->
                {
                    fragments[0]++;
                    lastPosition[0] = header.position();
                }, FRAGMENT_LIMIT);

                checkDeadline("a replay of recording " + recordingId);
                Thread.sleep(1);
            }

            System.out.println("REPLAYED " + fragments[0] + " " + lastPosition[0]);
            System.out.flush();

            archive.stopReplay(replaySessionId);

            System.out.println("DONE");
            System.out.flush();
        }
    }

    /**
     * Offer every message, waiting for the publication to link, and answer where it
     * got to.
     *
     * The wait is not politeness: a message offered before the archive's subscription
     * has linked is a message nothing records, and the assertion at the far end is
     * that the recording holds what was sent.
     */
    private static long publishAndPoll(final ExclusivePublication publication) throws Exception
    {
        int sent = 0;
        final UnsafeBuffer payload = new UnsafeBuffer(new byte[64]);

        while (sent < MESSAGE_COUNT)
        {
            final long position = publication.offer(payload, 0, payload.capacity());
            if (position > 0)
            {
                sent++;
            }
            else
            {
                checkDeadline("the publication to link");
            }

            Thread.sleep(1);
        }

        System.out.println("PUBLISHED " + sent + " " + publication.position());
        System.out.flush();

        return publication.position();
    }

    /**
     * Wait for the test to say it has read the counter, bounded so that a test which
     * died is a timeout rather than a hang.
     */
    private static void awaitGo() throws Exception
    {
        while (System.in.available() <= 0)
        {
            checkDeadline("the test to read the recording position counter");
            Thread.sleep(1);
        }

        System.in.read();
    }

    /** Fail the run when the deadline has passed, saying what was being waited for. */
    private static void checkDeadline(final String what)
    {
        if (System.currentTimeMillis() >= deadlineMs)
        {
            System.err.println("TIMEOUT waiting for " + what);
            System.exit(1);
        }
    }
}
