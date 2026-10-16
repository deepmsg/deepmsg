/*
 * The recording events, read by the reference's own adapter.
 *
 * `io.aeron.archive.client.RecordingEventsAdapter` is the reader the archive's
 * events exist for: it decodes the three SBE templates and dispatches them to a
 * `RecordingEventsListener`. It is also what the reference's own benchmark
 * harness uses as a **pacemaker** — `LiveRecordingMessageTransceiver` consumes
 * only what the archive has recorded, and the events are how it hears that.
 *
 * Every other test of these events is this build's encoder talking to this
 * build's decoder, and two copies of one implementation agreeing about a wire
 * format is not yet a wire format. This is the other half: the reference's bytes
 * reader, decoding bytes we wrote, with the schema's own generated code on the
 * far side.
 *
 * The readings are one per line on stdout and they are the assertion:
 *
 *     READY
 *     STARTED <recordingId> <startPosition> <sessionId> <streamId> <channel> <sourceIdentity>
 *     PROGRESS <recordingId> <startPosition> <position>
 *     STOPPED <recordingId> <startPosition> <stopPosition>
 *     DONE <event count>
 *
 * `READY` goes out once the subscription exists, which is the handshake: an IPC
 * publication that has no subscriber drops what it offers, so the recording must
 * not be started until the reader is there. After `READY` this polls until it
 * sees a stop or the deadline passes, and exits **0 either way** — a probe that
 * saw nothing is a reading too, and the caller is what decides what it means.
 *
 * Usage: <aeron dir> <channel> <stream id> <deadline ms>
 */

import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.archive.client.RecordingEventsAdapter;
import io.aeron.archive.client.RecordingEventsListener;

public class RecordingEventsProbe
{
    private static final int FRAGMENT_LIMIT = 10;
    private static final long IDLE_SLEEP_MS = 1;

    /** What the listener has seen, which is also where the count lives. */
    static final class Listener implements RecordingEventsListener
    {
        int count;
        boolean stopped;

        public void onStart(
            final long recordingId,
            final long startPosition,
            final int sessionId,
            final int streamId,
            final String channel,
            final String sourceIdentity)
        {
            count++;
            System.out.println("STARTED " + recordingId + " " + startPosition + " " + sessionId +
                " " + streamId + " " + channel + " " + sourceIdentity);
            System.out.flush();
        }

        public void onProgress(final long recordingId, final long startPosition, final long position)
        {
            count++;
            System.out.println("PROGRESS " + recordingId + " " + startPosition + " " + position);
            System.out.flush();
        }

        public void onStop(final long recordingId, final long startPosition, final long stopPosition)
        {
            count++;
            stopped = true;
            System.out.println("STOPPED " + recordingId + " " + startPosition + " " + stopPosition);
            System.out.flush();
        }
    }

    public static void main(final String[] args) throws Exception
    {
        if (args.length < 4)
        {
            System.err.println("Usage: RecordingEventsProbe <aeron dir> <channel> <stream id> <deadline ms>");
            System.exit(2);
        }

        final String aeronDir = args[0];
        final String channel = args[1];
        final int streamId = Integer.parseInt(args[2]);
        final long deadlineMs = System.currentTimeMillis() + Long.parseLong(args[3]);

        final Listener listener = new Listener();

        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir)))
        {
            final Subscription subscription = aeron.addSubscription(channel, streamId);
            final RecordingEventsAdapter adapter =
                new RecordingEventsAdapter(listener, subscription, FRAGMENT_LIMIT);

            System.out.println("READY");
            System.out.flush();

            while (!listener.stopped && System.currentTimeMillis() < deadlineMs)
            {
                if (0 == adapter.poll())
                {
                    Thread.sleep(IDLE_SLEEP_MS);
                }
            }

            System.out.println("DONE " + listener.count);
            System.out.flush();
        }
    }
}
