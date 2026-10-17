import io.aeron.cluster.service.ClusterMarkFile;
import io.aeron.cluster.codecs.mark.ClusterComponentType;
import org.agrona.concurrent.EpochClock;
import java.io.*;

/** Writes a real cluster mark file with the reference and dumps its first
 *  8 KiB — the header — as the golden the Rust reader must agree with. */
public class GenMark
{
    public static void main(final String[] args) throws Exception
    {
        final File dir = new File(args[0]);
        dir.mkdirs();
        final File file = new File(dir, "cluster-mark.dat");
        file.delete();
        final EpochClock clock = () -> 1_700_000_000_000L;
        try (ClusterMarkFile mark = new ClusterMarkFile(
            file, ClusterComponentType.CONSENSUS_MODULE, 1024 * 1024, clock, 10_000, 4096))
        {
            mark.clusterId(7);
            mark.memberId(3);
            mark.signalReady(1_700_000_000_123L);
            mark.force();
        }
        final byte[] header = new byte[8192];
        try (FileInputStream in = new FileInputStream(file))
        {
            int read = 0;
            while (read < header.length)
            {
                final int n = in.read(header, read, header.length - read);
                if (n < 0) break;
                read += n;
            }
        }
        final File out = new File(args[1]);
        try (FileOutputStream os = new FileOutputStream(out)) { os.write(header); }
        System.out.println("wrote " + out + " length=" + file.length());
    }
}
