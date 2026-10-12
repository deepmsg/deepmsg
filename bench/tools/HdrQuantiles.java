/*
 * Read one or more `.hdr` files and print the quantiles a batch's tables are
 * built from, in nanoseconds and nothing else.
 *
 *   java -cp <harness jar> HdrQuantiles.java [--skip N] [LABEL=PATH ...]
 *
 * The histogram files this reads are written by the reference's
 * `PersistedHistogram` — the same HdrHistogram interval-log format the harness
 * writes and the same one this build writes (`crates/bench/src/loadtest/
 * result.rs`), which is what makes a number from one client readable beside a
 * number from the other. The classes that decode it come out of the harness's
 * own jar, so this needs no build step of its own.
 *
 * Several intervals in one file are summed; `--skip N` drops the first N
 * intervals, which is how a warmup chunk is left out.
 *
 * The row is one line per file, tab-separated:
 *
 *   label  count  min  p10  p20  p40  p50  p60  p80  p90  p99  max  mean  e_lt_p99
 *
 * `e_lt_p99` is E[X | X < p99]. Cross-arm comparisons in these batches are read
 * in that column and in p40/p50, because a mean is a place a single stalled
 * iteration can drag anywhere (the reference's node stops for ~8 ms in every
 * cell, which is a whole microsecond of mean and nothing of a p50).
 *
 * p20 and p80 are here and p99.9 is not: the gaps these batches look for live
 * in the body of the distribution.
 */
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogReader;
import org.HdrHistogram.RecordedValuesIterator;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public final class HdrQuantiles
{
    private static final double[] QS = { 10, 20, 40, 50, 60, 80, 90, 99 };

    private static List<Histogram> chunks(final File f) throws Exception
    {
        final List<Histogram> out = new ArrayList<>();
        try (HistogramLogReader reader = new HistogramLogReader(f))
        {
            while (reader.hasNext())
            {
                out.add((Histogram)reader.nextIntervalHistogram());
            }
        }
        return out;
    }

    private static Histogram read(final File f, final int skip) throws Exception
    {
        final List<Histogram> all = chunks(f);
        if (all.isEmpty() || skip >= all.size())
        {
            throw new IllegalStateException(f + ": only " + all.size() + " intervals, cannot skip " + skip);
        }
        Histogram total = null;
        for (int i = skip; i < all.size(); i++)
        {
            if (null == total)
            {
                total = all.get(i).copy();
            }
            else
            {
                total.add(all.get(i));
            }
        }
        return total;
    }

    public static void main(final String[] args) throws Exception
    {
        int skip = 0;
        final List<String[]> files = new ArrayList<>();
        for (int i = 0; i < args.length; i++)
        {
            if (args[i].equals("--skip"))
            {
                skip = Integer.parseInt(args[++i]);
            }
            else
            {
                final int eq = args[i].indexOf('=');
                files.add(new String[]{ args[i].substring(0, eq), args[i].substring(eq + 1) });
            }
        }

        System.out.print("label\tcount\tmin");
        for (final double q : QS)
        {
            System.out.printf(Locale.US, "\tp%d", (int)q);
        }
        System.out.println("\tmax\tmean\te_lt_p99");

        for (final String[] f : files)
        {
            final Histogram h = read(new File(f[1]), skip);
            System.out.printf(Locale.US, "%s\t%d\t%d", f[0], h.getTotalCount(), h.getMinValue());
            for (final double q : QS)
            {
                System.out.printf(Locale.US, "\t%d", h.getValueAtPercentile(q));
            }
            System.out.printf(Locale.US, "\t%d\t%.1f\t%.1f%n",
                h.getMaxValue(), h.getMean(), meanBelow(h, h.getValueAtPercentile(99)));
        }
    }

    /** E[X | X < ceiling], over the recorded values. */
    private static double meanBelow(final Histogram h, final long ceiling)
    {
        final RecordedValuesIterator it = new RecordedValuesIterator(h);
        long n = 0;
        double sum = 0;
        while (it.hasNext())
        {
            final org.HdrHistogram.HistogramIterationValue v = it.next();
            final long value = v.getValueIteratedTo();
            if (value >= ceiling)
            {
                break;
            }
            n += v.getCountAtValueIteratedTo();
            sum += (double)value * v.getCountAtValueIteratedTo();
        }
        return 0 == n ? 0.0 : sum / n;
    }
}
