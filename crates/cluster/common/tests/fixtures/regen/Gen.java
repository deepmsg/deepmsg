import org.agrona.collections.Long2LongHashMap;

/** Prints the iteration order of Agrona 2.6.1's Long2LongHashMap for a set of
 *  insertion sequences, so the Rust reimplementation can be compared against
 *  the real thing rather than against my reading of the bytecode. */
public class Gen
{
    private static void emit(final String name, final Long2LongHashMap m)
    {
        final StringBuilder sb = new StringBuilder(name);
        sb.append('\t');
        m.forEachLong((k, v) ->
        {
            sb.append(k).append('=').append(v).append(',');
        });
        System.out.println(sb);
    }

    /** `entrySet().iterator()`: the order `WheelTimerService.snapshot` writes.
     *  It is a different order from `forEachLong` over the same map. */
    private static void emitEntry(final String name, final Long2LongHashMap m)
    {
        final StringBuilder sb = new StringBuilder(name);
        sb.append('\t');
        for (final java.util.Map.Entry<Long, Long> e : m.entrySet())
        {
            sb.append(e.getKey()).append('=').append(e.getValue()).append(',');
        }
        System.out.println(sb);
    }

    public static void main(final String[] args)
    {
        final boolean entry = args.length > 0 && "entry".equals(args[0]);
        final Long2LongHashMap a = new Long2LongHashMap(Long.MAX_VALUE);
        for (long i = 1; i <= 16; i++) a.put(i, i * 10);
        if (entry) emitEntry("seq-16", a); else emit("seq-16", a);

        final Long2LongHashMap b = new Long2LongHashMap(Long.MAX_VALUE);
        for (long i = 1; i <= 100; i++) b.put(i, i * 10);
        if (entry) emitEntry("seq-100", b); else emit("seq-100", b);

        // scattered keys: a mix that exercises the SplitMix64 finaliser's upper bits
        final Long2LongHashMap c = new Long2LongHashMap(Long.MAX_VALUE);
        long k = 0x9E3779B97F4A7C15L;
        for (int i = 0; i < 64; i++) { c.put(k, i); k += 0x5851F42D4C957F2DL; }
        if (entry) emitEntry("scatter-64", c); else emit("scatter-64", c);

        // removals exercise compactChain, which moves entries in the probe chain
        final Long2LongHashMap d = new Long2LongHashMap(Long.MAX_VALUE);
        for (long i = 1; i <= 64; i++) d.put(i, i * 10);
        for (long i = 1; i <= 64; i += 2) d.remove(i);
        if (entry) emitEntry("remove-odd-64", d); else emit("remove-odd-64", d);

        // remove then reinsert changes which slots are occupied
        final Long2LongHashMap e = new Long2LongHashMap(Long.MAX_VALUE);
        for (long i = 1; i <= 40; i++) e.put(i, i);
        for (long i = 5; i <= 35; i += 5) e.remove(i);
        for (long i = 5; i <= 35; i += 5) e.put(i, i * 100);
        if (entry) emitEntry("reinsert-40", e); else emit("reinsert-40", e);
    }
}
