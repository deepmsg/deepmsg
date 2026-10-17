import org.agrona.ExpandableRingBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/** Prints what Agrona 2.6.1's ExpandableRingBuffer does to a scripted sequence
 *  of appends and consumes, so the Rust port can be compared against the real
 *  thing: the data offsets, the head offsets handed to the consumer, the value
 *  `consume` returns, and what happens when a fixed capacity is reached. */
public class RingGen
{
    private static final UnsafeBuffer SOURCE = new UnsafeBuffer(new byte[64]);

    private static String hex(final int length)
    {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++)
        {
            sb.append(Character.forDigit((SOURCE.getByte(i) >> 4) & 0xF, 16));
            sb.append(Character.forDigit(SOURCE.getByte(i) & 0xF, 16));
        }
        return sb.toString();
    }

    private static void fill(final int length)
    {
        for (int i = 0; i < length; i++)
        {
            SOURCE.putByte(i, (byte)(0xA0 + i));
        }
    }

    private static void each(final String prefix, final ExpandableRingBuffer ring, final int headOffset, final int limit)
    {
        final StringBuilder sb = new StringBuilder(prefix);
        ring.forEach(headOffset, (buffer, offset, length, messageHeadOffset) ->
        {
            sb.append("len=").append(length)
              .append(",head=").append(messageHeadOffset)
              .append(",data=");
            for (int i = 0; i < length; i++)
            {
                sb.append(Character.forDigit((buffer.getByte(offset + i) >> 4) & 0xF, 16));
                sb.append(Character.forDigit(buffer.getByte(offset + i) & 0xF, 16));
            }
            sb.append(';');
            return true;
        }, limit);
        System.out.println(sb);
    }

    public static void main(final String[] args)
    {
        final ExpandableRingBuffer ring = new ExpandableRingBuffer();
        fill(3);
        System.out.println("append3=" + ring.append(SOURCE, 0, 3));
        fill(8);
        System.out.println("append8=" + ring.append(SOURCE, 0, 8));
        fill(9);
        System.out.println("append9=" + ring.append(SOURCE, 0, 9));
        System.out.println("size=" + ring.size() + ",capacity=" + ring.capacity());
        each("each0=", ring, 0, 10);
        each("eachFrom16=", ring, 16, 10);

        final int consumed = ring.consume((buffer, offset, length, messageHeadOffset) ->
        {
            System.out.println("consume-one=len=" + length + ",head=" + messageHeadOffset);
            return false;
        }, 10);
        System.out.println("consume-return=" + consumed + ",size=" + ring.size());
        each("eachAfter=", ring, 0, 10);

        // A fixed capacity that the next frame does not fit.
        final ExpandableRingBuffer fixed = new ExpandableRingBuffer(64, 64, false);
        fill(24);
        System.out.println("fixed1=" + fixed.append(SOURCE, 0, 24));
        System.out.println("fixed2=" + fixed.append(SOURCE, 0, 24));
        System.out.println("fixed3=" + fixed.append(SOURCE, 0, 24));
        System.out.println("fixed-size=" + fixed.size() + ",capacity=" + fixed.capacity());
        each("fixed-each=", fixed, 0, 10);
    }
}
