import org.agrona.SystemUtil;
public class Gen6
{
    public static void main(final String[] a)
    {
        final String v = System.getProperty("probe");
        String dur, size;
        try { dur = Long.toString(SystemUtil.getDurationInNanos("probe", -1)); }
        catch (final Throwable t) { dur = "throws"; }
        try { size = Long.toString(SystemUtil.getSizeAsInt("probe", -1)); }
        catch (final Throwable t) { size = "throws"; }
        System.out.println(v + "\tdur=" + dur + "\tsize=" + size);
    }
}
