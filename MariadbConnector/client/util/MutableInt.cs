namespace MariadbConnector.client.util;

public class MutableInt
{
    public int Value { get; set; }

    public int incrementAndGet()
    {
        return ++Value;
    }
}