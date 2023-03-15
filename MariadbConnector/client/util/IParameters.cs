namespace MariadbConnector.client.util;

public interface IParameters
{
    IParameter get(int index);
    bool containsKey(int index);
    void set(int index, IParameter element);
    uint size();
    IParameters clone();
}