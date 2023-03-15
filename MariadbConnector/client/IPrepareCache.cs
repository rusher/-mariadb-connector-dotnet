using MariadbConnector.client.util;

namespace MariadbConnector.client;

public interface IPrepareCache
{
    IPrepare Get(string key, MariaDbCommand preparedStatement);
    IPrepare Put(string key, IPrepare result, MariaDbCommand preparedStatement);
    void Reset();
}