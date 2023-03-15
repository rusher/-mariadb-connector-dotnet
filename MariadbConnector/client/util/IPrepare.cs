using MariadbConnector.message.server;

namespace MariadbConnector.client.util;

public interface IPrepare
{
    uint StatementId { get; }
    IColumnDecoder[] Parameters { get; }
    IColumnDecoder[] Columns { get; set; }
    void Close(IClient con);
    void DecrementUse(IClient con, MariaDbCommand dbCommand);
    void IncrementUse(MariaDbCommand dbCommand);
    void UnCache(IClient client);
}