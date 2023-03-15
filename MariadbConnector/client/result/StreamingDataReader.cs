using System.Data;
using MariadbConnector.client.socket;
using MariadbConnector.message.server;

namespace MariadbConnector.client.result;

public class StreamingDataReader : AbstractDataReader
{
    private readonly SemaphoreSlim _lock;

    public StreamingDataReader(MariaDbCommand stmt, bool binaryProtocol, IColumnDecoder[] metaDataList, IReader reader,
        IContext context, bool traceEnable, SemaphoreSlim lockObj, CommandBehavior behavior) : base(stmt,
        binaryProtocol,
        metaDataList, reader, context,
        traceEnable, behavior)
    {
        _lock = lockObj;
        //FetchRemaining(CancellationToken.None);
    }

    public StreamingDataReader(IColumnDecoder[] metadataList, byte[] data, IContext context, CommandBehavior behavior)
        : base(metadataList, data, context, behavior)
    {
    }

    internal override async Task FetchRemaining(CancellationToken cancellationToken)
    {
        if (!Loaded)
        {
            while (!Loaded) await ReadNextPacketAsync(cancellationToken).ConfigureAwait(false);
            Loaded = true;
        }
    }
}