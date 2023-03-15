using System.Data;
using MariadbConnector.client;
using MariadbConnector.client.socket;
using MariadbConnector.utils;

namespace MariadbConnector.message;

public interface IClientMessage
{
    public string Description { get; }
    ByteBufferWriter BuildPayLoad(IContext context);
    Task WriteAsync(CancellationToken cancellationToken, ByteBufferWriter buf, IWriter writer);

    void WriteSync(ByteBufferWriter buf, IWriter writer);
    uint BatchUpdateLength();


    bool BinaryProtocol();

    bool CanSkipMeta();

    Task<ICompletion> ReadPacketAsync(
        CancellationToken cancellationToken,
        MariaDbCommand stmt,
        CommandBehavior behavior,
        IReader reader,
        IWriter writer,
        IContext context,
        ExceptionFactory exceptionFactory,
        bool traceEnable,
        SemaphoreSlim lockObj,
        IClientMessage message);

    ICompletion ReadPacketSync(
        MariaDbCommand stmt,
        CommandBehavior behavior,
        IReader reader,
        IWriter writer,
        IContext context,
        ExceptionFactory exceptionFactory,
        bool traceEnable,
        SemaphoreSlim lockObj,
        IClientMessage message);

    Stream GetLocalInfileInputStream();
}