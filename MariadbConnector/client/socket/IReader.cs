using MariadbConnector.client.util;

namespace MariadbConnector.client.socket;

public interface IReader
{
    Task<IReadableByteBuf> ReadReusablePacket(CancellationToken cancellationToken, bool traceEnable);
    Task<IReadableByteBuf> ReadReusablePacket(CancellationToken cancellationToken);
    IReadableByteBuf ReadReusablePacketSync();

    Task<byte[]> ReadPacket(CancellationToken cancellationToken, bool traceEnable);
    byte[] ReadPacketSync(bool traceEnable);
    IReadableByteBuf ReadableBufFromArray(byte[] buf);
    Task SkipPacket(CancellationToken cancellationToken);
    void SkipPacketSync();
    MutableByte GetSequence();
    void Close();
    void SetServerThreadId(long? serverThreadId, HostAddress hostAddress);
}