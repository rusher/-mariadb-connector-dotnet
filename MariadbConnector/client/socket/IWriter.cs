using MariadbConnector.utils;

namespace MariadbConnector.client.socket;

public interface IWriter
{
    void WritePayloadSync(PayloadData payload);
    Task WritePayloadAsync(PayloadData payload, CancellationToken cancellationToken);
    Task WritePayload(IoBehavior ioBehavior, PayloadData payload, CancellationToken cancellationToken);

    Task WriteBytes(IoBehavior ioBehavior, byte[] buf, int offset, int len);
    Task WriteEmptyPacket(IoBehavior ioBehavior);

    void Close();
    void PermitTrace(bool permitTrace);
    void SetServerThreadId(long? serverThreadId, HostAddress hostAddress);
    void Init();
    void Flush();
}