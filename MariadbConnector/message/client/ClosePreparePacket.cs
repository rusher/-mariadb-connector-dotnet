using MariadbConnector.client;
using MariadbConnector.client.socket;

namespace MariadbConnector.message.client;

public class ClosePreparePacket : AbstractClientMessage
{
    private readonly uint _statementId;

    public ClosePreparePacket(uint statementId)
    {
        _statementId = statementId;
    }

    public override string Description => "Closing PREPARE " + _statementId;

    public override ByteBufferWriter BuildPayLoad(IContext context)
    {
        var buf = new ByteBufferWriter();
        buf.WriteByte(0x19);
        buf.WriteUInt(_statementId);
        return buf;
    }
}