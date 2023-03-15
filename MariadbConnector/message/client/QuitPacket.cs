using MariadbConnector.client;
using MariadbConnector.client.socket;

namespace MariadbConnector.message.client;

public class QuitPacket : AbstractClientMessage
{
    /**
     * default instance to encode packet
     */
    public static QuitPacket INSTANCE = new();

    public override string Description => "QUIT";

    public override ByteBufferWriter BuildPayLoad(IContext context)
    {
        var buf = new ByteBufferWriter();
        buf.WriteByte(0x01);
        return buf;
    }
}