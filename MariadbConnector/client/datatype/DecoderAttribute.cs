using MariadbConnector.client.util;
using MariadbConnector.message.server;

namespace MariadbConnector.client;

public class DecoderAttribute : Attribute
{
    public DecoderAttribute(
        Func<IReadableByteBuf, int, long, DataType, byte, int, int[], string, string, IColumnDecoder> signedDecoder,
        Func<IReadableByteBuf, int, long, DataType, byte, int, int[], string, string, IColumnDecoder> unsignedDecoder)
    {
    }
}