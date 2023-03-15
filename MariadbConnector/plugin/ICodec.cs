using MariadbConnector.client;
using MariadbConnector.client.socket;
using MariadbConnector.client.util;
using MariadbConnector.message.server;

namespace MariadbConnector.plugin;

public interface ICodec<T>
{
    string ClassName();
    bool CanDecode(IColumnDecoder column, Type type);

    bool CanEncode(object value);

    T DecodeText(
        IReadableByteBuf buffer,
        int length,
        IColumnDecoder column);

    T DecodeBinary(
        IReadableByteBuf buffer,
        int length,
        IColumnDecoder column);

    void EncodeText(IWriter encoder, IContext context, object value, long? length);

    void EncodeBinary(IWriter encoder, object value, long? length);

    bool CanEncodeLongData();

    void EncodeLongData(IWriter encoder, T value, long? length);
    byte[] EncodeData(T value, long? length);
    int GetBinaryEncodeType();
}