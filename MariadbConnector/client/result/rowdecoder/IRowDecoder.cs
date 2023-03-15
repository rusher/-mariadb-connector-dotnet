using MariadbConnector.client.impl;
using MariadbConnector.client.util;
using MariadbConnector.message.server;
using MariadbConnector.plugin;

namespace MariadbConnector.client.result.rowdecoder;

public interface IRowDecoder
{
    bool WasNull(byte[] nullBitmap, MutableInt fieldIndex, int fieldLength);

    int SetPosition(
        int newIndex,
        MutableInt fieldIndex,
        int maxIndex,
        StandardReadableByteBuf rowBuf,
        byte[] nullBitmap,
        IColumnDecoder[] metadataList);

    T Decode<T>(
        ICodec<T> codec,
        StandardReadableByteBuf rowBuf,
        int fieldLength,
        IColumnDecoder[] metadataList,
        MutableInt fieldIndex);

    object defaultDecode(
        Configuration conf,
        IColumnDecoder[] metadataList,
        MutableInt fieldIndex,
        StandardReadableByteBuf rowBuf,
        int fieldLength);

    byte DecodeByte(
        IColumnDecoder[] metadataList,
        MutableInt fieldIndex,
        StandardReadableByteBuf rowBuf,
        int fieldLength);

    bool DecodeBoolean(
        IColumnDecoder[] metadataList,
        MutableInt fieldIndex,
        StandardReadableByteBuf rowBuf,
        int fieldLength);

    DateTime DecodeDateTime(
        IColumnDecoder[] metadataList,
        MutableInt fieldIndex,
        StandardReadableByteBuf rowBuf,
        int fieldLength);

    short DecodeShort(
        IColumnDecoder[] metadataList,
        MutableInt fieldIndex,
        StandardReadableByteBuf rowBuf,
        int fieldLength);

    int DecodeInt(
        IColumnDecoder[] metadataList,
        MutableInt fieldIndex,
        StandardReadableByteBuf rowBuf,
        int fieldLength);

    string DecodeString(
        IColumnDecoder[] metadataList,
        MutableInt fieldIndex,
        StandardReadableByteBuf rowBuf,
        int fieldLength);

    long DecodeLong(
        IColumnDecoder[] metadataList,
        MutableInt fieldIndex,
        StandardReadableByteBuf rowBuf,
        int fieldLength);

    float DecodeFloat(
        IColumnDecoder[] metadataList,
        MutableInt fieldIndex,
        StandardReadableByteBuf rowBuf,
        int fieldLength);

    double DecodeDouble(
        IColumnDecoder[] metadataList,
        MutableInt fieldIndex,
        StandardReadableByteBuf rowBuf,
        int fieldLength);
}