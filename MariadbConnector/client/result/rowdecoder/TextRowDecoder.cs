using MariadbConnector.client.impl;
using MariadbConnector.client.util;
using MariadbConnector.message.server;
using MariadbConnector.plugin;

namespace MariadbConnector.client.result.rowdecoder;

public class TextRowDecoder : IRowDecoder
{
    public T Decode<T>(
        ICodec<T> codec,
        StandardReadableByteBuf rowBuf,
        int fieldLength,
        IColumnDecoder[] metadataList,
        MutableInt fieldIndex)
    {
        return codec.DecodeText(rowBuf, fieldLength, metadataList[fieldIndex.Value]);
    }

    public object defaultDecode(
        Configuration conf,
        IColumnDecoder[] metadataList,
        MutableInt fieldIndex,
        StandardReadableByteBuf rowBuf,
        int fieldLength)
    {
        return metadataList[fieldIndex.Value].GetDefaultText(conf, rowBuf, fieldLength);
    }

    public string DecodeString(
        IColumnDecoder[] metadataList,
        MutableInt fieldIndex,
        StandardReadableByteBuf rowBuf,
        int fieldLength)
    {
        return metadataList[fieldIndex.Value].DecodeStringText(rowBuf, fieldLength);
    }

    public byte DecodeByte(
        IColumnDecoder[] metadataList,
        MutableInt fieldIndex,
        StandardReadableByteBuf rowBuf,
        int fieldLength)
    {
        return metadataList[fieldIndex.Value].DecodeByteText(rowBuf, fieldLength);
    }

    public bool DecodeBoolean(
        IColumnDecoder[] metadataList,
        MutableInt fieldIndex,
        StandardReadableByteBuf rowBuf,
        int fieldLength)
    {
        return metadataList[fieldIndex.Value].DecodeBooleanText(rowBuf, fieldLength);
    }

    public DateTime DecodeDateTime(
        IColumnDecoder[] metadataList,
        MutableInt fieldIndex,
        StandardReadableByteBuf rowBuf,
        int fieldLength)
    {
        return metadataList[fieldIndex.Value].DecodeDateTimeText(rowBuf, fieldLength);
    }

    public short DecodeShort(
        IColumnDecoder[] metadataList,
        MutableInt fieldIndex,
        StandardReadableByteBuf rowBuf,
        int fieldLength)
    {
        return metadataList[fieldIndex.Value].DecodeShortText(rowBuf, fieldLength);
    }

    public int DecodeInt(
        IColumnDecoder[] metadataList,
        MutableInt fieldIndex,
        StandardReadableByteBuf rowBuf,
        int fieldLength)
    {
        return metadataList[fieldIndex.Value].DecodeIntText(rowBuf, fieldLength);
    }

    public long DecodeLong(
        IColumnDecoder[] metadataList,
        MutableInt fieldIndex,
        StandardReadableByteBuf rowBuf,
        int fieldLength)
    {
        return metadataList[fieldIndex.Value].DecodeLongText(rowBuf, fieldLength);
    }

    public float DecodeFloat(
        IColumnDecoder[] metadataList,
        MutableInt fieldIndex,
        StandardReadableByteBuf rowBuf,
        int fieldLength)
    {
        return metadataList[fieldIndex.Value].DecodeFloatText(rowBuf, fieldLength);
    }

    public double DecodeDouble(
        IColumnDecoder[] metadataList,
        MutableInt fieldIndex,
        StandardReadableByteBuf rowBuf,
        int fieldLength)
    {
        return metadataList[fieldIndex.Value].DecodeDoubleText(rowBuf, fieldLength);
    }

    public bool WasNull(byte[] nullBitmap, MutableInt fieldIndex, int fieldLength)
    {
        return fieldLength == AbstractDataReader.NULL_LENGTH;
    }

    public int SetPosition(
        int newIndex,
        MutableInt fieldIndex,
        int maxIndex,
        StandardReadableByteBuf rowBuf,
        byte[] nullBitmap,
        IColumnDecoder[] metadataList)
    {
        if (fieldIndex.Value >= newIndex)
        {
            fieldIndex.Value = 0;
            rowBuf.Pos = 0;
        }
        else
        {
            fieldIndex.incrementAndGet();
        }

        while (fieldIndex.Value < newIndex)
        {
            rowBuf.SkipLengthEncoded();
            fieldIndex.incrementAndGet();
        }

        var len = rowBuf.ReadByte();
        switch (len)
        {
            case 251:
                return AbstractDataReader.NULL_LENGTH;
            case 252:
                return rowBuf.ReadUnsignedShort();
            case 253:
                return rowBuf.ReadUnsignedMedium();
            case 254:
                var fieldLength = (int)rowBuf.ReadUnsignedInt();
                rowBuf.Skip(4);
                return fieldLength;
            default:
                return len & 0xff;
        }
    }
}