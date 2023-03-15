using MariadbConnector.client.impl;
using MariadbConnector.client.util;
using MariadbConnector.message.server;
using MariadbConnector.plugin;

namespace MariadbConnector.client.result.rowdecoder;

public class BinaryRowDecoder : IRowDecoder
{
    public T Decode<T>(
        ICodec<T> codec,
        StandardReadableByteBuf rowBuf,
        int fieldLength,
        IColumnDecoder[] metadataList,
        MutableInt fieldIndex)
    {
        return codec.DecodeBinary(rowBuf, fieldLength, metadataList[fieldIndex.Value]);
    }

    public object defaultDecode(
        Configuration conf,
        IColumnDecoder[] metadataList,
        MutableInt fieldIndex,
        StandardReadableByteBuf rowBuf,
        int fieldLength)
    {
        return metadataList[fieldIndex.Value].GetDefaultBinary(conf, rowBuf, fieldLength);
    }

    public string DecodeString(
        IColumnDecoder[] metadataList,
        MutableInt fieldIndex,
        StandardReadableByteBuf rowBuf,
        int fieldLength)
    {
        return metadataList[fieldIndex.Value].DecodeStringBinary(rowBuf, fieldLength);
    }

    public byte DecodeByte(
        IColumnDecoder[] metadataList,
        MutableInt fieldIndex,
        StandardReadableByteBuf rowBuf,
        int fieldLength)
    {
        return metadataList[fieldIndex.Value].DecodeByteBinary(rowBuf, fieldLength);
    }

    public bool DecodeBoolean(
        IColumnDecoder[] metadataList,
        MutableInt fieldIndex,
        StandardReadableByteBuf rowBuf,
        int fieldLength)
    {
        return metadataList[fieldIndex.Value].DecodeBooleanBinary(rowBuf, fieldLength);
    }

    public short DecodeShort(
        IColumnDecoder[] metadataList,
        MutableInt fieldIndex,
        StandardReadableByteBuf rowBuf,
        int fieldLength)
    {
        return metadataList[fieldIndex.Value].DecodeShortBinary(rowBuf, fieldLength);
    }

    public int DecodeInt(
        IColumnDecoder[] metadataList,
        MutableInt fieldIndex,
        StandardReadableByteBuf rowBuf,
        int fieldLength)
    {
        return metadataList[fieldIndex.Value].DecodeIntBinary(rowBuf, fieldLength);
    }

    public long DecodeLong(
        IColumnDecoder[] metadataList,
        MutableInt fieldIndex,
        StandardReadableByteBuf rowBuf,
        int fieldLength)
    {
        return metadataList[fieldIndex.Value].DecodeLongBinary(rowBuf, fieldLength);
    }

    public DateTime DecodeDateTime(
        IColumnDecoder[] metadataList,
        MutableInt fieldIndex,
        StandardReadableByteBuf rowBuf,
        int fieldLength)
    {
        return metadataList[fieldIndex.Value].DecodeDateTimeBinary(rowBuf, fieldLength);
    }

    public float DecodeFloat(
        IColumnDecoder[] metadataList,
        MutableInt fieldIndex,
        StandardReadableByteBuf rowBuf,
        int fieldLength)
    {
        return metadataList[fieldIndex.Value].DecodeFloatBinary(rowBuf, fieldLength);
    }

    public double DecodeDouble(
        IColumnDecoder[] metadataList,
        MutableInt fieldIndex,
        StandardReadableByteBuf rowBuf,
        int fieldLength)
    {
        return metadataList[fieldIndex.Value].DecodeDoubleBinary(rowBuf, fieldLength);
    }

    public bool WasNull(byte[] nullBitmap, MutableInt fieldIndex, int fieldLength)
    {
        return (nullBitmap[(fieldIndex.Value + 2) / 8] & (1 << ((fieldIndex.Value + 2) % 8))) > 0;
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
            rowBuf.Pos = 1;
            rowBuf.ReadBytes(nullBitmap);
        }
        else
        {
            fieldIndex.incrementAndGet();
            if (fieldIndex.Value == 0)
            {
                // skip header + null-bitmap
                rowBuf.Pos = 1;
                rowBuf.ReadBytes(nullBitmap);
            }
        }

        while (fieldIndex.Value < newIndex)
        {
            if ((nullBitmap[(fieldIndex.Value + 2) / 8] & (1 << ((fieldIndex.Value + 2) % 8))) == 0)
                // skip bytes
                switch (metadataList[fieldIndex.Value].GetType())
                {
                    case DataType.BIGINT:
                    case DataType.DOUBLE:
                        rowBuf.Skip(8);
                        break;

                    case DataType.INTEGER:
                    case DataType.MEDIUMINT:
                    case DataType.FLOAT:
                        rowBuf.Skip(4);
                        break;

                    case DataType.SMALLINT:
                    case DataType.YEAR:
                        rowBuf.Skip(2);
                        break;

                    case DataType.TINYINT:
                        rowBuf.Skip(1);
                        break;

                    default:
                        rowBuf.SkipLengthEncoded();
                        break;
                }

            fieldIndex.incrementAndGet();
        }

        if (WasNull(nullBitmap, fieldIndex, 0)) return AbstractDataReader.NULL_LENGTH;

        // read asked field position and length
        switch (metadataList[fieldIndex.Value].GetType())
        {
            case DataType.BIGINT:
            case DataType.DOUBLE:
                return 8;

            case DataType.INTEGER:
            case DataType.MEDIUMINT:
            case DataType.FLOAT:
                return 4;

            case DataType.SMALLINT:
            case DataType.YEAR:
                return 2;

            case DataType.TINYINT:
                return 1;

            default:
                // field with variable length
                var len = rowBuf.ReadByte();
                switch (len)
                {
                    case 252:
                        // length is encoded on 3 bytes (0xfc header + 2 bytes indicating length)
                        return rowBuf.ReadUnsignedShort();

                    case 253:
                        // length is encoded on 4 bytes (0xfd header + 3 bytes indicating length)
                        return rowBuf.ReadUnsignedMedium();

                    case 254:
                        // length is encoded on 9 bytes (0xfe header + 8 bytes indicating length)
                        return (int)rowBuf.ReadLong();
                    default:
                        return len & 0xff;
                }
        }
    }

    public DateTime DecodeTimestamp(
        IColumnDecoder[] metadataList,
        MutableInt fieldIndex,
        StandardReadableByteBuf rowBuf,
        int fieldLength)
    {
        return metadataList[fieldIndex.Value].DecodeDateTimeBinary(rowBuf, fieldLength);
    }
}