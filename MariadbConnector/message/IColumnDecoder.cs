using System.Text;
using MariadbConnector.client;
using MariadbConnector.client.impl;
using MariadbConnector.client.util;
using MariadbConnector.utils.constant;

namespace MariadbConnector.message.server;

public interface IColumnDecoder : IColumn
{
    int GetPrecision();

    object GetDefaultText(Configuration conf, IReadableByteBuf buf, int length);
    object GetDefaultBinary(Configuration conf, IReadableByteBuf buf, int length);
    string DecodeStringText(IReadableByteBuf buf, int length);
    string DecodeStringBinary(IReadableByteBuf buf, int length);
    byte DecodeByteText(IReadableByteBuf buf, int length);
    byte DecodeByteBinary(IReadableByteBuf buf, int length);
    DateTime DecodeDateTimeText(IReadableByteBuf buf, int length);
    DateTime DecodeDateTimeBinary(IReadableByteBuf buf, int length);
    bool DecodeBooleanText(IReadableByteBuf buf, int length);
    bool DecodeBooleanBinary(IReadableByteBuf buf, int length);
    short DecodeShortText(IReadableByteBuf buf, int length);
    short DecodeShortBinary(IReadableByteBuf buf, int length);
    int DecodeIntText(IReadableByteBuf buf, int length);
    int DecodeIntBinary(IReadableByteBuf buf, int length);
    long DecodeLongText(IReadableByteBuf buf, int length);
    long DecodeLongBinary(IReadableByteBuf buf, int length);
    float DecodeFloatText(IReadableByteBuf buf, int length);
    float DecodeFloatBinary(IReadableByteBuf buf, int length);
    double DecodeDoubleText(IReadableByteBuf buf, int length);
    double DecodeDoubleBinary(IReadableByteBuf buf, int length);
    decimal DecodeDecimalText(IReadableByteBuf buf, int length);
    decimal DecodeDecimalBinary(IReadableByteBuf buf, int length);
    Guid DecodeGuidText(IReadableByteBuf buf, int length);
    Guid DecodeGuidBinary(IReadableByteBuf buf, int length);

    static IColumnDecoder Decode(IReadableByteBuf buf, bool extendedInfo)
    {
        // skip first strings
        var stringPos = new int[5];
        stringPos[0] = buf.SkipIdentifier(); // schema pos
        stringPos[1] = buf.SkipIdentifier(); // table alias pos
        stringPos[2] = buf.SkipIdentifier(); // table pos
        stringPos[3] = buf.SkipIdentifier(); // column alias pos
        stringPos[4] = buf.SkipIdentifier(); // column pos
        buf.SkipIdentifier();

        string? extTypeName = null;
        string? extTypeFormat = null;
        if (extendedInfo)
        {
            // fast skipping extended info (usually not set)
            if (buf.GetByte() != 0)
            {
                // revert position, because has extended info.

                var subPacket = buf.ReadLengthBuffer();
                while (subPacket.ReadableBytes() > 0)
                    switch (subPacket.ReadByte())
                    {
                        case 0:
                            extTypeName = subPacket.ReadAscii(subPacket.ReadIntLengthEncodedNotNull());
                            break;
                        case 1:
                            extTypeFormat = subPacket.ReadAscii(subPacket.ReadIntLengthEncodedNotNull());
                            break;
                        default: // skip data
                            subPacket.Skip(subPacket.ReadIntLengthEncodedNotNull());
                            break;
                    }
            }
            else
            {
                buf.Skip();
            }
        }

        buf.Skip(); // skip length always 0x0c
        var charset = buf.ReadShort();
        var length = buf.ReadInt();
        var dataType = (DataType)buf.ReadUnsignedByte();
        int flags = buf.ReadUnsignedShort();
        var decimals = buf.ReadByte();

        var constructor =
            string.Equals(extTypeName, "uuid")
                ? DataTypeDefaultDecoder.GuidLamdba
                : (flags & ColumnFlags.UNSIGNED) == 0
                    ? DataTypeDefaultDecoder.signedDecoders[dataType]
                    : DataTypeDefaultDecoder.unsignedDecoders[dataType];
        return constructor.Invoke(
            buf, charset, length, dataType, decimals, flags, stringPos, extTypeName, extTypeFormat);
    }

    /**
     * Create fake MySQL column definition packet with indicated datatype
     * 
     * @param name column name
     * @param type data type
     * @param flags column flags
     * @return Column
     */
    static IColumnDecoder Create(string name, DataType type, int flags)
    {
        var nameBytes = Encoding.UTF8.GetBytes(name);
        var arr = new byte[9 + 2 * nameBytes.Length];
        arr[0] = 3;
        arr[1] = Convert.ToByte('D');
        arr[2] = Convert.ToByte('E');
        arr[3] = Convert.ToByte('F');

        var stringPos = new int[5];
        stringPos[0] = 4; // schema pos
        stringPos[1] = 5; // table alias pos
        stringPos[2] = 6; // table pos

        // lenenc_str     name
        // lenenc_str     org_name
        var pos = 7;
        for (var i = 0; i < 2; i++)
        {
            stringPos[i + 3] = pos;
            arr[pos++] = (byte)nameBytes.Length;
            Array.Copy(nameBytes, 0, arr, pos, nameBytes.Length);
            pos += nameBytes.Length;
        }

        int len;

        /* Sensible predefined length - since we're dealing with I_S here, most char fields are 64 char long */
        switch (type)
        {
            case DataType.VARCHAR:
            case DataType.VARSTRING:
                len = 64 * 3; /* 3 bytes per UTF8 char */
                break;
            case DataType.SMALLINT:
                len = 5;
                break;
            case DataType.NULL:
                len = 0;
                break;
            default:
                len = 1;
                break;
        }

        var constructor =
            (flags & ColumnFlags.UNSIGNED) == 0
                ? DataTypeDefaultDecoder.signedDecoders[type]
                : DataTypeDefaultDecoder.unsignedDecoders[type];
        return constructor.Invoke(
            new StandardReadableByteBuf(arr, arr.Length),
            33,
            len,
            type,
            0,
            flags,
            stringPos,
            null,
            null);
    }
}