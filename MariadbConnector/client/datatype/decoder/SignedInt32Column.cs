using MariadbConnector.client.util;
using MariadbConnector.message.server;
using MariadbConnector.utils.exception;

namespace MariadbConnector.client.decoder;

public class SignedInt32Column : ColumnDefinitionPacket, IColumnDecoder
{
    public SignedInt32Column(
        IReadableByteBuf buf,
        int charset,
        long length,
        DataType dataType,
        byte decimals,
        int flags,
        int[] stringPos,
        string extTypeName,
        string extTypeFormat) : base(buf, charset, length, dataType, decimals, flags, stringPos, extTypeName,
        extTypeFormat)
    {
    }

    public object GetDefaultText(Configuration conf, IReadableByteBuf buf, int length)
    {
        return (int)buf.Atoll(length);
    }

    public object GetDefaultBinary(Configuration conf, IReadableByteBuf buf, int length)
    {
        return buf.ReadInt();
    }

    public bool DecodeBooleanText(IReadableByteBuf buf, int length)
    {
        return !string.Equals("0", buf.ReadAscii(length));
    }

    public bool DecodeBooleanBinary(IReadableByteBuf buf, int length)
    {
        return buf.ReadInt() != 0;
    }

    public byte DecodeByteText(IReadableByteBuf buf, int length)
    {
        return Convert.ToByte(buf.Atoll(length));
    }

    public byte DecodeByteBinary(IReadableByteBuf buf, int length)
    {
        return Convert.ToByte(buf.ReadInt());
    }

    public string DecodeStringText(IReadableByteBuf buf, int length)
    {
        return buf.ReadAscii(length);
    }

    public string DecodeStringBinary(IReadableByteBuf buf, int length)
    {
        return buf.ReadInt().ToString();
    }

    public short DecodeShortText(IReadableByteBuf buf, int length)
    {
        return Convert.ToInt16(buf.Atoll(length));
    }

    public short DecodeShortBinary(IReadableByteBuf buf, int length)
    {
        return Convert.ToInt16(buf.ReadInt());
    }

    public int DecodeIntText(IReadableByteBuf buf, int length)
    {
        return (int)buf.Atoll(length);
    }

    public int DecodeIntBinary(IReadableByteBuf buf, int length)
    {
        return buf.ReadInt();
    }

    public long DecodeLongText(IReadableByteBuf buf, int length)
    {
        return buf.Atoll(length);
    }

    public long DecodeLongBinary(IReadableByteBuf buf, int length)
    {
        return buf.ReadInt();
    }

    public float DecodeFloatText(IReadableByteBuf buf, int length)
    {
        return float.Parse(buf.ReadAscii(length));
    }

    public float DecodeFloatBinary(IReadableByteBuf buf, int length)
    {
        return buf.ReadInt();
    }

    public double DecodeDoubleText(IReadableByteBuf buf, int length)
    {
        return double.Parse(buf.ReadAscii(length));
    }

    public double DecodeDoubleBinary(IReadableByteBuf buf, int length)
    {
        return buf.ReadInt();
    }

    public DateTime DecodeDateTimeText(IReadableByteBuf buf, int length)
    {
        buf.Skip(length);
        throw new DbDataException($"Data type {_dataType} cannot be decoded as Date");
    }

    public DateTime DecodeDateTimeBinary(IReadableByteBuf buf, int length)
    {
        buf.Skip(length);
        throw new DbDataException($"Data type {_dataType} cannot be decoded as Date");
    }

    public decimal DecodeDecimalText(IReadableByteBuf buf, int length)
    {
        return buf.Atoll(length);
    }

    public decimal DecodeDecimalBinary(IReadableByteBuf buf, int length)
    {
        return buf.ReadInt();
    }

    public Guid DecodeGuidText(IReadableByteBuf buf, int length)
    {
        buf.Skip(length);
        throw new DbDataException($"Data type {_dataType} cannot be decoded as Guid");
    }

    public Guid DecodeGuidBinary(IReadableByteBuf buf, int length)
    {
        buf.Skip(length);
        throw new DbDataException($"Data type {_dataType} cannot be decoded as Guid");
    }
}