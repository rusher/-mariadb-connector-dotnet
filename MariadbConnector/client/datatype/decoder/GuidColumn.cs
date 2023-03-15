using MariadbConnector.client.util;
using MariadbConnector.message.server;

namespace MariadbConnector.client.decoder;

public class GuidColumn : ColumnDefinitionPacket, IColumnDecoder
{
    public GuidColumn(
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
        return Guid.Parse(buf.ReadAscii(length));
    }

    public object GetDefaultBinary(Configuration conf, IReadableByteBuf buf, int length)
    {
        return Guid.Parse(buf.ReadAscii(length));
    }

    public bool DecodeBooleanText(IReadableByteBuf buf, int length)
    {
        buf.Skip(length);
        throw new ArgumentException("Data type UUID cannot be decoded as bool");
    }

    public bool DecodeBooleanBinary(IReadableByteBuf buf, int length)
    {
        buf.Skip(length);
        throw new ArgumentException("Data type UUID cannot be decoded as bool");
    }

    public byte DecodeByteText(IReadableByteBuf buf, int length)
    {
        buf.Skip(length);
        throw new ArgumentException("Data type UUID cannot be decoded as byte");
    }

    public byte DecodeByteBinary(IReadableByteBuf buf, int length)
    {
        buf.Skip(length);
        throw new ArgumentException("Data type UUID cannot be decoded as byte");
    }

    public string DecodeStringText(IReadableByteBuf buf, int length)
    {
        return buf.ReadAscii(length);
    }

    public string DecodeStringBinary(IReadableByteBuf buf, int length)
    {
        return buf.ReadAscii(length);
    }

    public short DecodeShortText(IReadableByteBuf buf, int length)
    {
        buf.Skip(length);
        throw new ArgumentException("Data type UUID cannot be decoded as short");
    }

    public short DecodeShortBinary(IReadableByteBuf buf, int length)
    {
        buf.Skip(length);
        throw new ArgumentException("Data type UUID cannot be decoded as short");
    }

    public int DecodeIntText(IReadableByteBuf buf, int length)
    {
        buf.Skip(length);
        throw new ArgumentException("Data type UUID cannot be decoded as int");
    }

    public int DecodeIntBinary(IReadableByteBuf buf, int length)
    {
        buf.Skip(length);
        throw new ArgumentException("Data type UUID cannot be decoded as int");
    }

    public long DecodeLongText(IReadableByteBuf buf, int length)
    {
        buf.Skip(length);
        throw new ArgumentException("Data type UUID cannot be decoded as long");
    }

    public long DecodeLongBinary(IReadableByteBuf buf, int length)
    {
        buf.Skip(length);
        throw new ArgumentException("Data type UUID cannot be decoded as long");
    }

    public float DecodeFloatText(IReadableByteBuf buf, int length)
    {
        buf.Skip(length);
        throw new ArgumentException("Data type UUID cannot be decoded as float");
    }

    public float DecodeFloatBinary(IReadableByteBuf buf, int length)
    {
        buf.Skip(length);
        throw new ArgumentException("Data type UUID cannot be decoded as float");
    }

    public double DecodeDoubleText(IReadableByteBuf buf, int length)
    {
        buf.Skip(length);
        throw new ArgumentException("Data type UUID cannot be decoded as double");
    }

    public double DecodeDoubleBinary(IReadableByteBuf buf, int length)
    {
        buf.Skip(length);
        throw new ArgumentException("Data type UUID cannot be decoded as double");
    }

    public DateTime DecodeDateTimeText(IReadableByteBuf buf, int length)
    {
        buf.Skip(length);
        throw new ArgumentException("Data type UUID cannot be decoded as DateTime");
    }

    public DateTime DecodeDateTimeBinary(IReadableByteBuf buf, int length)
    {
        buf.Skip(length);
        throw new ArgumentException("Data type UUID cannot be decoded as DateTime");
    }

    public decimal DecodeDecimalText(IReadableByteBuf buf, int length)
    {
        buf.Skip(length);
        throw new ArgumentException("Data type UUID cannot be decoded as decimal");
    }

    public decimal DecodeDecimalBinary(IReadableByteBuf buf, int length)
    {
        return DecodeDecimalText(buf, length);
    }

    public Guid DecodeGuidText(IReadableByteBuf buf, int length)
    {
        var str = buf.ReadAscii(length);
        return new Guid(str);
    }

    public Guid DecodeGuidBinary(IReadableByteBuf buf, int length)
    {
        var str = buf.ReadAscii(length);
        return new Guid(str);
    }
}