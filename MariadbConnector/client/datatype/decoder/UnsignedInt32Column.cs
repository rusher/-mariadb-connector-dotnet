using MariadbConnector.client.util;
using MariadbConnector.message.server;
using MariadbConnector.utils.exception;

namespace MariadbConnector.client.decoder;

public class UnsignedInt32Column : ColumnDefinitionPacket, IColumnDecoder
{
    public UnsignedInt32Column(
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
        return buf.ReadUnsignedInt();
    }

    public bool DecodeBooleanText(IReadableByteBuf buf, int length)
    {
        return !string.Equals("0", buf.ReadAscii(length));
    }

    public bool DecodeBooleanBinary(IReadableByteBuf buf, int length)
    {
        return buf.ReadUnsignedInt() != 0;
    }

    public byte DecodeByteText(IReadableByteBuf buf, int length)
    {
        var result = buf.Atoll(length);
        if ((byte)result != result) throw new ArgumentException("byte overflow");
        return (byte)result;
    }

    public byte DecodeByteBinary(IReadableByteBuf buf, int length)
    {
        long result = buf.ReadUnsignedInt();

        if ((byte)result != result) throw new ArgumentException("byte overflow");
        return (byte)result;
    }

    public string DecodeStringText(IReadableByteBuf buf, int length)
    {
        return buf.ReadAscii(length);
    }

    public string? DecodeStringBinary(IReadableByteBuf buf, int length)
    {
        return buf.ReadUnsignedInt().ToString();
    }

    public short DecodeShortText(IReadableByteBuf buf, int length)
    {
        var result = buf.Atoll(length);
        if ((short)result != result) throw new ArgumentException("Short overflow");
        return (short)result;
    }

    public short DecodeShortBinary(IReadableByteBuf buf, int length)
    {
        var result = buf.ReadUnsignedInt();
        if ((short)result != result) throw new ArgumentException("Short overflow");
        return (short)result;
    }

    public int DecodeIntText(IReadableByteBuf buf, int length)
    {
        return (int)buf.Atoll(length);
    }

    public int DecodeIntBinary(IReadableByteBuf buf, int length)
    {
        var result = buf.ReadUnsignedInt();
        if ((int)result != result) throw new ArgumentException("int overflow");
        return (int)result;
    }

    public long DecodeLongText(IReadableByteBuf buf, int length)
    {
        return buf.Atoll(length);
    }

    public long DecodeLongBinary(IReadableByteBuf buf, int length)
    {
        return buf.ReadUnsignedInt();
    }

    public float DecodeFloatText(IReadableByteBuf buf, int length)
    {
        return float.Parse(buf.ReadAscii(length));
    }

    public float DecodeFloatBinary(IReadableByteBuf buf, int length)
    {
        return buf.ReadUnsignedInt();
    }

    public double DecodeDoubleText(IReadableByteBuf buf, int length)
    {
        return double.Parse(buf.ReadAscii(length));
    }

    public double DecodeDoubleBinary(IReadableByteBuf buf, int length)
    {
        return buf.ReadUnsignedInt();
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
        return buf.ReadUnsignedInt();
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