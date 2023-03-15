using System.Globalization;
using MariadbConnector.client.util;
using MariadbConnector.message.server;

namespace MariadbConnector.client.decoder;

public class StringColumn : ColumnDefinitionPacket, IColumnDecoder
{
    public StringColumn(
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
        if (IsBinary())
        {
            var arr = new byte[length];
            buf.ReadBytes(arr);
            return arr;
        }

        return buf.ReadString(length);
    }

    public object GetDefaultBinary(Configuration conf, IReadableByteBuf buf, int length)
    {
        return GetDefaultText(conf, buf, length);
    }

    public bool DecodeBooleanText(IReadableByteBuf buf, int length)
    {
        return !string.Equals("0", buf.ReadAscii(length));
    }

    public bool DecodeBooleanBinary(IReadableByteBuf buf, int length)
    {
        return buf.ReadByte() != 0;
    }

    public byte DecodeByteText(IReadableByteBuf buf, int length)
    {
        if (length > 0) throw new ArgumentException("byte overflow");
        return buf.ReadByte();
    }

    public byte DecodeByteBinary(IReadableByteBuf buf, int length)
    {
        return buf.ReadByte();
    }

    public string DecodeStringText(IReadableByteBuf buf, int length)
    {
        return buf.ReadString(length);
    }

    public string DecodeStringBinary(IReadableByteBuf buf, int length)
    {
        return buf.ReadString(length);
    }

    public short DecodeShortText(IReadableByteBuf buf, int length)
    {
        var str = buf.ReadString(length);
        short s;
        if (short.TryParse(str, out s)) return s;
        throw new ArgumentException($"value '{str}' cannot be decoded as short");
    }

    public short DecodeShortBinary(IReadableByteBuf buf, int length)
    {
        return DecodeShortText(buf, length);
    }

    public int DecodeIntText(IReadableByteBuf buf, int length)
    {
        var str = buf.ReadString(length);
        int s;
        if (int.TryParse(str, out s)) return s;
        throw new ArgumentException($"value '{str}' cannot be decoded as int");
    }

    public int DecodeIntBinary(IReadableByteBuf buf, int length)
    {
        return DecodeIntText(buf, length);
    }

    public long DecodeLongText(IReadableByteBuf buf, int length)
    {
        var str = buf.ReadString(length);
        long s;
        if (long.TryParse(str, out s)) return s;
        throw new ArgumentException($"value '{str}' cannot be decoded as long");
    }

    public long DecodeLongBinary(IReadableByteBuf buf, int length)
    {
        return DecodeLongText(buf, length);
    }

    public float DecodeFloatText(IReadableByteBuf buf, int length)
    {
        var str = buf.ReadString(length);
        float s;
        if (float.TryParse(str, out s)) return s;
        throw new ArgumentException($"value '{str}' cannot be decoded as float");
    }

    public float DecodeFloatBinary(IReadableByteBuf buf, int length)
    {
        return DecodeFloatText(buf, length);
    }

    public double DecodeDoubleText(IReadableByteBuf buf, int length)
    {
        var str = buf.ReadString(length);
        double s;
        if (double.TryParse(str, out s)) return s;
        throw new ArgumentException($"value '{str}' cannot be decoded as double");
    }

    public double DecodeDoubleBinary(IReadableByteBuf buf, int length)
    {
        return DecodeDoubleText(buf, length);
    }

    public DateTime DecodeDateTimeText(IReadableByteBuf buf, int length)
    {
        var str = buf.ReadString(length);
        DateTime s;
        if (DateTime.TryParseExact(str, "yyyy-MM-dd hh:mm:ss.ffffff", CultureInfo.InvariantCulture, DateTimeStyles.None,
                out s)) return s;
        throw new ArgumentException($"value '{str}' cannot be decoded as double");
    }

    public DateTime DecodeDateTimeBinary(IReadableByteBuf buf, int length)
    {
        return DecodeDateTimeText(buf, length);
    }

    public decimal DecodeDecimalText(IReadableByteBuf buf, int length)
    {
        var str = buf.ReadString(length);
        decimal s;
        if (decimal.TryParse(str, out s)) return s;
        throw new ArgumentException($"value '{str}' cannot be decoded as decimal");
    }

    public decimal DecodeDecimalBinary(IReadableByteBuf buf, int length)
    {
        return DecodeDecimalText(buf, length);
    }

    public Guid DecodeGuidText(IReadableByteBuf buf, int length)
    {
        var str = buf.ReadString(length);
        Guid s;
        if (Guid.TryParse(str, out s)) return s;
        throw new ArgumentException($"value '{str}' cannot be decoded as Guid");
    }

    public Guid DecodeGuidBinary(IReadableByteBuf buf, int length)
    {
        return DecodeGuidText(buf, length);
    }
}