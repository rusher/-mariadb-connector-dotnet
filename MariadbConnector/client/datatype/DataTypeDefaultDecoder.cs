using MariadbConnector.client.decoder;
using MariadbConnector.client.util;
using MariadbConnector.message.server;

namespace MariadbConnector.client;

public class DataTypeDefaultDecoder
{
    public static
        Dictionary<DataType, Func<IReadableByteBuf, int, long, DataType, byte, int, int[], string, string,
            IColumnDecoder>> signedDecoders;

    public static
        Dictionary<DataType, Func<IReadableByteBuf, int, long, DataType, byte, int, int[], string, string,
            IColumnDecoder>> unsignedDecoders;

    public static Func<IReadableByteBuf, int, long, DataType, byte, int, int[], string, string, IColumnDecoder>
        BigDecimalLamdba = (buf,
            charset,
            length,
            dataType,
            decimals,
            flags,
            stringPos,
            extTypeName,
            extTypeFormat) => new BigDecimalColumn(buf, charset, length, dataType, decimals, flags, stringPos,
            extTypeName, extTypeFormat);

    public static Func<IReadableByteBuf, int, long, DataType, byte, int, int[], string, string, IColumnDecoder>
        SignedInt8Lamdba = (buf,
            charset,
            length,
            dataType,
            decimals,
            flags,
            stringPos,
            extTypeName,
            extTypeFormat) => new SignedInt8Column(buf, charset, length, dataType, decimals, flags, stringPos,
            extTypeName, extTypeFormat);

    public static Func<IReadableByteBuf, int, long, DataType, byte, int, int[], string, string, IColumnDecoder>
        UnsignedInt8Lamdba = (buf,
            charset,
            length,
            dataType,
            decimals,
            flags,
            stringPos,
            extTypeName,
            extTypeFormat) => new UnsignedInt8Column(buf, charset, length, dataType, decimals, flags, stringPos,
            extTypeName, extTypeFormat);

    public static Func<IReadableByteBuf, int, long, DataType, byte, int, int[], string, string, IColumnDecoder>
        SignedInt32Lamdba = (buf,
            charset,
            length,
            dataType,
            decimals,
            flags,
            stringPos,
            extTypeName,
            extTypeFormat) => new SignedInt32Column(buf, charset, length, dataType, decimals, flags, stringPos,
            extTypeName, extTypeFormat);

    public static Func<IReadableByteBuf, int, long, DataType, byte, int, int[], string, string, IColumnDecoder>
        UnsignedInt32Lamdba = (buf,
            charset,
            length,
            dataType,
            decimals,
            flags,
            stringPos,
            extTypeName,
            extTypeFormat) => new UnsignedInt32Column(buf, charset, length, dataType, decimals, flags, stringPos,
            extTypeName, extTypeFormat);


    public static Func<IReadableByteBuf, int, long, DataType, byte, int, int[], string, string, IColumnDecoder>
        GuidLamdba = (buf,
            charset,
            length,
            dataType,
            decimals,
            flags,
            stringPos,
            extTypeName,
            extTypeFormat) => new GuidColumn(buf, charset, length, dataType, decimals, flags, stringPos, extTypeName,
            extTypeFormat);

    public static Func<IReadableByteBuf, int, long, DataType, byte, int, int[], string, string, IColumnDecoder>
        StringLamdba = (buf,
            charset,
            length,
            dataType,
            decimals,
            flags,
            stringPos,
            extTypeName,
            extTypeFormat) => new StringColumn(buf, charset, length, dataType, decimals, flags, stringPos, extTypeName,
            extTypeFormat);

    static DataTypeDefaultDecoder()
    {
        signedDecoders =
            new Dictionary<DataType, Func<IReadableByteBuf, int, long, DataType, byte, int, int[], string, string,
                IColumnDecoder>>();
        signedDecoders.Add(DataType.OLDDECIMAL, BigDecimalLamdba);
        signedDecoders.Add(DataType.TINYINT, SignedInt8Lamdba);
        signedDecoders.Add(DataType.INTEGER, SignedInt32Lamdba);
        signedDecoders.Add(DataType.VARSTRING, StringLamdba);
        signedDecoders.Add(DataType.VARCHAR, StringLamdba);
        signedDecoders.Add(DataType.STRING, StringLamdba);
        signedDecoders.Add(DataType.ENUM, StringLamdba);
        signedDecoders.Add(DataType.NULL, StringLamdba);
        signedDecoders.Add(DataType.SET, StringLamdba);

        unsignedDecoders =
            new Dictionary<DataType, Func<IReadableByteBuf, int, long, DataType, byte, int, int[], string, string,
                IColumnDecoder>>();
        unsignedDecoders.Add(DataType.OLDDECIMAL, BigDecimalLamdba);
        unsignedDecoders.Add(DataType.TINYINT, UnsignedInt8Lamdba);
        unsignedDecoders.Add(DataType.INTEGER, UnsignedInt32Lamdba);
        unsignedDecoders.Add(DataType.VARSTRING, StringLamdba);
        unsignedDecoders.Add(DataType.VARCHAR, StringLamdba);
        unsignedDecoders.Add(DataType.STRING, StringLamdba);
        unsignedDecoders.Add(DataType.ENUM, StringLamdba);
        unsignedDecoders.Add(DataType.NULL, StringLamdba);
        unsignedDecoders.Add(DataType.SET, StringLamdba);
    }
}