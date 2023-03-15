using MariadbConnector.client;
using MariadbConnector.client.util;
using MariadbConnector.utils;
using MariadbConnector.utils.constant;

namespace MariadbConnector.message.server;

public class ColumnDefinitionPacket : IColumn, IServerMessage
{
    private readonly IReadableByteBuf _buf;
    protected readonly int _charset;
    protected readonly long _columnLength;
    protected readonly DataType _dataType;
    protected readonly byte _decimals;
    protected readonly string? _extTypeFormat;
    protected readonly string? _extTypeName;
    private readonly int _flags;
    private readonly int[] _stringPos;

    public ColumnDefinitionPacket(
        IReadableByteBuf buf,
        int charset,
        long columnLength,
        DataType dataType,
        byte decimals,
        int flags,
        int[] stringPos,
        string? extTypeName,
        string? extTypeFormat)
    {
        _buf = buf;
        _charset = charset;
        _columnLength = columnLength;
        _dataType = dataType;
        _decimals = decimals;
        _flags = flags;
        _stringPos = stringPos;
        _extTypeName = extTypeName;
        _extTypeFormat = extTypeFormat;
    }

    public string GetSchema()
    {
        _buf.SetPos(_stringPos[0]);
        return _buf.ReadString(_buf.ReadIntLengthEncodedNotNull());
    }

    public string GetTable()
    {
        _buf.SetPos(_stringPos[1]);
        return _buf.ReadString(_buf.ReadIntLengthEncodedNotNull());
    }

    public string GetTableAlias()
    {
        _buf.SetPos(_stringPos[2]);
        return _buf.ReadString(_buf.ReadIntLengthEncodedNotNull());
    }

    public string GetColumnName()
    {
        _buf.SetPos(_stringPos[3]);
        return _buf.ReadString(_buf.ReadIntLengthEncodedNotNull());
    }

    public string GetColumnAlias()
    {
        _buf.SetPos(_stringPos[4]);
        return _buf.ReadString(_buf.ReadIntLengthEncodedNotNull());
    }

    public long GetColumnLength()
    {
        return _columnLength;
    }

    public DataType GetType()
    {
        return _dataType;
    }

    public byte GetDecimals()
    {
        return _decimals;
    }

    public bool IsSigned()
    {
        return (_flags & ColumnFlags.UNSIGNED) == 0;
    }

    public int GetDisplaySize()
    {
        if (!IsBinary()
            && (_dataType == DataType.VARCHAR
                || _dataType == DataType.JSON
                || _dataType == DataType.ENUM
                || _dataType == DataType.SET
                || _dataType == DataType.VARSTRING
                || _dataType == DataType.STRING
                || _dataType == DataType.BLOB
                || _dataType == DataType.TINYBLOB
                || _dataType == DataType.MEDIUMBLOB
                || _dataType == DataType.LONGBLOB))
        {
            int? maxWidth = CharsetEncodingLength.MaxCharlen[_charset];
            if (maxWidth != null) return (int)(_columnLength / maxWidth.Value);
        }

        return (int)_columnLength;
    }

    public bool IsPrimaryKey()
    {
        return (_flags & ColumnFlags.PRIMARY_KEY) > 0;
    }

    public bool IsAutoIncrement()
    {
        return (_flags & ColumnFlags.AUTO_INCREMENT) > 0;
    }

    public bool HasDefault()
    {
        return (_flags & ColumnFlags.NO_DEFAULT_VALUE_FLAG) == 0;
    }

    // doesn't use & 128 bit filter, because char binary and varchar binary are not binary (handle
    // like string), but have the binary flag
    public bool IsBinary()
    {
        return _charset == 63;
    }

    public int GetFlags()
    {
        return _flags;
    }

    public string GetExtTypeName()
    {
        return _extTypeName;
    }

    public string GetDataTypeName()
    {
        if (_dataType == DataType.VARCHAR
            || _dataType == DataType.JSON
            || _dataType == DataType.ENUM
            || _dataType == DataType.SET
            || _dataType == DataType.VARSTRING
            || _dataType == DataType.STRING
            || _dataType == DataType.BLOB
            || _dataType == DataType.TINYBLOB
            || _dataType == DataType.MEDIUMBLOB
            || _dataType == DataType.LONGBLOB)
        {
            if (!IsBinary())
            {
                int? maxWidth = CharsetEncodingLength.MaxCharlen[_charset];
                if (maxWidth != null) return _dataType + "(" + (int)(_columnLength / maxWidth.Value) + ")";
            }

            return _dataType + "(" + _columnLength + ")";
        }

        return _dataType.ToString();
    }

    public virtual int GetPrecision()
    {
        return (int)GetColumnLength();
    }
}