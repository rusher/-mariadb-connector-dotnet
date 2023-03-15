using System.Collections;
using System.Data;
using System.Data.Common;
using MariadbConnector.client.impl;
using MariadbConnector.client.result.rowdecoder;
using MariadbConnector.client.socket;
using MariadbConnector.client.util;
using MariadbConnector.message;
using MariadbConnector.message.server;
using MariadbConnector.utils;
using MariadbConnector.utils.constant;

namespace MariadbConnector.client.result;

public abstract class AbstractDataReader : DbDataReader, ICompletion
{
    private static readonly BinaryRowDecoder BINARY_ROW_DECODER = new();
    private static readonly TextRowDecoder TEXT_ROW_DECODER = new();
    public static int NULL_LENGTH = -1;

    private readonly int _maxIndex;
    private readonly byte[] _nullBitmap;
    private readonly bool _traceEnable;
    protected CommandBehavior _behavior;
    protected bool _closed;
    protected IContext _context;
    protected Queue<byte[]> _data = new();
    protected ExceptionFactory _exceptionFactory;
    protected MutableInt _fieldIndex = new();
    private int _fieldLength;
    private bool _forceAlias;
    private Dictionary<string, int> _mapper;
    protected IColumnDecoder[] _metaDataList;
    protected bool _outputParameter;

    protected IReader _reader;

    protected StandardReadableByteBuf _rowBuf = new(null, 0);
    protected IRowDecoder _rowDecoder;
    protected MariaDbCommand _statement;

    public AbstractDataReader(
        MariaDbCommand stmt,
        bool binaryProtocol,
        IColumnDecoder[] metaDataList,
        IReader reader,
        IContext context,
        bool traceEnable, CommandBehavior behavior
    )
    {
        _statement = stmt;
        _metaDataList = metaDataList;
        _maxIndex = _metaDataList.Length;
        _reader = reader;
        _exceptionFactory = context.ExceptionFactory;
        _context = context;
        _traceEnable = traceEnable;
        _behavior = behavior;
        if (binaryProtocol)
        {
            _rowDecoder = BINARY_ROW_DECODER;
            _nullBitmap = new byte[(_maxIndex + 9) / 8];
        }
        else
        {
            _rowDecoder = TEXT_ROW_DECODER;
        }
    }

    public AbstractDataReader(IColumnDecoder[] metadataList, byte[] data, IContext context, CommandBehavior behavior)
    {
        _metaDataList = metadataList;
        _maxIndex = _metaDataList.Length;
        _reader = null;
        Loaded = true;
        _exceptionFactory = context.ExceptionFactory;
        _context = context;
        _data.Enqueue(data);
        _statement = null;
        _behavior = behavior;
    }

    public bool Loaded { get; protected set; }

    public override int Depth { get; }
    public override int FieldCount { get; }
    public override bool HasRows { get; }
    public override bool IsClosed { get; }

    public override object this[int ordinal] => throw new NotImplementedException();

    public override object this[string name] => throw new NotImplementedException();

    public override int RecordsAffected { get; }


    public override bool Read()
    {
        if (_data.Count > 0)
        {
            SetRow(_data.Dequeue());
            return true;
        }

        ReadNextPacketSync();
        if (_data.Count > 0)
        {
            SetRow(_data.Dequeue());
            return true;
        }

        // all _data are reads and pointer is after last
        SetNull_rowBuf();
        return false;
    }

    public override async Task<bool> ReadAsync(CancellationToken cancellationToken)
    {
        if (_data.Count > 0)
        {
            SetRow(_data.Dequeue());
            return true;
        }

        await ReadNextPacketAsync(cancellationToken).ConfigureAwait(false);
        if (_data.Count > 0)
        {
            SetRow(_data.Dequeue());
            return true;
        }

        // all _data are reads and pointer is after last
        SetNull_rowBuf();
        return false;
    }


    protected async Task<bool> ReadNextPacketAsync(CancellationToken cancellationToken)
    {
        var buf = await _reader.ReadPacket(cancellationToken, _traceEnable).ConfigureAwait(false);
        switch (buf[0])
        {
            case 0xFF:
                Loaded = true;
                var errorPacket = new ErrorPacket(_reader.ReadableBufFromArray(buf), _context);
                throw _exceptionFactory.Create(
                    errorPacket.Message, errorPacket.SqlState, errorPacket.ErrorCode);

            case 0xFE:
                if ((_context.EofDeprecated && buf.Length < 16777215)
                    || (!_context.EofDeprecated && buf.Length < 8))
                {
                    var readBuf = _reader.ReadableBufFromArray(buf);
                    readBuf.Skip(); // skip header
                    int serverStatus;
                    int warnings;

                    if (!_context.EofDeprecated)
                    {
                        // EOF_Packet
                        warnings = readBuf.ReadUnsignedShort();
                        serverStatus = readBuf.ReadUnsignedShort();
                    }
                    else
                    {
                        // OK_Packet with a 0xFE header
                        readBuf.ReadLongLengthEncodedNotNull(); // skip update count
                        readBuf.ReadLongLengthEncodedNotNull(); // skip insert id
                        serverStatus = readBuf.ReadUnsignedShort();
                        warnings = readBuf.ReadUnsignedShort();
                    }

                    _outputParameter = (serverStatus & ServerStatus.PS_OUT_PARAMETERS) != 0;
                    _context.ServerStatus = serverStatus;
                    _context.Warning = warnings;
                    Loaded = true;
                    return false;
                }

                // continue reading rows
                _data.Enqueue(buf);
                break;

            default:
                _data.Enqueue(buf);
                break;
        }

        return true;
    }

    protected bool ReadNextPacketSync()
    {
        var buf = _reader.ReadPacketSync(_traceEnable);
        switch (buf[0])
        {
            case 0xFF:
                Loaded = true;
                var errorPacket = new ErrorPacket(_reader.ReadableBufFromArray(buf), _context);
                throw _exceptionFactory.Create(
                    errorPacket.Message, errorPacket.SqlState, errorPacket.ErrorCode);

            case 0xFE:
                if ((_context.EofDeprecated && buf.Length < 16777215)
                    || (!_context.EofDeprecated && buf.Length < 8))
                {
                    var readBuf = _reader.ReadableBufFromArray(buf);
                    readBuf.Skip(); // skip header
                    int serverStatus;
                    int warnings;

                    if (!_context.EofDeprecated)
                    {
                        // EOF_Packet
                        warnings = readBuf.ReadUnsignedShort();
                        serverStatus = readBuf.ReadUnsignedShort();
                    }
                    else
                    {
                        // OK_Packet with a 0xFE header
                        readBuf.ReadLongLengthEncodedNotNull(); // skip update count
                        readBuf.ReadLongLengthEncodedNotNull(); // skip insert id
                        serverStatus = readBuf.ReadUnsignedShort();
                        warnings = readBuf.ReadUnsignedShort();
                    }

                    _outputParameter = (serverStatus & ServerStatus.PS_OUT_PARAMETERS) != 0;
                    _context.ServerStatus = serverStatus;
                    _context.Warning = warnings;
                    Loaded = true;
                    return false;
                }

                // continue reading rows
                _data.Enqueue(buf);
                break;

            default:
                _data.Enqueue(buf);
                break;
        }

        return true;
    }

    private void SetRow(byte[] row)
    {
        _rowBuf.Buf(row, row.Length, 0);
        _fieldIndex.Value = -1;
    }

    private void SetNull_rowBuf()
    {
        _rowBuf.Buf(null, 0, 0);
    }

    public override async Task CloseAsync()
    {
        if (!Loaded)
            try
            {
                await SkipRemaining().ConfigureAwait(false);
            }
            catch (Exception ioe)
            {
                throw _exceptionFactory.Create("Error while streaming resultSet data", "08000", ioe);
            }

        _closed = true;
        if (_behavior == CommandBehavior.CloseConnection && _statement != null)
            await _statement.CloseConnection().ConfigureAwait(false);
    }

    public async Task CloseFromCommandClose(SemaphoreSlim lockObj)
    {
        await lockObj.WaitAsync();
        try
        {
            await FetchRemaining(CancellationToken.None).ConfigureAwait(false);
            _closed = true;
        }
        finally
        {
            lockObj.Release();
        }
    }

    internal abstract Task FetchRemaining(CancellationToken cancellationToken);

    protected async Task SkipRemaining()
    {
        while (true)
        {
            var buf = await _reader.ReadReusablePacket(CancellationToken.None, _traceEnable).ConfigureAwait(false);
            switch (buf.GetUnsignedByte())
            {
                case 0xFF:
                    Loaded = true;
                    var errorPacket = new ErrorPacket(buf, _context);
                    throw _exceptionFactory.Create(
                        errorPacket.Message, errorPacket.SqlState, errorPacket.ErrorCode);

                case 0xFE:
                    if ((_context.EofDeprecated && buf.ReadableBytes() < 0xffffff)
                        || (!_context.EofDeprecated && buf.ReadableBytes() < 8))
                    {
                        buf.Skip(); // skip header
                        int serverStatus;
                        int warnings;

                        if (!_context.EofDeprecated)
                        {
                            // EOF_Packet
                            warnings = buf.ReadUnsignedShort();
                            serverStatus = buf.ReadUnsignedShort();
                        }
                        else
                        {
                            // OK_Packet with a 0xFE header
                            buf.ReadLongLengthEncodedNotNull(); // skip update count
                            buf.ReadLongLengthEncodedNotNull(); // skip insert id
                            serverStatus = buf.ReadUnsignedShort();
                            warnings = buf.ReadUnsignedShort();
                        }

                        _outputParameter = (serverStatus & ServerStatus.PS_OUT_PARAMETERS) != 0;
                        _context.ServerStatus = serverStatus;
                        _context.Warning = warnings;
                        Loaded = true;
                        return;
                    }

                    break;
            }
        }
    }

    private void CheckIndex(int index)
    {
        if (index < 0 || index >= _maxIndex)
            throw new ArgumentOutOfRangeException(
                $"Wrong index position. Is {index} but must be in 1-{_maxIndex} range");
        if (_rowBuf.Buf() == null) throw new InvalidOperationException("wrong row position");
    }

    private int FindColumn(string label)
    {
        if (label == null) throw new ArgumentException("null is not a valid label value");
        if (_mapper == null)
        {
            _mapper = new Dictionary<string, int>();
            for (var i = 0; i < _maxIndex; i++)
            {
                var ci = _metaDataList[i];
                var columnAlias = ci.GetColumnAlias();
                if (columnAlias != null)
                {
                    columnAlias = columnAlias.ToLower();
                    _mapper.Add(columnAlias, i);
                    var tableAlias = ci.GetTableAlias();
                    var tableLabel = tableAlias != null ? tableAlias : ci.GetTable();
                    _mapper.Add(tableLabel.ToLower() + "." + columnAlias, i);
                }
            }
        }

        int ind;
        if (_mapper.TryGetValue(label.ToLower(), out ind)) return ind;
        throw new ArgumentException(
            $"Unknown label '{label}'. Possible value {string.Join(",", _mapper.Keys.ToList())}");
    }

    public override bool GetBoolean(int ordinal)
    {
        CheckIndex(ordinal);
        _fieldLength =
            _rowDecoder.SetPosition(
                ordinal, _fieldIndex, _maxIndex, _rowBuf, _nullBitmap, _metaDataList);
        if (_fieldLength == NULL_LENGTH) return false;
        return _rowDecoder.DecodeBoolean(_metaDataList, _fieldIndex, _rowBuf, _fieldLength);
    }

    public override byte GetByte(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override long GetBytes(int ordinal, long _dataOffset, byte[]? buffer, int bufferOffset, int length)
    {
        throw new NotImplementedException();
    }

    public override char GetChar(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override long GetChars(int ordinal, long _dataOffset, char[]? buffer, int bufferOffset, int length)
    {
        throw new NotImplementedException();
    }

    public override string GetDataTypeName(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override DateTime GetDateTime(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override decimal GetDecimal(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override double GetDouble(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override IEnumerator GetEnumerator()
    {
        throw new NotImplementedException();
    }

    public override Type GetFieldType(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override float GetFloat(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override Guid GetGuid(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override short GetInt16(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override int GetInt32(int ordinal)
    {
        CheckIndex(ordinal);
        _fieldLength =
            _rowDecoder.SetPosition(
                ordinal, _fieldIndex, _maxIndex, _rowBuf, _nullBitmap, _metaDataList);
        if (_fieldLength == NULL_LENGTH) return 0;
        return _rowDecoder.DecodeInt(_metaDataList, _fieldIndex, _rowBuf, _fieldLength);
    }

    public override long GetInt64(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override string GetName(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override int GetOrdinal(string name)
    {
        throw new NotImplementedException();
    }

    public override string GetString(int ordinal)
    {
        CheckIndex(ordinal);
        _fieldLength =
            _rowDecoder.SetPosition(
                ordinal, _fieldIndex, _maxIndex, _rowBuf, _nullBitmap, _metaDataList);
        if (_fieldLength == NULL_LENGTH) return null;
        return _rowDecoder.DecodeString(_metaDataList, _fieldIndex, _rowBuf, _fieldLength);
    }

    public override object GetValue(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override int GetValues(object[] values)
    {
        throw new NotImplementedException();
    }

    public override bool IsDBNull(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override bool NextResult()
    {
        throw new NotImplementedException();
    }
}