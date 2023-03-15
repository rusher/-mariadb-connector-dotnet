using System.Data;
using System.Data.Common;
using MariadbConnector.client.result;
using MariadbConnector.message;
using MariadbConnector.message.client;
using MariadbConnector.message.server;
using MariadbConnector.utils;
using MariadbConnector.utils.constant;
using MariadbConnector.utils.exception;
using ConnectionState = System.Data.ConnectionState;

namespace MariadbConnector;

public class MariaDbCommand : DbCommand
{
    private readonly PrepareResultPacket _prepare = null;
    private ClientParser _clientParser;
    private bool _closed;
    private MariaDbConnection _conn;
    private ICompletion _currResult;
    private SemaphoreSlim _lock;
    private readonly MariaDbParameterCollection _parameters = new();
    private List<ICompletion> _results;
    public Stream LocalInfileInputStream;

    public MariaDbCommand(MariaDbConnection? dbConnection)
    {
        _conn = dbConnection;
        _lock = dbConnection!.Lock;
    }

    protected override DbConnection? DbConnection
    {
        get => _conn;
        set
        {
            _conn = (MariaDbConnection?)value;
            _lock = _conn!.Lock;
        }
    }

    public override string CommandText { get; set; }

    public override int CommandTimeout { get; set; }
    public override CommandType CommandType { get; set; }
    protected override DbTransaction? DbTransaction { get; set; }
    public override bool DesignTimeVisible { get; set; }
    public override UpdateRowSource UpdatedRowSource { get; set; }

    protected override DbParameterCollection DbParameterCollection => _parameters;


    public override void Cancel()
    {
        throw new NotImplementedException();
    }


    internal async Task CloseConnection()
    {
        await CloseAsync();
        await _conn.CloseAsync();
    }

    public async Task CloseAsync()
    {
        if (!_closed)
        {
            _closed = true;

            if (_currResult != null && _currResult is AbstractDataReader)
                await ((AbstractDataReader)_currResult).CloseFromCommandClose(_lock);

            // close result-set
            if (_results != null && _results.Any())
                foreach (var completion in _results)
                    if (completion is AbstractDataReader)
                        await ((AbstractDataReader)completion).CloseFromCommandClose(_lock);
        }
    }

    private void CheckNotClosed()
    {
        if (_closed) throw new SqlException("Cannot do an operation on a closed statement");
        if (_conn == null)
            throw new SqlException("Cannot do an operation without connection set");
        if (_conn.State != ConnectionState.Open)
            throw new SqlException(
                $"Cannot do an operation without open connection (State is {_conn.State.ToString()})");
        if (_conn.Client.IsClosed()) throw new SqlException("Cannot do an operation on closed connection");
        if (CommandText == null) throw new SqlException("CommandText need to be set to be execute");
    }


    private ExceptionFactory ExceptionFactory()
    {
        return _conn!.ExceptionFactory!.Of(this);
    }


    public IColumnDecoder[] _getMeta()
    {
        return _prepare.Columns;
    }

    internal void UpdateMeta(IColumnDecoder[] cols)
    {
        _prepare.Columns = cols;
    }

    internal void FetchRemaining()
    {
        if (_currResult != null && _currResult is AbstractDataReader)
        {
            var result = (AbstractDataReader)_currResult;
            //TODO diego fetch
            // result.FetchRemaining();
            // if (result.Streaming()
            //     && (_conn!.Client!.Context.ServerStatus & ServerStatus.MORE_RESULTS_EXISTS) > 0)
            //     _conn!.Client!.ReadStreamingResults(
            //         _results, CommandBehavior.Default);
        }
    }

    protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior,
        CancellationToken cancellationToken)
    {
        ExecuteInternal(IoBehavior.Asynchronous, cancellationToken, CommandBehavior.Default);
        if (_currResult is DbDataReader) return (DbDataReader)_currResult;
        return new StreamingDataReader(new IColumnDecoder[0], new byte[0], _conn!.Client!.Context, behavior);
    }

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        ExecuteInternal(IoBehavior.Synchronous, CancellationToken.None, CommandBehavior.Default);
        if (_currResult is DbDataReader) return (DbDataReader)_currResult;
        return new StreamingDataReader(new IColumnDecoder[0], new byte[0], _conn!.Client!.Context, behavior);
    }

    public override int ExecuteNonQuery()
    {
        ExecuteInternal(IoBehavior.Synchronous, CancellationToken.None, CommandBehavior.Default);
        if (_currResult is DbDataReader)
            throw ExceptionFactory()
                .Create("the given SQL statement produces an unexpected ResultSet object", "HY000");
        return (int)((OkPacket)_currResult).AffectedRows;
    }

    public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
    {
        ExecuteInternal(IoBehavior.Asynchronous, cancellationToken, CommandBehavior.Default);
        if (_currResult is DbDataReader)
            throw ExceptionFactory()
                .Create("the given SQL statement produces an unexpected ResultSet object", "HY000");
        return (int)((OkPacket)_currResult).AffectedRows;
    }

    private void ExecuteInternal(IoBehavior ioBehavior, CancellationToken cancellationToken, CommandBehavior behavior)
    {
        CheckNotClosed();
        //_lock.Wait();
        try
        {
            if (_clientParser == null)
            {
                var noBackslashEscapes =
                    (_conn.Client.Context.ServerStatus & ServerStatus.NO_BACKSLASH_ESCAPES) > 0;
                _clientParser = ClientParser.ParameterParts(CommandText, noBackslashEscapes);
            }

            IClientMessage message;
            if (_clientParser.ParamCount > 0)
                message = new QueryWithParametersPacket(_clientParser, _parameters,
                    _conn!.CanUseServerTimeout ? CommandTimeout : 0, LocalInfileInputStream);
            else
                message = new QueryPacket(CommandText, _conn!.CanUseServerTimeout ? CommandTimeout : 0,
                    LocalInfileInputStream);
            _results =
                _conn.Client
                    .Execute(
                        ioBehavior,
                        cancellationToken,
                        message,
                        this,
                        behavior,
                        false);
            _currResult = _results[0];
            _results.RemoveAt(0);
        }
        finally
        {
            LocalInfileInputStream = null;
            //_lock.Release();
        }
    }

    public override object? ExecuteScalar()
    {
        throw new NotImplementedException();
    }

    public override void Prepare()
    {
        throw new NotImplementedException();
    }


    protected override DbParameter CreateDbParameter()
    {
        return new MariaDbParameter();
    }

    public new MariaDbParameter CreateParameter()
    {
        return (MariaDbParameter)base.CreateParameter();
    }

    private enum ParsingType
    {
        NoParameter,
        ClientSide,
        ServerSide
    }
}