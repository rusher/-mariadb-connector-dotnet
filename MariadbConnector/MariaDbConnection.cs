using System.Data;
using System.Data.Common;
using MariadbConnector.client;
using MariadbConnector.client.impl;
using MariadbConnector.utils;

namespace MariadbConnector;

public sealed class MariaDbConnection : DbConnection
{
    internal readonly SemaphoreSlim Lock = new(1);
    private Configuration _conf;
    private string _connectionString;
    private ConnectionState _state;

    public bool CanUseServerTimeout;
    internal IClient Client;

    public MariaDbConnection(string? connectionString)
    {
        _state = ConnectionState.Closed;
        ConnectionString = connectionString ?? "";
        Client = null;
    }

    internal ExceptionFactory ExceptionFactory { get; set; }

    public override string ConnectionString
    {
        get => _connectionString;
        set
        {
            if (State == ConnectionState.Closed)
            {
                _conf = new Configuration(value);
                _connectionString = value;
            }
            else
            {
                throw new InvalidOperationException("Cannot set ConnectionString while connection is used");
            }
        }
    }

    public override string Database { get; }

    public override ConnectionState State => _state;

    public override string DataSource { get; }
    public override string ServerVersion { get; }

    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
    {
        throw new NotImplementedException();
    }

    public override void ChangeDatabase(string databaseName)
    {
        throw new NotImplementedException();
    }

    protected override void Dispose(bool disposing)
    {
        try
        {
            if (disposing)
                Close();
        }
        finally
        {
            base.Dispose(disposing);
        }
    }

    public override void Close()
    {
        CloseAsync().GetAwaiter().GetResult();
    }

    public override async Task CloseAsync()
    {
        switch (State)
        {
            case ConnectionState.Connecting:
            case ConnectionState.Open:
            case ConnectionState.Executing:
            case ConnectionState.Fetching:
                Client.CloseAsync();
                _state = ConnectionState.Closed;
                break;
        }
    }

    public override void Open()
    {
        if (State != ConnectionState.Closed)
            throw new InvalidOperationException($"Cannot Open: State is {State}.");
        Lock.WaitAsync();
        try
        {
            _state = ConnectionState.Connecting;
            Client = StandardClient.BuildClient(IoBehavior.Synchronous, CancellationToken.None, _conf, Lock,
                HostAddress.From(_conf.Server, _conf.Port, true), false).GetAwaiter().GetResult();
            ExceptionFactory = Client.ExceptionFactory.SetConnection(this);
            _state = ConnectionState.Open;
        }
        finally
        {
            Lock.Release();
        }
    }

    public override async Task OpenAsync(CancellationToken cancellationToken)
    {
        if (State != ConnectionState.Closed)
            throw new InvalidOperationException($"Cannot Open: State is {State}.");
        Lock.WaitAsync();
        try
        {
            _state = ConnectionState.Connecting;
            Client = await StandardClient.BuildClient(IoBehavior.Asynchronous, cancellationToken, _conf, Lock,
                HostAddress.From(_conf.Server, _conf.Port, true), false);
            ExceptionFactory = Client.ExceptionFactory.SetConnection(this);
            _state = ConnectionState.Open;
        }
        finally
        {
            Lock.Release();
        }
    }

    protected override MariaDbCommand CreateDbCommand()
    {
        return new MariaDbCommand(this);
    }
}