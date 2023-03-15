using System.Data;
using System.IO.Pipes;
using System.Net.Sockets;
using MariadbConnector.client.context;
using MariadbConnector.client.result;
using MariadbConnector.client.socket;
using MariadbConnector.client.util;
using MariadbConnector.message;
using MariadbConnector.message.client;
using MariadbConnector.message.server;
using MariadbConnector.plugin.authentication;
using MariadbConnector.utils;
using MariadbConnector.utils.constant;
using MariadbConnector.utils.exception;
using MariadbConnector.utils.log;

namespace MariadbConnector.client.impl;

public class StandardClient : IClient
{
    private static readonly Ilogger _logger = Loggers.getLogger("StandardClient");
    private readonly MutableByte _compressionSequence = new();
    private readonly Configuration _conf;
    private readonly bool _disablePipeline;
    private readonly SemaphoreSlim _lock;
    private readonly MutableByte _sequence = new();
    private bool _closed;
    private Stream _in;
    private Stream _out;
    private IReader _reader;
    private Socket _socket;
    private int _socketTimeout;
    private Stream _stream;
    private IClientMessage _streamMsg;
    private MariaDbCommand _streamStmt;
    private IWriter _writer;


    private StandardClient(
        Configuration conf, SemaphoreSlim lockObj, HostAddress hostAddress)
    {
        _conf = conf;
        _lock = lockObj;
        HostAddress = hostAddress;
        ExceptionFactory = new ExceptionFactory(conf, hostAddress);
        _disablePipeline = conf.DisablePipeline;
    }

    public HostAddress HostAddress { get; }
    public ExceptionFactory ExceptionFactory { get; }

    public IContext? Context { get; set; }

    public void SetReadOnly(bool readOnly)
    {
        if (_closed) throw new DbNonTransientConnectionException("Connection is closed", 1220, "08000");
    }

    public List<ICompletion> Execute(IoBehavior ioBehavior, CancellationToken cancellationToken, IClientMessage message,
        bool canRedo)
    {
        return Execute(ioBehavior, cancellationToken,
            message,
            null,
            0,
            canRedo);
    }

    public List<ICompletion> Execute(IoBehavior ioBehavior, CancellationToken cancellationToken,
        IClientMessage message, MariaDbCommand stmt, bool canRedo)
    {
        return Execute(ioBehavior, cancellationToken,
            message,
            stmt,
            0,
            canRedo);
    }

    public List<ICompletion> ExecutePipeline(IoBehavior ioBehavior, CancellationToken cancellationToken,
        IClientMessage[] messages,
        MariaDbCommand stmt,
        CommandBehavior behavior,
        bool canRedo)
    {
        var results = new List<ICompletion>();

        var readCounter = 0;
        var responseMsg = new int[messages.Length];
        try
        {
            if (_disablePipeline)
            {
                for (readCounter = 0; readCounter < messages.Length; readCounter++)
                    results.AddRange(
                        Execute(ioBehavior, cancellationToken,
                            messages[readCounter],
                            stmt,
                            behavior,
                            canRedo));
            }
            else
            {
                for (var i = 0; i < messages.Length; i++)
                    responseMsg[i] = SendQuery(ioBehavior, cancellationToken, messages[i]);
                while (readCounter < messages.Length)
                {
                    readCounter++;
                    for (var j = 0; j < responseMsg[readCounter - 1]; j++)
                        results.AddRange(
                            ReadResponse(
                                ioBehavior,
                                cancellationToken,
                                stmt,
                                messages[readCounter - 1],
                                behavior));
                }
            }

            return results;
        }
        catch (Exception sqlException)
        {
            if (!_closed)
            {
                // read remaining results
                for (var i = readCounter; i < messages.Length; i++)
                for (var j = 0; j < responseMsg[i]; j++)
                    try
                    {
                        results.AddRange(
                            ReadResponse(ioBehavior, cancellationToken,
                                stmt,
                                messages[i],
                                behavior));
                    }
                    catch (Exception e)
                    {
                        // eat
                    }

                // prepare associated to PrepareStatement need to be uncached
                foreach (var result in results)
                    if (result is PrepareResultPacket)
                        try
                        {
                            ((PrepareResultPacket)result).DecrementUse(this, stmt);
                        }
                        catch (Exception e)
                        {
                            // eat
                        }
            }

            throw sqlException;
        }
    }

    public List<ICompletion> Execute(IoBehavior ioBehavior, CancellationToken cancellationToken,
        IClientMessage message,
        MariaDbCommand stmt,
        CommandBehavior behavior,
        bool canRedo)
    {
        var nbResp = SendQuery(ioBehavior, cancellationToken, message);
        if (nbResp == 1)
            return ReadResponse(
                ioBehavior, cancellationToken,
                stmt,
                message,
                behavior);

        if (_streamStmt != null)
        {
            _streamStmt.FetchRemaining();
            _streamStmt = null;
        }

        var completions = new List<ICompletion>();
        try
        {
            while (nbResp-- > 0)
                ReadResults(
                    ioBehavior, cancellationToken,
                    stmt,
                    message,
                    completions,
                    behavior);
            return completions;
        }
        catch (Exception e)
        {
            while (nbResp-- > 0)
                try
                {
                    ReadResults(
                        ioBehavior, cancellationToken,
                        stmt,
                        message,
                        completions,
                        behavior);
                }
                catch (Exception ee)
                {
                    // eat
                }

            throw e;
        }
    }

    public void ClosePrepare(IPrepare prepare)
    {
        CheckNotClosed();
        try
        {
            var packet = new ClosePreparePacket(prepare.StatementId);
            var payload = packet.BuildPayLoad(Context);
            packet.WriteSync(payload, _writer);
        }
        catch (Exception ioException)
        {
            destroySocket();
            throw ExceptionFactory.Create(
                "Socket error during post connection queries: " + ioException.Message,
                "08000",
                ioException);
        }
    }

    public void ReadStreamingResults(
        IoBehavior ioBehavior, CancellationToken cancellationToken,
        List<ICompletion> completions,
        CommandBehavior behavior)
    {
        if (_streamStmt != null)
            ReadResults(
                ioBehavior,
                cancellationToken,
                _streamStmt,
                _streamMsg,
                completions,
                behavior);
    }

    public bool IsClosed()
    {
        return _closed;
    }

    public async Task CloseAsync()
    {
        if (!_closed)
        {
            _closed = true;
            try
            {
                var payload = QuitPacket.INSTANCE.BuildPayLoad(Context);
                await QuitPacket.INSTANCE.WriteAsync(CancellationToken.None, payload, _writer);
            }
            catch (IOException e)
            {
                // eat
            }

            CloseSocket();
        }
    }

    public bool IsPrimary()
    {
        return HostAddress.Primary == true;
    }

    public void Reset()
    {
        Context.ResetStateFlag();
        Context.ResetPrepareCache();
    }

    public static async Task<StandardClient> BuildClient(IoBehavior ioBehavior, CancellationToken cancellationToken,
        Configuration conf,
        SemaphoreSlim lockObj, HostAddress hostAddress,
        bool skipPostCommands)
    {
        var client = new StandardClient(conf, lockObj, hostAddress);
        await client.Initialize(ioBehavior, cancellationToken, skipPostCommands).ConfigureAwait(false);
        return client;
    }

    private async Task Initialize(IoBehavior ioBehavior, CancellationToken cancellationToken, bool skipPostCommands)
    {
        var host = HostAddress?.Host;
        await ConnectSocket(ioBehavior, _conf, HostAddress).ConfigureAwait(false);

        try
        {
            // **********************************************************************
            // creating socket
            // **********************************************************************
            AssignStream(_conf, null);

            // read server handshake
            var buf = await _reader.ReadReusablePacket(cancellationToken, _logger.isTraceEnabled())
                .ConfigureAwait(false);
            if (buf.GetByte() == -1)
            {
                var errorPacket = new ErrorPacket(buf, null);
                throw ExceptionFactory.Create(
                    errorPacket.Message, errorPacket.SqlState, errorPacket.ErrorCode);
            }

            var handshake = InitialHandshakePacket.decode(buf);

            ExceptionFactory.SetThreadId(handshake.ThreadId);
            var clientCapabilities =
                InitializeClientCapabilities(
                    _conf, handshake.Capabilities, HostAddress);
            Context = new StandardContext(
                handshake,
                clientCapabilities,
                _conf,
                ExceptionFactory,
                _conf.CachePrepStmts ? new LruPrepareCache(_conf.PrepStmtCacheSize, this) : null);

            _reader.SetServerThreadId(handshake.ThreadId, HostAddress);
            _writer.SetServerThreadId(handshake.ThreadId, HostAddress);

            var exchangeCharset = DecideLanguage(handshake);

            // **********************************************************************
            // changing to SSL socket if needed
            // **********************************************************************
            // SSLSocket sslSocket =
            //     ConnectionHelper.sslWrapper(
            //         hostAddress, socket, clientCapabilities, exchangeCharset, context, writer);
            //
            // if (sslSocket != null) {
            //   out = new BufferedOutputStream(sslSocket.getOutputStream(), 16384);
            //   in =
            //       conf.useReadAheadInput()
            //           ? new ReadAheadBufferedStream(sslSocket.getInputStream())
            //           : new BufferedInputStream(sslSocket.getInputStream(), 16384);
            //   assignStream(out, in, conf, handshake.getThreadId());
            // }

            // **********************************************************************
            // handling authentication
            // **********************************************************************
            var authenticationPluginType = handshake.AuthenticationPluginType;
            var handshakeResponse = new HandshakeResponse(
                _conf.User,
                _conf.Password,
                authenticationPluginType,
                Context.Seed,
                _conf,
                host,
                clientCapabilities,
                exchangeCharset);
            var payload = handshakeResponse.BuildPayLoad(Context);
            if (ioBehavior == IoBehavior.Synchronous)
            {
                _writer.WritePayloadSync(payload.ToPayloadData());
                _writer.Flush();
            }
            else
            {
                await _writer.WritePayloadAsync(payload.ToPayloadData(), cancellationToken);
                _writer.Flush();
            }

            await AuthenticationHandler(ioBehavior, _conf.Password, _writer, _reader, Context);

            // **********************************************************************
            // activate compression if required
            // **********************************************************************
            // if ((clientCapabilities & Capabilities.COMPRESS) != 0) {
            //   AssignStream(
            //       new CompressOutputStream(out, compressionSequence),
            //       new CompressInputStream(in, compressionSequence),
            //       conf,
            //       handshake.getThreadId());
            // }

            // **********************************************************************
            // post queries
            // **********************************************************************
            if (!skipPostCommands) PostConnectionQueries(ioBehavior, cancellationToken);
        }
        catch (IOException ioException)
        {
            destroySocket();

            var errorMsg =
                host == null
                    ? $"Could not connect to socket : {ioException.Message}"
                    : $"Could not connect to {host}:{HostAddress.Port} : {ioException.Message}";

            throw ExceptionFactory.Create(errorMsg, "08000", ioException);
        }
        catch (SqlException sqlException)
        {
            destroySocket();
            throw sqlException;
        }
    }

    private static ulong InitializeClientCapabilities(
        Configuration configuration,
        ulong serverCapabilities,
        HostAddress hostAddress)
    {
        var capabilities =
            Capabilities.IGNORE_SPACE
            | Capabilities.CLIENT_PROTOCOL_41
            | Capabilities.TRANSACTIONS
            | Capabilities.SECURE_CONNECTION
            | Capabilities.MULTI_RESULTS
            | Capabilities.PS_MULTI_RESULTS
            | Capabilities.PLUGIN_AUTH
            | Capabilities.CONNECT_ATTRS
            | Capabilities.PLUGIN_AUTH_LENENC_CLIENT_DATA
            | Capabilities.CLIENT_SESSION_TRACK
            | Capabilities.EXTENDED_TYPE_INFO
            | Capabilities.CACHE_METADATA
            | Capabilities.STMT_BULK_OPERATIONS
            | Capabilities.FOUND_ROWS
            | Capabilities.CLIENT_DEPRECATE_EOF;

        if (configuration.AllowMultiQueries) capabilities |= Capabilities.MULTI_STATEMENTS;

        if (configuration.AllowLoadLocalInfile) capabilities |= Capabilities.LOCAL_FILES;

        if (configuration.UseCompression) capabilities |= Capabilities.COMPRESS;

        // connect to database directly if not needed to be created, or if slave, since cannot be
        // created
        if (configuration.Database != null) capabilities |= Capabilities.CONNECT_WITH_DB;

        if (configuration.SslMode != SslMode.Disabled) capabilities |= Capabilities.SSL;
        return capabilities & serverCapabilities;
    }

    public static byte DecideLanguage(InitialHandshakePacket handshake)
    {
        var serverLanguage = handshake.DefaultCollation;
        // return current server utf8mb4 collation
        return (byte)
            (serverLanguage == 45 // utf8mb4_general_ci
             || serverLanguage == 46 // utf8mb4_bin
             || (serverLanguage >= 224 && serverLanguage <= 247)
                ? serverLanguage
                : 224); // UTF8MB4_UNICODE_CI;
    }


    public async Task ConnectSocket(IoBehavior ioBehavior, Configuration conf, HostAddress hostAddress)
    {
        try
        {
            if (conf.Protocol == Protocol.Socket && hostAddress == null)
                throw new ArgumentException("hostname must be set to connect socket if not using local socket or pipe");
            if (conf.Protocol == Protocol.Pipe)
            {
                var pipeName = $@"\\{hostAddress.Host}\pipe\{conf.Pipe}";
                var namedPipeStream = new NamedPipeClientStream(hostAddress!.Host, pipeName, PipeDirection.InOut,
                    PipeOptions.Asynchronous);
                await namedPipeStream.ConnectAsync((int)conf.ConnectionTimeout).ConfigureAwait(false);
                _stream = namedPipeStream;
            }
            else if (conf.Protocol == Protocol.Unix)
            {
                _socket = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.IP);
                var unixEp = new UnixDomainSocketEndPoint(hostAddress.Host);
                await _socket.ConnectAsync(unixEp).ConfigureAwait(false);
                _stream = new NetworkStream(_socket, true);
                _socket.NoDelay = true;
            }
            else
            {
                var tcpClient = new TcpClient();
                if (ioBehavior == IoBehavior.Synchronous)
                    tcpClient.Connect(hostAddress.Host, (int)hostAddress.Port);
                else
                    await tcpClient.ConnectAsync(hostAddress.Host, (int)hostAddress.Port).ConfigureAwait(false);
                _socket = tcpClient.Client;
                _stream = tcpClient.GetStream();
                _socket.NoDelay = true;
            }
        }
        catch (Exception ioe)
        {
            throw new DbNonTransientConnectionException(
                $"Socket fail to connect to host:{hostAddress!.Host}. {ioe.Message}",
                ioe);
        }
    }

    private void AssignStream(Configuration conf, long? threadId)
    {
        _writer =
            new PacketWriter(
                _stream, conf.MaxQuerySizeToLog, conf.MaxAllowedPacket, _sequence, _compressionSequence);
        _writer.SetServerThreadId(threadId, HostAddress);

        _reader = new PacketReader(_stream, _socket, conf, _sequence);
        _reader.SetServerThreadId(threadId, HostAddress);
    }

    public static async Task AuthenticationHandler(IoBehavior ioBehavior, string password, IWriter writer,
        IReader reader, IContext context)
    {
        writer.PermitTrace(true);
        var conf = context.Conf;
        var buf = await reader.ReadReusablePacket(CancellationToken.None).ConfigureAwait(false);

        while (true)
            switch (buf.GetByte() & 0xFF)
            {
                case 0xFE:
                    // *************************************************************************************
                    // Authentication Switch Request see
                    // https://mariadb.com/kb/en/library/connection/#authentication-switch-request
                    // *************************************************************************************
                    var authSwitchPacket = AuthSwitchPacket.Decode(buf);
                    var authenticationPlugin =
                        Authentications.get(authSwitchPacket.Plugin, conf);

                    authenticationPlugin.Initialize(
                        password, authSwitchPacket.Seed, conf);
                    buf = await authenticationPlugin
                        .Process(ioBehavior, CancellationToken.None, writer, reader, context).ConfigureAwait(false);
                    break;

                case 0xFF:
                    // *************************************************************************************
                    // ERR_Packet
                    // see https://mariadb.com/kb/en/library/err_packet/
                    // *************************************************************************************
                    var errorPacket = new ErrorPacket(buf, context);
                    throw context
                        .ExceptionFactory
                        .Create(
                            errorPacket.Message, errorPacket.SqlState, errorPacket.ErrorCode);

                case 0x00:
                    // *************************************************************************************
                    // OK_Packet -> Authenticated !
                    // see https://mariadb.com/kb/en/library/ok_packet/
                    // *************************************************************************************
                    buf.Skip(); // 0x00 OkPacket Header
                    buf.ReadLongLengthEncodedNotNull(); // skip affectedRows
                    buf.ReadLongLengthEncodedNotNull(); // skip insert id
                    // insertId
                    context.ServerStatus = buf.ReadShort();
                    goto authentication_end;

                default:
                    throw context
                        .ExceptionFactory
                        .Create(
                            $"unexpected data during authentication (header={buf.GetUnsignedByte()}",
                            "08000");
            }

        authentication_end:
        writer.PermitTrace(true);
    }

    private void PostConnectionQueries(IoBehavior ioBehavior, CancellationToken cancellationToken)
    {
        var commands = new List<string>();

        var serverTz = _conf.Timezone != null ? HandleTimezone(ioBehavior, cancellationToken) : null;
        var sessionVariableQuery = CreateSessionVariableQuery(serverTz);
        if (sessionVariableQuery != null) commands.Add(sessionVariableQuery);

        if (HostAddress!.Primary == false
            && Context.Version.VersionGreaterOrEqual(5, 6, 5))
            commands.Add("SET SESSION TRANSACTION READ ONLY");

        if (_conf.InitSql != null) commands.Add(_conf.InitSql);

        if (_conf.InitSql != null)
        {
            var initialCommands = _conf.InitSql.Split(";");
            foreach (var cmd in initialCommands) commands.Add(cmd);
        }

        if (commands.Any())
            try
            {
                List<ICompletion> res;
                var msgs = new IClientMessage[commands.Count];
                for (var i = 0; i < commands.Count; i++) msgs[i] = new QueryPacket(commands[i]);
                res =
                    ExecutePipeline(ioBehavior, cancellationToken,
                        msgs,
                        null,
                        0,
                        true);
            }
            catch (SqlException sqlException)
            {
                if (_conf.Timezone != null &&
                    string.Compare("disable", _conf.Timezone, StringComparison.OrdinalIgnoreCase) != 0)
                    // timezone is not valid
                    throw ExceptionFactory.Create(
                        $"Setting configured timezone '{_conf.Timezone}' fail on server.\nLook at https://mariadb.com/kb/en/mysql_tzinfo_to_sql/ to load tz data on server, or set timezone=disable to disable setting client timezone.",
                        "HY000",
                        sqlException);
                throw ExceptionFactory.Create("Initialization command fail", "08000", sqlException);
            }
    }


    private string HandleTimezone(IoBehavior ioBehavior, CancellationToken cancellationToken)
    {
        if (!string.Equals("disable", _conf.Timezone))
        {
            string timeZone = null;
            try
            {
                var res =
                    (AbstractDataReader)
                    Execute(ioBehavior, cancellationToken, new QueryPacket("SELECT @@time_zone, @@system_time_zone"),
                        true)[0];
                res.Read();
                timeZone = res.GetString(1);
                if (string.Equals("SYSTEM", timeZone)) timeZone = res.GetString(2);
            }
            catch (SqlException sqle)
            {
                var res =
                    (AbstractDataReader)
                    Execute(ioBehavior, cancellationToken,
                        new QueryPacket(
                            "SHOW VARIABLES WHERE Variable_name in ("
                            + "'system_time_zone',"
                            + "'time_zone')"),
                        true)[0];
                string systemTimeZone = null;
                while (res.Read())
                    if (string.Equals("system_time_zone", res.GetString(1)))
                        systemTimeZone = res.GetString(2);
                    else
                        timeZone = res.GetString(2);
                if (string.Equals("SYSTEM", timeZone)) timeZone = systemTimeZone;
            }

            return timeZone;
        }

        return null;
    }

    public string CreateSessionVariableQuery(string serverTz)
    {
        // connection must start in autocommit mode
        // we cannot rely on serverStatus & ServerStatus.AUTOCOMMIT before this command to
        // avoid this command.
        // if autocommit=0 is set on server configuration, DB always send Autocommit on serverStatus
        // flag
        // after setting autocommit, we can rely on serverStatus value
        var sessionCommands = new List<string>();
        if (_conf.Autocommit != null) sessionCommands.Add("autocommit=" + (_conf.Autocommit ? "1" : "0"));

        // add configured session variable if configured
        if (_conf.SessionVariables != null) sessionCommands.Add(Security.ParseSessionVariables(_conf.SessionVariables));

        // force client timezone to connection to ensure result of now(), ...
        if (_conf.Timezone != null &&
            string.Compare("disable", _conf.Timezone, StringComparison.OrdinalIgnoreCase) != 0)
        {
            var mustSetTimezone = true;

            TimeZoneInfo connectionTz = null;
            if (string.Compare("auto", _conf.Timezone, StringComparison.OrdinalIgnoreCase) == 0)
                connectionTz = TimeZoneInfo.Local;
            else
                try
                {
                    connectionTz = TimeZoneInfo.FindSystemTimeZoneById(_conf.Timezone);
                }
                catch (Exception e)
                {
                    string? windowsId;
                    if (TimeZoneInfo.TryConvertIanaIdToWindowsId(_conf.Timezone, out windowsId))
                        connectionTz = TimeZoneInfo.FindSystemTimeZoneById(windowsId);
                }

            // try to avoid timezone consideration if server use the same one
            try
            {
                var serverZoneId = TimeZoneInfo.FindSystemTimeZoneById(serverTz);
                if (Equals(serverZoneId, connectionTz)) mustSetTimezone = false;
            }
            catch (Exception e)
            {
                // eat
            }

            if (mustSetTimezone)
                // if (connectionTz.getRules().isFixedOffset()) {
                //   ZoneOffset zoneOffset = clientZoneId.getRules().getOffset(Instant.now());
                //   if (zoneOffset.getTotalSeconds() == 0) {
                //     // specific for UTC timezone, server permitting only SYSTEM/UTC offset or named time
                //     // zone
                //     // not 'UTC'/'Z'
                //     sessionCommands.Add("time_zone='+00:00'");
                //   } else {
                //     sessionCommands.Add("time_zone='" + zoneOffset.getId() + "'");
                //   }
                // } else {
                sessionCommands.Add("time_zone='" + connectionTz.DisplayName + "'");
            // }
        }

        if (_conf.IsolationLevel != IsolationLevel.Unspecified)
        {
            var major = Context.Version.MajorVersion;
            if (!Context.Version.MariaDBServer
                && ((major >= 8 && Context.Version.VersionGreaterOrEqual(8, 0, 3))
                    || (major < 8 && Context.Version.VersionGreaterOrEqual(5, 7, 20))))
                sessionCommands.Add(
                    $"transaction_isolation='{ConvertToCommand(_conf.IsolationLevel)}'");
            else
                sessionCommands.Add($"tx_isolation='{ConvertToCommand(_conf.IsolationLevel)}'");
        }

        if (sessionCommands.Any()) return "set " + string.Join(", ", sessionCommands.ToArray());
        return null;
    }

    public static string ConvertToCommand(IsolationLevel isolationLevel)
    {
        switch (isolationLevel)
        {
            case IsolationLevel.ReadCommitted:
                return "READ-COMMITTED";
            case IsolationLevel.Serializable:
                return "SERIALIZABLE";
            case IsolationLevel.ReadUncommitted:
                return "READ-UNCOMMITTED";
            case IsolationLevel.RepeatableRead:
                return "REPEATABLE-READ";
        }

        throw new SqlException($"unsupported isolation level : {isolationLevel.ToString()}");
    }

    /**
     * Closing socket in case of Connection error after socket creation.
     */
    protected void destroySocket()
    {
        _closed = true;
        try
        {
            _reader.Close();
        }
        catch (Exception ee)
        {
            // eat exception
        }

        try
        {
            _writer.Close();
        }
        catch (Exception ee)
        {
            // eat exception
        }

        try
        {
            _socket.Close();
        }
        catch (IOException ee)
        {
            // eat exception
        }
    }

    public int SendQuery(IoBehavior ioBehavior, CancellationToken cancellationToken, IClientMessage message)
    {
        CheckNotClosed();
        try
        {
            if (_logger.isDebugEnabled() && message.Description != null)
                _logger.debug($"execute query: {message.Description}");
            var payload = message.BuildPayLoad(Context);
            if (ioBehavior == IoBehavior.Synchronous)
                message.WriteSync(payload, _writer);
            else
                message.WriteAsync(cancellationToken, payload, _writer).ConfigureAwait(false).GetAwaiter().GetResult();
            return 1;
        }
        catch (Exception e)
        {
            if (e is DbMaxAllowedPacketException)
            {
                if (((DbMaxAllowedPacketException)e).MustReconnect)
                {
                    destroySocket();
                    throw ExceptionFactory
                        .WithSql(message.Description)
                        .Create(
                            "Packet too big for current server max_allowed_packet value",
                            "08000",
                            e);
                }

                throw ExceptionFactory
                    .WithSql(message.Description)
                    .Create(
                        "Packet too big for current server max_allowed_packet value", "HZ000", e);
            }

            destroySocket();
            throw ExceptionFactory
                .WithSql(message.Description)
                .Create("Socket error", "08000", e);
        }
    }

    public List<ICompletion> ReadResponse(
        IoBehavior ioBehavior,
        CancellationToken cancellationToken,
        MariaDbCommand stmt,
        IClientMessage message,
        CommandBehavior behavior)
    {
        CheckNotClosed();
        if (_streamStmt != null)
        {
            _streamStmt.FetchRemaining();
            _streamStmt = null;
        }

        var completions = new List<ICompletion>();
        ReadResults(
            ioBehavior,
            cancellationToken,
            stmt,
            message,
            completions,
            behavior);
        return completions;
    }

    public void ReadResponse(IoBehavior ioBehavior, CancellationToken cancellationToken, IClientMessage message)
    {
        CheckNotClosed();
        if (_streamStmt != null)
        {
            _streamStmt.FetchRemaining();
            _streamStmt = null;
        }

        var completions = new List<ICompletion>();
        ReadResults(ioBehavior, cancellationToken,
            null,
            message,
            completions,
            CommandBehavior.Default);
    }

    private void ReadResults(
        IoBehavior ioBehavior,
        CancellationToken cancellationToken,
        MariaDbCommand stmt,
        IClientMessage message,
        List<ICompletion> completions,
        CommandBehavior behavior)
    {
        completions.Add(
            ReadPacket(
                ioBehavior,
                cancellationToken,
                stmt,
                message,
                behavior));

        while ((Context.ServerStatus & ServerStatus.MORE_RESULTS_EXISTS) > 0)
            completions.Add(
                ReadPacket(ioBehavior, cancellationToken,
                    stmt,
                    message,
                    behavior));
    }

    public ICompletion ReadPacket(IoBehavior ioBehavior, CancellationToken cancellationToken, IClientMessage message)
    {
        return ReadPacket(ioBehavior, cancellationToken, null, message, 0);
    }

    public ICompletion ReadPacket(
        IoBehavior ioBehavior,
        CancellationToken cancellationToken,
        MariaDbCommand stmt,
        IClientMessage message,
        CommandBehavior behavior)
    {
        try
        {
            var traceEnable = _logger.isTraceEnabled();
            var completion =
                ioBehavior == IoBehavior.Synchronous
                    ? message.ReadPacketSync(
                        stmt,
                        behavior,
                        _reader,
                        _writer,
                        Context,
                        ExceptionFactory,
                        traceEnable,
                        _lock,
                        message)
                    : message.ReadPacketAsync(
                        cancellationToken,
                        stmt,
                        behavior,
                        _reader,
                        _writer,
                        Context,
                        ExceptionFactory,
                        traceEnable,
                        _lock,
                        message).ConfigureAwait(false).GetAwaiter().GetResult();
            if (completion is StreamingDataReader && !((StreamingDataReader)completion).Loaded)
            {
                _streamStmt = stmt;
                _streamMsg = message;
            }

            return completion;
        }
        catch (IOException ioException)
        {
            destroySocket();
            throw ExceptionFactory
                .WithSql(message.Description)
                .Create("Socket error", "08000", ioException);
        }
    }

    private void CheckNotClosed()
    {
        if (_closed) throw ExceptionFactory.Create("Connection is closed", "08000", 1220);
    }

    private void CloseSocket()
    {
        try
        {
            try
            {
                Thread.Sleep(10);
            }
            catch (Exception t)
            {
                // eat exception
            }

            _writer.Close();
            _reader.Close();
        }
        catch (IOException e)
        {
            // eat
        }
        finally
        {
            try
            {
                _socket.Close();
            }
            catch (Exception e)
            {
                // socket closed, if any error, so not throwing error
            }
        }
    }
}