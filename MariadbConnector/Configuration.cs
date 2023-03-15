using System.Data;
using System.Data.Common;
using MariadbConnector.utils.constant;

namespace MariadbConnector;

public class Configuration
{
    public readonly bool AllowLoadLocalInfile;
    public readonly bool AllowMultiQueries;
    public readonly bool Autocommit;
    public readonly bool CachePrepStmts;
    public readonly string ConnectionAttributes;
    public readonly uint ConnectionTimeout;
    public readonly string Database;
    public readonly bool DisablePipeline;
    public readonly bool DumpQueriesOnException;
    public readonly string InitSql;
    public readonly IsolationLevel IsolationLevel;
    public readonly uint? MaxAllowedPacket;
    public readonly uint MaxQuerySizeToLog;
    public readonly string Password;
    public readonly string Pipe;
    public readonly uint Port;
    public readonly uint PrepStmtCacheSize;
    public readonly Protocol Protocol;
    public readonly string RestrictedAuth;
    public readonly string Server;
    public readonly string SessionVariables;
    public readonly SslMode SslMode;
    public readonly string Timezone;
    public readonly bool UseCompression;
    public readonly string User;

    public Configuration(string connectionString) : this(FromString(connectionString))
    {
    }

    public Configuration(DbConnectionStringBuilder builder)
    {
        var parser = new Parser(builder);
        Server = parser.getStringOption(
            new[] { "Server", "Host", "Data Source", "DataSource", "Address", "Addr", "Network Address" },
            "localhost");
        Port = parser.getIntOption(new[] { "Port" }, 3306);
        User = parser.getStringOption(
            new[] { "User", "User ID", "UserID", "Username", "Uid", "User name" }, null);
        Password = parser.getStringOption(
            new[] { "Password", "pwd" }, null);
        Database = parser.getStringOption(
            new[] { "Database", "Initial Catalog" }, null);
        DumpQueriesOnException = parser.getBoolOption(
            new[] { "DumpQueriesOnException" }, false);
        MaxQuerySizeToLog = parser.getIntOption(new[] { "MaxQuerySizeToLog" }, 1024);
        AllowLoadLocalInfile = parser.getBoolOption(
            new[] { "AllowLoadLocalInfile", "Allow Load Local Infile" }, false);
        DisablePipeline = parser.getBoolOption(
            new[] { "DisablePipeline" }, false);
        Protocol = parser.getEnumOption(
            new[] { "ConnectionProtocol", "Connection Protocol", "Protocol" },
            Protocol.Socket);
        Pipe = parser.getStringOption(
            new[] { "Pipe", "PipeName", "Pipe Name" }, null);
        ConnectionTimeout =
            parser.getIntOption(new[] { "ConnectionTimeout", "Connect Timeout", "Connection Timeout" }, 10);
        MaxAllowedPacket =
            parser.getIntorNullOption(new[] { "ConnectionTimeout", "Connect Timeout", "Connection Timeout" }, null);
        UseCompression = parser.getBoolOption(
            new[] { "UseCompression", "Compress", "Use Compression" }, false);
        SslMode = parser.getEnumOption(
            new[] { "SslMode", "Ssl Mode", "Ssl-Mode" },
            SslMode.Disabled);
        CachePrepStmts = parser.getBoolOption(
            new[] { "CachePrepStmts" }, true);
        PrepStmtCacheSize = parser.getIntOption(new[] { "MaxQuerySizeToLog" }, 256);
        ConnectionAttributes = parser.getStringOption(
            new[] { "ConnectionAttributes", "connection-attributes" }, null);
        RestrictedAuth = parser.getStringOption(
            new[] { "RestrictedAuth" }, null);
        Timezone = parser.getStringOption(
            new[] { "Timezone" }, null);
        InitSql = parser.getStringOption(
            new[] { "InitSql" }, null);
        Autocommit = parser.getBoolOption(
            new[] { "Autocommit" }, true);
        SessionVariables = parser.getStringOption(
            new[] { "SessionVariables" }, null);
        IsolationLevel = parser.getEnumOption(
            new[] { "SslMode", "Ssl Mode", "Ssl-Mode" },
            IsolationLevel.Unspecified);
    }

    public static DbConnectionStringBuilder FromString(string connectionString)
    {
        var builder =
            new DbConnectionStringBuilder();
        builder.ConnectionString = connectionString;
        return builder;
    }
}

internal class Parser
{
    private readonly DbConnectionStringBuilder _builder;

    public Parser(DbConnectionStringBuilder builder)
    {
        _builder = builder;
    }

    public string getStringOption(string[] keyAliases, string defaultValue)
    {
        foreach (var k in keyAliases)
        {
            object value;
            if (_builder.TryGetValue(k, out value)) return value.ToString();
        }

        return defaultValue;
    }

    public T getEnumOption<T>(string[] keyAliases, T defaultValue) where T : struct, Enum, IComparable
    {
        foreach (var k in keyAliases)
        {
            object obj;
            if (_builder.TryGetValue(k, out obj))
            {
                T val;
                if (Enum.TryParse(obj.ToString(), out val)) return val;
            }
        }

        return defaultValue;
    }

    public uint getIntOption(string[] keyAliases, uint defaultValue)
    {
        foreach (var k in keyAliases)
        {
            object obj;
            if (_builder.TryGetValue(k, out obj))
            {
                if (obj == null) continue;
                if (obj is uint ui) return ui;
                if (obj is int i) return (uint)i;
                if (obj is string str)
                    if (uint.TryParse(str, out var parseRes))
                        return parseRes;

                throw new ArgumentException($"Parameter {k} has wrong int32 value '{obj}'.");
            }
        }

        return defaultValue;
    }

    public uint? getIntorNullOption(string[] keyAliases, uint? defaultValue)
    {
        foreach (var k in keyAliases)
        {
            object obj;
            if (_builder.TryGetValue(k, out obj))
            {
                if (obj == null) continue;
                if (obj is uint ui) return ui;
                if (obj is int i) return (uint)i;
                if (obj is string str)
                    if (uint.TryParse(str, out var parseRes))
                        return parseRes;

                throw new ArgumentException($"Parameter {k} has wrong int32 value '{obj}'.");
            }
        }

        return defaultValue;
    }

    public bool getBoolOption(string[] keyAliases, bool defaultValue)
    {
        foreach (var k in keyAliases)
        {
            object obj;
            if (_builder.TryGetValue(k, out obj))
            {
                if (obj == null) continue;
                if (obj is bool b) return b;
                if (obj is string str)
                    if (bool.TryParse(str, out var parseRes))
                        return parseRes;

                throw new ArgumentException($"Parameter {k} has wrong boolean value '{obj}'.");
            }
        }

        return defaultValue;
    }
}