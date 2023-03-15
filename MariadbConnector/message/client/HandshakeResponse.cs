using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Security.Principal;
using System.Text;
using MariadbConnector.client;
using MariadbConnector.client.socket;
using MariadbConnector.plugin.authentication.standard;
using MariadbConnector.utils.constant;

namespace MariadbConnector.message.client;

public class HandshakeResponse : AbstractClientMessage
{
    private static readonly string _CLIENT_NAME = "_client_name";
    private static readonly string _CLIENT_VERSION = "_client_version";
    private static readonly string _SERVER_HOST = "_server_host";
    private static readonly string _OS = "_os";
    private static readonly string _THREAD = "_thread";
    private static string _JAVA_VENDOR = "_java_vendor";
    private static string _JAVA_VERSION = "_java_version";
    private readonly ulong _clientCapabilities;
    private readonly string _connectionAttributes;
    private readonly string _database;
    private readonly byte _exchangeCharset;
    private readonly string _host;
    private readonly string _password;
    private readonly byte[] _seed;

    private readonly string _username;
    private string _authenticationPluginType;

    public HandshakeResponse(
        string username,
        string password,
        string authenticationPluginType,
        byte[] seed,
        Configuration conf,
        string host,
        ulong clientCapabilities,
        byte exchangeCharset)
    {
        _authenticationPluginType = authenticationPluginType;
        _seed = seed;
        _username = username;
        _password = password;
        _database = conf.Database;
        _connectionAttributes = conf.ConnectionAttributes;
        _host = host;
        _clientCapabilities = clientCapabilities;
        _exchangeCharset = exchangeCharset;
    }

    public override string Description => "HandshakeResponse";

    private static void WriteStringLengthAscii(ByteBufferWriter buf, string value)
    {
        var valBytes = Encoding.ASCII.GetBytes(value);
        buf.WriteLength(valBytes.Length);
        buf.WriteBytes(valBytes);
    }

    private static void WriteStringLength(ByteBufferWriter buf, string value)
    {
        var valBytes = Encoding.UTF8.GetBytes(value);
        buf.WriteLength(valBytes.Length);
        buf.WriteBytes(valBytes);
    }

    private static void WriteConnectAttributes(
        ByteBufferWriter buf, string connectionAttributes, string host)
    {
        var tmpWriter = new ByteBufferWriter(200, 0);
        WriteStringLengthAscii(tmpWriter, _CLIENT_NAME);
        WriteStringLength(tmpWriter, "MariaDB dot.net");

        WriteStringLengthAscii(tmpWriter, _CLIENT_VERSION);
        WriteStringLength(tmpWriter, "0.0.1");

        WriteStringLengthAscii(tmpWriter, _SERVER_HOST);
        WriteStringLength(tmpWriter, host != null ? host : "");

        WriteStringLengthAscii(tmpWriter, _OS);
        WriteStringLength(tmpWriter, RuntimeInformation.OSDescription);

        WriteStringLengthAscii(tmpWriter, _THREAD);
        WriteStringLength(tmpWriter, Process.GetCurrentProcess().Id.ToString());

        if (connectionAttributes != null)
        {
            var tokenizer = connectionAttributes.Split(",");
            foreach (var token in tokenizer)
            {
                var separator = token.IndexOf(":");
                if (separator != -1)
                {
                    WriteStringLength(tmpWriter, token.Substring(0, separator));
                    WriteStringLength(tmpWriter, token.Substring(separator + 1));
                }
                else
                {
                    WriteStringLength(tmpWriter, token);
                    WriteStringLength(tmpWriter, "");
                }
            }
        }

        buf.WriteLength(tmpWriter.Position);
        buf.WriteBytes(tmpWriter.Buf, 0, tmpWriter.Position);
    }

    public override ByteBufferWriter BuildPayLoad(IContext context)
    {
        byte[] authData;
        if (string.Equals("mysql_clear_password", _authenticationPluginType))
        {
            if (!context.HasClientCapability(Capabilities.SSL))
                throw new ArgumentException("Cannot send password in clear if SSL is not enabled.");
            authData =
                _password == null ? new byte[0] : Encoding.UTF8.GetBytes(_password);
        }
        else
        {
            _authenticationPluginType = "mysql_native_password";
            authData = NativePasswordPlugin.encryptPassword(_password, _seed);
        }

        var buf = new ByteBufferWriter();
        buf.WriteInt((int)_clientCapabilities);
        buf.WriteInt(1024 * 1024 * 1024);
        buf.WriteByte(_exchangeCharset); // 1

        buf.WriteBytes(new byte[19]); // 19
        buf.WriteInt((int)(_clientCapabilities >> 32)); // Maria extended flag

        buf.WriteString(_username != null ? _username : WindowsIdentity.GetCurrent().Name);
        buf.WriteByte(0x00);

        if (context.HasServerCapability(Capabilities.PLUGIN_AUTH_LENENC_CLIENT_DATA))
        {
            buf.WriteLength(authData.Length);
            buf.WriteBytes(authData);
        }
        else if (context.HasServerCapability(Capabilities.SECURE_CONNECTION))
        {
            buf.WriteByte((byte)authData.Length);
            buf.WriteBytes(authData);
        }
        else
        {
            buf.WriteBytes(authData);
            buf.WriteByte(0x00);
        }

        if (context.HasServerCapability(Capabilities.CONNECT_WITH_DB))
        {
            buf.WriteString(_database);
            buf.WriteByte(0x00);
        }

        if (context.HasServerCapability(Capabilities.PLUGIN_AUTH))
        {
            buf.WriteString(_authenticationPluginType);
            buf.WriteByte(0x00);
        }

        if (context.HasServerCapability(Capabilities.CONNECT_ATTRS))
            WriteConnectAttributes(buf, _connectionAttributes, _host);
        return buf;
    }
}