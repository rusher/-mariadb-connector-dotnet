using MariadbConnector.client.util;
using MariadbConnector.message.server.util;

namespace MariadbConnector.message.server;

public class InitialHandshakePacket : IServerMessage
{
    private static readonly string MARIADB_RPL_HACK_PREFIX = "5.5.5-";

    private InitialHandshakePacket(
        string serverVersion,
        long threadId,
        byte[] seed,
        ulong capabilities,
        short defaultCollation,
        short serverStatus,
        bool mariaDBServer,
        string authenticationPluginType)
    {
        ThreadId = threadId;
        Seed = seed;
        Capabilities = capabilities;
        DefaultCollation = defaultCollation;
        ServerStatus = serverStatus;
        AuthenticationPluginType = authenticationPluginType;
        Version = new ServerVersion(serverVersion, mariaDBServer);
    }

    public long ThreadId { get; }
    public byte[] Seed { get; }
    public ulong Capabilities { get; }
    public short DefaultCollation { get; }
    public short ServerStatus { get; }
    public string AuthenticationPluginType { get; }
    public ServerVersion Version { get; }

    /**
     * parsing packet
     * 
     * @param reader packet reader
     * @return Parsed packet
     */
    public static InitialHandshakePacket decode(IReadableByteBuf reader)
    {
        var protocolVersion = reader.ReadByte();
        if (protocolVersion != 0x0a)
            throw new ArgumentException(
                $"Unexpected initial handshake protocol value [{protocolVersion}]");

        var serverVersion = reader.ReadStringNullEnd();
        long threadId = reader.ReadInt();
        var seed1 = new byte[8];
        reader.ReadBytes(seed1);
        reader.Skip();
        int serverCapabilities2FirstBytes = reader.ReadUnsignedShort();
        var defaultCollation = reader.ReadUnsignedByte();
        var serverStatus = reader.ReadShort();
        var serverCapabilities4FirstBytes = serverCapabilities2FirstBytes + (reader.ReadShort() << 16);
        var saltLength = 0;

        if ((serverCapabilities4FirstBytes & utils.constant.Capabilities.PLUGIN_AUTH) != 0)
            saltLength = Math.Max(12, reader.ReadByte() - 9);
        else
            reader.Skip();
        reader.Skip(6);

        // MariaDB additional capabilities.
        // Filled only if MariaDB server 10.2+
        long mariaDbAdditionalCapacities = reader.ReadInt();
        byte[] seed;
        if ((serverCapabilities4FirstBytes & utils.constant.Capabilities.SECURE_CONNECTION) != 0)
        {
            byte[] seed2;
            if (saltLength > 0)
            {
                seed2 = new byte[saltLength];
                reader.ReadBytes(seed2);
            }
            else
            {
                seed2 = reader.ReadBytesNullEnd();
            }

            seed = new byte[seed1.Length + seed2.Length];
            Array.Copy(seed1, 0, seed, 0, seed1.Length);
            Array.Copy(seed2, 0, seed, seed1.Length, seed2.Length);
        }
        else
        {
            seed = seed1;
        }

        reader.Skip();

        /*
         * check for MariaDB 10.x replication hack , remove fake prefix if needed
         *  (see comments about MARIADB_RPL_HACK_PREFIX)
         */
        bool serverMariaDb;
        if (serverVersion.StartsWith(MARIADB_RPL_HACK_PREFIX))
        {
            serverMariaDb = true;
            serverVersion = serverVersion.Substring(MARIADB_RPL_HACK_PREFIX.Length);
        }
        else
        {
            serverMariaDb = serverVersion.Contains("MariaDB");
        }

        // since MariaDB 10.2
        ulong serverCapabilities;
        if ((serverCapabilities4FirstBytes & utils.constant.Capabilities.CLIENT_MYSQL) == 0)
        {
            serverCapabilities = (ulong)(
                (serverCapabilities4FirstBytes & 0xffffffffL) + (mariaDbAdditionalCapacities << 32));
            serverMariaDb = true;
        }
        else
        {
            serverCapabilities = (ulong)serverCapabilities4FirstBytes & 0xffffffffL;
        }

        string authenticationPluginType = null;
        if ((serverCapabilities4FirstBytes & utils.constant.Capabilities.PLUGIN_AUTH) != 0)
            authenticationPluginType = reader.ReadStringNullEnd();

        return new InitialHandshakePacket(
            serverVersion,
            threadId,
            seed,
            serverCapabilities,
            defaultCollation,
            serverStatus,
            serverMariaDb,
            authenticationPluginType);
    }
}