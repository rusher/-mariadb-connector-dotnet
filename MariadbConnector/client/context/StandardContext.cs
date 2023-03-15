using MariadbConnector.message.server;
using MariadbConnector.message.server.util;
using MariadbConnector.utils;
using MariadbConnector.utils.constant;

namespace MariadbConnector.client.context;

public class StandardContext : IContext
{
    public StandardContext(
        InitialHandshakePacket handshake,
        ulong clientCapabilities,
        Configuration conf,
        ExceptionFactory exceptionFactory,
        IPrepareCache prepareCache)
    {
        ThreadId = handshake.ThreadId;
        Seed = handshake.Seed;
        ServerCapabilities = handshake.Capabilities;
        ServerStatus = handshake.ServerStatus;
        Version = handshake.Version;
        ClientCapabilities = clientCapabilities;
        EofDeprecated = HasClientCapability(Capabilities.CLIENT_DEPRECATE_EOF);
        SkipMeta = HasClientCapability(Capabilities.CACHE_METADATA);
        ExtendedInfo = HasClientCapability(Capabilities.EXTENDED_TYPE_INFO);
        Conf = conf;
        Database = conf.Database;
        ExceptionFactory = exceptionFactory;
        PrepareCache = prepareCache;
        StateFlag = 0;
    }

    public long ThreadId { get; }
    public ulong ServerCapabilities { get; }
    public ulong ClientCapabilities { get; }
    public byte[] Seed { get; }
    public ServerVersion Version { get; }
    public bool EofDeprecated { get; }
    public bool SkipMeta { get; }
    public bool ExtendedInfo { get; }
    public Configuration Conf { get; }
    public ExceptionFactory ExceptionFactory { get; }

    public int ServerStatus { get; set; }
    public string Database { get; set; }
    public int TransactionIsolationLevel { get; set; }
    public int Warning { get; set; }
    public IPrepareCache PrepareCache { get; }
    public int StateFlag { get; set; }

    public bool HasServerCapability(ulong flag)
    {
        return (ServerCapabilities & flag) > 0;
    }

    public bool HasClientCapability(ulong flag)
    {
        return (ClientCapabilities & flag) > 0;
    }

    public bool PermitPipeline()
    {
        return !Conf.DisablePipeline && HasClientCapability(Capabilities.STMT_BULK_OPERATIONS);
    }

    public void ResetPrepareCache()
    {
        if (PrepareCache != null) PrepareCache.Reset();
    }

    public void ResetStateFlag()
    {
        StateFlag = 0;
    }

    public void AddStateFlag(int state)
    {
        StateFlag |= state;
    }
}