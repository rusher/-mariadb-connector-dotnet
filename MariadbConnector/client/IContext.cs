using MariadbConnector.message.server.util;
using MariadbConnector.utils;

namespace MariadbConnector.client;

public interface IContext
{
    long ThreadId { get; }
    ulong ServerCapabilities { get; }
    ulong ClientCapabilities { get; }
    byte[] Seed { get; }
    ServerVersion Version { get; }
    bool EofDeprecated { get; }
    bool SkipMeta { get; }
    bool ExtendedInfo { get; }
    Configuration Conf { get; }
    ExceptionFactory ExceptionFactory { get; }

    int ServerStatus { get; set; }
    string Database { get; set; }
    int TransactionIsolationLevel { get; set; }
    int Warning { get; set; }
    IPrepareCache PrepareCache { get; }
    int StateFlag { get; set; }

    bool HasServerCapability(ulong flag);

    bool HasClientCapability(ulong flag);

    bool PermitPipeline();


    void ResetPrepareCache();
    void ResetStateFlag();
    void AddStateFlag(int state);
}