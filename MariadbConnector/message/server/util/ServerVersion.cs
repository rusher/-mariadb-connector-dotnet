using MariadbConnector.client;
using Version = MariadbConnector.utils.Version;

namespace MariadbConnector.message.server.util;

public class ServerVersion : Version, IServerVersion
{
    public ServerVersion(string serverVersion, bool mariaDBServer) : base(serverVersion)
    {
        MariaDBServer = mariaDBServer;
    }

    public bool MariaDBServer { get; }
}