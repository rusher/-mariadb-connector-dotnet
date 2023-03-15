using MariadbConnector.client;
using MariadbConnector.client.socket;
using MariadbConnector.client.util;
using MariadbConnector.utils;

namespace MariadbConnector.plugin.authentication;

public interface IAuthenticationPlugin
{
    string Type { get; }
    void Initialize(string authenticationData, byte[] seed, Configuration conf);

    Task<IReadableByteBuf> Process(IoBehavior ioBehavior, CancellationToken token, IWriter writer, IReader reader,
        IContext context);
}