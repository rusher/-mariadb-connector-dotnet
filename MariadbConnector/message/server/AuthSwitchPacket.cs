using MariadbConnector.client.util;

namespace MariadbConnector.message.server;

public class AuthSwitchPacket : IServerMessage
{
    public readonly string Plugin;
    public readonly byte[] Seed;

    public AuthSwitchPacket(string plugin, byte[] seed)
    {
        Plugin = plugin;
        Seed = seed;
    }

    public static AuthSwitchPacket Decode(IReadableByteBuf buf)
    {
        buf.Skip(1);
        var plugin = buf.ReadStringNullEnd();

        var seed = new byte[buf.ReadableBytes()];
        buf.ReadBytes(seed);
        return new AuthSwitchPacket(plugin, seed);
    }

    public static byte[] TruncatedSeed(byte[] seed)
    {
        if (seed.Length > 0)
        {
            var truncatedSeed = new byte[seed.Length - 1];
            Array.Copy(seed, 0, truncatedSeed, 0, truncatedSeed.Length);
            return truncatedSeed;
        }

        return new byte[0];
    }
}