namespace MariadbConnector.utils.exception;

public class DbMaxAllowedPacketException : DbNonTransientConnectionException
{
    public DbMaxAllowedPacketException(string? message, bool mustReconnect) : base(message)
    {
        MustReconnect = mustReconnect;
    }

    public bool MustReconnect { get; }
}