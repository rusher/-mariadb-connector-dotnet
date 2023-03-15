namespace MariadbConnector.utils.exception;

public class DbTimeoutException : DbTransientConnectionException
{
    public DbTimeoutException()
    {
    }

    public DbTimeoutException(string? message) : base(message)
    {
    }

    public DbTimeoutException(string? message, Exception? innerException) : base(message, innerException)
    {
    }

    public DbTimeoutException(string? message, int errorCode, string sqlState) : base(message, errorCode, sqlState)
    {
    }
}