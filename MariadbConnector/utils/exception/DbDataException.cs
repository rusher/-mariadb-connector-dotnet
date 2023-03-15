namespace MariadbConnector.utils.exception;

public class DbDataException : DbNonTransientConnectionException
{
    public DbDataException()
    {
    }

    public DbDataException(string? message) : base(message)
    {
    }

    public DbDataException(string? message, Exception? innerException) : base(message, innerException)
    {
    }

    public DbDataException(string? message, int errorCode, string sqlState) : base(message, errorCode, sqlState)
    {
    }
}