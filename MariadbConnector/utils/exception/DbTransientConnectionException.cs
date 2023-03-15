namespace MariadbConnector.utils.exception;

public class DbTransientConnectionException : SqlException
{
    public DbTransientConnectionException()
    {
    }

    public DbTransientConnectionException(string? message) : base(message)
    {
    }

    public DbTransientConnectionException(string? message, Exception? innerException) : base(message, innerException)
    {
    }

    public DbTransientConnectionException(string? message, int errorCode, string sqlState) : base(message, errorCode,
        sqlState)
    {
    }

    public override bool IsTransient => true;
}