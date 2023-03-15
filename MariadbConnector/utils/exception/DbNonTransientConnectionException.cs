namespace MariadbConnector.utils.exception;

public class DbNonTransientConnectionException : SqlException
{
    public DbNonTransientConnectionException()
    {
    }

    public DbNonTransientConnectionException(string? message) : base(message)
    {
    }

    public DbNonTransientConnectionException(string? message, Exception? innerException) : base(message, innerException)
    {
    }

    public DbNonTransientConnectionException(string? message, int errorCode, string sqlState) : base(message, errorCode,
        sqlState)
    {
    }

    public override bool IsTransient => false;
}