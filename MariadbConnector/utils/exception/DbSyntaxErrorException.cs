namespace MariadbConnector.utils.exception;

public class DbSyntaxErrorException : DbNonTransientConnectionException
{
    public DbSyntaxErrorException()
    {
    }

    public DbSyntaxErrorException(string? message) : base(message)
    {
    }

    public DbSyntaxErrorException(string? message, Exception? innerException) : base(message, innerException)
    {
    }

    public DbSyntaxErrorException(string? message, int errorCode, string sqlState) : base(message, errorCode, sqlState)
    {
    }
}