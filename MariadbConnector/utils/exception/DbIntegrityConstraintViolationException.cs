namespace MariadbConnector.utils.exception;

public class DbIntegrityConstraintViolationException : DbNonTransientConnectionException
{
    public DbIntegrityConstraintViolationException()
    {
    }

    public DbIntegrityConstraintViolationException(string? message) : base(message)
    {
    }

    public DbIntegrityConstraintViolationException(string? message, Exception? innerException) : base(message,
        innerException)
    {
    }

    public DbIntegrityConstraintViolationException(string? message, int errorCode, string sqlState) : base(message,
        errorCode, sqlState)
    {
    }
}