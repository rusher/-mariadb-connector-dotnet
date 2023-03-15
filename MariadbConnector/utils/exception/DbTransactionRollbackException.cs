namespace MariadbConnector.utils.exception;

public class DbTransactionRollbackException : DbTransientConnectionException
{
    public DbTransactionRollbackException()
    {
    }

    public DbTransactionRollbackException(string? message) : base(message)
    {
    }

    public DbTransactionRollbackException(string? message, Exception? innerException) : base(message, innerException)
    {
    }

    public DbTransactionRollbackException(string? message, int errorCode, string sqlState) : base(message, errorCode,
        sqlState)
    {
    }
}