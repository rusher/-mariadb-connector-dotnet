using System.Data.Common;

namespace MariadbConnector.utils.exception;

public class SqlException : DbException
{
    public SqlException()
    {
    }

    public SqlException(string? message) : base(message)
    {
    }

    public SqlException(string? message, Exception? innerException) : base(message, innerException)
    {
    }

    public SqlException(string? message, int errorCode, string sqlState) : base(message, errorCode)
    {
        SqlState = sqlState;
    }

    public override string? SqlState { get; }
}