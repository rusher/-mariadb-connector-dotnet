namespace MariadbConnector.utils.constant;

public class ConnectionState
{
    /**
     * flag indicating that network timeout has been changed
     */
    public static readonly uint STATE_NETWORK_TIMEOUT = 1;

    /**
     * flag indicating that default database has been changed
     */
    public static readonly uint STATE_DATABASE = 2;

    /**
     * flag indicating that connection read only has been changed
     */
    public static readonly uint STATE_READ_ONLY = 4;

    /**
     * flag indicating that autocommit has been changed
     */
    public static readonly uint STATE_AUTOCOMMIT = 8;

    /**
     * flag indicating that transaction isolation has been changed
     */
    public static readonly uint STATE_TRANSACTION_ISOLATION = 16;
}