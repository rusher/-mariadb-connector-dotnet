namespace MariadbConnector.utils.constant;

public class Capabilities
{
    /// Is client mysql
    public static readonly uint CLIENT_MYSQL = 1;

    /// use Found rows instead of affected rows
    public static readonly uint FOUND_ROWS = 2;

    /**
     * Get all column flags
     */
    public static readonly uint LONG_FLAG = 4;

    /**
     * One can specify db on connect
     */
    public static readonly uint CONNECT_WITH_DB = 8;

    /**
     * Don't allow database.table.column
     */
    public static readonly uint NO_SCHEMA = 16;

    /**
     * use compression protocol
     */
    public static readonly uint COMPRESS = 32;

    /**
     * Odbc client
     */
    public static readonly uint ODBC = 64;

    /**
     * Can use LOAD DATA LOCAL
     */
    public static readonly uint LOCAL_FILES = 128;

    /**
     * Ignore spaces before '('
     */
    public static readonly uint IGNORE_SPACE = 256;

    /**
     * Use 4.1 protocol
     */
    public static readonly uint CLIENT_PROTOCOL_41 = 512;

    /**
     * Is interactive client
     */
    public static readonly uint CLIENT_INTERACTIVE = 1024;

    /**
     * Switch to SSL after handshake
     */
    public static readonly uint SSL = 2048;

    /**
     * IGNORE sigpipes
     */
    public static readonly uint IGNORE_SIGPIPE = 4096;

    /**
     * transactions
     */
    public static readonly uint TRANSACTIONS = 8192;

    /**
     * reserved - not used
     */
    public static readonly uint RESERVED = 16384;

    /**
     * New 4.1 authentication
     */
    public static readonly uint SECURE_CONNECTION = 32768;

    /**
     * Enable/disable multi-stmt support
     */
    public static readonly uint MULTI_STATEMENTS = 1 << 16;

    /**
     * Enable/disable multi-results
     */
    public static readonly uint MULTI_RESULTS = 1 << 17;

    /**
     * Enable/disable multi-results for PrepareStatement
     */
    public static readonly uint PS_MULTI_RESULTS = 1 << 18;

    /**
     * Client supports plugin authentication
     */
    public static readonly uint PLUGIN_AUTH = 1 << 19;

    /**
     * Client send connection attributes
     */
    public static readonly uint CONNECT_ATTRS = 1 << 20;

    /**
     * authentication data length is a length auth integer
     */
    public static readonly uint PLUGIN_AUTH_LENENC_CLIENT_DATA = 1 << 21;

    /**
     * server send session tracking info
     */
    public static readonly uint CLIENT_SESSION_TRACK = 1 << 23;

    /**
     * EOF packet deprecated
     */
    public static readonly uint CLIENT_DEPRECATE_EOF = 1 << 24;

    /**
     * Client support progress indicator (before 10.2)
     */
    public static readonly uint PROGRESS_OLD = 1 << 29;

    /* MariaDB specific capabilities */

    /**
     * Client progression
     */
    public static readonly ulong PROGRESS = 1L << 32;

    /**
     * not used anymore - reserved
     */
    public static readonly ulong MARIADB_RESERVED = 1L << 33;

    /**
     * permit COM_STMT_BULK commands
     */
    public static readonly ulong STMT_BULK_OPERATIONS = 1L << 34;

    /**
     * metadata extended information
     */
    public static readonly ulong EXTENDED_TYPE_INFO = 1L << 35;

    /**
     * permit metadata caching
     */
    public static readonly ulong CACHE_METADATA = 1L << 36;
}