namespace MariadbConnector.utils.constant;

public class ServerStatus
{
    /**
     * is in transaction
     */
    public static readonly ushort IN_TRANSACTION = 1;

    /**
     * autocommit
     */
    public static readonly ushort AUTOCOMMIT = 2;

    /**
     * more result exists (packet follows)
     */
    public static readonly ushort MORE_RESULTS_EXISTS = 8;

    /**
     * no good index was used
     */
    public static readonly ushort QUERY_NO_GOOD_INDEX_USED = 16;

    /**
     * no index was used
     */
    public static readonly ushort QUERY_NO_INDEX_USED = 32;

    /**
     * cursor exists
     */
    public static readonly ushort CURSOR_EXISTS = 64;

    /**
     * last row sent
     */
    public static readonly ushort LAST_ROW_SENT = 128;

    /**
     * database dropped
     */
    public static readonly ushort DB_DROPPED = 256;

    /**
     * escape type
     */
    public static readonly ushort NO_BACKSLASH_ESCAPES = 512;

    /**
     * metadata changed
     */
    public static readonly ushort METADATA_CHANGED = 1024;

    /**
     * query was slow
     */
    public static readonly ushort QUERY_WAS_SLOW = 2048;

    /**
     * resultset contains output parameters
     */
    public static readonly ushort PS_OUT_PARAMETERS = 4096;

    /**
     * session state change (OK_Packet contains additional data)
     */
    public static readonly ushort SERVER_SESSION_STATE_CHANGED = 1 << 14;
}