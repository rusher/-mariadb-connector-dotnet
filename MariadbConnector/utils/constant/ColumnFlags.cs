namespace MariadbConnector.utils.constant;

public class ColumnFlags
{
    /**
     * must a column have non-null value only
     */
    public static readonly ushort NOT_NULL = 1;

    /**
     * Is column a primary key
     */
    public static readonly ushort PRIMARY_KEY = 2;

    /**
     * Is this column a unique key
     */
    public static readonly ushort UNIQUE_KEY = 4;

    /**
     * Is this column part of a multiple column key
     */
    public static readonly ushort MULTIPLE_KEY = 8;

    /**
     * Does this column contain blob
     */
    public static readonly ushort BLOB = 16;

    /**
     * Is column number value unsigned
     */
    public static readonly ushort UNSIGNED = 32;

    /**
     * Must number value be filled with Zero
     */
    public static readonly ushort ZEROFILL = 64;

    /**
     * Is binary value
     */
    public static readonly ushort BINARY_COLLATION = 128;

    /**
     * Is column of type enum
     */
    public static readonly ushort ENUM = 256;

    /**
     * Does column auto-increment
     */
    public static readonly ushort AUTO_INCREMENT = 512;

    /**
     * Is column of type Timestamp
     */
    public static readonly ushort TIMESTAMP = 1024;

    /**
     * Is column type set
     */
    public static readonly ushort SET = 2048;

    /**
     * Does column have no default value
     */
    public static readonly ushort NO_DEFAULT_VALUE_FLAG = 4096;
}