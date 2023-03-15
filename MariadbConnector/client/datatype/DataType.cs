namespace MariadbConnector.client;

public enum DataType : byte
{
    OLDDECIMAL = 0,
    TINYINT = 1, //SignedTinyIntColumn::new, UnsignedTinyIntColumn::new),
    SMALLINT = 2, //SignedSmallIntColumn::new, UnsignedSmallIntColumn::new),
    INTEGER = 3, //SignedIntColumn::new, UnsignedIntColumn::new),
    FLOAT = 4, //FloatColumn::new, FloatColumn::new),
    DOUBLE = 5, //DoubleColumn::new, DoubleColumn::new),
    NULL = 6, //StringColumn::new, StringColumn::new),
    TIMESTAMP = 7, //TimestampColumn::new, TimestampColumn::new),
    BIGINT = 8, //SignedBigIntColumn::new, UnsignedBigIntColumn::new),
    MEDIUMINT = 9, //SignedMediumIntColumn::new, UnsignedMediumIntColumn::new),
    DATE = 10, //DateColumn::new, DateColumn::new),
    TIME = 11, //TimeColumn::new, TimeColumn::new),
    DATETIME = 12, //TimestampColumn::new, TimestampColumn::new),
    YEAR = 13, //YearColumn::new, YearColumn::new),
    NEWDATE = 14, //DateColumn::new, DateColumn::new),
    VARCHAR = 15, //StringColumn::new, StringColumn::new),
    BIT = 16, //BitColumn::new, BitColumn::new),
    JSON = 245, //JsonColumn::new, JsonColumn::new),
    DECIMAL = 246, //BigDecimalColumn::new, BigDecimalColumn::new),
    ENUM = 247, //StringColumn::new, StringColumn::new),
    SET = 248, //StringColumn::new, StringColumn::new),
    TINYBLOB = 249, //BlobColumn::new, BlobColumn::new),
    MEDIUMBLOB = 250, //BlobColumn::new, BlobColumn::new),
    LONGBLOB = 251, //BlobColumn::new, BlobColumn::new),
    BLOB = 252, //BlobColumn::new, BlobColumn::new),
    VARSTRING = 253, //StringColumn::new, StringColumn::new),
    STRING = 254, //StringColumn::new, StringColumn::new),
    GEOMETRY = 255 //GeometryColumn::new, GeometryColumn::new);
}