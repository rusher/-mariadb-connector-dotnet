namespace MariadbConnector.client;

public interface IColumn
{
    string GetSchema();
    string GetTableAlias();
    string GetTable();
    string GetColumnAlias();
    string GetColumnName();
    long GetColumnLength();
    DataType GetType();
    byte GetDecimals();
    bool IsSigned();
    int GetDisplaySize();
    bool IsPrimaryKey();
    bool IsAutoIncrement();
    bool HasDefault();
    bool IsBinary();
    int GetFlags();
    string? GetExtTypeName();
}