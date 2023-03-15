using System.Data;
using System.Data.Common;

namespace MariadbConnector;

public class MariaDbParameter : DbParameter, IDbDataParameter, ICloneable
{
    public override DbType DbType { get; set; }
    public override ParameterDirection Direction { get; set; }
    public override string ParameterName { get; set; }
    public override int Size { get; set; }
    public override string SourceColumn { get; set; }
    public override bool SourceColumnNullMapping { get; set; }
    public override DataRowVersion SourceVersion { get; set; }
    public override object? Value { get; set; }

    public object Clone()
    {
        throw new NotImplementedException();
    }

    public override bool IsNullable { get; set; }
    public override byte Precision { get; set; }
    public override byte Scale { get; set; }

    DbType IDataParameter.DbType
    {
        get => DbType;
        set => DbType = value;
    }

    ParameterDirection IDataParameter.Direction
    {
        get => Direction;
        set => Direction = value;
    }

    string IDataParameter.ParameterName
    {
        get => ParameterName;
        set => ParameterName = value;
    }

    string IDataParameter.SourceColumn
    {
        get => SourceColumn;
        set => SourceColumn = value;
    }

    DataRowVersion IDataParameter.SourceVersion
    {
        get => SourceVersion;
        set => SourceVersion = value;
    }

    object? IDataParameter.Value
    {
        get => Value;
        set => Value = value;
    }

    int IDbDataParameter.Size
    {
        get => Size;
        set => Size = value;
    }

    public override void ResetDbType()
    {
        throw new NotImplementedException();
    }

    public override object InitializeLifetimeService()
    {
        return base.InitializeLifetimeService();
    }

    public override bool Equals(object? obj)
    {
        return base.Equals(obj);
    }

    public override int GetHashCode()
    {
        return base.GetHashCode();
    }

    public override string ToString()
    {
        return base.ToString();
    }
}