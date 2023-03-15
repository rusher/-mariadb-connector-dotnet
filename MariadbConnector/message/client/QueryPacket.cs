using MariadbConnector.client;
using MariadbConnector.client.socket;

namespace MariadbConnector.message.client;

public class QueryPacket : AbstractClientMessage
{
    private readonly int CommandTimeout;
    private readonly Stream LocalInfileInputStream;

    private readonly string Sql;

    public QueryPacket(string sql, int commandTimeout = 0, Stream localInfileInputStream = null)
    {
        Sql = sql;
        CommandTimeout = commandTimeout;
        LocalInfileInputStream = localInfileInputStream;
    }

    public override string Description => Sql;

    public uint BatchUpdateLength()
    {
        return 1;
    }

    public override ByteBufferWriter BuildPayLoad(IContext context)
    {
        var buf = new ByteBufferWriter();
        buf.WriteByte(0x03);
        if (CommandTimeout > 0) buf.WriteAscii("SET STATEMENT max_statement_time=" + CommandTimeout + " FOR ");
        buf.WriteString(Sql);
        return buf;
    }

    public bool IsCommit()
    {
        return string.Compare("COMMIT", Sql, StringComparison.OrdinalIgnoreCase) == 0;
    }

    public bool ValidateLocalFileName(string fileName, IContext context)
    {
        return ValidateLocalFileName(Sql, null, fileName, context);
    }
}