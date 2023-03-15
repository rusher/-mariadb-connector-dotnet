using System.Data.Common;
using System.Globalization;
using System.Numerics;
using System.Text;
using MariadbConnector.client;
using MariadbConnector.client.socket;
using MariadbConnector.utils;
using MariadbConnector.utils.constant;

namespace MariadbConnector.message.client;

public class QueryWithParametersPacket : AbstractClientMessage
{
    public static byte[] BINARY_PREFIX = Encoding.ASCII.GetBytes("_binary '");

    private readonly int CommandTimeout;

    private readonly Stream LocalInfileInputStream;
    private readonly DbParameterCollection Parameters;
    private readonly ClientParser Parser;

    public QueryWithParametersPacket(ClientParser parser, DbParameterCollection parameters, int commandTimeout,
        Stream localInfileInputStream = null)
    {
        CommandTimeout = commandTimeout;
        LocalInfileInputStream = localInfileInputStream;
        Parameters = parameters;
        Parser = parser;
    }

    public override string Description => Parser.Sql;

    public uint BatchUpdateLength()
    {
        return 1;
    }

    public override ByteBufferWriter BuildPayLoad(IContext context)
    {
        var buf = new ByteBufferWriter();
        buf.WriteByte(0x03);
        if (CommandTimeout > 0) buf.WriteAscii("SET STATEMENT max_statement_time=" + CommandTimeout + " FOR ");

        var pos = 0;
        int paramPos;
        for (var i = 0; i < Parser.ParamPositions.Count; i++)
        {
            paramPos = Parser.ParamPositions[i];
            buf.WriteBytes(Parser.Query, pos, paramPos - pos);
            pos = paramPos + 1;
            var param = Parameters[i];
            if (param == null)
            {
                buf.WriteAscii("null");
            }
            else
            {
                var val = param.Value;

                switch (val)
                {
                    case string stringValue:
                        buf.WriteByte('\'');
                        buf.WriteStringEscaped(stringValue,
                            (context.ServerStatus & ServerStatus.NO_BACKSLASH_ESCAPES) > 0);
                        buf.WriteByte('\'');
                        break;
                    case char charValue:
                    case Guid d:

                        buf.WriteByte('\'');
                        buf.WriteAscii(val.ToString());
                        buf.WriteByte('\'');
                        break;
                    case byte b:
                    case decimal d:
                    case short s:
                    case ushort us:
                    case int ii:
                    case uint ui:
                    case long lo:
                    case ulong ulo:
                        buf.WriteAscii(val.ToString());
                        break;
                    case byte[] ba:
                        buf.WriteBytes(BINARY_PREFIX);
                        buf.WriteBytes(ba);
                        buf.WriteByte('\'');
                        break;
                    case bool b:
                        buf.WriteAscii(b ? "1" : "0");
                        break;
                    case float f:
                        buf.WriteAscii(f.ToString("R", CultureInfo.InvariantCulture));
                        break;
                    case double d:
                        buf.WriteAscii(d.ToString("R", CultureInfo.InvariantCulture));
                        break;
                    case BigInteger b:
                        buf.WriteAscii(b.ToString(CultureInfo.InvariantCulture));
                        break;
                    case DateTime d:
                        buf.WriteByte('\'');
                        buf.WriteAscii(d.ToString("yyyy-MM-dd HH:mm:ss"));
                        var microseconds = d.Nanosecond / 1000;
                        if (microseconds > 0)
                        {
                            if (microseconds % 1000 == 0)
                                buf.WriteAscii("." + (microseconds / 1000 + 1000).ToString().Substring(1));
                            else
                                buf.WriteAscii("." + (microseconds + 1000000).ToString().Substring(1));
                        }

                        buf.WriteByte('\'');
                        break;
                    default:
                        throw new ArgumentException("parameter not supported");
                }
            }
        }

        buf.WriteBytes(Parser.Query, pos, Parser.Query.Length - pos);
        return buf;
    }

    public bool IsCommit()
    {
        return string.Compare("COMMIT", Parser.Sql, StringComparison.OrdinalIgnoreCase) == 0;
    }

    public bool ValidateLocalFileName(string fileName, IContext context)
    {
        return ValidateLocalFileName(Parser.Sql, null, fileName, context);
    }
}