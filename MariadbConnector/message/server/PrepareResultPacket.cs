using MariadbConnector.client;
using MariadbConnector.client.impl;
using MariadbConnector.client.socket;
using MariadbConnector.client.util;
using MariadbConnector.utils.constant;
using MariadbConnector.utils.log;

namespace MariadbConnector.message.server;

public class PrepareResultPacket : ICompletion, IPrepare
{
    private static readonly IColumnDecoder CONSTANT_PARAMETER;
    private static readonly Ilogger _logger = Loggers.getLogger("PrepareResultPacket");

    static PrepareResultPacket()
    {
        byte[] bytes =
        {
            0x03,
            0x64,
            0x65,
            0x66,
            0x00,
            0x00,
            0x00,
            0x01,
            0x3F,
            0x00,
            0x00,
            0x0C,
            0x3F,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x06,
            0x80,
            0x00,
            0x00,
            0x00,
            0x00
        };
        CONSTANT_PARAMETER =
            IColumnDecoder.Decode(new StandardReadableByteBuf(bytes, bytes.Length), true);
    }

    public PrepareResultPacket(uint statementId, IColumnDecoder[] parameters, IColumnDecoder[] columns)
    {
        StatementId = statementId;
        Parameters = parameters;
        Columns = columns;
    }

    public uint StatementId { get; }
    public IColumnDecoder[] Parameters { get; }
    public IColumnDecoder[] Columns { get; set; }

    public void Close(IClient con)
    {
        con.ClosePrepare(this);
    }

    public void DecrementUse(IClient con, MariaDbCommand dbCommand)
    {
        Close(con);
    }

    public void IncrementUse(MariaDbCommand dbCommand)
    {
    }

    public void UnCache(IClient client)
    {
    }

    public static async Task<PrepareResultPacket> Decode(CancellationToken cancellationToken, IReadableByteBuf buf,
        IReader reader, IContext context)
    {
        var trace = _logger.isTraceEnabled();
        buf.ReadByte(); /* skip COM_STMT_PREPARE_OK */
        var statementId = buf.ReadUnsignedInt();
        int numColumns = buf.ReadUnsignedShort();
        int numParams = buf.ReadUnsignedShort();
        var parameters = new IColumnDecoder[numParams];
        var columns = new IColumnDecoder[numColumns];

        if (numParams > 0)
        {
            for (var i = 0; i < numParams; i++)
            {
                // skipping packet, since there is no metadata information.
                // might change when https://jira.mariadb.org/browse/MDEV-15031 is done
                parameters[i] = CONSTANT_PARAMETER;
                await reader.SkipPacket(cancellationToken);
            }

            if (!context.EofDeprecated) await reader.SkipPacket(cancellationToken);
        }

        if (numColumns > 0)
        {
            for (var i = 0; i < numColumns; i++)
                columns[i] =
                    IColumnDecoder.Decode(
                        new StandardReadableByteBuf(await reader.ReadPacket(cancellationToken, trace)),
                        context.HasClientCapability(Capabilities.EXTENDED_TYPE_INFO));
            if (!context.EofDeprecated) await reader.SkipPacket(cancellationToken);
        }

        return new PrepareResultPacket(statementId, parameters, columns);
    }
}