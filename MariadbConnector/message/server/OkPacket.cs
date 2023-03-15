using MariadbConnector.client;
using MariadbConnector.client.util;
using MariadbConnector.utils.constant;
using MariadbConnector.utils.log;

namespace MariadbConnector.message.server;

public class OkPacket : ICompletion
{
    private static readonly Ilogger logger = Loggers.getLogger("OkPacket");

    public readonly long AffectedRows;
    public readonly long LastInsertId;

    public OkPacket(IReadableByteBuf buf, IContext context)
    {
        buf.Skip(); // ok header
        AffectedRows = buf.ReadLongLengthEncodedNotNull();
        LastInsertId = buf.ReadLongLengthEncodedNotNull();
        context.ServerStatus = buf.ReadUnsignedShort();
        context.Warning = buf.ReadUnsignedShort();

        if (buf.ReadableBytes() > 0 && context.HasClientCapability(Capabilities.CLIENT_SESSION_TRACK))
        {
            buf.Skip(buf.ReadIntLengthEncodedNotNull()); // skip info
            while (buf.ReadableBytes() > 0)
                if (buf.ReadIntLengthEncodedNotNull() > 0)
                    switch (Convert.ToUInt16(buf.ReadByte()))
                    {
                        case StateChange.SESSION_TRACK_SYSTEM_VARIABLES:
                            buf.ReadIntLengthEncodedNotNull();
                            var variable = buf.ReadString(buf.ReadIntLengthEncodedNotNull());
                            var len = buf.ReadLength();
                            var value = len == null ? null : buf.ReadString(len.Value);
                            logger.debug($"System variable change:  {variable} = {value}");
                            break;

                        case StateChange.SESSION_TRACK_SCHEMA:
                            buf.ReadIntLengthEncodedNotNull();
                            var dbLen = buf.ReadLength();
                            var database = dbLen == null ? null : buf.ReadString(dbLen.Value);
                            context.Database = string.IsNullOrEmpty(database) ? null : database;
                            logger.debug($"Database change: is '{database}'");
                            break;

                        default:
                            buf.Skip(buf.ReadIntLengthEncodedNotNull());
                            break;
                    }
        }
    }
}