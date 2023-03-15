using System.Data;
using System.Text.RegularExpressions;
using MariadbConnector.client;
using MariadbConnector.client.impl;
using MariadbConnector.client.result;
using MariadbConnector.client.socket;
using MariadbConnector.client.util;
using MariadbConnector.message.server;
using MariadbConnector.utils;

namespace MariadbConnector.message;

public abstract class AbstractClientMessage : IClientMessage
{
    public abstract ByteBufferWriter BuildPayLoad(IContext context);

    public async Task WriteAsync(CancellationToken cancellationToken, ByteBufferWriter buf, IWriter writer)
    {
        writer.Init();
        await writer.WritePayloadAsync(buf.ToPayloadData(), cancellationToken);
        writer.Flush();
    }

    public void WriteSync(ByteBufferWriter buf, IWriter writer)
    {
        writer.Init();
        writer.WritePayloadSync(buf.ToPayloadData());
        writer.Flush();
    }

    public abstract string Description { get; }

    public uint BatchUpdateLength()
    {
        return 0;
    }

    public bool BinaryProtocol()
    {
        return false;
    }

    public bool CanSkipMeta()
    {
        return false;
    }

    public async Task<ICompletion> ReadPacketAsync(
        CancellationToken cancellationToken,
        MariaDbCommand stmt,
        CommandBehavior behavior,
        IReader reader,
        IWriter writer,
        IContext context,
        ExceptionFactory exceptionFactory,
        bool traceEnable,
        SemaphoreSlim lockObj,
        IClientMessage message)
    {
        var buf = await reader.ReadReusablePacket(cancellationToken, traceEnable);

        switch (buf.GetByte())
        {
            // *********************************************************************************************************
            // * OK response
            // *********************************************************************************************************
            case 0x00:
                return new OkPacket(buf, context);

            // *********************************************************************************************************
            // * ERROR response
            // *********************************************************************************************************
            case 0xff:
                // force current status to in transaction to ensure rollback/commit, since command may
                // have issue a transaction
                var errorPacket = new ErrorPacket(buf, context);
                throw exceptionFactory
                    .WithSql(Description)
                    .Create(
                        errorPacket.Message, errorPacket.SqlState, errorPacket.ErrorCode);
            case 0xfb:
                buf.Skip(1); // skip header
                Exception exception = null;
                reader.GetSequence().Value = 0x01;
                var inputStream = GetLocalInfileInputStream();
                if (inputStream == null)
                {
                    var fileName = buf.ReadStringNullEnd();
                    if (!ValidateLocalFileName(fileName, context))
                        exception =
                            exceptionFactory
                                .WithSql(Description)
                                .Create(
                                    $"LOAD DATA LOCAL INFILE asked for file '{fileName}' that doesn't correspond to initial query {Description}. Possible malicious proxy changing server answer ! Command interrupted",
                                    "HY000");
                    else
                        try
                        {
                            inputStream = File.OpenRead(fileName);
                        }
                        catch (FileNotFoundException f)
                        {
                            exception =
                                exceptionFactory
                                    .WithSql(Description)
                                    .Create("Could not send file : " + f.Message, "HY000", f);
                        }
                }

                // sending stream
                if (inputStream != null)
                    try
                    {
                        var fileBuf = new byte[8192];
                        int len;
                        while ((len = inputStream.Read(fileBuf)) > 0)
                            writer.WriteBytes(IoBehavior.Asynchronous, fileBuf, 0, len);
                    }
                    finally
                    {
                        inputStream.Close();
                    }

                // after file send / having an error, sending an empty packet to keep connection state ok
                writer.WriteEmptyPacket(IoBehavior.Asynchronous);
                var completion =
                    await ReadPacketAsync(cancellationToken,
                        stmt,
                        behavior,
                        reader,
                        writer,
                        context,
                        exceptionFactory,
                        traceEnable,
                        lockObj,
                        message);
                if (exception != null) throw exception;
                return completion;

            // *********************************************************************************************************
            // * ResultSet
            // *********************************************************************************************************
            default:
                var fieldCount = buf.ReadIntLengthEncodedNotNull();

                IColumnDecoder[] ci;
                var canSkipMeta = context.SkipMeta && CanSkipMeta();
                var skipMeta = canSkipMeta ? buf.ReadByte() == 0 : false;
                if (canSkipMeta && skipMeta)
                {
                    ci = stmt._getMeta();
                }
                else
                {
                    // read columns information's
                    ci = new IColumnDecoder[fieldCount];
                    for (var i = 0; i < fieldCount; i++)
                        ci[i] =
                            IColumnDecoder.Decode(
                                new StandardReadableByteBuf(await reader.ReadPacket(cancellationToken, traceEnable)),
                                context.ExtendedInfo);
                }

                if (canSkipMeta && !skipMeta) stmt.UpdateMeta(ci);

                // intermediate EOF
                if (!context.EofDeprecated) await reader.SkipPacket(cancellationToken);
                return new StreamingDataReader(stmt,
                    BinaryProtocol(),
                    ci,
                    reader,
                    context,
                    traceEnable,
                    lockObj, behavior);
        }
    }

    public ICompletion ReadPacketSync(
        MariaDbCommand stmt,
        CommandBehavior behavior,
        IReader reader,
        IWriter writer,
        IContext context,
        ExceptionFactory exceptionFactory,
        bool traceEnable,
        SemaphoreSlim lockObj,
        IClientMessage message)
    {
        var buf = reader.ReadReusablePacketSync();

        switch (buf.GetByte())
        {
            // *********************************************************************************************************
            // * OK response
            // *********************************************************************************************************
            case 0x00:
                return new OkPacket(buf, context);

            // *********************************************************************************************************
            // * ERROR response
            // *********************************************************************************************************
            case 0xff:
                // force current status to in transaction to ensure rollback/commit, since command may
                // have issue a transaction
                var errorPacket = new ErrorPacket(buf, context);
                throw exceptionFactory
                    .WithSql(Description)
                    .Create(
                        errorPacket.Message, errorPacket.SqlState, errorPacket.ErrorCode);
            case 0xfb:
                buf.Skip(1); // skip header
                Exception exception = null;
                reader.GetSequence().Value = 0x01;
                var inputStream = GetLocalInfileInputStream();
                if (inputStream == null)
                {
                    var fileName = buf.ReadStringNullEnd();
                    if (!ValidateLocalFileName(fileName, context))
                        exception =
                            exceptionFactory
                                .WithSql(Description)
                                .Create(
                                    $"LOAD DATA LOCAL INFILE asked for file '{fileName}' that doesn't correspond to initial query {Description}. Possible malicious proxy changing server answer ! Command interrupted",
                                    "HY000");
                    else
                        try
                        {
                            inputStream = File.OpenRead(fileName);
                        }
                        catch (FileNotFoundException f)
                        {
                            exception =
                                exceptionFactory
                                    .WithSql(Description)
                                    .Create("Could not send file : " + f.Message, "HY000", f);
                        }
                }

                // sending stream
                if (inputStream != null)
                    try
                    {
                        var fileBuf = new byte[8192];
                        int len;
                        while ((len = inputStream.Read(fileBuf)) > 0)
                            writer.WriteBytes(IoBehavior.Synchronous, fileBuf, 0, len);
                    }
                    finally
                    {
                        inputStream.Close();
                    }

                // after file send / having an error, sending an empty packet to keep connection state ok
                writer.WriteEmptyPacket(IoBehavior.Synchronous);
                var completion =
                    ReadPacketSync(
                        stmt,
                        behavior,
                        reader,
                        writer,
                        context,
                        exceptionFactory,
                        traceEnable,
                        lockObj,
                        message);
                if (exception != null) throw exception;
                return completion;

            // *********************************************************************************************************
            // * ResultSet
            // *********************************************************************************************************
            default:
                var fieldCount = buf.ReadIntLengthEncodedNotNull();

                IColumnDecoder[] ci;
                var canSkipMeta = context.SkipMeta && CanSkipMeta();
                var skipMeta = canSkipMeta ? buf.ReadByte() == 0 : false;
                if (canSkipMeta && skipMeta)
                {
                    ci = stmt._getMeta();
                }
                else
                {
                    // read columns information's
                    ci = new IColumnDecoder[fieldCount];
                    for (var i = 0; i < fieldCount; i++)
                        ci[i] =
                            IColumnDecoder.Decode(
                                new StandardReadableByteBuf(reader.ReadPacketSync(traceEnable)),
                                context.ExtendedInfo);
                }

                if (canSkipMeta && !skipMeta) stmt.UpdateMeta(ci);

                // intermediate EOF
                if (!context.EofDeprecated) reader.SkipPacketSync();
                return new StreamingDataReader(stmt,
                    BinaryProtocol(),
                    ci,
                    reader,
                    context,
                    traceEnable,
                    lockObj, behavior);
        }
    }

    public Stream GetLocalInfileInputStream()
    {
        return null;
    }

    public bool ValidateLocalFileName(string fileName, IContext context)
    {
        return false;
    }

    public static bool ValidateLocalFileName(
        string sql, IParameters parameters, string fileName, IContext context)
    {
        var pattern =
            new Regex(
                "^(\\s*\\/\\*([^\\*]|\\*[^\\/])*\\*\\/)*\\s*LOAD\\s+(DATA|XML)\\s+((LOW_PRIORITY|CONCURRENT)\\s+)?LOCAL\\s+INFILE\\s+'"
                + fileName
                + "'",
                RegexOptions.IgnoreCase);
        if (pattern.Match(sql).Success) return true;

        if (parameters != null)
        {
            pattern =
                new Regex(
                    "^(\\s*\\/\\*([^\\*]|\\*[^\\/])*\\*\\/)*\\s*LOAD\\s+(DATA|XML)\\s+((LOW_PRIORITY|CONCURRENT)\\s+)?LOCAL\\s+INFILE\\s+\\?",
                    RegexOptions.IgnoreCase);
            if (pattern.Match(sql).Success && parameters.size() > 0)
            {
                var paramString = parameters.get(0).bestEffortStringValue(context);
                if (paramString != null)
                    return paramString.ToLowerInvariant().Equals("'" + fileName.ToLowerInvariant() + "'");
                return true;
            }
        }

        return false;
    }
}