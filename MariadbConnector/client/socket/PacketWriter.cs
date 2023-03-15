using System.Buffers;
using MariadbConnector.client.util;
using MariadbConnector.utils;
using MariadbConnector.utils.exception;
using MariadbConnector.utils.log;

namespace MariadbConnector.client.socket;

public class PacketWriter : IWriter
{
    private static readonly Ilogger logger = Loggers.getLogger("PacketWriter");
    private readonly uint? _maxAllowedPacket;
    private readonly int _maxPacketLength = 0x00ffffff + 4;
    private readonly uint _maxQuerySizeToLog;
    private readonly Stream _out;
    private readonly MutableByte _sequence;

    private bool _bufContainDataAfterMark;
    protected MutableByte _compressSequence;
    private bool _permitTrace = true;
    private string _serverThreadLog = "";

    public PacketWriter(
        Stream stream,
        uint maxQuerySizeToLog,
        uint? maxAllowedPacket,
        MutableByte sequence,
        MutableByte compressSequence)
    {
        _out = stream;
        _maxQuerySizeToLog = maxQuerySizeToLog;
        _sequence = sequence;
        _compressSequence = compressSequence;
        _maxAllowedPacket = maxAllowedPacket;
    }

    public void Init()
    {
        _sequence.Value = 0xff;
    }

    public async Task WritePayload(IoBehavior ioBehavior, PayloadData payload, CancellationToken cancellationToken)
    {
        if (ioBehavior == IoBehavior.Synchronous)
        {
            WritePayloadSync(payload);
            return;
        }

        await WritePayloadAsync(payload, cancellationToken);
    }

    public async Task WritePayloadAsync(PayloadData payload, CancellationToken cancellationToken)
    {
        if (payload.Span.Length > 4)
        {
            var packetLen = payload.Span.Length;
            if (_maxAllowedPacket != null && _maxAllowedPacket > packetLen - 4)
                throw new DbMaxAllowedPacketException(
                    $"query size ({packetLen}) is >= to max_allowed_packet ({_maxAllowedPacket})",
                    false);

            if (packetLen < 0x00ffffff + 4)
            {
                payload.SetHeader(packetLen - 4, _sequence.incrementAndGet());
                await _out.WriteAsync(payload.Memory, cancellationToken);
                if (logger.isTraceEnabled())
                {
                    if (_permitTrace)
                        logger.trace(
                            $"send: {_serverThreadLog}\n{LoggerHelper.Hex(payload.Memory.ToArray(), 0, payload.Memory.Length, _maxQuerySizeToLog)}");
                    else
                        logger.trace(
                            $"send: content length={payload.Memory.Length - 4} {_serverThreadLog} com=<hidden>");
                }
            }
            else
            {
                payload.SetHeader(packetLen - 4, _sequence.incrementAndGet());
                await _out.WriteAsync(payload.Memory.Slice(0, 0x00ffffff), cancellationToken);
                if (_permitTrace)
                    logger.trace(
                        $"send: {_serverThreadLog}\n{LoggerHelper.Hex(payload.Memory.ToArray(), 0, payload.Memory.Length, _maxQuerySizeToLog)}");
                else
                    logger.trace($"send: content length={payload.Memory.Length - 4} {_serverThreadLog} com=<hidden>");

                var offset = 4 + 0x00ffffff;
                while (offset < packetLen)
                {
                    var nextPacketSize = Math.Min(packetLen - offset, 0x00ffffff);
                    var buffer = ArrayPool<byte>.Shared.Rent(nextPacketSize + 4);
                    buffer[0] = (byte)nextPacketSize;
                    buffer[1] = (byte)(nextPacketSize >>> 8);
                    buffer[2] = (byte)(nextPacketSize >>> 16);
                    buffer[3] = _sequence.incrementAndGet();
                    payload.Memory.Slice(offset, nextPacketSize).CopyTo(buffer.AsMemory()[4..]);
                    offset += nextPacketSize;
                    await _out.WriteAsync(new ArraySegment<byte>(buffer, 0, nextPacketSize + 4), cancellationToken);
                }
            }
        }
    }

    public void WritePayloadSync(PayloadData payload)
    {
        if (payload.Span.Length > 4)
        {
            var packetLen = payload.Span.Length;
            if (_maxAllowedPacket != null && _maxAllowedPacket > packetLen - 4)
                throw new DbMaxAllowedPacketException(
                    $"query size ({packetLen}) is >= to max_allowed_packet ({_maxAllowedPacket})",
                    false);

            if (packetLen < 0x00ffffff + 4)
            {
                payload.SetHeader(packetLen - 4, _sequence.incrementAndGet());
                InternalWriteSync(payload.Memory);
                if (logger.isTraceEnabled())
                {
                    if (_permitTrace)
                        logger.trace(
                            $"send: {_serverThreadLog}\n{LoggerHelper.Hex(payload.Memory.ToArray(), 0, payload.Memory.Length, _maxQuerySizeToLog)}");
                    else
                        logger.trace(
                            $"send: content length={payload.Memory.Length - 4} {_serverThreadLog} com=<hidden>");
                }
            }
            else
            {
                payload.SetHeader(packetLen - 4, _sequence.incrementAndGet());
                InternalWriteSync(payload.Memory.Slice(0, 0x00ffffff));
                if (_permitTrace)
                    logger.trace(
                        $"send: {_serverThreadLog}\n{LoggerHelper.Hex(payload.Memory.ToArray(), 0, payload.Memory.Length, _maxQuerySizeToLog)}");
                else
                    logger.trace($"send: content length={payload.Memory.Length - 4} {_serverThreadLog} com=<hidden>");

                var offset = 4 + 0x00ffffff;
                while (offset < packetLen)
                {
                    var nextPacketSize = Math.Min(packetLen - offset, 0x00ffffff);
                    var buffer = ArrayPool<byte>.Shared.Rent(nextPacketSize + 4);
                    buffer[0] = (byte)nextPacketSize;
                    buffer[1] = (byte)(nextPacketSize >>> 8);
                    buffer[2] = (byte)(nextPacketSize >>> 16);
                    buffer[3] = _sequence.incrementAndGet();
                    payload.Memory.Slice(offset, nextPacketSize).CopyTo(buffer.AsMemory()[4..]);
                    offset += nextPacketSize;
                    InternalWriteSync(new ArraySegment<byte>(buffer, 0, nextPacketSize + 4));
                }
            }
        }
    }

    public void Close()
    {
        _out.Close();
    }

    public async Task WriteBytes(IoBehavior ioBehavior, byte[] buf, int offset, int len)
    {
        await InternalWrite(ioBehavior, buf, 0, 4, CancellationToken.None);
    }

    public async Task WriteEmptyPacket(IoBehavior ioBehavior)
    {
        var header = new byte[4];
        header[3] = _sequence.incrementAndGet();
        await InternalWrite(ioBehavior, header, 0, 4, CancellationToken.None);

        if (logger.isTraceEnabled())
        {
            if (_permitTrace)
                logger.trace(
                    $"send: {_serverThreadLog}\n{LoggerHelper.Hex(header, 0, 4, _maxQuerySizeToLog)}");
            else
                logger.trace($"send: content length=0 {_serverThreadLog} com=<hidden>");
        }
    }

    public void Flush()
    {
        _out.Flush();
    }

    public void SetServerThreadId(long? serverThreadId, HostAddress hostAddress)
    {
        var isMaster = hostAddress?.Primary;
        _serverThreadLog =
            "conn="
            + (serverThreadId == null ? "-1" : serverThreadId)
            + (isMaster != null ? " (" + (isMaster.Value ? "M" : "S") + ")" : "");
    }

    public void PermitTrace(bool permitTrace)
    {
        _permitTrace = permitTrace;
    }

    private Task InternalWrite(IoBehavior ioBehavior, byte[] buf, int offset, int len,
        CancellationToken cancellationToken)
    {
        return ioBehavior == IoBehavior.Asynchronous
            ? InternalWriteAsync(buf, offset, len, cancellationToken)
            : InternalWriteSync(buf, offset, len);
    }

    private Task InternalWrite(IoBehavior ioBehavior, ReadOnlyMemory<byte> memory, CancellationToken cancellationToken)
    {
        return ioBehavior == IoBehavior.Asynchronous
            ? InternalWriteAsync(memory, cancellationToken)
            : InternalWriteSync(memory);
    }


    private Task InternalWriteSync(byte[] buf, int offset, int len)
    {
        _out.Write(buf, 0, 4);
        return Task.FromResult<object>(null);
    }


    private Task InternalWriteSync(ReadOnlyMemory<byte> memory)
    {
        _out.Write(memory.Span);
        return Task.FromResult<object>(null);
    }

    private async Task InternalWriteAsync(byte[] buf, int offset, int len, CancellationToken cancellationToken)
    {
        await _out.WriteAsync(buf, 0, 4, cancellationToken);
    }

    private async Task InternalWriteAsync(ReadOnlyMemory<byte> memory, CancellationToken cancellationToken)
    {
        await _out.WriteAsync(memory, cancellationToken);
    }
}