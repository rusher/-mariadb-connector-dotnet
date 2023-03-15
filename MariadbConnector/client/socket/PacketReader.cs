using System.Net.Sockets;
using MariadbConnector.client.impl;
using MariadbConnector.client.util;
using MariadbConnector.utils.log;

namespace MariadbConnector.client.socket;

public class PacketReader : IReader
{
    private const int REUSABLE_BUFFER_LENGTH = 1024;
    private const int MAX_PACKET_SIZE = 0xffffff;
    private static readonly Ilogger logger = Loggers.getLogger("PacketReader");
    private readonly BufferedReader _bufferedReader;

    private readonly byte[] _header = new byte[4];
    private readonly uint _maxQuerySizeToLog;

    private readonly IReadableByteBuf _readBuf = new StandardReadableByteBuf(null, 0);
    private readonly byte[] _reusableArray = new byte[REUSABLE_BUFFER_LENGTH];

    private readonly MutableByte _sequence;
    private readonly Socket _socket;
    private readonly Stream _stream;
    private string _serverThreadLog = "";

    public PacketReader(Stream stream, Socket socket, Configuration conf, MutableByte sequence)
    {
        _stream = stream;
        _socket = socket;
        _bufferedReader = new BufferedReader(_stream, _socket);
        _maxQuerySizeToLog = conf.MaxQuerySizeToLog;
        _sequence = sequence;
    }

    public IReadableByteBuf ReadableBufFromArray(byte[] buf)
    {
        _readBuf.Buf(buf, buf.Length, 0);
        return _readBuf;
    }

    public async Task<IReadableByteBuf> ReadReusablePacket(CancellationToken cancellationToken)
    {
        return await ReadReusablePacket(cancellationToken, logger.isTraceEnabled()).ConfigureAwait(false);
    }

    public async Task<IReadableByteBuf> ReadReusablePacket(CancellationToken cancellationToken, bool traceEnable)
    {
        // ***************************************************
        // Read 4 byte header
        // ***************************************************
        var arr = await _bufferedReader.ReadAsync(4).ConfigureAwait(false);

        var lastPacketLength =
            (arr.Array[arr.Offset] & 0xff) + ((arr.Array[arr.Offset + 1] & 0xff) << 8) +
            ((arr.Array[arr.Offset + 2] & 0xff) << 16);
        _sequence.Value = arr.Array[arr.Offset + 3];

        // ***************************************************
        // Read content
        // ***************************************************
        var remaining = lastPacketLength;
        arr = await _bufferedReader.ReadAsync(remaining).ConfigureAwait(false);

        if (traceEnable)
        {
            _header[0] = (byte)lastPacketLength;
            _header[1] = (byte)(lastPacketLength >> 8);
            _header[2] = (byte)(lastPacketLength >> 16);
            _header[3] = _sequence.Value;
            logger.trace(
                $"read: {_serverThreadLog}\n{LoggerHelper.Hex(_header, arr.Array, arr.Offset, lastPacketLength, _maxQuerySizeToLog)}");
        }

        _readBuf.Buf(arr.Array, arr.Offset + lastPacketLength, arr.Offset);
        return _readBuf;
    }

    public IReadableByteBuf ReadReusablePacketSync()
    {
        // ***************************************************
        // Read 4 byte header
        // ***************************************************
        var arr = _bufferedReader.ReadSync(4);

        var lastPacketLength =
            (arr.Array[arr.Offset] & 0xff) + ((arr.Array[arr.Offset + 1] & 0xff) << 8) +
            ((arr.Array[arr.Offset + 2] & 0xff) << 16);
        _sequence.Value = arr.Array[arr.Offset + 3];

        // ***************************************************
        // Read content
        // ***************************************************
        var remaining = lastPacketLength;
        arr = _bufferedReader.ReadSync(remaining);

        if (logger.isTraceEnabled())
        {
            _header[0] = (byte)lastPacketLength;
            _header[1] = (byte)(lastPacketLength >> 8);
            _header[2] = (byte)(lastPacketLength >> 16);
            _header[3] = _sequence.Value;
            logger.trace(
                $"read: {_serverThreadLog}\n{LoggerHelper.Hex(_header, arr.Array, arr.Offset, lastPacketLength, _maxQuerySizeToLog)}");
        }

        _readBuf.Buf(arr.Array, arr.Offset + lastPacketLength, arr.Offset);
        return _readBuf;
    }

    public async Task<byte[]> ReadPacket(CancellationToken cancellationToken, bool traceEnable)
    {
        // ***************************************************
        // Read 4 byte header
        // ***************************************************
        var arr = await _bufferedReader.ReadAsync(4).ConfigureAwait(false);
        var lastPacketLength =
            (arr.Array[arr.Offset] & 0xff) + ((arr.Array[arr.Offset + 1] & 0xff) << 8) +
            ((arr.Array[arr.Offset + 2] & 0xff) << 16);
        _sequence.Value = arr.Array[arr.Offset + 3];

        // ***************************************************
        // Read content
        // ***************************************************
        var rawBytes = new byte[lastPacketLength];
        arr = await _bufferedReader.ReadAsync(lastPacketLength).ConfigureAwait(false);
        arr.CopyTo(rawBytes);

        if (traceEnable)
        {
            _header[0] = (byte)lastPacketLength;
            _header[1] = (byte)(lastPacketLength >> 8);
            _header[2] = (byte)(lastPacketLength >> 16);
            _header[3] = _sequence.Value;
            logger.trace(
                $"read: {_serverThreadLog}\n{LoggerHelper.Hex(_header, rawBytes, 0, lastPacketLength, _maxQuerySizeToLog)}");
        }

        // ***************************************************
        // In case content length is big, content will be separate in many 16Mb packets
        // ***************************************************
        if (lastPacketLength == MAX_PACKET_SIZE)
        {
            int packetLength;
            do
            {
                arr = await _bufferedReader.ReadAsync(4).ConfigureAwait(false);
                packetLength =
                    (arr.Array[arr.Offset] & 0xff) + ((arr.Array[arr.Offset + 1] & 0xff) << 8) +
                    ((arr.Array[arr.Offset + 2] & 0xff) << 16);
                _sequence.Value = arr.Array[arr.Offset + 3];

                var currentBufLength = rawBytes.Length;
                var newRawBytes = new byte[currentBufLength + packetLength];
                Array.Copy(rawBytes, 0, newRawBytes, 0, currentBufLength);
                rawBytes = newRawBytes;

                // ***************************************************
                // Read content
                // ***************************************************
                arr = await _bufferedReader.ReadAsync(packetLength).ConfigureAwait(false);
                arr.CopyTo(rawBytes, currentBufLength);

                if (traceEnable)
                {
                    _header[0] = (byte)packetLength;
                    _header[1] = (byte)(packetLength >> 8);
                    _header[2] = (byte)(packetLength >> 16);
                    _header[3] = _sequence.Value;
                    logger.trace(
                        $"read: {_serverThreadLog}\n{LoggerHelper.Hex(_header, rawBytes, currentBufLength, packetLength, _maxQuerySizeToLog)}");
                }

                lastPacketLength += packetLength;
            } while (packetLength == MAX_PACKET_SIZE);
        }

        return rawBytes;
    }


    public byte[] ReadPacketSync(bool traceEnable)
    {
        // ***************************************************
        // Read 4 byte header
        // ***************************************************
        var arr = _bufferedReader.ReadSync(4);
        var lastPacketLength =
            (arr.Array[arr.Offset] & 0xff) + ((arr.Array[arr.Offset + 1] & 0xff) << 8) +
            ((arr.Array[arr.Offset + 2] & 0xff) << 16);
        _sequence.Value = arr.Array[arr.Offset + 3];

        // ***************************************************
        // Read content
        // ***************************************************
        var rawBytes = new byte[lastPacketLength];
        arr = _bufferedReader.ReadSync(lastPacketLength);
        arr.CopyTo(rawBytes);

        if (traceEnable)
        {
            _header[0] = (byte)lastPacketLength;
            _header[1] = (byte)(lastPacketLength >> 8);
            _header[2] = (byte)(lastPacketLength >> 16);
            _header[3] = _sequence.Value;
            logger.trace(
                $"read: {_serverThreadLog}\n{LoggerHelper.Hex(_header, rawBytes, 0, lastPacketLength, _maxQuerySizeToLog)}");
        }

        // ***************************************************
        // In case content length is big, content will be separate in many 16Mb packets
        // ***************************************************
        if (lastPacketLength == MAX_PACKET_SIZE)
        {
            int packetLength;
            do
            {
                arr = _bufferedReader.ReadSync(4);
                packetLength =
                    (arr.Array[arr.Offset] & 0xff) + ((arr.Array[arr.Offset + 1] & 0xff) << 8) +
                    ((arr.Array[arr.Offset + 2] & 0xff) << 16);
                _sequence.Value = arr.Array[arr.Offset + 3];

                var currentBufLength = rawBytes.Length;
                var newRawBytes = new byte[currentBufLength + packetLength];
                Array.Copy(rawBytes, 0, newRawBytes, 0, currentBufLength);
                rawBytes = newRawBytes;

                // ***************************************************
                // Read content
                // ***************************************************
                arr = _bufferedReader.ReadSync(packetLength);
                arr.CopyTo(rawBytes, currentBufLength);

                if (traceEnable)
                {
                    _header[0] = (byte)packetLength;
                    _header[1] = (byte)(packetLength >> 8);
                    _header[2] = (byte)(packetLength >> 16);
                    _header[3] = _sequence.Value;
                    logger.trace(
                        $"read: {_serverThreadLog}\n{LoggerHelper.Hex(_header, rawBytes, currentBufLength, packetLength, _maxQuerySizeToLog)}");
                }

                lastPacketLength += packetLength;
            } while (packetLength == MAX_PACKET_SIZE);
        }

        return rawBytes;
    }

    public void SkipPacketSync()
    {
        ReadPacketSync(logger.isTraceEnabled());
    }

    public async Task SkipPacket(CancellationToken cancellationToken)
    {
        // if (logger.isTraceEnabled())
        // {
        await ReadReusablePacket(cancellationToken, logger.isTraceEnabled()).ConfigureAwait(false);
        // }
        //
        // // ***************************************************
        // // Read 4 byte header
        // // ***************************************************
        // var remaining = 4;
        // var off = 0;
        // do
        // {
        //     var count = await _stream.ReadAsync(_header, off, remaining, cancellationToken);
        //     if (count < 0)
        //         throw new IOException(
        //             "unexpected end of stream, read "
        //             + off
        //             + " bytes from 4 (socket was closed by server)");
        //     remaining -= count;
        //     off += count;
        // } while (remaining > 0);
        //
        // var lastPacketLength =
        //     (_header[0] & 0xff) + ((_header[1] & 0xff) << 8) + ((_header[2] & 0xff) << 16);
        //
        // remaining = lastPacketLength;
        // // skipping 
        // do
        // {
        //     var count = await _stream.ReadAsync(_header, 0, Math.Min(4, remaining), cancellationToken);
        //     if (count < 0)
        //         throw new IOException(
        //             "unexpected end of stream, skipping bytes (socket was closed by server)");
        //     remaining -= count;
        //     off += count;
        // } while (remaining > 0);
        //
        // // ***************************************************
        // // In case content length is big, content will be separate in many 16Mb packets
        // // ***************************************************
        // if (lastPacketLength == MAX_PACKET_SIZE)
        // {
        //     int packetLength;
        //     do
        //     {
        //         remaining = 4;
        //         off = 0;
        //         do
        //         {
        //             var count = await _stream.ReadAsync(_header, off, remaining, cancellationToken);
        //             if (count < 0) throw new IOException("unexpected end of stream, read " + off + " bytes from 4");
        //             remaining -= count;
        //             off += count;
        //         } while (remaining > 0);
        //
        //         packetLength = (_header[0] & 0xff) + ((_header[1] & 0xff) << 8) + ((_header[2] & 0xff) << 16);
        //
        //         remaining = packetLength;
        //         // skipping 
        //         do
        //         {
        //             var count = await _stream.ReadAsync(_header, 0, Math.Min(4, remaining), cancellationToken);
        //             if (count < 0)
        //                 throw new IOException(
        //                     "unexpected end of stream, skipping bytes (socket was closed by server)");
        //             remaining -= count;
        //             off += count;
        //         } while (remaining > 0);
        //
        //         lastPacketLength += packetLength;
        //     } while (packetLength == MAX_PACKET_SIZE);
        // }
    }

    public MutableByte GetSequence()
    {
        return _sequence;
    }

    public void Close()
    {
        _stream.Close();
    }

    public void SetServerThreadId(long? serverThreadId, HostAddress hostAddress)
    {
        var isMaster = hostAddress?.Primary;
        _serverThreadLog =
            "conn="
            + (serverThreadId == null ? "-1" : serverThreadId)
            + (isMaster != null ? " (" + (isMaster.Value ? "M" : "S") + ")" : "");
    }

    private class BufferedReader
    {
        private readonly Socket _socket;
        private readonly Stream _stream;

        private readonly byte[] m_buffer;

        private ArraySegment<byte> _internal;
        private ArraySegment<byte> m_remainingData;

        public BufferedReader(Stream stream, Socket socket)
        {
            _stream = stream;
            _socket = socket;
            m_buffer = new byte[16384];
            // m_remainingData = new(m_buffer, 0, 0);
        }

        public ValueTask<ArraySegment<byte>> ReadAsync(int count)
        {
// check if read can be satisfied from the buffer
            if (m_remainingData.Count >= count)
            {
                var readBytes = m_remainingData.Slice(0, count);
                m_remainingData = m_remainingData.Slice(count);
                return new ValueTask<ArraySegment<byte>>(readBytes);
            }

            // get a buffer big enough to hold all the data, and move any buffered data to the beginning
            var buffer = count > m_buffer.Length ? new byte[count] : m_buffer;
            if (m_remainingData.Count > 0)
            {
                Buffer.BlockCopy(m_remainingData.Array!, m_remainingData.Offset, buffer, 0, m_remainingData.Count);
                m_remainingData = new ArraySegment<byte>(buffer, 0, m_remainingData.Count);
            }

            return ReadBytesAsync(
                new ArraySegment<byte>(buffer, m_remainingData.Count, buffer.Length - m_remainingData.Count), count);
        }

        public ArraySegment<byte> ReadSync(int count)
        {
// check if read can be satisfied from the buffer
            if (m_remainingData.Count >= count)
            {
                var readBytes = m_remainingData.Slice(0, count);
                m_remainingData = m_remainingData.Slice(count);
                return readBytes;
            }

            // get a buffer big enough to hold all the data, and move any buffered data to the beginning
            var buffer = count > m_buffer.Length ? new byte[count] : m_buffer;
            if (m_remainingData.Count > 0)
            {
                Buffer.BlockCopy(m_remainingData.Array!, m_remainingData.Offset, buffer, 0, m_remainingData.Count);
                m_remainingData = new ArraySegment<byte>(buffer, 0, m_remainingData.Count);
            }

            return ReadBytesSync(
                new ArraySegment<byte>(buffer, m_remainingData.Count, buffer.Length - m_remainingData.Count), count);
        }

        private async ValueTask<ArraySegment<byte>> ReadBytesAsync(ArraySegment<byte> buffer, int totalBytesToRead)
        {
            while (true)
            {
                var readBytesCount = await _socket.ReceiveAsync(buffer, SocketFlags.None);
                if (readBytesCount == 0)
                {
                    var data = m_remainingData;
                    m_remainingData = default;
                    return data;
                }

                var bufferSize = buffer.Offset + readBytesCount;
                if (bufferSize >= totalBytesToRead)
                {
                    var bufferBytes = new ArraySegment<byte>(buffer.Array!, 0, bufferSize);
                    var requestedBytes = bufferBytes.Slice(0, totalBytesToRead);
                    m_remainingData = bufferBytes.Slice(totalBytesToRead);
                    return requestedBytes;
                }

                buffer = buffer.Slice(readBytesCount);
            }
        }

        private ArraySegment<byte> ReadBytesSync(ArraySegment<byte> buffer, int totalBytesToRead)
        {
            while (true)
            {
                var readBytesCount = _socket.Receive(buffer, SocketFlags.None);
                if (readBytesCount == 0)
                {
                    var data = m_remainingData;
                    m_remainingData = default;
                    return data;
                }

                var bufferSize = buffer.Offset + readBytesCount;
                if (bufferSize >= totalBytesToRead)
                {
                    var bufferBytes = new ArraySegment<byte>(buffer.Array!, 0, bufferSize);
                    var requestedBytes = bufferBytes.Slice(0, totalBytesToRead);
                    m_remainingData = bufferBytes.Slice(totalBytesToRead);
                    return requestedBytes;
                }

                buffer = buffer.Slice(readBytesCount);
            }
        }
    }
}