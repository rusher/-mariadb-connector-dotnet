using System.Buffers;
using System.Runtime.InteropServices;

namespace MariadbConnector.client.socket;

public readonly struct PayloadData : IDisposable
{
    public PayloadData(byte[] data)
    {
        Memory = data;
    }

    private readonly byte[] _buffer;

    public PayloadData(ReadOnlyMemory<byte> data, byte[] buffer, bool isPooled = false)
    {
        _buffer = buffer;
        Memory = data;
        m_isPooled = isPooled;
    }

    public ReadOnlyMemory<byte> Memory { get; }
    public ReadOnlySpan<byte> Span => Memory.Span;
    public byte HeaderByte => Span[0];

    public void SetHeader(int len, byte sequence)
    {
        _buffer[0] = (byte)len;
        _buffer[1] = (byte)(len >>> 8);
        _buffer[2] = (byte)(len >>> 16);
        _buffer[3] = sequence;
    }

    public void Dispose()
    {
        if (m_isPooled && MemoryMarshal.TryGetArray(Memory, out var arraySegment))
            ArrayPool<byte>.Shared.Return(arraySegment.Array!);
    }

    private readonly bool m_isPooled;
}