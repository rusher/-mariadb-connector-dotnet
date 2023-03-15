using System.Buffers;
using System.Buffers.Binary;
using System.Text;

namespace MariadbConnector.client.socket;

public class ByteBufferWriter
{
    private static readonly byte QUOTE = (byte)'\'';
    private static readonly byte DBL_QUOTE = (byte)'"';
    private static readonly byte ZERO_BYTE = (byte)'\0';
    private static readonly byte BACKSLASH = (byte)'\\';
    private static readonly int MEDIUM_BUFFER_SIZE = 128 * 1024;
    private static readonly int LARGE_BUFFER_SIZE = 1024 * 1024;
    private static readonly int MAX_PACKET_LENGTH = 0x00ffffff + 4;
    private Memory<byte> m_output;


    public ByteBufferWriter(int capacity = 0, int initialPos = 4)
    {
        Buf = ArrayPool<byte>.Shared.Rent(Math.Max(capacity, 128));
        m_output = Buf;
        m_output = m_output[initialPos..]; // reserve header size
    }

    public int Position => Buf.Length - m_output.Length;

    public ArraySegment<byte> ArraySegment => new(Buf, 0, Position);

    public byte[] Buf { get; private set; }

    public PayloadData ToPayloadData()
    {
        return new PayloadData(ArraySegment, Buf, true);
    }

    private void Reallocate(int additional = 0)
    {
        var usedLength = Position;
        var newBuffer = ArrayPool<byte>.Shared.Rent(Math.Max(usedLength + additional, Buf.Length * 2));
        Buffer.BlockCopy(Buf, 0, newBuffer, 0, usedLength);
        ArrayPool<byte>.Shared.Return(Buf);
        Buf = newBuffer;
        m_output = new Memory<byte>(Buf, usedLength, Buf.Length - usedLength);
    }


    public void WriteByte(int value)
    {
        if (m_output.Length < 1)
            Reallocate();
        m_output.Span[0] = (byte)value;
        m_output = m_output[1..];
    }

    public void WriteShort(short value)
    {
        if (m_output.Length < 2)
            Reallocate(2);
        m_output.Span[0] = (byte)value;
        m_output.Span[1] = (byte)(value >> 8);
        m_output = m_output[2..];
    }

    public void WriteInt(int value)
    {
        if (m_output.Length < 4)
            Reallocate(4);
        m_output.Span[0] = (byte)value;
        m_output.Span[1] = (byte)(value >> 8);
        m_output.Span[2] = (byte)(value >> 16);
        m_output.Span[3] = (byte)(value >> 24);
        m_output = m_output[4..];
    }

    public void WriteUInt(uint value)
    {
        if (m_output.Length < 4)
            Reallocate(4);
        m_output.Span[0] = (byte)value;
        m_output.Span[1] = (byte)(value >> 8);
        m_output.Span[2] = (byte)(value >> 16);
        m_output.Span[3] = (byte)(value >> 24);
        m_output = m_output[4..];
    }

    public void WriteLong(long value)
    {
        if (m_output.Length < 8)
            Reallocate(8);
        m_output.Span[0] = (byte)value;
        m_output.Span[1] = (byte)(value >> 8);
        m_output.Span[2] = (byte)(value >> 16);
        m_output.Span[3] = (byte)(value >> 24);
        m_output.Span[4] = (byte)(value >> 32);
        m_output.Span[5] = (byte)(value >> 40);
        m_output.Span[6] = (byte)(value >> 48);
        m_output.Span[7] = (byte)(value >> 56);
        m_output = m_output[8..];
    }

    public void WriteDouble(double value)
    {
        if (m_output.Length < 8)
            Reallocate(8);
        BinaryPrimitives.WriteDoubleLittleEndian(m_output.Span, value);
        m_output = m_output[8..];
    }

    public void WriteFloat(float value)
    {
        WriteBytes(new ReadOnlySpan<byte>(BitConverter.GetBytes(value)));
    }

    public void WriteBytes(ReadOnlySpan<byte> span)
    {
        if (m_output.Length < span.Length)
            Reallocate(span.Length);
        span.CopyTo(m_output.Span);
        m_output = m_output[span.Length..];
    }

    public void WriteBytes(byte[] arr, int off, int len)
    {
        if (m_output.Length < len)
            Reallocate(len);
        new ReadOnlySpan<byte>(arr, off, len).CopyTo(m_output.Span);
        m_output = m_output[len..];
    }

    public void WriteLength(long length)
    {
        if (length < 251)
        {
            WriteByte((byte)length);
            return;
        }

        if (length < 65536)
        {
            if (m_output.Length < 3)
                Reallocate(3);
            m_output.Span[0] = 0xfc;
            m_output.Span[1] = (byte)length;
            m_output.Span[2] = (byte)(length >>> 8);
            m_output = m_output[3..];
            return;
        }

        if (length < 16777216)
        {
            // not enough space remaining
            if (m_output.Length < 4)
                Reallocate(4);
            m_output.Span[0] = 0xfd;
            m_output.Span[1] = (byte)length;
            m_output.Span[2] = (byte)(length >>> 8);
            m_output.Span[3] = (byte)(length >>> 16);
            m_output = m_output[4..];
            return;
        }

        if (m_output.Length < 9)
            Reallocate(9);
        m_output.Span[0] = 0xfe;
        m_output.Span[1] = (byte)length;
        m_output.Span[2] = (byte)(length >>> 8);
        m_output.Span[3] = (byte)(length >>> 16);
        m_output.Span[4] = (byte)(length >>> 24);
        m_output.Span[5] = (byte)(length >>> 32);
        m_output.Span[6] = (byte)(length >>> 40);
        m_output.Span[7] = (byte)(length >>> 48);
        m_output.Span[8] = (byte)(length >>> 56);
        m_output = m_output[9..];
    }

    public void WriteAscii(string str)
    {
        var len = str.Length;
        if (m_output.Length < len)
            Reallocate(len);

        for (var off = 0; off < len;) m_output.Span[off] = (byte)str[off++];
        m_output = m_output[len..];
    }

    public void WriteString(string str)
    {
        var charsLength = str.Length;

        // not enough space remaining
        if (m_output.Length < 3 * charsLength)
        {
            var arr = Encoding.UTF8.GetBytes(str);
            WriteBytes(arr, 0, arr.Length);
            return;
        }

        // create UTF-8 byte array
        // 4 bytes unicode characters will
        // represent 2 characters : example "\uD83C\uDFA4" = 🎤 unicode 8 "no microphones"
        // so max size is 3 * charLength
        // (escape characters are 1 byte encoded, so length might only be 2 when escape)
        var charsOffset = 0;
        char currChar;

        // quick loop if only ASCII chars for faster escape
        for (;
             charsOffset < charsLength && (currChar = str[charsOffset]) < 0x80;
             charsOffset++)
        {
            m_output.Span[0] = (byte)currChar;
            m_output = m_output[1..];
        }

        // if quick loop not finished
        while (charsOffset < charsLength)
        {
            currChar = str[charsOffset++];
            if (currChar < 0x80)
            {
                m_output.Span[0] = (byte)currChar;
                m_output = m_output[1..];
            }
            else if (currChar < 0x800)
            {
                m_output.Span[0] = (byte)(0xc0 | (currChar >> 6));
                m_output.Span[1] = (byte)(0x80 | (currChar & 0x3f));
                m_output = m_output[2..];
            }
            else if (currChar >= 0xD800 && currChar < 0xE000)
            {
                // reserved for surrogate - see https://en.wikipedia.org/wiki/UTF-16
                if (currChar < 0xDC00)
                {
                    // is high surrogate
                    if (charsOffset + 1 > charsLength)
                    {
                        m_output.Span[0] = 0x63;
                        m_output = m_output[1..];
                    }
                    else
                    {
                        var nextChar = str[charsOffset];
                        if (nextChar >= 0xDC00 && nextChar < 0xE000)
                        {
                            // is low surrogate
                            var surrogatePairs =
                                (currChar << 10) + nextChar + (0x010000 - (0xD800 << 10) - 0xDC00);
                            m_output.Span[0] = (byte)(0xf0 | (surrogatePairs >> 18));
                            m_output.Span[1] = (byte)(0x80 | ((surrogatePairs >> 12) & 0x3f));
                            m_output.Span[2] = (byte)(0x80 | ((surrogatePairs >> 6) & 0x3f));
                            m_output.Span[3] = (byte)(0x80 | (surrogatePairs & 0x3f));
                            m_output = m_output[4..];
                            charsOffset++;
                        }
                        else
                        {
                            // must have low surrogate
                            m_output.Span[0] = 0x3f;
                            m_output = m_output[1..];
                        }
                    }
                }
                else
                {
                    // low surrogate without high surrogate before
                    m_output.Span[0] = 0x3f;
                    m_output = m_output[1..];
                }
            }
            else
            {
                m_output.Span[0] = (byte)(0xe0 | (currChar >> 12));
                m_output.Span[1] = (byte)(0x80 | ((currChar >> 6) & 0x3f));
                m_output.Span[2] = (byte)(0x80 | (currChar & 0x3f));
                m_output = m_output[3..];
            }
        }
    }

    public void WriteStringEscaped(string str, bool noBackslashEscapes)
    {
        var charsLength = str.Length;

        // not enough space remaining
        if (m_output.Length < 3 * charsLength)
        {
            var arr = Encoding.UTF8.GetBytes(str);
            WriteBytesEscaped(arr, arr.Length, noBackslashEscapes);
            return;
        }

        // create UTF-8 byte array
        // 4 bytes unicode characters will
        // represent 2 characters : example "\uD83C\uDFA4" = 🎤 unicode 8 "no microphones"
        // so max size is 3 * charLength
        // (escape characters are 1 byte encoded, so length might only be 2 when escape)
        // + 2 for the quotes for text protocol
        var charsOffset = 0;
        char currChar;

        // quick loop if only ASCII chars for faster escape
        if (noBackslashEscapes)
            for (;
                 charsOffset < charsLength && (currChar = str[charsOffset]) < 0x80;
                 charsOffset++)
            {
                if (currChar == QUOTE)
                {
                    m_output.Span[0] = QUOTE;
                    m_output = m_output[1..];
                }

                m_output.Span[0] = (byte)currChar;
                m_output = m_output[1..];
            }
        else
            for (;
                 charsOffset < charsLength && (currChar = str[charsOffset]) < 0x80;
                 charsOffset++)
            {
                if (currChar == BACKSLASH || currChar == QUOTE || currChar == 0 || currChar == DBL_QUOTE)
                {
                    m_output.Span[0] = BACKSLASH;
                    m_output = m_output[1..];
                }

                m_output.Span[0] = (byte)currChar;
                m_output = m_output[1..];
            }

        // if quick loop not finished
        while (charsOffset < charsLength)
        {
            currChar = str[charsOffset++];
            if (currChar < 0x80)
            {
                if (noBackslashEscapes)
                {
                    if (currChar == QUOTE)
                    {
                        m_output.Span[0] = QUOTE;
                        m_output = m_output[1..];
                    }
                }
                else if (currChar == BACKSLASH
                         || currChar == QUOTE
                         || currChar == ZERO_BYTE
                         || currChar == DBL_QUOTE)
                {
                    m_output.Span[0] = BACKSLASH;
                    m_output = m_output[1..];
                }

                m_output.Span[0] = (byte)currChar;
                m_output = m_output[1..];
            }
            else if (currChar < 0x800)
            {
                m_output.Span[0] = (byte)(0xc0 | (currChar >> 6));
                m_output.Span[1] = (byte)(0x80 | (currChar & 0x3f));
                m_output = m_output[2..];
            }
            else if (currChar >= 0xD800 && currChar < 0xE000)
            {
                // reserved for surrogate - see https://en.wikipedia.org/wiki/UTF-16
                if (currChar < 0xDC00)
                {
                    // is high surrogate
                    if (charsOffset + 1 > charsLength)
                    {
                        m_output.Span[0] = 0x63;
                        m_output = m_output[1..];
                    }
                    else
                    {
                        var nextChar = str[charsOffset];
                        if (nextChar >= 0xDC00 && nextChar < 0xE000)
                        {
                            // is low surrogate
                            var surrogatePairs =
                                (currChar << 10) + nextChar + (0x010000 - (0xD800 << 10) - 0xDC00);

                            m_output.Span[0] = (byte)(0xf0 | (surrogatePairs >> 18));
                            m_output.Span[1] = (byte)(0x80 | ((surrogatePairs >> 12) & 0x3f));
                            m_output.Span[2] = (byte)(0x80 | ((surrogatePairs >> 6) & 0x3f));
                            m_output.Span[3] = (byte)(0x80 | (surrogatePairs & 0x3f));
                            m_output = m_output[4..];

                            charsOffset++;
                        }
                        else
                        {
                            // must have low surrogate
                            m_output.Span[0] = 0x3f;
                            m_output = m_output[1..];
                        }
                    }
                }
                else
                {
                    // low surrogate without high surrogate before
                    m_output.Span[0] = 0x3f;
                    m_output = m_output[1..];
                }
            }
            else
            {
                m_output.Span[0] = (byte)(0xe0 | (currChar >> 12));
                m_output.Span[1] = (byte)(0x80 | ((currChar >> 6) & 0x3f));
                m_output.Span[2] = (byte)(0x80 | (currChar & 0x3f));
                m_output = m_output[3..];
            }
        }
    }

    public void WriteBytesEscaped(byte[] bytes, int len, bool noBackslashEscapes)
    {
        if (m_output.Length < len)
            Reallocate(len);


        // sure to have enough place filling buf directly
        if (noBackslashEscapes)
            for (var i = 0; i < len; i++)
            {
                if (QUOTE == bytes[i])
                {
                    m_output.Span[0] = QUOTE;
                    m_output = m_output[1..];
                }

                m_output.Span[0] = bytes[i];
                m_output = m_output[1..];
            }
        else
            for (var i = 0; i < len; i++)
            {
                if (bytes[i] == QUOTE
                    || bytes[i] == BACKSLASH
                    || bytes[i] == '"'
                    || bytes[i] == ZERO_BYTE)
                {
                    m_output.Span[0] = BACKSLASH;
                    m_output = m_output[1..];
                }

                m_output.Span[0] = bytes[i];
                m_output = m_output[1..];
            }
    }
}