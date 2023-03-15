using System.Text;
using MariadbConnector.client.util;

namespace MariadbConnector.client.impl;

public class StandardReadableByteBuf : IReadableByteBuf
{
    private byte[]? _buf;
    private int _limit;
    public int Pos;

    public StandardReadableByteBuf(byte[] buf, int limit)
    {
        Pos = 0;
        _buf = buf;
        _limit = limit;
    }

    public StandardReadableByteBuf(byte[] buf)
    {
        Pos = 0;
        _buf = buf;
        _limit = buf.Length;
    }

    public int ReadableBytes()
    {
        return _limit - Pos;
    }

    public void SetPos(int pos)
    {
        Pos = pos;
    }

    public void Buf(byte[] buf, int limit, int pos)
    {
        _buf = buf;
        _limit = limit;
        Pos = pos;
    }

    public void Skip()
    {
        Pos++;
    }

    public void Skip(int length)
    {
        Pos += length;
    }

    public void SkipLengthEncoded()
    {
        var len = _buf[Pos++];
        switch (len)
        {
            case 251:
                return;
            case 252:
                Skip(ReadUnsignedShort());
                return;
            case 253:
                Skip(ReadUnsignedMedium());
                return;
            case 254:
                Skip((int)(4 + ReadUnsignedInt()));
                return;
            default:
                Pos += len & 0xff;
                return;
        }
    }

    // public MariaDbBlob readBlob(int length) {
    //   pos += length;
    //   return MariaDbBlob.safeMariaDbBlob(buf, pos - length, length);
    // }

    public long Atoll(int length)
    {
        var negate = false;
        var idx = 0;
        long result = 0;

        if (length > 0 && _buf[Pos] == 45)
        {
            // minus sign
            negate = true;
            Pos++;
            idx++;
        }

        while (idx++ < length) result = result * 10 + _buf[Pos++] - 48;

        return negate ? -1 * result : result;
    }

    public long Atoull(int length)
    {
        long result = 0;
        for (var idx = 0; idx < length; idx++) result = result * 10 + _buf[Pos++] - 48;
        return result;
    }

    public byte GetByte()
    {
        return _buf[Pos];
    }

    public byte GetByte(int index)
    {
        return _buf[index];
    }

    public short GetUnsignedByte()
    {
        return (short)(_buf[Pos] & 0xff);
    }

    public long ReadLongLengthEncodedNotNull()
    {
        var type = _buf[Pos++] & 0xff;
        if (type < 251) return type;
        switch (type)
        {
            case 252: // 0xfc
                return ReadUnsignedShort();
            case 253: // 0xfd
                return ReadUnsignedMedium();
            default: // 0xfe
                return ReadLong();
        }
    }

    public int ReadIntLengthEncodedNotNull()
    {
        var type = _buf[Pos++] & 0xff;
        if (type < 251) return type;
        switch (type)
        {
            case 252:
                return ReadUnsignedShort();
            case 253:
                return ReadUnsignedMedium();
            case 254:
                return (int)ReadLong();
            default:
                return type;
        }
    }

    public int SkipIdentifier()
    {
        var len = ReadIntLengthEncodedNotNull();
        Pos += len;
        return Pos;
    }

    public int? ReadLength()
    {
        int type = ReadUnsignedByte();
        switch (type)
        {
            case 251:
                return null;
            case 252:
                return ReadUnsignedShort();
            case 253:
                return ReadUnsignedMedium();
            case 254:
                return (int)ReadLong();
            default:
                return type;
        }
    }

    public byte ReadByte()
    {
        return _buf[Pos++];
    }

    public short ReadUnsignedByte()
    {
        return (short)(_buf[Pos++] & 0xff);
    }

    public short ReadShort()
    {
        return (short)((_buf[Pos++] & 0xff) + (_buf[Pos++] << 8));
    }

    public ushort ReadUnsignedShort()
    {
        return (ushort)(((_buf[Pos++] & 0xff) + (_buf[Pos++] << 8)) & 0xffff);
    }

    public int ReadMedium()
    {
        var value = ReadUnsignedMedium();
        if ((value & 0x800000) != 0) return unchecked((int)(value | 0xff000000));
        return value;
    }

    public int ReadUnsignedMedium()
    {
        return (_buf[Pos++] & 0xff) + ((_buf[Pos++] & 0xff) << 8) + ((_buf[Pos++] & 0xff) << 16);
    }

    public int ReadInt()
    {
        return (_buf[Pos++] & 0xff)
               + ((_buf[Pos++] & 0xff) << 8)
               + ((_buf[Pos++] & 0xff) << 16)
               + ((_buf[Pos++] & 0xff) << 24);
    }

    public int ReadIntBE()
    {
        return ((_buf[Pos++] & 0xff) << 24)
               + ((_buf[Pos++] & 0xff) << 16)
               + ((_buf[Pos++] & 0xff) << 8)
               + (_buf[Pos++] & 0xff);
    }

    public uint ReadUnsignedInt()
    {
        return (uint)(((_buf[Pos++] & 0xff)
                       + ((_buf[Pos++] & 0xff) << 8)
                       + ((_buf[Pos++] & 0xff) << 16)
                       + ((long)(_buf[Pos++] & 0xff) << 24))
                      & 0xffffffffL);
    }

    public long ReadLong()
    {
        return (_buf[Pos++] & 0xffL)
               + ((_buf[Pos++] & 0xffL) << 8)
               + ((_buf[Pos++] & 0xffL) << 16)
               + ((_buf[Pos++] & 0xffL) << 24)
               + ((_buf[Pos++] & 0xffL) << 32)
               + ((_buf[Pos++] & 0xffL) << 40)
               + ((_buf[Pos++] & 0xffL) << 48)
               + ((_buf[Pos++] & 0xffL) << 56);
    }

    public long ReadLongBE()
    {
        return ((_buf[Pos++] & 0xffL) << 56)
               + ((_buf[Pos++] & 0xffL) << 48)
               + ((_buf[Pos++] & 0xffL) << 40)
               + ((_buf[Pos++] & 0xffL) << 32)
               + ((_buf[Pos++] & 0xffL) << 24)
               + ((_buf[Pos++] & 0xffL) << 16)
               + ((_buf[Pos++] & 0xffL) << 8)
               + (_buf[Pos++] & 0xffL);
    }

    public void ReadBytes(byte[] dst)
    {
        Array.Copy(_buf, Pos, dst, 0, dst.Length);
        Pos += dst.Length;
    }

    public byte[] ReadBytesNullEnd()
    {
        var initialPosition = Pos;
        var cnt = 0;
        while (ReadableBytes() > 0 && _buf[Pos++] != 0) cnt++;
        var dst = new byte[cnt];
        Array.Copy(_buf, initialPosition, dst, 0, dst.Length);
        return dst;
    }

    public IReadableByteBuf ReadLengthBuffer()
    {
        var len = ReadIntLengthEncodedNotNull();
        var tmp = new byte[len];
        ReadBytes(tmp);
        return new StandardReadableByteBuf(tmp, len);
    }

    public string ReadString(int length)
    {
        Pos += length;
        return Encoding.UTF8.GetString(_buf, Pos - length, length);
    }

    public string ReadAscii(int length)
    {
        Pos += length;
        return Encoding.ASCII.GetString(_buf, Pos - length, length);
    }

    public string ReadStringNullEnd()
    {
        var initialPosition = Pos;
        var cnt = 0;
        while (ReadableBytes() > 0 && _buf[Pos++] != 0) cnt++;
        return Encoding.UTF8.GetString(_buf, initialPosition, cnt);
    }

    public string ReadStringEof()
    {
        var initialPosition = Pos;
        Pos = _limit;
        return Encoding.UTF8.GetString(_buf, initialPosition, Pos - initialPosition);
    }

    public float ReadFloat()
    {
        var f = BitConverter.ToSingle(_buf, Pos);
        Pos += 4;
        return f;
    }

    public double ReadDouble()
    {
        var d = BitConverter.ToDouble(_buf, Pos);
        Pos += 8;
        return d;
    }

    public byte[]? Buf()
    {
        return _buf;
    }
}