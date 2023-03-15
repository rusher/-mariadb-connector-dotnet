namespace MariadbConnector.client.util;

public interface IReadableByteBuf
{
    int ReadableBytes();
    void Buf(byte[] buf, int limit, int pos);
    void SetPos(int pos);
    void Skip();
    void Skip(int length);
    void SkipLengthEncoded();
    byte GetByte();
    byte GetByte(int index);
    short GetUnsignedByte();
    long ReadLongLengthEncodedNotNull();
    int ReadIntLengthEncodedNotNull();
    int SkipIdentifier();
    long Atoll(int length);
    long Atoull(int length);
    int? ReadLength();
    byte ReadByte();
    short ReadUnsignedByte();
    short ReadShort();
    ushort ReadUnsignedShort();
    int ReadMedium();
    int ReadUnsignedMedium();
    int ReadInt();
    int ReadIntBE();
    uint ReadUnsignedInt();
    long ReadLong();
    long ReadLongBE();
    void ReadBytes(byte[] dst);
    byte[] ReadBytesNullEnd();
    IReadableByteBuf ReadLengthBuffer();
    string ReadString(int length);
    string ReadAscii(int length);
    string ReadStringNullEnd();
    string ReadStringEof();
    float ReadFloat();
    double ReadDouble();
}