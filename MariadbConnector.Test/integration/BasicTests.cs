using System.Collections;
using System.Data;
using System.Text;
using MariadbConnector.Test.integration;

namespace MariadbConnector.Test;

public class Tests : Common
{
    protected static ArrayList chars = new();
    protected static readonly string do1000Cmd;

    static Tests()
    {
        var sb = new StringBuilder("do ?");
        for (var i = 1; i < 1000; i++) sb.Append(",?");
        do1000Cmd = sb.ToString();

        chars.AddRange("123456789abcdefghijklmnop\\Z".ToCharArray());
        chars.Add("😎");
        chars.Add("🌶");
        chars.Add("🎤");
        chars.Add("🥂");
    }


    public static string RandomString(int length)
    {
        var result = new StringBuilder();
        var random = new Random();
        for (var i = length; i > 0; --i)
            result.Append(chars[random.Next(0, chars.Count - 1)]);
        return result.ToString();
    }

    [SetUp]
    public void Setup()
    {
    }

    [Test]
    public void SimpleDo()
    {
        using (var cmd = Db.CreateCommand())
        {
            cmd.CommandText = "DO 1";
            Assert.AreEqual(0, cmd.ExecuteNonQuery());
        }
    }


    [Test]
    public void SimpleSelect()
    {
        for (var loop = 0; loop < 10000; loop++)
            using (var cmd = Db.CreateCommand())
            {
                cmd.CommandText = "select 1";

                using var reader = cmd.ExecuteReader();
                while (reader.Read()) reader.GetInt32(0);

                reader.Close();
            }
    }


    [Test]
    public async Task SimpleSelectMultiFields()
    {
        var total = 0;
        using (var cmd = Db.CreateCommand())
        {
            cmd.CommandText = "select * FROM test100";
            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
                for (var i = 0; i < 100; i++)
                    total += reader.GetInt32(i);

            reader.Close();
        }
    }


    [Test]
    public void SimpleSelect1000Rows()
    {
        var total = 0;
        int loop;
        for (loop = 0; loop < 1; loop++)
            using (var cmd = Db.CreateCommand())
            {
                cmd.CommandText = "select * from 1000rows";

                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        total += reader.GetInt32(0);
                        reader.GetString(1);
                    }
                }
            }
    }


    [Test]
    public void Insert1000Data()
    {
        var total = 0;
        int loop;
        for (loop = 0; loop < 1; loop++)
        {
            var s = RandomString(100);
            using (var cmd = Db.CreateCommand())
            {
                cmd.CommandText = "INSERT INTO perfTestTextBatch(t0) VALUES (?)";
                var param = cmd.CreateParameter();
                param.Value = s;
                param.DbType = DbType.String;
                cmd.Parameters.Add(param);
                for (var i = 0; i < 100; i++) cmd.ExecuteNonQuery();
            }
        }
    }
}