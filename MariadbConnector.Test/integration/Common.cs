namespace MariadbConnector.Test.integration;

public class Common
{
    public MariaDbConnection Db { get; internal set; }
    public string DefaultConnString { get; internal set; }

    [OneTimeSetUp]
    public void SetupDb()
    {
        var conf = new Conf();
        conf.Server = ReadEnv("DB_HOST", "localhost");
        conf.User = ReadEnv("DB_USER", "root");
        conf.Password = ReadEnv("DB_USER", null);
        conf.Port = int.Parse(ReadEnv("DB_PORT", "3306"));
        conf.Database = ReadEnv("DB_DATABASE", "bench");

        DefaultConnString =
            $"Server={conf.Server};User ID={conf.User};Password={conf.Password};Database={conf.Database};Port={conf.Port};";
        Db = new MariaDbConnection(DefaultConnString);
        Db.Open();
    }

    public void Dispose()
    {
        Db.Dispose();
    }

    private string ReadEnv(string key, string defaultValue)
    {
        var envValue = Environment.GetEnvironmentVariable("TEST_" + key);
        return envValue == null ? defaultValue : envValue;
    }

    public class Conf
    {
        public string Database;
        public string Password;
        public int Port;
        public string Server;
        public string User;
    }
}