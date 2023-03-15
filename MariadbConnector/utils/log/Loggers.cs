namespace MariadbConnector.utils.log;

public class Loggers
{
    /**
     * factory
     */
    private static readonly ILoggerFactory LOGGER_FACTORY;

    static Loggers()
    {
        LOGGER_FACTORY = new ConsoleLoggerFactory();
    }

    public static Ilogger getLogger(string name)
    {
        return LOGGER_FACTORY.getLogger(name);
    }

    private interface ILoggerFactory
    {
        Ilogger getLogger(string name);
    }

    private class ConsoleLoggerFactory : ILoggerFactory
    {
        public Ilogger getLogger(string name)
        {
            return new ConsoleLogger(name, 2);
        }
    }
}