namespace MariadbConnector.utils.log;

public class ConsoleLogger : Ilogger
{
    private static readonly TextWriter _err = Console.Error;
    private readonly int _logLvl;

    private readonly string _name;

    public ConsoleLogger(string name, int logLvl)
    {
        _name = name;
        _logLvl = logLvl;
    }

    public string getName()
    {
        return _name;
    }

    public bool isTraceEnabled()
    {
        return _logLvl == 0;
    }

    public void trace(string msg)
    {
        Console.Write(msg);
    }

    public void trace(string msg, Exception e)
    {
        Console.Write(msg, e);
    }

    public bool isDebugEnabled()
    {
        return _logLvl == 1;
    }

    public void debug(string msg)
    {
        Console.Write(msg);
    }

    public void debug(string msg, Exception e)
    {
        Console.Write(msg, e);
    }

    public bool isInfoEnabled()
    {
        return _logLvl == 2;
    }

    public void info(string msg)
    {
        Console.Write(msg);
    }

    public void info(string msg, Exception e)
    {
        Console.Write(msg, e);
    }

    public bool isWarnEnabled()
    {
        return _logLvl == 3;
    }

    public void warn(string msg)
    {
        Console.Write(msg);
    }

    public void warn(string msg, Exception e)
    {
        Console.Write(msg, e);
    }

    public bool isErrorEnabled()
    {
        return _logLvl == 4;
    }

    public void error(string msg)
    {
        _err.Write(msg);
    }

    public void error(string msg, Exception e)
    {
        _err.Write(msg, e);
    }
}