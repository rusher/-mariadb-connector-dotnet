namespace MariadbConnector.utils.log;

public interface Ilogger
{
    string getName();
    bool isTraceEnabled();
    void trace(string msg);
    void trace(string msg, Exception e);
    bool isDebugEnabled();

    void debug(string msg);
    void debug(string msg, Exception e);

    bool isInfoEnabled();
    void info(string msg);
    void info(string msg, Exception e);

    bool isWarnEnabled();
    void warn(string msg);
    void warn(string msg, Exception e);

    bool isErrorEnabled();
    void error(string msg);
    void error(string msg, Exception e);
}