using MariadbConnector.plugin.authentication.standard;
using MariadbConnector.utils.exception;

namespace MariadbConnector.plugin.authentication;

public class Authentications
{
    private static readonly Dictionary<string, Func<NativePasswordPlugin>> _plugins = new();

    static Authentications()
    {
        _plugins.Add("mysql_native_password", () => new NativePasswordPlugin());
    }


    public static IAuthenticationPlugin get(string type, Configuration conf)
    {
        var authList = conf.RestrictedAuth != null ? conf.RestrictedAuth.Split(",") : null;
        var pluginFct = _plugins[type];
        if (conf.RestrictedAuth == null || authList.Any(type.Contains))
            return pluginFct.Invoke();
        throw new SqlException(
            $"Client restrict authentication plugin to a limited set of authentication plugin and doesn't permit requested plugin ('{type}'). Current list is `restrictedAuth={conf.RestrictedAuth}`",
            1251, "08004");
    }
}