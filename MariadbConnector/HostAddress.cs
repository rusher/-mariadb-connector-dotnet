namespace MariadbConnector;

public class HostAddress
{
    public string Host;
    public uint Port;
    public bool? Primary;

    private HostAddress(string host, uint port, bool? primary)
    {
        Host = host;
        Port = port;
        Primary = primary;
    }

    public static HostAddress From(string host, uint port)
    {
        return new HostAddress(host, port, null);
    }

    public static HostAddress From(string host, uint port, bool? primary)
    {
        return new HostAddress(host, port, primary);
    }

    public static List<HostAddress> Parse(string spec, HaMode haMode)
    {
        if ("".Equals(spec)) return new List<HostAddress>();
        var tokens = spec.Trim().Split(",");
        var size = tokens.Length;
        var arr = new List<HostAddress>(size);

        for (var i = 0; i < tokens.Length; i++)
        {
            var token = tokens[i];
            if (token.StartsWith("address="))
                arr.Add(ParseParameterHostAddress(token, haMode, i == 0));
            else
                arr.Add(ParseSimpleHostAddress(token, haMode, i == 0));
        }

        return arr;
    }

    private static HostAddress ParseSimpleHostAddress(string str, HaMode haMode, bool first)
    {
        string host;
        uint port = 3306;

        if (str[0] == '[')
        {
            /* IPv6 addresses in URLs are enclosed in square brackets */
            var ind = str.IndexOf(']');
            host = str.Substring(1, ind);
            if (ind != str.Length - 1 && str[ind + 1] == ':') port = GetPort(str.Substring(ind + 2));
        }
        else if (str.Contains(":"))
        {
            /* Parse host:port */
            var hostPort = str.Split(":");
            host = hostPort[0];
            port = GetPort(hostPort[1]);
        }
        else
        {
            /* Just host name is given */
            host = str;
        }

        var primary = haMode != HaMode.REPLICATION || first;

        return new HostAddress(host, port, primary);
    }

    private static uint GetPort(string portString)
    {
        if (uint.TryParse(portString, out var port)) return port;
        throw new ArgumentException($"Port has wrong int32 value '{portString}'.");
    }

    private static HostAddress ParseParameterHostAddress(string str, HaMode haMode, bool first)
    {
        string host = null;
        uint port = 3306;
        bool? primary = null;

        var array = str.Replace(" ", "").Split("(?=\\()|(?<=\\))");
        for (var i = 1; i < array.Length; i++)
        {
            var token = array[i].Replace("(", "").Replace(")", "").Trim().Split("=");
            if (token.Length != 2)
                throw new ArgumentException(
                    $"Invalid connection URL, expected key=value pairs, found '{array[i]}'.");
            var key = token[0].ToLowerInvariant();
            var value = token[1].ToLowerInvariant();

            switch (key)
            {
                case "host":
                    host = value.Replace("[", "").Replace("]", "");
                    break;
                case "port":
                    port = GetPort(value);
                    break;
                case "type":
                    if ("master".Equals(value.ToLowerInvariant()) || "primary".Equals(value.ToLowerInvariant()))
                        primary = true;
                    else if ("slave".Equals(value.ToLowerInvariant()) || "replica".Equals(value.ToLowerInvariant()))
                        primary = false;
                    else
                        throw new ArgumentException(
                            $"Invalid type value '{array[i]}' (possible value primary/replica).");
                    break;
            }
        }

        if (primary == null)
        {
            if (haMode == HaMode.REPLICATION)
                primary = first;
            else
                primary = true;
        }

        return new HostAddress(host, port, primary);
    }

    public override string ToString()
    {
        return string.Format(
            "address=(host={0})(port={1}){2}",
            Host, Port, Primary != null ? "(type=" + (Primary == true ? "primary)" : "replica)") : "");
    }
}