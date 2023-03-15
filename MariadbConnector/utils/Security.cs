using System.Text;

namespace MariadbConnector.utils;

public class Security
{
    /**
     * Parse the option "sessionVariable" to ensure having no injection. semi-column not in string
     * will be replaced by comma.
     * 
     * @param sessionVariable option value
     * @return parsed String
     */
    public static string ParseSessionVariables(string sessionVariable)
    {
        var _out = new StringBuilder();
        var sb = new StringBuilder();
        var state = Parse.Normal;
        var iskey = true;
        var singleQuotes = true;
        var first = true;
        string key = null;

        var chars = sessionVariable.ToCharArray();

        foreach (var car in chars)
        {
            if (state == Parse.Escape)
            {
                sb.Append(car);
                state = Parse.String;
                continue;
            }

            switch (car)
            {
                case '"':
                    if (state == Parse.Normal)
                    {
                        state = Parse.String;
                        singleQuotes = false;
                    }
                    else if (!singleQuotes)
                    {
                        state = Parse.Normal;
                    }

                    break;

                case '\'':
                    if (state == Parse.Normal)
                    {
                        state = Parse.String;
                        singleQuotes = true;
                    }
                    else if (singleQuotes)
                    {
                        state = Parse.Normal;
                    }

                    break;

                case '\\':
                    if (state == Parse.String) state = Parse.Escape;
                    break;

                case ';':
                case ',':
                    if (state == Parse.Normal)
                    {
                        if (!iskey)
                        {
                            if (!first) _out.Append(",");
                            _out.Append(key);
                            _out.Append(sb);
                            first = false;
                        }
                        else
                        {
                            key = sb.ToString().Trim();
                            if (!string.IsNullOrEmpty(key))
                            {
                                if (!first) _out.Append(",");
                                _out.Append(key);
                                first = false;
                            }
                        }

                        iskey = true;
                        key = null;
                        sb = new StringBuilder();
                        continue;
                    }

                    break;

                case '=':
                    if (state == Parse.Normal && iskey)
                    {
                        key = sb.ToString().Trim();
                        iskey = false;
                        sb = new StringBuilder();
                    }

                    break;
            }

            sb.Append(car);
        }

        if (!iskey)
        {
            if (!first) _out.Append(",");
            _out.Append(key);
            _out.Append(sb);
        }
        else
        {
            var tmpkey = sb.ToString().Trim();
            if (!string.IsNullOrEmpty(tmpkey) && !first) _out.Append(",");
            _out.Append(tmpkey);
        }

        return _out.ToString();
    }

    private enum Parse
    {
        Normal,
        String, /* inside string */
        Escape /* found backslash */
    }
}