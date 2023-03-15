using System.Text;

namespace MariadbConnector.utils;

public class ClientParser
{
    public int ParamCount;
    public List<int> ParamPositions;
    public byte[] Query;

    public string Sql;

    private ClientParser(string sql, byte[] query, List<int> paramPositions)
    {
        Sql = sql;
        Query = query;
        ParamPositions = paramPositions;
        ParamCount = paramPositions.Count;
    }

    /**
     * Separate query in a String list and set flag isQueryMultipleRewritable. The resulting string
     * list is separed by ? that are not in comments. isQueryMultipleRewritable flag is set if query
     * can be rewrite in one query (all case but if using "-- comment"). example for query : "INSERT
     * INTO tableName(id, name) VALUES (?, ?)" result list will be : {"INSERT INTO tableName(id, name)
     * VALUES (", ", ", ")"}
     * 
     * @param queryString query
     * @param noBackslashEscapes escape mode
     * @return ClientPrepareResult
     */
    public static ClientParser ParameterParts(string queryString, bool noBackslashEscapes)
    {
        var paramPositions = new List<int>();
        var state = LexState.Normal;
        byte lastChar = 0x00;

        var singleQuotes = false;
        var query = Encoding.UTF8.GetBytes(queryString);
        var queryLength = query.Length;
        for (var i = 0; i < queryLength; i++)
        {
            var car = query[i];
            if (state == LexState.Escape
                && !((car == '\'' && singleQuotes) || (car == '"' && !singleQuotes)))
            {
                state = LexState.String;
                lastChar = car;
                continue;
            }

            switch (car)
            {
                case (byte)'*':
                    if (state == LexState.Normal && lastChar == (byte)'/') state = LexState.SlashStarComment;
                    break;

                case (byte)'/':
                    if (state == LexState.SlashStarComment && lastChar == (byte)'*')
                        state = LexState.Normal;
                    else if (state == LexState.Normal && lastChar == (byte)'/') state = LexState.EOLComment;
                    break;

                case (byte)'#':
                    if (state == LexState.Normal) state = LexState.EOLComment;
                    break;

                case (byte)'-':
                    if (state == LexState.Normal && lastChar == (byte)'-') state = LexState.EOLComment;
                    break;

                case (byte)'\n':
                    if (state == LexState.EOLComment) state = LexState.Normal;
                    break;

                case (byte)'"':
                    if (state == LexState.Normal)
                    {
                        state = LexState.String;
                        singleQuotes = false;
                    }
                    else if (state == LexState.String && !singleQuotes)
                    {
                        state = LexState.Normal;
                    }
                    else if (state == LexState.Escape)
                    {
                        state = LexState.String;
                    }

                    break;

                case (byte)'\'':
                    if (state == LexState.Normal)
                    {
                        state = LexState.String;
                        singleQuotes = true;
                    }
                    else if (state == LexState.String && singleQuotes)
                    {
                        state = LexState.Normal;
                    }
                    else if (state == LexState.Escape)
                    {
                        state = LexState.String;
                    }

                    break;

                case (byte)'\\':
                    if (noBackslashEscapes) break;
                    if (state == LexState.String) state = LexState.Escape;
                    break;
                case (byte)'?':
                    if (state == LexState.Normal) paramPositions.Add(i);
                    break;
                case (byte)'`':
                    if (state == LexState.Backtick)
                        state = LexState.Normal;
                    else if (state == LexState.Normal) state = LexState.Backtick;
                    break;
            }

            lastChar = car;
        }

        return new ClientParser(queryString, query, paramPositions);
    }

    private enum LexState
    {
        Normal, /* inside  query */
        String, /* inside string */
        SlashStarComment, /* inside slash-star comment */
        Escape, /* found backslash */
        EOLComment, /* # comment, or // comment, or -- comment */
        Backtick /* found backtick */
    }
}