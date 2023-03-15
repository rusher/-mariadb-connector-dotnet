using MariadbConnector.client;

namespace MariadbConnector.utils;

public class Version : IServerVersion
{
    public Version(string versionString)
    {
        VersionString = versionString;
        var major = 0;
        var minor = 0;
        var patch = 0;
        var qualif = "";

        var length = VersionString.Length;
        char car;
        var offset = 0;
        var type = 0;
        var val = 0;
        for (; offset < length; offset++)
        {
            car = VersionString[offset];
            if (car < '0' || car > '9')
            {
                switch (type)
                {
                    case 0:
                        major = val;
                        break;
                    case 1:
                        minor = val;
                        break;
                    case 2:
                        patch = val;
                        qualif = VersionString.Substring(offset);
                        offset = length;
                        break;
                }

                type++;
                val = 0;
            }
            else
            {
                val = val * 10 + car - 48;
            }
        }

        if (type == 2) patch = val;
        MajorVersion = major;
        MinorVersion = minor;
        PatchVersion = patch;
        Qualifier = qualif;
    }

    public string VersionString { get; }
    public int MajorVersion { get; }
    public int MinorVersion { get; }
    public int PatchVersion { get; }
    public string Qualifier { get; }

    /**
     * Utility method to check if database version is greater than parameters.
     * 
     * @param major major version
     * @param minor minor version
     * @param patch patch version
     * @return true if version is greater than parameters
     */
    public bool VersionGreaterOrEqual(int major, int minor, int patch)
    {
        if (MajorVersion > major) return true;

        if (MajorVersion < major) return false;

        /*
         * Major versions are equal, compare minor versions
         */
        if (MinorVersion > minor) return true;
        if (MinorVersion < minor) return false;

        // Minor versions are equal, compare patch version.
        return PatchVersion >= patch;
    }
}