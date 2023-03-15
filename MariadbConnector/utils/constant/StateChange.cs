namespace MariadbConnector.utils.constant;

public class StateChange
{
    /**
     * system variable change
     */
    public const ushort SESSION_TRACK_SYSTEM_VARIABLES = 0;

    /**
     * schema change
     */
    public const ushort SESSION_TRACK_SCHEMA = 1;

    /**
     * state change
     */
    public const ushort SESSION_TRACK_STATE_CHANGE = 2;

    /**
     * GTID change
     */
    public const ushort SESSION_TRACK_GTIDS = 3;

    /**
     * transaction characteristics change
     */
    public const ushort SESSION_TRACK_TRANSACTION_CHARACTERISTICS = 4;

    /**
     * transaction state change
     */
    public const ushort SESSION_TRACK_TRANSACTION_STATE = 5;
}