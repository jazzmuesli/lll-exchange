package exchange.wire;

/**
 * Message type ids used on the internal Agrona ring buffers (not the wire protocol).
 * Small ints keep the ring-buffer header compact.
 */
public final class MsgType {
    private MsgType() {}

    // gateway -> engine
    public static final int NEW_ORDER = 1;
    public static final int CANCEL = 2;

    // engine -> gateway
    public static final int ACK = 101;
    public static final int FILL = 102;
    public static final int STATUS = 103;
    public static final int BOOK = 104;
}
