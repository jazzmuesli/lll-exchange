package exchange.engine;

/**
 * Integer status / reason codes. Kept as {@code int} constants (not enums) so they can travel
 * through lock-free ring buffers without allocation and be switched on cheaply.
 */
public final class Codes {
    private Codes() {}

    // ---- Sides ------------------------------------------------------------
    public static final byte BUY = 1;
    public static final byte SELL = 2;

    // ---- Order types ------------------------------------------------------
    public static final byte LIMIT = 1;
    public static final byte MARKET = 2;

    // ---- Time in force ----------------------------------------------------
    public static final byte GTC = 1;
    public static final byte IOC = 2;
    public static final byte FOK = 3;

    // ---- Order status (mirrors exchange.v1.OrdStatus) ---------------------
    public static final byte ST_ACCEPTED = 1;
    public static final byte ST_REJECTED = 2;
    public static final byte ST_PARTIAL = 3;
    public static final byte ST_FILLED = 4;
    public static final byte ST_CANCELLED = 5;
    public static final byte ST_EXPIRED = 6;

    // ---- Reject reason codes ---------------------------------------------
    public static final int REJ_NONE = 0;
    public static final int REJ_UNKNOWN_PRODUCT = 1;
    public static final int REJ_BAD_PRICE = 2;
    public static final int REJ_BAD_QUANTITY = 3;
    public static final int REJ_BAD_SIDE = 4;
    public static final int REJ_BAD_TYPE = 5;
    public static final int REJ_BAD_TIF = 6;
    public static final int REJ_PRICE_OUT_OF_RANGE = 7;
    public static final int REJ_PRICE_NOT_ON_TICK = 8;
    public static final int REJ_ORDER_CAPACITY = 9;
    public static final int REJ_UNKNOWN_ORDER = 10;

    private static final String[] REASONS = {
            "ok",
            "unknown product",
            "bad price",
            "bad quantity",
            "bad side",
            "bad order type",
            "bad time-in-force",
            "price out of range",
            "price not on tick",
            "order id capacity exhausted",
            "unknown order",
    };

    /** Reason text for a code. Returns a precomputed constant (never allocates) for known codes. */
    public static String reasonText(int code) {
        if (code >= 0 && code < REASONS.length) {
            return REASONS[code];
        }
        return "reason(" + code + ")";
    }
}
