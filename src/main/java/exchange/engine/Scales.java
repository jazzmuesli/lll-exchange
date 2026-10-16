package exchange.engine;

/**
 * Fixed-point conventions used throughout the engine.
 *
 * <p>All money and quantity values are carried as {@code long} fixed-point integers. Never use
 * floating point on the hot path: {@code double} arithmetic is non-deterministic across
 * architectures and cannot be compared bit-for-bit.
 *
 * <ul>
 *   <li>{@link #PRICE_SCALE} = 100 &rarr; price is in units of 0.01 EUR/MWh (cents).</li>
 *   <li>{@link #QTY_SCALE}   = 10  &rarr; quantity is in units of 0.1 MW.</li>
 * </ul>
 */
public final class Scales {
    private Scales() {}

    public static final int PRICE_SCALE = 100;
    public static final int QTY_SCALE = 10;

    /** Format a scaled price as a human string (only for logs / tests). */
    public static String priceToString(long price) {
        final long whole = price / PRICE_SCALE;
        final long frac = Math.abs(price % PRICE_SCALE);
        return whole + "." + (frac < 10 ? "0" : "") + frac;
    }

    /** Format a scaled quantity as a human string (only for logs / tests). */
    public static String qtyToString(long qty) {
        final long whole = qty / QTY_SCALE;
        final long frac = Math.abs(qty % QTY_SCALE);
        return whole + "." + frac;
    }
}
