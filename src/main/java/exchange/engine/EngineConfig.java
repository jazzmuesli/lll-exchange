package exchange.engine;

/**
 * Static geometry of the engine. Immutable after start-up so the matching thread never has to
 * re-read it from shared memory.
 *
 * <p>All prices are fixed-point integers in units of {@link Scales#PRICE_SCALE} (0.01 EUR/MWh).
 * The price ladder is dense over {@code [minPrice, maxPrice]} with step {@code tickSize}; a dense
 * ladder maps a price to a tick in one step and indexes the level arrays directly. To keep that
 * mapping cheap on the hot path, the constructor precomputes an exact multiply-shift "divide by
 * {@code tickSize}" (see {@link #tickFor}), so no hardware {@code idiv} is executed per order. That
 * cached state is why this is a plain final class rather than a record.
 */
public final class EngineConfig {

    private final int maxProducts;
    private final int maxOrders;
    private final long minPrice;
    private final long maxPrice;
    private final long tickSize;

    /**
     * Magic multiplier {@code ceil(2^{@link #tickShift} / tickSize)} used by {@link #tickFor}.
     * A value {@code <= 0} means the precomputation did not apply and the hardware division
     * fallback is used instead.
     */
    private final long tickMagic;

    /** Shift paired with {@link #tickMagic}. */
    private final int tickShift;

    /**
     * @param maxProducts maximum number of distinct contracts (products) addressable
     * @param maxOrders   maximum number of live orders (table size; ids are recycled)
     * @param minPrice    lowest addressable price, inclusive (scaled)
     * @param maxPrice    highest addressable price, inclusive (scaled)
     * @param tickSize    price granularity (scaled); must be &gt; 0
     */
    public EngineConfig(int maxProducts, int maxOrders, long minPrice, long maxPrice, long tickSize) {
        if (maxProducts <= 0 || maxOrders <= 0) {
            throw new IllegalArgumentException("maxProducts/maxOrders must be positive");
        }
        if (tickSize <= 0) {
            throw new IllegalArgumentException("tickSize must be > 0");
        }
        if (maxPrice < minPrice) {
            throw new IllegalArgumentException("maxPrice must be >= minPrice");
        }
        final long span = (maxPrice - minPrice) / tickSize;
        if (span + 1 > Integer.MAX_VALUE - 8) {
            throw new IllegalArgumentException("price ladder too large: " + (span + 1) + " ticks");
        }

        // Precompute an exact "divide by tickSize" as a multiply + shift, avoiding the ~20-40 cycle
        // hardware idiv that `d / tickSize` would emit (tickSize is a runtime value, so HotSpot
        // cannot strength-reduce it on its own). For 0 <= n and
        //     m = ceil(2^s / tickSize),   e = m - 2^s / tickSize   (so 0 <= e < 1),
        // we get floor(n*m / 2^s) = floor(n/tickSize + n*e/2^s). The extra term is < 1/tickSize
        // exactly when maxDelta < 2^s / tickSize, which makes the result exact over the whole
        // ladder. We also require n*m to fit in a signed long. If no shift qualifies (e.g. an
        // enormous ladder or tick), we leave tickMagic <= 0 and fall back to division.
        final long maxDelta = maxPrice - minPrice;
        long magic = -1;
        int shift = 0;
        for (int s = 32; s < 63; s++) {
            final long pow = 1L << s;
            final long m = (pow + tickSize - 1) / tickSize; // ceil(2^s / tickSize)
            if (m <= 0 || m > Long.MAX_VALUE / Math.max(1L, maxDelta)) {
                continue; // n * m could overflow for some n in [0, maxDelta]
            }
            if (maxDelta < pow / tickSize) { // guarantees exactness for every n in [0, maxDelta]
                magic = m;
                shift = s;
                break;
            }
        }
        this.tickMagic = magic;
        this.tickShift = shift;

        this.maxProducts = maxProducts;
        this.maxOrders = maxOrders;
        this.minPrice = minPrice;
        this.maxPrice = maxPrice;
        this.tickSize = tickSize;
    }

    public int maxProducts() {
        return maxProducts;
    }

    public int maxOrders() {
        return maxOrders;
    }

    public long minPrice() {
        return minPrice;
    }

    public long maxPrice() {
        return maxPrice;
    }

    public long tickSize() {
        return tickSize;
    }

    /** Number of distinct ticks in the ladder. */
    public int numTicks() {
        return (int) ((maxPrice - minPrice) / tickSize + 1);
    }

    /**
     * Map a (scaled) price to its tick index, or -1 if off-ladder or off-tick.
     *
     * <p>Hot path (once per incoming limit order): uses the precomputed multiply-shift when
     * available and confirms divisibility with a single multiply, so no division is executed.
     */
    public int tickFor(long price) {
        if (price < minPrice || price > maxPrice) {
            return -1;
        }
        final long d = price - minPrice;
        final long magic = tickMagic;
        if (magic > 0) {
            final int tick = (int) ((d * magic) >>> tickShift);
            return (long) tick * tickSize == d ? tick : -1;
        }
        if (d % tickSize != 0) {
            return -1;
        }
        return (int) (d / tickSize);
    }

    public long priceForTick(int tick) {
        return minPrice + (long) tick * tickSize;
    }

    /**
     * Sensible defaults for a European intraday power contract:
     * price range -1000.00 .. +10000.00 EUR/MWh, tick 0.10 EUR/MWh, up to 1024 products and
     * ~1M live orders.
     */
    public static EngineConfig defaults() {
        return new EngineConfig(
                1024,
                1 << 20,
                -1000L * Scales.PRICE_SCALE,
                10000L * Scales.PRICE_SCALE,
                10L); // 0.10 EUR/MWh
    }
}
