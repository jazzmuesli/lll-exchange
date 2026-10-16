package exchange.engine;

/**
 * Static geometry of the engine. Immutable after start-up so the matching thread never has to
 * re-read it from shared memory.
 *
 * <p>All prices are fixed-point integers in units of {@link Scales#PRICE_SCALE} (0.01 EUR/MWh).
 * The price ladder is dense over {@code [minPrice, maxPrice]} with step {@code tickSize}; a
 * dense ladder lets us map a price to a tick in one division and index the level arrays directly.
 *
 * @param maxProducts maximum number of distinct contracts (products) addressable
 * @param maxOrders   maximum number of live orders (table size; ids are recycled)
 * @param minPrice    lowest addressable price, inclusive (scaled)
 * @param maxPrice    highest addressable price, inclusive (scaled)
 * @param tickSize    price granularity (scaled); must be &gt; 0
 */
public record EngineConfig(
        int maxProducts,
        int maxOrders,
        long minPrice,
        long maxPrice,
        long tickSize) {

    public EngineConfig {
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
    }

    /** Number of distinct ticks in the ladder. */
    public int numTicks() {
        return (int) ((maxPrice - minPrice) / tickSize + 1);
    }

    /** Map a (scaled) price to its tick index, or -1 if off-ladder or off-tick. */
    public int tickFor(long price) {
        if (price < minPrice || price > maxPrice) {
            return -1;
        }
        final long d = price - minPrice;
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
