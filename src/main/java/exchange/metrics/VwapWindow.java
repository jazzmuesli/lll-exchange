package exchange.metrics;

/**
 * Rolling VWAP (volume-weighted average price) and high/low over a per-product trailing window.
 *
 * <p>Design: one bucket per second, {@code windowSeconds} buckets in a ring. Each bucket keeps the
 * sum of {@code price*qty}, the sum of {@code qty}, and the second it belongs to. A trade lands in
 * the bucket for its second (clearing a stale bucket first); a query sums the buckets whose second
 * is still inside the window. So the hot path is a handful of array writes, and queries are O(window)
 * and only happen when an endpoint is scraped.
 *
 * <p>Per-product state is allocated lazily on the first trade for that product, mirroring how the
 * order books are allocated, so an exchange with few active contracts does not pay for hundreds.
 *
 * <p>All values are engine-scaled longs: price in 0.01 EUR/MWh, quantity in 0.1 MW. The weighted
 * average price is therefore {@code notional / volume} in the same price scale.
 *
 * <p>Not thread-safe by design: written by the matching thread, read (tolerantly, best-effort) by
 * the management thread. Reads of a value that is being updated may be slightly stale, never torn
 * in a way that breaks the arithmetic.
 */
public final class VwapWindow {
    private final int windowSeconds;
    private final ProductWindow[] products;

    public VwapWindow(int maxProducts, int windowSeconds) {
        this.windowSeconds = Math.max(1, windowSeconds);
        this.products = new ProductWindow[Math.max(1, maxProducts)];
    }

    public int windowSeconds() {
        return windowSeconds;
    }

    /** Record a trade. Called on the matching thread for every public tape print. */
    public void onTrade(int productId, long priceScaled, long qtyScaled, long tsNs) {
        if (productId < 0 || productId >= products.length) {
            return;
        }
        ProductWindow p = products[productId];
        if (p == null) {
            p = new ProductWindow(windowSeconds);
            products[productId] = p;
        }
        final long sec = tsNs / 1_000_000_000L;
        final int b = (int) Math.floorMod(sec, windowSeconds);
        if (p.bucketSecond[b] != sec) {
            p.bucketSecond[b] = sec;
            p.bucketNotional[b] = 0;
            p.bucketVolume[b] = 0;
            p.bucketTrades[b] = 0;
        }
        final long notional = priceScaled * qtyScaled;
        p.bucketNotional[b] += notional;
        p.bucketVolume[b] += qtyScaled;
        p.bucketTrades[b]++;
        p.totalNotional += notional;
        p.totalVolume += qtyScaled;
        p.totalTrades++;
        p.lastPrice = priceScaled;
        if (priceScaled > p.high) {
            p.high = priceScaled;
        }
        if (p.low == Long.MAX_VALUE || priceScaled < p.low) {
            p.low = priceScaled;
        }
    }

    /** Trailing-window VWAP for a product. {@code window} is clamped to the configured window. */
    public Window window(int productId, long nowNs, int window) {
        final ProductWindow p = (productId >= 0 && productId < products.length) ? products[productId] : null;
        if (p == null) {
            return Window.EMPTY;
        }
        final int w = Math.min(Math.max(1, window), windowSeconds);
        final long nowSec = nowNs / 1_000_000_000L;
        final long oldest = nowSec - w + 1;
        long volume = 0;
        long notional = 0;
        long trades = 0;
        for (int i = 0; i < windowSeconds; i++) {
            if (p.bucketSecond[i] >= oldest) {
                volume += p.bucketVolume[i];
                notional += p.bucketNotional[i];
                trades += p.bucketTrades[i];
            }
        }
        final long vwap = volume > 0 ? notional / volume : 0;
        return new Window(productId, w, volume, notional, trades, p.lastPrice, p.high == Long.MIN_VALUE ? 0 : p.high,
                p.low == Long.MAX_VALUE ? 0 : p.low, vwap);
    }

    /** All-time totals for a product (since process start), regardless of the rolling window. */
    public Totals totals(int productId) {
        final ProductWindow p = (productId >= 0 && productId < products.length) ? products[productId] : null;
        if (p == null) {
            return new Totals(productId, 0, 0, 0, 0, 0, 0, 0);
        }
        final long vwap = p.totalVolume > 0 ? p.totalNotional / p.totalVolume : 0;
        return new Totals(productId, p.totalTrades, p.totalVolume, p.totalNotional, p.lastPrice,
                p.high == Long.MIN_VALUE ? 0 : p.high, p.low == Long.MAX_VALUE ? 0 : p.low, vwap);
    }

    public int maxProducts() {
        return products.length;
    }

    /** A trailing-window view for one product. Prices/quantities are engine-scaled longs. */
    public record Window(int productId, int windowSeconds, long volume, long notional, long trades,
                         long lastPrice, long high, long low, long vwapPrice) {
        static final Window EMPTY = new Window(-1, 0, 0, 0, 0, 0, 0, 0, 0);
    }

    /** All-time (process lifetime) view for one product. */
    public record Totals(int productId, long trades, long volume, long notional, long lastPrice,
                         long high, long low, long vwapPrice) {}

    private static final class ProductWindow {
        final long[] bucketNotional;
        final long[] bucketVolume;
        final long[] bucketTrades;
        final long[] bucketSecond;
        long totalNotional;
        long totalVolume;
        long totalTrades;
        long lastPrice;
        long high = Long.MIN_VALUE;
        long low = Long.MAX_VALUE;

        ProductWindow(int buckets) {
            this.bucketNotional = new long[buckets];
            this.bucketVolume = new long[buckets];
            this.bucketTrades = new long[buckets];
            this.bucketSecond = new long[buckets];
        }
    }
}
