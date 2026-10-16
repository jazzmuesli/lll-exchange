package exchange.metrics;

import java.util.concurrent.atomic.AtomicLongArray;

/**
 * A rolling event-rate meter: counts events into per-second buckets and answers "how many in the
 * last N seconds?" / "per second?" over any window up to its capacity.
 *
 * <p>The bucket is chosen by {@code nanoTime()/1e9}, so the meter needs no scheduler: older buckets
 * are simply overwritten, and a bucket whose stored second is outside the requested window is
 * ignored when summing. Reads are approximate under concurrency (they may catch a bucket mid-reset),
 * which is fine for observability and keeps writers CAS-only.
 */
public final class RateMeter {
    private final int buckets;
    private final AtomicLongArray counts;
    private final AtomicLongArray seconds;

    /** @param windowSeconds number of 1-second buckets to retain (also the max queryable window) */
    public RateMeter(int windowSeconds) {
        this.buckets = Math.max(1, windowSeconds);
        this.counts = new AtomicLongArray(buckets);
        this.seconds = new AtomicLongArray(buckets);
    }

    public int windowSeconds() {
        return buckets;
    }

    /** Record one event (hot path). */
    public void record() {
        record(1);
    }

    public void record(long amount) {
        final long sec = System.nanoTime() / 1_000_000_000L;
        final int b = (int) Math.floorMod(sec, buckets);
        if (seconds.get(b) != sec) {
            seconds.set(b, sec);
            counts.set(b, 0);
        }
        counts.addAndGet(b, amount);
    }

    /** Total events recorded in the last {@code windowSeconds} seconds (clamped to capacity). */
    public long sumOver(long nowNs, int windowSeconds) {
        final int window = Math.min(Math.max(1, windowSeconds), buckets);
        final long nowSec = nowNs / 1_000_000_000L;
        final long oldest = nowSec - window + 1;
        long total = 0;
        for (int i = 0; i < buckets; i++) {
            if (seconds.get(i) >= oldest) {
                total += counts.get(i);
            }
        }
        return total;
    }

    /** Events per second over the last {@code windowSeconds} seconds. */
    public double ratePerSecond(long nowNs, int windowSeconds) {
        final int window = Math.min(Math.max(1, windowSeconds), buckets);
        return sumOver(nowNs, window) / (double) window;
    }
}
