package exchange.metrics;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

/**
 * A latency distribution recorded on the hot path and read from the management thread.
 *
 * <p>Uses HdrHistogram's {@link Recorder}: {@link #record(long)} writes into an active histogram,
 * and {@link #snapshot()} (called only when an endpoint is scraped) swaps in a fresh histogram and
 * merges the interval into a cumulative one. Recording therefore never blocks on, or allocates for,
 * a scrape, and the snapshot is a full-reservoir distribution accurate to the configured number of
 * significant digits (so p99.9 and p99.99 are meaningful, unlike a fixed-size sampling histogram).
 *
 * <p>Values are nanoseconds. Out-of-range samples are clamped rather than throwing, so a single
 * pathological latency can never take down the matching thread.
 */
public final class Latency {
    private final Recorder recorder;
    private final Histogram cumulative;
    private final long highestTrackable;

    public Latency(long highestTrackableValue, int numberOfSignificantDigits) {
        this.highestTrackable = highestTrackableValue;
        this.recorder = new Recorder(highestTrackableValue, numberOfSignificantDigits);
        this.cumulative = new Histogram(highestTrackableValue, numberOfSignificantDigits);
    }

    /** Record one sample (nanoseconds). Called on the hot path. */
    public void record(long nanos) {
        long v = nanos;
        if (v < 0) {
            v = 0;
        } else if (v > highestTrackable) {
            v = highestTrackable;
        }
        recorder.recordValue(v);
    }

    /** Merge the pending interval into the cumulative distribution and return a read-only view. */
    public synchronized Snapshot snapshot() {
        cumulative.add(recorder.getIntervalHistogram());
        return new Snapshot(
                cumulative.getTotalCount(),
                cumulative.getMean(),
                cumulative.getMinValue(),
                cumulative.getMaxValue(),
                cumulative.getValueAtPercentile(50.0),
                cumulative.getValueAtPercentile(90.0),
                cumulative.getValueAtPercentile(99.0),
                cumulative.getValueAtPercentile(99.9),
                cumulative.getValueAtPercentile(99.99));
    }

    /** Reset the cumulative view (does not affect in-flight recording). */
    public synchronized void reset() {
        recorder.getIntervalHistogram();
        cumulative.reset();
    }

    /** An immutable point-in-time view of the latency distribution, all in nanoseconds. */
    public record Snapshot(long count, double mean, long min, long max,
                           long p50, long p90, long p99, long p999, long p9999) {}
}
