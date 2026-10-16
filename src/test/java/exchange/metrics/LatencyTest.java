package exchange.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * {@link Latency} must give honest percentiles (not a fixed-size sample) and must clamp rather than
 * throw when a sample is out of range, because it is called on the matching thread.
 */
class LatencyTest {

    @Test
    void percentilesTrackRecordedDistribution() {
        final Latency latency = new Latency(1_000_000, 3);
        for (int i = 1; i <= 1000; i++) {
            latency.record(i * 100L); // 100 ns .. 100_000 ns, uniform
        }

        final Latency.Snapshot s = latency.snapshot();
        assertEquals(1000, s.count());
        assertTrue(s.p50() >= 40_000 && s.p50() <= 60_000, "p50=" + s.p50());
        assertTrue(s.p99() >= 90_000 && s.p99() <= 110_000, "p99=" + s.p99());
        assertTrue(s.p50() <= s.p99());
        assertTrue(s.p99() <= s.p999());
        assertTrue(s.p999() <= s.max());
        assertTrue(s.max() >= 100_000);
    }

    @Test
    void outOfRangeSamplesAreClampedNotFatal() {
        final Latency latency = new Latency(1_000_000, 3);
        latency.record(-5);
        latency.record(999_999_999L);
        final Latency.Snapshot s = latency.snapshot();
        assertEquals(2, s.count());
        // Clamped to the highest trackable value (HdrHistogram reports that bucket's upper edge).
        assertTrue(s.max() >= 999_000 && s.max() <= 1_001_000, "max=" + s.max());
    }
}
