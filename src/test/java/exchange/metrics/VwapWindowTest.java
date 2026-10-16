package exchange.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/**
 * The rolling VWAP/high-low window is the number an operator watches for "what did we trade at in
 * the last few minutes?". These tests pin the arithmetic (volume weighting, high/low, last) and the
 * eviction behaviour that makes it a genuine trailing window rather than an all-time average.
 */
class VwapWindowTest {
    private static final long EUR = 100;  // price scale: 0.01 EUR/MWh
    private static final long MW10 = 10;  // quantity scale: 0.1 MW

    @Test
    void weightedAverageOverWindow() {
        final VwapWindow window = new VwapWindow(4, 300);
        final long t0 = 1_000_000_000_000L;

        window.onTrade(1, 50 * EUR, 10 * MW10, t0);                    // 50.00 x 10 MW
        window.onTrade(1, 60 * EUR, 30 * MW10, t0 + 1_000_000_000L);   // 60.00 x 30 MW

        final VwapWindow.Window w = window.window(1, t0 + 2_000_000_000L, 300);
        // (50*10 + 60*30) / 40 = 57.5 EUR/MWh -> 5750 in scaled units.
        assertEquals(5750, w.vwapPrice());
        assertEquals(40 * MW10, w.volume());
        assertEquals(2, w.trades());
        assertEquals(60 * EUR, w.high());
        assertEquals(50 * EUR, w.low());
        assertEquals(60 * EUR, w.lastPrice());
    }

    @Test
    void tradesFallOutOfTheWindowButStayInTotals() {
        final VwapWindow window = new VwapWindow(2, 10); // 10-second window
        final long t0 = 5_000_000_000_000L;
        window.onTrade(0, 50 * EUR, 10 * MW10, t0);

        // Query exactly 10 seconds later: the only bucket is now outside the trailing window.
        final VwapWindow.Window w = window.window(0, t0 + 10_000_000_000L, 10);
        assertEquals(0, w.volume());
        assertEquals(0, w.vwapPrice());

        // All-time totals still remember it.
        assertEquals(10 * MW10, window.totals(0).volume());
        assertEquals(50 * EUR, window.totals(0).lastPrice());
        assertEquals(1, window.totals(0).trades());
    }

    @Test
    void unknownProductIsEmpty() {
        final VwapWindow window = new VwapWindow(4, 300);
        assertEquals(0, window.window(2, 12_345L, 300).volume());
        assertEquals(0, window.totals(2).trades());
    }
}
