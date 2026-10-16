package exchange.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;

import exchange.engine.Codes;
import exchange.engine.EngineConfig;
import exchange.engine.MatchingEngine;
import exchange.engine.Scales;
import exchange.engine.Sink;
import org.junit.jupiter.api.Test;

/**
 * The metrics decorator must derive correct business/market numbers purely from the engine's event
 * stream, without the matcher knowing anything about metrics.
 */
class MetricsSinkTest {
    private static final long EUR = Scales.PRICE_SCALE;
    private static final long MW10 = Scales.QTY_SCALE;

    @Test
    void derivesOrderTradeAndVwapMetricsFromTheEventStream() {
        final Metrics metrics = new Metrics(8, 300);
        final MatchingEngine engine = new MatchingEngine(
                EngineConfig.defaults(), new MetricsSink(metrics, Sink.NOOP));
        final long t = 1_000_000_000L;

        // Sell 10 MW rests, buy 4 MW crosses -> one trade of 4 MW at the resting 50.00.
        engine.submit(1, 1, 0, Codes.SELL, Codes.LIMIT, Codes.GTC, 50 * EUR, 10 * MW10, t);
        engine.submit(2, 2, 0, Codes.BUY, Codes.LIMIT, Codes.GTC, 55 * EUR, 4 * MW10, t + 1);
        // One off-tick order must show up as a rejection, not an acceptance.
        engine.submit(3, 3, 0, Codes.BUY, Codes.LIMIT, Codes.GTC, 50 * EUR + 5, 1 * MW10, t + 2);

        assertEquals(2, metrics.counters().get("orders.accepted").sum());
        assertEquals(1, metrics.counters().get("orders.rejected").sum());
        assertEquals(1, metrics.counters().get("orders.rejected.price_not_on_tick").sum());
        assertEquals(1, metrics.counters().get("fills").sum());
        assertEquals(4 * MW10, metrics.counters().get("fill.qty").sum());
        assertEquals(1, metrics.counters().get("trades").sum());
        assertEquals(4 * MW10, metrics.counters().get("trade.qty").sum());
        assertEquals(50 * EUR * 4 * MW10, metrics.counters().get("trade.notional").sum());

        // Status events: the resting sell ends partial, the crossing buy ends filled.
        assertEquals(1, metrics.counters().get("orders.status.accepted").sum());
        assertEquals(1, metrics.counters().get("orders.status.filled").sum());
        assertEquals(1, metrics.counters().get("orders.status.partial").sum());

        final VwapWindow.Window w = metrics.vwap().window(0, t + 3, 300);
        assertEquals(50 * EUR, w.vwapPrice());
        assertEquals(4 * MW10, w.volume());
        assertEquals(1, w.trades());
    }
}
