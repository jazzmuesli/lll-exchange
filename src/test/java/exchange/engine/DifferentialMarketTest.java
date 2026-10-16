package exchange.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Differential ("oracle") test: a deliberately naive, straight-from-the-textbook reference matching
 * engine is run side by side with the real one on identical randomised order flow, and the two must
 * agree trade-for-trade and on the final top of book.
 *
 * <p>The reference is written for clarity, not speed (boxed {@code TreeMap}s, {@code ArrayDeque}s,
 * no fixed-point tricks, no id recycling), so agreement between the two is strong evidence that the
 * optimised engine really implements a realistic continuous, price–time-priority market and has not
 * been broken by the performance work.
 */
class DifferentialMarketTest {

    private static final int PRODUCTS = 4;
    private static final long TICK = 10L;   // 0.10 EUR/MWh
    private static final long MID = 5000L;  // 50.00 EUR/MWh

    @Test
    @DisplayName("Wide book, occasional market orders")
    void wideBook() {
        runDifferential(0xC0FFEE_1234L, 40, 12);
    }

    @Test
    @DisplayName("Tight one-tick book, many market orders (match-heavy)")
    void tightBook() {
        runDifferential(0x5EED_2222L, 3, 30);
    }

    @Test
    @DisplayName("Wide book with more FOK/IOC and cancels")
    void choppyBook() {
        runDifferential(0xABCD_9876L, 60, 8);
    }

    private void runDifferential(long seed, int spanTicks, int marketPct) {
        final EngineConfig cfg = EngineConfig.defaults();
        final RecordingSink sink = new RecordingSink();
        final MatchingEngine engine = new MatchingEngine(cfg, sink);
        final RefMarket ref = new RefMarket();

        final Random rnd = new Random(seed);
        final int n = 20_000;
        for (long seq = 0; seq < n; seq++) {
            final int product = rnd.nextInt(PRODUCTS);
            final byte side = rnd.nextBoolean() ? Codes.BUY : Codes.SELL;
            final byte type = rnd.nextInt(100) < marketPct ? Codes.MARKET : Codes.LIMIT;
            final int r = rnd.nextInt(100);
            final byte tif = r < 65 ? Codes.GTC : (r < 90 ? Codes.IOC : Codes.FOK);
            final long price = type == Codes.LIMIT
                    ? MID + (rnd.nextInt(2 * spanTicks + 1) - spanTicks) * TICK
                    : 0L;
            final long qty = (1 + rnd.nextInt(20)) * 10L;

            final int id = engine.submit(1, seq, product, side, type, tif, price, qty, 0L);
            ref.submit(product, side, type, tif, price, qty, id);

            // Cancel the order we just placed (safe from id recycling) some of the time.
            if (rnd.nextInt(9) == 0 && id != 0) {
                engine.cancel(1, id, 0L);
                ref.cancel(id);
            }
        }

        // 1) The trade tape must match exactly: product, price, quantity, aggressor side, in order.
        final List<String> actual = new ArrayList<>(sink.trades.size());
        for (RecordingSink.Trade t : sink.trades) {
            actual.add(t.productId() + ":" + t.price() + ":" + t.qty() + ":" + t.aggressorSide());
        }
        assertEquals(ref.tape(), actual, "engine trade tape must equal the reference model");
        assertTrue(actual.size() > 500, "expected plenty of prints, got " + actual.size());

        // 2) The final top of book must match for every product.
        for (int p = 0; p < PRODUCTS; p++) {
            assertEquals(ref.bestBidPrice(p), engine.bestBidPrice(p), "bid px, product " + p);
            assertEquals(ref.bestBidQty(p), engine.bestBidQty(p), "bid qty, product " + p);
            assertEquals(ref.bestAskPrice(p), engine.bestAskPrice(p), "ask px, product " + p);
            assertEquals(ref.bestAskQty(p), engine.bestAskQty(p), "ask qty, product " + p);
        }
    }

    // ------------------------------------------------------------------ reference model

    /** A naive, obviously-correct continuous price–time-priority market. */
    private static final class RefMarket {
        static final class Ord {
            int productId;
            byte side;
            byte type;
            byte tif;
            long price;
            long qty;      // remaining
            int engineId;
        }

        private final Map<Integer, TreeMap<Long, ArrayDeque<Ord>>> bids = new HashMap<>();
        private final Map<Integer, TreeMap<Long, ArrayDeque<Ord>>> asks = new HashMap<>();
        private final Map<Integer, Ord> resting = new HashMap<>();
        private final List<String> trades = new ArrayList<>();

        private TreeMap<Long, ArrayDeque<Ord>> book(int productId, byte side) {
            final Map<Integer, TreeMap<Long, ArrayDeque<Ord>>> m =
                    side == Codes.BUY ? bids : asks;
            return m.computeIfAbsent(productId, k -> new TreeMap<>(
                    side == Codes.BUY ? Comparator.<Long>reverseOrder() : Comparator.<Long>naturalOrder()));
        }

        void submit(int productId, byte side, byte type, byte tif, long price, long qty, int engineId) {
            final Ord t = new Ord();
            t.productId = productId;
            t.side = side;
            t.type = type;
            t.tif = tif;
            t.price = price;
            t.qty = qty;
            t.engineId = engineId;

            if (tif == Codes.FOK && !hasLiquidity(t)) {
                return; // killed: no prints, does not rest
            }
            final long filled = match(t);
            final long remaining = t.qty - filled;
            if (remaining > 0 && type == Codes.LIMIT && tif == Codes.GTC) {
                t.qty = remaining;
                book(productId, side).computeIfAbsent(price, k -> new ArrayDeque<>()).addLast(t);
                resting.put(engineId, t);
            }
        }

        void cancel(int engineId) {
            final Ord o = resting.remove(engineId);
            if (o == null) {
                return;
            }
            final ArrayDeque<Ord> level = book(o.productId, o.side).get(o.price);
            if (level != null) {
                level.remove(o);
                if (level.isEmpty()) {
                    book(o.productId, o.side).remove(o.price);
                }
            }
        }

        private long match(Ord t) {
            final byte opp = t.side == Codes.BUY ? Codes.SELL : Codes.BUY;
            final TreeMap<Long, ArrayDeque<Ord>> b = book(t.productId, opp);
            long remaining = t.qty;
            while (remaining > 0 && !b.isEmpty()) {
                final long bestPrice = b.firstKey();
                if (t.type == Codes.LIMIT
                        && (t.side == Codes.BUY ? bestPrice > t.price : bestPrice < t.price)) {
                    break;
                }
                final ArrayDeque<Ord> level = b.get(bestPrice);
                while (remaining > 0 && !level.isEmpty()) {
                    final Ord maker = level.peekFirst();
                    final long q = Math.min(remaining, maker.qty);
                    trades.add(t.productId + ":" + bestPrice + ":" + q + ":" + t.side);
                    remaining -= q;
                    maker.qty -= q;
                    if (maker.qty == 0) {
                        level.pollFirst();
                        resting.remove(maker.engineId);
                    }
                }
                if (level.isEmpty()) {
                    b.remove(bestPrice);
                }
            }
            return t.qty - remaining;
        }

        private boolean hasLiquidity(Ord t) {
            final byte opp = t.side == Codes.BUY ? Codes.SELL : Codes.BUY;
            long available = 0;
            for (Map.Entry<Long, ArrayDeque<Ord>> e : book(t.productId, opp).entrySet()) {
                final long p = e.getKey();
                if (t.type == Codes.LIMIT
                        && (t.side == Codes.BUY ? p > t.price : p < t.price)) {
                    break;
                }
                for (Ord o : e.getValue()) {
                    available += o.qty;
                }
                if (available >= t.qty) {
                    break;
                }
            }
            return available >= t.qty;
        }

        List<String> tape() {
            return trades;
        }

        long bestBidPrice(int productId) {
            final TreeMap<Long, ArrayDeque<Ord>> m = bids.get(productId);
            return m == null || m.isEmpty() ? 0 : m.firstKey();
        }

        long bestBidQty(int productId) {
            final TreeMap<Long, ArrayDeque<Ord>> m = bids.get(productId);
            if (m == null || m.isEmpty()) {
                return 0;
            }
            long s = 0;
            for (Ord o : m.firstEntry().getValue()) {
                s += o.qty;
            }
            return s;
        }

        long bestAskPrice(int productId) {
            final TreeMap<Long, ArrayDeque<Ord>> m = asks.get(productId);
            return m == null || m.isEmpty() ? 0 : m.firstKey();
        }

        long bestAskQty(int productId) {
            final TreeMap<Long, ArrayDeque<Ord>> m = asks.get(productId);
            if (m == null || m.isEmpty()) {
                return 0;
            }
            long s = 0;
            for (Ord o : m.firstEntry().getValue()) {
                s += o.qty;
            }
            return s;
        }
    }
}
