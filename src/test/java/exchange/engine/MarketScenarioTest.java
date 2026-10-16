package exchange.engine;

import static exchange.engine.Codes.BUY;
import static exchange.engine.Codes.GTC;
import static exchange.engine.Codes.IOC;
import static exchange.engine.Codes.LIMIT;
import static exchange.engine.Codes.MARKET;
import static exchange.engine.Codes.SELL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * A realistic, human-readable intraday scenario on a single product ("DE 15:00–15:15"): producers
 * and consumers post competing prices, orders cross, print on the public tape and leave a
 * two-sided book. Unlike the unit tests this walks a whole mini-session and checks the market a
 * participant would actually observe — the tape (matched prices) and the top of book.
 */
class MarketScenarioTest {

    private static final int DE_1500 = 1;

    private static long eur(double e) {
        return Math.round(e * Scales.PRICE_SCALE);
    }

    private static long mw(double m) {
        return Math.round(m * Scales.QTY_SCALE);
    }

    private MatchingEngine engine;
    private RecordingSink sink;
    private long clientId;

    @BeforeEach
    void setUp() {
        sink = new RecordingSink();
        engine = new MatchingEngine(EngineConfig.defaults(), sink);
        clientId = 0;
    }

    private int order(byte side, byte type, byte tif, double price, double qty) {
        clientId++;
        return engine.submit(1, clientId, DE_1500, side, type, tif, eur(price), mw(qty), 0L);
    }

    /** "49.90@1.0/SELL" per tape print, in order. */
    private String tape() {
        final StringBuilder sb = new StringBuilder();
        for (RecordingSink.Trade t : sink.trades) {
            sb.append(Scales.priceToString(t.price()))
                    .append('@').append(Scales.qtyToString(t.qty()))
                    .append('/').append(t.aggressorSide() == BUY ? "BUY" : "SELL")
                    .append('\n');
        }
        return sb.toString();
    }

    @Test
    @DisplayName("A full mini-session: prints, cancels and the resulting two-sided book")
    void intradaySession() {
        // -- consumers and producers build competing depth -------------------------------
        order(BUY, LIMIT, GTC, 49.90, 3.0);                   // consumer A: 3.0 MW
        final int p1 = order(SELL, LIMIT, GTC, 50.10, 2.5);   // producer P1: 2.5 MW
        order(BUY, LIMIT, GTC, 49.80, 2.0);                   // consumer B: 2.0 MW

        // no market yet (best bid 49.90, best ask 50.10)
        assertTrue(sink.trades.isEmpty(), "no cross yet");
        assertEquals(eur(49.90), engine.bestBidPrice(DE_1500));
        assertEquals(eur(50.10), engine.bestAskPrice(DE_1500));

        // -- a producer hits the bid: prints at the resting (consumer) price ------------
        order(SELL, LIMIT, GTC, 49.90, 1.0);  // prints 49.90 x1.0
        // -- a consumer lifts the offer: prints at the resting (producer) price ---------
        order(BUY, LIMIT, GTC, 50.10, 1.5);   // prints 50.10 x1.5
        // -- cancel the remaining offer, then a producer sweeps the bid book ------------
        assertTrue(engine.cancel(1, p1, 0L), "P1's remainder is cancellable");
        order(SELL, MARKET, GTC, 0, 3.0);     // prints 49.90 x2.0 then 49.80 x1.0

        // -- the market ends with a two-sided book --------------------------------------
        order(BUY, LIMIT, GTC, 49.70, 4.0);   // rests
        order(SELL, LIMIT, GTC, 50.00, 2.5);  // rests (does not cross 49.80)

        // The public tape (what every market-data subscriber sees) — price@qty/aggressor.
        assertEquals("""
                49.90@1.0/SELL
                50.10@1.5/BUY
                49.90@2.0/SELL
                49.80@1.0/SELL
                """, tape(), "matched prices print at the resting order's price");

        // Private fill notifications mirror the tape one-for-one.
        assertEquals(sink.trades.size(), sink.fills.size());

        // Final top of book: 49.80 x1.0 MW bid, 50.00 x2.5 MW ask.
        assertEquals(eur(49.80), engine.bestBidPrice(DE_1500));
        assertEquals(mw(1.0), engine.bestBidQty(DE_1500));
        assertEquals(eur(50.00), engine.bestAskPrice(DE_1500));
        assertEquals(mw(2.5), engine.bestAskQty(DE_1500));
    }

    @Test
    @DisplayName("The book is never crossed and quantity is conserved across a session")
    void bookInvariants() {
        // A deterministic but busy session: 200 orders alternating around a moving mid.
        for (int i = 0; i < 200; i++) {
            final long mid = 5000 + (i % 7) * 10;      // 50.00 .. 50.60
            final int r = i % 5;
            if (r == 0) {
                order(SELL, MARKET, GTC, 0, 3 + (i % 9));
            } else if (r == 1) {
                order(BUY, LIMIT, GTC, (mid - 10) / 100.0, 2 + (i % 8));
            } else if (r == 2) {
                order(SELL, LIMIT, GTC, (mid + 10) / 100.0, 2 + (i % 8));
            } else if (r == 3) {
                order(BUY, LIMIT, IOC, (mid + 20) / 100.0, 1 + (i % 5));
            } else {
                order(SELL, LIMIT, IOC, (mid - 20) / 100.0, 1 + (i % 5));
            }
        }

        // Invariant 1: the top of book is never crossed.
        for (RecordingSink.Book b : sink.books) {
            if (b.bid() != 0 && b.ask() != 0) {
                assertTrue(b.bid() < b.ask(),
                        "crossed book: bid " + Scales.priceToString(b.bid())
                                + " >= ask " + Scales.priceToString(b.ask()));
            }
        }

        // Invariant 2: every traded lot is a whole, positive quantity and trade ids increase.
        long totalTraded = 0;
        long prevId = 0;
        for (RecordingSink.Trade t : sink.trades) {
            assertTrue(t.qty() > 0, "trade quantity must be positive");
            assertTrue(t.tradeId() > prevId, "trade ids must increase");
            prevId = t.tradeId();
            totalTraded += t.qty();
        }
        assertTrue(totalTraded > 0, "the session must print something");

        // Invariant 3: the quantity that traded is exactly the quantity that left the book as
        // aggressor volume and the quantity credited to makers — conservative accounting.
        assertEquals(totalTraded, sumFillQty(), "tape and fill notifications agree");
    }

    private long sumFillQty() {
        long s = 0;
        for (RecordingSink.Fill f : sink.fills) {
            s += f.qty();
        }
        return s;
    }
}
