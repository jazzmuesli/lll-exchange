package exchange.engine;

import static exchange.engine.Codes.BUY;
import static exchange.engine.Codes.FOK;
import static exchange.engine.Codes.GTC;
import static exchange.engine.Codes.IOC;
import static exchange.engine.Codes.LIMIT;
import static exchange.engine.Codes.MARKET;
import static exchange.engine.Codes.SELL;
import static exchange.engine.Codes.ST_ACCEPTED;
import static exchange.engine.Codes.ST_CANCELLED;
import static exchange.engine.Codes.ST_FILLED;
import static exchange.engine.Codes.ST_PARTIAL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Acceptance tests for the continuous price-time-priority matching engine. */
class MatchingEngineTest {

    private static final int P = 1; // product id

    /** price in EUR -> scaled (0.01) */
    private static long eur(double e) {
        return Math.round(e * Scales.PRICE_SCALE);
    }

    /** quantity in MW -> scaled (0.1) */
    private static long mw(double m) {
        return Math.round(m * Scales.QTY_SCALE);
    }

    private MatchingEngine engine;
    private RecordingSink sink;

    @BeforeEach
    void setUp() {
        sink = new RecordingSink();
        engine = new MatchingEngine(EngineConfig.defaults(), sink);
    }

    private int submit(long clientId, byte side, byte type, byte tif, double price, double qty) {
        return engine.submit(1, clientId, P, side, type, tif, eur(price), mw(qty), System.nanoTime());
    }

    @Test
    @DisplayName("A crossing order trades at the resting order's price")
    void tradesAtRestingPrice() {
        submit(1, SELL, LIMIT, GTC, 50.00, 10);
        submit(2, BUY, LIMIT, GTC, 55.00, 10);

        assertEquals(1, sink.fills.size());
        assertEquals(eur(50.00), sink.fills.get(0).price(), "trade must print at the resting (sell) price");
        assertEquals(mw(10), sink.fills.get(0).qty());
        assertEquals(ST_FILLED, lastStatus().status());
    }

    @Test
    @DisplayName("Equal price is served in time order (FIFO)")
    void priceTimePriority() {
        int first = submit(1, SELL, LIMIT, GTC, 50.00, 5);
        int second = submit(2, SELL, LIMIT, GTC, 50.00, 5);
        submit(3, BUY, LIMIT, GTC, 50.00, 5);

        assertEquals(1, sink.fills.size());
        assertEquals(first, sink.fills.get(0).makerOrderId(), "earlier order must be filled first");
        assertFalse(sink.fills.get(0).makerOrderId() == second);
    }

    @Test
    @DisplayName("Remainder rests in the book; later order matches it")
    void partialFillAndRest() {
        submit(1, SELL, LIMIT, GTC, 50.00, 10);     // rests
        submit(2, BUY, LIMIT, GTC, 50.00, 25);      // takes 10, rests 15

        assertEquals(1, sink.fills.size());
        assertEquals(eur(50.00), sink.fills.get(0).price());
        assertEquals(mw(10), sink.fills.get(0).qty());
        assertEquals(ST_PARTIAL, lastStatus().status());
        assertEquals(mw(15), lastStatus().leaves());

        // best bid is now 50.00 with 15 MW resting
        RecordingSink.Book book = sink.books.get(sink.books.size() - 1);
        assertEquals(eur(50.00), book.bid());
        assertEquals(mw(15), book.bidQty());
    }

    @Test
    @DisplayName("IOC fills what it can and cancels the rest, never resting")
    void iocSemantics() {
        submit(1, SELL, LIMIT, GTC, 50.00, 4);
        int ioc = submit(2, BUY, LIMIT, IOC, 50.00, 10);

        assertEquals(1, sink.fills.size());
        assertEquals(mw(4), sink.fills.get(0).qty());
        assertEquals(ST_CANCELLED, lastStatus().status(), "IOC remainder is cancelled");
        assertEquals(ioc, lastStatus().orderId());

        // nothing rests: a fresh sell finds no bid
        sink.fills.clear();
        submit(3, SELL, LIMIT, GTC, 50.00, 1);
        assertTrue(sink.fills.isEmpty(), "IOC order must not have rested");
    }

    @Test
    @DisplayName("FOK is all-or-nothing")
    void fokSemantics() {
        submit(1, SELL, LIMIT, GTC, 50.00, 4);

        sink.fills.clear();
        submit(2, BUY, LIMIT, FOK, 50.00, 10);
        assertTrue(sink.fills.isEmpty(), "FOK with insufficient liquidity must not fill");
        assertEquals(ST_CANCELLED, lastStatus().status());

        sink.fills.clear();
        submit(3, BUY, LIMIT, FOK, 50.00, 4);
        assertEquals(1, sink.fills.size());
        assertEquals(mw(4), sink.fills.get(0).qty());
        assertEquals(ST_FILLED, lastStatus().status());
    }

    @Test
    @DisplayName("Cancel removes liquidity and moves the best price")
    void cancelUpdatesBest() {
        int best = submit(1, BUY, LIMIT, GTC, 50.00, 5);
        submit(2, BUY, LIMIT, GTC, 49.00, 5);

        assertTrue(engine.cancel(1, best, System.nanoTime()));
        RecordingSink.Book book = sink.books.get(sink.books.size() - 1);
        assertEquals(eur(49.00), book.bid(), "best bid must fall back to 49.00");
        assertEquals(ST_CANCELLED, lastStatus().status());
    }

    @Test
    @DisplayName("Market orders sweep the book, printing at each resting price")
    void marketOrderSweeps() {
        submit(1, SELL, LIMIT, GTC, 50.00, 10);
        submit(2, SELL, LIMIT, GTC, 51.00, 10);
        submit(3, SELL, LIMIT, GTC, 52.00, 10);

        submit(4, BUY, MARKET, GTC, 0, 25);

        assertEquals(3, sink.fills.size());
        assertEquals(eur(50.00), sink.fills.get(0).price());
        assertEquals(eur(51.00), sink.fills.get(1).price());
        assertEquals(eur(52.00), sink.fills.get(2).price());
        assertEquals(mw(5), sink.fills.get(2).qty());
        assertEquals(ST_FILLED, lastStatus().status());
    }

    @Test
    @DisplayName("Negative prices are supported (renewable over-supply)")
    void negativePrices() {
        // seller is willing to pay up to 25 EUR/MWh to off-load (asks -25.00);
        // buyer accepts being paid at least 20 (bids -20.00); they cross, print at -25.00
        submit(1, SELL, LIMIT, GTC, -25.00, 10);
        submit(2, BUY, LIMIT, GTC, -20.00, 10);
        assertEquals(1, sink.fills.size());
        assertEquals(eur(-25.00), sink.fills.get(0).price());
    }

    @Test
    @DisplayName("Books are isolated per product (delivery period / zone)")
    void productIsolation() {
        engine.submit(1, 1, 10, SELL, LIMIT, GTC, eur(50.00), mw(10), System.nanoTime());
        engine.submit(1, 2, 11, BUY, LIMIT, GTC, eur(55.00), mw(10), System.nanoTime());
        assertTrue(sink.fills.isEmpty(), "orders on different products must not match");
    }

    @Test
    @DisplayName("Off-tick and out-of-range prices are rejected")
    void rejections() {
        submit(1, BUY, LIMIT, GTC, 50.05, 10); // 5005 not on the 0.10 grid
        assertEquals(Codes.REJ_PRICE_NOT_ON_TICK, sink.acks.get(0).reason());

        submit(2, BUY, LIMIT, GTC, 99999.00, 10);
        assertEquals(Codes.REJ_PRICE_OUT_OF_RANGE, sink.acks.get(1).reason());

        submit(3, BUY, LIMIT, GTC, 50.00, 0);
        assertEquals(Codes.REJ_BAD_QUANTITY, sink.acks.get(2).reason());
    }

    @Test
    @DisplayName("Trades get unique, monotonically increasing trade ids")
    void tradeIdsIncrease() {
        submit(1, SELL, LIMIT, GTC, 50.00, 10);
        submit(2, BUY, LIMIT, GTC, 50.00, 4);
        submit(3, BUY, LIMIT, GTC, 50.00, 4);
        assertEquals(2, sink.fills.size());
        assertTrue(sink.fills.get(1).tradeId() > sink.fills.get(0).tradeId());
    }

    private RecordingSink.Status lastStatus() {
        return sink.statuses.get(sink.statuses.size() - 1);
    }
}
