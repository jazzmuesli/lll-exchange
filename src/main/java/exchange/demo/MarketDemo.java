package exchange.demo;

import exchange.engine.Codes;
import exchange.engine.EngineConfig;
import exchange.engine.MatchingEngine;
import exchange.engine.Scales;
import exchange.engine.Sink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A runnable walk-through of a small, realistic intraday session on the real matching engine
 * (no sockets, no rings). It logs the public trade tape — the matched prices — and the top of book
 * as they change, so you can <em>see</em> the market.
 *
 * <p>Run with {@code tools/run.sh demo}.
 */
public final class MarketDemo {

    private static final Logger LOG = LoggerFactory.getLogger(MarketDemo.class);

    private static final int PRODUCT = 1; // "DE 15:00-15:15"
    private static long seq;

    private MarketDemo() {}

    public static void main(String[] args) {
        final MatchingEngine engine = new MatchingEngine(EngineConfig.defaults(), new Sink() {
            @Override
            public void onAccepted(int sessionId, long clientOrderId, int orderId, int productId,
                                   int reason, long recvNs) {
                if (reason != Codes.REJ_NONE) {
                    LOG.warn("REJECT   {} reason={}", name(productId), Codes.reasonText(reason));
                }
            }

            @Override
            public void onFill(int sessionId, long clientOrderId, int orderId, int makerOrderId,
                               int productId, long price, long qty, long tradeId, long tsNs) {
                // Private fill notification (would be routed to the two participants); the public
                // tape is logged from onTrade below, so nothing to do here in the demo.
            }

            @Override
            public void onTrade(byte aggressorSide, int productId, long price, long qty,
                                long tradeId, long tsNs) {
                LOG.info("TAPE  {}  {} EUR/MWh x {} MW   ({})",
                        name(productId), Scales.priceToString(price), Scales.qtyToString(qty),
                        aggressorSide == Codes.BUY ? "buy aggressor" : "sell aggressor");
            }

            @Override
            public void onStatus(int sessionId, long clientOrderId, int orderId, int productId,
                                 byte status, long leavesQty, long filledQty, long tsNs) {
            }

            @Override
            public void onBook(int productId, long bidPrice, long bidQty, long askPrice, long askQty,
                               long tsNs) {
                LOG.info("BOOK  {}  bid {} x {}   |   ask {} x {}",
                        name(productId),
                        bidPrice == 0 ? "  -  " : Scales.priceToString(bidPrice), Scales.qtyToString(bidQty),
                        askPrice == 0 ? "  -  " : Scales.priceToString(askPrice), Scales.qtyToString(askQty));
            }
        });

        LOG.info("market open: {}", name(PRODUCT));

        // A producer offers, a consumer bids — the book forms.
        order(engine, Codes.SELL, Codes.LIMIT, Codes.GTC, 50.10, 2.5);
        order(engine, Codes.BUY, Codes.LIMIT, Codes.GTC, 49.90, 3.0);

        // A consumer lifts the offer: prints at the resting (producer) price.
        order(engine, Codes.BUY, Codes.LIMIT, Codes.GTC, 50.10, 1.0);

        // A producer hits the bid: prints at the resting (consumer) price.
        order(engine, Codes.SELL, Codes.LIMIT, Codes.GTC, 49.80, 2.0);

        LOG.info("market close: {}   best bid {} / best ask {}",
                name(PRODUCT),
                Scales.priceToString(engine.bestBidPrice(PRODUCT)),
                Scales.priceToString(engine.bestAskPrice(PRODUCT)));
    }

    private static void order(MatchingEngine engine, byte side, byte type, byte tif,
                              double price, double qty) {
        seq++;
        final String who = side == Codes.BUY ? "BUY " : "SELL";
        LOG.info("ORDER  {}  {} {} MW @ {}", name(PRODUCT), who,
                Scales.qtyToString(Math.round(qty * Scales.QTY_SCALE)),
                Scales.priceToString(Math.round(price * Scales.PRICE_SCALE)));
        engine.submit(1, seq, PRODUCT, side, type, tif,
                Math.round(price * Scales.PRICE_SCALE), Math.round(qty * Scales.QTY_SCALE), 0L);
    }

    private static String name(int productId) {
        return "DE 15:00-15:15 (#" + productId + ")";
    }
}
