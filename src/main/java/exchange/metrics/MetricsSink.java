package exchange.metrics;

import exchange.engine.Codes;
import exchange.engine.Sink;

/**
 * A {@link Sink} decorator that derives business and market metrics from the engine's event stream
 * and forwards every event to the real sink (ring / journal / tests).
 *
 * <p>Wrapping the sink — rather than teaching the matcher about metrics — keeps {@code
 * MatchingEngine} pure and deterministic, and puts all counters at the exact point of truth: the
 * events the engine actually produced. The decorator sits on the matching thread, so it only ever
 * does additive work: a handful of {@code LongAdder} increments, a couple of array writes for the
 * VWAP buckets, and one call through to the delegate.
 *
 * <p>Counters are looked up once in the constructor and cached in code-indexed arrays, so the hot
 * path never touches the registry map or formats a string.
 */
public final class MetricsSink implements Sink {
    private static final String[] STATUS_NAMES = {
            "", "accepted", "rejected", "partial", "filled", "cancelled", "expired",
    };

    private final Sink delegate;
    private final Metrics metrics;

    private final Counter ordersAccepted;
    private final Counter ordersRejected;
    private final Counter fills;
    private final Counter fillQty;
    private final Counter trades;
    private final Counter tradesBuy;
    private final Counter tradesSell;
    private final Counter tradeQty;
    private final Counter tradeNotional;
    private final Counter bookUpdates;
    private final RateMeter tradeRate;
    private final VwapWindow vwap;

    private final Counter[] rejectedByReason;
    private final Counter[] statusByCode;

    public MetricsSink(Metrics metrics, Sink delegate) {
        this.delegate = delegate;
        this.metrics = metrics;

        this.ordersAccepted = metrics.counter("orders.accepted");
        this.ordersRejected = metrics.counter("orders.rejected");
        this.fills = metrics.counter("fills");
        this.fillQty = metrics.counter("fill.qty");
        this.trades = metrics.counter("trades");
        this.tradesBuy = metrics.counter("trades.buy");
        this.tradesSell = metrics.counter("trades.sell");
        this.tradeQty = metrics.counter("trade.qty");
        this.tradeNotional = metrics.counter("trade.notional");
        this.bookUpdates = metrics.counter("book.updates");
        this.tradeRate = metrics.rate("trades.rate", 60);
        this.vwap = metrics.vwap();

        // Pre-create a counter per known reject reason so the hot path never touches the registry.
        this.rejectedByReason = new Counter[Codes.REJ_UNKNOWN_ORDER + 1];
        for (int code = 1; code < rejectedByReason.length; code++) {
            rejectedByReason[code] = metrics.counter("orders.rejected." + slug(Codes.reasonText(code)));
        }
        this.statusByCode = new Counter[STATUS_NAMES.length];
        for (int code = 1; code < statusByCode.length; code++) {
            statusByCode[code] = metrics.counter("orders.status." + STATUS_NAMES[code]);
        }
    }

    @Override
    public void onAccepted(int sessionId, long clientOrderId, int orderId, int productId,
                           int reason, long recvNs) {
        if (reason == Codes.REJ_NONE) {
            ordersAccepted.inc();
        } else {
            ordersRejected.inc();
            if (reason >= 1) {
                Counter counter = reason < rejectedByReason.length ? rejectedByReason[reason] : null;
                if (counter == null) {
                    // A reason the engine knows but we did not pre-create (future-proofing).
                    counter = metrics.counter("orders.rejected." + slug(Codes.reasonText(reason)));
                }
                counter.inc();
            }
        }
        delegate.onAccepted(sessionId, clientOrderId, orderId, productId, reason, recvNs);
    }

    @Override
    public void onFill(int sessionId, long clientOrderId, int orderId, int makerOrderId, int productId,
                       long price, long qty, long tradeId, long tsNs) {
        fills.inc();
        fillQty.add(qty);
        delegate.onFill(sessionId, clientOrderId, orderId, makerOrderId, productId,
                price, qty, tradeId, tsNs);
    }

    @Override
    public void onMakerFill(int sessionId, long clientOrderId, int orderId, int makerOrderId,
                            int productId, long price, long qty, long tradeId, long tsNs) {
        delegate.onMakerFill(sessionId, clientOrderId, orderId, makerOrderId, productId,
                price, qty, tradeId, tsNs);
    }

    @Override
    public void onTrade(byte aggressorSide, int productId, long price, long qty, long tradeId, long tsNs) {
        trades.inc();
        tradeRate.record();
        if (aggressorSide == Codes.BUY) {
            tradesBuy.inc();
        } else {
            tradesSell.inc();
        }
        tradeQty.add(qty);
        tradeNotional.add(price * qty);
        vwap.onTrade(productId, price, qty, tsNs);
        delegate.onTrade(aggressorSide, productId, price, qty, tradeId, tsNs);
    }

    @Override
    public void onStatus(int sessionId, long clientOrderId, int orderId, int productId,
                         byte status, long leavesQty, long filledQty, long tsNs) {
        if (status >= 1 && status < statusByCode.length) {
            statusByCode[status].inc();
        }
        delegate.onStatus(sessionId, clientOrderId, orderId, productId, status, leavesQty, filledQty, tsNs);
    }

    @Override
    public void onBook(int productId, long bidPrice, long bidQty, long askPrice, long askQty, long tsNs) {
        bookUpdates.inc();
        delegate.onBook(productId, bidPrice, bidQty, askPrice, askQty, tsNs);
    }

    /** Turn a human reason like "price not on tick" into a metric-name-safe "price_not_on_tick". */
    static String slug(String text) {
        final StringBuilder out = new StringBuilder(text.length());
        for (int i = 0; i < text.length(); i++) {
            final char c = text.charAt(i);
            if (Character.isLetterOrDigit(c)) {
                out.append(Character.toLowerCase(c));
            } else {
                out.append('_');
            }
        }
        return out.toString();
    }

    /** Human name for a status code (used by the JSON endpoints). */
    public static String statusName(byte status) {
        return status >= 1 && status < STATUS_NAMES.length ? STATUS_NAMES[status] : ("status(" + status + ")");
    }
}
