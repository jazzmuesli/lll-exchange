package exchange.journal;

import exchange.engine.Sink;

/**
 * Decorates another {@link Sink} with durable journaling into a {@link TradeJournal}.
 *
 * <p>Runs on the matching thread, so it must stay allocation-free and fast; appending to a
 * {@link TradeJournal} is a handful of stores into memory-mapped pages (sub-microsecond). Journaling
 * can be disabled entirely for pure-latency benchmarks (see {@code --journal} in
 * {@code EngineMain}).
 */
public final class JournalingSink implements Sink {
    private final Sink delegate;
    private final TradeJournal journal;

    public JournalingSink(Sink delegate, TradeJournal journal) {
        this.delegate = delegate;
        this.journal = journal;
    }

    @Override
    public void onAccepted(int sessionId, long clientOrderId, int orderId, int productId,
                           int reason, long recvNs) {
        journal.append(TradeJournal.EV_NEW, recvNs, clientOrderId, orderId, productId,
                (byte) 0, 0, 0, 0, (byte) reason, 0);
        delegate.onAccepted(sessionId, clientOrderId, orderId, productId, reason, recvNs);
    }

    @Override
    public void onFill(int sessionId, long clientOrderId, int orderId, int makerOrderId,
                       int productId, long price, long qty, long tradeId, long tsNs) {
        journal.append(TradeJournal.EV_FILL, tsNs, clientOrderId, orderId, productId,
                (byte) 0, price, qty, tradeId, (byte) 0, makerOrderId);
        delegate.onFill(sessionId, clientOrderId, orderId, makerOrderId, productId,
                price, qty, tradeId, tsNs);
    }

    @Override
    public void onStatus(int sessionId, long clientOrderId, int orderId, int productId,
                         byte status, long leavesQty, long filledQty, long tsNs) {
        journal.append(TradeJournal.EV_STATUS, tsNs, clientOrderId, orderId, productId,
                (byte) 0, leavesQty, filledQty, 0, status, 0);
        delegate.onStatus(sessionId, clientOrderId, orderId, productId, status, leavesQty, filledQty, tsNs);
    }

    @Override
    public void onBook(int productId, long bidPrice, long bidQty, long askPrice, long askQty, long tsNs) {
        delegate.onBook(productId, bidPrice, bidQty, askPrice, askQty, tsNs);
    }
}
