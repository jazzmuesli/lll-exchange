package exchange.journal;

import exchange.engine.Sink;

/**
 * Decorates another {@link Sink} with durable journaling into a {@link TradeJournal}.
 *
 * <h2>Why a decorator</h2>
 *
 * <p>Journaling is orthogonal to what the engine does with an event (route it to a client, broadcast
 * it, record it in a test). Wrapping keeps the matching engine unaware of persistence and lets
 * journaling be switched on with {@code --journal} or off entirely for pure-latency benchmarks,
 * without touching the hot-path call sites.
 *
 * <h2>Hot-path requirements</h2>
 *
 * <p>Runs on the matching thread, so it must stay allocation-free and must not block. Appending to a
 * {@link TradeJournal} is a handful of stores into already-faulted-in memory-mapped pages
 * (sub-microsecond); the one caveat is that {@code append} may {@code grow()} the file when full,
 * which remaps and can briefly stall the caller — so size the journal generously for latency runs.
 *
 * <p>The events are journaled with a compact fixed mapping (see {@link TradeJournal#append}): unused
 * fields are zero. In particular {@link #onTrade} is <b>not</b> journaled again — the public tape is
 * exactly the set of fills, which are already recorded by {@link #onFill}, so storing it twice would
 * only bloat the log and slow the merge on replay.
 */
public final class JournalingSink implements Sink {
    /** Where non-journaled side effects go (e.g. {@link exchange.engine.RingSink}). */
    private final Sink delegate;
    private final TradeJournal journal;

    public JournalingSink(Sink delegate, TradeJournal journal) {
        this.delegate = delegate;
        this.journal = journal;
    }

    @Override
    public void onAccepted(int sessionId, long clientOrderId, int orderId, int productId,
                           int reason, long recvNs) {
        // EV_NEW: reason rides in the `status` slot; price/qty/tradeId/maker are unused (0).
        journal.append(TradeJournal.EV_NEW, recvNs, clientOrderId, orderId, productId,
                (byte) 0, 0, 0, 0, (byte) reason, 0);
        delegate.onAccepted(sessionId, clientOrderId, orderId, productId, reason, recvNs);
    }

    @Override
    public void onFill(int sessionId, long clientOrderId, int orderId, int makerOrderId,
                       int productId, long price, long qty, long tradeId, long tsNs) {
        // EV_FILL: side/status unused by a fill; makerOrderId is carried in its dedicated slot.
        journal.append(TradeJournal.EV_FILL, tsNs, clientOrderId, orderId, productId,
                (byte) 0, price, qty, tradeId, (byte) 0, makerOrderId);
        delegate.onFill(sessionId, clientOrderId, orderId, makerOrderId, productId,
                price, qty, tradeId, tsNs);
    }

    @Override
    public void onTrade(byte aggressorSide, int productId, long price, long qty, long tradeId, long tsNs) {
        // Not journaled: the tape is derivable from the fills recorded in onFill (see class doc).
        delegate.onTrade(aggressorSide, productId, price, qty, tradeId, tsNs);
    }

    @Override
    public void onStatus(int sessionId, long clientOrderId, int orderId, int productId,
                         byte status, long leavesQty, long filledQty, long tsNs) {
        // EV_STATUS: `status` is the Codes.ST_* code; price/qty carry the leave/fill split.
        journal.append(TradeJournal.EV_STATUS, tsNs, clientOrderId, orderId, productId,
                (byte) 0, leavesQty, filledQty, 0, status, 0);
        delegate.onStatus(sessionId, clientOrderId, orderId, productId, status, leavesQty, filledQty, tsNs);
    }

    @Override
    public void onBook(int productId, long bidPrice, long bidQty, long askPrice, long askQty, long tsNs) {
        // Not journaled: top-of-book is a derived view, rebuildable from the order events above.
        delegate.onBook(productId, bidPrice, bidQty, askPrice, askQty, tsNs);
    }
}
