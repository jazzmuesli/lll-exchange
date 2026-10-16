package exchange.engine;

/**
 * Output side of the matching engine. The engine calls these methods synchronously on the
 * matching thread; implementations must not allocate on the hot path and must not block.
 *
 * <p>All values are primitives and all times are nanosecond monotonic timestamps, so an
 * implementation can encode straight into an off-heap ring buffer with zero garbage.
 */
public interface Sink {

    /** An order was received. {@code reason} is {@link Codes#REJ_NONE} when accepted. */
    void onAccepted(int sessionId, long clientOrderId, int orderId, int productId,
                    int reason, long recvNs);

    /** One execution against a resting (maker) order. Trade price is the maker's price. */
    void onFill(int sessionId, long clientOrderId, int orderId, int makerOrderId, int productId,
                long price, long qty, long tradeId, long tsNs);

    /** Terminal / progress status for an order. */
    void onStatus(int sessionId, long clientOrderId, int orderId, int productId,
                  byte status, long leavesQty, long filledQty, long tsNs);

    /** Top-of-book changed for a product. */
    void onBook(int productId, long bidPrice, long bidQty, long askPrice, long askQty, long tsNs);

    /** A sink that discards everything (useful for pure-throughput microbenchmarks). */
    Sink NOOP = new Sink() {
        @Override public void onAccepted(int s, long c, int o, int p, int r, long t) {}
        @Override public void onFill(int s, long c, int o, int m, int p, long pr, long q, long tid, long t) {}
        @Override public void onStatus(int s, long c, int o, int p, byte st, long l, long f, long t) {}
        @Override public void onBook(int p, long bp, long bq, long ap, long aq, long t) {}
    };
}
