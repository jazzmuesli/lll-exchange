package exchange.engine;

/**
 * Output side ("egress") of the matching engine.
 *
 * <p>The engine calls exactly one of these methods for every event it produces — an order was
 * accepted or rejected, a fill happened, a public trade printed, an order's status changed, or the
 * top of book moved. A sink is therefore the single seam between matching and everything downstream
 * (wire encoding, journaling, market-data fan-out, tests).
 *
 * <h2>Threading &amp; performance contract</h2>
 *
 * <p>These methods are called <b>synchronously on the matching thread</b>, while the engine holds no
 * locks. That makes the sink a hard part of the latency budget:
 *
 * <ul>
 *   <li><b>Never block.</b> No I/O, no locks, no {@code Thread.sleep}, no waiting on another
 *       thread. Any blocking call stalls every other order behind it.</li>
 *   <li><b>Never allocate (on the steady-state path).</b> These events fire millions of times per
 *       second; a single allocation per event would flood the collector and show up as tail
 *       latency. {@link RingSink} writes into pre-allocated off-heap memory for exactly this
 *       reason, and {@link Sink#NOOP} exists so pure-throughput benchmarks can skip egress
 *       entirely.</li>
 *   <li><b>Be fast.</b> Whatever time a sink spends is added directly to the matching latency of
 *       the order that triggered the event.</li>
 * </ul>
 *
 * <h2>Value conventions (no conversion on the hot path)</h2>
 *
 * <p>Every field is a primitive already in the engine's internal representation, so a sink can copy
 * it straight into a buffer without parsing or arithmetic:
 *
 * <ul>
 *   <li><b>Prices</b> are fixed-point {@code long}s in <b>0.01 EUR/MWh</b> (never floating point).</li>
 *   <li><b>Quantities</b> are fixed-point {@code long}s in <b>0.1 MW</b>.</li>
 *   <li><b>Timestamps</b> are monotonic nanoseconds. The engine never reads the clock itself — the
 *       caller passes the receive/match time in — so matching stays deterministic and testable. The
 *       engine's reaction time for an order is simply {@code tsNs - recvNs}.</li>
 *   <li><b>Codes</b> ({@link Codes}: sides, order status, reject reasons) are cheap {@code byte}/
 *       {@code int} constants rather than enums, so they travel through lock-free rings and
 *       {@code switch} without allocating.</li>
 * </ul>
 *
 * @see RingSink
 * @see exchange.journal.JournalingSink
 */
public interface Sink {

    /**
     * An order was received and validated. <b>Also emitted for rejects</b> (with a non-zero
     * {@code reason}); callers should check {@code reason} first.
     *
     * @param sessionId     gateway session that submitted the order; used to route the reply back
     *                      to the right client connection
     * @param clientOrderId the caller-assigned order id, echoed back so the client can match the
     *                      ack to its request (unique only within a session)
     * @param orderId       exchange-assigned order id ({@code > 0}) when accepted, or the id that
     *                      could not be allocated on a capacity reject
     * @param productId     contract the order was for (delivery period + bidding area)
     * @param reason        {@link Codes#REJ_NONE} when accepted, otherwise a {@code Codes.REJ_*} code
     *                      (see {@link Codes#reasonText(int)})
     * @param recvNs        monotonic nanoseconds at which the gateway received the order; the
     *                      engine's own reaction time is measured against this
     */
    void onAccepted(int sessionId, long clientOrderId, int orderId, int productId,
                    int reason, long recvNs);

    /**
     * One execution against a resting ("maker") order. A single aggressing order that sweeps several
     * resting orders produces one {@code onFill} <b>per</b> maker it trades with.
     *
     * <p>Per the exchange's regular-matching rule, {@code price} is always the <b>resting</b> order's
     * price, not the aggressor's limit price.
     *
     * <p>This is the <b>private</b> leg of a trade, routed to the aggressor's session. (A future
     * revision will emit a mirror event to the maker's session so both counterparties are notified;
     * today the maker only sees the public {@link #onTrade} print.)
     *
     * @param sessionId     aggressor's gateway session
     * @param clientOrderId aggressor's caller-assigned order id
     * @param orderId       aggressor's exchange-assigned order id
     * @param makerOrderId  exchange-assigned id of the resting order that was hit
     * @param productId     contract that traded
     * @param price         execution price in 0.01 EUR/MWh (the maker's resting price)
     * @param qty           executed quantity in 0.1 MW ({@code <=} both orders' remaining quantity)
     * @param tradeId       unique, monotonically increasing id for this print, shared with
     *                      {@link #onTrade}
     * @param tsNs          monotonic nanoseconds at which the match occurred
     */
    void onFill(int sessionId, long clientOrderId, int orderId, int makerOrderId, int productId,
                long price, long qty, long tradeId, long tsNs);

    /**
     * The <b>mirror</b> of {@link #onFill}, routed to the <b>resting ("maker")</b> order's owner:
     * the private notification that one of its resting orders was hit.
     *
     * <p>Emitted exactly once per execution, immediately after the aggressor's {@link #onFill}. The
     * two legs carry the <i>same</i> {@code price}, {@code qty}, {@code tradeId} and {@code tsNs}, but
     * the order identities are swapped: here {@code orderId} is the maker's order and
     * {@code makerOrderId} is the aggressor's. Without this leg a maker would only learn of a fill
     * from the anonymous public {@link #onTrade} print, which carries no order identity.
     *
     * <p>Defaulted to a no-op so a sink that only cares about one side (e.g. a throughput
     * benchmark) need not implement it.
     *
     * @param sessionId     maker's gateway session
     * @param clientOrderId maker's caller-assigned order id
     * @param orderId       maker's exchange-assigned order id (the resting order)
     * @param makerOrderId  exchange-assigned id of the <b>aggressor</b> that hit it
     * @param productId     contract that traded
     * @param price         execution price in 0.01 EUR/MWh (the maker's resting price)
     * @param qty           executed quantity in 0.1 MW
     * @param tradeId       unique, monotonically increasing id for this print, shared with
     *                      {@link #onTrade}
     * @param tsNs          monotonic nanoseconds at which the match occurred
     */
    default void onMakerFill(int sessionId, long clientOrderId, int orderId, int makerOrderId,
                             int productId, long price, long qty, long tradeId, long tsNs) {}

    /**
     * A <b>public</b> tape print: emitted once per match, independent of which participant(s) are
     * notified. Unlike {@link #onFill} it is anonymous (no session or order identity), so a gateway
     * can broadcast it to every connected market-data subscriber as the trade tape.
     *
     * <p>Kept separate from {@link #onFill} so the routing decision (private reply vs. broadcast)
     * stays on the gateway, not in the matcher.
     *
     * @param aggressorSide side of the order that caused the match
     *                      ({@link Codes#BUY} or {@link Codes#SELL}); the maker was the other side
     * @param productId     contract that traded
     * @param price         execution price in 0.01 EUR/MWh (the maker's resting price)
     * @param qty           executed quantity in 0.1 MW
     * @param tradeId       unique, monotonically increasing id, shared with the matching
     *                      {@link #onFill}
     * @param tsNs          monotonic nanoseconds at which the match occurred
     */
    void onTrade(byte aggressorSide, int productId, long price, long qty, long tradeId, long tsNs);

    /**
     * Terminal or progress status for an order, emitted after it is accepted and after every change
     * to its remaining quantity (partial fill, full fill, cancel, IOC/FOK expiry).
     *
     * @param sessionId     owner's gateway session
     * @param clientOrderId owner's caller-assigned order id
     * @param orderId       exchange-assigned order id
     * @param productId     contract the order is for
     * @param status        a {@code Codes.ST_*} code ({@link Codes#ST_ACCEPTED},
     *                      {@link Codes#ST_PARTIAL}, {@link Codes#ST_FILLED},
     *                      {@link Codes#ST_CANCELLED}, {@link Codes#ST_REJECTED},
     *                      {@link Codes#ST_EXPIRED})
     * @param leavesQty     quantity still working in 0.1 MW (0 once terminal)
     * @param filledQty     cumulative quantity filled in 0.1 MW
     * @param tsNs          monotonic nanoseconds at which the status changed
     */
    void onStatus(int sessionId, long clientOrderId, int orderId, int productId,
                  byte status, long leavesQty, long filledQty, long tsNs);

    /**
     * The top of book for a product changed (best bid/ask price <b>or</b> resting size at the best
     * level). A price of {@code 0} means that side of the book is empty.
     *
     * <p>Only top-of-book is published (not full depth); a multi-level sweep emits a single update
     * for the resulting book, not one per level.
     *
     * @param productId contract whose book moved
     * @param bidPrice  best bid in 0.01 EUR/MWh, or {@code 0} if there are no bids
     * @param bidQty    aggregate quantity resting at the best bid in 0.1 MW, or {@code 0}
     * @param askPrice  best ask in 0.01 EUR/MWh, or {@code 0} if there are no asks
     * @param askQty    aggregate quantity resting at the best ask in 0.1 MW, or {@code 0}
     * @param tsNs      monotonic nanoseconds at which the book changed
     */
    void onBook(int productId, long bidPrice, long bidQty, long askPrice, long askQty, long tsNs);

    /**
     * A sink that discards every event. Used by latency/throughput microbenchmarks
     * ({@code MatchingBenchmark}) to measure the matcher in isolation, without egress cost.
     */
    Sink NOOP = new Sink() {
        @Override public void onAccepted(int sessionId, long clientOrderId, int orderId,
                                         int productId, int reason, long recvNs) {}

        @Override public void onFill(int sessionId, long clientOrderId, int orderId,
                                     int makerOrderId, int productId, long price, long qty,
                                     long tradeId, long tsNs) {}

        @Override public void onTrade(byte aggressorSide, int productId, long price, long qty,
                                      long tradeId, long tsNs) {}

        @Override public void onStatus(int sessionId, long clientOrderId, int orderId,
                                       int productId, byte status, long leavesQty, long filledQty,
                                       long tsNs) {}

        @Override public void onBook(int productId, long bidPrice, long bidQty, long askPrice,
                                     long askQty, long tsNs) {}
    };
}
