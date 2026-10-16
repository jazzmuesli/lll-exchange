package exchange.engine;

/**
 * The single-threaded matching engine for a continuous, price-time-priority electricity
 * intraday market.
 *
 * <p>Semantics (see {@code docs/01-research-electricity-markets.md}):
 * <ul>
 *   <li>Better price wins; at equal price the earlier order wins (FIFO per level).</li>
 *   <li>Trades execute at the <b>resting</b> order's price (EPEX "regular matching").</li>
 *   <li>{@code IOC} fills what it can and cancels the rest; {@code FOK} is all-or-nothing;
 *       {@code GTC} rests the remainder. Market orders that cannot fully fill are cancelled.</li>
 *   <li>Negative prices are supported (the ladder starts below zero).</li>
 * </ul>
 *
 * <p>This class is pure and deterministic: it never reads the clock except for the caller-supplied
 * timestamps, and never allocates on the hot path. It is intended to be owned by exactly one
 * thread.
 */
public final class MatchingEngine {
    private final EngineConfig cfg;
    private final OrderStore os;
    private final OrderBook[] books;
    private final Sink sink;
    private long nextTradeId = 1;

    public MatchingEngine(EngineConfig cfg, Sink sink) {
        this.cfg = cfg;
        this.os = new OrderStore(cfg.maxOrders());
        this.books = new OrderBook[cfg.maxProducts()];
        this.sink = sink;
    }

    public EngineConfig config() {
        return cfg;
    }

    public OrderStore orderStore() {
        return os;
    }

    public long nextTradeId() {
        return nextTradeId;
    }

    /**
     * Submit a new order.
     *
     * @return the exchange order id, or 0 if the order was rejected (reason already emitted)
     */
    public int submit(int sessionId, long clientOrderId, int productId, byte side, byte type,
                      byte tif, long price, long qty, long recvNs) {
        final int reason = validate(productId, side, type, tif, price, qty);
        if (reason != Codes.REJ_NONE) {
            sink.onAccepted(sessionId, clientOrderId, 0, productId, reason, recvNs);
            return 0;
        }

        final int orderId = os.allocate();
        if (orderId == 0) {
            sink.onAccepted(sessionId, clientOrderId, 0, productId, Codes.REJ_ORDER_CAPACITY, recvNs);
            return 0;
        }

        os.clientOrderId[orderId] = clientOrderId;
        os.sessionId[orderId] = sessionId;
        os.productId[orderId] = productId;
        os.side[orderId] = side;
        os.type[orderId] = type;
        os.tif[orderId] = tif;
        os.price[orderId] = price;
        os.qty[orderId] = qty;
        os.origQty[orderId] = qty;
        os.levelTick[orderId] = type == Codes.LIMIT ? cfg.tickFor(price) : -1;
        os.resting[orderId] = 0;

        sink.onAccepted(sessionId, clientOrderId, orderId, productId, Codes.REJ_NONE, recvNs);

        final OrderBook book = bookFor(productId);
        final int prevBid = book.bestBidTick();
        final int prevAsk = book.bestAskTick();

        // Fill-or-kill needs a liquidity pre-check so it can be all-or-nothing.
        if (tif == Codes.FOK && !hasLiquidity(book, side, type, price, qty)) {
            os.release(orderId);
            sink.onStatus(sessionId, clientOrderId, orderId, productId, Codes.ST_CANCELLED, 0, 0, recvNs);
            return orderId;
        }

        final long filled = match(book, orderId, sessionId, clientOrderId, productId,
                side, type, tif, price, recvNs);
        final long remaining = qty - filled;

        final byte status;
        if (remaining == 0) {
            status = Codes.ST_FILLED;
            os.release(orderId);
        } else if (type == Codes.LIMIT && tif == Codes.GTC) {
            os.qty[orderId] = remaining;
            os.levelTick[orderId] = cfg.tickFor(price);
            book.add(orderId, os);
            status = filled > 0 ? Codes.ST_PARTIAL : Codes.ST_ACCEPTED;
        } else {
            // IOC / FOK / market remainder: cancel what is left.
            status = Codes.ST_CANCELLED;
            os.release(orderId);
        }

        sink.onStatus(sessionId, clientOrderId, orderId, productId, status, remaining, filled, recvNs);
        emitBookIfChanged(book, productId, prevBid, prevAsk, recvNs);
        return orderId;
    }

    /** Cancel a resting order by exchange order id. @return true if it was cancelled */
    public boolean cancel(int sessionId, int orderId, long tsNs) {
        if (orderId <= 0 || orderId >= os.capacity() || os.resting[orderId] != 1) {
            return false;
        }
        final int productId = os.productId[orderId];
        final long clientOrderId = os.clientOrderId[orderId];
        final long filled = os.origQty[orderId] - os.qty[orderId];
        final OrderBook book = bookFor(productId);
        final int prevBid = book.bestBidTick();
        final int prevAsk = book.bestAskTick();

        book.remove(orderId, os);
        sink.onStatus(sessionId, clientOrderId, orderId, productId, Codes.ST_CANCELLED, 0, filled, tsNs);
        emitBookIfChanged(book, productId, prevBid, prevAsk, tsNs);
        os.release(orderId);
        return true;
    }

    // ------------------------------------------------------------------

    private long match(OrderBook book, int orderId, int sessionId, long clientOrderId, int productId,
                       byte side, byte type, byte tif, long price, long tsNs) {
        final byte oppSide = side == Codes.BUY ? Codes.SELL : Codes.BUY;
        long remaining = os.qty[orderId];
        long filled = 0;
        final boolean limit = type == Codes.LIMIT;

        while (remaining > 0) {
            final int bestTick = oppSide == Codes.SELL ? book.bestAskTick() : book.bestBidTick();
            if (bestTick < 0) {
                break;
            }
            final long bestPrice = cfg.priceForTick(bestTick);
            if (limit) {
                if (side == Codes.BUY ? bestPrice > price : bestPrice < price) {
                    break; // does not cross
                }
            }
            final int makerId = book.headAt(bestTick, oppSide);
            if (makerId == 0) {
                break; // defensive: empty level should have been cleared from the bitmap
            }
            final long makerQty = os.qty[makerId];
            final long tradeQty = Math.min(remaining, makerQty);
            final long tradeId = nextTradeId++;

            remaining -= tradeQty;
            filled += tradeQty;
            os.qty[makerId] = makerQty - tradeQty;
            book.adjustLevelQty(bestTick, oppSide, -tradeQty);

            sink.onFill(sessionId, clientOrderId, orderId, makerId, productId,
                    bestPrice, tradeQty, tradeId, tsNs);

            if (os.qty[makerId] == 0) {
                book.remove(makerId, os);   // unlink while data still valid
                os.release(makerId);        // then recycle the id
            }
        }
        return filled;
    }

    private boolean hasLiquidity(OrderBook book, byte side, byte type, long price, long qty) {
        final byte oppSide = side == Codes.BUY ? Codes.SELL : Codes.BUY;
        final boolean limit = type == Codes.LIMIT;
        long available = 0;
        int tick = oppSide == Codes.SELL ? book.bestAskTick() : book.bestBidTick();
        while (tick >= 0 && available < qty) {
            final long levelPrice = cfg.priceForTick(tick);
            if (limit && (side == Codes.BUY ? levelPrice > price : levelPrice < price)) {
                break;
            }
            final int head = book.headAt(tick, oppSide);
            for (int id = head; id != 0; id = os.next[id]) {
                available += os.qty[id];
            }
            tick = oppSide == Codes.SELL ? book.nextAskTick(tick) : book.prevBidTick(tick);
        }
        return available >= qty;
    }

    private int validate(int productId, byte side, byte type, byte tif, long price, long qty) {
        if (productId < 0 || productId >= cfg.maxProducts()) {
            return Codes.REJ_UNKNOWN_PRODUCT;
        }
        if (side != Codes.BUY && side != Codes.SELL) {
            return Codes.REJ_BAD_SIDE;
        }
        if (type != Codes.LIMIT && type != Codes.MARKET) {
            return Codes.REJ_BAD_TYPE;
        }
        if (tif != Codes.GTC && tif != Codes.IOC && tif != Codes.FOK) {
            return Codes.REJ_BAD_TIF;
        }
        if (qty <= 0) {
            return Codes.REJ_BAD_QUANTITY;
        }
        if (type == Codes.LIMIT) {
            if (price < cfg.minPrice() || price > cfg.maxPrice()) {
                return Codes.REJ_PRICE_OUT_OF_RANGE;
            }
            if (cfg.tickFor(price) < 0) {
                return Codes.REJ_PRICE_NOT_ON_TICK;
            }
        }
        return Codes.REJ_NONE;
    }

    private OrderBook bookFor(int productId) {
        OrderBook b = books[productId];
        if (b == null) {
            b = new OrderBook(cfg.numTicks());
            books[productId] = b;
        }
        return b;
    }

    private void emitBookIfChanged(OrderBook book, int productId, int prevBid, int prevAsk, long tsNs) {
        final int bid = book.bestBidTick();
        final int ask = book.bestAskTick();
        if (bid == prevBid && ask == prevAsk) {
            return;
        }
        final long bidPrice = bid < 0 ? 0 : cfg.priceForTick(bid);
        final long askPrice = ask < 0 ? 0 : cfg.priceForTick(ask);
        final long bidQty = bid < 0 ? 0 : book.qtyAt(bid, Codes.BUY, os);
        final long askQty = ask < 0 ? 0 : book.qtyAt(ask, Codes.SELL, os);
        sink.onBook(productId, bidPrice, bidQty, askPrice, askQty, tsNs);
    }
}
