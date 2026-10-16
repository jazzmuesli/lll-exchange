package exchange.engine;

/**
 * A single product's limit order book: price-ladder FIFO queues with cached best prices.
 *
 * <p>Layout per side:
 * <ul>
 *   <li>{@code head[tick]} / {@code tail[tick]}: intrusive FIFO of order ids at that price,
 *       giving price-time priority (append at tail, consume at head);</li>
 *   <li>a {@link TickBitmap} over occupied ticks so the next best price is O(1)-ish;</li>
 *   <li>cached {@code bestBidTick}/{@code bestAskTick} so the common case needs no bitmap query.</li>
 * </ul>
 *
 * <p>Not thread-safe: owned exclusively by the matching thread.
 */
public final class OrderBook {
    private final int numTicks;
    private final int[] bidHead;
    private final int[] bidTail;
    private final int[] askHead;
    private final int[] askTail;
    private final long[] bidLevelQty;   // aggregate resting quantity per bid tick (O(1) top-of-book)
    private final long[] askLevelQty;   // aggregate resting quantity per ask tick
    private final TickBitmap bidBits;
    private final TickBitmap askBits;

    private int bestBidTick = -1;
    private int bestAskTick = -1;

    public OrderBook(int numTicks) {
        this.numTicks = numTicks;
        this.bidHead = new int[numTicks];
        this.bidTail = new int[numTicks];
        this.askHead = new int[numTicks];
        this.askTail = new int[numTicks];
        this.bidLevelQty = new long[numTicks];
        this.askLevelQty = new long[numTicks];
        this.bidBits = new TickBitmap(numTicks);
        this.askBits = new TickBitmap(numTicks);
    }

    public int numTicks() {
        return numTicks;
    }

    /** Append an order to the tail (time priority) of its price level. */
    public void add(int orderId, OrderStore os) {
        final byte side = os.side[orderId];
        final int tick = os.levelTick[orderId];
        final int[] head = side == Codes.BUY ? bidHead : askHead;
        final int[] tail = side == Codes.BUY ? bidTail : askTail;
        final TickBitmap bits = side == Codes.BUY ? bidBits : askBits;

        final int t = tail[tick];
        if (t == 0) { // level was empty
            head[tick] = orderId;
            bits.set(tick);
            if (side == Codes.BUY) {
                if (bestBidTick < 0 || tick > bestBidTick) {
                    bestBidTick = tick;
                }
            } else {
                if (bestAskTick < 0 || tick < bestAskTick) {
                    bestAskTick = tick;
                }
            }
        } else {
            os.next[t] = orderId;
            os.prev[orderId] = t;
        }
        tail[tick] = orderId;
        os.resting[orderId] = 1;
        adjustLevelQty(tick, side, os.qty[orderId]);
    }

    /** Unlink an order from its price level. */
    public void remove(int orderId, OrderStore os) {
        final byte side = os.side[orderId];
        final int tick = os.levelTick[orderId];
        final int[] head = side == Codes.BUY ? bidHead : askHead;
        final int[] tail = side == Codes.BUY ? bidTail : askTail;
        final TickBitmap bits = side == Codes.BUY ? bidBits : askBits;

        final int p = os.prev[orderId];
        final int n = os.next[orderId];
        if (p != 0) {
            os.next[p] = n;
        } else {
            head[tick] = n;
        }
        if (n != 0) {
            os.prev[n] = p;
        } else {
            tail[tick] = p;
        }
        os.prev[orderId] = 0;
        os.next[orderId] = 0;
        os.resting[orderId] = 0;
        adjustLevelQty(tick, side, -os.qty[orderId]);

        if (head[tick] == 0) {
            bits.clear(tick);
            if (side == Codes.BUY) {
                if (tick == bestBidTick) {
                    bestBidTick = bits.prevSetBit(tick);
                }
            } else {
                if (tick == bestAskTick) {
                    bestAskTick = bits.nextSetBit(tick);
                }
            }
        }
    }

    /** Head order id at a tick on a side, or 0 if the level is empty. */
    public int headAt(int tick, byte side) {
        return (side == Codes.BUY ? bidHead : askHead)[tick];
    }

    public int bestBidTick() {
        return bestBidTick;
    }

    public int bestAskTick() {
        return bestAskTick;
    }

    /** Next occupied ask tick strictly above {@code tick}, or -1. */
    public int nextAskTick(int tick) {
        return askBits.nextSetBit(tick + 1);
    }

    /** Next occupied bid tick strictly below {@code tick}, or -1. */
    public int prevBidTick(int tick) {
        return bidBits.prevSetBit(tick - 1);
    }

    /** Adjust the aggregate resting quantity at a price level (used on partial fills). */
    public void adjustLevelQty(int tick, byte side, long delta) {
        if (side == Codes.BUY) {
            bidLevelQty[tick] += delta;
        } else {
            askLevelQty[tick] += delta;
        }
    }

    /** Aggregate resting quantity at a level (O(1)). */
    public long qtyAt(int tick, byte side, OrderStore os) {
        return (side == Codes.BUY ? bidLevelQty : askLevelQty)[tick];
    }
}
