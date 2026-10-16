package exchange.engine;

/**
 * Struct-of-arrays storage for live orders, indexed by an {@code int} order id (0 is reserved
 * as "null"). Keeping the fields in parallel primitive arrays means an order occupies no object
 * header, no per-order allocation happens on the hot path, and the matching thread stays on
 * cache-friendly contiguous memory.
 *
 * <p>Order ids are recycled through a free stack, so a long-running process has bounded memory.
 */
public final class OrderStore {
    private final int capacity;

    public final long[] clientOrderId;
    public final int[] sessionId;
    public final int[] productId;
    public final byte[] side;
    public final byte[] type;
    public final byte[] tif;
    public final long[] price;
    public final long[] qty;       // remaining
    public final long[] origQty;
    public final int[] next;       // FIFO within a price level
    public final int[] prev;
    public final int[] levelTick;  // tick index while resting, else -1
    public final byte[] resting;   // 1 = in a book

    private final int[] freeStack;
    private int freeTop;
    private int highWater; // next never-used id

    public OrderStore(int capacity) {
        this.capacity = capacity;
        this.clientOrderId = new long[capacity];
        this.sessionId = new int[capacity];
        this.productId = new int[capacity];
        this.side = new byte[capacity];
        this.type = new byte[capacity];
        this.tif = new byte[capacity];
        this.price = new long[capacity];
        this.qty = new long[capacity];
        this.origQty = new long[capacity];
        this.next = new int[capacity];
        this.prev = new int[capacity];
        this.levelTick = new int[capacity];
        this.resting = new byte[capacity];
        this.freeStack = new int[capacity];
    }

    /** @return a fresh order id, or 0 if capacity is exhausted. */
    public int allocate() {
        if (freeTop > 0) {
            final int id = freeStack[--freeTop];
            return id;
        }
        if (highWater + 1 >= capacity) {
            return 0;
        }
        return ++highWater;
    }

    /** Return an order id to the free list. */
    public void release(int id) {
        next[id] = 0;
        prev[id] = 0;
        levelTick[id] = -1;
        resting[id] = 0;
        freeStack[freeTop++] = id;
    }

    public int capacity() {
        return capacity;
    }

    /** Number of order ids handed out since start-up (monotonic; for statistics only). */
    public int highWater() {
        return highWater;
    }
}
