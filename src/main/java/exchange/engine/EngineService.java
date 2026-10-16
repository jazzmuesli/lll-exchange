package exchange.engine;

import exchange.journal.TradeJournal;
import exchange.metrics.Counter;
import exchange.metrics.Latency;
import exchange.metrics.Metrics;
import exchange.metrics.MetricsSink;
import exchange.metrics.RateMeter;
import exchange.wire.MsgType;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.OneToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBufferDescriptor;

/**
 * Owns the single matching thread and the lock-free hand-off rings.
 *
 * <p>Data flow:
 * <pre>
 *   gateway readers ──(ManyToOneRingBuffer inbound, MPSC)──▶ [matching thread] ──▶ MatchingEngine
 *                                                              │
 *                        gateway reactor ◀──(OneToOneRingBuffer outbound, SPSC)── RingSink
 * </pre>
 *
 * <p>The matching thread busy-spins on the inbound ring (no locks, no park/unpark, no syscalls) so
 * it reacts to a new order in nanoseconds. It writes results into the outbound ring; the gateway
 * thread encodes the wire protocol off the hot path.
 */
public final class EngineService implements AutoCloseable {
    private static final int DEFAULT_INBOUND = 1 << 18;
    private static final int DEFAULT_OUTBOUND = 1 << 22;

    private final MatchingEngine engine;
    private final ManyToOneRingBuffer inbound;
    private final OneToOneRingBuffer outbound;
    private final RingSink ringSink;
    private final TradeJournal journal;
    private final Thread thread;
    private volatile boolean running;

    // Optional observability handles (null when metrics are disabled). Cached so the matching loop
    // never does a registry lookup; null-checks are a predictable branch, far cheaper than a map get.
    private final Latency orderLatency;
    private final Counter ordersProcessedCounter;
    private final Counter cancelsProcessedCounter;
    private final RateMeter ordersRate;

    private volatile long ordersProcessed;
    private volatile long cancelsProcessed;

    public EngineService(EngineConfig cfg) {
        this(cfg, DEFAULT_INBOUND, DEFAULT_OUTBOUND, null, null);
    }

    public EngineService(EngineConfig cfg, int inboundCapacity, int outboundCapacity) {
        this(cfg, inboundCapacity, outboundCapacity, null, null);
    }

    public EngineService(EngineConfig cfg, int inboundCapacity, int outboundCapacity, String journalPath) {
        this(cfg, inboundCapacity, outboundCapacity, journalPath, null);
    }

    /**
     * @param metrics optional observability registry; when non-null the service wraps its sink in a
     *                {@link MetricsSink} and measures per-order reaction latency
     */
    public EngineService(EngineConfig cfg, int inboundCapacity, int outboundCapacity, String journalPath,
                         Metrics metrics) {
        this.outbound = new OneToOneRingBuffer(ringBuffer(outboundCapacity));
        this.ringSink = new RingSink(outbound);
        Sink sink = ringSink;
        if (journalPath != null) {
            this.journal = new TradeJournal(journalPath);
            sink = new exchange.journal.JournalingSink(ringSink, journal);
        } else {
            this.journal = null;
        }
        // MetricsSink sits closest to the engine so it counts exactly what the matcher emitted.
        if (metrics != null) {
            sink = new MetricsSink(metrics, sink);
        }
        this.engine = new MatchingEngine(cfg, sink);
        this.inbound = new ManyToOneRingBuffer(ringBuffer(inboundCapacity));
        this.thread = new Thread(this::run, "matching-engine");
        this.thread.setDaemon(true);

        this.orderLatency = metrics != null ? metrics.latency("order.latency") : null;
        this.ordersProcessedCounter = metrics != null ? metrics.counter("orders.processed") : null;
        this.cancelsProcessedCounter = metrics != null ? metrics.counter("cancels.processed") : null;
        this.ordersRate = metrics != null ? metrics.rate("orders.rate", 60) : null;
        if (metrics != null) {
            metrics.gauge("engine.inbound.pending.bytes", () -> (long) inbound.size());
            metrics.gauge("engine.outbound.pending.bytes", () -> (long) outbound.size());
        }
    }

    private static UnsafeBuffer ringBuffer(int capacity) {
        final int bytes = capacity + RingBufferDescriptor.TRAILER_LENGTH;
        return new UnsafeBuffer(ByteBuffer.allocateDirect(bytes).order(ByteOrder.nativeOrder()));
    }

    public ManyToOneRingBuffer inbound() {
        return inbound;
    }

    public OneToOneRingBuffer outbound() {
        return outbound;
    }

    public MatchingEngine engine() {
        return engine;
    }

    public RingSink sink() {
        return ringSink;
    }

    public TradeJournal journal() {
        return journal;
    }

    public long ordersProcessed() {
        return ordersProcessed;
    }

    public long cancelsProcessed() {
        return cancelsProcessed;
    }

    public void start() {
        running = true;
        thread.start();
    }

    /** Run the matching loop on the calling thread (used by tests). */
    public void runOnCallingThread() {
        running = true;
        run();
    }

    private void run() {
        final MessageHandler handler = this::onMessage;
        while (running) {
            final int read = inbound.read(handler, 256);
            if (read == 0) {
                Thread.onSpinWait();
            }
        }
    }

    private void onMessage(int msgTypeId, org.agrona.MutableDirectBuffer buf, int index, int length) {
        switch (msgTypeId) {
            case MsgType.NEW_ORDER -> {
                int i = index;
                final int sessionId = buf.getInt(i); i += 4;
                final long clientOrderId = buf.getLong(i); i += 8;
                final int productId = buf.getInt(i); i += 4;
                final byte side = buf.getByte(i); i += 1;
                final byte type = buf.getByte(i); i += 1;
                final byte tif = buf.getByte(i); i += 1;
                final long price = buf.getLong(i); i += 8;
                final long qty = buf.getLong(i); i += 8;
                final long recvNs = buf.getLong(i);
                engine.submit(sessionId, clientOrderId, productId, side, type, tif, price, qty, recvNs);
                ordersProcessed++;
                if (orderLatency != null) {
                    // Reaction latency as the engine process sees it: queue wait + match + egress
                    // encode, measured against the gateway receive timestamp carried in the message.
                    orderLatency.record(System.nanoTime() - recvNs);
                    ordersProcessedCounter.inc();
                    ordersRate.record();
                }
            }
            case MsgType.CANCEL -> {
                int i = index;
                final int sessionId = buf.getInt(i); i += 4;
                final int orderId = buf.getInt(i); i += 4;
                final long tsNs = buf.getLong(i);
                engine.cancel(sessionId, orderId, tsNs);
                cancelsProcessed++;
                if (cancelsProcessedCounter != null) {
                    cancelsProcessedCounter.inc();
                }
            }
            default -> { /* ignore unknown */ }
        }
    }

    @Override
    public void close() {
        running = false;
        if (thread.isAlive()) {
            try {
                thread.join(2_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (journal != null) {
            try {
                journal.close();
            } catch (java.io.IOException ignore) {
                // ignore
            }
        }
    }
}
