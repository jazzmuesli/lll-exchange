package exchange.gateway;

import exchange.metrics.Counter;
import exchange.metrics.Metrics;
import exchange.metrics.RateMeter;
import exchange.wire.FastWire;
import exchange.wire.MsgType;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.OneToOneRingBuffer;

/**
 * NIO gateway: terminates TCP, decodes length-prefixed protobuf (via the hand-rolled
 * {@link FastWire} codec), publishes fixed-layout order events into the inbound ring buffer, and
 * fans engine output back to the owning session.
 *
 * <p>Single reactor thread performs accept/read/write so the outbound hand-off can be a
 * single-consumer {@link OneToOneRingBuffer} (SPSC). Writes are batched per drain cycle to keep
 * syscalls low. The matching thread is never blocked by the network: a slow client fills the
 * outbound ring, and excess is dropped and counted.
 */
public final class GatewayServer implements AutoCloseable {
    private static final int MAX_SESSIONS = 4096;
    private static final int READ_BUF = 1 << 16;
    private static final int OUT_BUF = 1 << 16;
    private static final int MAX_FRAME = 16 << 20;

    private final int port;
    private final ManyToOneRingBuffer inbound;
    private final OneToOneRingBuffer outbound;

    private final UnsafeBuffer scratch = new UnsafeBuffer(new byte[128]);
    private final FastWire.ClientParser clientParser = new FastWire.ClientParser();
    private final FastWire.ServerEncoder serverEncoder = new FastWire.ServerEncoder();
    private final ArrayList<Session> dirty = new ArrayList<>();
    private final Session[] sessions = new Session[MAX_SESSIONS];
    private final org.agrona.concurrent.MessageHandler engineHandler = this::onEngineMessage;

    private Selector selector;
    private ServerSocketChannel server;
    private Thread reactor;
    private volatile boolean running;
    private int nextSessionId = 1;
    private long droppedOutbound;

    // Optional observability handles (null when metrics are disabled): cached so the reactor thread
    // never looks a name up in the registry map.
    private final Counter ordersSubmitted;
    private final Counter cancelsSubmitted;
    private final Counter connectionsOpened;
    private final Counter connectionsClosed;
    private final Counter bytesReadCounter;
    private final Counter bytesWrittenCounter;
    private final Counter messagesOut;
    private final RateMeter bytesReadRate;
    private final RateMeter bytesWrittenRate;

    /**
     * Allocation-free ready-key set, installed over the JDK selector's own sets in {@link #start()}.
     * {@code null} means the install was not possible and the loop uses the standard iterator.
     */
    private SelectedSelectionKeySet fastSelectedKeys;

    public GatewayServer(int port, ManyToOneRingBuffer inbound, OneToOneRingBuffer outbound) {
        this(port, inbound, outbound, null);
    }

    /**
     * @param metrics optional observability registry; when non-null the gateway publishes connection,
     *                byte, and message counters plus session/backlog gauges
     */
    public GatewayServer(int port, ManyToOneRingBuffer inbound, OneToOneRingBuffer outbound, Metrics metrics) {
        this.port = port;
        this.inbound = inbound;
        this.outbound = outbound;
        if (metrics != null) {
            this.ordersSubmitted = metrics.counter("orders.submitted");
            this.cancelsSubmitted = metrics.counter("cancels.submitted");
            this.connectionsOpened = metrics.counter("gateway.connections.opened");
            this.connectionsClosed = metrics.counter("gateway.connections.closed");
            this.bytesReadCounter = metrics.counter("gateway.bytes.read");
            this.bytesWrittenCounter = metrics.counter("gateway.bytes.written");
            this.messagesOut = metrics.counter("gateway.messages.out");
            this.bytesReadRate = metrics.rate("gateway.bytes.read.rate", 60);
            this.bytesWrittenRate = metrics.rate("gateway.bytes.written.rate", 60);
            metrics.gauge("gateway.sessions.active", this::activeSessions);
            metrics.gauge("gateway.fast_selector", () -> usingFastSelector() ? 1L : 0L);
            metrics.gauge("gateway.dropped.outbound", () -> droppedOutbound);
        } else {
            this.ordersSubmitted = null;
            this.cancelsSubmitted = null;
            this.connectionsOpened = null;
            this.connectionsClosed = null;
            this.bytesReadCounter = null;
            this.bytesWrittenCounter = null;
            this.messagesOut = null;
            this.bytesReadRate = null;
            this.bytesWrittenRate = null;
        }
    }

    public int port() {
        return port;
    }

    /** Actual bound port (useful when constructed with port 0). */
    public int boundPort() {
        if (server != null) {
            try {
                return ((java.net.InetSocketAddress) server.getLocalAddress()).getPort();
            } catch (IOException ignore) {
                // fall through
            }
        }
        return port;
    }

    public long droppedOutbound() {
        return droppedOutbound;
    }

    public void start() throws IOException {
        selector = Selector.open();
        installFastSelectedKeys();
        server = ServerSocketChannel.open();
        server.configureBlocking(false);
        server.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        server.bind(new InetSocketAddress(port));
        server.register(selector, SelectionKey.OP_ACCEPT);
        running = true;
        reactor = new Thread(this::loop, "gateway-reactor");
        reactor.setDaemon(true);
        reactor.start();
    }

    private void loop() {
        while (running) {
            final int ready;
            try {
                ready = selector.selectNow();
            } catch (IOException e) {
                if (!running) {
                    break;
                }
                continue;
            }
            if (ready > 0) {
                final SelectedSelectionKeySet fast = fastSelectedKeys;
                if (fast != null) {
                    // Allocation-free path: snapshot the array + count, clear the set, dispatch by
                    // index. Clearing up front means keys cancelled during dispatch (channel close)
                    // are skipped by the isValid() check below; the array is reused every cycle.
                    final SelectionKey[] keys = fast.keys;
                    final int count = fast.size;
                    fast.size = 0;
                    for (int i = 0; i < count; i++) {
                        handleKey(keys[i]);
                    }
                } else {
                    final Iterator<SelectionKey> it = selector.selectedKeys().iterator();
                    while (it.hasNext()) {
                        final SelectionKey key = it.next();
                        it.remove();
                        handleKey(key);
                    }
                }
            }
            drainOutbound();
            flushDirty();
            Thread.onSpinWait();
        }
    }

    /**
     * Replace the JDK selector's ready-key sets with our allocation-free {@link
     * SelectedSelectionKeySet} so the reactor loop does not allocate an iterator per select.
     *
     * <p>Reflection on {@code sun.nio.ch.SelectorImpl} (needs {@code --add-opens
     * java.base/sun.nio.ch=ALL-UNNAMED}, which {@code tools/run.sh} and Maven surefire both pass).
     * Any failure — non-OpenJDK selector, missing module open — is swallowed and the loop falls back
     * to the standard iterator, so this is strictly an optimisation, never a correctness risk.
     */
    private void installFastSelectedKeys() {
        try {
            final Class<?> selectorImplClass = Class.forName("sun.nio.ch.SelectorImpl");
            if (!selectorImplClass.isInstance(selector)) {
                return;
            }
            final SelectedSelectionKeySet fast = new SelectedSelectionKeySet();
            final Field selectedKeys = selectorImplClass.getDeclaredField("selectedKeys");
            final Field publicSelectedKeys = selectorImplClass.getDeclaredField("publicSelectedKeys");
            selectedKeys.setAccessible(true);
            publicSelectedKeys.setAccessible(true);
            selectedKeys.set(selector, fast);
            publicSelectedKeys.set(selector, fast);
            fastSelectedKeys = fast;
        } catch (Throwable t) {
            fastSelectedKeys = null;
        }
    }

    /** True when the allocation-free ready-key set is in use (i.e. the install succeeded). */
    boolean usingFastSelector() {
        return fastSelectedKeys != null;
    }

    /** Handle one ready key; shared by the fast and fallback loops. */
    private void handleKey(SelectionKey key) {
        try {
            if (!key.isValid()) {
                return;
            }
            if (key.isAcceptable()) {
                accept();
            } else {
                final Session session = (Session) key.attachment();
                if (key.isReadable()) {
                    read(session);
                }
                if (key.isValid() && key.isWritable()) {
                    flush(session);
                }
            }
        } catch (IOException e) {
            final Object attachment = key.attachment();
            if (attachment instanceof Session session) {
                closeSession(session);
            }
        }
    }

    private void accept() throws IOException {
        SocketChannel channel;
        while ((channel = server.accept()) != null) {
            channel.configureBlocking(false);
            channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
            if (nextSessionId >= MAX_SESSIONS) {
                channel.close();
                continue;
            }
            final int sessionId = nextSessionId++;
            final Session session = new Session(sessionId, channel);
            sessions[sessionId] = session;
            session.key = channel.register(selector, SelectionKey.OP_READ, session);
            if (connectionsOpened != null) {
                connectionsOpened.inc();
            }
        }
    }

    /** Live connection count, sampled by the management thread. */
    private long activeSessions() {
        long live = 0;
        final int limit = Math.min(nextSessionId, sessions.length);
        for (int i = 1; i < limit; i++) {
            if (sessions[i] != null) {
                live++;
            }
        }
        return live;
    }

    private void read(Session session) throws IOException {
        final int n = session.channel.read(session.readBuf);
        if (n < 0) {
            closeSession(session);
            return;
        }
        if (n == 0) {
            return;
        }
        if (bytesReadCounter != null) {
            bytesReadCounter.add(n);
            bytesReadRate.record(n);
        }
        session.readBuf.flip();
        try {
            while (session.readBuf.remaining() >= 4) {
                final int len = session.readBuf.getInt();
                if (len < 0 || len > MAX_FRAME) {
                    closeSession(session);
                    return;
                }
                if (session.readBuf.remaining() < len) {
                    session.readBuf.position(session.readBuf.position() - 4);
                    break;
                }
                final int payloadStart = session.readBuf.position();
                final int kind = clientParser.parse(session.readBuf, payloadStart, len);
                dispatch(session, kind);
            }
        } finally {
            session.readBuf.compact();
        }
    }

    private void dispatch(Session session, int kind) {
        switch (kind) {
            case FastWire.CK_NEW_ORDER -> {
                final MutableDirectBuffer b = scratch;
                int i = 0;
                b.putInt(i, session.sessionId); i += 4;
                b.putLong(i, clientParser.clientOrderId); i += 8;
                b.putInt(i, clientParser.productId); i += 4;
                b.putByte(i, (byte) clientParser.side); i += 1;
                b.putByte(i, (byte) clientParser.type); i += 1;
                b.putByte(i, (byte) clientParser.tif); i += 1;
                b.putLong(i, clientParser.price); i += 8;
                b.putLong(i, clientParser.quantity); i += 8;
                b.putLong(i, System.nanoTime()); i += 8;
                inbound.write(MsgType.NEW_ORDER, b, 0, i);
                if (ordersSubmitted != null) {
                    ordersSubmitted.inc();
                }
            }
            case FastWire.CK_CANCEL -> {
                final MutableDirectBuffer b = scratch;
                int i = 0;
                b.putInt(i, session.sessionId); i += 4;
                b.putInt(i, (int) clientParser.exchangeOrderId); i += 4;
                b.putLong(i, System.nanoTime()); i += 8;
                inbound.write(MsgType.CANCEL, b, 0, i);
                if (cancelsSubmitted != null) {
                    cancelsSubmitted.inc();
                }
            }
            default -> { /* heartbeat / ignore */ }
        }
    }

    // ------------------------------------------------------- outbound (egress)

    private void drainOutbound() {
        int read;
        while ((read = outbound.read(engineHandler, 512)) > 0) {
            // keep draining
        }
    }

    private void onEngineMessage(int msgTypeId, MutableDirectBuffer buf, int index, int length) {
        switch (msgTypeId) {
            case MsgType.ACK -> onAck(buf, index);
            case MsgType.FILL -> onFill(buf, index);
            case MsgType.TRADE -> onTrade(buf, index);
            case MsgType.STATUS -> onStatus(buf, index);
            case MsgType.BOOK -> onBook(buf, index);
            default -> { }
        }
    }

    private void onAck(MutableDirectBuffer buf, int index) {
        int i = index;
        final int sessionId = buf.getInt(i); i += 4;
        final long clientOrderId = buf.getLong(i); i += 8;
        final int orderId = buf.getInt(i); i += 4;
        i += 4; // productId (unused in ack)
        final int reason = buf.getInt(i); i += 4;
        final long recvNs = buf.getLong(i);

        final Session session = sessions[sessionId];
        if (session == null) {
            return;
        }
        append(session, serverEncoder.ack(clientOrderId, orderId, reason == 0,
                reason == 0 ? "" : exchange.engine.Codes.reasonText(reason), recvNs));
    }

    private void onFill(MutableDirectBuffer buf, int index) {
        int i = index;
        final int sessionId = buf.getInt(i); i += 4;
        final long clientOrderId = buf.getLong(i); i += 8;
        final int orderId = buf.getInt(i); i += 4;
        final int makerOrderId = buf.getInt(i); i += 4;
        final int productId = buf.getInt(i); i += 4;
        final long price = buf.getLong(i); i += 8;
        final long qty = buf.getLong(i); i += 8;
        final long tradeId = buf.getLong(i); i += 8;
        final long ts = buf.getLong(i);

        final Session session = sessions[sessionId];
        if (session == null) {
            return;
        }
        append(session, serverEncoder.exec(clientOrderId, orderId, productId, exchange.engine.Codes.ST_PARTIAL,
                0, qty, true, tradeId, makerOrderId, price, qty, ts));
    }

    private void onStatus(MutableDirectBuffer buf, int index) {
        int i = index;
        final int sessionId = buf.getInt(i); i += 4;
        final long clientOrderId = buf.getLong(i); i += 8;
        final int orderId = buf.getInt(i); i += 4;
        final int productId = buf.getInt(i); i += 4;
        final byte status = buf.getByte(i); i += 1;
        final long leaves = buf.getLong(i); i += 8;
        final long filled = buf.getLong(i); i += 8;
        final long ts = buf.getLong(i);

        final Session session = sessions[sessionId];
        if (session == null) {
            return;
        }
        append(session, serverEncoder.exec(clientOrderId, orderId, productId, status,
                leaves, filled, false, 0, 0, 0, 0, ts));
    }

    private void onTrade(MutableDirectBuffer buf, int index) {
        int i = index;
        final byte aggressorSide = buf.getByte(i); i += 1;
        final int productId = buf.getInt(i); i += 4;
        final long price = buf.getLong(i); i += 8;
        final long qty = buf.getLong(i); i += 8;
        final long tradeId = buf.getLong(i); i += 8;
        final long ts = buf.getLong(i);

        broadcast(serverEncoder.trade(tradeId, productId, price, qty, aggressorSide, ts));
    }

    private void onBook(MutableDirectBuffer buf, int index) {
        int i = index;
        final int productId = buf.getInt(i); i += 4;
        final long bidPrice = buf.getLong(i); i += 8;
        final long bidQty = buf.getLong(i); i += 8;
        final long askPrice = buf.getLong(i); i += 8;
        final long askQty = buf.getLong(i); i += 8;
        final long ts = buf.getLong(i);

        broadcast(serverEncoder.book(productId, bidPrice, bidQty, askPrice, askQty, ts));
    }

    /** Fan a shared (already encoded) payload out to every connected session. */
    private void broadcast(ByteBuffer payload) {
        for (int sessionId = 1; sessionId < nextSessionId; sessionId++) {
            final Session session = sessions[sessionId];
            if (session != null) {
                append(session, payload);
            }
        }
    }

    private void append(Session session, ByteBuffer payload) {
        final int need = 4 + payload.remaining();
        if (session.out.remaining() < need) {
            flush(session);
            if (session.out.remaining() < need) {
                droppedOutbound++;
                return;
            }
        }
        // payload may be shared (broadcast), so consume without disturbing its position
        final int start = payload.position();
        final int limit = payload.limit();
        session.out.putInt(limit - start);
        session.out.put(payload);
        payload.position(start);
        payload.limit(limit);
        if (messagesOut != null) {
            messagesOut.inc();
        }
        if (!session.dirty) {
            session.dirty = true;
            dirty.add(session);
        }
    }

    private void flushDirty() {
        for (int k = 0; k < dirty.size(); k++) {
            final Session session = dirty.get(k);
            session.dirty = false;
            flush(session);
        }
        dirty.clear();
    }

    private void flush(Session session) {
        if (session.out.position() == 0) {
            clearWriteInterest(session);
            return;
        }
        session.out.flip();
        final int written;
        try {
            written = session.channel.write(session.out);
        } catch (IOException e) {
            closeSession(session);
            return;
        }
        if (bytesWrittenCounter != null) {
            bytesWrittenCounter.add(written);
            bytesWrittenRate.record(written);
        }
        if (session.out.hasRemaining()) {
            addWriteInterest(session);
        } else {
            clearWriteInterest(session);
        }
        session.out.compact();
    }

    private void addWriteInterest(Session session) {
        if (session.key != null && (session.key.interestOps() & SelectionKey.OP_WRITE) == 0) {
            session.key.interestOps(session.key.interestOps() | SelectionKey.OP_WRITE);
        }
    }

    private void clearWriteInterest(Session session) {
        if (session.key != null && (session.key.interestOps() & SelectionKey.OP_WRITE) != 0) {
            session.key.interestOps(session.key.interestOps() & ~SelectionKey.OP_WRITE);
        }
    }

    private void closeSession(Session session) {
        sessions[session.sessionId] = null;
        try {
            session.channel.close();
        } catch (IOException ignore) {
            // ignore
        }
        if (connectionsClosed != null) {
            connectionsClosed.inc();
        }
    }

    @Override
    public void close() {
        running = false;
        if (reactor != null && reactor.isAlive()) {
            try {
                reactor.join(2_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        try {
            if (server != null) {
                server.close();
            }
            if (selector != null) {
                selector.close();
            }
        } catch (IOException ignore) {
            // ignore
        }
    }

    /** Per-connection state: assigned id, the socket, and reusable read/write buffers. */
    private static final class Session {
        final int sessionId;
        final SocketChannel channel;
        final ByteBuffer readBuf = ByteBuffer.allocate(READ_BUF);
        final ByteBuffer out = ByteBuffer.allocateDirect(OUT_BUF);
        SelectionKey key;
        boolean dirty;

        Session(int sessionId, SocketChannel channel) {
            this.sessionId = sessionId;
            this.channel = channel;
        }
    }
}
