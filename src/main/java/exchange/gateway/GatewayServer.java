package exchange.gateway;

import exchange.wire.FastWire;
import exchange.wire.MsgType;
import java.io.IOException;
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

    public GatewayServer(int port, ManyToOneRingBuffer inbound, OneToOneRingBuffer outbound) {
        this.port = port;
        this.inbound = inbound;
        this.outbound = outbound;
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
                final Iterator<SelectionKey> it = selector.selectedKeys().iterator();
                while (it.hasNext()) {
                    final SelectionKey key = it.next();
                    it.remove();
                    try {
                        if (!key.isValid()) {
                            continue;
                        }
                        if (key.isAcceptable()) {
                            accept();
                        } else {
                            final Session s = (Session) key.attachment();
                            if (key.isReadable()) {
                                read(s);
                            }
                            if (key.isValid() && key.isWritable()) {
                                flush(s);
                            }
                        }
                    } catch (IOException e) {
                        final Object a = key.attachment();
                        if (a instanceof Session s) {
                            closeSession(s);
                        }
                    }
                }
            }
            drainOutbound();
            flushDirty();
            Thread.onSpinWait();
        }
    }

    private void accept() throws IOException {
        SocketChannel ch;
        while ((ch = server.accept()) != null) {
            ch.configureBlocking(false);
            ch.setOption(StandardSocketOptions.TCP_NODELAY, true);
            if (nextSessionId >= MAX_SESSIONS) {
                ch.close();
                continue;
            }
            final int id = nextSessionId++;
            final Session s = new Session(id, ch);
            sessions[id] = s;
            s.key = ch.register(selector, SelectionKey.OP_READ, s);
        }
    }

    private void read(Session s) throws IOException {
        final int n = s.ch.read(s.readBuf);
        if (n < 0) {
            closeSession(s);
            return;
        }
        if (n == 0) {
            return;
        }
        s.readBuf.flip();
        try {
            while (s.readBuf.remaining() >= 4) {
                final int len = s.readBuf.getInt();
                if (len < 0 || len > MAX_FRAME) {
                    closeSession(s);
                    return;
                }
                if (s.readBuf.remaining() < len) {
                    s.readBuf.position(s.readBuf.position() - 4);
                    break;
                }
                final int payloadStart = s.readBuf.position();
                final int kind = clientParser.parse(s.readBuf, payloadStart, len);
                dispatch(s, kind);
            }
        } finally {
            s.readBuf.compact();
        }
    }

    private void dispatch(Session s, int kind) {
        switch (kind) {
            case FastWire.CK_NEW_ORDER -> {
                final MutableDirectBuffer b = scratch;
                int i = 0;
                b.putInt(i, s.id); i += 4;
                b.putLong(i, clientParser.clientOrderId); i += 8;
                b.putInt(i, clientParser.productId); i += 4;
                b.putByte(i, (byte) clientParser.side); i += 1;
                b.putByte(i, (byte) clientParser.type); i += 1;
                b.putByte(i, (byte) clientParser.tif); i += 1;
                b.putLong(i, clientParser.price); i += 8;
                b.putLong(i, clientParser.quantity); i += 8;
                b.putLong(i, System.nanoTime()); i += 8;
                inbound.write(MsgType.NEW_ORDER, b, 0, i);
            }
            case FastWire.CK_CANCEL -> {
                final MutableDirectBuffer b = scratch;
                int i = 0;
                b.putInt(i, s.id); i += 4;
                b.putInt(i, (int) clientParser.exchangeOrderId); i += 4;
                b.putLong(i, System.nanoTime()); i += 8;
                inbound.write(MsgType.CANCEL, b, 0, i);
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

        final Session s = sessions[sessionId];
        if (s == null) {
            return;
        }
        append(s, serverEncoder.ack(clientOrderId, orderId, reason == 0,
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

        final Session s = sessions[sessionId];
        if (s == null) {
            return;
        }
        append(s, serverEncoder.exec(clientOrderId, orderId, productId, exchange.engine.Codes.ST_PARTIAL,
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

        final Session s = sessions[sessionId];
        if (s == null) {
            return;
        }
        append(s, serverEncoder.exec(clientOrderId, orderId, productId, status,
                leaves, filled, false, 0, 0, 0, 0, ts));
    }

    private void onBook(MutableDirectBuffer buf, int index) {
        int i = index;
        final int productId = buf.getInt(i); i += 4;
        final long bidPrice = buf.getLong(i); i += 8;
        final long bidQty = buf.getLong(i); i += 8;
        final long askPrice = buf.getLong(i); i += 8;
        final long askQty = buf.getLong(i); i += 8;
        final long ts = buf.getLong(i);

        final ByteBuffer p = serverEncoder.book(productId, bidPrice, bidQty, askPrice, askQty, ts);
        for (int id = 1; id < nextSessionId; id++) {
            final Session s = sessions[id];
            if (s != null) {
                append(s, p);
            }
        }
    }

    private void append(Session s, ByteBuffer payload) {
        final int need = 4 + payload.remaining();
        if (s.out.remaining() < need) {
            flush(s);
            if (s.out.remaining() < need) {
                droppedOutbound++;
                return;
            }
        }
        // payload may be shared (broadcast), so consume without disturbing its position
        final int start = payload.position();
        final int limit = payload.limit();
        s.out.putInt(limit - start);
        s.out.put(payload);
        payload.position(start);
        payload.limit(limit);
        if (!s.dirty) {
            s.dirty = true;
            dirty.add(s);
        }
    }

    private void flushDirty() {
        for (int k = 0; k < dirty.size(); k++) {
            final Session s = dirty.get(k);
            s.dirty = false;
            flush(s);
        }
        dirty.clear();
    }

    private void flush(Session s) {
        if (s.out.position() == 0) {
            clearWriteInterest(s);
            return;
        }
        s.out.flip();
        try {
            s.ch.write(s.out);
        } catch (IOException e) {
            closeSession(s);
            return;
        }
        if (s.out.hasRemaining()) {
            addWriteInterest(s);
        } else {
            clearWriteInterest(s);
        }
        s.out.compact();
    }

    private void addWriteInterest(Session s) {
        if (s.key != null && (s.key.interestOps() & SelectionKey.OP_WRITE) == 0) {
            s.key.interestOps(s.key.interestOps() | SelectionKey.OP_WRITE);
        }
    }

    private void clearWriteInterest(Session s) {
        if (s.key != null && (s.key.interestOps() & SelectionKey.OP_WRITE) != 0) {
            s.key.interestOps(s.key.interestOps() & ~SelectionKey.OP_WRITE);
        }
    }

    private void closeSession(Session s) {
        sessions[s.id] = null;
        try {
            s.ch.close();
        } catch (IOException ignore) {
            // ignore
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

    /** Per-connection state. */
    private static final class Session {
        final int id;
        final SocketChannel ch;
        final ByteBuffer readBuf = ByteBuffer.allocate(READ_BUF);
        final ByteBuffer out = ByteBuffer.allocateDirect(OUT_BUF);
        SelectionKey key;
        boolean dirty;

        Session(int id, SocketChannel ch) {
            this.id = id;
            this.ch = ch;
        }
    }
}
