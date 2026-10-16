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
                            final Session session = (Session) key.attachment();
                            if (key.isReadable()) {
                                read(session);
                            }
                            if (key.isValid() && key.isWritable()) {
                                flush(session);
                            }
                        }
                    } catch (IOException e) {
                        final Object a = key.attachment();
                        if (a instanceof Session session) {
                            closeSession(session);
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
        }
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
            }
            case FastWire.CK_CANCEL -> {
                final MutableDirectBuffer b = scratch;
                int i = 0;
                b.putInt(i, session.sessionId); i += 4;
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
        try {
            session.channel.write(session.out);
        } catch (IOException e) {
            closeSession(session);
            return;
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
