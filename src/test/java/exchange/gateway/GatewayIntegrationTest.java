package exchange.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import exchange.engine.EngineConfig;
import exchange.engine.EngineService;
import exchange.engine.Scales;
import exchange.wire.FastWire;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * End-to-end acceptance test: a real TCP client speaks length-prefixed protobuf to the gateway, the
 * single matching thread processes orders, and results come back as protobuf. Exercises the whole
 * path (codec, ring buffers, engine, egress).
 */
class GatewayIntegrationTest {

    private EngineService engine;
    private GatewayServer gateway;
    private SocketChannel client;
    private FastWire.Encoder enc;
    private FastWire.ServerParser parser;
    private ByteBuffer readBuf;
    private int clientSeq;
    private int lastKind;

    @BeforeEach
    void setUp() throws IOException {
        engine = new EngineService(EngineConfig.defaults());
        engine.start();
        gateway = new GatewayServer(0, engine.inbound(), engine.outbound());
        gateway.start();
        client = SocketChannel.open();
        client.connect(new InetSocketAddress("127.0.0.1", gateway.boundPort()));
        client.configureBlocking(true);
        client.socket().setSoTimeout(2000);
        enc = new FastWire.Encoder();
        parser = new FastWire.ServerParser();
        readBuf = ByteBuffer.allocate(1 << 16);
        clientSeq = 0;
    }

    @AfterEach
    void tearDown() throws IOException {
        if (client != null) {
            client.close();
        }
        if (gateway != null) {
            gateway.close();
        }
        if (engine != null) {
            engine.close();
        }
    }

    private static long eur(double e) {
        return Math.round(e * Scales.PRICE_SCALE);
    }

    private void sendOrder(byte side, int tif, double price, double qty) throws IOException {
        final ByteBuffer payload = enc.newOrder(++clientSeq, 1, side, 1, tif, eur(price),
                Math.round(qty * Scales.QTY_SCALE));
        final ByteBuffer frame = ByteBuffer.allocate(4 + payload.remaining());
        frame.putInt(payload.remaining()).put(payload).flip();
        while (frame.hasRemaining()) {
            client.write(frame);
        }
    }

    /** Read frames until {@code predicate} matches or the socket times out. */
    private FastWire.ServerParser await(java.util.function.Predicate<FastWire.ServerParser> done)
            throws IOException {
        final ByteBuffer one = ByteBuffer.allocate(1 << 16);
        while (true) {
            final int n = client.read(one);
            if (n < 0) {
                throw new IOException("closed");
            }
            if (n == 0) {
                continue;
            }
            one.flip();
            while (one.remaining() >= 4) {
                final int len = one.getInt();
                if (one.remaining() < len) {
                    one.position(one.position() - 4);
                    break;
                }
                lastKind = parser.parse(one, one.position(), len);
                if (done.test(parser)) {
                    return parser;
                }
            }
            one.compact();
        }
    }

    @Test
    @Timeout(10)
    void orderGetsAcknowledged() throws IOException {
        sendOrder((byte) 1, 1, 50.00, 5);
        final FastWire.ServerParser p = await(x -> lastKind == FastWire.KIND_ACK);
        assertEquals(FastWire.KIND_ACK, lastKind);
        assertTrue(p.accepted);
        assertEquals(1L, p.clientOrderId);
    }

    @Test
    @Timeout(10)
    void crossingOrdersProduceAFill() throws IOException {
        sendOrder((byte) 2, 1, 50.00, 5);   // sell rests
        sendOrder((byte) 1, 1, 55.00, 5);   // buy crosses -> fill at 50.00

        // collect until we see a fill for order #2
        final boolean[] filled = {false};
        final long[] fillPrice = {0};
        await(p -> {
            if (p.hasFill && p.fillQty > 0) {
                filled[0] = true;
                fillPrice[0] = p.fillPrice;
            }
            return filled[0];
        });
        assertTrue(filled[0], "expected a fill");
        assertEquals(eur(50.00), fillPrice[0], "trade prints at the resting price");
    }

    @Test
    @Timeout(10)
    void rejectedOrderComesBackRejected() throws IOException {
        sendOrder((byte) 1, 1, 50.05, 5); // off the 0.10 tick grid
        final FastWire.ServerParser p = await(x -> lastKind == FastWire.KIND_ACK);
        assertTrue(!p.accepted, "off-tick order must be rejected");
    }
}
