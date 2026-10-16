package exchange.gateway;

import static exchange.engine.Codes.BUY;
import static exchange.engine.Codes.GTC;
import static exchange.engine.Codes.SELL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import exchange.engine.Codes;
import exchange.engine.EngineConfig;
import exchange.engine.EngineService;
import exchange.engine.Scales;
import exchange.wire.FastWire;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.function.Predicate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Realistic market-data test: one client trades, another connects purely to watch the market and
 * must observe the same prints and book. This exercises the full path a subscriber cares about —
 * matching → public trade tape ({@code Trade}) and top-of-book ({@code BookUpdate}) broadcast to
 * every session — over real TCP.
 */
class MarketDataIntegrationTest {

    private EngineService engine;
    private GatewayServer gateway;
    private int port;

    @BeforeEach
    void setUp() throws IOException {
        engine = new EngineService(EngineConfig.defaults());
        engine.start();
        gateway = new GatewayServer(0, engine.inbound(), engine.outbound());
        gateway.start();
        port = gateway.boundPort();
    }

    @AfterEach
    void tearDown() throws IOException {
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

    private static long mw(double m) {
        return Math.round(m * Scales.QTY_SCALE);
    }

    @Test
    @DisplayName("A passive subscriber sees the print at the matched (resting) price")
    @Timeout(10)
    void subscriberSeesTheMatchedPrice() throws IOException {
        try (Conn subscriber = new Conn(port); Conn trader = new Conn(port)) {
            subscriber.prime();

            // A producer rests an offer.
            trader.sendNewOrder(SELL, GTC, 50.00, 10);

            // The subscriber sees the offer appear in the public book.
            subscriber.readUntil(p -> subscriber.lastKind == FastWire.KIND_BOOK
                    && p.bookAskPrice == eur(50.00) && p.bookAskQty == mw(10));
            assertEquals(eur(50.00), subscriber.parser.bookAskPrice);
            assertEquals(mw(10), subscriber.parser.bookAskQty);

            // A consumer crosses it.
            trader.sendNewOrder(BUY, GTC, 55.00, 10);

            // The subscriber sees the public tape print at the resting price...
            subscriber.readUntil(p -> subscriber.lastKind == FastWire.KIND_TRADE
                    && p.tradePrice == eur(50.00));
            assertEquals(eur(50.00), subscriber.parser.tradePrice, "print at the resting price");
            assertEquals(mw(10), subscriber.parser.tradeQty);
            assertEquals(BUY, subscriber.parser.tradeAggressorSide, "the buyer was the aggressor");

            // ...and the aggressor gets a private fill report for the same trade.
            trader.readUntil(p -> p.hasFill && p.fillPrice == eur(50.00) && p.fillQty == mw(10));
            assertEquals(eur(50.00), trader.parser.fillPrice);
        }
    }

    @Test
    @DisplayName("A market sweep prints a ladder of trades on the tape")
    @Timeout(10)
    void sweepPrintsALadder() throws IOException {
        try (Conn subscriber = new Conn(port); Conn trader = new Conn(port)) {
            subscriber.prime();

            trader.sendNewOrder(SELL, GTC, 50.00, 10);
            trader.sendNewOrder(SELL, GTC, 50.10, 10);
            trader.sendNewOrder(SELL, GTC, 50.20, 10);
            trader.sendNewOrder(BUY, Codes.MARKET, GTC, 0, 25); // sweeps all three levels

            final long[] seen = new long[3];
            subscriber.readUntil(p -> subscriber.lastKind == FastWire.KIND_TRADE
                    && p.tradePrice == eur(50.00));
            seen[0] = subscriber.parser.tradePrice;
            subscriber.readUntil(p -> subscriber.lastKind == FastWire.KIND_TRADE
                    && p.tradePrice == eur(50.10));
            seen[1] = subscriber.parser.tradePrice;
            subscriber.readUntil(p -> subscriber.lastKind == FastWire.KIND_TRADE
                    && p.tradePrice == eur(50.20));
            seen[2] = subscriber.parser.tradePrice;

            assertEquals(List.of(eur(50.00), eur(50.10), eur(50.20)), List.of(seen[0], seen[1], seen[2]),
                    "prints walk up the resting asks");
            assertEquals(mw(5), subscriber.parser.tradeQty, "the last print is the 5 MW partial");
        }
    }

    /** A blocking, framed FastWire client (order sender + server-message reader). */
    private static final class Conn implements AutoCloseable {
        final SocketChannel ch;
        final FastWire.Encoder enc = new FastWire.Encoder();
        final FastWire.ServerParser parser = new FastWire.ServerParser();
        final ByteBuffer read = ByteBuffer.allocate(1 << 16);
        int lastKind = FastWire.KIND_NONE;
        long seq;

        Conn(int port) throws IOException {
            ch = SocketChannel.open();
            ch.configureBlocking(true);
            ch.connect(new InetSocketAddress("127.0.0.1", port));
            ch.socket().setSoTimeout(4000);
        }

        void sendNewOrder(byte side, int tif, double price, double qty) throws IOException {
            sendNewOrder(side, (byte) Codes.LIMIT, tif, price, qty);
        }

        void sendNewOrder(byte side, byte type, int tif, double price, double qty) throws IOException {
            final ByteBuffer payload = enc.newOrder(++seq, 1, side, type, tif,
                    Math.round(price * Scales.PRICE_SCALE), Math.round(qty * Scales.QTY_SCALE));
            final ByteBuffer frame = ByteBuffer.allocate(4 + payload.remaining());
            frame.putInt(payload.remaining()).put(payload).flip();
            while (frame.hasRemaining()) {
                ch.write(frame);
            }
        }

        /**
         * Confirm this session is registered with the gateway by sending an off-tick order (which
         * is rejected but acknowledged). After this we know broadcasts will reach us.
         */
        void prime() throws IOException {
            sendNewOrder(SELL, GTC, 50.05, 5);
            readUntil(p -> lastKind == FastWire.KIND_ACK);
            assertFalse(parser.accepted, "prime order must be rejected");
        }

        /** Read frames until {@code done} matches (blocking, with a socket timeout). */
        FastWire.ServerParser readUntil(Predicate<FastWire.ServerParser> done) throws IOException {
            while (true) {
                read.flip();
                while (read.remaining() >= 4) {
                    final int start = read.position();
                    final int len = read.getInt();
                    if (read.remaining() < len) {
                        read.position(start);
                        break;
                    }
                    lastKind = parser.parse(read, read.position(), len);
                    read.position(start + 4 + len);
                    if (done.test(parser)) {
                        read.compact();
                        return parser;
                    }
                }
                read.compact();
                final int n = ch.read(read);
                if (n < 0) {
                    throw new IOException("connection closed");
                }
            }
        }

        @Override
        public void close() throws IOException {
            ch.close();
        }
    }
}
