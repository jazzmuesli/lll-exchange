package exchange.loadgen;

import exchange.wire.FastWire;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import org.HdrHistogram.ConcurrentHistogram;
import org.HdrHistogram.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Open-loop load generator.
 *
 * <p>One non-blocking socket per connection, driven by a single thread that both paces sends
 * against a target rate (so we avoid coordinated omission) and drains responses. Round-trip
 * latency is measured from the monotonic send timestamp to the matching {@code OrderAck}.
 *
 * <p>Uses the hand-rolled {@link FastWire} codec, so the generator is not the bottleneck and the
 * measured latency reflects the exchange, not the test harness.
 */
public final class LoadGen {

    private static final Logger LOG = LoggerFactory.getLogger(LoadGen.class);

    private static final int SEND_INDEX_MASK = (1 << 20) - 1;

    private final String host;
    private final int port;
    private final int connections;
    private final long targetRate;   // orders/sec across all connections; 0 = as fast as possible
    private final long durationMs;
    private final int products;
    private final long maxInflightPerConn;
    private final int tifMix;        // percent of orders that are IOC (aggressive)

    private final ConcurrentHistogram latencyNs = new ConcurrentHistogram(3);
    private final AtomicLong sent = new AtomicLong();
    private final AtomicLong acked = new AtomicLong();
    private final AtomicLong errors = new AtomicLong();

    public LoadGen(String host, int port, int connections, long targetRate, long durationMs,
                   int products, long maxInflightPerConn, int tifMix) {
        this.host = host;
        this.port = port;
        this.connections = connections;
        this.targetRate = targetRate;
        this.durationMs = durationMs;
        this.products = products;
        this.maxInflightPerConn = maxInflightPerConn;
        this.tifMix = tifMix;
    }

    public void run() throws Exception {
        final long endNs = System.nanoTime() + durationMs * 1_000_000L;
        final CountDownLatch done = new CountDownLatch(connections);
        for (int c = 0; c < connections; c++) {
            final int id = c;
            final Thread t = new Thread(() -> {
                try {
                    connLoop(id, endNs);
                } catch (Exception e) {
                    errors.incrementAndGet();
                    LOG.warn("conn {} error", id, e);
                } finally {
                    done.countDown();
                }
            }, "loadgen-" + c);
            t.setDaemon(true);
            t.start();
        }
        final long start = System.nanoTime();
        while (done.getCount() > 0) {
            Thread.sleep(1000);
            final double secs = (System.nanoTime() - start) / 1e9;
            LOG.info("t={}s sent={} acked={} rate={}/s",
                    fmt("%.0f", secs), sent.get(), acked.get(), fmt("%.0f", sent.get() / secs));
        }
        report();
    }

    private void connLoop(int connId, long endNs) throws IOException {
        try (SocketChannel ch = SocketChannel.open()) {
            ch.configureBlocking(false);
            ch.setOption(StandardSocketOptions.TCP_NODELAY, true);
            ch.connect(new InetSocketAddress(host, port));
            while (!ch.finishConnect()) {
                Thread.onSpinWait();
            }

            final ByteBuffer readBuf = ByteBuffer.allocate(1 << 16);
            final ByteBuffer out = ByteBuffer.allocateDirect(1 << 16);
            final long[] sendTimes = new long[1 << 20];
            final FastWire.Encoder enc = new FastWire.Encoder();
            final FastWire.ServerParser parser = new FastWire.ServerParser();
            final Random rnd = new Random(0x9E3779B97F4A7C15L ^ connId);
            final long startNs = System.nanoTime();
            final double perConnRate = targetRate > 0 ? (double) targetRate / connections : 0;
            long seq = 0;
            long outstanding = 0;

            while (System.nanoTime() < endNs) {
                // drain responses first so we never inflate our own measured latency
                outstanding -= receive(ch, readBuf, sendTimes, parser);

                final long now = System.nanoTime();
                if (perConnRate > 0) {
                    final long due = (long) ((now - startNs) / 1e9 * perConnRate);
                    while (seq < due && outstanding < maxInflightPerConn) {
                        sendOrder(ch, seq, rnd, sendTimes, enc, out);
                        seq++;
                        outstanding++;
                    }
                    if (seq >= due) {
                        Thread.onSpinWait();
                    }
                } else {
                    while (outstanding < maxInflightPerConn) {
                        sendOrder(ch, seq, rnd, sendTimes, enc, out);
                        seq++;
                        outstanding++;
                    }
                }
            }
        }
    }

    private void sendOrder(SocketChannel ch, long seq, Random rnd, long[] sendTimes,
                           FastWire.Encoder enc, ByteBuffer out) throws IOException {
        final int product = rnd.nextInt(products);
        final boolean buy = rnd.nextBoolean();
        final long mid = 5000L + rnd.nextInt(41) - 20; // around 50.00 EUR/MWh
        final long price = (mid / 10) * 10;            // snap to 0.10 grid
        final long qty = 10L + rnd.nextInt(10) * 10L;  // 1..10 MW
        final int tif = rnd.nextInt(100) < tifMix ? 2 /*IOC*/ : 1 /*GTC*/;

        final ByteBuffer payload = enc.newOrder(seq, product, buy ? 1 : 2, 1 /*LIMIT*/, tif, price, qty);
        out.clear();
        out.putInt(payload.remaining());
        out.put(payload);
        out.flip();

        sendTimes[(int) (seq & SEND_INDEX_MASK)] = System.nanoTime();
        while (out.hasRemaining()) {
            ch.write(out);
        }
        sent.incrementAndGet();
    }

    private int receive(SocketChannel ch, ByteBuffer buf, long[] sendTimes, FastWire.ServerParser parser)
            throws IOException {
        int acks = 0;
        final int n = ch.read(buf);
        if (n > 0) {
            buf.flip();
            while (buf.remaining() >= 4) {
                final int len = buf.getInt();
                if (buf.remaining() < len) {
                    buf.position(buf.position() - 4);
                    break;
                }
                final int kind = parser.parse(buf, buf.position(), len);
                if (kind == FastWire.KIND_ACK) {
                    final long coid = parser.clientOrderId;
                    final long t0 = sendTimes[(int) (coid & SEND_INDEX_MASK)];
                    if (t0 != 0) {
                        final long now = System.nanoTime();
                        final long lat = now - t0;
                        if (lat >= 0 && lat < 3_000_000_000L) {
                            latencyNs.recordValue(lat);
                        }
                        acked.incrementAndGet();
                        acks++;
                    }
                }
            }
            buf.compact();
        }
        return acks;
    }

    private void report() {
        final Histogram h = latencyNs.copy();
        LOG.info("==================== LOADGEN RESULTS ====================");
        LOG.info("sent              : {}", sent.get());
        LOG.info("acked             : {}", acked.get());
        LOG.info("errors            : {}", errors.get());
        if (h.getTotalCount() > 0) {
            LOG.info("round-trip order->ack latency (ns):");
            LOG.info("  min   : {}", h.getMinValue());
            LOG.info("  p50   : {}", h.getValueAtPercentile(50));
            LOG.info("  p90   : {}", h.getValueAtPercentile(90));
            LOG.info("  p99   : {}", h.getValueAtPercentile(99));
            LOG.info("  p99.9 : {}", h.getValueAtPercentile(99.9));
            LOG.info("  p99.99: {}", h.getValueAtPercentile(99.99));
            LOG.info("  max   : {}", h.getMaxValue());
            LOG.info("  mean  : {}", fmt("%.0f", h.getMean()));
        }
    }

    private static String fmt(String format, double value) {
        return String.format(format, value);
    }

    // ------------------------------------------------------------------ main

    public static void main(String[] args) throws Exception {
        String host = "127.0.0.1";
        int port = 9001;
        int conns = 4;
        long rate = 200_000;
        long duration = 10_000;
        int products = 16;
        long maxInflight = 512;
        int tifMix = 30;

        for (int i = 0; i < args.length - 1; i++) {
            switch (args[i]) {
                case "--host" -> host = args[++i];
                case "--port" -> port = Integer.parseInt(args[++i]);
                case "--conns" -> conns = Integer.parseInt(args[++i]);
                case "--rate" -> rate = Long.parseLong(args[++i]);
                case "--duration" -> duration = Long.parseLong(args[++i]);
                case "--products" -> products = Integer.parseInt(args[++i]);
                case "--inflight" -> maxInflight = Long.parseLong(args[++i]);
                case "--tifmix" -> tifMix = Integer.parseInt(args[++i]);
                default -> { }
            }
        }

        LOG.info("loadgen -> {}:{} conns={} rate={}/s duration={}ms products={} inflight={}",
                host, port, conns, rate, duration, products, maxInflight);
        new LoadGen(host, port, conns, rate, duration, products, maxInflight, tifMix).run();
    }
}
