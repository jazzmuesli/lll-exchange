package exchange.bench;

import exchange.engine.Codes;
import exchange.engine.EngineConfig;
import exchange.engine.MatchingEngine;
import exchange.engine.Sink;
import org.HdrHistogram.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Microbenchmark of the matching kernel in isolation (no sockets, no rings), so we can attribute
 * latency to the data structures themselves.
 *
 * <p>Three workloads:
 * <ul>
 *   <li>{@code rest}  — passive GTC orders that build depth (insert-heavy)</li>
 *   <li>{@code cross} — IOC orders that cross and print (match-heavy)</li>
 *   <li>{@code mixed} — 50/50 blend</li>
 * </ul>
 *
 * <p>Usage: {@code bench [--ops 20000000] [--warmup 5000000] [--scenario mixed]}
 */
public final class MatchingBenchmark {
    private static final Logger LOG = LoggerFactory.getLogger(MatchingBenchmark.class);

    private MatchingBenchmark() {}

    public static void main(String[] args) {
        long ops = 20_000_000;
        long warmup = 5_000_000;
        String scenario = "mixed";
        for (int i = 0; i < args.length - 1; i++) {
            switch (args[i]) {
                case "--ops" -> ops = Long.parseLong(args[++i]);
                case "--warmup" -> warmup = Long.parseLong(args[++i]);
                case "--scenario" -> scenario = args[++i];
                default -> { }
            }
        }

        LOG.info("matching kernel benchmark: scenario={} warmup={} ops={}", scenario, warmup, ops);

        final EngineConfig cfg = EngineConfig.defaults();
        final MatchingEngine engine = new MatchingEngine(cfg, Sink.NOOP);

        run(engine, cfg, scenario, warmup, null);
        System.gc();
        final Histogram h = new Histogram(3);
        final long elapsed = run(engine, cfg, scenario, ops, h);

        LOG.info("==================== KERNEL RESULTS ====================");
        LOG.info("ops            : {}", ops);
        LOG.info("elapsed        : {} s", fmt("%.3f", elapsed / 1e9));
        LOG.info("throughput     : {} M ops/s", fmt("%.1f", ops / (elapsed / 1e9) / 1e6));
        LOG.info("avg            : {} ns/op", fmt("%.1f", (double) elapsed / ops));
        LOG.info("p50            : {} ns", h.getValueAtPercentile(50));
        LOG.info("p99            : {} ns", h.getValueAtPercentile(99));
        LOG.info("p99.9          : {} ns", h.getValueAtPercentile(99.9));
        LOG.info("p99.99         : {} ns", h.getValueAtPercentile(99.99));
        LOG.info("max            : {} ns", h.getMaxValue());
    }

    private static String fmt(String format, double value) {
        return String.format(format, value);
    }

    private static long run(MatchingEngine engine, EngineConfig cfg, String scenario,
                            long ops, Histogram h) {
        final java.util.Random rnd = new java.util.Random(42);
        final int mid = cfg.tickFor(5000); // 50.00 EUR/MWh
        final int products = 16;

        final boolean record = h != null;
        // Pre-generate to keep RNG out of the measured loop.
        final byte[] side = new byte[1 << 16];
        final byte[] tif = new byte[1 << 16];
        final int[] tick = new int[1 << 16];
        final int[] prod = new int[1 << 16];
        final long[] qty = new long[1 << 16];
        for (int i = 0; i < side.length; i++) {
            final int m = i & 1;
            switch (scenario) {
                case "rest" -> {
                    side[i] = m == 0 ? Codes.BUY : Codes.SELL;
                    tif[i] = Codes.GTC;
                    tick[i] = mid + (m == 0 ? -1 - (rnd.nextInt(20)) : 1 + rnd.nextInt(20));
                }
                case "cross" -> {
                    side[i] = m == 0 ? Codes.BUY : Codes.SELL;
                    tif[i] = Codes.IOC;
                    tick[i] = mid;
                }
                default -> {
                    side[i] = rnd.nextBoolean() ? Codes.BUY : Codes.SELL;
                    tif[i] = rnd.nextInt(10) < 5 ? Codes.IOC : Codes.GTC;
                    tick[i] = mid + rnd.nextInt(21) - 10;
                }
            }
            prod[i] = rnd.nextInt(products);
            qty[i] = 10L + rnd.nextInt(10) * 10L;
        }

        long seq = 0;
        long nanos = 0;
        final long t0 = System.nanoTime();
        for (long i = 0; i < ops; i++) {
            final int k = (int) (i & 0xFFFF);
            final long start = record ? System.nanoTime() : 0;
            engine.submit(1, i, prod[k], side[k], Codes.LIMIT, tif[k],
                    cfg.priceForTick(tick[k]), qty[k], 0L);
            if (record) {
                nanos = System.nanoTime() - start;
                h.recordValue(nanos);
            }
            seq++;
        }
        final long elapsed = System.nanoTime() - t0;
        return elapsed;
    }
}
