package exchange.metrics;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * The process-wide metrics registry: named counters, latency distributions, rate meters, gauges, and
 * the rolling VWAP/high-low windows.
 *
 * <h2>Why a registry with handles</h2>
 *
 * <p>Observability must not cost the hot path. Components therefore obtain a typed handle
 * <b>once</b> at construction ({@code Counter c = metrics.counter("trades")}) and then call
 * {@code c.inc()} — a single {@code LongAdder} increment with no string hashing or map lookup on the
 * order path. The management thread reads the same handles concurrently; every metric type is chosen
 * so a concurrent read is safe and only ever slightly stale, never blocking.
 *
 * <h2>What is here, and why</h2>
 * <ul>
 *   <li>{@link Counter} — monotonic totals (orders accepted/rejected, trades, bytes).</li>
 *   <li>{@link Latency} — full-reservoir nanosecond distributions (p50/p90/p99/p99.9/p99.99), the
 *       only honest way to talk about tail latency.</li>
 *   <li>{@link RateMeter} — events-per-second over 1s/5s/60s windows (throughput, not totals).</li>
 *   <li>{@link VwapWindow} — per-product trailing VWAP, high/low, and last price (the market-data
 *       numbers an operator actually watches).</li>
 *   <li>Gauges — values pulled at scrape time (ring backlog, live sessions, ring sizes).</li>
 * </ul>
 *
 * <p>This class is safe for concurrent use: all mutable state is either concurrent or single-writer
 * with tolerant readers.
 */
public final class Metrics {
    /** Latency samples above this are clamped (60 s is already pathological for an in-process hop). */
    public static final long DEFAULT_LATENCY_HIGHEST_NS = 60_000_000_000L;

    /** Three significant digits keeps p99.9/p99.99 meaningful without a huge histogram. */
    public static final int DEFAULT_LATENCY_SIG_DIGITS = 3;

    private final long startNs = System.nanoTime();
    private final Map<String, Counter> counters = new ConcurrentHashMap<>();
    private final Map<String, Latency> latencies = new ConcurrentHashMap<>();
    private final Map<String, RateMeter> rates = new ConcurrentHashMap<>();
    private final Map<String, LongSupplier> gauges = new ConcurrentHashMap<>();
    private final VwapWindow vwap;

    public Metrics(int maxProducts, int vwapWindowSeconds) {
        this.vwap = new VwapWindow(maxProducts, vwapWindowSeconds);
    }

    /** Get (or lazily create) the counter called {@code name}; hold the handle, don't re-look-up. */
    public Counter counter(String name) {
        return counters.computeIfAbsent(name, key -> new Counter());
    }

    public Latency latency(String name) {
        return latency(name, DEFAULT_LATENCY_HIGHEST_NS, DEFAULT_LATENCY_SIG_DIGITS);
    }

    public Latency latency(String name, long highestTrackableNs, int sigDigits) {
        return latencies.computeIfAbsent(name, key -> new Latency(highestTrackableNs, sigDigits));
    }

    public RateMeter rate(String name, int windowSeconds) {
        return rates.computeIfAbsent(name, key -> new RateMeter(windowSeconds));
    }

    /** Register a scrape-time gauge (e.g. ring backlog). The supplier runs on the management thread. */
    public void gauge(String name, LongSupplier supplier) {
        gauges.put(name, supplier);
    }

    /** The rolling VWAP/high-low windows shared by all products. */
    public VwapWindow vwap() {
        return vwap;
    }

    public long uptimeSeconds() {
        return (System.nanoTime() - startNs) / 1_000_000_000L;
    }

    public Map<String, Counter> counters() {
        return counters;
    }

    public Map<String, Latency> latencies() {
        return latencies;
    }

    public Map<String, RateMeter> rates() {
        return rates;
    }

    public Map<String, LongSupplier> gauges() {
        return gauges;
    }

    /** Every registered metric name, sorted — used by {@code /actuator/metrics}. */
    public List<String> names() {
        final TreeSet<String> all = new TreeSet<>();
        all.addAll(counters.keySet());
        all.addAll(latencies.keySet());
        all.addAll(rates.keySet());
        all.addAll(gauges.keySet());
        return new ArrayList<>(all);
    }
}
