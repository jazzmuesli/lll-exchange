package exchange.metrics;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import exchange.engine.Codes;
import exchange.engine.MatchingEngine;
import exchange.engine.Scales;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.function.LongSupplier;

/**
 * A small, dependency-free HTTP management/observability server (the "actuator") for the exchange.
 *
 * <h2>Why hand-rolled instead of Spring Boot Actuator</h2>
 *
 * <p>Spring Boot would drag an application container, a DI graph, and a lot of startup work into a
 * process whose entire point is a nanosecond-sensitive single matching thread. The endpoints and
 * JSON shapes here deliberately mirror Spring Boot Actuator conventions ({@code /actuator/health},
 * {@code /actuator/metrics}, {@code /actuator/metrics/{name}} with {@code measurements}) so existing
 * tooling and habits transfer, while the implementation reuses only the JDK's built-in HTTP server.
 *
 * <p>The server runs on its own daemon thread pool and only ever <em>reads</em> metrics that the
 * trading threads publish, so scraping cannot perturb the hot path beyond a few atomic reads.
 *
 * <h2>Endpoints</h2>
 * <ul>
 *   <li>{@code GET /actuator} — index of links.</li>
 *   <li>{@code GET /actuator/health} — liveness + uptime.</li>
 *   <li>{@code GET /actuator/metrics} — every metric name.</li>
 *   <li>{@code GET /actuator/metrics/{name}} — one metric, Actuator-style measurements.</li>
 *   <li>{@code GET /actuator/latency} — p50/p90/p99/p99.9/p99.99 for every latency histogram.</li>
 *   <li>{@code GET /actuator/orders} — order flow: submitted/processed/accepted/rejected/status.</li>
 *   <li>{@code GET /actuator/trades} — trade counts, volume, notional, VWAP, per product.</li>
 *   <li>{@code GET /actuator/vwap?window=300&amp;product=0} — trailing-window VWAP (default 5 min).</li>
 *   <li>{@code GET /actuator/books} — live top of book per product (best bid/ask, spread, mid).</li>
 *   <li>{@code GET /actuator/throughput} — orders/trades/bytes per second over 1s/5s/60s.</li>
 *   <li>{@code GET /actuator/jvm} — heap, GC, threads, classes.</li>
 *   <li>{@code GET /actuator/prometheus} — the same numbers in Prometheus text exposition format.</li>
 * </ul>
 *
 * <p>Binds to loopback only: the management plane must not be exposed to the trading network.
 */
public final class ManagementServer implements AutoCloseable {
    private static final String PREFIX = "/actuator";

    private final int port;
    private final Metrics metrics;
    private final MatchingEngine engine; // may be null (metrics-only deployments / tests)

    private HttpServer server;

    public ManagementServer(int port, Metrics metrics, MatchingEngine engine) {
        this.port = port;
        this.metrics = metrics;
        this.engine = engine;
    }

    public void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), port), 0);
        server.createContext("/", this::route);
        server.setExecutor(Executors.newFixedThreadPool(2, runnable -> {
            final Thread t = new Thread(runnable, "management-http");
            t.setDaemon(true);
            return t;
        }));
        server.start();
    }

    /** Actual bound port (useful when constructed with port 0 in tests). */
    public int boundPort() {
        return server == null ? port : server.getAddress().getPort();
    }

    @Override
    public void close() {
        if (server != null) {
            server.stop(0);
        }
    }

    // ----------------------------------------------------------------- routing

    private void route(HttpExchange exchange) throws IOException {
        try {
            if (!"GET".equals(exchange.getRequestMethod())) {
                sendJson(exchange, 405, "{\"error\":\"method not allowed\"}");
                return;
            }
            final String path = exchange.getRequestURI().getPath();
            switch (path) {
                case "/", PREFIX, PREFIX + "/" -> sendJson(exchange, 200, indexJson());
                case PREFIX + "/health" -> sendJson(exchange, 200, healthJson());
                case PREFIX + "/metrics" -> sendJson(exchange, 200, metricNamesJson());
                case PREFIX + "/latency" -> sendJson(exchange, 200, latencyJson());
                case PREFIX + "/orders" -> sendJson(exchange, 200, ordersJson());
                case PREFIX + "/trades" -> sendJson(exchange, 200, tradesJson());
                case PREFIX + "/vwap" -> sendJson(exchange, 200, vwapJson(query(exchange)));
                case PREFIX + "/books" -> sendJson(exchange, 200, booksJson());
                case PREFIX + "/throughput" -> sendJson(exchange, 200, throughputJson());
                case PREFIX + "/jvm" -> sendJson(exchange, 200, jvmJson());
                case PREFIX + "/prometheus" -> sendText(exchange, 200, prometheus());
                default -> {
                    if (path.startsWith(PREFIX + "/metrics/")) {
                        final String name = path.substring((PREFIX + "/metrics/").length());
                        sendJson(exchange, 200, oneMetricJson(name));
                    } else {
                        sendJson(exchange, 404, "{\"error\":\"not found\"}");
                    }
                }
            }
        } catch (RuntimeException failure) {
            sendJson(exchange, 500, "{\"error\":\"internal\",\"detail\":\""
                    + Json.escapeForError(failure.getClass().getSimpleName()) + "\"}");
        }
    }

    // ----------------------------------------------------------------- endpoints

    private String indexJson() {
        final Json j = new Json();
        j.beginObject();
        j.name("_links").beginObject();
        for (String link : List.of("health", "metrics", "latency", "orders", "trades", "vwap",
                "books", "throughput", "jvm", "prometheus")) {
            j.name(link).beginObject().field("href", PREFIX + "/" + link).endObject();
        }
        j.endObject();
        j.field("uptimeSeconds", metrics.uptimeSeconds());
        j.endObject();
        return j.toString();
    }

    private String healthJson() {
        final Json j = new Json();
        j.beginObject();
        j.field("status", "UP");
        j.field("uptimeSeconds", metrics.uptimeSeconds());
        final LongSupplier fast = metrics.gauges().get("gateway.fast_selector");
        if (fast != null) {
            j.field("fastSelector", fast.getAsLong() == 1);
        }
        j.endObject();
        return j.toString();
    }

    private String metricNamesJson() {
        final Json j = new Json();
        j.beginObject();
        j.name("names").beginArray();
        for (String name : metrics.names()) {
            j.value(name);
        }
        j.endArray();
        j.endObject();
        return j.toString();
    }

    private String oneMetricJson(String name) {
        final Json j = new Json();
        j.beginObject();
        j.field("name", name);

        final Counter counter = metrics.counters().get(name);
        if (counter != null) {
            j.name("measurements").beginArray();
            measurement(j, "COUNT", counter.sum());
            j.endArray();
            j.endObject();
            return j.toString();
        }

        final Latency latency = metrics.latencies().get(name);
        if (latency != null) {
            final Latency.Snapshot s = latency.snapshot();
            j.field("baseUnit", "nanoseconds");
            j.name("measurements").beginArray();
            measurement(j, "COUNT", s.count());
            measurement(j, "MEAN", s.mean());
            measurement(j, "MAX", s.max());
            measurement(j, "0.5", s.p50());
            measurement(j, "0.9", s.p90());
            measurement(j, "0.99", s.p99());
            measurement(j, "0.999", s.p999());
            measurement(j, "0.9999", s.p9999());
            j.endArray();
            j.endObject();
            return j.toString();
        }

        final RateMeter rate = metrics.rates().get(name);
        if (rate != null) {
            j.field("baseUnit", "events");
            final long now = System.nanoTime();
            j.name("measurements").beginArray();
            measurement(j, "COUNT", rate.sumOver(now, 60));
            measurement(j, "PER_SECOND", rate.ratePerSecond(now, 60));
            j.endArray();
            j.endObject();
            return j.toString();
        }

        final LongSupplier gauge = metrics.gauges().get(name);
        if (gauge != null) {
            j.name("measurements").beginArray();
            measurement(j, "VALUE", gauge.getAsLong());
            j.endArray();
            j.endObject();
            return j.toString();
        }

        return "{\"error\":\"unknown metric: " + Json.escapeForError(name) + "\"}";
    }

    private String latencyJson() {
        final Json j = new Json();
        j.beginObject();
        j.field("baseUnit", "nanoseconds");
        for (Map.Entry<String, Latency> entry : metrics.latencies().entrySet()) {
            final Latency.Snapshot s = entry.getValue().snapshot();
            j.name(entry.getKey()).beginObject();
            j.field("count", s.count());
            j.field("meanNs", round(s.mean(), 1));
            j.field("minNs", s.min());
            j.field("maxNs", s.max());
            j.field("p50Ns", s.p50());
            j.field("p90Ns", s.p90());
            j.field("p99Ns", s.p99());
            j.field("p999Ns", s.p999());
            j.field("p9999Ns", s.p9999());
            j.endObject();
        }
        j.endObject();
        return j.toString();
    }

    private String ordersJson() {
        final Json j = new Json();
        j.beginObject();
        j.field("submitted", counter("orders.submitted"));
        j.field("processed", counter("orders.processed"));
        j.field("accepted", counter("orders.accepted"));
        j.field("rejected", counter("orders.rejected"));
        j.field("fills", counter("fills"));
        j.field("fillQty", counter("fill.qty"));
        j.field("cancelsSubmitted", counter("cancels.submitted"));
        j.field("cancelsProcessed", counter("cancels.processed"));

        j.name("rejectedByReason").beginObject();
        for (int code = 1; code < 16; code++) {
            final String text = Codes.reasonText(code);
            final long value = counter("orders.rejected." + MetricsSink.slug(text));
            if (value > 0) {
                j.field(text, value);
            }
        }
        j.endObject();

        j.name("status").beginObject();
        for (byte code = 1; code < 16; code++) {
            final String name = MetricsSink.statusName(code);
            final long value = counter("orders.status." + name);
            if (value > 0) {
                j.field(name, value);
            }
        }
        j.endObject();

        j.endObject();
        return j.toString();
    }

    private String tradesJson() {
        final long count = counter("trades");
        final long qty = counter("trade.qty");
        final long notional = counter("trade.notional");

        final Json j = new Json();
        j.beginObject();
        j.field("count", count);
        j.field("buy", counter("trades.buy"));
        j.field("sell", counter("trades.sell"));
        j.field("aggressorBalance", round(balance(counter("trades.buy"), counter("trades.sell")), 4));
        j.field("volumeQty", qty);
        j.field("volumeMWh", round(mwh(qty), 4));
        j.field("notionalScaled", notional);
        j.field("notionalEur", round(eur(notional), 2));
        j.field("avgTradeMWh", round(count == 0 ? 0 : mwh(qty) / count, 4));
        j.field("vwapEurPerMwh", round(vwapEur(notional, qty), 4));
        j.field("ratePerSecond", round(rateValue("trades.rate", 60), 2));
        j.name("products").beginArray();
        writeProductTotals(j);
        j.endArray();
        j.endObject();
        return j.toString();
    }

    private String vwapJson(Map<String, String> query) {
        final int maxWindow = metrics.vwap().windowSeconds();
        final int window = Math.min(Math.max(1, parseInt(query.get("window"), maxWindow)), maxWindow);
        final long now = System.nanoTime();
        final Json j = new Json();
        j.beginObject();
        j.field("windowSeconds", Math.min(Math.max(1, window), metrics.vwap().windowSeconds()));
        j.field("nowNs", now);

        final String productParam = query.get("product");
        if (productParam != null) {
            final int productId = parseInt(productParam, 0);
            j.name("product");
            writeVwapWindow(j, productId, now, window);
        } else {
            j.name("products").beginArray();
            final int max = metrics.vwap().maxProducts();
            for (int productId = 0; productId < max; productId++) {
                final VwapWindow.Totals totals = metrics.vwap().totals(productId);
                if (totals.trades() > 0) {
                    writeVwapWindow(j, productId, now, window);
                }
            }
            j.endArray();
        }
        j.endObject();
        return j.toString();
    }

    private String booksJson() {
        final Json j = new Json();
        j.beginObject();
        j.name("products").beginArray();
        if (engine != null) {
            final int max = engine.config().maxProducts();
            for (int productId = 0; productId < max; productId++) {
                final long bid = engine.bestBidPrice(productId);
                final long bidQty = engine.bestBidQty(productId);
                final long ask = engine.bestAskPrice(productId);
                final long askQty = engine.bestAskQty(productId);
                if (bid == 0 && ask == 0) {
                    continue;
                }
                j.beginObject();
                j.field("productId", productId);
                j.field("bid", Scales.priceToString(bid));
                j.field("bidQty", round(mwh(bidQty), 4));
                j.field("ask", Scales.priceToString(ask));
                j.field("askQty", round(mwh(askQty), 4));
                j.field("spread", bid == 0 || ask == 0 ? 0 : round((ask - bid) / (double) Scales.PRICE_SCALE, 4));
                j.field("mid", bid == 0 || ask == 0 ? 0
                        : round(((bid + ask) / 2.0) / Scales.PRICE_SCALE, 4));
                j.endObject();
            }
        }
        j.endArray();
        j.endObject();
        return j.toString();
    }

    private String throughputJson() {
        final Json j = new Json();
        j.beginObject();
        j.name("ordersPerSecond").beginObject();
        j.field("1s", round(rateValue("orders.rate", 1), 2));
        j.field("5s", round(rateValue("orders.rate", 5), 2));
        j.field("60s", round(rateValue("orders.rate", 60), 2));
        j.endObject();
        j.name("tradesPerSecond").beginObject();
        j.field("1s", round(rateValue("trades.rate", 1), 2));
        j.field("5s", round(rateValue("trades.rate", 5), 2));
        j.field("60s", round(rateValue("trades.rate", 60), 2));
        j.endObject();
        j.name("bytesReadPerSecond").beginObject();
        j.field("1s", round(rateValue("gateway.bytes.read.rate", 1), 2));
        j.field("5s", round(rateValue("gateway.bytes.read.rate", 5), 2));
        j.field("60s", round(rateValue("gateway.bytes.read.rate", 60), 2));
        j.endObject();
        j.name("bytesWrittenPerSecond").beginObject();
        j.field("1s", round(rateValue("gateway.bytes.written.rate", 1), 2));
        j.field("5s", round(rateValue("gateway.bytes.written.rate", 5), 2));
        j.field("60s", round(rateValue("gateway.bytes.written.rate", 60), 2));
        j.endObject();
        j.endObject();
        return j.toString();
    }

    private String jvmJson() {
        final MemoryUsage heap = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        final MemoryUsage nonHeap = ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage();
        long gcCount = 0;
        long gcTimeMs = 0;
        for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
            if (gc.getCollectionCount() > 0) {
                gcCount += gc.getCollectionCount();
            }
            if (gc.getCollectionTime() > 0) {
                gcTimeMs += gc.getCollectionTime();
            }
        }
        final Json j = new Json();
        j.beginObject();
        j.field("uptimeSeconds", metrics.uptimeSeconds());
        j.field("processUptimeMs", ManagementFactory.getRuntimeMXBean().getUptime());
        j.field("availableProcessors", Runtime.getRuntime().availableProcessors());
        j.name("heap").beginObject();
        j.field("usedBytes", heap.getUsed());
        j.field("committedBytes", heap.getCommitted());
        j.field("maxBytes", heap.getMax());
        j.endObject();
        j.name("nonHeap").beginObject();
        j.field("usedBytes", nonHeap.getUsed());
        j.field("committedBytes", nonHeap.getCommitted());
        j.endObject();
        j.name("gc").beginObject();
        j.field("collections", gcCount);
        j.field("timeMs", gcTimeMs);
        j.endObject();
        j.name("threads").beginObject();
        j.field("count", ManagementFactory.getThreadMXBean().getThreadCount());
        j.field("peak", ManagementFactory.getThreadMXBean().getPeakThreadCount());
        j.endObject();
        j.field("classesLoaded", ManagementFactory.getClassLoadingMXBean().getLoadedClassCount());
        j.endObject();
        return j.toString();
    }

    private String prometheus() {
        final StringBuilder out = new StringBuilder(2048);
        out.append("# lowlat exchange metrics\n");
        out.append("lowlat_uptime_seconds ").append(metrics.uptimeSeconds()).append('\n');

        for (Map.Entry<String, Counter> entry : metrics.counters().entrySet()) {
            final String name = "lowlat_" + sanitize(entry.getKey()) + "_total";
            out.append("# TYPE ").append(name).append(" counter\n");
            out.append(name).append(' ').append(entry.getValue().sum()).append('\n');
        }
        for (Map.Entry<String, LongSupplier> entry : metrics.gauges().entrySet()) {
            final String name = "lowlat_" + sanitize(entry.getKey());
            out.append("# TYPE ").append(name).append(" gauge\n");
            out.append(name).append(' ').append(entry.getValue().getAsLong()).append('\n');
        }
        for (Map.Entry<String, RateMeter> entry : metrics.rates().entrySet()) {
            final String name = "lowlat_" + sanitize(entry.getKey()) + "_per_second";
            out.append("# TYPE ").append(name).append(" gauge\n");
            out.append(name).append(' ')
                    .append(round(entry.getValue().ratePerSecond(System.nanoTime(), 60), 3)).append('\n');
        }
        for (Map.Entry<String, Latency> entry : metrics.latencies().entrySet()) {
            final String base = "lowlat_" + sanitize(entry.getKey()) + "_ns";
            final Latency.Snapshot s = entry.getValue().snapshot();
            out.append("# TYPE ").append(base).append(" gauge\n");
            out.append(base).append("_count ").append(s.count()).append('\n');
            out.append(base).append("{quantile=\"0.5\"} ").append(s.p50()).append('\n');
            out.append(base).append("{quantile=\"0.9\"} ").append(s.p90()).append('\n');
            out.append(base).append("{quantile=\"0.99\"} ").append(s.p99()).append('\n');
            out.append(base).append("{quantile=\"0.999\"} ").append(s.p999()).append('\n');
            out.append(base).append("{quantile=\"0.9999\"} ").append(s.p9999()).append('\n');
        }
        return out.toString();
    }

    // ----------------------------------------------------------------- helpers

    private void writeProductTotals(Json j) {
        final int max = metrics.vwap().maxProducts();
        for (int productId = 0; productId < max; productId++) {
            final VwapWindow.Totals t = metrics.vwap().totals(productId);
            if (t.trades() == 0) {
                continue;
            }
            j.beginObject();
            j.field("productId", productId);
            j.field("trades", t.trades());
            j.field("volumeMWh", round(mwh(t.volume()), 4));
            j.field("notionalEur", round(eur(t.notional()), 2));
            j.field("vwapEurPerMwh", round(vwapEur(t.notional(), t.volume()), 4));
            j.field("lastPrice", Scales.priceToString(t.lastPrice()));
            j.field("high", Scales.priceToString(t.high()));
            j.field("low", Scales.priceToString(t.low()));
            j.endObject();
        }
    }

    private void writeVwapWindow(Json j, int productId, long nowNs, int window) {
        final VwapWindow.Window w = metrics.vwap().window(productId, nowNs, window);
        j.beginObject();
        j.field("productId", productId);
        j.field("windowSeconds", w.windowSeconds());
        j.field("volumeMWh", round(mwh(w.volume()), 4));
        j.field("notionalEur", round(eur(w.notional()), 2));
        j.field("trades", metrics.vwap().totals(productId).trades());
        if (w.volume() > 0) {
            j.field("vwapEurPerMwh", round(vwapEur(w.notional(), w.volume()), 4));
            j.field("lastPrice", Scales.priceToString(w.lastPrice()));
            j.field("high", Scales.priceToString(w.high()));
            j.field("low", Scales.priceToString(w.low()));
        } else {
            j.nullField("vwapEurPerMwh");
            j.nullField("lastPrice");
        }
        j.endObject();
    }

    private static void measurement(Json j, String statistic, long value) {
        j.beginObject().field("statistic", statistic).field("value", value).endObject();
    }

    private static void measurement(Json j, String statistic, double value) {
        j.beginObject().field("statistic", statistic).field("value", value).endObject();
    }

    private long counter(String name) {
        final Counter c = metrics.counters().get(name);
        return c == null ? 0 : c.sum();
    }

    private double rateValue(String name, int windowSeconds) {
        final RateMeter r = metrics.rates().get(name);
        return r == null ? 0 : r.ratePerSecond(System.nanoTime(), windowSeconds);
    }

    /** Quantity is stored in 0.1 MW; MWh assumes a one-hour delivery block. */
    private static double mwh(long qtyScaled) {
        return qtyScaled / (double) Scales.QTY_SCALE;
    }

    /** Notional is price*qty in (0.01 EUR/MWh x 0.1 MW); EUR = scaled / (100*10). */
    private static double eur(long notionalScaled) {
        return notionalScaled / (double) (Scales.PRICE_SCALE * Scales.QTY_SCALE);
    }

    private static double vwapEur(long notionalScaled, long qtyScaled) {
        return qtyScaled == 0 ? 0 : (notionalScaled / (double) qtyScaled) / Scales.PRICE_SCALE;
    }

    private static double balance(long buy, long sell) {
        final long total = buy + sell;
        return total == 0 ? 0 : buy / (double) total;
    }

    private static double round(double value, int decimals) {
        final double factor = Math.pow(10, decimals);
        return Math.round(value * factor) / factor;
    }

    private static String sanitize(String name) {
        final StringBuilder out = new StringBuilder(name.length());
        for (int i = 0; i < name.length(); i++) {
            final char c = name.charAt(i);
            out.append(Character.isLetterOrDigit(c) ? c : '_');
        }
        return out.toString();
    }

    private static int parseInt(String value, int fallback) {
        if (value == null) {
            return fallback;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException notANumber) {
            return fallback;
        }
    }

    private static Map<String, String> query(HttpExchange exchange) {
        final Map<String, String> params = new HashMap<>();
        final String raw = exchange.getRequestURI().getRawQuery();
        if (raw == null || raw.isEmpty()) {
            return params;
        }
        for (String pair : raw.split("&")) {
            final int eq = pair.indexOf('=');
            if (eq > 0) {
                params.put(URLDecoder.decode(pair.substring(0, eq), StandardCharsets.UTF_8),
                        URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8));
            }
        }
        return params;
    }

    private static void sendJson(HttpExchange exchange, int status, String body) throws IOException {
        send(exchange, status, "application/json; charset=utf-8", body);
    }

    private static void sendText(HttpExchange exchange, int status, String body) throws IOException {
        send(exchange, status, "text/plain; version=0.0.4; charset=utf-8", body);
    }

    private static void send(HttpExchange exchange, int status, String contentType, String body)
            throws IOException {
        final byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }
}
