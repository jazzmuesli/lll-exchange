package exchange.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import exchange.engine.Codes;
import exchange.engine.EngineConfig;
import exchange.engine.MatchingEngine;
import exchange.engine.Sink;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Drives the management ("actuator") HTTP server end to end over a real socket and asserts on the
 * JSON it returns. The server binds to an ephemeral port so tests never collide.
 */
class ManagementServerTest {

    private Metrics metrics;
    private ManagementServer server;
    private HttpClient http;

    @BeforeEach
    void setUp() throws IOException {
        metrics = new Metrics(4, 300);
        metrics.counter("orders.accepted").add(7);
        metrics.counter("trades").add(3);
        metrics.counter("trade.qty").add(42);
        metrics.gauge("gateway.fast_selector", () -> 1L);
        metrics.latency("order.latency").record(1_000);
        metrics.latency("order.latency").record(2_000);
        metrics.vwap().onTrade(0, 5_000, 100, System.nanoTime());

        // A tiny live book so /actuator/books has something to show.
        final MatchingEngine engine = new MatchingEngine(EngineConfig.defaults(), Sink.NOOP);
        engine.submit(1, 1, 0, Codes.BUY, Codes.LIMIT, Codes.GTC, 50 * 100, 5 * 10, 1);
        engine.submit(1, 2, 0, Codes.SELL, Codes.LIMIT, Codes.GTC, 51 * 100, 5 * 10, 2);

        server = new ManagementServer(0, metrics, engine);
        server.start();
        http = HttpClient.newHttpClient();
    }

    @AfterEach
    void tearDown() {
        server.close();
    }

    private String get(String path) throws Exception {
        final HttpResponse<String> response = http.send(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.boundPort() + path))
                        .GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode(), path);
        return response.body();
    }

    @Test
    void healthAndIndex() throws Exception {
        assertTrue(get("/actuator/health").contains("\"status\":\"UP\""));
        assertTrue(get("/actuator/health").contains("\"fastSelector\":true"));
        assertTrue(get("/actuator").contains("\"health\""));
    }

    @Test
    void metricNamesAndSingleMetric() throws Exception {
        assertTrue(get("/actuator/metrics").contains("orders.accepted"));
        assertTrue(get("/actuator/metrics/orders.accepted").contains("\"COUNT\""));
        assertTrue(get("/actuator/metrics/orders.accepted").contains("7"));
        assertTrue(get("/actuator/metrics/nope").contains("unknown metric"));
    }

    @Test
    void latencyPercentilesAreExposed() throws Exception {
        final String body = get("/actuator/latency");
        assertTrue(body.contains("order.latency"));
        assertTrue(body.contains("p50Ns"));
        assertTrue(body.contains("p99Ns"));
        assertTrue(body.contains("p9999Ns"));
    }

    @Test
    void ordersAndTradesCounts() throws Exception {
        assertTrue(get("/actuator/orders").contains("\"accepted\":7"));
        assertTrue(get("/actuator/trades").contains("\"count\":3"));
    }

    @Test
    void vwapEndpointReturnsWindow() throws Exception {
        final String body = get("/actuator/vwap?product=0&window=300");
        assertTrue(body.contains("\"productId\":0"));
        assertTrue(body.contains("\"vwapEurPerMwh\""));
        assertTrue(body.contains("\"windowSeconds\":300"));
    }

    @Test
    void booksEndpointShowsTopOfBook() throws Exception {
        final String body = get("/actuator/books");
        assertTrue(body.contains("\"productId\":0"));
        assertTrue(body.contains("\"bid\":\"50.00\""));
        assertTrue(body.contains("\"ask\":\"51.00\""));
        assertTrue(body.contains("\"spread\":1.0"));
    }

    @Test
    void prometheusExposition() throws Exception {
        final String body = get("/actuator/prometheus");
        assertTrue(body.contains("lowlat_orders_accepted_total 7"));
        assertTrue(body.contains("lowlat_order_latency_ns{quantile=\"0.5\"}"));
    }
}
