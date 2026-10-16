package exchange.engine;

import exchange.gateway.GatewayServer;
import exchange.metrics.ManagementServer;
import exchange.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs the exchange as a single Java process: one matching thread plus one gateway reactor thread,
 * handing orders to each other through lock-free ring buffers.
 *
 * <p>All diagnostics go through SLF4J/logback (asynchronously). The matching thread itself never
 * logs, so logging cannot perturb the measured hot path.
 *
 * <p>Observability ("actuator") endpoints are served by {@link ManagementServer} on loopback only
 * (default port 9002, override with {@code --metrics-port}, disable with {@code --metrics-port 0}).
 */
public final class EngineMain {
    private static final Logger LOG = LoggerFactory.getLogger(EngineMain.class);

    /** Default actuator port; the trading port is 9001, so keep the management plane adjacent. */
    private static final int DEFAULT_METRICS_PORT = 9002;

    /** Trailing window for VWAP/high/low: 5 minutes, as required by the market-data consumers. */
    private static final int VWAP_WINDOW_SECONDS = 300;

    private EngineMain() {}

    public static void main(String[] args) throws Exception {
        int port = 9001;
        int maxProducts = 1024;
        int maxOrders = 1 << 20;
        int metricsPort = DEFAULT_METRICS_PORT;
        String journalPath = null;
        for (int i = 0; i < args.length - 1; i++) {
            switch (args[i]) {
                case "--port" -> port = Integer.parseInt(args[++i]);
                case "--products" -> maxProducts = Integer.parseInt(args[++i]);
                case "--orders" -> maxOrders = Integer.parseInt(args[++i]);
                case "--journal" -> journalPath = args[++i];
                case "--metrics-port" -> metricsPort = Integer.parseInt(args[++i]);
                default -> { }
            }
        }

        final EngineConfig cfg = new EngineConfig(
                maxProducts, maxOrders, EngineConfig.defaults().minPrice(),
                EngineConfig.defaults().maxPrice(), EngineConfig.defaults().tickSize());

        final Metrics metrics = new Metrics(maxProducts, VWAP_WINDOW_SECONDS);

        final EngineService engine = new EngineService(cfg, 1 << 18, 1 << 22, journalPath, metrics);
        engine.start();

        final GatewayServer gateway = new GatewayServer(port, engine.inbound(), engine.outbound(), metrics);
        gateway.start();

        final ManagementServer management = metricsPort > 0
                ? new ManagementServer(metricsPort, metrics, engine.engine())
                : null;
        if (management != null) {
            management.start();
        }

        LOG.info("engine listening on :{} products={} maxOrders={} pid={}",
                port, maxProducts, maxOrders, ProcessHandle.current().pid());
        if (journalPath != null) {
            LOG.info("journaling to {}", journalPath);
        }
        if (management != null) {
            LOG.info("actuator on http://127.0.0.1:{}/actuator", management.boundPort());
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("engine shutting down");
            if (management != null) {
                management.close();
            }
            gateway.close();
            engine.close();
        }));

        // periodic stats
        while (true) {
            Thread.sleep(5000);
            LOG.info("orders={} cancels={} dropped={} heapUsedMB={}",
                    engine.ordersProcessed(), engine.cancelsProcessed(), engine.sink().droppedCount(),
                    (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) >> 20);
        }
    }
}
