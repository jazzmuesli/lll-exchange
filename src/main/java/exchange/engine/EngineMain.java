package exchange.engine;

import exchange.gateway.GatewayServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs the exchange as a single Java process: one matching thread plus one gateway reactor thread,
 * handing orders to each other through lock-free ring buffers.
 *
 * <p>All diagnostics go through SLF4J/logback (asynchronously). The matching thread itself never
 * logs, so logging cannot perturb the measured hot path.
 */
public final class EngineMain {
    private static final Logger LOG = LoggerFactory.getLogger(EngineMain.class);

    private EngineMain() {}

    public static void main(String[] args) throws Exception {
        int port = 9001;
        int maxProducts = 1024;
        int maxOrders = 1 << 20;
        String journalPath = null;
        for (int i = 0; i < args.length - 1; i++) {
            switch (args[i]) {
                case "--port" -> port = Integer.parseInt(args[++i]);
                case "--products" -> maxProducts = Integer.parseInt(args[++i]);
                case "--orders" -> maxOrders = Integer.parseInt(args[++i]);
                case "--journal" -> journalPath = args[++i];
                default -> { }
            }
        }

        final EngineConfig cfg = new EngineConfig(
                maxProducts, maxOrders, EngineConfig.defaults().minPrice(),
                EngineConfig.defaults().maxPrice(), EngineConfig.defaults().tickSize());

        final EngineService engine = new EngineService(cfg, 1 << 18, 1 << 22, journalPath);
        engine.start();

        final GatewayServer gateway = new GatewayServer(port, engine.inbound(), engine.outbound());
        gateway.start();

        LOG.info("engine listening on :{} products={} maxOrders={} pid={}",
                port, maxProducts, maxOrders, ProcessHandle.current().pid());
        if (journalPath != null) {
            LOG.info("journaling to {}", journalPath);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("engine shutting down");
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
