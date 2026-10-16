package exchange.launcher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Single entry point for all runnable roles. The shaded jar's {@code Main-Class} is this class.
 *
 * <pre>
 *   java -jar lowlat-exchange.jar engine  [--port 9001]
 *   java -jar lowlat-exchange.jar loadgen [--host 127.0.0.1 --port 9001 --conns 4 --rate 200000 ...]
 *   java -jar lowlat-exchange.jar bench
 *   java -jar lowlat-exchange.jar journal-dump &lt;path&gt;
 * </pre>
 */
public final class Launcher {
    private static final Logger LOG = LoggerFactory.getLogger(Launcher.class);

    private Launcher() {}

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            LOG.error("usage: <engine|loadgen|bench|demo|journal-dump> [options]");
            System.exit(2);
        }
        final String role = args[0];
        final String[] rest = java.util.Arrays.copyOfRange(args, 1, args.length);
        switch (role) {
            case "engine" -> exchange.engine.EngineMain.main(rest);
            case "loadgen" -> exchange.loadgen.LoadGen.main(rest);
            case "bench" -> exchange.bench.MatchingBenchmark.main(rest);
            case "demo" -> exchange.demo.MarketDemo.main(rest);
            case "journal-dump" -> exchange.journal.JournalDump.main(rest);
            default -> {
                LOG.error("unknown role: {}", role);
                System.exit(2);
            }
        }
    }
}
