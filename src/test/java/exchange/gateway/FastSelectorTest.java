package exchange.gateway;

import static org.junit.jupiter.api.Assertions.assertTrue;

import exchange.engine.EngineConfig;
import exchange.engine.EngineService;
import org.junit.jupiter.api.Test;

/**
 * Confirms the reactor installs the allocation-free ready-key set on OpenJDK. The install needs
 * {@code --add-opens java.base/sun.nio.ch=ALL-UNNAMED} (surefire and {@code tools/run.sh} both pass
 * it). If a future JDK changes {@code sun.nio.ch.SelectorImpl}, this test flags it while the reactor
 * keeps working on the safe fallback.
 */
class FastSelectorTest {

    @Test
    void reactorUsesAllocationFreeSelectedKeySet() throws Exception {
        final EngineService engine = new EngineService(EngineConfig.defaults());
        engine.start();
        final GatewayServer gateway = new GatewayServer(0, engine.inbound(), engine.outbound());
        try {
            gateway.start();
            assertTrue(gateway.usingFastSelector(),
                    "expected the sun.nio.ch.SelectorImpl fast-path install to succeed on OpenJDK");
        } finally {
            gateway.close();
            engine.close();
        }
    }
}
