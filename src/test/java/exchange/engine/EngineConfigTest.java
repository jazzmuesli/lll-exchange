package exchange.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/**
 * The hot-path price→tick map ({@link EngineConfig#tickFor}) uses a precomputed exact
 * multiply-shift instead of a division. These tests pin it against the naive div/mod reference
 * across the whole ladder, several tick sizes, negative prices, and the division fallback.
 */
class EngineConfigTest {

    /** The straightforward reference implementation this optimisation must equal. */
    private static int naiveTick(EngineConfig cfg, long price) {
        if (price < cfg.minPrice() || price > cfg.maxPrice()) {
            return -1;
        }
        final long d = price - cfg.minPrice();
        if (d % cfg.tickSize() != 0) {
            return -1;
        }
        return (int) (d / cfg.tickSize());
    }

    private static void assertMatchesNaiveEverywhere(EngineConfig cfg) {
        for (int tick = 0; tick < cfg.numTicks(); tick++) {
            final long price = cfg.priceForTick(tick);
            assertEquals(naiveTick(cfg, price), cfg.tickFor(price), "on-tick price " + price);
            assertEquals(tick, cfg.tickFor(price), "round-trip tick " + tick);
        }
    }

    @Test
    void defaultLadderMapsExactly() {
        final EngineConfig cfg = EngineConfig.defaults();
        assertMatchesNaiveEverywhere(cfg);

        // Negative prices are supported: the first tick sits at -1000.00 EUR/MWh.
        assertEquals(0, cfg.tickFor(cfg.minPrice()));

        // Off-tick (half a 0.10 tick above a level) and off-ladder must be rejected.
        assertEquals(-1, cfg.tickFor(cfg.priceForTick(3) + cfg.tickSize() / 2));
        assertEquals(-1, cfg.tickFor(cfg.minPrice() - cfg.tickSize()));
        assertEquals(-1, cfg.tickFor(cfg.maxPrice() + cfg.tickSize()));
    }

    @Test
    void oddAndPowerOfTwoTickSizes() {
        for (long tick : new long[] {1, 2, 3, 7, 10, 16, 100, 250, 1000}) {
            assertMatchesNaiveEverywhere(new EngineConfig(8, 128, -500L * 100, 500L * 100, tick));
        }
    }

    @Test
    void fallsBackToDivisionWhenNoMagicApplies() {
        // A tick far larger than the ladder leaves no valid shift, so the division path runs.
        final EngineConfig cfg = new EngineConfig(4, 16, 0, 10L << 40, 1L << 40);
        assertMatchesNaiveEverywhere(cfg);
        assertEquals(-1, cfg.tickFor(1));
    }
}
