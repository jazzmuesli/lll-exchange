package exchange.metrics;

import java.util.concurrent.atomic.LongAdder;

/**
 * A monotonically increasing counter.
 *
 * <p>Backed by a {@link LongAdder} because counters are bumped from the matching thread, the gateway
 * reactor, and sometimes the journal, then read by the management thread. {@code LongAdder} spreads
 * writers over cells so increments never contend, and a read is a cheap sum. A {@code Counter}
 * handle is obtained <b>once</b> from {@link Metrics} and then reused, so the hot path costs a
 * single adder increment with no map lookup.
 */
public final class Counter {
    private final LongAdder adder = new LongAdder();

    public void inc() {
        adder.increment();
    }

    public void add(long delta) {
        adder.add(delta);
    }

    public long sum() {
        return adder.sum();
    }
}
