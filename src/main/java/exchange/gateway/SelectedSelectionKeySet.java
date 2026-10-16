package exchange.gateway;

import java.nio.channels.SelectionKey;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Ready-key set for the gateway reactor, with an allocation-free hot path.
 *
 * <h2>Why</h2>
 *
 * <p>Stock NIO hands the selected keys back as a {@link java.util.HashSet} wrapper, so a select loop
 * allocates a fresh {@link Iterator} on <b>every</b> non-empty {@code selectNow()}. At a few hundred
 * thousand wakeups per second that is the only steady-state allocation in the whole engine (see the
 * README GC section: it is what stops an Epsilon run from being truly allocation-free). This class
 * instead keeps the ready keys in a plain array and exposes that array for direct, indexed
 * iteration, so {@link GatewayServer#loop()} allocates nothing.
 *
 * <h2>How it is wired in</h2>
 *
 * <p>OpenJDK's {@code sun.nio.ch.SelectorImpl} holds two fields — {@code selectedKeys} (written to
 * by the selector) and {@code publicSelectedKeys} (returned by {@link
 * java.nio.channels.Selector#selectedKeys()}). {@link GatewayServer} reflectively replaces both with
 * an instance of this class (the same trick Netty uses); if that ever fails the reactor falls back
 * to the standard iterator, so behaviour is never worse than stock NIO.
 *
 * <h2>Correctness notes</h2>
 *
 * <p>The selector calls {@link #add} for each ready key and uses {@link #contains} to de-duplicate
 * within a select cycle, so both are implemented for real (unlike Netty's stub). The reactor clears
 * the set at the start of every dispatch, which keeps {@link #contains} cheap (it only ever scans
 * the keys selected in the current cycle). A plain array is used rather than a {@code HashSet}
 * because it also gives the reactor a stable, cache-friendly iteration order.
 *
 * <p>Not thread-safe: touched only by the reactor thread (and the selector it drives).
 */
final class SelectedSelectionKeySet extends AbstractSet<SelectionKey> {

    /** Backing store; {@code [0, size)} are the live keys. Owned by the reactor thread. */
    SelectionKey[] keys;

    /** Number of live entries in {@link #keys}. */
    int size;

    SelectedSelectionKeySet() {
        this.keys = new SelectionKey[1024];
    }

    @Override
    public boolean add(SelectionKey key) {
        if (size == keys.length) {
            final SelectionKey[] grown = new SelectionKey[keys.length << 1];
            System.arraycopy(keys, 0, grown, 0, size);
            keys = grown;
        }
        keys[size++] = key;
        return true;
    }

    @Override
    public boolean remove(Object o) {
        for (int i = 0; i < size; i++) {
            if (keys[i].equals(o)) {
                final int moved = size - i - 1;
                if (moved > 0) {
                    System.arraycopy(keys, i + 1, keys, i, moved);
                }
                keys[--size] = null;
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean contains(Object o) {
        for (int i = 0; i < size; i++) {
            if (keys[i].equals(o)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public void clear() {
        // Null the slots so a closed channel is not pinned by a stale array reference.
        for (int i = 0; i < size; i++) {
            keys[i] = null;
        }
        size = 0;
    }

    @Override
    public Iterator<SelectionKey> iterator() {
        // Only used on the fallback path / by the Set contract; the reactor indexes the array.
        return new Iterator<>() {
            private int index;

            @Override
            public boolean hasNext() {
                return index < size;
            }

            @Override
            public SelectionKey next() {
                if (index >= size) {
                    throw new NoSuchElementException();
                }
                return keys[index++];
            }
        };
    }
}
