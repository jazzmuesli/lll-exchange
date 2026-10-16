package exchange.engine;

/**
 * A two-level bitmap over price ticks, supporting O(1)-ish next/previous set-bit queries.
 *
 * <p>Level 0 ({@code words}) has one bit per tick. Level 1 ({@code summary}) has one bit per
 * level-0 word, kept in sync so that finding the next non-empty word is a short 64-bit scan
 * rather than a linear walk of the whole array. This is how a matching engine finds the next
 * best price after the current best level is consumed, without a tree and without allocation.
 *
 * <p>Not thread-safe: owned exclusively by the matching thread.
 */
public final class TickBitmap {
    private final long[] words;
    private final long[] summary;
    private final int numTicks;
    private final int numWords;

    public TickBitmap(int numTicks) {
        this.numTicks = numTicks;
        this.numWords = (numTicks + 63) >>> 6;
        this.words = new long[numWords];
        this.summary = new long[(numWords + 63) >>> 6];
    }

    /** Set bit {@code tick}. Idempotent. */
    public void set(int tick) {
        final int w = tick >>> 6;
        final long mask = 1L << tick;
        if ((words[w] & mask) == 0) {
            words[w] |= mask;
            summary[w >>> 6] |= 1L << w;
        }
    }

    /** Clear bit {@code tick}. Idempotent. */
    public void clear(int tick) {
        final int w = tick >>> 6;
        final long mask = 1L << tick;
        if ((words[w] & mask) != 0) {
            final long nw = words[w] & ~mask;
            words[w] = nw;
            if (nw == 0) {
                summary[w >>> 6] &= ~(1L << w);
            }
        }
    }

    /** @return the lowest set tick &gt;= {@code from}, or -1 if none. */
    public int nextSetBit(int from) {
        if (from >= numTicks) {
            return -1;
        }
        int w = from >>> 6;
        long word = words[w] & (-1L << from);
        while (true) {
            if (word != 0) {
                return (w << 6) + Long.numberOfTrailingZeros(word);
            }
            w = findNextWord(w + 1);
            if (w < 0) {
                return -1;
            }
            word = words[w];
        }
    }

    /** @return the highest set tick &lt;= {@code from}, or -1 if none. */
    public int prevSetBit(int from) {
        if (from < 0) {
            return -1;
        }
        if (from >= numTicks) {
            from = numTicks - 1;
        }
        int w = from >>> 6;
        long word = words[w] & (-1L >>> (63 - (from & 63)));
        while (true) {
            if (word != 0) {
                return (w << 6) + 63 - Long.numberOfLeadingZeros(word);
            }
            w = findPrevWord(w - 1);
            if (w < 0) {
                return -1;
            }
            word = words[w];
        }
    }

    /** @return the first non-empty level-0 word index &gt;= {@code fromWord}, or -1. */
    private int findNextWord(int fromWord) {
        if (fromWord >= numWords) {
            return -1;
        }
        int sw = fromWord >>> 6;
        long s = summary[sw] & (-1L << fromWord);
        while (true) {
            if (s != 0) {
                return (sw << 6) + Long.numberOfTrailingZeros(s);
            }
            if (++sw >= summary.length) {
                return -1;
            }
            s = summary[sw];
        }
    }

    /** @return the last non-empty level-0 word index &lt;= {@code fromWord}, or -1. */
    private int findPrevWord(int fromWord) {
        if (fromWord < 0) {
            return -1;
        }
        int sw = fromWord >>> 6;
        long s = summary[sw] & (-1L >>> (63 - (fromWord & 63)));
        while (true) {
            if (s != 0) {
                return (sw << 6) + 63 - Long.numberOfLeadingZeros(s);
            }
            if (--sw < 0) {
                return -1;
            }
            s = summary[sw];
        }
    }

    public boolean isEmpty() {
        for (long s : summary) {
            if (s != 0) {
                return false;
            }
        }
        return true;
    }

    public int numTicks() {
        return numTicks;
    }
}
