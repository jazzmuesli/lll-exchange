package exchange.metrics;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Minimal, allocation-light JSON writer for the management endpoints.
 *
 * <p>We deliberately do not pull in Jackson/Gson (and certainly not Spring): responses are tiny, the
 * shapes are fully under our control, and the management thread must not add dependencies to the
 * trading process. The builder tracks one "is this the first element" flag per open container so
 * commas and nesting are always correct.
 *
 * <p>Not thread-safe: one instance per request.
 */
public final class Json {
    private final StringBuilder sb = new StringBuilder(256);
    private final Deque<Boolean> firstStack = new ArrayDeque<>();
    private boolean expectValue; // set right after name()

    public Json beginObject() {
        valuePrefix();
        sb.append('{');
        firstStack.push(Boolean.TRUE);
        return this;
    }

    public Json beginArray() {
        valuePrefix();
        sb.append('[');
        firstStack.push(Boolean.TRUE);
        return this;
    }

    public Json endObject() {
        sb.append('}');
        firstStack.pop();
        return this;
    }

    public Json endArray() {
        sb.append(']');
        firstStack.pop();
        return this;
    }

    /** Write a key; the next value call supplies its value. */
    public Json name(String key) {
        valuePrefix();
        sb.append('"').append(escape(key)).append("\":");
        expectValue = true;
        return this;
    }

    public Json value(long v) {
        valuePrefix();
        sb.append(v);
        return this;
    }

    public Json value(double v) {
        valuePrefix();
        if (Double.isNaN(v) || Double.isInfinite(v)) {
            sb.append("null");
        } else if (v != 0.0 && (Math.abs(v) >= 1.0e7 || Math.abs(v) < 1.0e-3)) {
            // Avoid scientific notation (e.g. 1.03E8) so consumers see a plain decimal.
            sb.append(java.math.BigDecimal.valueOf(v).toPlainString());
        } else {
            sb.append(v);
        }
        return this;
    }

    public Json value(boolean v) {
        valuePrefix();
        sb.append(v);
        return this;
    }

    public Json value(String v) {
        valuePrefix();
        if (v == null) {
            sb.append("null");
        } else {
            sb.append('"').append(escape(v)).append('"');
        }
        return this;
    }

    public Json nullValue() {
        valuePrefix();
        sb.append("null");
        return this;
    }

    /** Write {@code "key": null}. */
    public Json nullField(String key) {
        return name(key).nullValue();
    }

    /** Escape arbitrary text for embedding inside a hand-built JSON string (error paths). */
    public static String escapeForError(String s) {
        return s == null ? "" : escape(s);
    }

    /** Convenience for a flat object entry: {@code key: value}. */
    public Json field(String key, long v) {
        return name(key).value(v);
    }

    public Json field(String key, double v) {
        return name(key).value(v);
    }

    public Json field(String key, String v) {
        return name(key).value(v);
    }

    public Json field(String key, boolean v) {
        return name(key).value(v);
    }

    private void valuePrefix() {
        if (expectValue) {
            expectValue = false;
            return;
        }
        if (!firstStack.isEmpty()) {
            final boolean first = firstStack.pop();
            if (!first) {
                sb.append(',');
            }
            firstStack.push(Boolean.FALSE);
        }
    }

    private static String escape(String s) {
        for (int i = 0; i < s.length(); i++) {
            final char c = s.charAt(i);
            if (c == '"' || c == '\\' || c < 0x20) {
                return slowEscape(s);
            }
        }
        return s;
    }

    private static String slowEscape(String s) {
        final StringBuilder out = new StringBuilder(s.length() + 8);
        for (int i = 0; i < s.length(); i++) {
            final char c = s.charAt(i);
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                default -> {
                    if (c < 0x20) {
                        out.append(String.format("\\u%04x", (int) c));
                    } else {
                        out.append(c);
                    }
                }
            }
        }
        return out.toString();
    }

    @Override
    public String toString() {
        return sb.toString();
    }
}
