package exchange.wire;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/**
 * Hand-rolled, <b>protobuf-wire-compatible</b> codec for the hot messages.
 *
 * <p>The protocol definition (and the reference implementation used in tests) is
 * {@code src/main/proto/exchange.proto} compiled by protoc. On the hot path we do <i>not</i> use
 * the protobuf-java runtime: constructing a {@code Message} and calling {@code toByteArray()} costs
 * microseconds and allocates, which is unacceptable for a matching gateway. Instead we emit exactly
 * the same protobuf wire bytes (varint field tags + values) into a reusable {@link ByteBuffer}.
 *
 * <p>{@code FastWireRoundTripTest} proves byte-level compatibility with protobuf-java in both
 * directions, so any standard protobuf client interoperates.
 *
 * <p>Instances are per-thread (they hold scratch buffers) and not thread-safe.
 */
public final class FastWire {
    private FastWire() {}

    // wire types
    static final int WT_VARINT = 0;
    static final int WT_I64 = 1;
    static final int WT_LEN = 2;
    static final int WT_I32 = 5;

    // ServerMessage kinds returned by ServerParser.parse
    public static final int KIND_NONE = 0;
    public static final int KIND_ACK = 1;
    public static final int KIND_EXEC = 2;
    public static final int KIND_BOOK = 4;
    public static final int KIND_HEARTBEAT = 5;

    // ClientMessage kinds returned by ClientParser.parse
    public static final int CK_NONE = 0;
    public static final int CK_NEW_ORDER = 1;
    public static final int CK_CANCEL = 2;
    public static final int CK_HEARTBEAT = 3;

    // ------------------------------------------------------------------ varint

    public static void writeVarint(ByteBuffer b, long v) {
        while ((v & ~0x7FL) != 0) {
            b.put((byte) ((v & 0x7F) | 0x80));
            v >>>= 7;
        }
        b.put((byte) v);
    }

    public static long readVarint(ByteBuffer b) {
        long v = 0;
        int s = 0;
        while (true) {
            final byte c = b.get();
            v |= (long) (c & 0x7F) << s;
            if ((c & 0x80) == 0) {
                return v;
            }
            s += 7;
            if (s > 63) {
                throw new IllegalArgumentException("varint too long");
            }
        }
    }

    private static void tag(ByteBuffer b, int field, int wt) {
        writeVarint(b, ((long) field << 3) | wt);
    }

    private static void int64(ByteBuffer b, int field, long v) {
        tag(b, field, WT_VARINT);
        writeVarint(b, v);
    }

    private static void int32(ByteBuffer b, int field, int v) {
        tag(b, field, WT_VARINT);
        writeVarint(b, v);
    }

    private static void bool(ByteBuffer b, int field, boolean v) {
        tag(b, field, WT_VARINT);
        b.put((byte) (v ? 1 : 0));
    }

    private static void string(ByteBuffer b, int field, String s) {
        final byte[] u = s.getBytes(StandardCharsets.UTF_8);
        tag(b, field, WT_LEN);
        writeVarint(b, u.length);
        b.put(u);
    }

    // ------------------------------------------------------------- encoders

    /** Client-side encoder: builds {@code ClientMessage} payloads (4-byte length framing is external). */
    public static final class Encoder {
        private final ByteBuffer payload = ByteBuffer.allocate(1024);
        private final ByteBuffer nested = ByteBuffer.allocate(1024);

        /** Encode {@code ClientMessage{ new_order = NewOrder{...} }}. @return payload in read mode */
        public ByteBuffer newOrder(long clientOrderId, int productId, int side, int type, int tif,
                                   long price, long qty) {
            nested.clear();
            int64(nested, 1, clientOrderId);
            int32(nested, 2, productId);
            int32(nested, 3, side);
            int32(nested, 4, type);
            int32(nested, 5, tif);
            int64(nested, 6, price);
            int64(nested, 7, qty);
            nested.flip();

            payload.clear();
            tag(payload, 1, WT_LEN);
            writeVarint(payload, nested.remaining());
            payload.put(nested);
            payload.flip();
            return payload;
        }

        /** Encode {@code ClientMessage{ cancel = CancelOrder{...} }}. @return payload in read mode */
        public ByteBuffer cancel(long clientOrderId, int productId, long exchangeOrderId) {
            nested.clear();
            int64(nested, 1, clientOrderId);
            int32(nested, 2, productId);
            int64(nested, 3, exchangeOrderId);
            nested.flip();

            payload.clear();
            tag(payload, 2, WT_LEN);
            writeVarint(payload, nested.remaining());
            payload.put(nested);
            payload.flip();
            return payload;
        }
    }

    /** Server-side encoder: builds {@code ServerMessage} payloads. */
    public static final class ServerEncoder {
        private final ByteBuffer payload = ByteBuffer.allocate(1024);
        private final ByteBuffer nested = ByteBuffer.allocate(1024);
        private final ByteBuffer nested2 = ByteBuffer.allocate(256);

        private ByteBuffer wrap(ByteBuffer src, int field) {
            payload.clear();
            tag(payload, field, WT_LEN);
            writeVarint(payload, src.remaining());
            payload.put(src);
            payload.flip();
            return payload;
        }

        /** Encode {@code ServerMessage{ ack = OrderAck{...} }}. */
        public ByteBuffer ack(long clientOrderId, long exchangeOrderId, boolean accepted,
                              String reason, long recvNs) {
            nested.clear();
            int64(nested, 1, clientOrderId);
            int64(nested, 2, exchangeOrderId);
            bool(nested, 3, accepted);
            if (reason != null && !reason.isEmpty()) {
                string(nested, 4, reason);
            }
            int64(nested, 5, recvNs);
            nested.flip();
            return wrap(nested, 1);
        }

        /** Encode {@code ServerMessage{ exec = ExecutionReport{ fills=[Fill{...}] } }} (0 or 1 fill). */
        public ByteBuffer exec(long clientOrderId, long exchangeOrderId, int productId, int status,
                               long leaves, long filled, boolean hasFill, long tradeId,
                               long makerOrderId, long price, long qty, long ts) {
            nested.clear();
            int64(nested, 1, clientOrderId);
            int64(nested, 2, exchangeOrderId);
            int32(nested, 3, productId);
            int32(nested, 4, status);
            int64(nested, 5, leaves);
            int64(nested, 6, filled);
            if (hasFill) {
                nested2.clear();
                int64(nested2, 1, tradeId);
                int64(nested2, 2, exchangeOrderId);
                int64(nested2, 3, makerOrderId);
                int64(nested2, 4, price);
                int64(nested2, 5, qty);
                int64(nested2, 6, ts);
                nested2.flip();
                tag(nested, 7, WT_LEN);
                writeVarint(nested, nested2.remaining());
                nested.put(nested2);
            }
            int64(nested, 8, ts);
            nested.flip();
            return wrap(nested, 2);
        }

        /** Encode {@code ServerMessage{ book = BookUpdate{...} }}. */
        public ByteBuffer book(int productId, long bidPrice, long bidQty, long askPrice, long askQty,
                               long ts) {
            nested.clear();
            int32(nested, 1, productId);
            int64(nested, 2, bidPrice);
            int64(nested, 3, bidQty);
            int64(nested, 4, askPrice);
            int64(nested, 5, askQty);
            int64(nested, 6, ts);
            nested.flip();
            return wrap(nested, 4);
        }
    }

    // ------------------------------------------------------------- decoders

    /** Server-side parser for incoming {@code ClientMessage}. */
    public static final class ClientParser {
        public long clientOrderId;
        public int productId;
        public int side;
        public int type;
        public int tif;
        public long price;
        public long quantity;
        public long exchangeOrderId;

        /** @return one of the {@code CK_*} kinds */
        public int parse(ByteBuffer b, int off, int len) {
            b.position(off);
            final int end = off + len;
            int kind = CK_NONE;
            while (b.position() < end) {
                final long tag = readVarint(b);
                final int field = (int) (tag >>> 3);
                final int wt = (int) (tag & 7);
                if (wt == WT_LEN) {
                    final int mlen = (int) readVarint(b);
                    final int mstart = b.position();
                    if (field == 1) {
                        kind = CK_NEW_ORDER;
                        parseNewOrder(b, mstart, mstart + mlen);
                    } else if (field == 2) {
                        kind = CK_CANCEL;
                        parseCancel(b, mstart, mstart + mlen);
                    } else if (field == 3) {
                        kind = CK_HEARTBEAT;
                    }
                    b.position(mstart + mlen);
                } else {
                    skip(b, wt);
                }
            }
            return kind;
        }

        private void parseNewOrder(ByteBuffer b, int start, int end) {
            b.position(start);
            while (b.position() < end) {
                final long tag = readVarint(b);
                final int field = (int) (tag >>> 3);
                final int wt = (int) (tag & 7);
                if (wt != WT_VARINT) {
                    skip(b, wt);
                    continue;
                }
                final long v = readVarint(b);
                switch (field) {
                    case 1 -> clientOrderId = v;
                    case 2 -> productId = (int) v;
                    case 3 -> side = (int) v;
                    case 4 -> type = (int) v;
                    case 5 -> tif = (int) v;
                    case 6 -> price = v;
                    case 7 -> quantity = v;
                    default -> { }
                }
            }
        }

        private void parseCancel(ByteBuffer b, int start, int end) {
            b.position(start);
            while (b.position() < end) {
                final long tag = readVarint(b);
                final int field = (int) (tag >>> 3);
                final int wt = (int) (tag & 7);
                if (wt != WT_VARINT) {
                    skip(b, wt);
                    continue;
                }
                final long v = readVarint(b);
                switch (field) {
                    case 1 -> clientOrderId = v;
                    case 2 -> productId = (int) v;
                    case 3 -> exchangeOrderId = v;
                    default -> { }
                }
            }
        }
    }

    /** Client-side parser for incoming {@code ServerMessage}. */
    public static final class ServerParser {
        public long clientOrderId;
        public long exchangeOrderId;
        public int productId;
        public boolean accepted;
        public String reason = "";
        public long recvNs;
        public int status;
        public long leaves;
        public long filled;
        public boolean hasFill;
        public long fillTradeId;
        public long fillMakerOrderId;
        public long fillPrice;
        public long fillQty;
        public long fillTs;
        public long bookBidPrice;
        public long bookBidQty;
        public long bookAskPrice;
        public long bookAskQty;
        public long bookTs;

        /** @return one of the {@code KIND_*} kinds */
        public int parse(ByteBuffer b, int off, int len) {
            b.position(off);
            final int end = off + len;
            int kind = KIND_NONE;
            while (b.position() < end) {
                final long tag = readVarint(b);
                final int field = (int) (tag >>> 3);
                final int wt = (int) (tag & 7);
                if (wt == WT_LEN) {
                    final int mlen = (int) readVarint(b);
                    final int mstart = b.position();
                    if (field == 1) {
                        kind = KIND_ACK;
                        parseAck(b, mstart, mstart + mlen);
                    } else if (field == 2) {
                        kind = KIND_EXEC;
                        parseExec(b, mstart, mstart + mlen);
                    } else if (field == 4) {
                        kind = KIND_BOOK;
                        parseBook(b, mstart, mstart + mlen);
                    } else if (field == 5) {
                        kind = KIND_HEARTBEAT;
                    }
                    b.position(mstart + mlen);
                } else {
                    skip(b, wt);
                }
            }
            return kind;
        }

        private void parseAck(ByteBuffer b, int start, int end) {
            b.position(start);
            while (b.position() < end) {
                final long tag = readVarint(b);
                final int field = (int) (tag >>> 3);
                final int wt = (int) (tag & 7);
                if (field == 4 && wt == WT_LEN) {
                    final int slen = (int) readVarint(b);
                    final byte[] u = new byte[slen];
                    b.get(u);
                    reason = new String(u, StandardCharsets.UTF_8);
                    continue;
                }
                if (wt != WT_VARINT) {
                    skip(b, wt);
                    continue;
                }
                final long v = readVarint(b);
                switch (field) {
                    case 1 -> clientOrderId = v;
                    case 2 -> exchangeOrderId = v;
                    case 3 -> accepted = v != 0;
                    case 5 -> recvNs = v;
                    default -> { }
                }
            }
        }

        private void parseExec(ByteBuffer b, int start, int end) {
            b.position(start);
            while (b.position() < end) {
                final long tag = readVarint(b);
                final int field = (int) (tag >>> 3);
                final int wt = (int) (tag & 7);
                if (field == 7 && wt == WT_LEN) {
                    final int mlen = (int) readVarint(b);
                    final int mstart = b.position();
                    parseFill(b, mstart, mstart + mlen);
                    b.position(mstart + mlen);
                    continue;
                }
                if (wt != WT_VARINT) {
                    skip(b, wt);
                    continue;
                }
                final long v = readVarint(b);
                switch (field) {
                    case 1 -> clientOrderId = v;
                    case 2 -> exchangeOrderId = v;
                    case 3 -> productId = (int) v;
                    case 4 -> status = (int) v;
                    case 5 -> leaves = v;
                    case 6 -> filled = v;
                    case 8 -> fillTs = v;
                    default -> { }
                }
            }
        }

        private void parseFill(ByteBuffer b, int start, int end) {
            b.position(start);
            hasFill = true;
            while (b.position() < end) {
                final long tag = readVarint(b);
                final int field = (int) (tag >>> 3);
                final int wt = (int) (tag & 7);
                if (wt != WT_VARINT) {
                    skip(b, wt);
                    continue;
                }
                final long v = readVarint(b);
                switch (field) {
                    case 1 -> fillTradeId = v;
                    case 3 -> fillMakerOrderId = v;
                    case 4 -> fillPrice = v;
                    case 5 -> fillQty = v;
                    default -> { }
                }
            }
        }

        private void parseBook(ByteBuffer b, int start, int end) {
            b.position(start);
            while (b.position() < end) {
                final long tag = readVarint(b);
                final int field = (int) (tag >>> 3);
                final int wt = (int) (tag & 7);
                if (wt != WT_VARINT) {
                    skip(b, wt);
                    continue;
                }
                final long v = readVarint(b);
                switch (field) {
                    case 1 -> productId = (int) v;
                    case 2 -> bookBidPrice = v;
                    case 3 -> bookBidQty = v;
                    case 4 -> bookAskPrice = v;
                    case 5 -> bookAskQty = v;
                    case 6 -> bookTs = v;
                    default -> { }
                }
            }
        }
    }

    static void skip(ByteBuffer b, int wireType) {
        switch (wireType) {
            case WT_VARINT -> readVarint(b);
            case WT_I64 -> b.position(b.position() + 8);
            case WT_LEN -> {
                final int len = (int) readVarint(b);
                b.position(b.position() + len);
            }
            case WT_I32 -> b.position(b.position() + 4);
            default -> throw new IllegalArgumentException("bad wire type " + wireType);
        }
    }

    /** Allocate a big-endian direct buffer suitable for framing on a socket. */
    public static ByteBuffer socketBuffer(int capacity) {
        return ByteBuffer.allocateDirect(capacity).order(ByteOrder.BIG_ENDIAN);
    }
}
