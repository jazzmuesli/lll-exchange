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
    public static final int KIND_TRADE = 3;
    public static final int KIND_BOOK = 4;
    public static final int KIND_HEARTBEAT = 5;

    // ClientMessage kinds returned by ClientParser.parse
    public static final int CK_NONE = 0;
    public static final int CK_NEW_ORDER = 1;
    public static final int CK_CANCEL = 2;
    public static final int CK_HEARTBEAT = 3;

    // ------------------------------------------------------------------ varint

    // Protobuf's variable-length integer ("varint"): 7 payload bits per byte, low group first, with
    // the top bit set on every byte except the last. We hand-roll it so encoding never touches the
    // protobuf-java runtime (see class doc). Field tags are just a varint of (fieldNumber << 3)|type.

    /** Write {@code value} as a protobuf varint (32- or 64-bit; the wire format is the same). */
    public static void writeVarint(ByteBuffer buf, long value) {
        while ((value & ~0x7FL) != 0) {
            buf.put((byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        buf.put((byte) value);
    }

    /** Read one protobuf varint from {@code buf}'s current position. */
    public static long readVarint(ByteBuffer buf) {
        long value = 0;
        int shift = 0;
        while (true) {
            final byte currentByte = buf.get();
            value |= (long) (currentByte & 0x7F) << shift;
            if ((currentByte & 0x80) == 0) {
                return value;
            }
            shift += 7;
            if (shift > 63) {
                throw new IllegalArgumentException("varint too long");
            }
        }
    }

    /** Write a field tag: {@code (fieldNumber << 3) | wireType}. */
    private static void tag(ByteBuffer buf, int fieldNumber, int wireType) {
        writeVarint(buf, ((long) fieldNumber << 3) | wireType);
    }

    /** Write a 64-bit field (varint-encoded; protobuf uses the compact form for small values). */
    private static void int64(ByteBuffer buf, int fieldNumber, long value) {
        tag(buf, fieldNumber, WT_VARINT);
        writeVarint(buf, value);
    }

    /** Write a 32-bit field (varint-encoded; width is not distinguishable on the wire). */
    private static void int32(ByteBuffer buf, int fieldNumber, int value) {
        tag(buf, fieldNumber, WT_VARINT);
        writeVarint(buf, value);
    }

    /** Write a {@code bool} field (protobuf encodes it as a 1-byte varint). */
    private static void bool(ByteBuffer buf, int fieldNumber, boolean value) {
        tag(buf, fieldNumber, WT_VARINT);
        buf.put((byte) (value ? 1 : 0));
    }

    /** Write a length-delimited UTF-8 string field. */
    private static void string(ByteBuffer buf, int fieldNumber, String value) {
        final byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
        tag(buf, fieldNumber, WT_LEN);
        writeVarint(buf, utf8.length);
        buf.put(utf8);
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

        /** Encode {@code ServerMessage{ trade = Trade{...} }} (the public tape print). */
        public ByteBuffer trade(long tradeId, int productId, long price, long qty, int aggressorSide,
                                long ts) {
            nested.clear();
            int64(nested, 1, tradeId);
            int32(nested, 2, productId);
            int64(nested, 3, price);
            int64(nested, 4, qty);
            int32(nested, 5, aggressorSide);
            int64(nested, 6, ts);
            nested.flip();
            return wrap(nested, 3);
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

        /**
         * Parse a {@code ClientMessage} payload occupying {@code [offset, offset + length)} and
         * return which kind it was (see {@code CK_*}). {@code length} is the message length only —
         * the 4-byte frame prefix has already been consumed by the caller.
         */
        public int parse(ByteBuffer buf, int offset, int length) {
            buf.position(offset);
            final int end = offset + length;
            int kind = CK_NONE;
            while (buf.position() < end) {
                final long tag = readVarint(buf);
                final int fieldNumber = (int) (tag >>> 3);
                final int wireType = (int) (tag & 7);
                if (wireType == WT_LEN) {
                    // Length-delimited field: the body is the oneof member (a nested sub-message).
                    final int subLength = (int) readVarint(buf);
                    final int subStart = buf.position();
                    if (fieldNumber == 1) {
                        kind = CK_NEW_ORDER;
                        parseNewOrder(buf, subStart, subStart + subLength);
                    } else if (fieldNumber == 2) {
                        kind = CK_CANCEL;
                        parseCancel(buf, subStart, subStart + subLength);
                    } else if (fieldNumber == 3) {
                        kind = CK_HEARTBEAT;
                    }
                    buf.position(subStart + subLength);
                } else {
                    skip(buf, wireType);
                }
            }
            return kind;
        }

        private void parseNewOrder(ByteBuffer buf, int start, int end) {
            buf.position(start);
            while (buf.position() < end) {
                final long tag = readVarint(buf);
                final int fieldNumber = (int) (tag >>> 3);
                final int wireType = (int) (tag & 7);
                if (wireType != WT_VARINT) {
                    skip(buf, wireType);
                    continue;
                }
                final long value = readVarint(buf);
                switch (fieldNumber) {
                    case 1 -> clientOrderId = value;
                    case 2 -> productId = (int) value;
                    case 3 -> side = (int) value;
                    case 4 -> type = (int) value;
                    case 5 -> tif = (int) value;
                    case 6 -> price = value;
                    case 7 -> quantity = value;
                    default -> { }
                }
            }
        }

        private void parseCancel(ByteBuffer buf, int start, int end) {
            buf.position(start);
            while (buf.position() < end) {
                final long tag = readVarint(buf);
                final int fieldNumber = (int) (tag >>> 3);
                final int wireType = (int) (tag & 7);
                if (wireType != WT_VARINT) {
                    skip(buf, wireType);
                    continue;
                }
                final long value = readVarint(buf);
                switch (fieldNumber) {
                    case 1 -> clientOrderId = value;
                    case 2 -> productId = (int) value;
                    case 3 -> exchangeOrderId = value;
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
        public long tradeId;
        public long tradePrice;
        public long tradeQty;
        public int tradeAggressorSide;
        public long tradeTs;

        /**
         * Parse a {@code ServerMessage} payload occupying {@code [offset, offset + length)} and
         * return which kind it was (see {@code KIND_*}). {@code length} excludes the 4-byte frame
         * prefix.
         */
        public int parse(ByteBuffer buf, int offset, int length) {
            buf.position(offset);
            final int end = offset + length;
            int kind = KIND_NONE;
            while (buf.position() < end) {
                final long tag = readVarint(buf);
                final int fieldNumber = (int) (tag >>> 3);
                final int wireType = (int) (tag & 7);
                if (wireType == WT_LEN) {
                    // Length-delimited field: the body is the oneof member (a nested sub-message).
                    final int subLength = (int) readVarint(buf);
                    final int subStart = buf.position();
                    if (fieldNumber == 1) {
                        kind = KIND_ACK;
                        parseAck(buf, subStart, subStart + subLength);
                    } else if (fieldNumber == 2) {
                        kind = KIND_EXEC;
                        parseExec(buf, subStart, subStart + subLength);
                    } else if (fieldNumber == 3) {
                        kind = KIND_TRADE;
                        parseTrade(buf, subStart, subStart + subLength);
                    } else if (fieldNumber == 4) {
                        kind = KIND_BOOK;
                        parseBook(buf, subStart, subStart + subLength);
                    } else if (fieldNumber == 5) {
                        kind = KIND_HEARTBEAT;
                    }
                    buf.position(subStart + subLength);
                } else {
                    skip(buf, wireType);
                }
            }
            return kind;
        }

        private void parseAck(ByteBuffer buf, int start, int end) {
            buf.position(start);
            while (buf.position() < end) {
                final long tag = readVarint(buf);
                final int fieldNumber = (int) (tag >>> 3);
                final int wireType = (int) (tag & 7);
                if (fieldNumber == 4 && wireType == WT_LEN) {
                    // Rejection reason string (only present on rejects).
                    final int stringLength = (int) readVarint(buf);
                    final byte[] utf8 = new byte[stringLength];
                    buf.get(utf8);
                    reason = new String(utf8, StandardCharsets.UTF_8);
                    continue;
                }
                if (wireType != WT_VARINT) {
                    skip(buf, wireType);
                    continue;
                }
                final long value = readVarint(buf);
                switch (fieldNumber) {
                    case 1 -> clientOrderId = value;
                    case 2 -> exchangeOrderId = value;
                    case 3 -> accepted = value != 0;
                    case 5 -> recvNs = value;
                    default -> { }
                }
            }
        }

        private void parseExec(ByteBuffer buf, int start, int end) {
            buf.position(start);
            while (buf.position() < end) {
                final long tag = readVarint(buf);
                final int fieldNumber = (int) (tag >>> 3);
                final int wireType = (int) (tag & 7);
                if (fieldNumber == 7 && wireType == WT_LEN) {
                    // Nested Fill sub-message.
                    final int subLength = (int) readVarint(buf);
                    final int subStart = buf.position();
                    parseFill(buf, subStart, subStart + subLength);
                    buf.position(subStart + subLength);
                    continue;
                }
                if (wireType != WT_VARINT) {
                    skip(buf, wireType);
                    continue;
                }
                final long value = readVarint(buf);
                switch (fieldNumber) {
                    case 1 -> clientOrderId = value;
                    case 2 -> exchangeOrderId = value;
                    case 3 -> productId = (int) value;
                    case 4 -> status = (int) value;
                    case 5 -> leaves = value;
                    case 6 -> filled = value;
                    case 8 -> fillTs = value;
                    default -> { }
                }
            }
        }

        private void parseFill(ByteBuffer buf, int start, int end) {
            buf.position(start);
            hasFill = true;
            while (buf.position() < end) {
                final long tag = readVarint(buf);
                final int fieldNumber = (int) (tag >>> 3);
                final int wireType = (int) (tag & 7);
                if (wireType != WT_VARINT) {
                    skip(buf, wireType);
                    continue;
                }
                final long value = readVarint(buf);
                switch (fieldNumber) {
                    case 1 -> fillTradeId = value;
                    case 3 -> fillMakerOrderId = value;
                    case 4 -> fillPrice = value;
                    case 5 -> fillQty = value;
                    default -> { }
                }
            }
        }

        private void parseTrade(ByteBuffer buf, int start, int end) {
            buf.position(start);
            while (buf.position() < end) {
                final long tag = readVarint(buf);
                final int fieldNumber = (int) (tag >>> 3);
                final int wireType = (int) (tag & 7);
                if (wireType != WT_VARINT) {
                    skip(buf, wireType);
                    continue;
                }
                final long value = readVarint(buf);
                switch (fieldNumber) {
                    case 1 -> tradeId = value;
                    case 2 -> productId = (int) value;
                    case 3 -> tradePrice = value;
                    case 4 -> tradeQty = value;
                    case 5 -> tradeAggressorSide = (int) value;
                    case 6 -> tradeTs = value;
                    default -> { }
                }
            }
        }

        private void parseBook(ByteBuffer buf, int start, int end) {
            buf.position(start);
            while (buf.position() < end) {
                final long tag = readVarint(buf);
                final int fieldNumber = (int) (tag >>> 3);
                final int wireType = (int) (tag & 7);
                if (wireType != WT_VARINT) {
                    skip(buf, wireType);
                    continue;
                }
                final long value = readVarint(buf);
                switch (fieldNumber) {
                    case 1 -> productId = (int) value;
                    case 2 -> bookBidPrice = value;
                    case 3 -> bookBidQty = value;
                    case 4 -> bookAskPrice = value;
                    case 5 -> bookAskQty = value;
                    case 6 -> bookTs = value;
                    default -> { }
                }
            }
        }
    }

    /** Advance past a field of the given wire type whose value we do not care about. */
    static void skip(ByteBuffer buf, int wireType) {
        switch (wireType) {
            case WT_VARINT -> readVarint(buf);
            case WT_I64 -> buf.position(buf.position() + 8);
            case WT_LEN -> {
                final int length = (int) readVarint(buf);
                buf.position(buf.position() + length);
            }
            case WT_I32 -> buf.position(buf.position() + 4);
            default -> throw new IllegalArgumentException("bad wire type " + wireType);
        }
    }

    /** Allocate a big-endian direct buffer suitable for framing on a socket. */
    public static ByteBuffer socketBuffer(int capacity) {
        return ByteBuffer.allocateDirect(capacity).order(ByteOrder.BIG_ENDIAN);
    }
}
