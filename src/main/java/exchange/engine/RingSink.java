package exchange.engine;

import exchange.wire.MsgType;
import org.agrona.concurrent.ringbuffer.OneToOneRingBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Engine {@link Sink} that serialises output events into a single-producer/single-consumer ring
 * buffer for the gateway thread. Fixed-layout, native-endian field order — no protobuf and no
 * allocation happens on the matching thread, so the hot path stays GC-free.
 *
 * <p>Layouts (native byte order, written with {@link UnsafeBuffer}):
 * <pre>
 * ACK    : sessionId i32 | clientOrderId i64 | orderId i32 | productId i32 | reason i32 | recvNs i64
 * FILL   : sessionId i32 | clientOrderId i64 | orderId i32 | makerOrderId i32 | productId i32
 *          | price i64 | qty i64 | tradeId i64 | ts i64
 * STATUS : sessionId i32 | clientOrderId i64 | orderId i32 | productId i32 | status i8
 *          | leaves i64 | filled i64 | ts i64
 * BOOK   : productId i32 | bidPrice i64 | bidQty i64 | askPrice i64 | askQty i64 | ts i64
 * </pre>
 */
public final class RingSink implements Sink {
    private final OneToOneRingBuffer out;
    private final UnsafeBuffer scratch = new UnsafeBuffer(new byte[128]);
    private long dropped;

    public RingSink(OneToOneRingBuffer out) {
        this.out = out;
    }

    public long droppedCount() {
        return dropped;
    }

    @Override
    public void onAccepted(int sessionId, long clientOrderId, int orderId, int productId,
                           int reason, long recvNs) {
        final UnsafeBuffer b = scratch;
        int i = 0;
        b.putInt(i, sessionId); i += 4;
        b.putLong(i, clientOrderId); i += 8;
        b.putInt(i, orderId); i += 4;
        b.putInt(i, productId); i += 4;
        b.putInt(i, reason); i += 4;
        b.putLong(i, recvNs); i += 8;
        if (!out.write(MsgType.ACK, b, 0, i)) {
            dropped++;
        }
    }

    @Override
    public void onFill(int sessionId, long clientOrderId, int orderId, int makerOrderId,
                       int productId, long price, long qty, long tradeId, long tsNs) {
        final UnsafeBuffer b = scratch;
        int i = 0;
        b.putInt(i, sessionId); i += 4;
        b.putLong(i, clientOrderId); i += 8;
        b.putInt(i, orderId); i += 4;
        b.putInt(i, makerOrderId); i += 4;
        b.putInt(i, productId); i += 4;
        b.putLong(i, price); i += 8;
        b.putLong(i, qty); i += 8;
        b.putLong(i, tradeId); i += 8;
        b.putLong(i, tsNs); i += 8;
        if (!out.write(MsgType.FILL, b, 0, i)) {
            dropped++;
        }
    }

    @Override
    public void onStatus(int sessionId, long clientOrderId, int orderId, int productId,
                         byte status, long leavesQty, long filledQty, long tsNs) {
        final UnsafeBuffer b = scratch;
        int i = 0;
        b.putInt(i, sessionId); i += 4;
        b.putLong(i, clientOrderId); i += 8;
        b.putInt(i, orderId); i += 4;
        b.putInt(i, productId); i += 4;
        b.putByte(i, status); i += 1;
        b.putLong(i, leavesQty); i += 8;
        b.putLong(i, filledQty); i += 8;
        b.putLong(i, tsNs); i += 8;
        if (!out.write(MsgType.STATUS, b, 0, i)) {
            dropped++;
        }
    }

    @Override
    public void onBook(int productId, long bidPrice, long bidQty, long askPrice, long askQty, long tsNs) {
        final UnsafeBuffer b = scratch;
        int i = 0;
        b.putInt(i, productId); i += 4;
        b.putLong(i, bidPrice); i += 8;
        b.putLong(i, bidQty); i += 8;
        b.putLong(i, askPrice); i += 8;
        b.putLong(i, askQty); i += 8;
        b.putLong(i, tsNs); i += 8;
        if (!out.write(MsgType.BOOK, b, 0, i)) {
            dropped++;
        }
    }
}
