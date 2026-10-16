package exchange.engine;

import exchange.wire.MsgType;
import org.agrona.concurrent.ringbuffer.OneToOneRingBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Engine {@link Sink} that serialises output events into a single-producer/single-consumer
 * {@link OneToOneRingBuffer} for the gateway thread to pick up.
 *
 * <h2>Why this shape</h2>
 *
 * <p>The matching thread must stay allocation-free and branch-light, so this sink does the minimum
 * possible: it packs each event's primitive fields into a small, <b>fixed-size, native-endian</b>
 * record and hands it to the ring in one {@code write()}.
 *
 * <ul>
 *   <li><b>No protobuf / no encoding here.</b> Turning events into wire bytes (protobuf framing,
 *       batching) is the gateway's job, on a different thread, so it never lengthens the matching
 *       critical path.</li>
 *   <li><b>Fixed layout, not self-describing.</b> The reader knows the exact field order, so there
 *       are no tags, lengths or reflection — just sequential stores/loads. The record sizes are
 *       small enough that a whole event fits one ring slot.</li>
 *   <li><b>Native byte order.</b> Producer and consumer are in the same process/JVM, so
 *       {@link UnsafeBuffer}'s native order lets the hardware store multi-byte values directly
 *       (no byte-swapping on the hot path).</li>
 *   <li><b>One reusable scratch buffer.</b> {@code scratch} is a pre-allocated direct-ish buffer
 *       reused for every event; {@code write()} copies the bytes into the ring, so reusing it is
 *       safe and avoids per-event allocation.</li>
 *   <li><b>Back-pressure without blocking.</b> If the ring is momentarily full, {@code write()}
 *       returns {@code false}; a {@link Sink} must never block, so the event is counted as dropped
 *       ({@link #droppedCount()}) and matching continues. With correctly sized rings this is a
 *       safety valve, not the steady state.</li>
 * </ul>
 *
 * <h2>Record layouts</h2>
 *
 * <p>Field order below is the contract with {@code GatewayServer}; all values are native-endian and
 * the byte counts in the comments are the on-ring offsets.
 *
 * <pre>
 * ACK    : sessionId i32 | clientOrderId i64 | orderId i32 | productId i32 | reason i32 | recvNs i64
 * FILL   : sessionId i32 | clientOrderId i64 | orderId i32 | makerOrderId i32 | productId i32
 *          | price i64 | qty i64 | tradeId i64 | ts i64
 * TRADE  : aggressorSide i8 | productId i32 | price i64 | qty i64 | tradeId i64 | ts i64
 * STATUS : sessionId i32 | clientOrderId i64 | orderId i32 | productId i32 | status i8
 *          | leaves i64 | filled i64 | ts i64
 * BOOK   : productId i32 | bidPrice i64 | bidQty i64 | askPrice i64 | askQty i64 | ts i64
 * </pre>
 */
public final class RingSink implements Sink {
    /** SPSC ring to the gateway thread. Exactly one producer (this thread). */
    private final OneToOneRingBuffer out;

    /** Reusable staging area (see "One reusable scratch buffer" above). Big enough for the widest record. */
    private final UnsafeBuffer scratch = new UnsafeBuffer(new byte[128]);

    /** Count of events dropped because the ring was full; should stay 0 in steady state. */
    private long dropped;

    public RingSink(OneToOneRingBuffer out) {
        this.out = out;
    }

    /** Events dropped because the ring was full. Non-zero signals an undersized/slow consumer. */
    public long droppedCount() {
        return dropped;
    }

    @Override
    public void onAccepted(int sessionId, long clientOrderId, int orderId, int productId,
                           int reason, long recvNs) {
        // ACK layout. `i` is the running byte offset; each store advances it by the field width.
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
        // FILL layout (private leg, routed to the aggressor's session).
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
    public void onMakerFill(int sessionId, long clientOrderId, int orderId, int makerOrderId,
                            int productId, long price, long qty, long tradeId, long tsNs) {
        // Mirror of FILL, routed to the maker's session. Same record layout; the gateway routes by
        // sessionId and encodes the same ExecReport, so no new ring type is needed.
        onFill(sessionId, clientOrderId, orderId, makerOrderId, productId, price, qty, tradeId, tsNs);
    }

    @Override
    public void onTrade(byte aggressorSide, int productId, long price, long qty, long tradeId, long tsNs) {
        // TRADE layout (public tape, broadcast to every session). Note: no session/order identity.
        final UnsafeBuffer b = scratch;
        int i = 0;
        b.putByte(i, aggressorSide); i += 1;
        b.putInt(i, productId); i += 4;
        b.putLong(i, price); i += 8;
        b.putLong(i, qty); i += 8;
        b.putLong(i, tradeId); i += 8;
        b.putLong(i, tsNs); i += 8;
        if (!out.write(MsgType.TRADE, b, 0, i)) {
            dropped++;
        }
    }

    @Override
    public void onStatus(int sessionId, long clientOrderId, int orderId, int productId,
                         byte status, long leavesQty, long filledQty, long tsNs) {
        // STATUS layout. status is a Codes.ST_* byte; leaves/filled are cumulative quantities.
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
        // BOOK layout (top of book only). Zero price means that side is empty.
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
