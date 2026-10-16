package exchange.journal;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Durable, replayable event log backed by a memory-mapped file: the exchange's "event sourcing"
 * ledger. Every accepted order, cancellation, fill and status change is appended in order. On
 * restart the log can be replayed to rebuild the book (recovery) and it doubles as the audit trail.
 *
 * <h2>Why not Chronicle Queue?</h2>
 * We originally used Chronicle Queue ({@code net.openhft:chronicle-queue}), which is the classic
 * near-zero-GC persisted queue. On <b>JDK 27</b> it cannot memory-map a file any more: it resolves
 * {@code sun.nio.ch.UnixFileDispatcherImpl.map0} with {@code MethodHandles.lookup().unreflect(...)},
 * and that now fails with <i>"class is not public"</i> because the dispatcher class is
 * package-private ({@code --add-opens} does not help; only {@code privateLookupIn} does, which
 * Chronicle does not use). The failure is a hard {@code AssertionError} in {@code OS.map0} and
 * cannot be patched from outside because HotSpot constant-folds the offending {@code static final}
 * field. See {@code docs/} and the README for the full investigation.
 *
 * <p>Rather than pin an older JVM, we implement the log directly on the modern
 * <b>Foreign Function &amp; Memory</b> API (JEP 454, stable since Java 22) — the supported
 * successor to {@code Unsafe}/{@code MappedByteBuffer} and, for a fixed-record append-only log,
 * exactly the "something better" we want: no reflection into JDK internals, a deterministic
 * on-disk layout, deterministic unmapping ({@link Arena#close()}), and no heavyweight dependency.
 *
 * <h2>File layout</h2>
 * 64-byte header, then fixed 64-byte records starting at offset 64:
 * <pre>
 *   header:  magic(8) version(4) recordSize(4) count(8) capacity(8) ... pad to 64
 *   record:  0  byte  event type (1 new, 2 cancel, 3 fill, 4 status)
 *             1  long  tsNs
 *             9  long  clientOrderId
 *            17  int   orderId
 *            21  int   productId
 *            25  byte  side
 *            26  long  price
 *            34  long  qty
 *            42  long  tradeId
 *            50  byte  status
 *            51  int   makerOrderId
 *            ... zero padding to 64
 * </pre>
 *
 * <p>All integers are native-endian. {@link #append} is a handful of store instructions into an
 * already-faulted-in page — no syscall, no allocation — so it is safe to call from the matching
 * thread (see {@link JournalingSink}). The file grows geometrically; growth is the only operation
 * that can stall the caller, so size the journal generously for latency work.
 */
public final class TradeJournal implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(TradeJournal.class);

    public static final byte EV_NEW = 1;
    public static final byte EV_CANCEL = 2;
    public static final byte EV_FILL = 3;
    public static final byte EV_STATUS = 4;

    public static final int RECORD_SIZE = 64;

    private static final long MAGIC = 0x4C4F574C41544A31L; // "LOWLATJ1"
    private static final int VERSION = 1;
    private static final int HEADER_BYTES = 64;
    private static final long COUNT_OFFSET = 16;
    private static final long CAPACITY_OFFSET = 24;
    private static final long RECORDS_OFFSET = HEADER_BYTES;

    private static final long DEFAULT_INITIAL_CAPACITY = 1L << 16;   // 65536 records = 4 MiB

    private final Path path;
    private final FileChannel channel;
    private MemorySegment segment;
    private Arena arena;

    private long capacity;
    private long count;
    private boolean closed;

    public TradeJournal(String path) {
        this(path, DEFAULT_INITIAL_CAPACITY);
    }

    public TradeJournal(String path, long initialCapacity) {
        this.path = Path.of(path).toAbsolutePath();
        FileChannel ch = null;
        try {
            ch = FileChannel.open(this.path,
                    StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
            this.channel = ch;
            open(Math.max(1, initialCapacity));
        } catch (Throwable t) {
            if (arena != null) {
                arena.close();
            }
            if (ch != null) {
                try {
                    ch.close();
                } catch (IOException ignore) {
                    // already failing; nothing useful to do
                }
            }
            if (t instanceof RuntimeException re) {
                throw re;
            }
            if (t instanceof Error e) {
                throw e;
            }
            throw new UncheckedIOException("cannot open journal " + this.path, (IOException) t);
        }
        LOG.info("journal {} open: {} records mapped, {} records present, {} bytes",
                this.path.getFileName(), capacity, count, mappingBytes());
    }

    // ------------------------------------------------------------------ open

    private void open(long requestedCapacity) throws IOException {
        if (channel.size() < HEADER_BYTES) {
            final long length = recordsBytes(requestedCapacity);
            extendTo(length);
            remap(length);
            writeHeader(requestedCapacity, 0L);
            this.capacity = requestedCapacity;
            this.count = 0L;
            return;
        }

        final ByteBuffer header = ByteBuffer.allocate(HEADER_BYTES).order(ByteOrder.nativeOrder());
        readFully(header, 0L);
        header.flip();
        final long magic = header.getLong(0);
        final int version = header.getInt(8);
        final int recordSize = header.getInt(12);
        final long storedCount = header.getLong(16);
        final long storedCapacity = header.getLong(24);

        if (magic != MAGIC) {
            throw new IllegalStateException("not a lowlat journal: " + path);
        }
        if (version != VERSION) {
            throw new IllegalStateException("unsupported journal version " + version + " in " + path);
        }
        if (recordSize != RECORD_SIZE) {
            throw new IllegalStateException("journal record size " + recordSize
                    + " != " + RECORD_SIZE + " in " + path);
        }
        if (storedCapacity <= 0 || storedCount < 0 || storedCount > storedCapacity) {
            throw new IllegalStateException("corrupt journal header in " + path
                    + " (count=" + storedCount + ", capacity=" + storedCapacity + ")");
        }

        final long length = recordsBytes(storedCapacity);
        if (channel.size() < length) {
            extendTo(length);
        }
        remap(length);
        this.capacity = storedCapacity;
        this.count = storedCount;
    }

    private void writeHeader(long cap, long cnt) {
        segment.set(ValueLayout.JAVA_LONG_UNALIGNED, 0, MAGIC);
        segment.set(ValueLayout.JAVA_INT_UNALIGNED, 8, VERSION);
        segment.set(ValueLayout.JAVA_INT_UNALIGNED, 12, RECORD_SIZE);
        segment.set(ValueLayout.JAVA_LONG_UNALIGNED, COUNT_OFFSET, cnt);
        segment.set(ValueLayout.JAVA_LONG_UNALIGNED, CAPACITY_OFFSET, cap);
    }

    private static long recordsBytes(long cap) {
        return RECORDS_OFFSET + cap * RECORD_SIZE;
    }

    private long mappingBytes() {
        return recordsBytes(capacity);
    }

    private void remap(long length) throws IOException {
        this.arena = Arena.ofShared();
        this.segment = channel.map(FileChannel.MapMode.READ_WRITE, 0L, length, arena);
    }

    /** Extend the file to {@code newLength} bytes (never shrinks). */
    private void extendTo(long newLength) throws IOException {
        if (channel.size() >= newLength) {
            return;
        }
        final ByteBuffer one = ByteBuffer.allocate(1);
        channel.write(one, newLength - 1);
    }

    private void readFully(ByteBuffer dst, long position) throws IOException {
        long pos = position;
        while (dst.hasRemaining()) {
            final int n = channel.read(dst, pos);
            if (n < 0) {
                throw new IOException("unexpected end of file " + path);
            }
            pos += n;
        }
    }

    // ------------------------------------------------------------------ append

    /**
     * Append one event. Allocation-free and syscall-free on the steady-state path; it must stay
     * that way because it runs on the matching thread.
     */
    public void append(byte type, long tsNs, long clientOrderId, int orderId, int productId,
                       byte side, long price, long qty, long tradeId, byte status, int makerOrderId) {
        if (count == capacity) {
            grow();
        }
        final MemorySegment s = segment;
        final long base = RECORDS_OFFSET + count * RECORD_SIZE;
        s.set(ValueLayout.JAVA_BYTE, base, type);
        s.set(ValueLayout.JAVA_LONG_UNALIGNED, base + 1, tsNs);
        s.set(ValueLayout.JAVA_LONG_UNALIGNED, base + 9, clientOrderId);
        s.set(ValueLayout.JAVA_INT_UNALIGNED, base + 17, orderId);
        s.set(ValueLayout.JAVA_INT_UNALIGNED, base + 21, productId);
        s.set(ValueLayout.JAVA_BYTE, base + 25, side);
        s.set(ValueLayout.JAVA_LONG_UNALIGNED, base + 26, price);
        s.set(ValueLayout.JAVA_LONG_UNALIGNED, base + 34, qty);
        s.set(ValueLayout.JAVA_LONG_UNALIGNED, base + 42, tradeId);
        s.set(ValueLayout.JAVA_BYTE, base + 50, status);
        s.set(ValueLayout.JAVA_INT_UNALIGNED, base + 51, makerOrderId);
        count++;
        // Publish the new length so a crash mid-write leaves a consistent, recoverable prefix.
        s.set(ValueLayout.JAVA_LONG_UNALIGNED, COUNT_OFFSET, count);
    }

    /** Double the journal, remapping the file. Rare; may stall the caller briefly. */
    private void grow() {
        final long newCapacity = capacity << 1;
        final long newLength = recordsBytes(newCapacity);
        try {
            extendTo(newLength);
            final MemorySegment old = segment;
            final Arena oldArena = arena;
            final long committed = count;
            remap(newLength);
            this.capacity = newCapacity;
            writeHeader(newCapacity, committed);
            old.force();
            oldArena.close();
            LOG.info("journal {} grew to {} records ({} bytes)",
                    path.getFileName(), newCapacity, newLength);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot grow journal " + path, e);
        }
    }

    // ------------------------------------------------------------------ read

    /** Replay every record to the given consumer (for recovery / audit / tests). */
    public void replay(Consumer<byte[]> consumer) {
        final MemorySegment s = segment;
        final byte[] out = new byte[RECORD_SIZE];
        for (long i = 0; i < count; i++) {
            final long base = RECORDS_OFFSET + i * RECORD_SIZE;
            MemorySegment.copy(s, ValueLayout.JAVA_BYTE, base, out, 0, RECORD_SIZE);
            consumer.accept(out.clone());
        }
    }

    public long size() {
        return count;
    }

    public long capacity() {
        return capacity;
    }

    public Path path() {
        return path;
    }

    // ------------------------------------------------------------------ lifecycle

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        try {
            if (segment != null) {
                segment.force();
            }
        } catch (RuntimeException e) {
            LOG.warn("journal {} flush on close failed", path, e);
        } finally {
            if (arena != null) {
                arena.close();
            }
            channel.close();
        }
    }
}
