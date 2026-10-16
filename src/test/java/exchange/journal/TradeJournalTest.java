package exchange.journal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Round-trips records through the memory-mapped {@link TradeJournal}. */
class TradeJournalTest {

    @Test
    void writesAndReplaysRecords(@TempDir Path dir) throws Exception {
        final String path = dir.resolve("journal").toString();
        try (TradeJournal j = new TradeJournal(path)) {
            j.append(TradeJournal.EV_NEW, 100L, 1L, 10, 5, (byte) 1, 5000, 100, 0, (byte) 0, 0);
            j.append(TradeJournal.EV_FILL, 200L, 1L, 10, 5, (byte) 0, 5000, 100, 77, (byte) 0, 42);
            j.append(TradeJournal.EV_STATUS, 300L, 1L, 10, 5, (byte) 0, 0, 100, 0, (byte) 4, 0);
        }

        final List<byte[]> records = new ArrayList<>();
        try (TradeJournal j = new TradeJournal(path)) {
            j.replay(records::add);
        }

        assertEquals(3, records.size());
        final String fill = JournalDump.format(records.get(1));
        assertTrue(fill.contains("FILL"), fill);
        assertTrue(fill.contains("trade=77"), fill);
        final java.nio.ByteBuffer b = java.nio.ByteBuffer.wrap(records.get(0))
                .order(java.nio.ByteOrder.nativeOrder());
        assertEquals(5000, b.getLong(26), "price round-trips");
        assertTrue(Files.exists(dir.resolve("journal")));
    }

    @Test
    void growsAcrossInitialCapacity(@TempDir Path dir) throws Exception {
        final String path = dir.resolve("journal").toString();
        final int n = 37; // deliberately far beyond the initial capacity of 4 slots
        try (TradeJournal j = new TradeJournal(path, 4)) {
            for (int i = 0; i < n; i++) {
                j.append(TradeJournal.EV_FILL, i, i, i, 1, (byte) 0, 1000 + i, 10, i * 3, (byte) 0, 0);
            }
            assertEquals(n, j.size());
            assertTrue(j.capacity() >= n, "journal grew to hold all records");
        }

        final List<byte[]> records = new ArrayList<>();
        try (TradeJournal j = new TradeJournal(path, 4)) {
            assertEquals(n, j.size(), "count survives reopen");
            j.replay(records::add);
        }
        assertEquals(n, records.size());
        final java.nio.ByteBuffer last = java.nio.ByteBuffer.wrap(records.get(n - 1))
                .order(java.nio.ByteOrder.nativeOrder());
        assertEquals(n - 1, last.getLong(9), "last clientOrderId");
        assertEquals(1000 + (n - 1), last.getLong(26), "last price");
    }

    @Test
    void reopenContinuesAppending(@TempDir Path dir) throws Exception {
        final String path = dir.resolve("journal").toString();
        try (TradeJournal j = new TradeJournal(path, 8)) {
            j.append(TradeJournal.EV_NEW, 1, 1, 1, 1, (byte) 1, 100, 10, 0, (byte) 0, 0);
            j.append(TradeJournal.EV_NEW, 2, 2, 2, 1, (byte) 1, 200, 10, 0, (byte) 0, 0);
        }
        try (TradeJournal j = new TradeJournal(path, 8)) {
            assertEquals(2, j.size());
            j.append(TradeJournal.EV_NEW, 3, 3, 3, 1, (byte) 1, 300, 10, 0, (byte) 0, 0);
        }

        final List<byte[]> records = new ArrayList<>();
        try (TradeJournal j = new TradeJournal(path, 8)) {
            j.replay(records::add);
        }
        assertEquals(3, records.size());
    }

    @Test
    void rejectsNonJournalFile(@TempDir Path dir) throws Exception {
        final Path path = dir.resolve("not-a-journal");
        Files.write(path, new byte[256]);
        assertThrows(IllegalStateException.class, () -> new TradeJournal(path.toString()));
    }
}
