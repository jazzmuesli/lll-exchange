package exchange.journal;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Replays a {@link TradeJournal} to the log (audit / recovery debugging). */
public final class JournalDump {
    private static final Logger LOG = LoggerFactory.getLogger(JournalDump.class);

    private JournalDump() {}

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            LOG.error("usage: journal-dump <path>");
            System.exit(2);
        }
        try (TradeJournal j = new TradeJournal(args[0])) {
            LOG.info("replaying {} ({} records)", args[0], j.size());
            j.replay(rec -> LOG.info(format(rec)));
        }
    }

    static String format(byte[] rec) {
        final ByteBuffer b = ByteBuffer.wrap(rec).order(ByteOrder.nativeOrder());
        final byte type = b.get(0);
        final long ts = b.getLong(1);
        final long coid = b.getLong(9);
        final int orderId = b.getInt(17);
        final int productId = b.getInt(21);
        final long price = b.getLong(26);
        final long qty = b.getLong(34);
        final long tradeId = b.getLong(42);
        final byte status = b.get(50);
        final int maker = b.getInt(51);
        return switch (type) {
            case TradeJournal.EV_NEW -> String.format("NEW    ts=%d coid=%d order=%d product=%d reason=%d",
                    ts, coid, orderId, productId, status);
            case TradeJournal.EV_CANCEL -> String.format("CANCEL ts=%d coid=%d order=%d", ts, coid, orderId);
            case TradeJournal.EV_FILL -> String.format("FILL   ts=%d coid=%d order=%d maker=%d px=%d qty=%d trade=%d",
                    ts, coid, orderId, maker, price, qty, tradeId);
            case TradeJournal.EV_STATUS -> String.format("STATUS ts=%d coid=%d order=%d status=%d leaves=%d filled=%d",
                    ts, coid, orderId, status, price, qty);
            default -> "UNKNOWN type=" + type;
        };
    }
}
