package exchange.engine;

import java.util.ArrayList;
import java.util.List;

/** Test sink that records engine output in plain collections. */
final class RecordingSink implements Sink {
    record Ack(long clientOrderId, int orderId, int reason) {}
    record Fill(int orderId, int makerOrderId, long price, long qty, long tradeId) {}
    record Status(int orderId, byte status, long leaves, long filled) {}
    record Book(long bid, long bidQty, long ask, long askQty) {}

    final List<Ack> acks = new ArrayList<>();
    final List<Fill> fills = new ArrayList<>();
    final List<Status> statuses = new ArrayList<>();
    final List<Book> books = new ArrayList<>();

    @Override public void onAccepted(int s, long c, int o, int p, int r, long t) {
        acks.add(new Ack(c, o, r));
    }
    @Override public void onFill(int s, long c, int o, int m, int p, long pr, long q, long tid, long t) {
        fills.add(new Fill(o, m, pr, q, tid));
    }
    @Override public void onStatus(int s, long c, int o, int p, byte st, long l, long f, long t) {
        statuses.add(new Status(o, st, l, f));
    }
    @Override public void onBook(int p, long bp, long bq, long ap, long aq, long t) {
        books.add(new Book(bp, bq, ap, aq));
    }
}
