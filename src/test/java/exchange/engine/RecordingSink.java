package exchange.engine;

import java.util.ArrayList;
import java.util.List;

/**
 * Test {@link Sink} that records engine output in plain collections so tests can assert on the
 * sequence of events. It deliberately ignores the fields it does not need, but keeps the parameter
 * names explicit (order/session identity, fixed-point units) to document what each event carries.
 */
final class RecordingSink implements Sink {
    record Ack(long clientOrderId, int orderId, int reason) {}
    record Fill(int orderId, int makerOrderId, long price, long qty, long tradeId) {}
    record Trade(int productId, long price, long qty, long tradeId, byte aggressorSide) {}
    record Status(int orderId, byte status, long leaves, long filled) {}
    record Book(long bid, long bidQty, long ask, long askQty) {}

    final List<Ack> acks = new ArrayList<>();
    final List<Fill> fills = new ArrayList<>();
    final List<Trade> trades = new ArrayList<>();
    final List<Status> statuses = new ArrayList<>();
    final List<Book> books = new ArrayList<>();

    @Override
    public void onAccepted(int sessionId, long clientOrderId, int orderId, int productId,
                           int reason, long recvNs) {
        acks.add(new Ack(clientOrderId, orderId, reason));
    }

    @Override
    public void onFill(int sessionId, long clientOrderId, int orderId, int makerOrderId,
                       int productId, long price, long qty, long tradeId, long tsNs) {
        fills.add(new Fill(orderId, makerOrderId, price, qty, tradeId));
    }

    @Override
    public void onTrade(byte aggressorSide, int productId, long price, long qty, long tradeId, long tsNs) {
        trades.add(new Trade(productId, price, qty, tradeId, aggressorSide));
    }

    @Override
    public void onStatus(int sessionId, long clientOrderId, int orderId, int productId,
                         byte status, long leavesQty, long filledQty, long tsNs) {
        statuses.add(new Status(orderId, status, leavesQty, filledQty));
    }

    @Override
    public void onBook(int productId, long bidPrice, long bidQty, long askPrice, long askQty, long tsNs) {
        books.add(new Book(bidPrice, bidQty, askPrice, askQty));
    }
}
