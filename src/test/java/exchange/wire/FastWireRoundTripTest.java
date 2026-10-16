package exchange.wire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import exchange.proto.ExchangeWire.ClientMessage;
import exchange.proto.ExchangeWire.ServerMessage;
import exchange.proto.ExchangeWire.OrdStatus;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

/**
 * Proves the hand-rolled {@link FastWire} codec is byte-level compatible with the reference
 * protobuf-java runtime, in both directions. This is what lets us keep "protobuf" as the interface
 * while running an allocation-free codec on the hot path.
 */
class FastWireRoundTripTest {

    private static byte[] bytes(ByteBuffer b) {
        final byte[] out = new byte[b.remaining()];
        b.get(out);
        return out;
    }

    @Test
    void fastWireNewOrderParsesWithProtobufJava() throws Exception {
        final FastWire.Encoder enc = new FastWire.Encoder();
        final byte[] wire = bytes(enc.newOrder(123456789L, 42, 1, 1, 2, 5000, 100));

        final ClientMessage m = ClientMessage.parseFrom(wire);
        assertTrue(m.hasNewOrder());
        assertEquals(123456789L, m.getNewOrder().getClientOrderId());
        assertEquals(42, m.getNewOrder().getProductId());
        assertEquals(1, m.getNewOrder().getSideValue());
        assertEquals(1, m.getNewOrder().getTypeValue());
        assertEquals(2, m.getNewOrder().getTifValue());
        assertEquals(5000, m.getNewOrder().getPrice());
        assertEquals(100, m.getNewOrder().getQuantity());
    }

    @Test
    void protobufJavaNewOrderParsesWithFastWire() throws Exception {
        final ClientMessage m = ClientMessage.newBuilder()
                .setNewOrder(exchange.proto.ExchangeWire.NewOrder.newBuilder()
                        .setClientOrderId(999L)
                        .setProductId(7)
                        .setSide(exchange.proto.ExchangeWire.Side.SELL)
                        .setType(exchange.proto.ExchangeWire.OrdType.LIMIT)
                        .setTif(exchange.proto.ExchangeWire.TimeInForce.IOC)
                        .setPrice(-2500)
                        .setQuantity(55))
                .build();
        final byte[] wire = m.toByteArray();

        final FastWire.ClientParser p = new FastWire.ClientParser();
        final ByteBuffer b = ByteBuffer.wrap(wire);
        final int kind = p.parse(b, 0, wire.length);
        assertEquals(FastWire.CK_NEW_ORDER, kind);
        assertEquals(999L, p.clientOrderId);
        assertEquals(7, p.productId);
        assertEquals(2, p.side);
        assertEquals(2, p.tif);
        assertEquals(-2500, p.price);
        assertEquals(55, p.quantity);
    }

    @Test
    void fastWireAckParsesWithProtobufJava() throws Exception {
        final FastWire.ServerEncoder enc = new FastWire.ServerEncoder();
        final byte[] wire = bytes(enc.ack(555L, 77L, true, "ok", 123456789012L));

        final ServerMessage m = ServerMessage.parseFrom(wire);
        assertTrue(m.hasAck());
        assertEquals(555L, m.getAck().getClientOrderId());
        assertEquals(77L, m.getAck().getExchangeOrderId());
        assertTrue(m.getAck().getAccepted());
        assertEquals("ok", m.getAck().getReason());
        assertEquals(123456789012L, m.getAck().getRecvTimeNs());
    }

    @Test
    void fastWireExecParsesWithProtobufJava() throws Exception {
        final FastWire.ServerEncoder enc = new FastWire.ServerEncoder();
        final byte[] wire = bytes(enc.exec(555L, 77L, 3, 3, 20, 80, true, 1001L, 42L, 5000L, 80L, 999L));

        final ServerMessage m = ServerMessage.parseFrom(wire);
        assertTrue(m.hasExec());
        assertEquals(555L, m.getExec().getClientOrderId());
        assertEquals(3, m.getExec().getStatusValue());
        assertEquals(20, m.getExec().getLeavesQuantity());
        assertEquals(80, m.getExec().getFilledQuantity());
        assertEquals(1, m.getExec().getFillsCount());
        assertEquals(1001L, m.getExec().getFills(0).getTradeId());
        assertEquals(42L, m.getExec().getFills(0).getMakerOrderId());
        assertEquals(5000L, m.getExec().getFills(0).getPrice());
        assertEquals(80L, m.getExec().getFills(0).getQuantity());
    }

    @Test
    void protobufJavaExecParsesWithFastWire() throws Exception {
        final ServerMessage m = ServerMessage.newBuilder()
                .setExec(exchange.proto.ExchangeWire.ExecutionReport.newBuilder()
                        .setClientOrderId(11L)
                        .setExchangeOrderId(22L)
                        .setProductId(5)
                        .setStatus(OrdStatus.FILLED)
                        .setLeavesQuantity(0)
                        .setFilledQuantity(30)
                        .setTsNs(4242L)
                        .addFills(exchange.proto.ExchangeWire.Fill.newBuilder()
                                .setTradeId(9L).setMakerOrderId(8L)
                                .setPrice(5100L).setQuantity(30L)))
                .build();
        final byte[] wire = m.toByteArray();

        final FastWire.ServerParser p = new FastWire.ServerParser();
        final ByteBuffer b = ByteBuffer.wrap(wire);
        final int kind = p.parse(b, 0, wire.length);
        assertEquals(FastWire.KIND_EXEC, kind);
        assertEquals(11L, p.clientOrderId);
        assertEquals(5, p.productId);
        assertEquals(4, p.status);
        assertEquals(30, p.filled);
        assertTrue(p.hasFill);
        assertEquals(9L, p.fillTradeId);
        assertEquals(8L, p.fillMakerOrderId);
        assertEquals(5100L, p.fillPrice);
        assertEquals(30L, p.fillQty);
    }

    @Test
    void fastWireBookParsesWithProtobufJava() throws Exception {
        final FastWire.ServerEncoder enc = new FastWire.ServerEncoder();
        final byte[] wire = bytes(enc.book(9, 4900, 120, 5100, 80, 777L));
        final ServerMessage m = ServerMessage.parseFrom(wire);
        assertTrue(m.hasBook());
        assertEquals(9, m.getBook().getProductId());
        assertEquals(4900, m.getBook().getBidPrice());
        assertEquals(120, m.getBook().getBidQuantity());
        assertEquals(5100, m.getBook().getAskPrice());
        assertEquals(80, m.getBook().getAskQuantity());
        assertFalse(m.hasAck());
    }
}
