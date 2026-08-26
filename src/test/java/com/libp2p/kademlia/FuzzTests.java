package com.libp2p.kademlia;

import com.libp2p.kademlia.pb.Dht;
import com.libp2p.kademlia.protocol.KademliaProtocol;
import com.libp2p.kademlia.protocol.RpcCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class FuzzTests {

    private List<Throwable> exceptions;

    @BeforeEach
    void setUp() {
        exceptions = new ArrayList<>();
    }

    private EmbeddedChannel createChannel() {
        return new EmbeddedChannel(
                new KademliaProtocol.KademliaCodec(),
                new ChannelInboundHandlerAdapter() {
                    @Override
                    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                        exceptions.add(cause);
                    }
                }
        );
    }

    @Test
    void testTruncatedVarint() {
        EmbeddedChannel channel = createChannel();
        ByteBuf buf = Unpooled.wrappedBuffer(new byte[]{(byte) 0x80});
        channel.writeInbound(buf);

        assertNull(channel.readInbound());
        assertTrue(exceptions.isEmpty());
    }

    @Test
    void testZeroLengthFrame() {
        EmbeddedChannel channel = createChannel();
        ByteBuf buf = Unpooled.wrappedBuffer(new byte[0]);
        channel.writeInbound(buf);

        assertNull(channel.readInbound());
        assertTrue(exceptions.isEmpty());
    }

    @Test
    void testHugeFrame() {
        EmbeddedChannel channel = createChannel();
        byte[] varint = RpcCodec.encodeVarint(16385);
        byte[] data = new byte[varint.length + 100];
        System.arraycopy(varint, 0, data, 0, varint.length);
        ByteBuf buf = Unpooled.wrappedBuffer(data);
        channel.writeInbound(buf);

        assertNull(channel.readInbound());
        assertFalse(exceptions.isEmpty());
    }

    @Test
    void testMultipleConcatenatedFrames() {
        EmbeddedChannel channel = createChannel();
        Dht.Message msg1 = RpcCodec.ping();
        Dht.Message msg2 = RpcCodec.findNode(new byte[32]);
        byte[] encoded1 = RpcCodec.encode(msg1);
        byte[] encoded2 = RpcCodec.encode(msg2);
        byte[] combined = new byte[encoded1.length + encoded2.length];
        System.arraycopy(encoded1, 0, combined, 0, encoded1.length);
        System.arraycopy(encoded2, 0, combined, encoded1.length, encoded2.length);

        ByteBuf buf = Unpooled.wrappedBuffer(combined);
        channel.writeInbound(buf);

        Dht.Message decoded1 = channel.readInbound();
        Dht.Message decoded2 = channel.readInbound();
        assertNotNull(decoded1);
        assertNotNull(decoded2);
        assertEquals(Dht.Message.MessageType.PING, decoded1.getType());
        assertEquals(Dht.Message.MessageType.FIND_NODE, decoded2.getType());
    }

    @Test
    void testMalformedProtobuf() {
        EmbeddedChannel channel = createChannel();
        byte[] garbage = new byte[10];
        new Random().nextBytes(garbage);
        byte[] varint = RpcCodec.encodeVarint(garbage.length);
        byte[] data = new byte[varint.length + garbage.length];
        System.arraycopy(varint, 0, data, 0, varint.length);
        System.arraycopy(garbage, 0, data, varint.length, garbage.length);

        ByteBuf buf = Unpooled.wrappedBuffer(data);
        channel.writeInbound(buf);

        assertNull(channel.readInbound());
        assertFalse(exceptions.isEmpty());
    }

    @Test
    void testUnknownEnumValue() {
        byte[] typeField = new byte[]{0x08, (byte) 0xF7, 0x07};
        byte[] varint = RpcCodec.encodeVarint(typeField.length);
        byte[] data = new byte[varint.length + typeField.length];
        System.arraycopy(varint, 0, data, 0, varint.length);
        System.arraycopy(typeField, 0, data, varint.length, typeField.length);

        EmbeddedChannel channel = createChannel();
        ByteBuf buf = Unpooled.wrappedBuffer(data);
        channel.writeInbound(buf);

        Dht.Message msg = channel.readInbound();
        if (msg != null) {
            assertEquals(Dht.Message.MessageType.PUT_VALUE, msg.getType());
        } else {
            assertFalse(exceptions.isEmpty());
        }
    }

    @Test
    void testEmptyMessage() {
        EmbeddedChannel channel = createChannel();
        byte[] varint = RpcCodec.encodeVarint(0);
        byte[] data = new byte[varint.length + 1];
        System.arraycopy(varint, 0, data, 0, varint.length);
        ByteBuf buf = Unpooled.wrappedBuffer(data);
        channel.writeInbound(buf);

        Dht.Message msg = channel.readInbound();
        boolean parsed = msg != null;
        boolean errored = !exceptions.isEmpty();
        assertTrue(parsed || errored);
    }

    @Test
    void testPartialFrame() {
        EmbeddedChannel channel = createChannel();
        byte[] varint = RpcCodec.encodeVarint(100);
        byte[] partial = new byte[50];
        new Random().nextBytes(partial);
        byte[] data = new byte[varint.length + partial.length];
        System.arraycopy(varint, 0, data, 0, varint.length);
        System.arraycopy(partial, 0, data, varint.length, partial.length);

        ByteBuf buf = Unpooled.wrappedBuffer(data);
        channel.writeInbound(buf);

        assertNull(channel.readInbound());
        assertTrue(exceptions.isEmpty());
    }

    @Test
    void testMaxVarint() {
        EmbeddedChannel channel = createChannel();
        byte[] varint = new byte[10];
        for (int i = 0; i < 9; i++) varint[i] = (byte) 0xFF;
        varint[9] = (byte) 0x7F;
        byte[] padding = new byte[200];
        new Random().nextBytes(padding);
        byte[] data = new byte[varint.length + padding.length];
        System.arraycopy(varint, 0, data, 0, varint.length);
        System.arraycopy(padding, 0, data, varint.length, padding.length);

        ByteBuf buf = Unpooled.wrappedBuffer(data);
        channel.writeInbound(buf);

        Dht.Message msg = channel.readInbound();
        if (msg == null) {
            assertFalse(exceptions.isEmpty());
        }
    }

    @Test
    void testNegativeVarint() {
        EmbeddedChannel channel = createChannel();
        ByteBuf buf = Unpooled.wrappedBuffer(new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF});
        channel.writeInbound(buf);

        assertNull(channel.readInbound());
        assertTrue(exceptions.isEmpty());
    }
}
