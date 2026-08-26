package com.libp2p.kademlia;

import com.google.protobuf.ByteString;
import com.libp2p.kademlia.pb.Dht;
import com.libp2p.kademlia.protocol.RpcCodec;
import com.libp2p.kademlia.records.WireRecord;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RpcCodecTest {

    private Dht.Message decode(byte[] framed) {
        try {
            int offset = 0;
            long length = 0;
            int shift = 0;
            while (offset < framed.length) {
                byte b = framed[offset++];
                length |= (long) (b & 0x7F) << shift;
                if ((b & 0x80) == 0) break;
                shift += 7;
            }
            byte[] payload = Arrays.copyOfRange(framed, offset, offset + (int) length);
            return Dht.Message.parseFrom(payload);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testEncodeDecodePing() {
        Dht.Message ping = RpcCodec.ping();
        byte[] encoded = RpcCodec.encode(ping);
        Dht.Message decoded = decode(encoded);
        assertEquals(Dht.Message.MessageType.PING, decoded.getType());
    }

    @Test
    void testEncodeDecodeFindNode() {
        byte[] key = new byte[32];
        Arrays.fill(key, (byte) 0x42);
        Dht.Message findNode = RpcCodec.findNode(key);
        byte[] encoded = RpcCodec.encode(findNode);
        Dht.Message decoded = decode(encoded);
        assertEquals(Dht.Message.MessageType.FIND_NODE, decoded.getType());
        assertArrayEquals(key, decoded.getKey().toByteArray());
    }

    @Test
    void testEncodeDecodeFindNodeResponse() {
        Dht.Message.Peer peer = Dht.Message.Peer.newBuilder()
                .setId(ByteString.copyFrom(new byte[]{1, 2, 3}))
                .setConnection(Dht.Message.ConnectionType.CONNECTED)
                .build();
        Dht.Message response = Dht.Message.newBuilder()
                .setType(Dht.Message.MessageType.FIND_NODE)
                .addCloserPeers(peer)
                .build();
        byte[] encoded = RpcCodec.encode(response);
        Dht.Message decoded = decode(encoded);
        assertEquals(Dht.Message.MessageType.FIND_NODE, decoded.getType());
        assertEquals(1, decoded.getCloserPeersCount());
        assertArrayEquals(new byte[]{1, 2, 3}, decoded.getCloserPeers(0).getId().toByteArray());
    }

    @Test
    void testEncodeDecodeGetValue() {
        byte[] key = new byte[32];
        Arrays.fill(key, (byte) 0x11);
        Dht.Message getValue = RpcCodec.getValue(key);
        byte[] encoded = RpcCodec.encode(getValue);
        Dht.Message decoded = decode(encoded);
        assertEquals(Dht.Message.MessageType.GET_VALUE, decoded.getType());
        assertArrayEquals(key, decoded.getKey().toByteArray());
    }

    @Test
    void testEncodeDecodePutValue() {
        WireRecord record = new WireRecord(new byte[]{1, 2, 3}, new byte[]{4, 5, 6});
        Dht.Message putValue = RpcCodec.putValue(record);
        byte[] encoded = RpcCodec.encode(putValue);
        Dht.Message decoded = decode(encoded);
        assertEquals(Dht.Message.MessageType.PUT_VALUE, decoded.getType());
        assertTrue(decoded.hasRecord());
        assertArrayEquals(new byte[]{1, 2, 3}, decoded.getRecord().getKey().toByteArray());
        assertArrayEquals(new byte[]{4, 5, 6}, decoded.getRecord().getValue().toByteArray());
    }

    @Test
    void testEncodeDecodeAddProvider() {
        byte[] key = new byte[]{10, 20, 30};
        byte[] peerId = new byte[]{7, 8, 9};
        Dht.Message addProvider = RpcCodec.addProvider(key, peerId, List.of());
        byte[] encoded = RpcCodec.encode(addProvider);
        Dht.Message decoded = decode(encoded);
        assertEquals(Dht.Message.MessageType.ADD_PROVIDER, decoded.getType());
        assertArrayEquals(key, decoded.getKey().toByteArray());
        assertEquals(1, decoded.getProviderPeersCount());
        assertArrayEquals(peerId, decoded.getProviderPeers(0).getId().toByteArray());
    }

    @Test
    void testEncodeDecodeGetProviders() {
        byte[] key = new byte[]{10, 20, 30};
        Dht.Message getProviders = RpcCodec.getProviders(key);
        byte[] encoded = RpcCodec.encode(getProviders);
        Dht.Message decoded = decode(encoded);
        assertEquals(Dht.Message.MessageType.GET_PROVIDERS, decoded.getType());
        assertArrayEquals(key, decoded.getKey().toByteArray());
    }

    @Test
    void testEncodeDecodeGetProvidersResponse() {
        Dht.Message.Peer provider = Dht.Message.Peer.newBuilder()
                .setId(ByteString.copyFrom(new byte[]{1}))
                .setConnection(Dht.Message.ConnectionType.CONNECTED)
                .build();
        Dht.Message response = Dht.Message.newBuilder()
                .setType(Dht.Message.MessageType.GET_PROVIDERS)
                .addProviderPeers(provider)
                .build();
        byte[] encoded = RpcCodec.encode(response);
        Dht.Message decoded = decode(encoded);
        assertEquals(Dht.Message.MessageType.GET_PROVIDERS, decoded.getType());
        assertEquals(1, decoded.getProviderPeersCount());
    }

    @Test
    void testVarintFraming() {
        Dht.Message ping = RpcCodec.ping();
        byte[] encoded = RpcCodec.encode(ping);
        assertNotNull(encoded);
        assertTrue(encoded.length > 0);
        Dht.Message decoded = decode(encoded);
        assertEquals(ping.getType(), decoded.getType());
    }

    @Test
    void testMalformedInput() {
        assertThrows(Exception.class, () -> {
            Dht.Message.parseFrom(new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF});
        });
    }
}
