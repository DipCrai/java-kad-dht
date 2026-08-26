package com.libp2p.kademlia.protocol;

import com.google.protobuf.ByteString;
import com.libp2p.kademlia.pb.Dht;
import com.libp2p.kademlia.records.MemoryProviderStore;
import com.libp2p.kademlia.records.MemoryRecordStore;
import com.libp2p.kademlia.records.RecordValidator;
import com.libp2p.kademlia.routing.RoutingTable;
import io.libp2p.core.PeerId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MultihashValidationTest {

    private KademliaProtocol protocol;
    private RoutingTable routingTable;
    private MemoryProviderStore providerStore;
    private PeerId selfPeer;

    @BeforeEach
    void setUp() {
        selfPeer = PeerId.random();
        routingTable = new RoutingTable(selfPeer, 20, Duration.ofSeconds(60));
        providerStore = new MemoryProviderStore(1024, 20);

        protocol = new KademliaProtocol("/ipfs/kad/1.0.0", 20,
                Duration.ofSeconds(10), Duration.ofHours(48), Duration.ofMinutes(30), 100);
        protocol.setRoutingTable(routingTable);
        protocol.setRecordStore(new MemoryRecordStore(1024, 65536, Duration.ofHours(48), RecordValidator.NOOP));
        protocol.setProviderStore(providerStore);
    }

    private byte[] createMultihashKey(int hashFuncCode, int digestLength) {
        List<Byte> keyList = new ArrayList<>();
        encodeVarint(hashFuncCode, keyList);
        encodeVarint(digestLength, keyList);
        byte[] digest = new byte[digestLength];
        for (int i = 0; i < digestLength; i++) digest[i] = (byte) i;
        for (byte b : digest) keyList.add(b);
        byte[] key = new byte[keyList.size()];
        for (int i = 0; i < keyList.size(); i++) key[i] = keyList.get(i);
        return key;
    }

    private void encodeVarint(int value, List<Byte> out) {
        while ((value & ~0x7F) != 0) {
            out.add((byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        out.add((byte) value);
    }

    @Test
    void testValidSHA256() {
        byte[] key = createMultihashKey(0x12, 32);
        Dht.Message.Peer selfPeerMsg = Dht.Message.Peer.newBuilder()
                .setId(ByteString.copyFrom(selfPeer.getBytes()))
                .setConnection(Dht.Message.ConnectionType.CONNECTED)
                .build();
        Dht.Message req = Dht.Message.newBuilder()
                .setType(Dht.Message.MessageType.ADD_PROVIDER)
                .setKey(ByteString.copyFrom(key))
                .addProviderPeers(selfPeerMsg)
                .build();

        Dht.Message response = protocol.handleAddProvider(req, selfPeer);
        assertEquals(Dht.Message.MessageType.ADD_PROVIDER, response.getType());
        assertFalse(providerStore.getProviders(key).isEmpty(), "valid multihash key should be accepted");
    }

    @Test
    void testValidMultihash() {
        byte[] key = createMultihashKey(0x17, 20);
        Dht.Message.Peer selfPeerMsg = Dht.Message.Peer.newBuilder()
                .setId(ByteString.copyFrom(selfPeer.getBytes()))
                .setConnection(Dht.Message.ConnectionType.CONNECTED)
                .build();
        Dht.Message req = Dht.Message.newBuilder()
                .setType(Dht.Message.MessageType.ADD_PROVIDER)
                .setKey(ByteString.copyFrom(key))
                .addProviderPeers(selfPeerMsg)
                .build();

        protocol.handleAddProvider(req, selfPeer);
        assertFalse(providerStore.getProviders(key).isEmpty());
    }

    @Test
    void testInvalidTooShort() {
        byte[] key = new byte[]{0x12};
        Dht.Message.Peer selfPeerMsg = Dht.Message.Peer.newBuilder()
                .setId(ByteString.copyFrom(selfPeer.getBytes()))
                .setConnection(Dht.Message.ConnectionType.CONNECTED)
                .build();
        Dht.Message req = Dht.Message.newBuilder()
                .setType(Dht.Message.MessageType.ADD_PROVIDER)
                .setKey(ByteString.copyFrom(key))
                .addProviderPeers(selfPeerMsg)
                .build();

        protocol.handleAddProvider(req, selfPeer);
        assertTrue(providerStore.getProviders(key).isEmpty(), "too short key should be rejected");
    }

    @Test
    void testInvalidGarbage() {
        byte[] key = new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF};
        Dht.Message.Peer selfPeerMsg = Dht.Message.Peer.newBuilder()
                .setId(ByteString.copyFrom(selfPeer.getBytes()))
                .setConnection(Dht.Message.ConnectionType.CONNECTED)
                .build();
        Dht.Message req = Dht.Message.newBuilder()
                .setType(Dht.Message.MessageType.ADD_PROVIDER)
                .setKey(ByteString.copyFrom(key))
                .addProviderPeers(selfPeerMsg)
                .build();

        protocol.handleAddProvider(req, selfPeer);
        assertTrue(providerStore.getProviders(key).isEmpty(), "garbage key should be rejected");
    }

    @Test
    void testInvalidWrongLength() {
        List<Byte> keyList = new ArrayList<>();
        encodeVarint(0x12, keyList);
        encodeVarint(32, keyList);
        byte[] digest = new byte[16];
        for (byte b : digest) keyList.add(b);
        byte[] key = new byte[keyList.size()];
        for (int i = 0; i < keyList.size(); i++) key[i] = keyList.get(i);

        Dht.Message.Peer selfPeerMsg = Dht.Message.Peer.newBuilder()
                .setId(ByteString.copyFrom(selfPeer.getBytes()))
                .setConnection(Dht.Message.ConnectionType.CONNECTED)
                .build();
        Dht.Message req = Dht.Message.newBuilder()
                .setType(Dht.Message.MessageType.ADD_PROVIDER)
                .setKey(ByteString.copyFrom(key))
                .addProviderPeers(selfPeerMsg)
                .build();

        protocol.handleAddProvider(req, selfPeer);
        assertTrue(providerStore.getProviders(key).isEmpty(), "wrong length key should be rejected");
    }
}
