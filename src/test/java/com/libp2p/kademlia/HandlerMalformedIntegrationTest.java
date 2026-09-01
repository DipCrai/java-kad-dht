package com.libp2p.kademlia;

import com.google.protobuf.ByteString;
import com.libp2p.kademlia.config.KadConfig;
import com.libp2p.kademlia.config.KadMode;
import com.libp2p.kademlia.pb.Dht;
import com.libp2p.kademlia.protocol.KademliaProtocol;
import com.libp2p.kademlia.protocol.RpcCodec;
import com.libp2p.kademlia.routing.KadPeer;
import io.libp2p.core.Host;
import io.libp2p.core.PeerId;
import io.libp2p.core.dsl.HostBuilder;
import io.libp2p.core.mux.StreamMuxerProtocol;
import io.libp2p.core.multiformats.Multiaddr;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(value = 60, unit = TimeUnit.SECONDS)
class HandlerMalformedIntegrationTest {

    private record NodeEntry(Host host, KadDht dht, PeerId peerId, Multiaddr addr) {}

    private NodeEntry createNode(int maxInbound) {
        KadDht dht = new KadDht(KadConfig.builder()
                .mode(KadMode.SERVER)
                .kValue(20)
                .maxInboundRequests(maxInbound)
                .queryTimeout(Duration.ofSeconds(5))
                .build());
        Host h = new HostBuilder()
                .muxer(StreamMuxerProtocol::getYamux)
                .listen("/ip4/127.0.0.1/tcp/0")
                .build();
        h.start().join();
        dht.setHost(h);
        dht.start();
        return new NodeEntry(h, dht, h.getPeerId(), h.listenAddresses().get(0));
    }

    private NodeEntry createNode() { return createNode(100); }

    private void connect(NodeEntry from, NodeEntry to) {
        from.host.getAddressBook().addAddrs(to.peerId, 60_000L, to.addr).join();
        from.dht.getRoutingTable().insert(to.peerId, List.of(to.addr));
    }

    @Test
    void findNodeEmptyKey() throws Exception {
        NodeEntry server = createNode();
        NodeEntry client = createNode();
        try {
            connect(client, server);
            byte[] key = new byte[0];
            Dht.Message msg = Dht.Message.newBuilder()
                    .setType(Dht.Message.MessageType.FIND_NODE)
                    .setKey(ByteString.copyFrom(key))
                    .build();
            Dht.Message resp = client.dht.getProtocol().sendMessage(server.peerId, msg)
                    .get(10, TimeUnit.SECONDS);
            assertNotNull(resp);
            assertEquals(Dht.Message.MessageType.FIND_NODE, resp.getType());
        } finally { server.dht.close(); server.host.stop().join(); client.dht.close(); client.host.stop().join(); }
    }

    @Test
    void findNodeOversizedKey() throws Exception {
        NodeEntry server = createNode();
        NodeEntry client = createNode();
        try {
            connect(client, server);
            byte[] key = new byte[256];
            Dht.Message msg = Dht.Message.newBuilder()
                    .setType(Dht.Message.MessageType.FIND_NODE)
                    .setKey(ByteString.copyFrom(key))
                    .build();
            Dht.Message resp = client.dht.getProtocol().sendMessage(server.peerId, msg)
                    .get(10, TimeUnit.SECONDS);
            assertNotNull(resp);
            assertEquals(Dht.Message.MessageType.FIND_NODE, resp.getType());
        } finally { server.dht.close(); server.host.stop().join(); client.dht.close(); client.host.stop().join(); }
    }

    @Test
    void putValueNoRecordField() throws Exception {
        NodeEntry server = createNode();
        NodeEntry client = createNode();
        try {
            connect(client, server);
            Dht.Message msg = Dht.Message.newBuilder()
                    .setType(Dht.Message.MessageType.PUT_VALUE)
                    .build();
            Dht.Message resp = client.dht.getProtocol().sendMessage(server.peerId, msg)
                    .get(10, TimeUnit.SECONDS);
            assertNotNull(resp);
            assertEquals(Dht.Message.MessageType.PUT_VALUE, resp.getType());
            assertFalse(resp.hasRecord(), "no record should be echoed for PUT_VALUE without record");

            Dht.Message ping = RpcCodec.ping();
            Dht.Message pingResp = client.dht.getProtocol().sendMessage(server.peerId, ping)
                    .get(10, TimeUnit.SECONDS);
            assertNotNull(pingResp, "connection should still work after malformed PUT_VALUE");
        } finally { server.dht.close(); server.host.stop().join(); client.dht.close(); client.host.stop().join(); }
    }

    @Test
    void putValueEmptyRecordKey() throws Exception {
        NodeEntry server = createNode();
        NodeEntry client = createNode();
        try {
            connect(client, server);
            Dht.Record rec = Dht.Record.newBuilder()
                    .setKey(ByteString.EMPTY)
                    .setValue(ByteString.copyFrom("val".getBytes()))
                    .build();
            Dht.Message msg = Dht.Message.newBuilder()
                    .setType(Dht.Message.MessageType.PUT_VALUE)
                    .setRecord(rec)
                    .build();
            Dht.Message resp = client.dht.getProtocol().sendMessage(server.peerId, msg)
                    .get(10, TimeUnit.SECONDS);
            assertNotNull(resp);
            assertEquals(Dht.Message.MessageType.PUT_VALUE, resp.getType());
            assertFalse(resp.hasRecord(), "empty key record should not be stored or echoed");

            byte[] checkKey = new byte[32];
            com.libp2p.kademlia.records.Record stored = server.dht.getRecordStore().get(checkKey);
            assertNull(stored, "empty key should not be stored");
        } finally { server.dht.close(); server.host.stop().join(); client.dht.close(); client.host.stop().join(); }
    }

    @Test
    void putValueEmptyValue() throws Exception {
        NodeEntry server = createNode();
        NodeEntry client = createNode();
        try {
            connect(client, server);
            byte[] key = new byte[32];
            Dht.Record rec = Dht.Record.newBuilder()
                    .setKey(ByteString.copyFrom(key))
                    .setValue(ByteString.EMPTY)
                    .build();
            Dht.Message msg = Dht.Message.newBuilder()
                    .setType(Dht.Message.MessageType.PUT_VALUE)
                    .setRecord(rec)
                    .build();
            Dht.Message resp = client.dht.getProtocol().sendMessage(server.peerId, msg)
                    .get(10, TimeUnit.SECONDS);
            assertNotNull(resp);
        } finally { server.dht.close(); server.host.stop().join(); client.dht.close(); client.host.stop().join(); }
    }

    @Test
    void addProviderEmptyKey() throws Exception {
        NodeEntry server = createNode();
        NodeEntry client = createNode();
        try {
            connect(client, server);
            Dht.Message msg = Dht.Message.newBuilder()
                    .setType(Dht.Message.MessageType.ADD_PROVIDER)
                    .setKey(ByteString.EMPTY)
                    .build();
            Dht.Message resp = client.dht.getProtocol().sendMessage(server.peerId, msg)
                    .get(10, TimeUnit.SECONDS);
            assertNotNull(resp);
            assertEquals(Dht.Message.MessageType.ADD_PROVIDER, resp.getType());

            Dht.Message ping = RpcCodec.ping();
            Dht.Message pingResp = client.dht.getProtocol().sendMessage(server.peerId, ping)
                    .get(10, TimeUnit.SECONDS);
            assertNotNull(pingResp, "connection should still work after malformed ADD_PROVIDER");
        } finally { server.dht.close(); server.host.stop().join(); client.dht.close(); client.host.stop().join(); }
    }

    @Test
    void addProviderOversizedKey() throws Exception {
        NodeEntry server = createNode();
        NodeEntry client = createNode();
        try {
            connect(client, server);
            byte[] bigKey = new byte[81];
            Dht.Message msg = Dht.Message.newBuilder()
                    .setType(Dht.Message.MessageType.ADD_PROVIDER)
                    .setKey(ByteString.copyFrom(bigKey))
                    .build();
            Dht.Message resp = client.dht.getProtocol().sendMessage(server.peerId, msg)
                    .get(10, TimeUnit.SECONDS);
            assertNotNull(resp);
        } finally { server.dht.close(); server.host.stop().join(); client.dht.close(); client.host.stop().join(); }
    }

    @Test
    void addProviderPeerIdMismatch() throws Exception {
        NodeEntry server = createNode();
        NodeEntry client = createNode();
        try {
            connect(client, server);
            byte[] key = new byte[]{1, 2, 3};
            byte[] fakeId = PeerId.random().getBytes();
            Dht.Message.Peer peer = Dht.Message.Peer.newBuilder()
                    .setId(ByteString.copyFrom(fakeId))
                    .setConnection(Dht.Message.ConnectionType.CONNECTED)
                    .build();
            Dht.Message msg = Dht.Message.newBuilder()
                    .setType(Dht.Message.MessageType.ADD_PROVIDER)
                    .setKey(ByteString.copyFrom(key))
                    .addProviderPeers(peer)
                    .build();
            Dht.Message resp = client.dht.getProtocol().sendMessage(server.peerId, msg)
                    .get(10, TimeUnit.SECONDS);
            assertNotNull(resp);
        } finally { server.dht.close(); server.host.stop().join(); client.dht.close(); client.host.stop().join(); }
    }

    @Test
    void getProvidersEmptyKey() throws Exception {
        NodeEntry server = createNode();
        NodeEntry client = createNode();
        try {
            connect(client, server);
            Dht.Message msg = Dht.Message.newBuilder()
                    .setType(Dht.Message.MessageType.GET_PROVIDERS)
                    .setKey(ByteString.EMPTY)
                    .build();
            Dht.Message resp = client.dht.getProtocol().sendMessage(server.peerId, msg)
                    .get(10, TimeUnit.SECONDS);
            assertNotNull(resp);
            assertEquals(Dht.Message.MessageType.GET_PROVIDERS, resp.getType());
        } finally { server.dht.close(); server.host.stop().join(); client.dht.close(); client.host.stop().join(); }
    }

    @Test
    void multipleRapidPings() throws Exception {
        NodeEntry server = createNode();
        NodeEntry client = createNode();
        try {
            connect(client, server);
            for (int i = 0; i < 10; i++) {
                boolean ok = client.dht.ping(server.peerId).get(5, TimeUnit.SECONDS);
                assertTrue(ok, "ping " + i + " should succeed");
            }
        } finally { server.dht.close(); server.host.stop().join(); client.dht.close(); client.host.stop().join(); }
    }

    @Test
    void putValueEchoPreservesKeyAndValue() throws Exception {
        NodeEntry server = createNode();
        NodeEntry client = createNode();
        try {
            connect(client, server);
            byte[] key = new byte[32];
            byte[] val = "test-value".getBytes(StandardCharsets.UTF_8);
            Dht.Record rec = Dht.Record.newBuilder()
                    .setKey(ByteString.copyFrom(key))
                    .setValue(ByteString.copyFrom(val))
                    .build();
            Dht.Message msg = Dht.Message.newBuilder()
                    .setType(Dht.Message.MessageType.PUT_VALUE)
                    .setKey(ByteString.copyFrom(key))
                    .setRecord(rec)
                    .build();
            Dht.Message resp = client.dht.getProtocol().sendMessage(server.peerId, msg)
                    .get(10, TimeUnit.SECONDS);
            assertNotNull(resp);
            assertTrue(resp.hasRecord());
            assertArrayEquals(key, resp.getRecord().getKey().toByteArray());
            assertArrayEquals(val, resp.getRecord().getValue().toByteArray());
        } finally { server.dht.close(); server.host.stop().join(); client.dht.close(); client.host.stop().join(); }
    }

    @Test
    void getValueReturnsStoredRecord() throws Exception {
        NodeEntry server = createNode();
        NodeEntry client = createNode();
        try {
            connect(client, server);
            byte[] key = new byte[32];
            byte[] val = "stored-value".getBytes(StandardCharsets.UTF_8);
            server.dht.getRecordStore().put(new com.libp2p.kademlia.records.Record(key, val));

            Dht.Message msg = Dht.Message.newBuilder()
                    .setType(Dht.Message.MessageType.GET_VALUE)
                    .setKey(ByteString.copyFrom(key))
                    .build();
            Dht.Message resp = client.dht.getProtocol().sendMessage(server.peerId, msg)
                    .get(10, TimeUnit.SECONDS);
            assertNotNull(resp);
            assertTrue(resp.hasRecord());
            assertArrayEquals(val, resp.getRecord().getValue().toByteArray());
        } finally { server.dht.close(); server.host.stop().join(); client.dht.close(); client.host.stop().join(); }
    }

    @Test
    void findNodeReturnsClosestPeers() throws Exception {
        NodeEntry server1 = createNode();
        NodeEntry server2 = createNode();
        NodeEntry client = createNode();
        try {
            connect(client, server1);
            connect(client, server2);
            connect(server1, server2);

            List<KadPeer> closest = client.dht.findNode(server2.peerId).get(10, TimeUnit.SECONDS);
            assertNotNull(closest);
            assertFalse(closest.isEmpty());
            boolean found = closest.stream().anyMatch(p -> p.nodeId.equals(server2.peerId));
            assertTrue(found, "should find server2 in closer peers");
        } finally {
            server1.dht.close(); server1.host.stop().join();
            server2.dht.close(); server2.host.stop().join();
            client.dht.close(); client.host.stop().join();
        }
    }

    @Test
    void getValueExpiredRecordReturnsNoRecord() throws Exception {
        NodeEntry server = createNode();
        NodeEntry client = createNode();
        try {
            connect(client, server);
            byte[] key = new byte[32];
            byte[] val = "expired-value".getBytes(StandardCharsets.UTF_8);
            com.libp2p.kademlia.records.Record rec =
                    new com.libp2p.kademlia.records.Record(key, val, null, java.time.Instant.now().minusSeconds(3600));
            rec.setTimeReceived(java.time.Instant.now().minusSeconds(7200));
            server.dht.getRecordStore().put(rec);

            Dht.Message msg = Dht.Message.newBuilder()
                    .setType(Dht.Message.MessageType.GET_VALUE)
                    .setKey(ByteString.copyFrom(key))
                    .build();
            Dht.Message resp = client.dht.getProtocol().sendMessage(server.peerId, msg)
                    .get(10, TimeUnit.SECONDS);
            assertNotNull(resp);
            assertEquals(Dht.Message.MessageType.GET_VALUE, resp.getType());
            assertFalse(resp.hasRecord(), "expired record should not be returned by GET_VALUE");
        } finally { server.dht.close(); server.host.stop().join(); client.dht.close(); client.host.stop().join(); }
    }

    @Test
    void malformedRequestDoesNotBreakConnection() throws Exception {
        NodeEntry server = createNode();
        NodeEntry client = createNode();
        try {
            connect(client, server);

            Dht.Message garbage = Dht.Message.newBuilder()
                    .setType(Dht.Message.MessageType.FIND_NODE)
                    .setKey(ByteString.copyFrom(new byte[512]))
                    .build();
            client.dht.getProtocol().sendMessage(server.peerId, garbage).get(5, TimeUnit.SECONDS);

            Dht.Message ping = RpcCodec.ping();
            Dht.Message resp = client.dht.getProtocol().sendMessage(server.peerId, ping)
                    .get(5, TimeUnit.SECONDS);
            assertNotNull(resp, "ping after malformed request should succeed");

            byte[] key = new byte[32];
            byte[] val = "after-malformed".getBytes(StandardCharsets.UTF_8);
            Dht.Record rec = Dht.Record.newBuilder()
                    .setKey(ByteString.copyFrom(key))
                    .setValue(ByteString.copyFrom(val))
                    .build();
            Dht.Message putMsg = Dht.Message.newBuilder()
                    .setType(Dht.Message.MessageType.PUT_VALUE)
                    .setKey(ByteString.copyFrom(key))
                    .setRecord(rec)
                    .build();
            Dht.Message putResp = client.dht.getProtocol().sendMessage(server.peerId, putMsg)
                    .get(5, TimeUnit.SECONDS);
            assertNotNull(putResp, "PUT_VALUE after malformed request should succeed");
            assertTrue(putResp.hasRecord());
        } finally { server.dht.close(); server.host.stop().join(); client.dht.close(); client.host.stop().join(); }
    }
}
