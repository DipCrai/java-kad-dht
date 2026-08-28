package com.libp2p.kademlia;

import com.libp2p.kademlia.config.KadConfig;
import com.libp2p.kademlia.config.KadMode;
import com.libp2p.kademlia.pb.Dht;
import com.libp2p.kademlia.protocol.RpcCodec;
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
class ReverseDirectionTest {

    private record NodeEntry(Host host, KadDht dht, PeerId peerId, Multiaddr addr) {}

    private NodeEntry createNode() {
        KadDht dht = new KadDht(KadConfig.builder()
                .mode(KadMode.SERVER)
                .kValue(20)
                .queryTimeout(Duration.ofSeconds(10))
                .writeQuorum(1)
                .readQuorum(1)
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

    private void connect(NodeEntry from, NodeEntry to) {
        from.host.getAddressBook().addAddrs(to.peerId, 60_000L, to.addr).join();
        from.dht.getRoutingTable().insert(to.peerId, List.of(to.addr));
    }

    private byte[] sha256Multihash(byte[] input) {
        try {
            java.security.MessageDigest digest = java.security.MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input);
            byte[] mh = new byte[34];
            mh[0] = 0x12;
            mh[1] = 0x20;
            System.arraycopy(hash, 0, mh, 2, 32);
            return mh;
        } catch (Exception e) { throw new RuntimeException(e); }
    }

    @Test
    void addProviderToRemoteGetProvidersFromRemote() throws Exception {
        NodeEntry sender = createNode();
        NodeEntry receiver = createNode();
        try {
            connect(sender, receiver);
            connect(receiver, sender);

            byte[] key = sha256Multihash("reverse-test-key".getBytes(StandardCharsets.UTF_8));

            Dht.Message addMsg = RpcCodec.addProvider(key, sender.host.getPeerId().getBytes(), sender.host.listenAddresses());
            Dht.Message resp = sender.dht.getProtocol().sendMessage(receiver.peerId, addMsg)
                    .get(10, TimeUnit.SECONDS);
            assertNotNull(resp);
            System.out.println("### REVERSE ADD_PROVIDER OK");

            Thread.sleep(500);

            var getResp = sender.dht.getProtocol().sendMessage(receiver.peerId,
                    RpcCodec.getProviders(key)).get(10, TimeUnit.SECONDS);
            System.out.println("### REVERSE GET_PROVIDERS providers=" + getResp.getProviderPeersCount());
            assertTrue(getResp.getProviderPeersCount() > 0, "should find provider from remote");
        } finally { sender.dht.close(); sender.host.stop().join(); receiver.dht.close(); receiver.host.stop().join(); }
    }

    @Test
    void addProviderViaChain() throws Exception {
        NodeEntry a = createNode();
        NodeEntry b = createNode();
        NodeEntry c = createNode();
        try {
            connect(a, b);
            connect(b, a);
            connect(b, c);
            connect(c, b);

            byte[] key = sha256Multihash("chain-test-key".getBytes(StandardCharsets.UTF_8));

            a.host.getAddressBook().addAddrs(a.host.getPeerId(), 60_000L, a.host.listenAddresses().get(0)).join();
            Dht.Message addMsg = RpcCodec.addProvider(key, a.host.getPeerId().getBytes(), a.host.listenAddresses());
            Dht.Message addResp = a.dht.getProtocol().sendMessage(b.peerId, addMsg).get(10, TimeUnit.SECONDS);
            assertNotNull(addResp);

            Thread.sleep(500);

            var getResp = c.dht.getProtocol().sendMessage(b.peerId,
                    RpcCodec.getProviders(key)).get(10, TimeUnit.SECONDS);
            System.out.println("### C->B GET_PROVIDERS providers=" + getResp.getProviderPeersCount());
            assertTrue(getResp.getProviderPeersCount() > 0, "C should find provider through B");
        } finally {
            a.dht.close(); a.host.stop().join();
            b.dht.close(); b.host.stop().join();
            c.dht.close(); c.host.stop().join();
        }
    }

    @Test
    void putValueFromRemoteGetValueLocally() throws Exception {
        NodeEntry sender = createNode();
        NodeEntry receiver = createNode();
        try {
            connect(sender, receiver);
            connect(receiver, sender);

            byte[] key = new byte[32];
            byte[] k = "reverse-put-key".getBytes(StandardCharsets.UTF_8);
            System.arraycopy(k, 0, key, 0, k.length);
            byte[] value = "reverse-put-value".getBytes(StandardCharsets.UTF_8);

            com.google.protobuf.ByteString bsKey = com.google.protobuf.ByteString.copyFrom(key);
            com.google.protobuf.ByteString bsValue = com.google.protobuf.ByteString.copyFrom(value);
            Dht.Record rec = Dht.Record.newBuilder()
                    .setKey(bsKey).setValue(bsValue).build();
            Dht.Message putMsg = Dht.Message.newBuilder()
                    .setType(Dht.Message.MessageType.PUT_VALUE)
                    .setKey(bsKey).setRecord(rec).setClusterLevelRaw(10).build();

            boolean sent = sender.dht.getProtocol().sendMessageFireAndForget(receiver.peerId, putMsg)
                    .get(10, TimeUnit.SECONDS);
            assertTrue(sent);

            Thread.sleep(500);

            var resp = sender.dht.getProtocol().sendMessage(receiver.peerId,
                    RpcCodec.getValue(key)).get(10, TimeUnit.SECONDS);
            assertTrue(resp.hasRecord(), "receiver should have the stored record");
            assertArrayEquals(value, resp.getRecord().getValue().toByteArray());
        } finally { sender.dht.close(); sender.host.stop().join(); receiver.dht.close(); receiver.host.stop().join(); }
    }
}
