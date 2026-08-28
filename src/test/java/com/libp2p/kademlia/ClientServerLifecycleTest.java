package com.libp2p.kademlia;

import com.libp2p.kademlia.config.KadConfig;
import com.libp2p.kademlia.config.KadMode;
import com.libp2p.kademlia.routing.KadPeer;
import io.libp2p.core.Host;
import io.libp2p.core.PeerId;
import io.libp2p.core.dsl.HostBuilder;
import io.libp2p.core.mux.StreamMuxerProtocol;
import io.libp2p.core.multiformats.Multiaddr;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(value = 60, unit = TimeUnit.SECONDS)
class ClientServerLifecycleTest {

    private record NodeEntry(Host host, KadDht dht, PeerId peerId, Multiaddr addr) {}

    private NodeEntry createNode(KadMode mode) {
        KadDht dht = new KadDht(KadConfig.builder()
                .mode(mode)
                .kValue(20)
                .queryTimeout(Duration.ofSeconds(10))
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

    @Test
    void serverAcceptsInboundPing() throws Exception {
        NodeEntry server = createNode(KadMode.SERVER);
        NodeEntry client = createNode(KadMode.CLIENT);
        try {
            connect(client, server);
            boolean ok = client.dht.ping(server.peerId).get(10, TimeUnit.SECONDS);
            assertTrue(ok, "client should ping server successfully");
        } finally { server.dht.close(); server.host.stop().join(); client.dht.close(); client.host.stop().join(); }
    }

    @Test
    void clientModeDoesNotAdvertiseKad() {
        NodeEntry client = createNode(KadMode.CLIENT);
        try {
            assertFalse(client.dht.getProtocol().isServerMode(), "client mode should not be server");
        } finally { client.dht.close(); client.host.stop().join(); }
    }

    @Test
    void serverModeAdvertisesKad() {
        NodeEntry server = createNode(KadMode.SERVER);
        try {
            assertTrue(server.dht.getProtocol().isServerMode(), "server mode should be server");
        } finally { server.dht.close(); server.host.stop().join(); }
    }

    @Test
    void serverToServerPing() throws Exception {
        NodeEntry a = createNode(KadMode.SERVER);
        NodeEntry b = createNode(KadMode.SERVER);
        try {
            connect(a, b);
            connect(b, a);
            assertTrue(a.dht.ping(b.peerId).get(10, TimeUnit.SECONDS));
            assertTrue(b.dht.ping(a.peerId).get(10, TimeUnit.SECONDS));
        } finally { a.dht.close(); a.host.stop().join(); b.dht.close(); b.host.stop().join(); }
    }

    @Test
    void closeStopsLifecycle() throws Exception {
        NodeEntry a = createNode(KadMode.SERVER);
        NodeEntry b = createNode(KadMode.SERVER);
        try {
            connect(a, b);
            assertTrue(a.dht.ping(b.peerId).get(10, TimeUnit.SECONDS));

            a.dht.close();
            a.host.stop().join();

            boolean ok = b.dht.ping(a.peerId).get(5, TimeUnit.SECONDS);
            assertFalse(ok, "ping after close should fail");
        } finally {
            try { a.dht.close(); a.host.stop().join(); } catch (Exception ignored) {}
            b.dht.close(); b.host.stop().join();
        }
    }

    @Test
    void findNodeAfterCloseReturnsEmpty() throws Exception {
        NodeEntry a = createNode(KadMode.SERVER);
        NodeEntry b = createNode(KadMode.SERVER);
        NodeEntry c = createNode(KadMode.SERVER);
        try {
            connect(a, b);
            connect(b, c);

            List<KadPeer> found = a.dht.findNode(c.peerId).get(10, TimeUnit.SECONDS);
            assertFalse(found.isEmpty(), "should find C before close");

            a.dht.close();
            a.host.stop().join();

            try {
                a.dht.findNode(c.peerId).get(5, TimeUnit.SECONDS);
                fail("should throw after close");
            } catch (Exception expected) {}
        } finally {
            try { a.dht.close(); a.host.stop().join(); } catch (Exception ignored) {}
            b.dht.close(); b.host.stop().join();
            c.dht.close(); c.host.stop().join();
        }
    }

    @Test
    void concurrentPingsDoNotInterfere() throws Exception {
        NodeEntry server = createNode(KadMode.SERVER);
        NodeEntry c1 = createNode(KadMode.CLIENT);
        NodeEntry c2 = createNode(KadMode.CLIENT);
        try {
            connect(c1, server);
            connect(c2, server);

            CompletableFuture<Boolean> f1 = c1.dht.ping(server.peerId);
            CompletableFuture<Boolean> f2 = c2.dht.ping(server.peerId);

            assertTrue(f1.get(10, TimeUnit.SECONDS));
            assertTrue(f2.get(10, TimeUnit.SECONDS));
        } finally {
            server.dht.close(); server.host.stop().join();
            c1.dht.close(); c1.host.stop().join();
            c2.dht.close(); c2.host.stop().join();
        }
    }
}
