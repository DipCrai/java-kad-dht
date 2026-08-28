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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(value = 180, unit = TimeUnit.SECONDS)
class MultiHopLookupTest {

    private record NodeEntry(Host host, KadDht dht, PeerId peerId, Multiaddr addr) {}

    private NodeEntry createNode() {
        KadDht dht = new KadDht(KadConfig.builder()
                .mode(KadMode.SERVER)
                .queryTimeout(Duration.ofSeconds(30))
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
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void twoHopFindNode() throws Exception {
        NodeEntry a = createNode();
        NodeEntry b = createNode();
        NodeEntry c = createNode();
        try {
            connect(a, b);
            connect(b, c);

            List<KadPeer> closest = a.dht.findNode(c.peerId).get(30, TimeUnit.SECONDS);
            System.out.println("### 2-HOP FIND_NODE: " + closest.size() + " peers");
            for (KadPeer p : closest) {
                System.out.println("  peer: " + p.nodeId);
            }
            boolean found = closest.stream().anyMatch(p -> p.nodeId.equals(c.peerId));
            assertTrue(found, "should find C through B (2 hops)");
        } finally {
            a.dht.close(); a.host.stop().join();
            b.dht.close(); b.host.stop().join();
            c.dht.close(); c.host.stop().join();
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void threeHopFindNode() throws Exception {
        NodeEntry a = createNode();
        NodeEntry b = createNode();
        NodeEntry c = createNode();
        NodeEntry d = createNode();
        try {
            connect(a, b);
            connect(b, c);
            connect(c, d);

            List<KadPeer> closest = a.dht.findNode(d.peerId).get(30, TimeUnit.SECONDS);
            System.out.println("### 3-HOP FIND_NODE: " + closest.size() + " peers");
            for (KadPeer p : closest) {
                System.out.println("  peer: " + p.nodeId);
            }
            boolean found = closest.stream().anyMatch(p -> p.nodeId.equals(d.peerId));
            assertTrue(found, "should find D through B→C (3 hops)");
        } finally {
            a.dht.close(); a.host.stop().join();
            b.dht.close(); b.host.stop().join();
            c.dht.close(); c.host.stop().join();
            d.dht.close(); d.host.stop().join();
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void twoHopGetValue() throws Exception {
        NodeEntry a = createNode();
        NodeEntry b = createNode();
        NodeEntry c = createNode();
        try {
            connect(a, b);
            connect(b, c);
            connect(c, b);
            connect(c, a);

            byte[] key = new byte[32];
            byte[] k = "multihop-key".getBytes();
            System.arraycopy(k, 0, key, 0, k.length);
            byte[] value = "multi-hop-value".getBytes();

            c.dht.putValue(key, value).get(10, TimeUnit.SECONDS);

            com.libp2p.kademlia.records.Record got = a.dht.getValue(key).get(30, TimeUnit.SECONDS);
            System.out.println("### 2-HOP GET_VALUE: " + (got != null));
            assertNotNull(got, "should get value through 2 hops");
            assertArrayEquals(value, got.getValue());
        } finally {
            a.dht.close(); a.host.stop().join();
            b.dht.close(); b.host.stop().join();
            c.dht.close(); c.host.stop().join();
        }
    }
}
