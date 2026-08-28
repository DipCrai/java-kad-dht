package com.libp2p.kademlia;

import com.libp2p.kademlia.config.KadConfig;
import com.libp2p.kademlia.config.KadMode;
import com.libp2p.kademlia.records.Record;
import com.libp2p.kademlia.records.ProviderRecord;
import com.libp2p.kademlia.routing.KadPeer;
import io.libp2p.core.Host;
import io.libp2p.core.PeerId;
import io.libp2p.core.dsl.HostBuilder;
import io.libp2p.core.mux.StreamMuxerProtocol;
import io.libp2p.core.multiformats.Multiaddr;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 120, unit = TimeUnit.SECONDS)
class InteropNetworkTest {

    private static final List<NodeRef> NODES = new ArrayList<>();

    private static final class NodeRef {
        final PeerId peerId;
        final Multiaddr addr;
        NodeRef(PeerId peerId, Multiaddr addr) {
            this.peerId = peerId;
            this.addr = addr;
        }
    }

    @BeforeAll
    static void readNodes() {
        String raw = System.getProperty("interop.nodes");
        Assumptions.assumeTrue(raw != null && !raw.isEmpty(),
                "set -Dinterop.nodes=\"/ip4/../p2p/...,/ip4/../p2p/...\" to run live-network interop");
        for (String ma : raw.split(",")) {
            ma = ma.trim();
            if (ma.isEmpty()) continue;
            Multiaddr addr = Multiaddr.fromString(ma);
            NODES.add(new NodeRef(addr.getPeerId(), addr));
        }
        Assumptions.assumeTrue(!NODES.isEmpty(), "at least one interop node required");
    }

    private Host newServerHost(KadDht dht) {
        Host host = new HostBuilder()
                .muxer(() -> StreamMuxerProtocol.getYamux())
                .listen("/ip4/127.0.0.1/tcp/0")
                .build();
        host.start().join();
        dht.setHost(host);
        return host;
    }

    private void registerOwn(Host host, KadDht dht, NodeRef n) {
        host.getAddressBook().addAddrs(n.peerId, 600_000L, n.addr).join();
        dht.getRoutingTable().insert(n.peerId, List.of(n.addr));
    }

    private void registerAll(Host host, KadDht dht) {
        for (NodeRef n : NODES) registerOwn(host, dht, n);
    }

    private static byte[] key(String prefix) {
        byte[] k = new byte[32];
        byte[] p = prefix.getBytes(StandardCharsets.UTF_8);
        System.arraycopy(p, 0, k, 0, Math.min(p.length, k.length));
        return k;
    }

    @Test
    @Timeout(value = 100, unit = TimeUnit.SECONDS)
    void nodeIsDialableAndPingable() {
        KadDht dht = new KadDht(KadConfig.builder().mode(KadMode.CLIENT).build());
        Host host = newServerHost(dht);
        try {
            for (NodeRef n : NODES) {
                host.getAddressBook().addAddrs(n.peerId, 60_000L, n.addr).join();
                boolean ok = dht.ping(n.peerId).join();
                assertTrue(ok, "node should be dialable/pingable: " + n.addr);
            }
        } finally {
            dht.close();
            try { host.stop().join(); } catch (Exception ignored) {
            }
        }
    }

    @Test
    @Timeout(value = 100, unit = TimeUnit.SECONDS)
    void findNodeInterop() {
        KadConfig cfg = KadConfig.builder()
                .mode(KadMode.SERVER)
                .queryTimeout(Duration.ofSeconds(15))
                .build();
        KadDht dht = new KadDht(cfg);
        Host host = newServerHost(dht);
        dht.start();
        try {
            registerAll(host, dht);
            for (NodeRef n : NODES) {
                List<KadPeer> closest = dht.findNode(n.peerId).join();
                assertFalse(closest.isEmpty(), "findNode should return peers for " + n.addr);
                boolean foundSelf = closest.stream().anyMatch(p -> p.nodeId.equals(n.peerId));
                assertTrue(foundSelf, "closest should contain the target node itself: " + n.addr);
            }
        } finally {
            dht.close();
            try { host.stop().join(); } catch (Exception ignored) {
            }
        }
    }

    @Test
    @Timeout(value = 100, unit = TimeUnit.SECONDS)
    void putGetValueInterop() {
        KadConfig cfg = KadConfig.builder()
                .mode(KadMode.SERVER)
                .queryTimeout(Duration.ofSeconds(15))
                .writeQuorum(1)
                .readQuorum(1)
                .build();
        KadDht dht = new KadDht(cfg);
        Host host = newServerHost(dht);
        dht.start();
        try {
            registerAll(host, dht);
            byte[] k = key("interopkey");
            byte[] value = "interop-value-from-java".getBytes(StandardCharsets.UTF_8);

            boolean stored = dht.putValue(k, value).join();
            assertTrue(stored, "putValue should succeed against remote node(s)");

            Record got = dht.getValue(k).join();
            assertNotNull(got, "getValue should retrieve the stored record from remote node(s)");
            assertArrayEquals(value, got.getValue(), "retrieved record value should match");
        } finally {
            dht.close();
            try { host.stop().join(); } catch (Exception ignored) {
            }
        }
    }

    @Test
    @Timeout(value = 100, unit = TimeUnit.SECONDS)
    void provideFindProvidersInterop() {
        KadConfig cfg = KadConfig.builder()
                .mode(KadMode.SERVER)
                .queryTimeout(Duration.ofSeconds(15))
                .writeQuorum(1)
                .readQuorum(1)
                .build();
        KadDht dht = new KadDht(cfg);
        Host host = newServerHost(dht);
        dht.start();
        try {
            registerAll(host, dht);
            byte[] k = key("provkey");
            boolean provided = dht.provide(k).join();
            assertTrue(provided, "provide should succeed against remote node(s)");

            List<ProviderRecord> providers = dht.findProviders(k).join();
            assertFalse(providers.isEmpty(), "findProviders should return at least one provider");

            boolean self = providers.stream().anyMatch(p -> p.getProvider().equals(host.getPeerId()));
            assertTrue(self, "findProviders should include this node as the provider (got: "
                    + Arrays.toString(providers.stream().map(p -> p.getProvider().toBase58()).toArray()) + ")");
        } finally {
            dht.close();
            try { host.stop().join(); } catch (Exception ignored) {
            }
        }
    }
}
