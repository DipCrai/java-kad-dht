package com.libp2p.kademlia;

import com.libp2p.kademlia.config.KadConfig;
import com.libp2p.kademlia.lookup.IterativeLookup;
import com.libp2p.kademlia.records.MemoryProviderStore;
import com.libp2p.kademlia.records.MemoryRecordStore;
import com.libp2p.kademlia.records.Record;
import com.libp2p.kademlia.records.RecordValidator;
import com.libp2p.kademlia.records.ProviderRecord;
import com.libp2p.kademlia.routing.KadPeer;
import com.libp2p.kademlia.routing.RoutingTable;
import io.libp2p.core.PeerId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class ChurnTest {

    private static final int K = 20;

    static class SimulatedNode {
        final PeerId peerId;
        final RoutingTable routingTable;
        final MemoryRecordStore recordStore;
        final MemoryProviderStore providerStore;

        SimulatedNode(PeerId peerId) {
            this.peerId = peerId;
            this.routingTable = new RoutingTable(peerId, K, Duration.ofSeconds(60));
            this.recordStore = new MemoryRecordStore(1024, 65536, Duration.ofHours(48), RecordValidator.NOOP);
            this.providerStore = new MemoryProviderStore(1024, 20);
        }
    }

    static class SimulatedNetwork {
        final Map<PeerId, SimulatedNode> nodes = new LinkedHashMap<>();

        void addNode(SimulatedNode node) {
            nodes.put(node.peerId, node);
            for (SimulatedNode other : nodes.values()) {
                if (!other.peerId.equals(node.peerId)) {
                    other.routingTable.insert(node.peerId, List.of());
                    node.routingTable.insert(other.peerId, List.of());
                }
            }
        }

        void removeNode(PeerId peerId) {
            nodes.remove(peerId);
            for (SimulatedNode node : nodes.values()) {
                node.routingTable.remove(peerId);
            }
        }

        void replicateRecord(byte[] key, byte[] value, int replicationFactor) {
            SimulatedNode putter = nodes.values().iterator().next();
            Record record = new Record(key, value, putter.peerId.getBytes(), null);
            putter.recordStore.put(record);
            List<KadPeer> closest = putter.routingTable.findClosest(key, replicationFactor);
            for (KadPeer p : closest) {
                SimulatedNode target = nodes.get(p.nodeId);
                if (target != null) {
                    target.recordStore.put(record);
                }
            }
        }

        void provideKey(byte[] key, PeerId providerPeer) {
            Instant now = Instant.now();
            ProviderRecord pr = new ProviderRecord(key, providerPeer,
                    now.plus(Duration.ofHours(48)), now.plus(Duration.ofMinutes(30)), List.of());
            SimulatedNode providerNode = nodes.get(providerPeer);
            if (providerNode != null) {
                providerNode.providerStore.addProvider(pr);
            }
            SimulatedNode requester = nodes.values().iterator().next();
            List<KadPeer> closest = requester.routingTable.findClosest(key, K);
            for (KadPeer p : closest) {
                SimulatedNode target = nodes.get(p.nodeId);
                if (target != null) {
                    target.providerStore.addProvider(pr);
                }
            }
        }

        Record findRecord(byte[] key) {
            for (SimulatedNode node : nodes.values()) {
                Record r = node.recordStore.get(key);
                if (r != null) return r;
            }
            return null;
        }

        boolean nodeHasRecord(PeerId peerId, byte[] key) {
            SimulatedNode node = nodes.get(peerId);
            return node != null && node.recordStore.get(key) != null;
        }

        List<ProviderRecord> findProviders(byte[] key) {
            List<ProviderRecord> all = new ArrayList<>();
            for (SimulatedNode node : nodes.values()) {
                all.addAll(node.providerStore.getProviders(key));
            }
            return all;
        }

        boolean nodeHasProvider(PeerId peerId, byte[] key) {
            SimulatedNode node = nodes.get(peerId);
            return node != null && !node.providerStore.getProviders(key).isEmpty();
        }
    }

    @Test
    void testRecordSurvivesChurn() {
        SimulatedNetwork network = new SimulatedNetwork();
        List<PeerId> peerIds = new ArrayList<>();
        List<SimulatedNode> nodes = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            SimulatedNode node = new SimulatedNode(PeerId.random());
            nodes.add(node);
            peerIds.add(node.peerId);
            network.addNode(node);
        }

        byte[] key = XorId.sha256("churn-test-key".getBytes());
        byte[] value = "important-data".getBytes();
        network.replicateRecord(key, value, 5);

        assertTrue(network.nodeHasRecord(peerIds.get(0), key));
        assertTrue(network.findRecord(key) != null);

        network.removeNode(peerIds.get(2));
        network.removeNode(peerIds.get(3));

        Record found = network.findRecord(key);
        assertNotNull(found, "record should survive after killing nodes 2 and 3");
        assertArrayEquals(value, found.getValue());
    }

    @Test
    void testProviderSurvivesChurn() {
        SimulatedNetwork network = new SimulatedNetwork();
        List<PeerId> peerIds = new ArrayList<>();
        List<SimulatedNode> nodes = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            SimulatedNode node = new SimulatedNode(PeerId.random());
            nodes.add(node);
            peerIds.add(node.peerId);
            network.addNode(node);
        }

        byte[] key = XorId.sha256("provider-churn-key".getBytes());
        network.provideKey(key, peerIds.get(0));

        List<ProviderRecord> providers = network.findProviders(key);
        assertFalse(providers.isEmpty(), "provider should be stored after providing");

        network.removeNode(peerIds.get(1));

        List<ProviderRecord> survivingProviders = network.findProviders(key);
        assertFalse(survivingProviders.isEmpty(),
                "provider should survive after killing node 1");
    }

    @Test
    void testRoutingConvergesAfterChurn() {
        PeerId localPeer = PeerId.random();
        RoutingTable rt = new RoutingTable(localPeer, K, Duration.ofSeconds(60));
        List<PeerId> inserted = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            PeerId p = PeerId.random();
            rt.insert(p, List.of());
            inserted.add(p);
        }
        assertEquals(20, rt.size());

        for (int i = 0; i < 5; i++) {
            rt.remove(inserted.get(i));
        }
        assertEquals(15, rt.size());

        byte[] target = new byte[32];
        new Random(42).nextBytes(target);
        List<KadPeer> closest = rt.findClosest(target, K);
        assertTrue(closest.size() >= K - 5,
                "routing table should still provide close peers after churn, got " + closest.size());
    }

    @Test
    void testLookupTerminatesWithFailures() {
        byte[] target = new byte[32];
        new Random().nextBytes(target);

        List<KadPeer> seed = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            seed.add(new KadPeer(PeerId.random(), List.of(), KadPeer.ConnectionType.CONNECTED));
        }

        IterativeLookup lookup = new IterativeLookup(target, seed, 20, 3, 3,
                Duration.ofSeconds(10), null);

        AtomicInteger failCount = new AtomicInteger(0);
        Random rng = new Random(12345);

        long start = System.currentTimeMillis();
        while (!lookup.isFinished()) {
            PeerId next = lookup.next();
            if (next == null) break;
            if (rng.nextDouble() < 0.3) {
                lookup.onFailure(next);
                failCount.incrementAndGet();
            } else {
                lookup.onResponse(next, List.of());
            }
        }
        long elapsed = System.currentTimeMillis() - start;

        assertTrue(lookup.isFinished(), "lookup must terminate even with failures");
        assertTrue(elapsed < 30000, "lookup should terminate within 30s, took " + elapsed + "ms");
    }
}
