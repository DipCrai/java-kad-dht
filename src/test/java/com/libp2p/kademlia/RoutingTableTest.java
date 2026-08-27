package com.libp2p.kademlia;

import com.libp2p.kademlia.routing.KadPeer;
import com.libp2p.kademlia.routing.RoutingTable;
import io.libp2p.core.PeerId;
import io.libp2p.core.multiformats.Multiaddr;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RoutingTableTest {

    private static final int K = 20;
    private PeerId localPeer;
    private RoutingTable rt;

    @BeforeEach
    void setUp() {
        localPeer = PeerId.random();
        rt = new RoutingTable(localPeer, K, Duration.ofSeconds(60));
    }

    @Test
    void testInsert() {
        PeerId remote = PeerId.random();
        rt.insert(remote, List.of());
        assertEquals(1, rt.size());
        assertTrue(rt.getAllPeers().contains(remote));
    }

    @Test
    void testSelfExclusion() {
        RoutingTable.InsertOutcome outcome = rt.insertOutcome(localPeer, List.of());
        assertEquals(RoutingTable.InsertOutcome.IGNORED, outcome);
        assertEquals(0, rt.size());
    }

    @Test
    void testFindClosest() {
        for (int i = 0; i < K + 5; i++) {
            rt.insert(PeerId.random(), List.of());
        }
        byte[] target = new byte[XorId.KEY_LENGTH];
        List<KadPeer> closest = rt.findClosest(target, K);
        assertEquals(K, closest.size(), "should return at most K peers");
    }

    @Test
    void testFindClosestEmpty() {
        byte[] target = new byte[XorId.KEY_LENGTH];
        List<KadPeer> closest = rt.findClosest(target, K);
        assertTrue(closest.isEmpty(), "empty routing table should return empty list");
    }

    @Test
    void testRemove() {
        PeerId remote = PeerId.random();
        rt.insert(remote, List.of());
        assertEquals(1, rt.size());

        var removed = rt.remove(remote);
        assertTrue(removed.isPresent());
        assertEquals(0, rt.size());
    }

    @Test
    void testInsertOutcome() {
        byte[] localKey = XorId.fromPeerId(localPeer);
        java.util.Random rng = new java.util.Random(42);
        for (int i = 0; i < K; i++) {
            rt.insertOutcome(randomPeerInBucket(localKey, 0, rng), List.of());
        }
        assertEquals(K, rt.size());
        PeerId extraPeer = randomPeerInBucket(localKey, 0, rng);
        RoutingTable.InsertOutcome outcome = rt.insertOutcome(extraPeer, List.of());
        assertTrue(outcome.needsPing(), "inserting into full bucket should need ping");
        assertNotNull(outcome.peerToPing());
    }

    private PeerId randomPeerInBucket(byte[] localKey, int bucket, java.util.Random rng) {
        byte[] raw = new byte[XorId.KEY_LENGTH];
        while (true) {
            rng.nextBytes(raw);
            PeerId peer = new PeerId(raw);
            if (XorId.bucketIndex(localKey, XorId.fromPeerId(peer)) == bucket) {
                return peer;
            }
        }
    }

    @Test
    void testGetAllPeers() {
        PeerId p1 = PeerId.random();
        PeerId p2 = PeerId.random();
        rt.insert(p1, List.of());
        rt.insert(p2, List.of());

        var all = rt.getAllPeers();
        assertEquals(2, all.size());
        assertTrue(all.contains(p1));
        assertTrue(all.contains(p2));
    }
}
