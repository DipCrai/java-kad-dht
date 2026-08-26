package com.libp2p.kademlia;

import com.libp2p.kademlia.routing.KBucketEntry;
import com.libp2p.kademlia.routing.KadPeer;
import com.libp2p.kademlia.routing.RoutingTable;
import io.libp2p.core.PeerId;
import org.junit.jupiter.api.RepeatedTest;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class PropertyTests {

    private static final int K = 20;
    private static final Random RNG = new Random();

    private byte[] randomKey() {
        byte[] key = new byte[XorId.KEY_LENGTH];
        RNG.nextBytes(key);
        return key;
    }

    @RepeatedTest(100)
    void testDistanceSymmetric() {
        byte[] a = randomKey();
        byte[] b = randomKey();
        byte[] dAb = XorId.xor(a, b);
        byte[] dBa = XorId.xor(b, a);
        assertArrayEquals(dAb, dBa);
    }

    @RepeatedTest(100)
    void testDistanceZero() {
        byte[] a = randomKey();
        byte[] dAa = XorId.xor(a, a);
        assertArrayEquals(new byte[XorId.KEY_LENGTH], dAa);
    }

    @RepeatedTest(100)
    void testDistanceNonNegative() {
        byte[] a = randomKey();
        byte[] b = randomKey();
        byte[] dist = XorId.xor(a, b);
        for (byte b1 : dist) {
            assertTrue((b1 & 0xFF) >= 0);
        }
    }

    @RepeatedTest(100)
    void testBucketIndexRange() {
        byte[] a = randomKey();
        byte[] b = randomKey();
        int idx = XorId.bucketIndex(a, b);
        assertTrue(idx >= 0 && idx < 256);
    }

    @RepeatedTest(100)
    void testClosestOrdering() {
        byte[] target = randomKey();
        PeerId localPeer = PeerId.random();
        RoutingTable rt = new RoutingTable(localPeer, K, 256);
        for (int i = 0; i < K * 3; i++) {
            rt.insert(PeerId.random(), List.of());
        }
        List<KadPeer> closest = rt.findClosest(target, K);
        for (int i = 1; i < closest.size(); i++) {
            byte[] dPrev = XorId.xor(target, XorId.fromPeerId(closest.get(i - 1).nodeId));
            byte[] dCurr = XorId.xor(target, XorId.fromPeerId(closest.get(i).nodeId));
            assertTrue(XorId.compareDistance(dPrev, dCurr) <= 0);
        }
    }

    @RepeatedTest(100)
    void testClosestMaxSize() {
        byte[] target = randomKey();
        PeerId localPeer = PeerId.random();
        RoutingTable rt = new RoutingTable(localPeer, K, 256);
        for (int i = 0; i < K * 5; i++) {
            rt.insert(PeerId.random(), List.of());
        }
        List<KadPeer> closest = rt.findClosest(target, K);
        assertTrue(closest.size() <= K);
    }

    @RepeatedTest(100)
    void testRoutingTableBucketSize() {
        PeerId localPeer = PeerId.random();
        RoutingTable rt = new RoutingTable(localPeer, K, 256);
        for (int i = 0; i < K * 10; i++) {
            rt.insert(PeerId.random(), List.of());
        }
        for (int i = 0; i < 256; i++) {
            assertTrue(rt.getBucket(i).size() <= K);
        }
    }

    @RepeatedTest(100)
    void testRoutingTableNoDuplicates() {
        PeerId localPeer = PeerId.random();
        RoutingTable rt = new RoutingTable(localPeer, K, 256);
        for (int i = 0; i < 50; i++) {
            rt.insert(PeerId.random(), List.of());
        }
        Set<PeerId> allFound = new HashSet<>();
        for (int i = 0; i < 256; i++) {
            for (KBucketEntry entry : rt.getBucket(i).getEntries()) {
                assertTrue(allFound.add(entry.peerId));
            }
        }
    }

    @RepeatedTest(100)
    void testRoutingTableSelfExclusion() {
        PeerId localPeer = PeerId.random();
        RoutingTable rt = new RoutingTable(localPeer, K, 256);
        rt.insert(localPeer, List.of());
        assertEquals(0, rt.size());
    }

    @RepeatedTest(100)
    void testXorIdFromPeerIdRoundtrip() {
        byte[] original = randomKey();
        PeerId peerId = XorId.toPeerId(original);
        byte[] recovered = XorId.fromPeerId(peerId);
        assertArrayEquals(original, recovered);
    }

    @RepeatedTest(100)
    void testGenerateRandomKeyInBucket() {
        byte[] selfKey = randomKey();
        int bucket = RNG.nextInt(256);
        byte[] generated = XorId.generateRandomKeyForBucket(selfKey, bucket);
        int actualBucket = XorId.bucketIndex(selfKey, generated);
        assertEquals(bucket, actualBucket);
    }
}
