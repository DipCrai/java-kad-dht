package com.libp2p.kademlia;

import com.libp2p.kademlia.lookup.IterativeLookup;
import com.libp2p.kademlia.routing.KadPeer;
import io.libp2p.core.PeerId;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class DisjointLookupPathsTest {

    private IterativeLookup lookup(byte[] target, List<PeerId> seeds) {
        List<KadPeer> seedPeers = seeds.stream()
                .map(p -> new KadPeer(p, List.of(), KadPeer.ConnectionType.CONNECTED))
                .toList();
        return new IterativeLookup(target, seedPeers, 20, 3, 3,
                Duration.ofSeconds(5), null);
    }

    @Test
    void testSucceededPeerInPathAExcludedFromPathB() {
        byte[] target = new byte[32];
        new java.util.Random(1).nextBytes(target);

        PeerId x = PeerId.random();
        PeerId y = PeerId.random();

        IterativeLookup pathA = lookup(target, List.of(x));
        IterativeLookup pathB = lookup(target, List.of(y));

        Set<PeerId> excluded = ConcurrentHashMap.newKeySet();
        pathA.setExcludedPeers(excluded);
        pathB.setExcludedPeers(excluded);

        pathA.onResponse(x, List.of());

        assertTrue(excluded.contains(x), "path A success should add peer to shared excluded set");

        pathB.onResponse(y, List.of(new KadPeer(x, List.of(), KadPeer.ConnectionType.NOT_CONNECTED)));

        Set<PeerId> pathBEntries = pathB.getAllPeerEntries().stream()
                .map(IterativeLookup.PeerEntry::getPeerId)
                .collect(Collectors.toSet());

        assertFalse(pathBEntries.contains(x), "peer succeeded in path A must not be in path B candidate set");
    }

    @Test
    void testSharedExcludedSetPreventsDuplicateQueriesAcrossPaths() {
        byte[] target = new byte[32];
        new java.util.Random(2).nextBytes(target);

        List<PeerId> peers = new java.util.ArrayList<>();
        for (int i = 0; i < 50; i++) peers.add(PeerId.random());

        IterativeLookup pathA = lookup(target, peers.subList(0, 20));
        IterativeLookup pathB = lookup(target, peers.subList(20, 40));

        Set<PeerId> excluded = ConcurrentHashMap.newKeySet();
        pathA.setExcludedPeers(excluded);
        pathB.setExcludedPeers(excluded);

        while (!pathA.isFinished()) {
            PeerId next = pathA.next();
            if (next == null) break;
            pathA.onResponse(next, java.util.List.of());
        }
        while (!pathB.isFinished()) {
            PeerId next = pathB.next();
            if (next == null) break;
            pathB.onResponse(next, java.util.List.of());
        }

        Set<PeerId> queriedA = pathA.getQueriedPeers().stream()
                .map(p -> p.nodeId)
                .collect(Collectors.toSet());
        Set<PeerId> queriedB = pathB.getQueriedPeers().stream()
                .map(p -> p.nodeId)
                .collect(Collectors.toSet());

        queriedA.retainAll(queriedB);
        assertTrue(queriedA.isEmpty(), "no peer should be queried in both disjoint paths, shared queried: " + queriedA);
    }

    @Test
    void testFailedPeerAlsoExcludedFromSiblingPath() {
        byte[] target = new byte[32];
        new java.util.Random(3).nextBytes(target);

        PeerId x = PeerId.random();
        PeerId y = PeerId.random();

        IterativeLookup pathA = lookup(target, List.of(x));
        IterativeLookup pathB = lookup(target, List.of(y));

        Set<PeerId> excluded = ConcurrentHashMap.newKeySet();
        pathA.setExcludedPeers(excluded);
        pathB.setExcludedPeers(excluded);

        pathA.onFailure(x);

        assertTrue(excluded.contains(x), "path A failure should also add peer to shared excluded set");

        pathB.onResponse(y, List.of(new KadPeer(x, List.of(), KadPeer.ConnectionType.NOT_CONNECTED)));

        Set<PeerId> pathBEntries = pathB.getAllPeerEntries().stream()
                .map(IterativeLookup.PeerEntry::getPeerId)
                .collect(Collectors.toSet());

        assertFalse(pathBEntries.contains(x));
    }
}
