package com.libp2p.kademlia;

import com.libp2p.kademlia.lookup.IterativeLookup;
import com.libp2p.kademlia.protocol.KademliaProtocol;
import com.libp2p.kademlia.records.Record;
import com.libp2p.kademlia.routing.KadPeer;
import io.libp2p.core.PeerId;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class LookupPropertyTest {

    @Test
    void testLookupTerminates() {
        byte[] target = new byte[32];
        new Random().nextBytes(target);

        for (int k : new int[]{5, 10, 20}) {
            List<KadPeer> seed = new ArrayList<>();
            for (int i = 0; i < k; i++) {
                seed.add(new KadPeer(PeerId.random(), List.of(), KadPeer.ConnectionType.CONNECTED));
            }

            IterativeLookup lookup = new IterativeLookup(target, seed, k, 3, 3,
                    Duration.ofSeconds(5), null);

            long start = System.currentTimeMillis();
            while (!lookup.isFinished()) {
                PeerId next = lookup.next();
                if (next == null) break;
                lookup.onFailure(next);
            }
            long elapsed = System.currentTimeMillis() - start;

            assertTrue(lookup.isFinished());
            assertTrue(elapsed < 10000);
        }
    }

    @Test
    void testNoDuplicateQueries() {
        byte[] target = new byte[32];
        new Random().nextBytes(target);

        List<KadPeer> seed = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            seed.add(new KadPeer(PeerId.random(), List.of(), KadPeer.ConnectionType.CONNECTED));
        }

        IterativeLookup lookup = new IterativeLookup(target, seed, 20, 3, 3,
                Duration.ofSeconds(5), null);

        Set<PeerId> queried = new HashSet<>();
        while (!lookup.isFinished()) {
            PeerId next = lookup.next();
            if (next == null) break;
            assertTrue(queried.add(next));
            lookup.onResponse(next, List.of());
        }
    }

    @Test
    void testInFlightBounded() {
        byte[] target = new byte[32];
        new Random().nextBytes(target);
        int alpha = 2;

        List<KadPeer> seed = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            seed.add(new KadPeer(PeerId.random(), List.of(), KadPeer.ConnectionType.CONNECTED));
        }

        IterativeLookup lookup = new IterativeLookup(target, seed, 20, alpha, 3,
                Duration.ofSeconds(5), null);

        int maxWaiting = 0;
        for (int i = 0; i < 20; i++) {
            PeerId next = lookup.next();
            if (next == null) break;
            int waiting = (int) lookup.getAllPeerEntries().stream()
                    .filter(pe -> pe.getState() == IterativeLookup.PeerStateInner.WAITING)
                    .count();
            maxWaiting = Math.max(maxWaiting, waiting);
        }

        assertTrue(maxWaiting <= alpha);
    }

    @Test
    void testCloserPeersIncorporated() {
        byte[] target = new byte[32];
        new Random().nextBytes(target);

        PeerId peer1 = PeerId.random();
        PeerId peer2 = PeerId.random();
        PeerId peer3 = PeerId.random();

        IterativeLookup lookup = new IterativeLookup(target,
                List.of(new KadPeer(peer1, List.of(), KadPeer.ConnectionType.CONNECTED)),
                20, 3, 3, Duration.ofSeconds(5), null);

        List<KadPeer> closer = List.of(
                new KadPeer(peer2, List.of(), KadPeer.ConnectionType.NOT_CONNECTED),
                new KadPeer(peer3, List.of(), KadPeer.ConnectionType.NOT_CONNECTED));

        lookup.onResponse(peer1, closer);

        Set<PeerId> entryIds = lookup.getAllPeerEntries().stream()
                .map(IterativeLookup.PeerEntry::getPeerId)
                .collect(Collectors.toSet());
        assertTrue(entryIds.contains(peer2));
        assertTrue(entryIds.contains(peer3));

        PeerId next = lookup.next();
        assertNotNull(next);
        assertTrue(next.equals(peer2) || next.equals(peer3));
    }

    @Test
    void testResultSizeAtMostK() {
        byte[] target = new byte[32];
        new Random().nextBytes(target);
        int k = 5;

        List<KadPeer> seed = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            seed.add(new KadPeer(PeerId.random(), List.of(), KadPeer.ConnectionType.CONNECTED));
        }

        IterativeLookup lookup = new IterativeLookup(target, seed, k, 3, 3,
                Duration.ofSeconds(5), null);

        while (!lookup.isFinished()) {
            PeerId next = lookup.next();
            if (next == null) break;
            lookup.onResponse(next, List.of());
        }

        List<KadPeer> closest = lookup.getClosestPeers();
        assertTrue(closest.size() <= k);
    }

    @Test
    void testQuorumTerminatesEarly() {
        byte[] target = new byte[32];
        new Random().nextBytes(target);

        List<KadPeer> seed = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            seed.add(new KadPeer(PeerId.random(), List.of(), KadPeer.ConnectionType.CONNECTED));
        }

        KademliaProtocol mockProto = new KademliaProtocol("/test", 20, Duration.ofSeconds(5),
                Duration.ofHours(1), Duration.ofMinutes(30), 100) {
            @Override
            public CompletableFuture<GetValueResponse> sendGetValue(byte[] key, PeerId peer) {
                Record record = new Record(target, "value".getBytes());
                return CompletableFuture.completedFuture(
                        new GetValueResponse(Optional.of(record), List.of()));
            }
        };

        IterativeLookup lookup = new IterativeLookup(target, seed, 20, 3, 3,
                Duration.ofSeconds(5), mockProto, 3);

        int queried = 0;
        while (!lookup.isFinished()) {
            PeerId next = lookup.next();
            if (next == null) break;
            lookup.queryGetValue(next);
            queried++;
        }

        assertTrue(lookup.isFinished());
        assertTrue(queried <= 5);
    }
}
