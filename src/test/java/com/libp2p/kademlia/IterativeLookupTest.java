package com.libp2p.kademlia;

import com.libp2p.kademlia.lookup.IterativeLookup;
import com.libp2p.kademlia.routing.KadPeer;
import io.libp2p.core.PeerId;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class IterativeLookupTest {

    private List<KadPeer> createPeers(int count) {
        List<KadPeer> peers = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            peers.add(new KadPeer(PeerId.random(), List.of(), KadPeer.ConnectionType.NOT_CONNECTED));
        }
        return peers;
    }

    @Test
    void testBasicLookup() {
        byte[] target = new byte[32];
        List<KadPeer> seed = createPeers(5);
        IterativeLookup lookup = new IterativeLookup(target, seed, 20, 3, 3,
                Duration.ofSeconds(5), null);
        assertNotNull(lookup);
        assertEquals(5, lookup.getAllPeerEntries().size());
        List<KadPeer> closest = lookup.getClosestPeers();
        assertNotNull(closest);
    }

    @Test
    void testCandidateRecords() {
        byte[] target = new byte[32];
        IterativeLookup lookup = new IterativeLookup(target, List.of(), 20, 3, 3,
                Duration.ofSeconds(5), null);
        List<com.libp2p.kademlia.records.Record> records = new ArrayList<>();
        records.add(new com.libp2p.kademlia.records.Record(new byte[]{1}, new byte[]{2}));
        lookup.addCandidateRecords(records);
        assertEquals(1, lookup.getCandidateRecords().size());
    }

    @Test
    void testQuorum() {
        byte[] target = new byte[32];
        List<KadPeer> seed = createPeers(5);
        IterativeLookup lookup = new IterativeLookup(target, seed, 20, 3, 3,
                Duration.ofSeconds(5), null, 2);
        assertEquals(3, lookup.getAlpha());
        assertNotNull(lookup.getCancellation());
    }

    @Test
    void testEmpty() {
        byte[] target = new byte[32];
        IterativeLookup lookup = new IterativeLookup(target, List.of(), 20, 3, 3,
                Duration.ofSeconds(5), null);
        PeerId next = lookup.next();
        assertNull(next, "no peers means no next peer to query");
        assertTrue(lookup.isFinished(), "should be finished with no peers");
    }

    @Test
    void testCancellation() {
        byte[] target = new byte[32];
        IterativeLookup lookup = new IterativeLookup(target, List.of(), 20, 3, 3,
                Duration.ofSeconds(5), null);
        lookup.getCancellation().cancel(true);
        assertTrue(lookup.getCancellation().isDone());
    }

    @Test
    void testGetAllPeerEntries() {
        byte[] target = new byte[32];
        List<KadPeer> seed = createPeers(3);
        IterativeLookup lookup = new IterativeLookup(target, seed, 20, 3, 3,
                Duration.ofSeconds(5), null);
        assertEquals(3, lookup.getAllPeerEntries().size());
        for (IterativeLookup.PeerEntry pe : lookup.getAllPeerEntries()) {
            assertEquals(IterativeLookup.PeerStateInner.NOT_CONTACTED, pe.getState());
        }
    }
}
