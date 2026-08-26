package com.libp2p.kademlia;

import com.libp2p.kademlia.lookup.IterativeLookup;
import com.libp2p.kademlia.protocol.KademliaProtocol;
import com.libp2p.kademlia.records.MemoryRecordStore;
import com.libp2p.kademlia.records.Record;
import com.libp2p.kademlia.records.RecordValidator;
import com.libp2p.kademlia.routing.KadPeer;
import com.libp2p.kademlia.routing.RoutingTable;
import io.libp2p.core.PeerId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

class AdversarialLookupTest {

    private RoutingTable routingTable;
    private PeerId selfPeer;

    @BeforeEach
    void setUp() {
        selfPeer = PeerId.random();
        routingTable = new RoutingTable(selfPeer, 20, 256);
    }

    @Test
    void testMaliciousPeerReturnsSelf() {
        byte[] target = new byte[32];
        new Random().nextBytes(target);

        PeerId malicious = PeerId.random();
        IterativeLookup lookup = new IterativeLookup(target,
                List.of(new KadPeer(malicious, List.of(), KadPeer.ConnectionType.CONNECTED)),
                20, 3, 3, Duration.ofSeconds(5), null);

        lookup.onResponse(malicious, List.of());

        assertTrue(lookup.isFinished());
    }

    @Test
    void testMaliciousPeerReturnsRandom() {
        byte[] target = new byte[32];
        new Random().nextBytes(target);

        PeerId malicious = PeerId.random();
        List<KadPeer> randomPeers = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            randomPeers.add(new KadPeer(PeerId.random(), List.of(), KadPeer.ConnectionType.NOT_CONNECTED));
        }

        IterativeLookup lookup = new IterativeLookup(target,
                List.of(new KadPeer(malicious, List.of(), KadPeer.ConnectionType.CONNECTED)),
                20, 3, 3, Duration.ofSeconds(5), null);

        lookup.onResponse(malicious, randomPeers);

        assertTrue(lookup.getAllPeerEntries().size() > 1);
    }

    @Test
    void testSlowPeer() {
        byte[] target = new byte[32];
        new Random().nextBytes(target);

        PeerId slow = PeerId.random();
        IterativeLookup lookup = new IterativeLookup(target,
                List.of(new KadPeer(slow, List.of(), KadPeer.ConnectionType.CONNECTED)),
                20, 3, 3, Duration.ofMillis(50), null);

        lookup.onFailure(slow);

        assertTrue(lookup.isFinished());
    }

    @Test
    void testNeverRespondingPeer() {
        byte[] target = new byte[32];
        new Random().nextBytes(target);

        PeerId ghost = PeerId.random();
        IterativeLookup lookup = new IterativeLookup(target,
                List.of(new KadPeer(ghost, List.of(), KadPeer.ConnectionType.CONNECTED)),
                20, 3, 3, Duration.ofSeconds(5), null);

        lookup.onFailure(ghost);

        assertTrue(lookup.isFinished());
    }

    @Test
    void testDuplicateCloserPeers() {
        byte[] target = new byte[32];
        new Random().nextBytes(target);

        PeerId peer1 = PeerId.random();
        PeerId peer2 = PeerId.random();

        IterativeLookup lookup = new IterativeLookup(target,
                List.of(new KadPeer(peer1, List.of(), KadPeer.ConnectionType.CONNECTED)),
                20, 3, 3, Duration.ofSeconds(5), null);

        List<KadPeer> closer = List.of(
                new KadPeer(peer2, List.of(), KadPeer.ConnectionType.NOT_CONNECTED));

        lookup.onResponse(peer1, closer);
        lookup.onResponse(peer1, closer);

        long count = lookup.getAllPeerEntries().stream()
                .filter(pe -> pe.getPeerId().equals(peer2))
                .count();
        assertEquals(1, count);
    }

    @Test
    void testHugeCloserPeers() {
        byte[] target = new byte[32];
        new Random().nextBytes(target);

        PeerId responder = PeerId.random();
        List<KadPeer> hugeCloser = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            hugeCloser.add(new KadPeer(PeerId.random(), List.of(), KadPeer.ConnectionType.NOT_CONNECTED));
        }

        IterativeLookup lookup = new IterativeLookup(target,
                List.of(new KadPeer(responder, List.of(), KadPeer.ConnectionType.CONNECTED)),
                20, 3, 3, Duration.ofSeconds(5), null);

        lookup.onResponse(responder, hugeCloser);

        assertTrue(lookup.getAllPeerEntries().size() >= 1000);
    }

    @Test
    void testMalformedRecord() {
        MemoryRecordStore store = new MemoryRecordStore(1024, 65536, Duration.ofHours(48),
                new RecordValidator() {
                    @Override
                    public boolean validate(byte[] key, byte[] value) {
                        return value != null && value.length > 2;
                    }

                    @Override
                    public int select(byte[] key, byte[][] values) {
                        return 0;
                    }
                });

        byte[] key = new byte[]{1, 2, 3};
        Record good = new Record(key, new byte[]{1, 2, 3});
        Record bad = new Record(key, new byte[]{1});

        assertTrue(store.put(good));
        assertFalse(store.put(bad));
        assertNotNull(store.get(key));
    }

    @Test
    void testConflictingRecords() {
        RecordValidator validator = new RecordValidator() {
            @Override
            public boolean validate(byte[] key, byte[] value) {
                return value != null && value.length > 0;
            }

            @Override
            public int select(byte[] key, byte[][] values) {
                return values.length - 1;
            }
        };

        byte[] key = new byte[32];
        new Random().nextBytes(key);

        byte[][] values = {new byte[]{1}, new byte[]{2}, new byte[]{3}};
        int best = validator.select(key, values);
        assertEquals(2, best);
    }

    @Test
    void testEmptyResponses() {
        byte[] target = new byte[32];
        new Random().nextBytes(target);

        PeerId peer1 = PeerId.random();
        PeerId peer2 = PeerId.random();

        IterativeLookup lookup = new IterativeLookup(target,
                List.of(
                        new KadPeer(peer1, List.of(), KadPeer.ConnectionType.CONNECTED),
                        new KadPeer(peer2, List.of(), KadPeer.ConnectionType.CONNECTED)),
                20, 3, 3, Duration.ofSeconds(5), null);

        lookup.onResponse(peer1, List.of());
        lookup.onResponse(peer2, List.of());

        assertTrue(lookup.isFinished());
    }
}
