package com.libp2p.kademlia;

import com.libp2p.kademlia.config.KadConfig;
import com.libp2p.kademlia.config.KadMode;
import com.libp2p.kademlia.records.Record;
import com.libp2p.kademlia.records.ProviderRecord;
import com.libp2p.kademlia.routing.KadPeer;
import io.libp2p.core.PeerId;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class IntegrationTest {

    private KadDht createDht() {
        KadConfig config = KadConfig.builder()
                .mode(KadMode.SERVER)
                .queryTimeout(Duration.ofSeconds(10))
                .substreamTimeout(Duration.ofSeconds(5))
                .build();
        KadDht dht = new KadDht(config);
        dht.getRoutingTable().setLocalPeerId(PeerId.random());
        return dht;
    }

    @Test
    void testRoutingTableInsertAndLookup() {
        KadDht d1 = createDht();
        KadDht d2 = createDht();

        PeerId peer1 = PeerId.random();
        PeerId peer2 = PeerId.random();

        d1.getRoutingTable().insert(peer1, List.of());
        d1.getRoutingTable().insert(peer2, List.of());

        byte[] target = XorId.fromPeerId(peer2);
        List<KadPeer> results = d1.getRoutingTable().findClosest(target, 20);

        boolean foundPeer2 = results.stream().anyMatch(p -> p.nodeId.equals(peer2));
        assertTrue(foundPeer2, "should find peer2 through routing table");
    }

    @Test
    void testRecordStorePutGet() {
        KadDht d1 = createDht();
        KadDht d2 = createDht();

        byte[] key = new byte[32];
        byte[] value = "test-value".getBytes();
        PeerId publisher = PeerId.random();
        d1.getRecordStore().put(new Record(key, value, publisher.getBytes(), null));

        Record stored = d1.getRecordStore().get(key);
        assertNotNull(stored, "record should be stored locally");
        assertArrayEquals(value, stored.getValue());

        d2.getRecordStore().put(new Record(key, value, publisher.getBytes(), null));
        Record stored2 = d2.getRecordStore().get(key);
        assertNotNull(stored2, "record should be retrievable on second DHT");
        assertArrayEquals(value, stored2.getValue());
    }

    @Test
    void testProviderStoreAddGet() {
        KadDht d1 = createDht();
        KadDht d2 = createDht();

        byte[] key = new byte[32];
        PeerId providerPeer = PeerId.random();
        ProviderRecord provider = new ProviderRecord(key, providerPeer,
                Instant.now().plus(Duration.ofHours(48)), Instant.now().plus(Duration.ofHours(24)), List.of());

        d1.getProviderStore().addProvider(provider);
        d2.getProviderStore().addProvider(provider);

        List<ProviderRecord> providers1 = d1.getProviderStore().getProviders(key);
        assertFalse(providers1.isEmpty(), "provider should be found on DHT1");
        assertEquals(providerPeer, providers1.get(0).getProvider());

        List<ProviderRecord> providers2 = d2.getProviderStore().getProviders(key);
        assertFalse(providers2.isEmpty(), "provider should be found on DHT2");
        assertEquals(providerPeer, providers2.get(0).getProvider());
    }

    @Test
    void testRoutingTableSize() {
        KadDht dht = createDht();
        for (int i = 0; i < 30; i++) {
            dht.getRoutingTable().insert(PeerId.random(), List.of());
        }
        assertTrue(dht.getRoutingTable().size() >= 20, "routing table should have at least K peers");
    }
}
