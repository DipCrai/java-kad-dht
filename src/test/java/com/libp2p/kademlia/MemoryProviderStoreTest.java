package com.libp2p.kademlia;

import com.libp2p.kademlia.records.MemoryProviderStore;
import com.libp2p.kademlia.records.ProviderRecord;
import io.libp2p.core.PeerId;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MemoryProviderStoreTest {

    @Test
    void testAddProvider() {
        MemoryProviderStore store = new MemoryProviderStore(10, 5);
        byte[] key = new byte[]{1, 2, 3};
        PeerId provider = PeerId.random();
        ProviderRecord record = new ProviderRecord(key, provider, Instant.now().plusSeconds(3600), List.of());

        assertTrue(store.addProvider(record));
        List<ProviderRecord> providers = store.getProviders(key);
        assertFalse(providers.isEmpty());
        assertEquals(provider, providers.get(0).getProvider());
    }

    @Test
    void testGetProviders() {
        MemoryProviderStore store = new MemoryProviderStore(10, 5);
        byte[] key = new byte[]{1, 2, 3};
        PeerId p1 = PeerId.random();
        PeerId p2 = PeerId.random();

        store.addProvider(new ProviderRecord(key, p1, Instant.now().plusSeconds(3600), List.of()));
        store.addProvider(new ProviderRecord(key, p2, Instant.now().plusSeconds(3600), List.of()));

        List<ProviderRecord> providers = store.getProviders(key);
        assertEquals(2, providers.size());
    }

    @Test
    void testMaxProvidersPerKey() {
        int maxProviders = 3;
        MemoryProviderStore store = new MemoryProviderStore(10, maxProviders);
        byte[] key = new byte[]{1, 2, 3};

        for (int i = 0; i < maxProviders + 2; i++) {
            store.addProvider(new ProviderRecord(key, PeerId.random(),
                    Instant.now().plusSeconds(3600), List.of()));
        }

        List<ProviderRecord> providers = store.getProviders(key);
        assertTrue(providers.size() <= maxProviders, "should not exceed max providers per key");
    }

    @Test
    void testProviderExpiration() {
        MemoryProviderStore store = new MemoryProviderStore(10, 5);
        byte[] key = new byte[]{1, 2, 3};
        PeerId provider = PeerId.random();

        store.addProvider(new ProviderRecord(key, provider,
                Instant.now().minusSeconds(10), Instant.now().plusSeconds(3600), List.of()));

        List<ProviderRecord> providers = store.getProviders(key);
        assertTrue(providers.isEmpty(), "expired providers should not be returned");
    }

    @Test
    void testAddrExpiration() {
        MemoryProviderStore store = new MemoryProviderStore(10, 5);
        byte[] key = new byte[]{1, 2, 3};
        PeerId provider = PeerId.random();

        ProviderRecord record = new ProviderRecord(key, provider,
                Instant.now().plusSeconds(3600), Instant.now().minusSeconds(10), List.of());
        store.addProvider(record);

        List<ProviderRecord> providers = store.getProviders(key);
        assertEquals(1, providers.size(), "provider should still be in store");
    }

    @Test
    void testProvided() {
        MemoryProviderStore store = new MemoryProviderStore(10, 5);
        byte[] key1 = new byte[]{1, 2, 3};
        byte[] key2 = new byte[]{4, 5, 6};
        PeerId provider = PeerId.random();

        store.addProvider(new ProviderRecord(key1, provider, Instant.now().plusSeconds(3600), List.of()));
        store.addProvider(new ProviderRecord(key2, provider, Instant.now().plusSeconds(3600), List.of()));

        int count = 0;
        for (ProviderRecord pr : store.provided()) {
            assertNotNull(pr);
            count++;
        }
        assertEquals(2, count);
    }
}
