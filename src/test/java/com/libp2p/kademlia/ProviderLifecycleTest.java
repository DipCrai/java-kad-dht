package com.libp2p.kademlia;

import com.libp2p.kademlia.records.MemoryProviderStore;
import com.libp2p.kademlia.records.ProviderRecord;
import io.libp2p.core.PeerId;
import io.libp2p.core.multiformats.Multiaddr;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ProviderLifecycleTest {

    @Test
    void testProviderAddressExpiry() throws Exception {
        byte[] key = XorId.sha256("addr-expiry".getBytes());
        PeerId provider = PeerId.random();
        List<Multiaddr> addrs = List.of(Multiaddr.fromString("/ip4/127.0.0.1/tcp/4001"));

        Instant now = Instant.now();
        ProviderRecord record = new ProviderRecord(key, provider,
                now.plus(Duration.ofHours(1)), now.plus(Duration.ofMillis(100)), addrs);

        MemoryProviderStore store = new MemoryProviderStore(1024, 20);
        store.addProvider(record);

        assertFalse(record.getAliveAddresses().isEmpty());

        Thread.sleep(200);

        assertTrue(record.getAliveAddresses().isEmpty());
        assertFalse(record.isExpired());
        assertFalse(store.getProviders(key).isEmpty());
    }

    @Test
    void testProviderRecordExpiry() throws Exception {
        byte[] key = XorId.sha256("record-expiry".getBytes());
        PeerId provider = PeerId.random();

        ProviderRecord record = new ProviderRecord(key, provider,
                Instant.now().plus(Duration.ofMillis(200)), Instant.now().plus(Duration.ofHours(1)), List.of());

        MemoryProviderStore store = new MemoryProviderStore(1024, 20);
        store.addProvider(record);

        assertFalse(store.getProviders(key).isEmpty());

        Thread.sleep(400);

        assertTrue(store.getProviders(key).isEmpty());
    }

    @Test
    void testReprovide() throws Exception {
        byte[] key = XorId.sha256("reprovide".getBytes());
        PeerId provider = PeerId.random();
        List<Multiaddr> addrs = List.of(Multiaddr.fromString("/ip4/192.168.1.1/tcp/5000"));

        Duration ttl = Duration.ofMillis(500);

        ProviderRecord original = new ProviderRecord(key, provider,
                Instant.now().plus(ttl), Instant.now().plus(ttl), addrs);

        MemoryProviderStore store = new MemoryProviderStore(1024, 20);
        store.addProvider(original);

        Thread.sleep(ttl.toMillis() / 2);

        ProviderRecord reprovided = new ProviderRecord(key, provider,
                Instant.now().plus(ttl), Instant.now().plus(ttl), addrs);
        store.addProvider(reprovided);

        Thread.sleep(ttl.toMillis() / 2 + 100);

        assertTrue(original.isExpired());
        assertFalse(reprovided.isExpired());
    }

    @Test
    void testMaxProvidersEviction() {
        int maxProviders = 3;
        MemoryProviderStore store = new MemoryProviderStore(1024, maxProviders);
        byte[] key = XorId.sha256("max-providers".getBytes());

        List<PeerId> providers = new java.util.ArrayList<>();
        for (int i = 0; i < maxProviders + 2; i++) {
            PeerId p = PeerId.random();
            providers.add(p);
            store.addProvider(new ProviderRecord(key, p,
                    Instant.now().plus(Duration.ofHours(1)), List.of()));
        }

        List<ProviderRecord> stored = store.getProviders(key);
        assertTrue(stored.size() <= maxProviders);

        for (PeerId p : providers.subList(maxProviders, providers.size())) {
            boolean found = stored.stream().anyMatch(r -> r.getProvider().equals(p));
            assertFalse(found);
        }
    }

    @Test
    void testDuplicateProviderSameKey() {
        MemoryProviderStore store = new MemoryProviderStore(1024, 20);
        byte[] key = XorId.sha256("dedup".getBytes());
        PeerId provider = PeerId.random();

        ProviderRecord r1 = new ProviderRecord(key, provider,
                Instant.now().plus(Duration.ofHours(1)), List.of());
        ProviderRecord r2 = new ProviderRecord(key, provider,
                Instant.now().plus(Duration.ofHours(2)), List.of());

        assertTrue(store.addProvider(r1));
        assertTrue(store.addProvider(r2));

        List<ProviderRecord> providers = store.getProviders(key);
        assertEquals(1, providers.size());
    }
}
