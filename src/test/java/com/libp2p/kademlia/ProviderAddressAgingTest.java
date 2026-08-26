package com.libp2p.kademlia;

import com.libp2p.kademlia.records.ProviderRecord;
import io.libp2p.core.PeerId;
import io.libp2p.core.multiformats.Multiaddr;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ProviderAddressAgingTest {

    @Test
    void testAddressExpiresBeforeRecord() throws Exception {
        byte[] key = XorId.sha256("test-key".getBytes());
        PeerId provider = PeerId.random();
        List<Multiaddr> addrs = List.of(Multiaddr.fromString("/ip4/127.0.0.1/tcp/4001"));

        Instant now = Instant.now();
        Instant recordExpiry = now.plus(Duration.ofSeconds(5));
        Instant addrExpiry = now.plus(Duration.ofMillis(100));

        ProviderRecord record = new ProviderRecord(key, provider, recordExpiry, addrExpiry, addrs);

        assertFalse(record.isExpired(), "record should not be expired yet");
        assertFalse(record.getAliveAddresses().isEmpty(), "addresses should be alive initially");

        Thread.sleep(150);

        assertTrue(record.getAliveAddresses().isEmpty(), "addresses should have expired after 150ms");
        assertFalse(record.isExpired(), "record should still be alive");

        Thread.sleep(Duration.ofSeconds(5).toMillis());

        assertTrue(record.isExpired(), "record should now be expired");
    }

    @Test
    void testRecordExpiresAfterAddress() throws Exception {
        byte[] key = XorId.sha256("test-key-2".getBytes());
        PeerId provider = PeerId.random();
        List<Multiaddr> addrs = List.of(Multiaddr.fromString("/ip4/10.0.0.1/tcp/9000"));

        Instant now = Instant.now();
        Instant addrExpiry = now.plus(Duration.ofMillis(80));
        Instant recordExpiry = now.plus(Duration.ofSeconds(3));

        ProviderRecord record = new ProviderRecord(key, provider, recordExpiry, addrExpiry, addrs);

        assertEquals(addrs, record.getAliveAddresses(), "should have addresses initially");

        Thread.sleep(120);

        assertTrue(record.getAliveAddresses().isEmpty(), "addresses should expire first");
        assertFalse(record.isExpired(), "record should still be alive");
    }

    @Test
    void testReprovideResetsTTL() throws Exception {
        byte[] key = XorId.sha256("reprovide-key".getBytes());
        PeerId provider = PeerId.random();
        List<Multiaddr> addrs = List.of(Multiaddr.fromString("/ip4/192.168.1.1/tcp/5000"));

        Instant now = Instant.now();
        Duration recordTTL = Duration.ofSeconds(2);
        Duration addrTTL = Duration.ofSeconds(2);

        ProviderRecord original = new ProviderRecord(key, provider, now.plus(recordTTL), now.plus(addrTTL), addrs);

        Thread.sleep(recordTTL.toMillis() / 2 + 100);

        Instant reprovideTime = Instant.now();
        ProviderRecord reprovided = new ProviderRecord(key, provider, reprovideTime.plus(recordTTL), reprovideTime.plus(addrTTL), addrs);

        assertFalse(reprovided.isExpired(), "reprovided record should not be expired");
        assertFalse(reprovided.getAliveAddresses().isEmpty(), "reprovided record should have alive addresses");

        Thread.sleep(recordTTL.toMillis() / 2 + 100);

        assertTrue(original.isExpired(), "original record should be expired now");
        assertFalse(reprovided.isExpired(), "reprovided record should still be alive (TTL was reset)");
    }

    @Test
    void testSeparateAddressAndRecordExpiry() {
        byte[] key = XorId.sha256("separate-expiry".getBytes());
        PeerId provider = PeerId.random();
        List<Multiaddr> addrs = List.of(Multiaddr.fromString("/ip4/172.16.0.1/tcp/8080"));

        ProviderRecord shortAddr = new ProviderRecord(key, provider,
                Instant.now().plus(Duration.ofHours(1)),
                Instant.now().plus(Duration.ofMillis(50)),
                addrs);

        ProviderRecord longAddr = new ProviderRecord(key, provider,
                Instant.now().plus(Duration.ofHours(1)),
                Instant.now().plus(Duration.ofHours(1)),
                addrs);

        assertEquals(addrs, shortAddr.getAliveAddresses());
        assertEquals(addrs, longAddr.getAliveAddresses());

        try { Thread.sleep(80); } catch (InterruptedException ignored) {}

        assertTrue(shortAddr.getAliveAddresses().isEmpty());
        assertFalse(longAddr.getAliveAddresses().isEmpty());
    }

    @Test
    void testProviderStoreRetainsExpiredAddresses() {
        byte[] key = XorId.sha256("store-test".getBytes());
        PeerId provider = PeerId.random();
        List<Multiaddr> addrs = List.of(Multiaddr.fromString("/ip4/10.0.0.1/tcp/4001"));

        ProviderRecord record = new ProviderRecord(key, provider,
                Instant.now().plus(Duration.ofHours(1)),
                Instant.now().plus(Duration.ofMillis(50)),
                addrs);

        com.libp2p.kademlia.records.MemoryProviderStore store = new com.libp2p.kademlia.records.MemoryProviderStore();
        store.addProvider(record);

        try { Thread.sleep(80); } catch (InterruptedException ignored) {}

        List<ProviderRecord> providers = store.getProviders(key);
        assertFalse(providers.isEmpty(), "provider record still alive should be returned even with expired addresses");
    }
}
