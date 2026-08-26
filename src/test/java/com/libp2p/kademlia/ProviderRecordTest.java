package com.libp2p.kademlia;

import com.libp2p.kademlia.records.ProviderRecord;
import io.libp2p.core.PeerId;
import io.libp2p.core.multiformats.Multiaddr;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ProviderRecordTest {

    @Test
    void testConstructor() {
        byte[] key = new byte[]{1, 2, 3};
        PeerId provider = PeerId.random();
        Instant expires = Instant.now().plusSeconds(3600);
        Instant addrExpiry = Instant.now().plusSeconds(1800);

        ProviderRecord record = new ProviderRecord(key, provider, expires, addrExpiry, List.of());
        assertArrayEquals(key, record.getKey());
        assertEquals(provider, record.getProvider());
        assertEquals(expires, record.getExpires());
        assertEquals(addrExpiry, record.getAddrExpiry());
        assertTrue(record.getAddresses().isEmpty());
    }

    @Test
    void testAddrExpiry() {
        byte[] key = new byte[]{1, 2};
        PeerId provider = PeerId.random();
        Instant expires = Instant.now().plusSeconds(3600);
        Instant addrExpiry = Instant.now().minusSeconds(10);

        ProviderRecord record = new ProviderRecord(key, provider, expires, addrExpiry, List.of());
        assertTrue(record.getAliveAddresses().isEmpty(), "expired addresses should be empty");
        assertFalse(record.isExpired(), "record itself should not be expired");
    }

    @Test
    void testGetAliveAddresses() {
        byte[] key = new byte[]{1, 2};
        PeerId provider = PeerId.random();
        Instant expires = Instant.now().plusSeconds(3600);
        Instant addrExpiry = Instant.now().plusSeconds(3600);

        ProviderRecord record = new ProviderRecord(key, provider, expires, addrExpiry, List.of());
        assertTrue(record.getAliveAddresses().isEmpty(), "no addresses added");
    }

    @Test
    void testIsExpired() {
        ProviderRecord notExpired = new ProviderRecord(new byte[]{1}, PeerId.random(),
                Instant.now().plusSeconds(3600), List.of());
        assertFalse(notExpired.isExpired());

        ProviderRecord expired = new ProviderRecord(new byte[]{1}, PeerId.random(),
                Instant.now().minusSeconds(10), List.of());
        assertTrue(expired.isExpired());
    }

    @Test
    void testEquals() {
        byte[] key = new byte[]{1, 2, 3};
        PeerId provider = PeerId.random();
        ProviderRecord r1 = new ProviderRecord(key, provider, Instant.now().plusSeconds(3600), List.of());
        ProviderRecord r2 = new ProviderRecord(key, provider, Instant.now().plusSeconds(1800), List.of());
        assertEquals(r1, r2, "records with same key and provider should be equal");
    }
}
