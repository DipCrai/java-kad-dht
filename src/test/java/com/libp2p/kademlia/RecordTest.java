package com.libp2p.kademlia;

import com.libp2p.kademlia.records.Record;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class RecordTest {

    @Test
    void testConstructor() {
        byte[] key = new byte[]{1, 2, 3};
        byte[] value = new byte[]{4, 5, 6};
        byte[] publisher = new byte[]{7, 8, 9};
        Instant expires = Instant.now().plusSeconds(3600);

        Record record = new Record(key, value, publisher, expires);
        assertArrayEquals(key, record.getKey());
        assertArrayEquals(value, record.getValue());
        assertArrayEquals(publisher, record.getPublisher());
        assertEquals(expires, record.getExpires());
    }

    @Test
    void testNullPublisher() {
        Record record = new Record(new byte[]{1}, new byte[]{2}, null, null);
        assertNull(record.getPublisher());
        assertNull(record.getExpires());
    }

    @Test
    void testTimeReceived() {
        Instant before = Instant.now();
        Record record = new Record(new byte[]{1}, new byte[]{2});
        Instant after = Instant.now();

        assertNotNull(record.getTimeReceived());
        assertTrue(record.getTimeReceived().compareTo(before) >= 0);
        assertTrue(record.getTimeReceived().compareTo(after) <= 0);
    }

    @Test
    void testCopy() {
        Record original = new Record(new byte[]{1}, new byte[]{2}, new byte[]{3}, Instant.now());
        Record copy = original.copy();
        assertArrayEquals(original.getKey(), copy.getKey());
        assertArrayEquals(original.getValue(), copy.getValue());
        assertArrayEquals(original.getPublisher(), copy.getPublisher());
        assertEquals(original.getExpires(), copy.getExpires());
        assertEquals(original.getTimeReceived(), copy.getTimeReceived());
    }

    @Test
    void testExpired() {
        Record expired = new Record(new byte[]{1}, new byte[]{2}, null,
                Instant.now().minusSeconds(10));
        assertTrue(expired.isExpired(), "record in the past should be expired");

        Record notExpired = new Record(new byte[]{1}, new byte[]{2}, null,
                Instant.now().plusSeconds(3600));
        assertFalse(notExpired.isExpired(), "record in the future should not be expired");

        Record noExpiry = new Record(new byte[]{1}, new byte[]{2}, null, null);
        assertFalse(noExpiry.isExpired(), "record with no expiry should not be expired");
    }

    @Test
    void testEquals() {
        Record r1 = new Record(new byte[]{1, 2}, new byte[]{3, 4});
        Record r2 = new Record(new byte[]{1, 2}, new byte[]{3, 4});
        Record r3 = new Record(new byte[]{1, 2}, new byte[]{5, 6});
        assertEquals(r1, r2);
        assertNotEquals(r1, r3);
    }
}
