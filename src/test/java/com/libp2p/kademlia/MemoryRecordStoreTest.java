package com.libp2p.kademlia;

import com.libp2p.kademlia.records.MemoryRecordStore;
import com.libp2p.kademlia.records.Record;
import com.libp2p.kademlia.records.RecordValidator;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class MemoryRecordStoreTest {

    private MemoryRecordStore createStore(int maxRecords) {
        return new MemoryRecordStore(maxRecords, 65536, Duration.ofHours(48), RecordValidator.NOOP);
    }

    @Test
    void testPutAndGet() {
        MemoryRecordStore store = createStore(10);
        byte[] key = new byte[]{1, 2, 3};
        byte[] value = new byte[]{4, 5, 6};
        Record record = new Record(key, value);

        assertTrue(store.put(record));
        Record got = store.get(key);
        assertNotNull(got);
        assertArrayEquals(value, got.getValue());
    }

    @Test
    void testPutExpired() {
        MemoryRecordStore store = createStore(10);
        byte[] key = new byte[]{1, 2};
        byte[] value = new byte[]{3, 4};
        Record expired = new Record(key, value, null, Instant.now().minusSeconds(10));

        store.put(expired);
        Record got = store.get(key);
        assertNull(got, "expired record should not be returned");
    }

    @Test
    void testTTL() {
        MemoryRecordStore store = new MemoryRecordStore(10, 65536, Duration.ofSeconds(1), RecordValidator.NOOP);
        byte[] key = new byte[]{1};
        byte[] value = new byte[]{2};
        store.put(new Record(key, value));

        assertNotNull(store.get(key), "record should exist immediately");

        try { Thread.sleep(1500); } catch (InterruptedException ignored) {}
        assertNull(store.get(key), "record should have expired");
    }

    @Test
    void testMaxCapacity() {
        MemoryRecordStore store = createStore(3);
        for (int i = 0; i < 3; i++) {
            store.put(new Record(new byte[]{(byte) i}, new byte[]{(byte) i}));
        }
        assertEquals(3, store.size());

        store.put(new Record(new byte[]{3}, new byte[]{3}));
        assertTrue(store.size() <= 3, "store should not exceed max capacity");
    }

    @Test
    void testRecords() {
        MemoryRecordStore store = createStore(10);
        store.put(new Record(new byte[]{1}, new byte[]{10}));
        store.put(new Record(new byte[]{2}, new byte[]{20}));

        Iterable<Record> records = store.records();
        int count = 0;
        for (Record r : records) {
            assertNotNull(r);
            count++;
        }
        assertEquals(2, count);
    }

    @Test
    void testSize() {
        MemoryRecordStore store = createStore(10);
        assertEquals(0, store.size());
        store.put(new Record(new byte[]{1}, new byte[]{10}));
        assertEquals(1, store.size());
        store.put(new Record(new byte[]{2}, new byte[]{20}));
        assertEquals(2, store.size());
    }

    @Test
    void testRemove() {
        MemoryRecordStore store = createStore(10);
        byte[] key = new byte[]{1};
        store.put(new Record(key, new byte[]{10}));
        assertEquals(1, store.size());

        store.remove(key);
        assertNull(store.get(key));
        assertEquals(0, store.size());
    }
}
