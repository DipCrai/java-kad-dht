package com.libp2p.kademlia;

import com.libp2p.kademlia.records.MemoryRecordStore;
import com.libp2p.kademlia.records.Record;
import com.libp2p.kademlia.records.RecordReplicationManager;
import com.libp2p.kademlia.records.RecordValidator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

class RecordReplicationManagerTest {

    private RecordReplicationManager manager;

    @AfterEach
    void tearDown() {
        if (manager != null) manager.stop();
    }

    @Test
    void testReplicationPassesRecordValue() throws Exception {
        MemoryRecordStore store = new MemoryRecordStore(1024, 65536,
                Duration.ofHours(48), RecordValidator.NOOP);
        byte[] key = new byte[]{1, 2, 3};
        byte[] value = "real-value-data".getBytes();
        store.put(new Record(key, value));

        List<byte[]> capturedKeys = new CopyOnWriteArrayList<>();
        List<byte[]> capturedValues = new CopyOnWriteArrayList<>();

        manager = new RecordReplicationManager(store,
                (k, v) -> {
                    capturedKeys.add(k.clone());
                    capturedValues.add(v.clone());
                    return CompletableFuture.completedFuture(true);
                },
                Duration.ofMillis(1));
        manager.start();

        Thread.sleep(500);

        assertFalse(capturedValues.isEmpty(), "putValueFn should have been called");
        byte[] capturedValue = capturedValues.get(0);
        assertNotNull(capturedValue, "value should not be null");
        assertArrayEquals(value, capturedValue, "record VALUE must be passed to putValue, not empty bytes");
    }

    @Test
    void testReplicationDoesNotPassEmptyBytes() throws Exception {
        MemoryRecordStore store = new MemoryRecordStore(1024, 65536,
                Duration.ofHours(48), RecordValidator.NOOP);
        byte[] key = new byte[]{4, 5, 6};
        byte[] value = new byte[]{10, 20, 30, 40, 50};
        store.put(new Record(key, value));

        List<byte[]> capturedValues = new CopyOnWriteArrayList<>();

        manager = new RecordReplicationManager(store,
                (k, v) -> {
                    capturedValues.add(v.clone());
                    return CompletableFuture.completedFuture(true);
                },
                Duration.ofMillis(1));
        manager.start();

        Thread.sleep(500);

        assertFalse(capturedValues.isEmpty(), "values should be captured");
        byte[] val = capturedValues.get(0);
        assertNotNull(val, "value should be captured");
        assertFalse(val.length == 0, "value must not be empty bytes");
        assertArrayEquals(value, val, "must pass actual record value, not new byte[0]");
    }

    @Test
    void testReplicationMultipleRecords() throws Exception {
        MemoryRecordStore store = new MemoryRecordStore(1024, 65536,
                Duration.ofHours(48), RecordValidator.NOOP);
        byte[] key1 = new byte[]{1};
        byte[] val1 = "alpha".getBytes();
        byte[] key2 = new byte[]{2};
        byte[] val2 = "bravo".getBytes();
        store.put(new Record(key1, val1));
        store.put(new Record(key2, val2));

        List<byte[]> capturedValues = new CopyOnWriteArrayList<>();

        manager = new RecordReplicationManager(store,
                (k, v) -> {
                    capturedValues.add(v.clone());
                    return CompletableFuture.completedFuture(true);
                },
                Duration.ofMillis(1));
        manager.start();

        Thread.sleep(500);

        assertFalse(capturedValues.isEmpty(), "values should be captured");
        boolean foundAlpha = false;
        boolean foundBravo = false;
        for (byte[] v : capturedValues) {
            if (assertArrayEqualsLenient(v, val1)) foundAlpha = true;
            if (assertArrayEqualsLenient(v, val2)) foundBravo = true;
        }
        assertTrue(foundAlpha, "alpha value must be present");
        assertTrue(foundBravo, "bravo value must be present");
    }

    private static boolean assertArrayEqualsLenient(byte[] a, byte[] b) {
        if (a.length != b.length) return false;
        for (int i = 0; i < a.length; i++) {
            if (a[i] != b[i]) return false;
        }
        return true;
    }
}
