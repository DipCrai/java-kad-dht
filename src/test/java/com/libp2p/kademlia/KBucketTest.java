package com.libp2p.kademlia;

import com.libp2p.kademlia.routing.KBucket;
import com.libp2p.kademlia.routing.KBucketEntry;
import io.libp2p.core.PeerId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class KBucketTest {

    private static final int K = 20;
    private KBucket bucket;

    @BeforeEach
    void setUp() {
        bucket = new KBucket(K);
    }

    private KBucketEntry entry(PeerId id) {
        return new KBucketEntry(id, List.of(), Instant.now());
    }

    @Test
    void testInsert() {
        KBucketEntry e = entry(PeerId.random());
        KBucket.InsertResult result = bucket.insert(e);
        assertEquals(KBucket.InsertResult.INSERTED, result);
        assertEquals(1, bucket.size());
        assertTrue(bucket.getEntries().contains(e));
    }

    @Test
    void testInsertDuplicate() {
        PeerId id = PeerId.random();
        KBucketEntry e1 = new KBucketEntry(id, List.of(), Instant.ofEpochSecond(1));
        bucket.insert(e1);

        KBucketEntry e2 = new KBucketEntry(id, List.of(), Instant.ofEpochSecond(2));
        KBucket.InsertResult result = bucket.insert(e2);
        assertEquals(KBucket.InsertResult.ALREADY_PRESENT, result);
        assertEquals(1, bucket.size(), "duplicate should not add new entry");
        assertEquals(e1, bucket.getEntries().get(0), "original entry should be moved to front");
    }

    @Test
    void testInsertFull() {
        for (int i = 0; i < K; i++) {
            assertEquals(KBucket.InsertResult.INSERTED, bucket.insert(entry(PeerId.random())));
        }
        assertTrue(bucket.isFull());

        KBucket.InsertResult result = bucket.insert(entry(PeerId.random()));
        assertEquals(KBucket.InsertResult.PING, result, "inserting into full bucket should return PING");
        assertEquals(K, bucket.size(), "bucket should remain full");
    }

    @Test
    void testReplacementCache() {
        for (int i = 0; i < K; i++) {
            bucket.insert(entry(PeerId.random()));
        }
        assertEquals(K, bucket.size());

        KBucketEntry replacement = entry(PeerId.random());
        bucket.insert(replacement);

        assertEquals(1, bucket.getReplacementCache().size());
        assertTrue(bucket.getReplacementCache().contains(replacement));
    }

    @Test
    void testPromoteReplacement() {
        for (int i = 0; i < K; i++) {
            bucket.insert(entry(PeerId.random()));
        }
        KBucketEntry replacement = entry(PeerId.random());
        bucket.insert(replacement);
        assertEquals(1, bucket.getReplacementCache().size());

        PeerId oldest = bucket.getOldest().get().peerId;
        boolean promoted = bucket.promoteReplacement(oldest);
        assertTrue(promoted);
        assertEquals(0, bucket.getReplacementCache().size());
        assertTrue(bucket.getEntries().stream().anyMatch(e -> e.peerId.equals(replacement.peerId)));
    }

    @Test
    void testGetAll() {
        for (int i = 0; i < 5; i++) {
            bucket.insert(entry(PeerId.random()));
        }
        List<KBucketEntry> entries = bucket.getEntries();
        assertEquals(5, entries.size());
    }

    @Test
    void testSize() {
        assertEquals(0, bucket.size());
        bucket.insert(entry(PeerId.random()));
        assertEquals(1, bucket.size());
        for (int i = 0; i < 3; i++) {
            bucket.insert(entry(PeerId.random()));
        }
        assertEquals(4, bucket.size());
    }

    @Test
    void testRemove() {
        PeerId id = PeerId.random();
        bucket.insert(entry(id));
        assertEquals(1, bucket.size());

        var removed = bucket.remove(id);
        assertTrue(removed.isPresent());
        assertEquals(0, bucket.size());
    }

    @Test
    void testGetOldest() {
        assertTrue(bucket.getOldest().isEmpty());

        PeerId first = PeerId.random();
        PeerId second = PeerId.random();
        bucket.insert(entry(first));
        bucket.insert(entry(second));

        assertEquals(first, bucket.getOldest().get().peerId, "oldest should be the first inserted");
    }
}
