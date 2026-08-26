package com.libp2p.kademlia;

import io.libp2p.core.PeerId;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class XorIdTest {

    private static final PeerId PEER_A = PeerId.random();
    private static final PeerId PEER_B = PeerId.random();
    private static final PeerId PEER_C = PeerId.random();

    @Test
    void testDistance() {
        byte[] a = XorId.fromPeerId(PEER_A);
        byte[] b = XorId.fromPeerId(PEER_B);

        byte[] dAb = XorId.distance(PEER_A, PEER_B);
        byte[] dBa = XorId.distance(PEER_B, PEER_A);
        assertArrayEquals(dAb, dBa, "distance(a,b) must equal distance(b,a)");

        byte[] dAa = XorId.distance(PEER_A, PEER_A);
        assertArrayEquals(new byte[XorId.KEY_LENGTH], dAa, "distance(a,a) must be zero");
    }

    @Test
    void testBucketIndex() {
        byte[] self = XorId.fromPeerId(PEER_A);
        byte[] sameKey = self.clone();
        byte[] bucket0Key = new byte[XorId.KEY_LENGTH];
        bucket0Key[bucket0Key.length - 1] = 0x01;

        int sameIdx = XorId.bucketIndex(self, sameKey);
        assertEquals(XorId.KEY_LENGTH * 8 - 1, sameIdx, "same key should map to last bucket");

        int idx = XorId.bucketIndex(self, bucket0Key);
        assertTrue(idx >= 0 && idx < XorId.KEY_LENGTH * 8, "bucket index must be in valid range");
    }

    @Test
    void testCompareTo() {
        byte[] zero = new byte[XorId.KEY_LENGTH];
        byte[] one = new byte[XorId.KEY_LENGTH];
        one[XorId.KEY_LENGTH - 1] = 1;
        byte[] two = new byte[XorId.KEY_LENGTH];
        two[XorId.KEY_LENGTH - 1] = 2;

        assertTrue(XorId.compareDistance(zero, one) < 0, "0 < 1");
        assertTrue(XorId.compareDistance(one, two) < 0, "1 < 2");
        assertEquals(0, XorId.compareDistance(one, one), "equal distances");
        assertTrue(XorId.compareDistance(two, zero) > 0, "2 > 0");
    }

    @Test
    void testFromPeerId() {
        PeerId peer = PeerId.random();
        byte[] key = XorId.fromPeerId(peer);
        assertNotNull(key);
        assertEquals(XorId.KEY_LENGTH, key.length);
    }

    @Test
    void testGenerateRandomKeyForBucket() {
        byte[] selfKey = XorId.fromPeerId(PEER_A);
        for (int bucket = 0; bucket < 256; bucket++) {
            byte[] generated = XorId.generateRandomKeyForBucket(selfKey, bucket);
            assertEquals(XorId.KEY_LENGTH, generated.length, "key must be 32 bytes");
            int actualBucket = XorId.bucketIndex(selfKey, generated);
            assertEquals(bucket, actualBucket, "generated key must be in bucket " + bucket);
        }
    }

    @Test
    void testHexRoundtrip() {
        byte[] data = new byte[]{0x00, (byte) 0xFF, 0x10, 0x2A, (byte) 0xAB, (byte) 0xCD};
        String hex = XorId.toHex(data);
        assertNotNull(hex);
        assertEquals(data.length * 2, hex.length());
        assertEquals("00ff102aabcd", hex);
    }

    @Test
    void testSha256() {
        byte[] data = "hello world".getBytes();
        byte[] hash = XorId.sha256(data);
        assertNotNull(hash);
        assertEquals(32, hash.length, "SHA-256 produces 32 bytes");
        assertArrayEquals(hash, XorId.sha256(data), "same input produces same hash");
    }
}
