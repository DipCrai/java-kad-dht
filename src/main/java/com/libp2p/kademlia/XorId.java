package com.libp2p.kademlia;

import io.libp2p.core.PeerId;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class XorId {
    public static final int KEY_LENGTH = 32;

    public interface HasPeerId {
        PeerId getPeerId();
    }

    public static byte[] fromPeerId(PeerId peerId) {
        return sha256(peerId.getBytes());
    }

    public static byte[] fromKey(byte[] rawKey) {
        return sha256(rawKey);
    }

    public static PeerId peerIdFromRawBytes(byte[] key) {
        return new PeerId(key);
    }

    public static byte[] sha256(byte[] data) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return digest.digest(data);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] xor(byte[] a, byte[] b) {
        byte[] result = new byte[KEY_LENGTH];
        int len = Math.min(a.length, Math.min(b.length, KEY_LENGTH));
        for (int i = 0; i < len; i++) {
            result[i] = (byte) (a[a.length - len + i] ^ b[b.length - len + i]);
        }
        return result;
    }

    public static int bucketIndex(byte[] localKey, byte[] remoteKey) {
        byte[] dist = xor(localKey, remoteKey);
        for (int i = 0; i < KEY_LENGTH; i++) {
            if (dist[i] != 0) {
                int bit = Integer.numberOfLeadingZeros(dist[i] & 0xFF) - 24;
                return i * 8 + bit;
            }
        }
        return KEY_LENGTH * 8 - 1;
    }

    public static int compareDistance(byte[] a, byte[] b) {
        for (int i = 0; i < KEY_LENGTH; i++) {
            int cmp = Integer.compareUnsigned(a[i] & 0xFF, b[i] & 0xFF);
            if (cmp != 0) return cmp;
        }
        return 0;
    }

    public static String toHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) sb.append(String.format("%02x", b));
        return sb.toString();
    }

    public static byte[] distance(PeerId a, PeerId b) {
        return xor(fromPeerId(a), fromPeerId(b));
    }

    public static byte[] generateRandomKeyForBucket(byte[] selfKey, int bucketIndex) {
        byte[] result = new byte[KEY_LENGTH];
        System.arraycopy(selfKey, 0, result, 0, KEY_LENGTH);

        int byteIdx = bucketIndex / 8;
        int bitIdx = 7 - (bucketIndex % 8);

        if (byteIdx < KEY_LENGTH) {
            result[byteIdx] = (byte) (result[byteIdx] ^ (1 << bitIdx));
        }

        java.security.SecureRandom sr = new java.security.SecureRandom();
        int lowerBitsMask = (1 << bitIdx) - 1;
        if (byteIdx < KEY_LENGTH && lowerBitsMask > 0) {
            result[byteIdx] = (byte) ((result[byteIdx] & ~lowerBitsMask) | (sr.nextInt(256) & lowerBitsMask));
        }
        for (int i = byteIdx + 1; i < KEY_LENGTH; i++) {
            result[i] = (byte) sr.nextInt(256);
        }
        return result;
    }
}
