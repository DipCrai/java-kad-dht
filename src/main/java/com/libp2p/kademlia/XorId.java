package com.libp2p.kademlia;

import io.libp2p.core.PeerId;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public final class XorId {
    public static final int KEY_LENGTH = 32;

    private XorId() {}

    public static byte[] xor(byte[] a, byte[] b) {
        if (a.length != KEY_LENGTH || b.length != KEY_LENGTH) {
            throw new IllegalArgumentException("Keys must be " + KEY_LENGTH + " bytes, got " + a.length + " and " + b.length);
        }
        byte[] result = new byte[KEY_LENGTH];
        for (int i = 0; i < KEY_LENGTH; i++) {
            result[i] = (byte) (a[i] ^ b[i]);
        }
        return result;
    }

    public static byte[] distance(PeerId a, PeerId b) {
        return xor(fromPeerId(a), fromPeerId(b));
    }

    public static int commonPrefixLength(byte[] a, byte[] b) {
        for (int i = 0; i < KEY_LENGTH; i++) {
            int xor = (a[i] ^ b[i]) & 0xFF;
            if (xor != 0) {
                return i * 8 + Integer.numberOfLeadingZeros(xor) - 24;
            }
        }
        return KEY_LENGTH * 8;
    }

    public static int bucketIndex(byte[] self, byte[] remote) {
        int cpl = commonPrefixLength(self, remote);
        return Math.min(cpl, KEY_LENGTH * 8 - 1);
    }

    public static int compareDistance(byte[] a, byte[] b) {
        for (int i = 0; i < KEY_LENGTH; i++) {
            int diff = (a[i] & 0xFF) - (b[i] & 0xFF);
            if (diff != 0) return diff;
        }
        return 0;
    }

    public static byte[] fromPeerId(PeerId peerId) {
        byte[] raw = peerId.getBytes();
        if (raw.length == KEY_LENGTH) return raw;
        byte[] padded = new byte[KEY_LENGTH];
        System.arraycopy(raw, 0, padded, 0, Math.min(raw.length, KEY_LENGTH));
        return padded;
    }

    public static PeerId toPeerId(byte[] raw) {
        return new PeerId(raw);
    }

    public static byte[] generateRandomKey() {
        byte[] key = new byte[KEY_LENGTH];
        new java.security.SecureRandom().nextBytes(key);
        return key;
    }

    public static byte[] generateRandomKeyForBucket(byte[] selfKey, int bucketIndex) {
        byte[] key = new byte[KEY_LENGTH];
        new java.security.SecureRandom().nextBytes(key);
        int bitPos = KEY_LENGTH * 8 - 1 - bucketIndex;
        int byteIdx = bitPos / 8;
        int bitIdx = 7 - (bitPos % 8);
        key[byteIdx] = (byte) ((key[byteIdx] & ~(1 << bitIdx)) | (1 << bitIdx));
        for (int i = 0; i < byteIdx; i++) {
            key[i] = selfKey[i];
        }
        return key;
    }

    public static <T extends HasPeerId> List<T> closestK(byte[] target, List<T> peers, int k) {
        return peers.stream()
                .sorted(Comparator.comparingInt(p -> {
                    byte[] dist = xor(target, fromPeerId(p.getPeerId()));
                    return ((dist[0] & 0xFF) << 24) | ((dist[1] & 0xFF) << 16) |
                           ((dist[2] & 0xFF) << 8) | (dist[3] & 0xFF);
                }))
                .limit(k)
                .collect(Collectors.toList());
    }

    static String toHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b & 0xFF));
        }
        return sb.toString();
    }

    public interface HasPeerId {
        PeerId getPeerId();
    }
}
