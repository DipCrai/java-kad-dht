package com.libp2p.kademlia;

import io.libp2p.core.PeerId;
import io.libp2p.core.multiformats.Multiaddr;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

public class RoutingTable {
    private final PeerId localPeerId;
    private final byte[] localKey;
    private final int k;
    private final KBucket[] buckets;
    private final Set<PeerId> allPeers = java.util.concurrent.ConcurrentHashMap.newKeySet();

    public RoutingTable(PeerId localPeerId, int k, Duration pendingTimeout) {
        this(localPeerId, k, pendingTimeout, 256);
    }

    public RoutingTable(PeerId localPeerId, int k, Duration pendingTimeout, int numBuckets) {
        this.localPeerId = localPeerId;
        this.localKey = XorId.fromPeerId(localPeerId);
        this.k = k;
        this.buckets = new KBucket[numBuckets];
        for (int i = 0; i < numBuckets; i++) {
            buckets[i] = new KBucket(k, pendingTimeout);
        }
    }

    public boolean insert(PeerId peerId, List<Multiaddr> addresses) {
        if (peerId.equals(localPeerId)) return false;
        int bucketIdx = XorId.bucketIndex(localKey, XorId.fromPeerId(peerId));
        KBucketEntry entry = new KBucketEntry(peerId, addresses, Instant.now());
        KBucket.InsertResult result = buckets[bucketIdx].insert(entry);
        if (result == KBucket.InsertResult.INSERTED || result == KBucket.InsertResult.EVICTED) {
            allPeers.add(peerId);
            return true;
        }
        if (result == KBucket.InsertResult.ALREADY_PRESENT) {
            allPeers.add(peerId);
            return false;
        }
        return false;
    }

    public Optional<KBucketEntry> remove(PeerId peerId) {
        byte[] remoteKey = XorId.fromPeerId(peerId);
        int bucketIdx = XorId.bucketIndex(localKey, remoteKey);
        Optional<KBucketEntry> removed = buckets[bucketIdx].remove(peerId);
        removed.ifPresent(e -> allPeers.remove(peerId));
        return removed;
    }

    public void markSeen(PeerId peerId) {
        byte[] remoteKey = XorId.fromPeerId(peerId);
        int bucketIdx = XorId.bucketIndex(localKey, remoteKey);
        buckets[bucketIdx].markSeen(peerId, Instant.now());
    }

    public List<KadPeer> findClosest(byte[] target, int count) {
        List<KadPeer> allKnown = new ArrayList<>();
        for (KBucket bucket : buckets) {
            for (KBucketEntry entry : bucket.getEntries()) {
                allKnown.add(new KadPeer(entry.peerId, entry.getAddresses(), KadPeer.ConnectionType.CONNECTED));
            }
        }
        allKnown.sort((a, b) -> {
            byte[] distA = XorId.xor(target, XorId.fromPeerId(a.nodeId));
            byte[] distB = XorId.xor(target, XorId.fromPeerId(b.nodeId));
            return XorId.compareDistance(distA, distB);
        });
        return allKnown.stream().limit(count).collect(Collectors.toList());
    }

    public List<PeerId> findClosestIds(byte[] target, int count) {
        return findClosest(target, count).stream().map(p -> p.nodeId).collect(Collectors.toList());
    }

    public Set<PeerId> getAllPeers() { return Set.copyOf(allPeers); }
    public PeerId getLocalPeerId() { return localPeerId; }
    public byte[] getLocalKey() { return localKey; }
    public int getK() { return k; }

    public int size() {
        int count = 0;
        for (KBucket bucket : buckets) count += bucket.size();
        return count;
    }

    public int nonEmptyBuckets() {
        int count = 0;
        for (KBucket bucket : buckets) if (bucket.size() > 0) count++;
        return count;
    }

    public KBucket getBucket(int index) { return buckets[index]; }
    public int getBucketCount() { return buckets.length; }

    public List<Integer> getNonEmptyBucketIndices() {
        List<Integer> result = new ArrayList<>();
        for (int i = 0; i < buckets.length; i++) {
            if (buckets[i].size() > 0) result.add(i);
        }
        return result;
    }

    @Override
    public String toString() {
        return "RoutingTable{local=" + localPeerId + ", peers=" + size() + ", buckets=" + nonEmptyBuckets() + "/" + buckets.length + "}";
    }
}
