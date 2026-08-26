package com.libp2p.kademlia.routing;

import io.libp2p.core.PeerId;
import io.libp2p.core.multiformats.Multiaddr;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

public class RoutingTable {
    private volatile PeerId localPeerId;
    private volatile byte[] localKey;
    private final int k;
    private final KBucket[] buckets;
    private final Set<PeerId> allPeers = java.util.concurrent.ConcurrentHashMap.newKeySet();

    public RoutingTable(PeerId localPeerId, int k, Duration pendingTimeout) {
        this(localPeerId, k, pendingTimeout, 256);
    }

    public void setLocalPeerId(PeerId peerId) {
        this.localPeerId = peerId;
        this.localKey = com.libp2p.kademlia.XorId.fromPeerId(peerId);
    }

    public RoutingTable(PeerId localPeerId, int k, Duration pendingTimeout, int numBuckets) {
        this.localPeerId = localPeerId;
        this.localKey = localPeerId != null ? com.libp2p.kademlia.XorId.fromPeerId(localPeerId) : new byte[32];
        this.k = k;
        this.buckets = new KBucket[numBuckets];
        for (int i = 0; i < numBuckets; i++) {
            buckets[i] = new KBucket(k, pendingTimeout);
        }
    }

    public InsertOutcome insertOutcome(PeerId peerId, List<Multiaddr> addresses) {
        if (peerId.equals(localPeerId)) return InsertOutcome.IGNORED;
        int bucketIdx = com.libp2p.kademlia.XorId.bucketIndex(localKey, com.libp2p.kademlia.XorId.fromPeerId(peerId));
        KBucketEntry entry = new KBucketEntry(peerId, addresses, Instant.now());
        KBucket.InsertResult result = buckets[bucketIdx].insert(entry);
        if (result == KBucket.InsertResult.INSERTED || result == KBucket.InsertResult.EVICTED) {
            allPeers.add(peerId);
            return InsertOutcome.INSERTED;
        }
        if (result == KBucket.InsertResult.ALREADY_PRESENT) {
            allPeers.add(peerId);
            return InsertOutcome.UPDATED;
        }
        if (result == KBucket.InsertResult.PING) {
            Optional<KBucketEntry> oldest = buckets[bucketIdx].getOldest();
            return oldest.map(e -> new InsertOutcome(e.peerId)).orElse(InsertOutcome.INSERTED);
        }
        return InsertOutcome.IGNORED;
    }

    public boolean insert(PeerId peerId, List<Multiaddr> addresses) {
        InsertOutcome outcome = insertOutcome(peerId, addresses);
        return outcome == InsertOutcome.INSERTED || outcome == InsertOutcome.UPDATED || outcome.needsPing();
    }

    public record InsertOutcome(PeerId peerToPing) {
        public static final InsertOutcome INSERTED = new InsertOutcome(null);
        public static final InsertOutcome UPDATED = new InsertOutcome(null);
        public static final InsertOutcome IGNORED = new InsertOutcome(null);
        public boolean needsPing() { return peerToPing != null; }
    }

    public Optional<KBucketEntry> remove(PeerId peerId) {
        byte[] remoteKey = com.libp2p.kademlia.XorId.fromPeerId(peerId);
        int bucketIdx = com.libp2p.kademlia.XorId.bucketIndex(localKey, remoteKey);
        Optional<KBucketEntry> removed = buckets[bucketIdx].remove(peerId);
        removed.ifPresent(e -> allPeers.remove(peerId));
        return removed;
    }

    public void markSeen(PeerId peerId) {
        int bucketIdx = com.libp2p.kademlia.XorId.bucketIndex(localKey, com.libp2p.kademlia.XorId.fromPeerId(peerId));
        buckets[bucketIdx].markSeen(peerId, Instant.now());
    }

    public void markSuccessfulOutbound(PeerId peerId) {
        int bucketIdx = com.libp2p.kademlia.XorId.bucketIndex(localKey, com.libp2p.kademlia.XorId.fromPeerId(peerId));
        buckets[bucketIdx].markSuccessfulOutbound(peerId, Instant.now());
    }

    public List<KadPeer> findClosest(byte[] target, int count) {
        List<KadPeer> allKnown = new ArrayList<>();
        for (KBucket bucket : buckets) {
            for (KBucketEntry entry : bucket.getEntries()) {
                allKnown.add(new KadPeer(entry.peerId, entry.getAddresses(), KadPeer.ConnectionType.CONNECTED));
            }
        }
        allKnown.sort((a, b) -> {
            byte[] distA = com.libp2p.kademlia.XorId.xor(target, com.libp2p.kademlia.XorId.fromPeerId(a.nodeId));
            byte[] distB = com.libp2p.kademlia.XorId.xor(target, com.libp2p.kademlia.XorId.fromPeerId(b.nodeId));
            return com.libp2p.kademlia.XorId.compareDistance(distA, distB);
        });
        return allKnown.stream().limit(count).collect(Collectors.toList());
    }

    public Set<PeerId> getAllPeers() { return Set.copyOf(allPeers); }
    public PeerId getLocalPeerId() { return localPeerId; }
    public byte[] getLocalKey() { return localKey; }
    public byte[] getLocalNodeId() { return localKey; }
    public int getK() { return k; }
    public int getBucketCount() { return buckets.length; }
    public KBucket getBucket(int index) { return buckets[index]; }

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
