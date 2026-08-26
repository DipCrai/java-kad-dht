package com.libp2p.kademlia.routing;

import io.libp2p.core.Host;
import io.libp2p.core.PeerId;
import io.libp2p.core.multiformats.Multiaddr;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class RoutingTable {
    private volatile PeerId localPeerId;
    private volatile byte[] localKey;
    private volatile Host host;
    private final int k;
    private final KBucket[] buckets;
    private volatile AdmissionCheck admissionCheck;

    public RoutingTable(PeerId localPeerId, int k, Duration pendingTimeout) {
        this(localPeerId, k, 256);
    }

    public void setLocalPeerId(PeerId peerId) {
        this.localPeerId = peerId;
        this.localKey = com.libp2p.kademlia.XorId.fromPeerId(peerId);
    }

    public void setHost(Host host) {
        this.host = host;
    }

    public void setAdmissionCheck(AdmissionCheck check) {
        this.admissionCheck = check;
    }

    public AdmissionCheck getAdmissionCheck() {
        return admissionCheck;
    }

    public void setDiversityPolicy(PeerDiversityPolicy policy) {
        for (int i = 0; i < buckets.length; i++) {
            buckets[i].setBucketIndex(i);
            buckets[i].setDiversityPolicy(policy);
        }
    }

    public RoutingTable(PeerId localPeerId, int k, Duration pendingTimeout, int numBuckets) {
        this(localPeerId, k, numBuckets);
    }

    public RoutingTable(PeerId localPeerId, int k, int numBuckets) {
        this.localPeerId = localPeerId;
        this.localKey = localPeerId != null ? com.libp2p.kademlia.XorId.fromPeerId(localPeerId) : new byte[32];
        this.k = k;
        this.buckets = new KBucket[numBuckets];
        for (int i = 0; i < numBuckets; i++) {
            buckets[i] = new KBucket(k);
        }
    }

    public InsertOutcome insertOutcome(PeerId peerId, List<Multiaddr> addresses) {
        if (peerId.equals(localPeerId)) return InsertOutcome.IGNORED;
        int bucketIdx = com.libp2p.kademlia.XorId.bucketIndex(localKey, com.libp2p.kademlia.XorId.fromPeerId(peerId));
        KBucketEntry entry = new KBucketEntry(peerId, addresses, Instant.now());
        KBucket.InsertResult result = buckets[bucketIdx].insert(entry);
        if (result == KBucket.InsertResult.INSERTED) {
            return InsertOutcome.INSERTED;
        }
        if (result == KBucket.InsertResult.ALREADY_PRESENT) {
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

    public CompletableFuture<Boolean> insertWithAdmissionCheck(PeerId peerId, List<Multiaddr> addresses) {
        if (admissionCheck == null || admissionCheck == AdmissionCheck.ALLOW_ALL) {
            return CompletableFuture.completedFuture(insert(peerId, addresses));
        }
        return admissionCheck.checkAdmission(peerId).thenApply(admitted -> {
            if (admitted) {
                return insert(peerId, addresses);
            }
            return false;
        });
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
        return removed;
    }

    public boolean promoteReplacement(PeerId evicted) {
        byte[] remoteKey = com.libp2p.kademlia.XorId.fromPeerId(evicted);
        int bucketIdx = com.libp2p.kademlia.XorId.bucketIndex(localKey, remoteKey);
        boolean promoted = buckets[bucketIdx].promoteReplacement(evicted);
        return promoted;
    }

    public boolean discardPending(PeerId pendingPeerId) {
        byte[] remoteKey = com.libp2p.kademlia.XorId.fromPeerId(pendingPeerId);
        int bucketIdx = com.libp2p.kademlia.XorId.bucketIndex(localKey, remoteKey);
        return buckets[bucketIdx].discardPending(pendingPeerId);
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
                KadPeer.ConnectionType connType = resolveConnectionType(entry.peerId);
                allKnown.add(new KadPeer(entry.peerId, entry.getAddresses(), connType));
            }
        }
        allKnown.sort((a, b) -> {
            byte[] distA = com.libp2p.kademlia.XorId.xor(target, com.libp2p.kademlia.XorId.fromPeerId(a.nodeId));
            byte[] distB = com.libp2p.kademlia.XorId.xor(target, com.libp2p.kademlia.XorId.fromPeerId(b.nodeId));
            return com.libp2p.kademlia.XorId.compareDistance(distA, distB);
        });
        return allKnown.stream().limit(count).collect(Collectors.toList());
    }

    public KadPeer.ConnectionType resolveConnectionType(PeerId peerId) {
        if (host != null) {
            try {
                for (io.libp2p.core.Connection conn : host.getNetwork().getConnections()) {
                    if (conn.secureSession().getRemoteId().equals(peerId)) {
                        return KadPeer.ConnectionType.CONNECTED;
                    }
                }
            } catch (Exception ignored) {}
        }
        return KadPeer.ConnectionType.NOT_CONNECTED;
    }

    public Set<PeerId> getAllPeers() {
        Set<PeerId> result = new HashSet<>();
        for (KBucket bucket : buckets) {
            for (KBucketEntry entry : bucket.getEntries()) {
                result.add(entry.peerId);
            }
        }
        return result;
    }
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

    public int getRoutingTableSize() {
        return size();
    }

    public double getAverageBucketOccupancy() {
        int totalEntries = 0;
        int nonEmpty = 0;
        for (KBucket bucket : buckets) {
            int sz = bucket.size();
            if (sz > 0) {
                totalEntries += sz;
                nonEmpty++;
            }
        }
        return nonEmpty == 0 ? 0.0 : (double) totalEntries / nonEmpty;
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
