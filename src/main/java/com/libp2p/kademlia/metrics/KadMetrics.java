package com.libp2p.kademlia.metrics;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class KadMetrics {
    public final AtomicLong lookupStarted = new AtomicLong();
    public final AtomicLong lookupFinished = new AtomicLong();
    public final AtomicLong lookupFailed = new AtomicLong();
    public final AtomicLong lookupDurationMs = new AtomicLong();
    public final AtomicLong rpcSuccess = new AtomicLong();
    public final AtomicLong rpcTimeout = new AtomicLong();
    public final AtomicLong rpcFailure = new AtomicLong();
    public final AtomicLong recordsStored = new AtomicLong();
    public final AtomicLong providersStored = new AtomicLong();
    public final AtomicLong bootstrapCount = new AtomicLong();
    public final AtomicLong timeoutCount = new AtomicLong();
    public final AtomicLong replicationSuccess = new AtomicLong();
    public final AtomicLong replicationFailure = new AtomicLong();
    public final AtomicLong quorumAchieved = new AtomicLong();
    public final AtomicLong quorumFailed = new AtomicLong();
    public final AtomicLong queryCancellation = new AtomicLong();

    private final ConcurrentHashMap<String, AtomicLong> rpcByType = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> rpcLatencySumMs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> rpcLatencyCount = new ConcurrentHashMap<>();

    public void recordLookupStart() { lookupStarted.incrementAndGet(); }
    public void recordLookupFinish() { lookupFinished.incrementAndGet(); }
    public void recordLookupFailure() { lookupFailed.incrementAndGet(); }
    public void recordLookupDuration(long millis) { lookupDurationMs.addAndGet(millis); }
    public void recordRpcSuccess() { rpcSuccess.incrementAndGet(); }
    public void recordRpcTimeout() { rpcTimeout.incrementAndGet(); timeoutCount.incrementAndGet(); }
    public void recordRpcFailure() { rpcFailure.incrementAndGet(); }

    public void recordRpcByType(String type) {
        rpcByType.computeIfAbsent(type, k -> new AtomicLong()).incrementAndGet();
    }

    public void recordRpcLatency(String type, long millis) {
        rpcLatencySumMs.computeIfAbsent(type, k -> new AtomicLong()).addAndGet(millis);
        rpcLatencyCount.computeIfAbsent(type, k -> new AtomicLong()).incrementAndGet();
    }

    public void recordRpc(String type, long millis, boolean success) {
        recordRpcByType(type);
        recordRpcLatency(type, millis);
        if (success) recordRpcSuccess(); else recordRpcFailure();
    }

    public long getRpcCountByType(String type) {
        AtomicLong v = rpcByType.get(type);
        return v != null ? v.get() : 0;
    }

    public double getAverageRpcLatency(String type) {
        AtomicLong sum = rpcLatencySumMs.get(type);
        AtomicLong count = rpcLatencyCount.get(type);
        if (sum == null || count == null || count.get() == 0) return 0;
        return (double) sum.get() / count.get();
    }

    public void recordReplicationSuccess() { replicationSuccess.incrementAndGet(); }
    public void recordReplicationFailure() { replicationFailure.incrementAndGet(); }
    public void recordQuorumAchieved() { quorumAchieved.incrementAndGet(); }
    public void recordQuorumFailed() { quorumFailed.incrementAndGet(); }
    public void recordQueryCancellation() { queryCancellation.incrementAndGet(); }

    public ConcurrentHashMap<String, AtomicLong> getRpcByType() { return rpcByType; }
    public ConcurrentHashMap<String, AtomicLong> getRpcLatencySumMs() { return rpcLatencySumMs; }
    public ConcurrentHashMap<String, AtomicLong> getRpcLatencyCount() { return rpcLatencyCount; }

    @Override
    public String toString() {
        return "KadMetrics{lookups=" + lookupStarted + "/" + lookupFinished + "/" + lookupFailed +
                ", rpc=" + rpcSuccess + "/" + rpcTimeout + "/" + rpcFailure +
                ", records=" + recordsStored + ", providers=" + providersStored +
                ", replication=" + replicationSuccess + "/" + replicationFailure +
                ", quorum=" + quorumAchieved + "/" + quorumFailed + "}";
    }
}
