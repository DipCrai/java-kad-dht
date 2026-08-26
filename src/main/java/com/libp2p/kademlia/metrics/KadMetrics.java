package com.libp2p.kademlia.metrics;

import java.util.concurrent.atomic.AtomicLong;

public class KadMetrics {
    public final AtomicLong lookupStarted = new AtomicLong();
    public final AtomicLong lookupFinished = new AtomicLong();
    public final AtomicLong lookupFailed = new AtomicLong();
    public final AtomicLong rpcSuccess = new AtomicLong();
    public final AtomicLong rpcTimeout = new AtomicLong();
    public final AtomicLong rpcFailure = new AtomicLong();
    public final AtomicLong recordsStored = new AtomicLong();
    public final AtomicLong providersStored = new AtomicLong();
    public final AtomicLong bootstrapCount = new AtomicLong();

    public void recordLookupStart() { lookupStarted.incrementAndGet(); }
    public void recordLookupFinish() { lookupFinished.incrementAndGet(); }
    public void recordLookupFailure() { lookupFailed.incrementAndGet(); }
    public void recordRpcSuccess() { rpcSuccess.incrementAndGet(); }
    public void recordRpcTimeout() { rpcTimeout.incrementAndGet(); }
    public void recordRpcFailure() { rpcFailure.incrementAndGet(); }

    @Override
    public String toString() {
        return "KadMetrics{lookups=" + lookupStarted + "/" + lookupFinished + "/" + lookupFailed +
                ", rpc=" + rpcSuccess + "/" + rpcTimeout + "/" + rpcFailure +
                ", records=" + recordsStored + ", providers=" + providersStored + "}";
    }
}
