package com.libp2p.kademlia.security;

public class RequestLimits {
    public final int maxMessageSize;
    public final int maxConcurrentQueries;
    public final int maxInboundRequests;
    public final int maxCloserPeers;
    public final int maxProviderPeers;
    public final int maxRecordSize;

    public RequestLimits(int maxMessageSize, int maxConcurrentQueries, int maxInboundRequests,
                         int maxCloserPeers, int maxProviderPeers, int maxRecordSize) {
        this.maxMessageSize = maxMessageSize;
        this.maxConcurrentQueries = maxConcurrentQueries;
        this.maxInboundRequests = maxInboundRequests;
        this.maxCloserPeers = maxCloserPeers;
        this.maxProviderPeers = maxProviderPeers;
        this.maxRecordSize = maxRecordSize;
    }

    public static RequestLimits defaults() {
        return new RequestLimits(16384, 100, 100, 40, 20, 65536);
    }
}
