package com.libp2p.kademlia.config;

import com.libp2p.kademlia.records.RecordValidator;
import io.libp2p.core.multiformats.Multiaddr;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class KadConfig {

    private final String protocolName;
    private final int kValue;
    private final int alphaValue;
    private final int betaValue;
    private final Duration queryTimeout;
    private final Duration substreamTimeout;
    private final int maxPacketSize;
    private final Duration bootstrapInterval;
    private final Duration pendingTimeout;
    private final Duration providerRecordTTL;
    private final Duration providerAddrTTL;
    private final Duration recordMaxAge;
    private final Duration recordReplicationInterval;
    private final Duration recordPublicationInterval;
    private final Duration providerPublicationInterval;
    private final int maxRecords;
    private final int maxProvidedKeys;
    private final int maxProvidersPerKey;
    private final int maxRecordValueSize;
    private final int maxConcurrentQueries;
    private final int maxInboundRequests;
    private final int quorum;
    private final int replicationFactor;
    private final int writeQuorum;
    private final int readQuorum;
    private final int disjointPaths;
    private final List<Multiaddr> bootstrapNodes;
    private final KadMode mode;
    private final RecordValidator validator;

    private KadConfig(Builder builder) {
        this.protocolName = builder.protocolName;
        this.kValue = builder.kValue;
        this.alphaValue = builder.alphaValue;
        this.betaValue = builder.betaValue;
        this.queryTimeout = builder.queryTimeout;
        this.substreamTimeout = builder.substreamTimeout;
        this.maxPacketSize = builder.maxPacketSize;
        this.bootstrapInterval = builder.bootstrapInterval;
        this.pendingTimeout = builder.pendingTimeout;
        this.providerRecordTTL = builder.providerRecordTTL;
        this.providerAddrTTL = builder.providerAddrTTL;
        this.recordMaxAge = builder.recordMaxAge;
        this.recordReplicationInterval = builder.recordReplicationInterval;
        this.recordPublicationInterval = builder.recordPublicationInterval;
        this.providerPublicationInterval = builder.providerPublicationInterval;
        this.maxRecords = builder.maxRecords;
        this.maxProvidedKeys = builder.maxProvidedKeys;
        this.maxProvidersPerKey = builder.maxProvidersPerKey;
        this.maxRecordValueSize = builder.maxRecordValueSize;
        this.maxConcurrentQueries = builder.maxConcurrentQueries;
        this.maxInboundRequests = builder.maxInboundRequests;
        this.quorum = builder.quorum;
        this.replicationFactor = builder.replicationFactor;
        this.writeQuorum = builder.writeQuorum;
        this.readQuorum = builder.readQuorum;
        this.disjointPaths = builder.disjointPaths;
        this.bootstrapNodes = Collections.unmodifiableList(new ArrayList<>(builder.bootstrapNodes));
        this.mode = builder.mode;
        this.validator = builder.validator;
    }

    public String getProtocolName() { return protocolName; }
    public int getKValue() { return kValue; }
    public int getAlphaValue() { return alphaValue; }
    public int getBetaValue() { return betaValue; }
    public Duration getQueryTimeout() { return queryTimeout; }
    public Duration getSubstreamTimeout() { return substreamTimeout; }
    public int getMaxPacketSize() { return maxPacketSize; }
    public Duration getBootstrapInterval() { return bootstrapInterval; }
    public Duration getPendingTimeout() { return pendingTimeout; }
    public Duration getProviderRecordTTL() { return providerRecordTTL; }
    public Duration getProviderAddrTTL() { return providerAddrTTL; }
    public Duration getRecordMaxAge() { return recordMaxAge; }
    public Duration getRecordReplicationInterval() { return recordReplicationInterval; }
    public Duration getRecordPublicationInterval() { return recordPublicationInterval; }
    public Duration getProviderPublicationInterval() { return providerPublicationInterval; }
    public int getMaxRecords() { return maxRecords; }
    public int getMaxProvidedKeys() { return maxProvidedKeys; }
    public int getMaxProvidersPerKey() { return maxProvidersPerKey; }
    public int getMaxRecordValueSize() { return maxRecordValueSize; }
    public int getMaxConcurrentQueries() { return maxConcurrentQueries; }
    public int getMaxInboundRequests() { return maxInboundRequests; }
    public int getQuorum() { return quorum; }
    public int getReplicationFactor() { return replicationFactor; }
    public int getWriteQuorum() { return writeQuorum; }
    public int getReadQuorum() { return readQuorum; }
    public int getDisjointPaths() { return disjointPaths; }
    public List<Multiaddr> getBootstrapNodes() { return bootstrapNodes; }
    public KadMode getMode() { return mode; }
    public RecordValidator getValidator() { return validator; }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String protocolName = "/ipfs/kad/1.0.0";
        private int kValue = 20;
        private int alphaValue = 3;
        private int betaValue = 3;
        private Duration queryTimeout = Duration.ofSeconds(60);
        private Duration substreamTimeout = Duration.ofSeconds(10);
        private int maxPacketSize = 16384;
        private Duration bootstrapInterval = Duration.ofMinutes(5);
        private Duration pendingTimeout = Duration.ofSeconds(60);
        private Duration providerRecordTTL = Duration.ofHours(48);
        private Duration providerAddrTTL = Duration.ofMinutes(30);
        private Duration recordMaxAge = Duration.ofHours(48);
        private Duration recordReplicationInterval = Duration.ofHours(1);
        private Duration recordPublicationInterval = Duration.ofHours(22);
        private Duration providerPublicationInterval = Duration.ofHours(12);
        private int maxRecords = 1024;
        private int maxProvidedKeys = 1024;
        private int maxProvidersPerKey = 20;
        private int maxRecordValueSize = 65536;
        private int maxConcurrentQueries = 100;
        private int maxInboundRequests = 100;
        private int quorum = 3;
        private int replicationFactor = 20;
        private int writeQuorum = 1;
        private int readQuorum = 1;
        private int disjointPaths = 1;
        private List<Multiaddr> bootstrapNodes = new ArrayList<>();
        private KadMode mode = KadMode.AUTO_SERVER;
        private RecordValidator validator = RecordValidator.NOOP;

        private Builder() {}

        public Builder protocolName(String protocolName) { this.protocolName = protocolName; return this; }
        public Builder kValue(int kValue) { this.kValue = kValue; return this; }
        public Builder alphaValue(int alphaValue) { this.alphaValue = alphaValue; return this; }
        public Builder betaValue(int betaValue) { this.betaValue = betaValue; return this; }
        public Builder queryTimeout(Duration queryTimeout) { this.queryTimeout = queryTimeout; return this; }
        public Builder substreamTimeout(Duration substreamTimeout) { this.substreamTimeout = substreamTimeout; return this; }
        public Builder maxPacketSize(int maxPacketSize) { this.maxPacketSize = maxPacketSize; return this; }
        public Builder bootstrapInterval(Duration bootstrapInterval) { this.bootstrapInterval = bootstrapInterval; return this; }
        public Builder pendingTimeout(Duration pendingTimeout) { this.pendingTimeout = pendingTimeout; return this; }
        public Builder providerRecordTTL(Duration providerRecordTTL) { this.providerRecordTTL = providerRecordTTL; return this; }
        public Builder providerAddrTTL(Duration providerAddrTTL) { this.providerAddrTTL = providerAddrTTL; return this; }
        public Builder recordMaxAge(Duration recordMaxAge) { this.recordMaxAge = recordMaxAge; return this; }
        public Builder recordReplicationInterval(Duration recordReplicationInterval) { this.recordReplicationInterval = recordReplicationInterval; return this; }
        public Builder recordPublicationInterval(Duration recordPublicationInterval) { this.recordPublicationInterval = recordPublicationInterval; return this; }
        public Builder providerPublicationInterval(Duration providerPublicationInterval) { this.providerPublicationInterval = providerPublicationInterval; return this; }
        public Builder maxRecords(int maxRecords) { this.maxRecords = maxRecords; return this; }
        public Builder maxProvidedKeys(int maxProvidedKeys) { this.maxProvidedKeys = maxProvidedKeys; return this; }
        public Builder maxProvidersPerKey(int maxProvidersPerKey) { this.maxProvidersPerKey = maxProvidersPerKey; return this; }
        public Builder maxRecordValueSize(int maxRecordValueSize) { this.maxRecordValueSize = maxRecordValueSize; return this; }
        public Builder maxConcurrentQueries(int maxConcurrentQueries) { this.maxConcurrentQueries = maxConcurrentQueries; return this; }
        public Builder maxInboundRequests(int maxInboundRequests) { this.maxInboundRequests = maxInboundRequests; return this; }
        public Builder quorum(int quorum) { this.quorum = quorum; return this; }
        public Builder replicationFactor(int replicationFactor) { this.replicationFactor = replicationFactor; return this; }
        public Builder writeQuorum(int writeQuorum) { this.writeQuorum = writeQuorum; return this; }
        public Builder readQuorum(int readQuorum) { this.readQuorum = readQuorum; return this; }
        public Builder disjointPaths(int disjointPaths) { this.disjointPaths = disjointPaths; return this; }
        public Builder bootstrapNodes(List<Multiaddr> bootstrapNodes) { this.bootstrapNodes = new ArrayList<>(bootstrapNodes); return this; }
        public Builder mode(KadMode mode) { this.mode = mode; return this; }
        public Builder validator(RecordValidator validator) { this.validator = validator; return this; }

        public KadConfig build() {
            return new KadConfig(this);
        }
    }
}
