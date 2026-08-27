package com.libp2p.kademlia.config;

import com.libp2p.kademlia.query.QueryFilter;
import com.libp2p.kademlia.records.RecordValidator;
import com.libp2p.kademlia.routing.AdmissionCheck;
import com.libp2p.kademlia.routing.DefaultPeerDiversityPolicy;
import com.libp2p.kademlia.routing.PeerDiversityPolicy;
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
    private final int replicationFactor;
    private final int writeQuorum;
    private final int readQuorum;
    private final int disjointPaths;
    private final List<Multiaddr> bootstrapNodes;
    private final KadMode mode;
    private final RecordValidator validator;
    private final PeerDiversityPolicy peerDiversityPolicy;
    private final QueryFilter queryFilter;
    private final AdmissionCheck admissionCheck;
    private final Duration bootstrapAddressTTL;
    private final Duration peerAddressTTL;

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
        this.replicationFactor = builder.replicationFactor;
        this.writeQuorum = builder.writeQuorum;
        this.readQuorum = builder.readQuorum;
        this.disjointPaths = builder.disjointPaths;
        this.bootstrapNodes = Collections.unmodifiableList(new ArrayList<>(builder.bootstrapNodes));
        this.mode = builder.mode;
        this.validator = builder.validator;
        this.peerDiversityPolicy = builder.peerDiversityPolicy;
        this.queryFilter = builder.queryFilter;
        this.admissionCheck = builder.admissionCheck;
        this.bootstrapAddressTTL = builder.bootstrapAddressTTL;
        this.peerAddressTTL = builder.peerAddressTTL;
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
    public int getReplicationFactor() { return replicationFactor; }
    public int getWriteQuorum() { return writeQuorum; }
    public int getReadQuorum() { return readQuorum; }
    public int getDisjointPaths() { return disjointPaths; }
    public List<Multiaddr> getBootstrapNodes() { return bootstrapNodes; }
    public KadMode getMode() { return mode; }
    public RecordValidator getValidator() { return validator; }
    public PeerDiversityPolicy getPeerDiversityPolicy() { return peerDiversityPolicy; }
    public QueryFilter getQueryFilter() { return queryFilter; }
    public AdmissionCheck getAdmissionCheck() { return admissionCheck; }
    public Duration getBootstrapAddressTTL() { return bootstrapAddressTTL; }
    public Duration getPeerAddressTTL() { return peerAddressTTL; }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String protocolName = "/ipfs/kad/1.0.0";
        private int kValue = 20;
        private int alphaValue = 10;
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
        private int replicationFactor = 20;
        private int writeQuorum = 1;
        private int readQuorum = 1;
        private int disjointPaths = 1;
        private List<Multiaddr> bootstrapNodes = new ArrayList<>();
        private KadMode mode = KadMode.AUTO_SERVER;
        private RecordValidator validator = RecordValidator.NOOP;
        private PeerDiversityPolicy peerDiversityPolicy = new DefaultPeerDiversityPolicy();
        private QueryFilter queryFilter;
        private AdmissionCheck admissionCheck = AdmissionCheck.ALLOW_ALL;
        private Duration bootstrapAddressTTL = Duration.ofMinutes(5);
        private Duration peerAddressTTL = Duration.ofMinutes(30);

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
        public Builder replicationFactor(int replicationFactor) { this.replicationFactor = replicationFactor; return this; }
        public Builder writeQuorum(int writeQuorum) { this.writeQuorum = writeQuorum; return this; }
        public Builder readQuorum(int readQuorum) { this.readQuorum = readQuorum; return this; }
        public Builder disjointPaths(int disjointPaths) { this.disjointPaths = disjointPaths; return this; }
        public Builder bootstrapNodes(List<Multiaddr> bootstrapNodes) { this.bootstrapNodes = new ArrayList<>(bootstrapNodes); return this; }
        public Builder mode(KadMode mode) { this.mode = mode; return this; }
        public Builder validator(RecordValidator validator) { this.validator = validator; return this; }
        public Builder peerDiversityPolicy(PeerDiversityPolicy peerDiversityPolicy) { this.peerDiversityPolicy = peerDiversityPolicy; return this; }
        public Builder queryFilter(QueryFilter queryFilter) { this.queryFilter = queryFilter; return this; }
        public Builder admissionCheck(AdmissionCheck admissionCheck) { this.admissionCheck = admissionCheck; return this; }
        public Builder bootstrapAddressTTL(Duration bootstrapAddressTTL) { this.bootstrapAddressTTL = bootstrapAddressTTL; return this; }
        public Builder peerAddressTTL(Duration peerAddressTTL) { this.peerAddressTTL = peerAddressTTL; return this; }

        public KadConfig build() {
            if (kValue <= 0) throw new IllegalArgumentException("kValue must be > 0, got " + kValue);
            if (alphaValue <= 0) throw new IllegalArgumentException("alphaValue must be > 0, got " + alphaValue);
            if (alphaValue > kValue) throw new IllegalArgumentException("alphaValue must be <= kValue (" + kValue + "), got " + alphaValue);
            if (betaValue < 0) throw new IllegalArgumentException("betaValue must be >= 0, got " + betaValue);
            if (betaValue > kValue) throw new IllegalArgumentException("betaValue must be <= kValue (" + kValue + "), got " + betaValue);
            if (replicationFactor <= 0) throw new IllegalArgumentException("replicationFactor must be > 0, got " + replicationFactor);
            if (replicationFactor > kValue) throw new IllegalArgumentException("replicationFactor must be <= kValue (" + kValue + "), got " + replicationFactor);
            if (writeQuorum <= 0) throw new IllegalArgumentException("writeQuorum must be > 0, got " + writeQuorum);
            if (writeQuorum > replicationFactor) throw new IllegalArgumentException("writeQuorum must be <= replicationFactor (" + replicationFactor + "), got " + writeQuorum);
            if (readQuorum <= 0) throw new IllegalArgumentException("readQuorum must be > 0, got " + readQuorum);
            if (maxPacketSize <= 0) throw new IllegalArgumentException("maxPacketSize must be > 0, got " + maxPacketSize);
            if (queryTimeout == null || queryTimeout.isNegative() || queryTimeout.isZero()) throw new IllegalArgumentException("queryTimeout must be positive");
            if (substreamTimeout == null || substreamTimeout.isNegative() || substreamTimeout.isZero()) throw new IllegalArgumentException("substreamTimeout must be positive");
            if (bootstrapInterval == null || bootstrapInterval.isNegative() || bootstrapInterval.isZero()) throw new IllegalArgumentException("bootstrapInterval must be positive");
            if (pendingTimeout == null || pendingTimeout.isNegative() || pendingTimeout.isZero()) throw new IllegalArgumentException("pendingTimeout must be positive");
            if (providerRecordTTL == null || providerRecordTTL.isNegative() || providerRecordTTL.isZero()) throw new IllegalArgumentException("providerRecordTTL must be positive");
            if (providerAddrTTL == null || providerAddrTTL.isNegative() || providerAddrTTL.isZero()) throw new IllegalArgumentException("providerAddrTTL must be positive");
            if (recordMaxAge == null || recordMaxAge.isNegative() || recordMaxAge.isZero()) throw new IllegalArgumentException("recordMaxAge must be positive");
            if (recordReplicationInterval == null || recordReplicationInterval.isNegative() || recordReplicationInterval.isZero()) throw new IllegalArgumentException("recordReplicationInterval must be positive");
            if (recordPublicationInterval == null || recordPublicationInterval.isNegative() || recordPublicationInterval.isZero()) throw new IllegalArgumentException("recordPublicationInterval must be positive");
            if (providerPublicationInterval == null || providerPublicationInterval.isNegative() || providerPublicationInterval.isZero()) throw new IllegalArgumentException("providerPublicationInterval must be positive");
            if (disjointPaths < 1) throw new IllegalArgumentException("disjointPaths must be >= 1, got " + disjointPaths);
            return new KadConfig(this);
        }
    }
}
