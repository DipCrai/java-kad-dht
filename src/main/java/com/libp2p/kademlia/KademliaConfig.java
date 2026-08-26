package com.libp2p.kademlia;

import io.libp2p.core.multiformats.Multiaddr;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class KademliaConfig {
    public final String protocolName;
    public final int kValue;
    public final int alphaValue;
    public final int betaValue;
    public final Duration queryTimeout;
    public final Duration substreamTimeout;
    public final int maxPacketSize;
    public final Duration bootstrapInterval;
    public final Duration pendingTimeout;
    public final Duration providerRecordTTL;
    public final Duration recordMaxAge;
    public final Duration providerPublicationInterval;
    public final int maxRecords;
    public final int maxProvidedKeys;
    public final int maxProvidersPerKey;
    public final int maxRecordValueSize;
    public final List<Multiaddr> bootstrapNodes;
    public final KademliaMode mode;

    private KademliaConfig(Builder builder) {
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
        this.recordMaxAge = builder.recordMaxAge;
        this.providerPublicationInterval = builder.providerPublicationInterval;
        this.maxRecords = builder.maxRecords;
        this.maxProvidedKeys = builder.maxProvidedKeys;
        this.maxProvidersPerKey = builder.maxProvidersPerKey;
        this.maxRecordValueSize = builder.maxRecordValueSize;
        this.bootstrapNodes = Collections.unmodifiableList(new ArrayList<>(builder.bootstrapNodes));
        this.mode = builder.mode;
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
        private Duration recordMaxAge = Duration.ofHours(48);
        private Duration providerPublicationInterval = Duration.ofHours(12);
        private int maxRecords = 1024;
        private int maxProvidedKeys = 1024;
        private int maxProvidersPerKey = 20;
        private int maxRecordValueSize = 65 * 1024;
        private List<Multiaddr> bootstrapNodes = new ArrayList<>();
        private KademliaMode mode = KademliaMode.AUTO_SERVER;

        public Builder protocolName(String v) { this.protocolName = v; return this; }
        public Builder kValue(int v) { this.kValue = v; return this; }
        public Builder alphaValue(int v) { this.alphaValue = v; return this; }
        public Builder betaValue(int v) { this.betaValue = v; return this; }
        public Builder queryTimeout(Duration v) { this.queryTimeout = v; return this; }
        public Builder substreamTimeout(Duration v) { this.substreamTimeout = v; return this; }
        public Builder maxPacketSize(int v) { this.maxPacketSize = v; return this; }
        public Builder bootstrapInterval(Duration v) { this.bootstrapInterval = v; return this; }
        public Builder pendingTimeout(Duration v) { this.pendingTimeout = v; return this; }
        public Builder providerRecordTTL(Duration v) { this.providerRecordTTL = v; return this; }
        public Builder recordMaxAge(Duration v) { this.recordMaxAge = v; return this; }
        public Builder providerPublicationInterval(Duration v) { this.providerPublicationInterval = v; return this; }
        public Builder maxRecords(int v) { this.maxRecords = v; return this; }
        public Builder maxProvidedKeys(int v) { this.maxProvidedKeys = v; return this; }
        public Builder maxProvidersPerKey(int v) { this.maxProvidersPerKey = v; return this; }
        public Builder maxRecordValueSize(int v) { this.maxRecordValueSize = v; return this; }
        public Builder mode(KademliaMode v) { this.mode = v; return this; }

        public Builder bootstrapNodes(List<Multiaddr> nodes) {
            this.bootstrapNodes = new ArrayList<>(nodes);
            return this;
        }

        public Builder bootstrapNode(Multiaddr addr) {
            this.bootstrapNodes.add(addr);
            return this;
        }

        public Builder bootstrapNode(String addr) {
            this.bootstrapNodes.add(Multiaddr.fromString(addr));
            return this;
        }

        public Builder bootstrapNodes(String... addrs) {
            for (String addr : addrs) {
                this.bootstrapNodes.add(Multiaddr.fromString(addr));
            }
            return this;
        }

        public KademliaConfig build() {
            return new KademliaConfig(this);
        }
    }
}
