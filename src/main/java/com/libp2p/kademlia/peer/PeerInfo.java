package com.libp2p.kademlia.peer;

import io.libp2p.core.PeerId;
import io.libp2p.core.multiformats.Multiaddr;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class PeerInfo {

    public final PeerId peerId;
    private volatile PeerState state;
    private volatile Instant lastSeen;
    private volatile Instant lastSuccessfulRequest;
    private volatile int failureCount;
    private volatile boolean supportsKad;
    private volatile List<Multiaddr> addresses;

    public PeerInfo(PeerId peerId) {
        this.peerId = peerId;
        this.state = PeerState.UNKNOWN;
        this.lastSeen = Instant.EPOCH;
        this.lastSuccessfulRequest = Instant.EPOCH;
        this.failureCount = 0;
        this.supportsKad = false;
        this.addresses = new ArrayList<>();
    }

    public PeerInfo(PeerId peerId, PeerState state, List<Multiaddr> addresses) {
        this.peerId = peerId;
        this.state = state;
        this.lastSeen = Instant.now();
        this.lastSuccessfulRequest = Instant.EPOCH;
        this.failureCount = 0;
        this.supportsKad = false;
        this.addresses = new ArrayList<>(addresses);
    }

    public PeerId getPeerId() { return peerId; }
    public PeerState getState() { return state; }
    public Instant getLastSeen() { return lastSeen; }
    public Instant getLastSuccessfulRequest() { return lastSuccessfulRequest; }
    public int getFailureCount() { return failureCount; }
    public boolean isSupportsKad() { return supportsKad; }
    public List<Multiaddr> getAddresses() { return new ArrayList<>(addresses); }

    public void setState(PeerState state) { this.state = state; }
    public void setLastSeen(Instant lastSeen) { this.lastSeen = lastSeen; }
    public void setLastSuccessfulRequest(Instant lastSuccessfulRequest) { this.lastSuccessfulRequest = lastSuccessfulRequest; }
    public void setSupportsKad(boolean supportsKad) { this.supportsKad = supportsKad; }
    public void setAddresses(List<Multiaddr> addresses) { this.addresses = new ArrayList<>(addresses); }

    public void recordSuccess() {
        this.failureCount = 0;
        this.state = PeerState.ACTIVE;
        this.lastSeen = Instant.now();
        this.lastSuccessfulRequest = Instant.now();
    }

    public void recordFailure() {
        this.failureCount++;
        this.lastSeen = Instant.now();
        if (this.failureCount >= 3) {
            this.state = PeerState.FAILED;
        }
    }

    public boolean isDead() {
        return this.state == PeerState.DEAD ||
                (this.failureCount >= 5 && this.state == PeerState.FAILED);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PeerInfo peerInfo = (PeerInfo) o;
        return Objects.equals(peerId, peerInfo.peerId);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(peerId);
    }

    @Override
    public String toString() {
        return "PeerInfo{peerId=" + peerId + ", state=" + state +
                ", failureCount=" + failureCount + '}';
    }
}
