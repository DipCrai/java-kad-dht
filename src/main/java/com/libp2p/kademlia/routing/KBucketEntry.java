package com.libp2p.kademlia.routing;

import io.libp2p.core.PeerId;
import io.libp2p.core.multiformats.Multiaddr;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class KBucketEntry implements com.libp2p.kademlia.XorId.HasPeerId {
    public final PeerId peerId;
    private final List<Multiaddr> addresses;
    private volatile Instant lastSeen;
    private volatile Instant lastSuccessfulOutbound;
    private final Instant firstSeen;

    public KBucketEntry(PeerId peerId, List<Multiaddr> addresses, Instant now) {
        this.peerId = Objects.requireNonNull(peerId);
        this.addresses = new ArrayList<>(addresses != null ? addresses : List.of());
        this.lastSeen = now;
        this.firstSeen = now;
    }

    @Override
    public PeerId getPeerId() { return peerId; }
    public List<Multiaddr> getAddresses() { return Collections.unmodifiableList(addresses); }
    public Instant getLastSeen() { return lastSeen; }
    public Instant getLastSuccessfulOutbound() { return lastSuccessfulOutbound; }
    public Instant getFirstSeen() { return firstSeen; }
    public void markSeen(Instant now) { this.lastSeen = now; }
    public void markSuccessfulOutbound(Instant now) { this.lastSuccessfulOutbound = now; }

    public void addAddress(Multiaddr addr) {
        if (!addresses.contains(addr)) addresses.add(addr);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof KBucketEntry other)) return false;
        return peerId.equals(other.peerId);
    }

    @Override
    public int hashCode() { return peerId.hashCode(); }

    @Override
    public String toString() {
        return "KBucketEntry{id=" + peerId + ", addrs=" + addresses.size() + ", seen=" + lastSeen + "}";
    }
}
