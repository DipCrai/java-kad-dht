package com.libp2p.kademlia;

import io.libp2p.core.PeerId;
import io.libp2p.core.multiformats.Multiaddr;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A provider record — indicates that a peer can provide content for a given key.
 * Port of rust-libp2p record::ProviderRecord.
 *
 * Equality is based on (key, provider) only — NOT addresses or expiry.
 */
public class ProviderRecord {
    private final byte[] key;
    private final PeerId provider;
    private final Instant expires;
    private final List<Multiaddr> addresses;

    public ProviderRecord(byte[] key, PeerId provider, Instant expires, List<Multiaddr> addresses) {
        this.key = key;
        this.provider = provider;
        this.expires = expires;
        this.addresses = addresses != null
                ? Collections.unmodifiableList(new ArrayList<>(addresses))
                : List.of();
    }

    public byte[] getKey() { return key; }
    public PeerId getProvider() { return provider; }
    public Instant getExpires() { return expires; }
    public List<Multiaddr> getAddresses() { return addresses; }

    public boolean isExpired() {
        return expires != null && Instant.now().isAfter(expires);
    }

    public boolean isExpired(Instant now) {
        return expires != null && now.isAfter(expires);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ProviderRecord other)) return false;
        return Objects.equals(provider, other.provider) && java.util.Arrays.equals(key, other.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(provider, java.util.Arrays.hashCode(key));
    }

    @Override
    public String toString() {
        return "ProviderRecord{key=" + XorId.toHex(key) +
                ", provider=" + provider +
                ", addrs=" + addresses.size() + "}";
    }
}
