package com.libp2p.kademlia.records;

import io.libp2p.core.PeerId;
import io.libp2p.core.multiformats.Multiaddr;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class ProviderRecord {

    private final byte[] key;
    private final PeerId provider;
    private final Instant expires;
    private final List<Multiaddr> addresses;

    public ProviderRecord(byte[] key, PeerId provider, Instant expires, List<Multiaddr> addresses) {
        this.key = key.clone();
        this.provider = provider;
        this.expires = expires;
        this.addresses = new ArrayList<>(addresses);
    }

    public byte[] getKey() { return key.clone(); }
    public PeerId getProvider() { return provider; }
    public Instant getExpires() { return expires; }
    public List<Multiaddr> getAddresses() { return new ArrayList<>(addresses); }

    public boolean isExpired() {
        return isExpired(Instant.now());
    }

    public boolean isExpired(Instant now) {
        return expires != null && now.isAfter(expires);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProviderRecord that = (ProviderRecord) o;
        return Arrays.equals(key, that.key) && Objects.equals(provider, that.provider);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(key);
        result = 31 * result + Objects.hashCode(provider);
        return result;
    }

    @Override
    public String toString() {
        return "ProviderRecord{key=" + Arrays.toString(key) +
                ", provider=" + provider +
                ", expires=" + expires +
                ", addresses.size=" + addresses.size() +
                '}';
    }
}
