package com.libp2p.kademlia.records;

import io.libp2p.core.PeerId;
import io.libp2p.core.multiformats.Multiaddr;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * A provider record announcing that a peer has data for a given key.
 *
 * <p>Provider records have two expiry times:</p>
 * <ul>
 *   <li>{@code expires} — when the provider record itself expires</li>
 *   <li>{@code addrExpiry} — when the advertised addresses expire (shorter)</li>
 * </ul>
 *
 * <p>Equality is based on key + provider peer ID (ignoring expiry and addresses).</p>
 *
 * @see ProviderStore
 * @see MemoryProviderStore
 */
public class ProviderRecord {

    private final byte[] key;
    private final PeerId provider;
    private final Instant expires;
    private final Instant addrExpiry;
    private final List<Multiaddr> addresses;

    /**
     * Create a provider record with separate record and address expiry.
     *
     * @param key       the content key
     * @param provider  the provider peer
     * @param expires   record expiry instant
     * @param addrExpiry address expiry instant (shorter than record expiry)
     * @param addresses the provider's multiaddresses
     */
    public ProviderRecord(byte[] key, PeerId provider, Instant expires, Instant addrExpiry, List<Multiaddr> addresses) {
        this.key = key.clone();
        this.provider = provider;
        this.expires = expires;
        this.addrExpiry = addrExpiry;
        this.addresses = new ArrayList<>(addresses);
    }

    /**
     * Create a provider record with same expiry for record and addresses.
     *
     * @param key       the content key
     * @param provider  the provider peer
     * @param expires   expiry instant
     * @param addresses the provider's multiaddresses
     */
    public ProviderRecord(byte[] key, PeerId provider, Instant expires, List<Multiaddr> addresses) {
        this(key, provider, expires, expires, addresses);
    }

    /** @return the content key (defensive copy) */
    public byte[] getKey() { return key.clone(); }

    /** @return the provider peer ID */
    public PeerId getProvider() { return provider; }

    /** @return the record expiry instant */
    public Instant getExpires() { return expires; }

    /** @return the address expiry instant */
    public Instant getAddrExpiry() { return addrExpiry; }

    /** @return a copy of the provider's multiaddresses */
    public List<Multiaddr> getAddresses() { return new ArrayList<>(addresses); }

    /**
     * Get addresses only if the address expiry has not passed.
     *
     * @return addresses if still valid, or empty list if expired
     */
    public List<Multiaddr> getAliveAddresses() {
        Instant now = Instant.now();
        if (addrExpiry != null && now.isAfter(addrExpiry)) return List.of();
        return new ArrayList<>(addresses);
    }

    /** @return true if this provider record has expired */
    public boolean isExpired() {
        return isExpired(Instant.now());
    }

    /**
     * Check if this record is expired at the given time.
     *
     * @param now the time to check
     * @return true if expired
     */
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
                ", addrExpiry=" + addrExpiry +
                ", addresses.size=" + addresses.size() +
                '}';
    }
}
