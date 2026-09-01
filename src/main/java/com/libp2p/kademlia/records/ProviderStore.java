package com.libp2p.kademlia.records;

import io.libp2p.core.PeerId;
import io.libp2p.core.multiformats.Multiaddr;
import io.libp2p.core.multistream.ProtocolBinding;
import java.util.Map;

/**
 * Storage interface for provider records.
 *
 * <p>A provider record announces that a peer has data for a given key.
 * Multiple providers can exist per key. Implementations must be thread-safe.</p>
 *
 * <p>Default implementation: {@link MemoryProviderStore}.</p>
 *
 * @see MemoryProviderStore
 * @see ProviderRecord
 */
public interface ProviderStore {

    /**
     * Add or update a provider record.
     *
     * @param record the provider record
     * @return true if added, false if duplicate or expired
     */
    boolean addProvider(ProviderRecord record);

    /**
     * Get all non-expired providers for a key.
     *
     * @param key the content key
     * @return list of provider records (may be empty)
     */
    java.util.List<ProviderRecord> getProviders(byte[] key);

    /**
     * Iterate over all provider records (including expired).
     *
     * @return iterable of all provider records
     */
    Iterable<ProviderRecord> provided();

    /**
     * Remove a specific provider for a key.
     *
     * @param key      the content key
     * @param provider the provider peer ID
     */
    void removeProvider(byte[] key, PeerId provider);

    /**
     * Number of keys with providers.
     *
     * @return key count
     */
    int keyCount();

    /**
     * Remove expired provider records.
     *
     * @return number of records removed
     */
    default int garbageCollect() { return 0; }
}
