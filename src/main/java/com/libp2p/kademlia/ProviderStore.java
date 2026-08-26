package com.libp2p.kademlia;

import io.libp2p.core.PeerId;

import java.util.List;

/**
 * Interface for DHT provider record storage.
 * Port of rust RecordStore provider methods + go ProviderManager.
 */
public interface ProviderStore {

    /**
     * Add a provider record. Returns true if stored.
     */
    boolean addProvider(ProviderRecord record);

    /**
     * Get all non-expired provider records for a key.
     */
    List<ProviderRecord> getProviders(byte[] key);

    /**
     * Get all provider records where the local node is the provider.
     */
    Iterable<ProviderRecord> provided();

    /**
     * Remove a specific provider for a key.
     */
    void removeProvider(byte[] key, PeerId provider);

    /**
     * Number of provider keys stored.
     */
    int keyCount();
}
