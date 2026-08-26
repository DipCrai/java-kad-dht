package com.libp2p.kademlia;

import java.util.Map;

/**
 * Interface for DHT record storage.
 * Port of rust RecordStore trait + go ValueStore.
 */
public interface RecordStore {

    /**
     * Get a record by key. Returns null if not found or expired.
     */
    Record get(byte[] key);

    /**
     * Store a record. Returns true if stored, false if rejected by validation or old.
     * @throws IllegalArgumentException if the record is invalid
     */
    boolean put(Record record);

    /**
     * Remove a record by key.
     */
    void remove(byte[] key);

    /**
     * Get all non-expired records.
     */
    Iterable<Record> records();

    /**
     * Get the number of stored records.
     */
    int size();
}
