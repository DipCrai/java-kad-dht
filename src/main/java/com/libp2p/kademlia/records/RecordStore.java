package com.libp2p.kademlia.records;

import java.util.List;

/**
 * Storage interface for DHT records.
 *
 * <p>Implementations must be thread-safe. The default implementation is
 * {@link MemoryRecordStore} which uses a ConcurrentHashMap with TTL-based GC.</p>
 *
 * <p>Key semantics: a single key can have multiple records (different publishers).
 * {@link #get(byte[])} returns the newest; {@link #getAll(byte[])} returns all.</p>
 *
 * @see MemoryRecordStore
 * @see Record
 */
public interface RecordStore {

    /**
     * Get the most recent record for a key.
     *
     * @param key the record key
     * @return the record, or null if not found or expired
     */
    Record get(byte[] key);

    /**
     * Get all records for a key (including expired ones).
     *
     * @param key the record key
     * @return list of records (may be empty)
     */
    List<Record> getAll(byte[] key);

    /**
     * Store a record. If a record with the same key+value exists,
     * updates its timeReceived. If max capacity reached, oldest is evicted.
     *
     * @param record the record to store
     * @return true if the record was actually stored (not a duplicate)
     */
    boolean put(Record record);

    /**
     * Remove all records for a key.
     *
     * @param key the record key
     */
    void remove(byte[] key);

    /**
     * Iterate over all stored records.
     *
     * @return iterable of all records
     */
    Iterable<Record> records();

    /**
     * Number of records currently stored.
     *
     * @return record count
     */
    int size();

    /**
     * Remove expired records.
     *
     * @return number of records removed
     */
    default int garbageCollect() { return 0; }

    /**
     * Get a non-expired record, or null if expired/not found.
     *
     * @param key the record key
     * @return the record, or null
     */
    default Record getAlive(byte[] key) {
        Record r = get(key);
        if (r != null && r.isExpired()) return null;
        return r;
    }

    /**
     * Get all non-expired records for a key.
     *
     * @param key the record key
     * @return list of non-expired records
     */
    default List<Record> getAllAlive(byte[] key) {
        return getAll(key).stream().filter(r -> !r.isExpired()).toList();
    }
}
