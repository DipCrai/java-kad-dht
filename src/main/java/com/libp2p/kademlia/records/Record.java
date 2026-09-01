package com.libp2p.kademlia.records;

import java.time.Instant;
import java.util.Arrays;

/**
 * A DHT record containing a key-value pair with metadata.
 *
 * <p>Records are immutable after construction (except {@link #setTimeReceived}).
 * All byte arrays are defensively copied on get/set to prevent external mutation.</p>
 *
 * <h3>Equality</h3>
 * <p>Two records are equal if they have the same key and value (ignoring publisher,
 * expiry, and timeReceived). This matches Go/Rust behavior for dedup.</p>
 *
 * <h3>Expiry</h3>
 * <p>If {@code expires} is non-null, the record is considered expired after that
 * instant. Expired records are garbage-collected by {@link MemoryRecordStore}.</p>
 *
 * @see RecordStore
 * @see MemoryRecordStore
 */
public class Record {

    private final byte[] key;
    private final byte[] value;
    private final byte[] publisher;
    private final Instant expires;
    private Instant timeReceived;

    /**
     * Create a record with no publisher and no expiry.
     *
     * @param key   the record key
     * @param value the record value
     */
    public Record(byte[] key, byte[] value) {
        this(key, value, null, null);
    }

    /**
     * Create a record with publisher and expiry.
     *
     * @param key       the record key
     * @param value     the record value
     * @param publisher the publisher's peer ID bytes, or null
     * @param expires   expiry instant, or null for no expiry
     */
    public Record(byte[] key, byte[] value, byte[] publisher, Instant expires) {
        this.key = key.clone();
        this.value = value.clone();
        this.publisher = publisher != null ? publisher.clone() : null;
        this.expires = expires;
        this.timeReceived = Instant.now();
    }

    /** @return the record key (defensive copy) */
    public byte[] getKey() { return key.clone(); }

    /** @return the record value (defensive copy) */
    public byte[] getValue() { return value.clone(); }

    /** @return the publisher's peer ID bytes, or null (defensive copy) */
    public byte[] getPublisher() { return publisher != null ? publisher.clone() : null; }

    /** @return the expiry instant, or null for no expiry */
    public Instant getExpires() { return expires; }

    /** @return the time this record was received/stored locally */
    public Instant getTimeReceived() { return timeReceived; }

    /**
     * Set the time this record was received. Used for stale detection.
     *
     * @param timeReceived the receive time
     */
    public void setTimeReceived(Instant timeReceived) {
        this.timeReceived = timeReceived;
    }

    public static Record fromWire(byte[] key, byte[] value) {
        Record r = new Record(key, value, null, null);
        r.timeReceived = null;
        return r;
    }

    public Record copy() {
        Record r = new Record(key, value, publisher, expires);
        r.timeReceived = this.timeReceived;
        return r;
    }

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
        Record record = (Record) o;
        return Arrays.equals(key, record.key) && Arrays.equals(value, record.value);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(value);
        return result;
    }

    @Override
    public String toString() {
        return "Record{key=" + Arrays.toString(key) +
                ", value.length=" + value.length +
                ", expires=" + expires +
                '}';
    }
}
