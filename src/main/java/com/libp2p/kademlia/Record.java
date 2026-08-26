package com.libp2p.kademlia;

import java.time.Instant;

/**
 * A DHT record (key-value pair).
 * Port of rust-libp2p record::Record and go-libp2p record.Record.
 */
public class Record {
    private final byte[] key;
    private final byte[] value;
    private final byte[] publisher;
    private final Instant expires;
    private Instant timeReceived;

    public Record(byte[] key, byte[] value, byte[] publisher, Instant expires) {
        this.key = key;
        this.value = value;
        this.publisher = publisher;
        this.expires = expires;
    }

    public Record(byte[] key, byte[] value) {
        this(key, value, null, null);
    }

    public byte[] getKey() { return key; }
    public byte[] getValue() { return value; }
    public byte[] getPublisher() { return publisher; }
    public Instant getExpires() { return expires; }
    public Instant getTimeReceived() { return timeReceived; }

    public void setTimeReceived(Instant timeReceived) {
        this.timeReceived = timeReceived;
    }

    public boolean isExpired() {
        return expires != null && Instant.now().isAfter(expires);
    }

    public boolean isExpired(Instant now) {
        return expires != null && now.isAfter(expires);
    }

    public Record copy() {
        Record r = new Record(key.clone(), value.clone(), publisher != null ? publisher.clone() : null, expires);
        r.timeReceived = this.timeReceived;
        return r;
    }

    public String toString() {
        return "Record{key=" + XorId.toHex(key) + ", valueLen=" + (value != null ? value.length : 0) +
                ", publisher=" + (publisher != null ? XorId.toHex(publisher) : "null") +
                ", expires=" + expires + "}";
    }
}
