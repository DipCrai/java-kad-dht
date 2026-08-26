package com.libp2p.kademlia.records;

import java.time.Instant;
import java.util.Arrays;

public class Record {

    private final byte[] key;
    private final byte[] value;
    private final byte[] publisher;
    private final Instant expires;
    private Instant timeReceived;

    public Record(byte[] key, byte[] value) {
        this(key, value, null, null);
    }

    public Record(byte[] key, byte[] value, byte[] publisher, Instant expires) {
        this.key = key.clone();
        this.value = value.clone();
        this.publisher = publisher.clone();
        this.expires = expires;
        this.timeReceived = Instant.now();
    }

    public byte[] getKey() { return key.clone(); }
    public byte[] getValue() { return value.clone(); }
    public byte[] getPublisher() { return publisher.clone(); }
    public Instant getExpires() { return expires; }
    public Instant getTimeReceived() { return timeReceived; }

    public void setTimeReceived(Instant timeReceived) {
        this.timeReceived = timeReceived;
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
