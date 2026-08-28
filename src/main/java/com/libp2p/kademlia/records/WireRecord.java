package com.libp2p.kademlia.records;

import java.util.Arrays;

public class WireRecord {

    private final byte[] key;
    private final byte[] value;
    private final byte[] author;
    private final byte[] signature;
    private final long seq;

    public WireRecord(byte[] key, byte[] value) {
        this(key, value, null, null, 0);
    }

    public WireRecord(byte[] key, byte[] value, byte[] author, byte[] signature, long seq) {
        this.key = key.clone();
        this.value = value.clone();
        this.author = author != null ? author.clone() : null;
        this.signature = signature != null ? signature.clone() : null;
        this.seq = seq;
    }

    public byte[] getKey() { return key.clone(); }
    public byte[] getValue() { return value.clone(); }
    public byte[] getAuthor() { return author != null ? author.clone() : null; }
    public byte[] getSignature() { return signature != null ? signature.clone() : null; }
    public long getSeq() { return seq; }

    public Record toRecord() {
        return new Record(key, value, author, null);
    }

    public static WireRecord fromRecord(Record record, byte[] author, long seq) {
        return new WireRecord(record.getKey(), record.getValue(), author, null, seq);
    }

    public static WireRecord fromRecord(Record record) {
        return new WireRecord(record.getKey(), record.getValue(), null, null, 0);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WireRecord that = (WireRecord) o;
        return seq == that.seq
                && Arrays.equals(key, that.key)
                && Arrays.equals(value, that.value)
                && Arrays.equals(author, that.author)
                && Arrays.equals(signature, that.signature);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(value);
        result = 31 * result + Arrays.hashCode(author);
        result = 31 * result + Long.hashCode(seq);
        return result;
    }
}
