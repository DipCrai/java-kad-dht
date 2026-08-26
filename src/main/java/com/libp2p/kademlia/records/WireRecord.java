package com.libp2p.kademlia.records;

import java.util.Arrays;

public class WireRecord {

    private final byte[] key;
    private final byte[] value;

    public WireRecord(byte[] key, byte[] value) {
        this.key = key.clone();
        this.value = value.clone();
    }

    public byte[] getKey() { return key.clone(); }
    public byte[] getValue() { return value.clone(); }

    public Record toRecord() {
        return new Record(key, value);
    }

    public static WireRecord fromRecord(Record record) {
        return new WireRecord(record.getKey(), record.getValue());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WireRecord that = (WireRecord) o;
        return Arrays.equals(key, that.key) && Arrays.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(value);
        return result;
    }
}
