package com.libp2p.kademlia.records;

public interface RecordValidator {
    boolean validate(byte[] key, byte[] value);
    int select(byte[] key, byte[][] values);

    RecordValidator NOOP = new RecordValidator() {
        @Override public boolean validate(byte[] key, byte[] value) { return value != null && value.length > 0; }
        @Override public int select(byte[] key, byte[][] values) { return (values != null && values.length > 0) ? 0 : -1; }
    };

    RecordValidator PUBLIC_KEY = NOOP;
}
