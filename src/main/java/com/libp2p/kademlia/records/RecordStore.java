package com.libp2p.kademlia.records;

public interface RecordStore {
    Record get(byte[] key);
    boolean put(Record record);
    void remove(byte[] key);
    Iterable<Record> records();
    int size();
}
