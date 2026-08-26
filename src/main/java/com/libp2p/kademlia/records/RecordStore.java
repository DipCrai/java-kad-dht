package com.libp2p.kademlia.records;

import java.util.List;

public interface RecordStore {
    Record get(byte[] key);
    List<Record> getAll(byte[] key);
    boolean put(Record record);
    void remove(byte[] key);
    Iterable<Record> records();
    int size();
    default int garbageCollect() { return 0; }
}
