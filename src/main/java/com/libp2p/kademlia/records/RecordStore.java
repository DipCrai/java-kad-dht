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

    default Record getAlive(byte[] key) {
        Record r = get(key);
        if (r != null && r.isExpired()) return null;
        return r;
    }

    default List<Record> getAllAlive(byte[] key) {
        return getAll(key).stream().filter(r -> !r.isExpired()).toList();
    }
}
