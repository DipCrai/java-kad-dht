package com.libp2p.kademlia.records;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class MemoryRecordStore implements RecordStore {
    private final int maxRecords;
    private final int maxRecordValueSize;
    private final Duration maxRecordAge;
    private final RecordValidator validator;
    private final Map<ByteKey, Record> records = new ConcurrentHashMap<>();
    private static final int STRIPE_COUNT = 256;
    private final ReentrantLock[] stripes = new ReentrantLock[STRIPE_COUNT];

    public MemoryRecordStore(int maxRecords, int maxRecordValueSize, Duration maxRecordAge, RecordValidator validator) {
        this.maxRecords = maxRecords;
        this.maxRecordValueSize = maxRecordValueSize;
        this.maxRecordAge = maxRecordAge;
        this.validator = validator;
        for (int i = 0; i < STRIPE_COUNT; i++) stripes[i] = new ReentrantLock();
    }

    private ReentrantLock stripeFor(byte[] key) {
        return stripes[key.length > 0 ? (key[key.length - 1] & 0xFF) : 0];
    }

    @Override
    public Record get(byte[] key) {
        Record record = records.get(new ByteKey(key));
        if (record == null) return null;
        if (record.isExpired()) { records.remove(new ByteKey(key)); return null; }
        if (maxRecordAge != null && record.getTimeReceived() != null) {
            if (Instant.now().isAfter(record.getTimeReceived().plus(maxRecordAge))) {
                records.remove(new ByteKey(key));
                return null;
            }
        }
        return record;
    }

    @Override
    public List<Record> getAll(byte[] key) {
        Record r = get(key);
        return r != null ? List.of(r) : List.of();
    }

    @Override
    public boolean put(Record record) {
        if (record.getKey() == null || record.getKey().length == 0) return false;
        if (record.getValue() == null || record.getValue().length == 0) return false;
        if (record.getValue().length > maxRecordValueSize) return false;
        if (!validator.validate(record.getKey(), record.getValue())) return false;

        ReentrantLock lock = stripeFor(record.getKey());
        lock.lock();
        try {
            ByteKey bk = new ByteKey(record.getKey());
            Record existing = records.get(bk);
            if (existing != null && !existing.isExpired()) {
                int best = validator.select(record.getKey(), new byte[][]{record.getValue(), existing.getValue()});
                if (best != 0) return false;
            }
            if (records.size() >= maxRecords && existing == null) evictOldest();
            Record toStore = record.copy();
            toStore.setTimeReceived(Instant.now());
            records.put(bk, toStore);
            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void remove(byte[] key) { records.remove(new ByteKey(key)); }

    @Override
    public Iterable<Record> records() {
        List<Record> alive = new ArrayList<>();
        Iterator<Map.Entry<ByteKey, Record>> it = records.entrySet().iterator();
        while (it.hasNext()) {
            Record r = it.next().getValue();
            if (r.isExpired()) it.remove(); else alive.add(r);
        }
        return alive;
    }

    @Override
    public int size() { return records.size(); }

    public int garbageCollect() {
        int removed = 0;
        Iterator<Map.Entry<ByteKey, Record>> it = records.entrySet().iterator();
        while (it.hasNext()) {
            Record r = it.next().getValue();
            if (r.isExpired()) { it.remove(); removed++; }
            else if (maxRecordAge != null && r.getTimeReceived() != null && Instant.now().isAfter(r.getTimeReceived().plus(maxRecordAge))) {
                it.remove(); removed++;
            }
        }
        return removed;
    }

    private void evictOldest() {
        if (records.size() <= 1) {
            if (records.size() == 1) records.clear();
            return;
        }
        Record oldest = null;
        ByteKey oldestKey = null;
        for (Map.Entry<ByteKey, Record> e : records.entrySet()) {
            Record r = e.getValue();
            if (oldest == null || (r.getTimeReceived() != null && oldest.getTimeReceived() != null && r.getTimeReceived().isBefore(oldest.getTimeReceived()))) {
                oldest = r; oldestKey = e.getKey();
            }
        }
        if (oldestKey != null) records.remove(oldestKey);
    }

    static final class ByteKey {
        private final byte[] bytes;
        private final int hash;
        ByteKey(byte[] bytes) { this.bytes = bytes; this.hash = Arrays.hashCode(bytes); }
        @Override public boolean equals(Object o) { return o instanceof ByteKey other && Arrays.equals(bytes, other.bytes); }
        @Override public int hashCode() { return hash; }
    }
}
