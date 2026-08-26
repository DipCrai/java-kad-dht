package com.libp2p.kademlia.records;

import io.libp2p.core.PeerId;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class MemoryProviderStore implements ProviderStore {
    private final int maxProvidedKeys;
    private final int maxProvidersPerKey;
    private final Map<ByteKey, CopyOnWriteArrayList<ProviderRecord>> providers = new ConcurrentHashMap<>();
    private final Set<ProviderRecord> provided = ConcurrentHashMap.newKeySet();

    public MemoryProviderStore(int maxProvidedKeys, int maxProvidersPerKey) {
        this.maxProvidedKeys = maxProvidedKeys;
        this.maxProvidersPerKey = maxProvidersPerKey;
    }

    public MemoryProviderStore() { this(1024, 20); }

    @Override
    public boolean addProvider(ProviderRecord record) {
        if (record.getKey() == null || record.getKey().length == 0 || record.getProvider() == null) return false;
        ByteKey bk = new ByteKey(record.getKey());
        boolean[] added = {false};
        providers.compute(bk, (key, list) -> {
            if (list == null) list = new CopyOnWriteArrayList<>();
            for (int i = 0; i < list.size(); i++) {
                if (list.get(i).getProvider().equals(record.getProvider())) {
                    list.set(i, record);
                    added[0] = true;
                    return list;
                }
            }
            if (list.size() >= maxProvidersPerKey) return list;
            list.add(record);
            added[0] = true;
            return list;
        });
        if (added[0]) provided.add(record);
        return added[0];
    }

    @Override
    public List<ProviderRecord> getProviders(byte[] key) {
        CopyOnWriteArrayList<ProviderRecord> list = providers.get(new ByteKey(key));
        if (list == null) return List.of();
        Instant now = Instant.now();
        List<ProviderRecord> alive = new ArrayList<>();
        for (ProviderRecord r : list) {
            if (!r.isExpired(now)) alive.add(r);
        }
        Collections.shuffle(alive);
        return alive;
    }

    @Override
    public Iterable<ProviderRecord> provided() {
        List<ProviderRecord> alive = new ArrayList<>();
        Instant now = Instant.now();
        for (ProviderRecord r : provided) {
            if (!r.isExpired(now)) alive.add(r); else provided.remove(r);
        }
        return alive;
    }

    @Override
    public void removeProvider(byte[] key, PeerId provider) {
        CopyOnWriteArrayList<ProviderRecord> list = providers.get(new ByteKey(key));
        if (list != null) {
            list.removeIf(r -> r.getProvider().equals(provider));
            if (list.isEmpty()) providers.remove(new ByteKey(key));
        }
    }

    @Override
    public int keyCount() { return providers.size(); }

    public int garbageCollect() {
        int[] removed = {0};
        Instant now = Instant.now();
        for (Map.Entry<ByteKey, CopyOnWriteArrayList<ProviderRecord>> e : providers.entrySet()) {
            e.getValue().removeIf(r -> { if (r.isExpired(now)) { removed[0]++; return true; } return false; });
            if (e.getValue().isEmpty()) providers.remove(e.getKey());
        }
        provided.removeIf(r -> r.isExpired(now));
        return removed[0];
    }

    static final class ByteKey {
        private final byte[] bytes;
        private final int hash;
        ByteKey(byte[] bytes) { this.bytes = bytes; this.hash = Arrays.hashCode(bytes); }
        @Override public boolean equals(Object o) { return o instanceof ByteKey other && Arrays.equals(bytes, other.bytes); }
        @Override public int hashCode() { return hash; }
    }
}
