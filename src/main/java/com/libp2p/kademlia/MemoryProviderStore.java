package com.libp2p.kademlia;

import io.libp2p.core.PeerId;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory provider store.
 * Port of rust MemoryStore providers + go ProviderManager.
 *
 * Limits:
 * - maxProvidedKeys: 1024
 * - maxProvidersPerKey: K_VALUE (20)
 * - providerRecordTTL: 48h
 *
 * Sybil mitigation (rust): If providers list ≥ maxProvidersPerKey, new provider silently dropped.
 * Equality: ProviderRecord keyed by (key, provider) — addresses/expire don't matter.
 */
public class MemoryProviderStore implements ProviderStore {
    private final int maxProvidedKeys;
    private final int maxProvidersPerKey;
    private final Duration providerRecordTTL;

    private final Map<ByteKey, List<ProviderRecord>> providers = new ConcurrentHashMap<>();
    private final Set<ProviderRecord> provided = ConcurrentHashMap.newKeySet();

    public MemoryProviderStore(int maxProvidedKeys, int maxProvidersPerKey, Duration providerRecordTTL) {
        this.maxProvidedKeys = maxProvidedKeys;
        this.maxProvidersPerKey = maxProvidersPerKey;
        this.providerRecordTTL = providerRecordTTL;
    }

    public MemoryProviderStore() {
        this(1024, 20, Duration.ofHours(48));
    }

    @Override
    public boolean addProvider(ProviderRecord record) {
        if (record.getKey() == null || record.getKey().length == 0) return false;
        if (record.getProvider() == null) return false;

        ByteKey bk = new ByteKey(record.getKey());

        providers.compute(bk, (key, list) -> {
            if (list == null) list = new ArrayList<>();

            // Update existing
            for (int i = 0; i < list.size(); i++) {
                if (list.get(i).getProvider().equals(record.getProvider())) {
                    list.set(i, record);
                    return list;
                }
            }

            // Sybil mitigation: silently drop if at capacity
            if (list.size() >= maxProvidersPerKey) {
                return list;
            }

            list.add(record);
            return list;
        });

        provided.add(record);
        return true;
    }

    @Override
    public List<ProviderRecord> getProviders(byte[] key) {
        List<ProviderRecord> list = providers.get(new ByteKey(key));
        if (list == null) return List.of();

        Instant now = Instant.now();
        List<ProviderRecord> alive = new ArrayList<>();
        Iterator<ProviderRecord> it = list.iterator();
        while (it.hasNext()) {
            ProviderRecord r = it.next();
            if (r.isExpired(now)) {
                it.remove();
            } else {
                alive.add(r);
            }
        }

        // Shuffle for load spreading (go: shuffleProviders)
        Collections.shuffle(alive);
        return alive;
    }

    @Override
    public Iterable<ProviderRecord> provided() {
        List<ProviderRecord> alive = new ArrayList<>();
        Instant now = Instant.now();
        for (ProviderRecord r : provided) {
            if (!r.isExpired(now)) {
                alive.add(r);
            } else {
                provided.remove(r);
            }
        }
        return alive;
    }

    @Override
    public void removeProvider(byte[] key, PeerId provider) {
        ByteKey bk = new ByteKey(key);
        List<ProviderRecord> list = providers.get(bk);
        if (list != null) {
            list.removeIf(r -> r.getProvider().equals(provider));
            if (list.isEmpty()) {
                providers.remove(bk);
            }
        }
    }

    @Override
    public int keyCount() {
        return providers.size();
    }

    public int garbageCollect() {
        int removed = 0;
        Instant now = Instant.now();
        for (Map.Entry<ByteKey, List<ProviderRecord>> e : providers.entrySet()) {
            List<ProviderRecord> list = e.getValue();
            Iterator<ProviderRecord> it = list.iterator();
            while (it.hasNext()) {
                if (it.next().isExpired(now)) {
                    it.remove();
                    removed++;
                }
            }
            if (list.isEmpty()) {
                providers.remove(e.getKey());
            }
        }
        Iterator<ProviderRecord> it = provided.iterator();
        while (it.hasNext()) {
            if (it.next().isExpired(now)) {
                it.remove();
            }
        }
        return removed;
    }

    static final class ByteKey {
        private final byte[] bytes;
        private final int hash;

        ByteKey(byte[] bytes) {
            this.bytes = bytes;
            this.hash = Arrays.hashCode(bytes);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof ByteKey other)) return false;
            return Arrays.equals(bytes, other.bytes);
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }
}
