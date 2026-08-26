package com.libp2p.kademlia.routing;

import io.libp2p.core.PeerId;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

public class KBucket {
    private final int k;
    private final Duration pendingTimeout;
    private final List<KBucketEntry> entries = new CopyOnWriteArrayList<>();
    private final List<KBucketEntry> replacementCache = new ArrayList<>();

    public KBucket(int k, Duration pendingTimeout) {
        this.k = k;
        this.pendingTimeout = pendingTimeout;
    }

    public synchronized InsertResult insert(KBucketEntry entry) {
        for (int i = 0; i < entries.size(); i++) {
            if (entries.get(i).peerId.equals(entry.peerId)) {
                KBucketEntry existing = entries.remove(i);
                existing.markSeen(Instant.now());
                entries.add(0, existing);
                return InsertResult.ALREADY_PRESENT;
            }
        }

        if (entries.size() < k) {
            entries.add(0, entry);
            return InsertResult.INSERTED;
        }

        KBucketEntry oldest = entries.get(entries.size() - 1);
        if (Duration.between(oldest.getLastSeen(), Instant.now()).compareTo(pendingTimeout) > 0) {
            entries.remove(entries.size() - 1);
            if (!replacementCache.isEmpty()) {
                entries.add(0, replacementCache.remove(0));
            } else {
                entries.add(0, entry);
            }
            return InsertResult.EVICTED;
        }

        replacementCache.remove(entry);
        if (replacementCache.size() >= k) {
            replacementCache.remove(replacementCache.size() - 1);
        }
        replacementCache.add(0, entry);
        return InsertResult.PING;
    }

    public synchronized Optional<KBucketEntry> remove(PeerId peerId) {
        for (int i = 0; i < entries.size(); i++) {
            if (entries.get(i).peerId.equals(peerId)) {
                KBucketEntry removed = entries.remove(i);
                if (!replacementCache.isEmpty()) {
                    entries.add(replacementCache.remove(0));
                }
                return Optional.of(removed);
            }
        }
        return Optional.empty();
    }

    public synchronized void markSeen(PeerId peerId, Instant now) {
        for (int i = 0; i < entries.size(); i++) {
            if (entries.get(i).peerId.equals(peerId)) {
                KBucketEntry entry = entries.remove(i);
                entry.markSeen(now);
                entries.add(0, entry);
                return;
            }
        }
    }

    public synchronized void markSuccessfulOutbound(PeerId peerId, Instant now) {
        for (KBucketEntry entry : entries) {
            if (entry.peerId.equals(peerId)) {
                entry.markSuccessfulOutbound(now);
                return;
            }
        }
    }

    public List<KBucketEntry> getEntries() { return List.copyOf(entries); }
    public int size() { return entries.size(); }
    public boolean isFull() { return entries.size() >= k; }

    public Optional<KBucketEntry> getOldest() {
        return entries.isEmpty() ? Optional.empty() : Optional.of(entries.get(entries.size() - 1));
    }

    public enum InsertResult {
        INSERTED, ALREADY_PRESENT, FULL, EVICTED, PING
    }
}
