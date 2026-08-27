package com.libp2p.kademlia.records;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.function.Function;

public class RecordReplicationManager {
    private final RecordStore recordStore;
    private final Function<Record, CompletableFuture<Boolean>> replicateFn;
    private final Duration replicationInterval;
    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> task;
    private volatile boolean running;

    public RecordReplicationManager(RecordStore recordStore,
                                     Function<Record, CompletableFuture<Boolean>> replicateFn,
                                     Duration replicationInterval) {
        this.recordStore = recordStore;
        this.replicateFn = replicateFn;
        this.replicationInterval = replicationInterval;
    }

    public void start() {
        if (running) return;
        running = true;
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "record-replication");
            t.setDaemon(true);
            return t;
        });
        task = scheduler.scheduleWithFixedDelay(this::replicate,
                replicationInterval.toMillis(), replicationInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    public void stop() {
        running = false;
        if (task != null) task.cancel(false);
        if (scheduler != null) scheduler.shutdownNow();
    }

    private void replicate() {
        if (!running) return;
        for (Record r : recordStore.records()) {
            replicateFn.apply(r);
        }
    }
}
