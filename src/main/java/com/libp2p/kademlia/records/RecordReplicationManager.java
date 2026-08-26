package com.libp2p.kademlia.records;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.function.BiFunction;

public class RecordReplicationManager {
    private final RecordStore recordStore;
    private final BiFunction<byte[], byte[], CompletableFuture<Boolean>> putValueFn;
    private final Duration replicationInterval;
    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> task;
    private volatile boolean running;

    public RecordReplicationManager(RecordStore recordStore,
                                     BiFunction<byte[], byte[], CompletableFuture<Boolean>> putValueFn,
                                     Duration replicationInterval) {
        this.recordStore = recordStore;
        this.putValueFn = putValueFn;
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
            putValueFn.apply(r.getKey(), r.getValue());
        }
    }
}
