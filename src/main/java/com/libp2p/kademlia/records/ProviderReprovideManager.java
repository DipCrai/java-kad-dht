package com.libp2p.kademlia.records;

import io.libp2p.core.Host;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.function.Function;

public class ProviderReprovideManager {
    private final ProviderStore providerStore;
    private final Function<byte[], CompletableFuture<Boolean>> provideFn;
    private final Duration reprovideInterval;
    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> task;
    private volatile boolean running;

    public ProviderReprovideManager(ProviderStore providerStore,
                                     Function<byte[], CompletableFuture<Boolean>> provideFn,
                                     Host host,
                                     Duration reprovideInterval) {
        this.providerStore = providerStore;
        this.provideFn = provideFn;
        this.reprovideInterval = reprovideInterval;
    }

    public void start() {
        if (running) return;
        running = true;
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "provider-reprovide");
            t.setDaemon(true);
            return t;
        });
        task = scheduler.scheduleWithFixedDelay(this::reprovide,
                reprovideInterval.toHours(), reprovideInterval.toHours(), TimeUnit.HOURS);
    }

    public void stop() {
        running = false;
        if (task != null) task.cancel(false);
        if (scheduler != null) scheduler.shutdownNow();
    }

    private void reprovide() {
        if (!running) return;
        for (ProviderRecord pr : providerStore.provided()) {
            provideFn.apply(pr.getKey());
        }
    }
}
