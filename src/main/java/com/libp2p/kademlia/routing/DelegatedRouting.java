package com.libp2p.kademlia.routing;

import com.libp2p.kademlia.records.ProviderRecord;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Optional routing path for provider operations that is not the native kad
 * crawl, for example an HTTP Delegated Routing V1 server resolving the content
 * key to the same dialable peers every caller coordinates on. A
 * {@link ProviderRoutingCoordinator} runs such a path alongside the native kad
 * path and combines the results, so the core DHT stays a plain kad
 * implementation and delegated routing stays an optional accelerator/fallback.
 */
public interface DelegatedRouting {

    /**
     * Announces that this node provides the given key through the delegated
     * path, targeting the peers responsible for that exact key.
     *
     * @param key the content key
     * @return future completing with {@code true} as soon as at least one target
     *         confirmed the announce, {@code false} if none did
     */
    CompletableFuture<Boolean> announce(byte[] key);

    /**
     * Asks the delegated path for provider records of the given key.
     *
     * @param key the content key
     * @return future completing with the provider records known to the path
     *         (possibly empty)
     */
    CompletableFuture<List<ProviderRecord>> findProviders(byte[] key);
}