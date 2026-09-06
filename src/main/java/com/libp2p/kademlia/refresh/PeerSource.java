package com.libp2p.kademlia.refresh;

import io.libp2p.core.PeerId;
import io.libp2p.core.multiformats.Multiaddr;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Source of live kad peer addresses used as a bootstrap fallback and for the
 * delegated routing {@code closest/peers} resolution. The default
 * implementation pulls from the public libp2p <em>Delegated Routing V1</em>
 * HTTP servers ({@link HttpBootstrapPeerSource}); callers can supply their own
 * so the core DHT never has to know the concrete HTTP implementation.
 */
public interface PeerSource {

    /**
     * Fetches candidate kad peer addresses for general bootstrap purposes.
     *
     * @param self     the local peer id (used to build lookup keys), or null
     * @param keyCount number of lookup keys to query per router
     * @return future with the candidate multiaddrs (never null), each carrying
     *         its peer id suffix
     */
    CompletableFuture<List<Multiaddr>> fetch(PeerId self, int keyCount);

    /**
     * Deterministically resolves the dialable peers responsible for a specific
     * DHT content key (the same key yields the same peers for every caller).
     *
     * @param dhtKey the content key
     * @return future with the responsible peers' multiaddrs (never null)
     */
    CompletableFuture<List<Multiaddr>> fetchForDhtKey(byte[] dhtKey);

    /** @return the number of lookup keys queried per router */
    int getLookupKeys();
}