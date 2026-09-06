package com.libp2p.kademlia.refresh;

import com.libp2p.kademlia.protocol.KademliaProtocol;
import com.libp2p.kademlia.routing.DelegatedRouting;
import com.libp2p.kademlia.routing.RoutingTable;

import java.time.Duration;

/**
 * Builds the concrete {@link DelegatedRouting} implementation so
 * {@code KadDht} (and the {@code ProviderRoutingCoordinator}) only ever depend
 * on the {@link DelegatedRouting} abstraction, never on the HTTP wiring. The
 * default factory constructs {@link HttpDelegatedRouting}; a custom factory can
 * substitute any other delegated backend.
 */
public interface DelegatedRoutingFactory {

    /**
     * @param peerSource       the source of responsible-peer addresses
     * @param protocol         the kad protocol for ADD_PROVIDER / GET_PROVIDERS
     * @param routingTable     peers discovered over the delegated path are
     *                         admitted here through the single admission point
     * @param bootstrapAddressTTL address-book TTL for the dialed responsables
     * @param host             the attached libp2p host (never null at build time)
     * @return the delegated routing implementation, or {@code null} if the
     *         configuration does not enable a delegated path
     */
    DelegatedRouting create(PeerSource peerSource, KademliaProtocol protocol, RoutingTable routingTable,
                            Duration bootstrapAddressTTL, io.libp2p.core.Host host);
}