package com.libp2p.kademlia.routing;

import io.libp2p.core.Host;
import io.libp2p.core.PeerId;

public interface PeerDiversityPolicy {
    boolean accept(PeerId peer, int bucketIndex);
    default void remove(PeerId peer, int bucketIndex) {}
    default void setHost(Host host) {}
}
