package com.libp2p.kademlia.query;

import com.libp2p.kademlia.peer.PeerInfo;
import io.libp2p.core.PeerId;

public interface QueryFilter {
    boolean allowQuery(PeerId peer, PeerInfo info);
    boolean allowResponse(PeerId peer, Object response);
}
