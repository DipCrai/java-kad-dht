package com.libp2p.kademlia.query;

import com.libp2p.kademlia.integration.IdentifyAdapter;
import com.libp2p.kademlia.peer.PeerInfo;
import com.libp2p.kademlia.peer.PeerState;
import io.libp2p.core.PeerId;

public class DefaultQueryFilter implements QueryFilter {
    private final IdentifyAdapter identifyAdapter;
    private static final int MAX_RECENT_FAILURES = 5;

    public DefaultQueryFilter(IdentifyAdapter identifyAdapter) {
        this.identifyAdapter = identifyAdapter;
    }

    @Override
    public boolean allowQuery(PeerId peer, PeerInfo info) {
        if (identifyAdapter != null) {
            Boolean kadSupport = identifyAdapter.getKadServerSupport(peer);
            if (kadSupport != null && !kadSupport) return false;
        }
        if (info != null && info.getFailureCount() >= MAX_RECENT_FAILURES) return false;
        if (info != null && info.getState() == PeerState.FAILED) return false;
        return true;
    }

    @Override
    public boolean allowResponse(PeerId peer, Object response) {
        return true;
    }
}
