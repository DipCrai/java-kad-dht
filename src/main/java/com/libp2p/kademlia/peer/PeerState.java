package com.libp2p.kademlia.peer;

public enum PeerState {
    UNKNOWN,
    ACTIVE,
    FAILED,
    DEAD,
    SERVER,
    CLIENT;

    public boolean isUsable() {
        return this == ACTIVE || this == SERVER || this == CLIENT;
    }
}
