package com.libp2p.kademlia.config;

public enum KadMode {
    CLIENT, SERVER, AUTO, AUTO_SERVER;

    public boolean isServer() {
        return this == SERVER || this == AUTO_SERVER;
    }

    public boolean isClient() {
        return this == CLIENT;
    }

    public boolean isAuto() {
        return this == AUTO || this == AUTO_SERVER;
    }
}
