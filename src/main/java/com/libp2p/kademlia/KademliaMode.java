package com.libp2p.kademlia;

/**
 * DHT operation mode.
 * Port of go mode.go + rust Mode.
 *
 * CLIENT: Can only query, not respond to inbound queries.
 *         Does NOT register stream handlers, resets inbound streams.
 * SERVER: Can both query and respond. Registers stream handlers.
 * AUTO:   Starts as CLIENT. Switches to SERVER when external address confirmed.
 * AUTO_SERVER: Like AUTO but starts as SERVER.
 */
public enum KademliaMode {
    CLIENT,
    SERVER,
    AUTO,
    AUTO_SERVER;

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
