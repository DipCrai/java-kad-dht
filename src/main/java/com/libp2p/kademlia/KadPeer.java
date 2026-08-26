package com.libp2p.kademlia;

import io.libp2p.core.PeerId;
import io.libp2p.core.multiformats.Multiaddr;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class KadPeer {
    public final PeerId nodeId;
    public final List<Multiaddr> multiaddrs;
    public final ConnectionType connectionType;

    public enum ConnectionType {
        NOT_CONNECTED(0),
        CONNECTED(1),
        CAN_CONNECT(2),
        CANNOT_CONNECT(3);

        public final int value;
        ConnectionType(int value) { this.value = value; }

        public static ConnectionType fromValue(int value) {
            for (ConnectionType ct : values()) {
                if (ct.value == value) return ct;
            }
            return NOT_CONNECTED;
        }
    }

    public KadPeer(PeerId nodeId, List<Multiaddr> multiaddrs, ConnectionType connectionType) {
        this.nodeId = Objects.requireNonNull(nodeId);
        this.multiaddrs = multiaddrs == null ? List.of() : Collections.unmodifiableList(new ArrayList<>(multiaddrs));
        this.connectionType = connectionType != null ? connectionType : ConnectionType.NOT_CONNECTED;
    }

    public byte[] toBytes() {
        return nodeId.getBytes();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof KadPeer other)) return false;
        return nodeId.equals(other.nodeId);
    }

    @Override
    public int hashCode() {
        return nodeId.hashCode();
    }

    @Override
    public String toString() {
        return "KadPeer{id=" + nodeId + ", addrs=" + multiaddrs.size() + ", conn=" + connectionType + "}";
    }
}
