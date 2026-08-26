package com.libp2p.kademlia.records;

import io.libp2p.core.PeerId;
import io.libp2p.core.multiformats.Multiaddr;
import io.libp2p.core.multistream.ProtocolBinding;
import java.util.Map;

public interface ProviderStore {
    boolean addProvider(ProviderRecord record);
    java.util.List<ProviderRecord> getProviders(byte[] key);
    Iterable<ProviderRecord> provided();
    void removeProvider(byte[] key, PeerId provider);
    int keyCount();
}
