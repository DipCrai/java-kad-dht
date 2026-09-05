package com.libp2p.kademlia.bootstrap;

import io.libp2p.core.multiformats.Multiaddr;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Default bootstrap peers used when no explicit bootstrap nodes are configured.
 * <p>
 * Mirrors {@code go-libp2p-kad-dht's DefaultBootstrapPeers}: the same entry points every
 * libp2p client starts from. Official nodes are addressed via {@code dnsaddr}, which is
 * re-resolved on every bootstrap round.
 */
public final class DefaultBootstrapPeers {

    public static final List<Multiaddr> DEFAULT_BOOTSTRAP_PEERS;

    static {
        List<Multiaddr> peers = new ArrayList<>();
        for (String s : new String[]{
                "/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
                "/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa",
                "/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
                "/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt",
                "/ip4/104.131.131.82/tcp/4001/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ"
        }) {
            peers.add(Multiaddr.fromString(s));
        }
        DEFAULT_BOOTSTRAP_PEERS = Collections.unmodifiableList(peers);
    }

    private DefaultBootstrapPeers() {}
}