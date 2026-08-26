package com.libp2p.kademlia.routing;

import io.libp2p.core.PeerId;

import java.util.concurrent.CompletableFuture;

@FunctionalInterface
public interface AdmissionCheck {
    CompletableFuture<Boolean> checkAdmission(PeerId peer);

    AdmissionCheck ALLOW_ALL = peer -> CompletableFuture.completedFuture(true);
}
