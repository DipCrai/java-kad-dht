package com.libp2p.kademlia;

import io.libp2p.core.PeerId;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Primary iterative lookup state machine.
 * Port of rust ClosestPeersIter + go query.run().
 *
 * States:
 * - Iterating: actively querying peers, tracking no_progress count
 * - Stalled: no progress for α consecutive responses, capacity increases
 * - Finished: k closest peers have all been queried
 */
public class ClosestPeersIter {

    public enum State {
        ITERATING,
        STALLED,
        FINISHED
    }

    private State state = State.ITERATING;
    private int noProgressCount;
    private int resultCounter;
    private final int numResults;    // k
    private final int parallelism;   // α
    private final int beta;          // β
    private final Duration peerTimeout;
    private final QueryPeerset peers;

    public ClosestPeersIter(byte[] target, List<KadPeer> seedPeers, int k, int alpha, int beta, Duration peerTimeout) {
        this.numResults = k;
        this.parallelism = alpha;
        this.beta = beta;
        this.peerTimeout = peerTimeout;
        this.peers = new QueryPeerset(target);

        for (KadPeer p : seedPeers) {
            peers.addHeard(p);
        }
    }

    /**
     * Get the next peer to query, or null if at capacity / finished.
     */
    public PeerId next() {
        int capacity = state == State.STALLED
                ? Math.max(parallelism, numResults)
                : parallelism;

        // Count currently waiting
        int waiting = 0;
        for (QueryPeerset.PeerInfo p : peers.getAll()) {
            if (p.state == PeerState.WAITING) {
                waiting++;
                // Check timeout
                if (p.waitSince != null && Instant.now().isAfter(p.waitSince.plus(peerTimeout))) {
                    peers.markUnresponsive(p.peerId);
                    waiting--;
                }
            }
        }

        // Check termination
        if (checkTermination()) {
            return null;
        }

        // Find next NOT_CONTACTED peer if under capacity
        if (waiting < capacity) {
            for (QueryPeerset.PeerInfo p : peers.getAll()) {
                if (p.state == PeerState.NOT_CONTACTED) {
                    peers.markWaiting(p.peerId);
                    return p.peerId;
                }
            }
        }

        // No more peers to contact but still waiting
        return null;
    }

    /**
     * Process a successful response from a peer.
     */
    public void onResponse(PeerId peer, List<KadPeer> closerPeers) {
        peers.markSucceeded(peer);
        resultCounter++;

        // Check if any returned peer is closer than current k-th closest
        boolean madeProgress = false;
        List<QueryPeerset.PeerInfo> closestActive = peers.getClosestActive(numResults);

        if (closerPeers != null) {
            for (KadPeer p : closerPeers) {
                if (!peers.contains(p.nodeId)) {
                    peers.addHeard(p);
                    madeProgress = true;
                }
            }
        }

        if (madeProgress || resultCounter < numResults) {
            noProgressCount = 0;
        } else {
            noProgressCount++;
            if (noProgressCount >= parallelism) {
                state = State.STALLED;
            }
        }

        // Check termination again
        checkTermination();
    }

    /**
     * Process a failed response from a peer.
     */
    public void onFailure(PeerId peer) {
        peers.markFailed(peer);
        noProgressCount++;
        if (noProgressCount >= parallelism && state == State.ITERATING) {
            state = State.STALLED;
        }
        checkTermination();
    }

    /**
     * Process a timed-out response.
     */
    public void onTimeout(PeerId peer) {
        peers.markUnresponsive(peer);
        noProgressCount++;
        if (noProgressCount >= parallelism && state == State.ITERATING) {
            state = State.STALLED;
        }
        checkTermination();
    }

    private boolean checkTermination() {
        // Check if k closest active peers are all succeeded
        List<QueryPeerset.PeerInfo> closestActive = peers.getClosestActive(beta);
        if (closestActive.size() >= numResults) {
            boolean allSucceeded = true;
            for (int i = 0; i < numResults; i++) {
                if (closestActive.get(i).state != PeerState.SUCCEEDED) {
                    allSucceeded = false;
                    break;
                }
            }
            if (allSucceeded) {
                state = State.FINISHED;
                return true;
            }
        }

        // Starvation: no heard, no waiting
        if (peers.numHeard() == 0 && peers.numWaiting() == 0) {
            state = State.FINISHED;
            return true;
        }

        return false;
    }

    public State getState() { return state; }
    public boolean isFinished() { return state == State.FINISHED; }
    public QueryPeerset getPeers() { return peers; }
    public int getResultCount() { return resultCounter; }

    /**
     * Get the final result: k closest peers that responded successfully.
     */
    public List<KadPeer> getResult() {
        return peers.getClosestSucceeded(numResults);
    }
}
