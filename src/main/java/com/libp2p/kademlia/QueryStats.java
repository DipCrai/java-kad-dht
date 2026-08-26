package com.libp2p.kademlia;

/**
 * Query statistics.
 * Port of rust QueryStats.
 */
public class QueryStats {
    public int queriesSucceeded;
    public int queriesFailed;
    public long totalLatencyMs;

    public void recordSuccess(long latencyMs) {
        queriesSucceeded++;
        totalLatencyMs += latencyMs;
    }

    public void recordFailure() {
        queriesFailed++;
    }

    public double averageLatencyMs() {
        int total = queriesSucceeded + queriesFailed;
        return total == 0 ? 0 : (double) totalLatencyMs / total;
    }

    @Override
    public String toString() {
        return "QueryStats{success=" + queriesSucceeded + ", failed=" + queriesFailed +
                ", avgLatency=" + String.format("%.0f", averageLatencyMs()) + "ms}";
    }
}
