package io.epochlog.replication;

import io.epochlog.common.TopicPartition;
import io.epochlog.network.NetworkClient;
import io.epochlog.protocol.message.BrokerInfo;
import java.util.HashMap;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * Keeps one {@link ReplicaFetcherThread} per source broker and moves partitions between them as
 * leadership changes.
 *
 * <p>Thread-safety: thread-safe (synchronized).
 */
public final class ReplicaFetcherManager {
    private final int localBrokerId;
    private final NetworkClient client;
    private final ReplicaFetcherThread.Settings settings;
    private final LongSupplier brokerEpoch;
    private final Map<Integer, ReplicaFetcherThread> fetchers = new HashMap<>();
    private final Map<TopicPartition, Integer> sourceOf = new HashMap<>();

    public ReplicaFetcherManager(int localBrokerId, NetworkClient client, ReplicaFetcherThread.Settings settings,
            LongSupplier brokerEpoch) {
        this.localBrokerId = localBrokerId;
        this.client = client;
        this.settings = settings;
        this.brokerEpoch = brokerEpoch;
    }

    /** Starts (or restarts, after a leader change) fetching {@code replica} from {@code leader}. */
    public synchronized void follow(FollowerReplica replica, BrokerInfo leader) {
        remove(replica.topicPartition());
        ReplicaFetcherThread t = fetchers.get(leader.id());
        if (t != null && !t.leader().equals(leader)) {
            // The broker came back on a different address.
            t.shutdown();
            fetchers.remove(leader.id());
            t = null;
        }
        if (t == null) {
            t = new ReplicaFetcherThread(localBrokerId, leader, client, settings, brokerEpoch);
            t.start();
            fetchers.put(leader.id(), t);
        }
        t.add(replica);
        sourceOf.put(replica.topicPartition(), leader.id());
    }

    public synchronized void remove(TopicPartition tp) {
        Integer source = sourceOf.remove(tp);
        if (source == null) {
            return;
        }
        ReplicaFetcherThread t = fetchers.get(source);
        if (t != null) {
            t.remove(tp);
            if (t.isEmpty()) {
                t.shutdown();
                fetchers.remove(source);
            }
        }
    }

    public synchronized int fetcherCount() {
        return fetchers.size();
    }

    public synchronized void shutdown() {
        for (ReplicaFetcherThread t : fetchers.values()) {
            t.shutdown();
        }
        fetchers.clear();
        sourceOf.clear();
    }
}
