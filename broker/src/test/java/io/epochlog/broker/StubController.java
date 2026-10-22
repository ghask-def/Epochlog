package io.epochlog.broker;

import io.epochlog.network.NetworkClient;
import io.epochlog.network.SocketServer;
import io.epochlog.protocol.Errors;
import io.epochlog.protocol.message.AlterIsrRequest;
import io.epochlog.protocol.message.AlterIsrResponse;
import io.epochlog.protocol.message.BrokerInfo;
import io.epochlog.protocol.message.HeartbeatRequest;
import io.epochlog.protocol.message.HeartbeatResponse;
import io.epochlog.protocol.message.LeaderAndIsrRequest;
import io.epochlog.protocol.message.PartitionErrorsResponse;
import io.epochlog.protocol.message.PartitionState;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Just enough controller to test replication without the real one: accepts heartbeats and every
 * ISR change, and pushes whatever partition state the test sets.
 */
final class StubController implements AutoCloseable {
    private final SocketServer server;
    private final NetworkClient client = new NetworkClient("stub-controller", "controller", 5_000);
    private final Map<Integer, BrokerInfo> brokers = new ConcurrentHashMap<>();
    private final Map<String, PartitionState> partitions = new TreeMap<>();
    private long version;

    StubController() throws Exception {
        server = new SocketServer("stub-controller", "127.0.0.1", 0, 1, 1 << 20, (ctx, body) -> {
            switch (ctx.header().apiKey()) {
                case HEARTBEAT -> {
                    HeartbeatRequest hb = HeartbeatRequest.read(body);
                    brokers.put(hb.brokerId(), new BrokerInfo(hb.brokerId(), hb.host(), hb.port()));
                    ctx.respond(new HeartbeatResponse(Errors.NONE, 1, version));
                }
                case ALTER_ISR -> ctx.respond(alterIsr(AlterIsrRequest.read(body)));
                default -> ctx.closeConnection();
            }
        }, Executors.newVirtualThreadPerTaskExecutor());
        server.start();
    }

    int port() {
        return server.boundPort();
    }

    private synchronized AlterIsrResponse alterIsr(AlterIsrRequest req) {
        List<AlterIsrResponse.PartitionResult> out = new ArrayList<>();
        for (AlterIsrRequest.PartitionData pd : req.partitions()) {
            String key = pd.topic() + "-" + pd.partition();
            PartitionState cur = partitions.get(key);
            if (cur == null || cur.leaderEpoch() != pd.leaderEpoch() || cur.partitionEpoch() != pd.partitionEpoch()) {
                out.add(new AlterIsrResponse.PartitionResult(pd.topic(), pd.partition(), Errors.INVALID_UPDATE_VERSION,
                        -1, -1, List.of()));
                continue;
            }
            PartitionState next = new PartitionState(cur.topic(), cur.partition(), cur.leader(), cur.leaderEpoch(),
                    cur.partitionEpoch() + 1, cur.replicas(), pd.newIsr());
            partitions.put(key, next);
            out.add(new AlterIsrResponse.PartitionResult(pd.topic(), pd.partition(), Errors.NONE, next.leaderEpoch(),
                    next.partitionEpoch(), next.isr()));
        }
        version++;
        return new AlterIsrResponse(Errors.NONE, out);
    }

    void awaitBrokers(int n) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (brokers.size() < n && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
    }

    synchronized void set(PartitionState ps) {
        partitions.put(ps.topic() + "-" + ps.partition(), ps);
    }

    synchronized PartitionState get(String topic, int partition) {
        return partitions.get(topic + "-" + partition);
    }

    /** Sends the current state to every known broker and waits for them to apply it. */
    void push() throws Exception {
        LeaderAndIsrRequest req;
        synchronized (this) {
            version++;
            req = new LeaderAndIsrRequest(0, version, new ArrayList<>(brokers.values()),
                    new ArrayList<>(partitions.values()), List.of());
        }
        for (BrokerInfo b : req.liveBrokers()) {
            try {
                client.send(new InetSocketAddress(b.host(), b.port()), req, PartitionErrorsResponse::read)
                        .get(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                // broker may be down
            }
        }
    }

    @Override
    public void close() {
        client.close();
        server.close();
    }
}
