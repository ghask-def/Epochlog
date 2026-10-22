package io.epochlog.client;

import io.epochlog.network.NetworkClient;
import io.epochlog.protocol.Errors;
import io.epochlog.protocol.message.CreateTopicRequest;
import io.epochlog.protocol.message.ErrorResponse;
import io.epochlog.protocol.message.MetadataRequest;
import io.epochlog.protocol.message.MetadataResponse;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Topic administration and cluster inspection.
 *
 * <p>Thread-safety: thread-safe.
 */
public final class Admin implements AutoCloseable {
    private final ClientConfig config;
    private final NetworkClient network;
    private final ClientMetadata metadata;

    public Admin(ClientConfig config) {
        this.config = config;
        this.network = new NetworkClient("admin-" + config.clientId(), config.clientId(), config.requestTimeoutMs());
        this.metadata = new ClientMetadata(config.bootstrap(), network);
    }

    public Admin(String bootstrapServers) {
        this(ClientConfig.from(Map.of("bootstrap.servers", bootstrapServers)));
    }

    /** Creates a topic and waits until every partition has a leader. */
    public void createTopic(String name, int partitions, int replicationFactor, Map<String, String> configs)
            throws InterruptedException, TimeoutException {
        CreateTopicRequest req = new CreateTopicRequest(name, partitions, (short) replicationFactor, configs);
        long deadline = System.currentTimeMillis() + config.requestTimeoutMs();
        while (true) {
            Errors error;
            String message = null;
            try {
                ErrorResponse resp = network.send(metadata.anyBroker(), req, ErrorResponse::read)
                        .get(config.requestTimeoutMs(), TimeUnit.MILLISECONDS);
                error = resp.error();
                message = resp.message();
            } catch (ExecutionException e) {
                error = Errors.NETWORK_EXCEPTION;
            }
            if (error == Errors.NONE) {
                break;
            }
            if (!error.retriable() || System.currentTimeMillis() > deadline) {
                throw error.exception(message == null ? "create topic " + name : message);
            }
            Thread.sleep(config.retryBackoffMs());
        }
        metadata.awaitTopic(name, Math.max(1, deadline - System.currentTimeMillis()));
    }

    public MetadataResponse describe(List<String> topics) throws InterruptedException, TimeoutException {
        try {
            return network.send(metadata.anyBroker(), new MetadataRequest(topics), MetadataResponse::read)
                    .get(config.requestTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw new TimeoutException("metadata request failed: " + e.getCause());
        }
    }

    @Override
    public void close() {
        network.close();
    }
}
