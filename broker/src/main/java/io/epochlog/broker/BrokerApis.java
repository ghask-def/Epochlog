package io.epochlog.broker;

import io.epochlog.network.RequestContext;
import io.epochlog.network.RequestHandler;
import io.epochlog.protocol.ApiKey;
import io.epochlog.protocol.Errors;
import io.epochlog.protocol.ProtocolReader;
import io.epochlog.protocol.message.CreateTopicRequest;
import io.epochlog.protocol.message.ErrorResponse;
import io.epochlog.protocol.message.FetchRequest;
import io.epochlog.protocol.message.InitProducerIdResponse;
import io.epochlog.protocol.message.LeaderAndIsrRequest;
import io.epochlog.protocol.message.ListOffsetsRequest;
import io.epochlog.protocol.message.MetadataRequest;
import io.epochlog.protocol.message.OffsetCommitRequest;
import io.epochlog.protocol.message.OffsetFetchRequest;
import io.epochlog.protocol.message.OffsetForLeaderEpochRequest;
import io.epochlog.protocol.message.ProduceRequest;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decodes requests and routes them to the broker's components.
 *
 * <p>Thread-safety: thread-safe; stateless apart from references to thread-safe components.
 */
final class BrokerApis implements RequestHandler {
    private static final Logger LOG = LoggerFactory.getLogger(BrokerApis.class);

    private final Broker broker;

    BrokerApis(Broker broker) {
        this.broker = broker;
    }

    @Override
    public void handle(RequestContext ctx, ProtocolReader body) throws IOException {
        ApiKey api = ctx.header().apiKey();
        switch (api) {
            case PRODUCE -> {
                ProduceRequest req = ProduceRequest.read(body);
                if (req.acks() == 0) {
                    broker.replicaManager().handleProduce(req, resp -> {});
                    ctx.noResponse();
                } else {
                    broker.replicaManager().handleProduce(req, ctx::respond);
                }
            }
            case FETCH, REPLICA_FETCH -> broker.replicaManager().handleFetch(FetchRequest.read(api, body), ctx::respond);
            case LIST_OFFSETS -> ctx.respond(broker.replicaManager().handleListOffsets(ListOffsetsRequest.read(body)));
            case METADATA -> ctx.respond(broker.metadataCache().describe(MetadataRequest.read(body).topics()));
            case CREATE_TOPIC -> broker.createTopic(CreateTopicRequest.read(body))
                    .whenComplete((resp, err) -> ctx.respond(err == null ? resp
                            : new ErrorResponse(ErrorMapping.of(err), err.getMessage())));
            case INIT_PRODUCER_ID -> ctx.respond(new InitProducerIdResponse(Errors.NONE, broker.nextProducerId()));
            case OFFSET_COMMIT -> broker.offsetCoordinator().commit(OffsetCommitRequest.read(body), ctx::respond);
            case OFFSET_FETCH -> ctx.respond(broker.offsetCoordinator().fetch(OffsetFetchRequest.read(body)));
            case LEADER_AND_ISR -> ctx.respond(broker.handleLeaderAndIsr(LeaderAndIsrRequest.read(body)));
            case OFFSET_FOR_LEADER_EPOCH ->
                    ctx.respond(broker.replicaManager().handleOffsetForLeaderEpoch(OffsetForLeaderEpochRequest.read(body)));
            default -> {
                LOG.warn("unsupported request {} from {}", api, ctx.remoteAddress());
                ctx.closeConnection();
            }
        }
    }
}
