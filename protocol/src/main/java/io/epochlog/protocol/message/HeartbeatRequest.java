package io.epochlog.protocol.message;

import io.epochlog.protocol.ApiKey;
import io.epochlog.protocol.ProtocolReader;
import io.epochlog.protocol.ProtocolWriter;
import io.epochlog.protocol.RequestMessage;

/**
 * Broker to controller liveness signal. Also registers the broker: a broker the controller
 * doesn't consider alive gets a new broker epoch. {@code metadataVersion} lets the controller
 * notice a broker that missed a LeaderAndIsr push.
 *
 * <p>Thread-safety: immutable.
 */
public record HeartbeatRequest(int brokerId, String host, int port, long brokerEpoch, long metadataVersion)
        implements RequestMessage {

    @Override
    public ApiKey apiKey() {
        return ApiKey.HEARTBEAT;
    }

    @Override
    public void write(ProtocolWriter w) {
        w.putInt(brokerId).putString(host).putInt(port).putLong(brokerEpoch).putLong(metadataVersion);
    }

    public static HeartbeatRequest read(ProtocolReader r) {
        return new HeartbeatRequest(r.getInt(), r.getString(), r.getInt(), r.getLong(), r.getLong());
    }
}
