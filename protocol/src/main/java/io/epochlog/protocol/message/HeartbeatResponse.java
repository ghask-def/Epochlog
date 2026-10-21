package io.epochlog.protocol.message;

import io.epochlog.protocol.Errors;
import io.epochlog.protocol.ProtocolReader;
import io.epochlog.protocol.ProtocolWriter;
import io.epochlog.protocol.ResponseMessage;

/**
 * Heartbeat acknowledgement with the broker's (possibly new) epoch.
 *
 * <p>Thread-safety: immutable.
 */
public record HeartbeatResponse(Errors error, long brokerEpoch, long metadataVersion) implements ResponseMessage {

    @Override
    public void write(ProtocolWriter w) {
        w.putShort(error.code).putLong(brokerEpoch).putLong(metadataVersion);
    }

    public static HeartbeatResponse read(ProtocolReader r) {
        return new HeartbeatResponse(Errors.forCode(r.getShort()), r.getLong(), r.getLong());
    }
}
