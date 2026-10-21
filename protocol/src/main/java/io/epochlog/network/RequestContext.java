package io.epochlog.network;

import io.epochlog.protocol.ProtocolWriter;
import io.epochlog.protocol.RequestHeader;
import io.epochlog.protocol.ResponseMessage;
import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * One received request and the way back to its connection.
 *
 * <p>Thread-safety: thread-safe. Exactly one of {@link #respond}, {@link #noResponse} or
 * {@link #closeConnection} takes effect; later calls are ignored.
 */
public final class RequestContext {
    private final SocketServer.Processor processor;
    private final SocketServer.Connection connection;
    private final RequestHeader header;
    private final AtomicBoolean done = new AtomicBoolean();

    RequestContext(SocketServer.Processor processor, SocketServer.Connection connection, RequestHeader header) {
        this.processor = processor;
        this.connection = connection;
        this.header = header;
    }

    public RequestHeader header() {
        return header;
    }

    public SocketAddress remoteAddress() {
        return connection.remote;
    }

    public void respond(ResponseMessage response) {
        if (!done.compareAndSet(false, true)) {
            return;
        }
        ProtocolWriter w = new ProtocolWriter();
        w.putInt(header.correlationId());
        response.write(w);
        processor.enqueue(connection, w.toSend(), false);
    }

    /** For requests that get no reply (produce with acks=0). Frees the connection's in-flight slot. */
    public void noResponse() {
        if (done.compareAndSet(false, true)) {
            processor.enqueue(connection, null, false);
        }
    }

    public void closeConnection() {
        if (done.compareAndSet(false, true)) {
            processor.enqueue(connection, null, true);
        }
    }
}
