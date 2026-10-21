package io.epochlog.network;

import io.epochlog.protocol.ProtocolReader;

/**
 * Server-side request dispatch. Called on a handler thread, in order per connection. The
 * handler may answer synchronously or keep the context and respond later from any thread.
 *
 * <p>Thread-safety: implementations must be thread-safe; different connections are handled
 * concurrently.
 */
@FunctionalInterface
public interface RequestHandler {
    void handle(RequestContext ctx, ProtocolReader body) throws Exception;
}
