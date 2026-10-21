package io.epochlog.network;

import io.epochlog.protocol.ProtocolReader;
import io.epochlog.protocol.ProtocolWriter;
import io.epochlog.protocol.RequestHeader;
import io.epochlog.protocol.RequestMessage;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Asynchronous request/response client on one selector thread. Keeps one connection per
 * destination and pipelines requests over it. A request that times out fails its future and
 * closes the connection, since a late response would otherwise be ambiguous.
 *
 * <p>Futures complete on the client's I/O thread, so callbacks must not block or call
 * {@code get()} on another request from this client.
 *
 * <p>Thread-safety: {@link #send} and {@link #close} are thread-safe; all socket work happens
 * on the I/O thread.
 */
public final class NetworkClient implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(NetworkClient.class);
    private static final int MAX_RESPONSE_BYTES = 128 << 20;

    private final String clientId;
    private final long requestTimeoutMs;
    private final UnaryOperator<InetSocketAddress> resolver;
    private final Selector selector;
    private final Thread thread;
    private final Queue<Pending> submitted = new ConcurrentLinkedQueue<>();
    private final Queue<String> disconnectRequests = new ConcurrentLinkedQueue<>();
    private final Map<String, Connection> connections = new HashMap<>();
    private final AtomicInteger correlation = new AtomicInteger();
    private volatile boolean running = true;

    public NetworkClient(String name, String clientId, long requestTimeoutMs, UnaryOperator<InetSocketAddress> resolver) {
        this.clientId = clientId;
        this.requestTimeoutMs = requestTimeoutMs;
        this.resolver = resolver;
        try {
            this.selector = Selector.open();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        this.thread = Thread.ofPlatform().name(name + "-io").daemon().start(this::run);
    }

    public NetworkClient(String name, String clientId, long requestTimeoutMs) {
        this(name, clientId, requestTimeoutMs, UnaryOperator.identity());
    }

    private static final class Pending {
        final String destKey;
        final InetSocketAddress dest;
        final int correlationId;
        final Send send;
        final boolean expectsResponse;
        final Function<ProtocolReader, ?> parser;
        final CompletableFuture<Object> future = new CompletableFuture<>();
        final long deadlineMs;

        Pending(InetSocketAddress dest, int correlationId, Send send, boolean expectsResponse,
                Function<ProtocolReader, ?> parser, long deadlineMs) {
            this.destKey = key(dest);
            this.dest = dest;
            this.correlationId = correlationId;
            this.send = send;
            this.expectsResponse = expectsResponse;
            this.parser = parser;
            this.deadlineMs = deadlineMs;
        }
    }

    private static final class Connection {
        final String key;
        final SocketChannel channel;
        final SelectionKey selectionKey;
        final ArrayDeque<Pending> toSend = new ArrayDeque<>();
        final Map<Integer, Pending> inFlight = new LinkedHashMap<>();
        NetworkReceive receive = new NetworkReceive(MAX_RESPONSE_BYTES);
        boolean connected;

        Connection(String key, SocketChannel channel, SelectionKey selectionKey) {
            this.key = key;
            this.channel = channel;
            this.selectionKey = selectionKey;
        }
    }

    private static String key(InetSocketAddress a) {
        return a.getHostString() + ":" + a.getPort();
    }

    /**
     * Sends a request. For requests that expect no response the future completes with null once
     * the bytes are written.
     */
    @SuppressWarnings("unchecked")
    public <R> CompletableFuture<R> send(InetSocketAddress dest, RequestMessage request, Function<ProtocolReader, R> parser) {
        int corr = correlation.incrementAndGet();
        ProtocolWriter w = new ProtocolWriter();
        new RequestHeader(request.apiKey(), (short) 0, corr, clientId).write(w);
        request.write(w);
        Pending p = new Pending(dest, corr, w.toSend(), request.expectsResponse(), parser,
                System.currentTimeMillis() + requestTimeoutMs);
        if (!running) {
            p.future.completeExceptionally(new NetworkException("client closed"));
        } else {
            submitted.add(p);
            selector.wakeup();
        }
        return (CompletableFuture<R>) p.future;
    }

    /** Closes the connection to {@code dest}, failing anything in flight on it. */
    public void disconnect(InetSocketAddress dest) {
        disconnectRequests.add(key(dest));
        selector.wakeup();
    }

    private void run() {
        while (running) {
            try {
                handleDisconnects();
                handleSubmitted();
                selector.select(20);
                Iterator<SelectionKey> it = selector.selectedKeys().iterator();
                while (it.hasNext()) {
                    SelectionKey k = it.next();
                    it.remove();
                    Connection c = (Connection) k.attachment();
                    try {
                        if (k.isConnectable()) {
                            c.channel.finishConnect();
                            c.connected = true;
                            updateInterest(c);
                        }
                        if (k.isValid() && k.isReadable()) {
                            read(c);
                        }
                        if (k.isValid() && k.isWritable()) {
                            write(c);
                        }
                    } catch (IOException | CancelledKeyException e) {
                        fail(c, new NetworkException("connection to " + c.key + " failed: " + e.getMessage(), e));
                    }
                }
                expireTimedOut();
            } catch (IOException | RuntimeException e) {
                LOG.error("network client loop error", e);
            }
        }
        for (Connection c : new ArrayList<>(connections.values())) {
            fail(c, new NetworkException("client closed"));
        }
        Pending p;
        while ((p = submitted.poll()) != null) {
            p.future.completeExceptionally(new NetworkException("client closed"));
        }
        try {
            selector.close();
        } catch (IOException ignored) {
        }
    }

    private void handleDisconnects() {
        String k;
        while ((k = disconnectRequests.poll()) != null) {
            Connection c = connections.get(k);
            if (c != null) {
                fail(c, new NetworkException("disconnected"));
            }
        }
    }

    private void handleSubmitted() {
        Pending p;
        while ((p = submitted.poll()) != null) {
            Connection c = connections.get(p.destKey);
            if (c == null) {
                try {
                    c = connect(p.destKey, p.dest);
                } catch (IOException | RuntimeException e) {
                    p.future.completeExceptionally(new NetworkException("cannot connect to " + p.destKey, e));
                    continue;
                }
            }
            c.toSend.add(p);
            updateInterest(c);
        }
    }

    private Connection connect(String key, InetSocketAddress dest) throws IOException {
        InetSocketAddress target = resolver.apply(dest);
        InetSocketAddress resolved = target.isUnresolved()
                ? new InetSocketAddress(target.getHostString(), target.getPort())
                : target;
        SocketChannel ch = SocketChannel.open();
        try {
            ch.configureBlocking(false);
            ch.setOption(StandardSocketOptions.TCP_NODELAY, true);
            boolean done = ch.connect(resolved);
            SelectionKey sk = ch.register(selector, done ? SelectionKey.OP_READ : SelectionKey.OP_CONNECT);
            Connection c = new Connection(key, ch, sk);
            c.connected = done;
            sk.attach(c);
            connections.put(key, c);
            return c;
        } catch (IOException | RuntimeException e) {
            ch.close();
            throw e;
        }
    }

    private void updateInterest(Connection c) {
        if (!c.selectionKey.isValid()) {
            return;
        }
        if (!c.connected) {
            c.selectionKey.interestOps(SelectionKey.OP_CONNECT);
            return;
        }
        int ops = SelectionKey.OP_READ;
        if (!c.toSend.isEmpty()) {
            ops |= SelectionKey.OP_WRITE;
        }
        c.selectionKey.interestOps(ops);
    }

    private void write(Connection c) throws IOException {
        while (!c.toSend.isEmpty()) {
            Pending p = c.toSend.peek();
            p.send.writeTo(c.channel);
            if (!p.send.completed()) {
                return;
            }
            c.toSend.poll();
            if (p.expectsResponse) {
                c.inFlight.put(p.correlationId, p);
            } else {
                p.future.complete(null);
            }
        }
        updateInterest(c);
    }

    private void read(Connection c) throws IOException {
        while (c.receive.readFrom(c.channel)) {
            ByteBuffer frame = c.receive.payload();
            c.receive = new NetworkReceive(MAX_RESPONSE_BYTES);
            int corr = frame.getInt();
            Pending p = c.inFlight.remove(corr);
            if (p == null) {
                LOG.debug("dropping response for unknown correlation id {} from {}", corr, c.key);
                continue;
            }
            try {
                p.future.complete(p.parser.apply(new ProtocolReader(frame)));
            } catch (RuntimeException e) {
                p.future.completeExceptionally(e);
            }
        }
    }

    private void expireTimedOut() {
        long now = System.currentTimeMillis();
        List<Connection> toClose = new ArrayList<>();
        for (Connection c : connections.values()) {
            Pending oldest = c.inFlight.isEmpty() ? c.toSend.peek() : c.inFlight.values().iterator().next();
            if (oldest != null && oldest.deadlineMs < now) {
                toClose.add(c);
            }
        }
        for (Connection c : toClose) {
            fail(c, new NetworkException("request to " + c.key + " timed out after " + requestTimeoutMs + " ms",
                    new TimeoutException()));
        }
    }

    private void fail(Connection c, NetworkException e) {
        connections.remove(c.key);
        c.selectionKey.cancel();
        try {
            c.channel.close();
        } catch (IOException ignored) {
        }
        for (Pending p : c.inFlight.values()) {
            p.future.completeExceptionally(e);
        }
        for (Pending p : c.toSend) {
            p.future.completeExceptionally(e);
        }
        c.inFlight.clear();
        c.toSend.clear();
    }

    @Override
    public void close() {
        running = false;
        selector.wakeup();
        try {
            thread.join(2_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
