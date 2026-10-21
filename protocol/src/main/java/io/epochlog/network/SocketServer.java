package io.epochlog.network;

import io.epochlog.concurrent.MpscArrayQueue;
import io.epochlog.concurrent.SerialExecutor;
import io.epochlog.protocol.ProtocolReader;
import io.epochlog.protocol.RequestHeader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Length-prefixed TCP server on raw {@code java.nio}.
 *
 * <ul>
 *   <li>One acceptor thread blocks in {@code accept()} and hands sockets to processors round robin.
 *   <li>N processor threads each own a selector and their connections. They read frames, write
 *       responses, and never block on anything else.
 *   <li>Requests run on the handler executor (virtual threads by default) behind a per-connection
 *       {@link SerialExecutor}, so requests on one connection are dispatched in order while
 *       different connections run in parallel. Responses may complete out of order; clients
 *       match them by correlation id.
 * </ul>
 *
 * A connection with too many requests in flight stops being read until responses drain.
 *
 * <p>Thread-safety: thread-safe. Connection state is confined to its processor thread; other
 * threads talk to a processor only through its queues.
 */
public final class SocketServer implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(SocketServer.class);

    private final String name;
    private final String host;
    private final int port;
    private final int maxRequestBytes;
    private final int maxInFlightPerConnection;
    private final RequestHandler handler;
    private final Executor handlerExecutor;
    private final List<Processor> processors = new ArrayList<>();
    private ServerSocketChannel serverChannel;
    private Thread acceptor;
    private volatile boolean running;
    private int boundPort;

    public SocketServer(String name, String host, int port, int numProcessors, int maxRequestBytes,
            RequestHandler handler, Executor handlerExecutor) {
        this.name = name;
        this.host = host;
        this.port = port;
        this.maxRequestBytes = maxRequestBytes;
        this.maxInFlightPerConnection = 128;
        this.handler = handler;
        this.handlerExecutor = handlerExecutor;
        for (int i = 0; i < numProcessors; i++) {
            processors.add(new Processor(i));
        }
    }

    public void start() throws IOException {
        serverChannel = ServerSocketChannel.open();
        serverChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        serverChannel.bind(new InetSocketAddress(host, port), 1024);
        boundPort = ((InetSocketAddress) serverChannel.getLocalAddress()).getPort();
        running = true;
        for (Processor p : processors) {
            p.thread.start();
        }
        acceptor = Thread.ofPlatform().name(name + "-acceptor").daemon().start(this::acceptLoop);
        LOG.info("{} listening on {}:{}", name, host, boundPort);
    }

    public int boundPort() {
        return boundPort;
    }

    private void acceptLoop() {
        int next = 0;
        while (running) {
            try {
                SocketChannel ch = serverChannel.accept();
                ch.configureBlocking(false);
                ch.setOption(StandardSocketOptions.TCP_NODELAY, true);
                processors.get(next++ % processors.size()).accept(ch);
            } catch (ClosedChannelException e) {
                return;
            } catch (IOException e) {
                if (running) {
                    LOG.warn("{} accept failed", name, e);
                }
            }
        }
    }

    /** Drops every open connection. Used by tests to simulate network failures. */
    public void closeAllConnections() {
        for (Processor p : processors) {
            p.closeAllRequested = true;
            p.selector.wakeup();
        }
    }

    @Override
    public void close() {
        running = false;
        try {
            if (serverChannel != null) {
                serverChannel.close();
            }
        } catch (IOException ignored) {
        }
        for (Processor p : processors) {
            p.shutdown();
        }
        if (acceptor != null) {
            try {
                acceptor.join(2_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Per-socket state. Fields other than {@code remote} are touched only by the owning processor.
     *
     * <p>Thread-safety: confined to its processor thread.
     */
    static final class Connection {
        final SocketChannel channel;
        final SocketAddress remote;
        final SerialExecutor dispatcher;
        final ArrayDeque<Send> sends = new ArrayDeque<>();
        final AtomicInteger inFlight = new AtomicInteger();
        SelectionKey key;
        NetworkReceive receive;
        boolean muted;
        volatile boolean closed;

        Connection(SocketChannel channel, SerialExecutor dispatcher, int maxRequestBytes) throws IOException {
            this.channel = channel;
            this.remote = channel.getRemoteAddress();
            this.dispatcher = dispatcher;
            this.receive = new NetworkReceive(maxRequestBytes);
        }
    }

    private record Outgoing(Connection connection, Send send, boolean close) {}

    /**
     * A selector thread.
     *
     * <p>Thread-safety: {@link #accept} and {@link #enqueue} are thread-safe; everything else
     * runs on the processor thread.
     */
    final class Processor {
        final Thread thread;
        final Selector selector;
        private final Queue<SocketChannel> newConnections = new ConcurrentLinkedQueue<>();
        private final MpscArrayQueue<Outgoing> responses = new MpscArrayQueue<>(1 << 16);
        private final Map<SocketChannel, Connection> connections = new HashMap<>();
        volatile boolean closeAllRequested;
        private volatile boolean stopped;

        Processor(int id) {
            try {
                this.selector = Selector.open();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            this.thread = Thread.ofPlatform().name(name + "-network-" + id).daemon().unstarted(this::run);
        }

        void accept(SocketChannel ch) {
            newConnections.add(ch);
            selector.wakeup();
        }

        void enqueue(Connection c, Send send, boolean close) {
            Outgoing o = new Outgoing(c, send, close);
            while (!responses.offer(o)) {
                if (stopped) {
                    return;
                }
                selector.wakeup();
                Thread.onSpinWait();
            }
            selector.wakeup();
        }

        private void run() {
            while (!stopped) {
                try {
                    registerNew();
                    processResponses();
                    if (closeAllRequested) {
                        closeAllRequested = false;
                        for (Connection c : new ArrayList<>(connections.values())) {
                            close(c);
                        }
                    }
                    selector.select(300);
                    Iterator<SelectionKey> it = selector.selectedKeys().iterator();
                    while (it.hasNext()) {
                        SelectionKey key = it.next();
                        it.remove();
                        Connection c = (Connection) key.attachment();
                        try {
                            if (key.isReadable()) {
                                read(c);
                            }
                            if (key.isValid() && key.isWritable()) {
                                write(c);
                            }
                        } catch (IOException | CancelledKeyException e) {
                            close(c);
                        }
                    }
                } catch (IOException | RuntimeException e) {
                    LOG.error("{} processor error", name, e);
                }
            }
            for (Connection c : new ArrayList<>(connections.values())) {
                close(c);
            }
            try {
                selector.close();
            } catch (IOException ignored) {
            }
        }

        private void registerNew() {
            SocketChannel ch;
            while ((ch = newConnections.poll()) != null) {
                try {
                    Connection c = new Connection(ch, new SerialExecutor(handlerExecutor), maxRequestBytes);
                    c.key = ch.register(selector, SelectionKey.OP_READ, c);
                    connections.put(ch, c);
                } catch (IOException e) {
                    try {
                        ch.close();
                    } catch (IOException ignored) {
                    }
                }
            }
        }

        private void read(Connection c) throws IOException {
            while (!c.muted) {
                if (!c.receive.readFrom(c.channel)) {
                    return;
                }
                ByteBuffer frame = c.receive.payload();
                c.receive = new NetworkReceive(maxRequestBytes);
                if (c.inFlight.incrementAndGet() >= maxInFlightPerConnection) {
                    c.muted = true;
                    c.key.interestOps(c.key.interestOps() & ~SelectionKey.OP_READ);
                }
                c.dispatcher.execute(() -> dispatch(c, frame));
            }
        }

        private void dispatch(Connection c, ByteBuffer frame) {
            if (c.closed) {
                return;
            }
            RequestContext ctx = null;
            try {
                ProtocolReader reader = new ProtocolReader(frame);
                RequestHeader header = RequestHeader.read(reader);
                ctx = new RequestContext(this, c, header);
                handler.handle(ctx, reader);
            } catch (Exception e) {
                LOG.warn("{} failed to handle request from {}, closing connection", name, c.remote, e);
                if (ctx != null) {
                    ctx.closeConnection();
                } else {
                    enqueue(c, null, true);
                }
            }
        }

        private void processResponses() {
            Outgoing o;
            while ((o = responses.poll()) != null) {
                Connection c = o.connection;
                if (c.closed) {
                    continue;
                }
                if (o.close) {
                    close(c);
                    continue;
                }
                if (o.send != null) {
                    c.sends.add(o.send);
                    c.key.interestOps(c.key.interestOps() | SelectionKey.OP_WRITE);
                }
                if (c.inFlight.decrementAndGet() < maxInFlightPerConnection && c.muted) {
                    c.muted = false;
                    c.key.interestOps(c.key.interestOps() | SelectionKey.OP_READ);
                }
            }
        }

        private void write(Connection c) throws IOException {
            while (!c.sends.isEmpty()) {
                Send s = c.sends.peek();
                s.writeTo(c.channel);
                if (!s.completed()) {
                    return;
                }
                c.sends.poll();
            }
            c.key.interestOps(c.key.interestOps() & ~SelectionKey.OP_WRITE);
        }

        private void close(Connection c) {
            if (c.closed) {
                return;
            }
            c.closed = true;
            connections.remove(c.channel);
            if (c.key != null) {
                c.key.cancel();
            }
            try {
                c.channel.close();
            } catch (IOException ignored) {
            }
        }

        void shutdown() {
            stopped = true;
            selector.wakeup();
            try {
                thread.join(2_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
