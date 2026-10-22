package io.epochlog.chaos;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A TCP relay for one directed link (one node dialing another). It can blackhole traffic (bytes
 * stop flowing in both directions but connections stay open, like a real partition), add
 * latency, or reset every connection.
 *
 * <p>Thread-safety: thread-safe. Each connection is pumped by two virtual threads.
 */
final class LinkProxy implements Closeable {
    private final ServerSocket server;
    private final InetSocketAddress target;
    private final Set<Socket> sockets = ConcurrentHashMap.newKeySet();
    private volatile boolean blocked;
    private volatile long delayMs;
    private volatile boolean closed;

    LinkProxy(InetSocketAddress target) throws IOException {
        this.target = target;
        this.server = new ServerSocket(0, 50, java.net.InetAddress.getLoopbackAddress());
        Thread.ofVirtual().start(this::acceptLoop);
    }

    int port() {
        return server.getLocalPort();
    }

    void setBlocked(boolean blocked) {
        this.blocked = blocked;
    }

    void setDelayMs(long delayMs) {
        this.delayMs = delayMs;
    }

    void dropConnections() {
        for (Socket s : sockets) {
            closeQuietly(s);
        }
        sockets.clear();
    }

    private void acceptLoop() {
        while (!closed) {
            try {
                Socket client = server.accept();
                Thread.ofVirtual().start(() -> connect(client));
            } catch (IOException e) {
                return;
            }
        }
    }

    private void connect(Socket client) {
        Socket upstream = new Socket();
        try {
            client.setTcpNoDelay(true);
            upstream.setTcpNoDelay(true);
            upstream.connect(new InetSocketAddress(target.getHostString(), target.getPort()), 2_000);
        } catch (IOException e) {
            closeQuietly(client);
            closeQuietly(upstream);
            return;
        }
        sockets.add(client);
        sockets.add(upstream);
        Thread.ofVirtual().start(() -> pump(client, upstream));
        Thread.ofVirtual().start(() -> pump(upstream, client));
    }

    private void pump(Socket from, Socket to) {
        byte[] buf = new byte[64 * 1024];
        try {
            InputStream in = from.getInputStream();
            OutputStream out = to.getOutputStream();
            int n;
            while ((n = in.read(buf)) > 0) {
                while (blocked && !from.isClosed()) {
                    Thread.sleep(5);
                }
                long d = delayMs;
                if (d > 0) {
                    Thread.sleep(d);
                }
                out.write(buf, 0, n);
                out.flush();
            }
        } catch (IOException | InterruptedException e) {
            // connection torn down
        } finally {
            closeQuietly(from);
            closeQuietly(to);
            sockets.remove(from);
            sockets.remove(to);
        }
    }

    private static void closeQuietly(Socket s) {
        try {
            s.close();
        } catch (IOException ignored) {
        }
    }

    @Override
    public void close() {
        closed = true;
        try {
            server.close();
        } catch (IOException ignored) {
        }
        dropConnections();
    }
}
