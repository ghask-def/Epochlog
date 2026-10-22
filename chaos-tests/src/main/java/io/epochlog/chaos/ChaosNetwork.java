package io.epochlog.chaos;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.UnaryOperator;

/**
 * Routes every connection between named nodes through its own {@link LinkProxy} so tests can cut,
 * slow down or reset individual links. Nodes dial each other by their real address; the resolver
 * handed to each node swaps that for the proxy of the (caller, callee) link.
 *
 * <p>Thread-safety: thread-safe.
 */
public final class ChaosNetwork implements Closeable {
    private final Map<String, String> nodeByAddress = new ConcurrentHashMap<>();
    private final Map<String, LinkProxy> links = new ConcurrentHashMap<>();
    private final Set<String> blockedLinks = ConcurrentHashMap.newKeySet();
    private final Map<String, Long> delays = new ConcurrentHashMap<>();

    /** Tells the network which node listens on {@code port}. */
    public void register(String node, int port) {
        nodeByAddress.put("127.0.0.1:" + port, node);
        nodeByAddress.put("localhost:" + port, node);
    }

    public UnaryOperator<InetSocketAddress> resolverFor(String from) {
        return addr -> {
            String key = addr.getHostString() + ":" + addr.getPort();
            String to = nodeByAddress.get(key);
            if (to == null) {
                return addr;
            }
            String link = from + "->" + to;
            LinkProxy proxy = links.computeIfAbsent(link, l -> {
                try {
                    LinkProxy p = new LinkProxy(new InetSocketAddress("127.0.0.1", addr.getPort()));
                    p.setBlocked(blockedLinks.contains(l));
                    p.setDelayMs(delays.getOrDefault(l, 0L));
                    return p;
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
            return new InetSocketAddress("127.0.0.1", proxy.port());
        };
    }

    private void setBlocked(String from, String to, boolean blocked) {
        String link = from + "->" + to;
        if (blocked) {
            blockedLinks.add(link);
        } else {
            blockedLinks.remove(link);
        }
        LinkProxy p = links.get(link);
        if (p != null) {
            p.setBlocked(blocked);
        }
    }

    /** Cuts traffic between two nodes in both directions, on connections either side opened. */
    public void partition(String a, String b) {
        setBlocked(a, b, true);
        setBlocked(b, a, true);
    }

    public void heal(String a, String b) {
        setBlocked(a, b, false);
        setBlocked(b, a, false);
    }

    public void healAll() {
        for (String link : Set.copyOf(blockedLinks)) {
            String[] ends = link.split("->");
            setBlocked(ends[0], ends[1], false);
        }
        for (String link : Set.copyOf(delays.keySet())) {
            String[] ends = link.split("->");
            delay(ends[0], ends[1], 0);
        }
    }

    /** Adds one-way latency to every chunk sent over connections from {@code from} to {@code to}. */
    public void delay(String from, String to, long ms) {
        String link = from + "->" + to;
        delays.put(link, ms);
        LinkProxy p = links.get(link);
        if (p != null) {
            p.setDelayMs(ms);
        }
    }

    /** Resets every open connection from {@code from} to {@code to}. */
    public void dropConnections(String from, String to) {
        LinkProxy p = links.get(from + "->" + to);
        if (p != null) {
            p.dropConnections();
        }
    }

    @Override
    public void close() {
        for (LinkProxy p : links.values()) {
            p.close();
        }
        links.clear();
    }
}
