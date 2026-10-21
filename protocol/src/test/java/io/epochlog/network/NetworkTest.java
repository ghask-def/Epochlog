package io.epochlog.network;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.epochlog.protocol.ApiKey;
import io.epochlog.protocol.ProtocolReader;
import io.epochlog.protocol.ProtocolWriter;
import io.epochlog.protocol.Records;
import io.epochlog.protocol.RequestMessage;
import io.epochlog.protocol.ResponseMessage;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class NetworkTest {
    @TempDir
    Path dir;

    private SocketServer server;
    private NetworkClient client;

    /** Echoes the payload back. Delays requests whose value is negative, to force out-of-order replies. */
    record Echo(long value) implements RequestMessage {
        @Override
        public ApiKey apiKey() {
            return ApiKey.METADATA;
        }

        @Override
        public void write(ProtocolWriter w) {
            w.putLong(value);
        }
    }

    record EchoReply(long value) implements ResponseMessage {
        @Override
        public void write(ProtocolWriter w) {
            w.putLong(value);
        }
    }

    private InetSocketAddress start(RequestHandler handler) throws IOException {
        server = new SocketServer("test", "127.0.0.1", 0, 2, 1 << 20, handler,
                Executors.newVirtualThreadPerTaskExecutor());
        server.start();
        client = new NetworkClient("test-client", "c1", 2_000);
        return new InetSocketAddress("127.0.0.1", server.boundPort());
    }

    @AfterEach
    void tearDown() {
        if (client != null) {
            client.close();
        }
        if (server != null) {
            server.close();
        }
    }

    @Test
    void pipelinedRequestsMatchByCorrelationId() throws Exception {
        InetSocketAddress addr = start((ctx, body) -> {
            long v = body.getLong();
            if (v % 7 == 0) {
                CompletableFuture.delayedExecutor(5, TimeUnit.MILLISECONDS).execute(() -> ctx.respond(new EchoReply(v)));
            } else {
                ctx.respond(new EchoReply(v));
            }
        });
        List<CompletableFuture<Long>> futures = new ArrayList<>();
        for (long i = 0; i < 2_000; i++) {
            futures.add(client.send(addr, new Echo(i), ProtocolReader::getLong));
        }
        for (int i = 0; i < futures.size(); i++) {
            assertThat(futures.get(i).get(5, TimeUnit.SECONDS)).isEqualTo(i);
        }
    }

    @Test
    void requestsOnOneConnectionAreDispatchedInOrder() throws Exception {
        List<Long> seen = new ArrayList<>();
        InetSocketAddress addr = start((ctx, body) -> {
            long v = body.getLong();
            synchronized (seen) {
                seen.add(v);
            }
            ctx.respond(new EchoReply(v));
        });
        List<CompletableFuture<Long>> futures = new ArrayList<>();
        for (long i = 0; i < 500; i++) {
            futures.add(client.send(addr, new Echo(i), ProtocolReader::getLong));
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0])).get(5, TimeUnit.SECONDS);
        assertThat(seen).isSorted().hasSize(500);
    }

    @Test
    void timeoutFailsTheRequest() throws Exception {
        InetSocketAddress addr = start((ctx, body) -> {
            // never respond
        });
        CompletableFuture<Long> f = client.send(addr, new Echo(1), ProtocolReader::getLong);
        assertThatThrownBy(() -> f.get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(NetworkException.class);
    }

    @Test
    void connectionRefusedFailsFast() throws Exception {
        client = new NetworkClient("test-client", "c1", 2_000);
        CompletableFuture<Long> f = client.send(new InetSocketAddress("127.0.0.1", 1), new Echo(1),
                ProtocolReader::getLong);
        assertThatThrownBy(() -> f.get(5, TimeUnit.SECONDS)).hasCauseInstanceOf(NetworkException.class);
    }

    @Test
    void serverSideDisconnectFailsInFlight() throws Exception {
        InetSocketAddress addr = start((ctx, body) -> {
            // hold the request
        });
        CompletableFuture<Long> f = client.send(addr, new Echo(1), ProtocolReader::getLong);
        Thread.sleep(100);
        server.closeAllConnections();
        assertThatThrownBy(() -> f.get(5, TimeUnit.SECONDS)).hasCauseInstanceOf(NetworkException.class);
    }

    /** A file region shipped with transferTo, the way fetch responses send log data. */
    record FileRegion(FileChannel ch, long position, int size) implements Records.Transferable {
        @Override
        public int sizeInBytes() {
            return size;
        }

        @Override
        public long transferTo(WritableByteChannel target, long offset, long count) throws IOException {
            return ch.transferTo(position + offset, Math.min(count, size - offset), target);
        }
    }

    @Test
    void transferableRecordsAreSentFromTheFile() throws Exception {
        Path file = dir.resolve("data");
        byte[] content = new byte[300_000];
        for (int i = 0; i < content.length; i++) {
            content[i] = (byte) (i * 31);
        }
        Files.write(file, content);
        FileChannel ch = FileChannel.open(file, StandardOpenOption.READ);
        InetSocketAddress addr = start((ctx, body) -> ctx.respond(w -> {
            w.putInt(123);
            w.putRecords(new FileRegion(ch, 1000, 250_000));
            w.putInt(456);
        }));
        byte[][] got = new byte[1][];
        int[] trailer = new int[2];
        client.send(addr, new Echo(0), r -> {
            trailer[0] = r.getInt();
            ByteBuffer recs = r.getRecords().buffer();
            got[0] = new byte[recs.remaining()];
            recs.get(got[0]);
            trailer[1] = r.getInt();
            return null;
        }).get(5, TimeUnit.SECONDS);
        ch.close();
        assertThat(trailer).containsExactly(123, 456);
        assertThat(got[0]).hasSize(250_000);
        for (int i = 0; i < 250_000; i++) {
            if (got[0][i] != content[1000 + i]) {
                throw new AssertionError("mismatch at " + i);
            }
        }
    }
}
