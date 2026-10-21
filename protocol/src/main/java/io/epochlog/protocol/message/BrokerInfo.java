package io.epochlog.protocol.message;

import io.epochlog.protocol.ProtocolReader;
import io.epochlog.protocol.ProtocolWriter;
import java.net.InetSocketAddress;

/**
 * A broker's id and advertised address.
 *
 * <p>Thread-safety: immutable.
 */
public record BrokerInfo(int id, String host, int port) {

    public InetSocketAddress address() {
        return InetSocketAddress.createUnresolved(host, port);
    }

    public void write(ProtocolWriter w) {
        w.putInt(id).putString(host).putInt(port);
    }

    public static BrokerInfo read(ProtocolReader r) {
        return new BrokerInfo(r.getInt(), r.getString(), r.getInt());
    }

    @Override
    public String toString() {
        return id + "@" + host + ":" + port;
    }
}
