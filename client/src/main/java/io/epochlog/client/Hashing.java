package io.epochlog.client;

/**
 * Murmur2, used to map keys to partitions so the same key always lands in the same partition.
 *
 * <p>Thread-safety: thread-safe (stateless).
 */
final class Hashing {
    private Hashing() {}

    @SuppressWarnings("fallthrough")
    static int murmur2(byte[] data) {
        int length = data.length;
        int seed = 0x9747b28c;
        final int m = 0x5bd1e995;
        final int r = 24;
        int h = seed ^ length;
        int length4 = length / 4;
        for (int i = 0; i < length4; i++) {
            int i4 = i * 4;
            int k = (data[i4] & 0xff) + ((data[i4 + 1] & 0xff) << 8) + ((data[i4 + 2] & 0xff) << 16)
                    + ((data[i4 + 3] & 0xff) << 24);
            k *= m;
            k ^= k >>> r;
            k *= m;
            h *= m;
            h ^= k;
        }
        switch (length % 4) {
            case 3:
                h ^= (data[(length & ~3) + 2] & 0xff) << 16;
                // fall through
            case 2:
                h ^= (data[(length & ~3) + 1] & 0xff) << 8;
                // fall through
            case 1:
                h ^= data[length & ~3] & 0xff;
                h *= m;
                break;
            default:
                break;
        }
        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;
        return h;
    }

    static int partitionFor(byte[] key, int partitions) {
        return (murmur2(key) & 0x7fffffff) % partitions;
    }
}
