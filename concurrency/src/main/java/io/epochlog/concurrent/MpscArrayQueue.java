package io.epochlog.concurrent;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Objects;
import java.util.function.Consumer;

abstract class MpscPad0 {
    long p00, p01, p02, p03, p04, p05, p06, p07;
    long p08, p09, p10, p11, p12, p13, p14, p15;
}

abstract class MpscProducerFields extends MpscPad0 {
    volatile long producerIndex;
    volatile long producerLimit;
}

abstract class MpscPad1 extends MpscProducerFields {
    long p10b, p11b, p12b, p13b, p14b, p15b, p16b, p17b;
    long p18b, p19b, p1ab, p1bb, p1cb, p1db, p1eb, p1fb;
}

abstract class MpscConsumerFields extends MpscPad1 {
    volatile long consumerIndex;
}

abstract class MpscPad2 extends MpscConsumerFields {
    long p20, p21, p22, p23, p24, p25, p26, p27;
    long p28, p29, p2a, p2b, p2c, p2d, p2e, p2f;
}

/**
 * Bounded lock-free multi-producer single-consumer queue over a power-of-two ring buffer, in the
 * style of JCTools' {@code MpscArrayQueue}.
 *
 * <p>Producers claim a slot by CAS on {@code producerIndex}, then publish the element with a
 * release store into the slot. The consumer reads the slot with an acquire load, so it sees the
 * element's fields; a claimed-but-unpublished slot reads as null and the consumer spins briefly
 * until the store lands. The consumer frees a slot by nulling it and then release-storing
 * {@code consumerIndex}; producers read that index before reusing the slot. Producer and
 * consumer indexes sit on separate cache lines.
 *
 * <p>Thread-safety: thread-safe for any number of producers calling {@link #offer}. Exactly one
 * thread may call {@link #poll} or {@link #drain}.
 */
public final class MpscArrayQueue<E> extends MpscPad2 {
    private static final VarHandle PRODUCER_INDEX;
    private static final VarHandle PRODUCER_LIMIT;
    private static final VarHandle CONSUMER_INDEX;
    private static final VarHandle SLOT = MethodHandles.arrayElementVarHandle(Object[].class);

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            PRODUCER_INDEX = l.findVarHandle(MpscProducerFields.class, "producerIndex", long.class);
            PRODUCER_LIMIT = l.findVarHandle(MpscProducerFields.class, "producerLimit", long.class);
            CONSUMER_INDEX = l.findVarHandle(MpscConsumerFields.class, "consumerIndex", long.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final Object[] buffer;
    private final int mask;
    private final int capacity;

    public MpscArrayQueue(int requestedCapacity) {
        if (requestedCapacity < 2) {
            throw new IllegalArgumentException("capacity must be at least 2");
        }
        int cap = Integer.highestOneBit(requestedCapacity - 1) << 1;
        this.capacity = cap;
        this.mask = cap - 1;
        this.buffer = new Object[cap];
        PRODUCER_LIMIT.setRelease(this, (long) cap);
    }

    public int capacity() {
        return capacity;
    }

    /** Adds an element. Returns false if the queue is full. */
    public boolean offer(E e) {
        Objects.requireNonNull(e, "element");
        long limit = (long) PRODUCER_LIMIT.getVolatile(this);
        long index;
        do {
            index = (long) PRODUCER_INDEX.getVolatile(this);
            if (index >= limit) {
                long consumed = (long) CONSUMER_INDEX.getVolatile(this);
                limit = consumed + capacity;
                if (index >= limit) {
                    return false;
                }
                PRODUCER_LIMIT.setRelease(this, limit);
            }
        } while (!PRODUCER_INDEX.compareAndSet(this, index, index + 1));
        SLOT.setRelease(buffer, (int) (index & mask), e);
        return true;
    }

    /** Removes the head, or returns null if the queue is empty. Consumer thread only. */
    @SuppressWarnings("unchecked")
    public E poll() {
        long index = (long) CONSUMER_INDEX.getOpaque(this);
        int slot = (int) (index & mask);
        Object e = SLOT.getAcquire(buffer, slot);
        if (e == null) {
            if (index == (long) PRODUCER_INDEX.getVolatile(this)) {
                return null;
            }
            // A producer claimed this slot but hasn't stored into it yet.
            do {
                Thread.onSpinWait();
                e = SLOT.getAcquire(buffer, slot);
            } while (e == null);
        }
        SLOT.setOpaque(buffer, slot, null);
        CONSUMER_INDEX.setRelease(this, index + 1);
        return (E) e;
    }

    /** Polls up to {@code limit} elements into {@code sink}. Consumer thread only. */
    public int drain(Consumer<? super E> sink, int limit) {
        int n = 0;
        E e;
        while (n < limit && (e = poll()) != null) {
            sink.accept(e);
            n++;
        }
        return n;
    }

    public boolean isEmpty() {
        return (long) CONSUMER_INDEX.getVolatile(this) == (long) PRODUCER_INDEX.getVolatile(this);
    }

    /** Approximate when called concurrently with producers. */
    public int size() {
        long before;
        long after = (long) CONSUMER_INDEX.getVolatile(this);
        long produced;
        do {
            before = after;
            produced = (long) PRODUCER_INDEX.getVolatile(this);
            after = (long) CONSUMER_INDEX.getVolatile(this);
        } while (before != after);
        return (int) Math.max(0, Math.min(capacity, produced - after));
    }
}
