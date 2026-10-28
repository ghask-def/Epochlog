# Threading model

Every public class documents its own thread-safety contract in its Javadoc. This page is the
map: which threads exist, what each one owns, and how data moves between them.

## Broker threads

| thread | count | owns / does |
|---|---|---|
| `*-acceptor` | 1 | blocking `accept()`, hands sockets to processors round-robin |
| `*-network-N` | `network.threads` (3) | one `Selector` each; reads frames, writes responses, `transferTo` for fetch data |
| request handlers | virtual thread per request, or `handler.threads` platform threads | decode, run the API, encode the response |
| `writer-<topic>-<p>` | 1 per partition | every mutation of that partition's log |
| `group-commit-N` | 1 | batches fsync requests; each round's fsyncs run on short-lived virtual threads |
| `produce-purgatory`, `fetch-purgatory` | 1 timer + 1 expiration thread each | timing wheel for `acks=all` and long-poll fetch timeouts |
| `replica-fetcher-N-from-<src>` | 1 per broker this broker follows | fetch loop, epoch truncation, follower appends (submitted to the writer) |
| `controller-channel-N` | 1 | heartbeats, applies `LeaderAndIsr` pushes, tracks the lease |
| `*-io` (NetworkClient) | 1 per client (controller, replication) | single-threaded NIO client with correlation IDs and timeouts |
| `broker-N-scheduler` | 1 | ISR maintenance, high watermark checkpoints |
| `log-maintenance` | 1 | retention and compaction |

The controller has its own socket server and a single failure-detector thread, and serializes
metadata changes on its own monitor. Every client owns a `NetworkClient` with one IO thread; the
producer adds a sender thread that drains batches, while the consumer is driven from the
caller's thread and is not thread-safe.

## The produce path

```
network-N        reads a complete frame, stops reading if 128 requests are in flight
   |
SerialExecutor   per connection: requests from one connection run one at a time, in order
   |
handler          decodes, validates, calls Partition.appendAsLeader
   |
MPSC queue
   |
writer thread    HLC stamp, Log.append (page cache), indexes, producer state
   |
GroupCommitter   fsync, then completes the append future
   |
handler/callback acks=1: respond.  acks=all: park in the produce purgatory
   |
purgatory        completed when the high watermark passes the batch (a follower fetch
                 advances it), or expired by the timer
   |
network-N        response queued on the connection, selector woken
```

Nothing on this path blocks a platform thread while it waits for something slow. Handlers that
wait do so on a `CompletableFuture`; with virtual handler threads, a request waiting for an fsync
costs a parked virtual thread, not an OS thread.

The per-connection `SerialExecutor` is what preserves request order for a connection even though
handlers run concurrently. That ordering is what idempotent producers rely on when they pipeline
requests to the same broker.

## Ownership rules

**Partition state.** The log, the leader epoch, the leader/follower role and the ISR change
in progress belong to the partition's writer thread. Other threads read published values
(`logEndOffset`, the high watermark, the active segment) through volatiles, and change state only
by submitting a task to the writer. See [ADR 1](adr/0001-single-writer-partitions.md).

**High watermark.** Several threads can advance it at once (fetch handlers recording a follower's
progress, the group committer finishing a sync). It is a single `AtomicLong` advanced with a
CAS-max loop, so it only ever moves forward and concurrent updates cannot lose each other. It is
lowered only on the writer thread during truncation.

**Follower progress.** Each `ReplicaState` is updated by whichever handler serves that follower's
fetch. Fetches from one follower arrive on one connection and are serialized, so each
`ReplicaState` has a single writer; the leader reads them to compute the high watermark.

**Replica fetchers.** A fetcher thread never mutates a log directly. It submits follower appends
and truncations to the partition's writer tagged with the leader epoch it fetched under; the
writer drops tasks whose epoch is stale. This is what makes it safe for a `LeaderAndIsr` push to
change a partition's role while a fetch for it is in flight.

**Delayed operations.** A `DelayedOperation` can be completed by the thread that created it, by
any thread that calls `checkAndComplete` on one of its keys, or by the timer on expiry. The first
CAS on its `completed` flag wins and only that thread runs `onComplete`, so a response is sent
exactly once.

## Lock-free structures and their tests

| structure | technique | jcstress test |
|---|---|---|
| `MpscArrayQueue` | VarHandle CAS on the producer index, release/acquire slots, padded indexes | `MpscQueueRaceTest`, `MpscQueueWrapTest` |
| `Log` end offset publication | volatile write after segment write | `LogEndOffsetVisibilityTest` |
| offset/time index entries | entry written before the volatile entry count | `IndexEntryVisibilityTest` |
| `DelayedOperation` completion | CAS on a completed flag | `DelayedOperationCompletionTest` |
| `HybridLogicalClock.reserve` | CAS loop on a single `long` | `HlcUniquenessTest` |
| `HighWatermark` | CAS-max | `HighWatermarkAdvanceTest` |

`SingleWriterExecutor` uses a parked flag in the style of Dekker's algorithm: the writer sets
`parked`, re-checks the queue, then parks; a producer offers, then checks `parked` and unparks.
Both sides write before they read, and both are volatile, so at least one of them sees the
other's write and a task is never left sitting in the queue with the writer asleep.

## Virtual threads

Handlers default to virtual threads (`handler.threads=virtual`). On Java 21 a virtual thread
that blocks inside `synchronized` pins its carrier, so the hot path avoids it: appends and
fsyncs happen on dedicated platform threads (writers, the group committer), handlers wait on
futures rather than monitors, and the log uses a `ReentrantLock` for structural changes. The
remaining monitors (metadata cache, producer ID allocation, topic creation) guard short
sections on rare requests.
Setting `handler.threads=8` switches to a fixed platform pool; the benchmark table in the
README compares the two.
