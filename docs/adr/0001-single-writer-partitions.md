# 1. One writer thread per partition

Status: accepted

## Context

A partition's log is a sequence of segments, an offset index, a time index, the leader epoch
cache and the producer state table. An append touches all of them, and they have to agree: the
index must never point past the end of the segment, producer sequence checks must see the
previous batch, and the HLC stamp on a batch must be greater than the last one written.

The obvious approach is a lock around `Log.append`. That works, but every reader of log state
(fetch handlers, the replica fetchers, the high watermark computation, retention) then has to
decide whether it needs the lock too, and under load the lock becomes the thing everybody queues
on. Request handlers run on virtual threads, and a virtual thread blocked on a contended monitor
pins its carrier on Java 21.

## Decision

Each partition owns a `SingleWriterExecutor`: a platform thread draining a bounded MPSC queue.
Every mutation of the partition's log (leader append, follower append, truncation, becoming
leader or follower) is a task on that queue. Nothing else writes to the log.

Readers do not go through the writer. The log publishes its end offset and the segment and index
positions through volatile fields written after the bytes they describe, so a reader that sees an
offset can read everything below it. Fetches are served directly from handler threads.

## Consequences

- The append and read paths take no locks. The contract for `Log`, `LogSegment` and the
  indexes is "one writer, any number of readers", checked by the jcstress tests
  `LogEndOffsetVisibilityTest` and `IndexEntryVisibilityTest`. Structural changes (segment roll,
  truncation, retention, the compaction swap) take a per-log lock, because retention and the
  cleaner run on background threads; those are rare, so the lock is almost never contended.
- Ordering of appends inside a partition is the queue order, so idempotent sequence checks are
  trivially correct.
- Partitions are independent, so a slow fsync or a large batch on one partition does not delay
  another.
- A thread per partition does not scale to tens of thousands of partitions. That is fine for
  this project; a real system would multiplex partitions onto a pool of writers keyed by
  partition, which keeps the same contract.
- Callers get a `CompletableFuture` back instead of a return value, which spreads async code
  through the produce path.
