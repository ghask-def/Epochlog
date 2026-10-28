# 2. Group commit for leader durability

Status: accepted

## Context

An fsync on a laptop SSD costs somewhere between a few hundred microseconds and tens of
milliseconds depending on the device and the filesystem. Syncing after every produce request caps
a partition at a few thousand requests per second. Never syncing (Kafka's default) makes
acknowledged data depend on the page cache surviving, and relies entirely on replication for
durability.

## Decision

Writers append to the page cache, then hand `(log, endOffset)` to a shared `GroupCommitter`. Its
thread collects requests for up to `group.commit.max.wait.us` (2 ms by default) or
`group.commit.max.batch` requests, issues one fsync per distinct log, and completes every waiter
whose offset is now durable. Syncs for different logs in the same round run in parallel on
virtual threads, so a round costs roughly one fsync, not one per partition.

The produce response for `acks=1` is sent after the leader's fsync. For `acks=all` the leader's
contribution to the high watermark is its durable end offset rather than its log end, so a
record is never committed until it is on the leader's disk and in the ISR followers' logs.

Group commit can be turned off with `group.commit.enabled=false`, which gives page-cache-only
semantics like Kafka's defaults.

## Consequences

- Throughput under concurrency is close to the no-fsync case because one fsync covers many
  appends. Latency at low load gets worse by up to `max.wait` plus one fsync; see the benchmark
  table in the README.
- Followers do not fsync before acknowledging. A committed record is guaranteed to be durable on
  the leader and present (possibly only in page cache) on every ISR member. Losing power on the
  whole cluster at once can still lose the tail on followers but not on the old leader.
- Failed fsyncs complete the waiting futures exceptionally. The broker does not try to continue
  writing to a log whose fsync failed.
