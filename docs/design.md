# Design

This document walks through how epochlog stores, replicates and serves records. The decisions
behind the less obvious choices are in [adr/](adr/).

## Modules

| module | contents |
|---|---|
| `common` | record batch format, CRC32C, varints, config parsing, atomic file writes |
| `log-storage` | segments, offset and time indexes, `Log`, recovery, retention, compaction |
| `concurrency` | MPSC queue, single-writer executor, group commit, timing wheel, purgatory |
| `clock` | hybrid logical clock |
| `protocol` | wire format, request/response messages, NIO socket server and client |
| `replication` | ISR tracking, high watermark, epoch-based truncation, replica fetchers |
| `broker` | partitions, request handling, controller channel, offset storage |
| `controller` | broker liveness, leader election, ISR changes, metadata persistence |
| `client` | producer, consumer, admin |
| `cli` | `epochlog-produce`, `epochlog-consume`, `epochlog-admin` |
| `stress-tests` | jcstress tests for every lock-free or volatile-published structure |
| `chaos-tests` | in-process clusters with fault-injecting TCP proxies |
| `benchmarks` | JMH microbenchmarks and an end-to-end load generator |
| `deploy` | distribution scripts, Dockerfile, docker compose cluster |

## Record batches

Producers send batches, the broker stores batches, and followers and consumers fetch batches.
A batch is never re-encoded on the way through, which is what makes zero-copy fetches possible.

```
offset  size  field
     0     8  baseOffset           assigned by the leader
     8     4  length               bytes after this field
    12     4  leaderEpoch          assigned by the leader
    16     4  crc                  CRC32C of everything from attributes to the end
    20     2  attributes           compression codec in the low bits
    22     4  lastOffsetDelta
    26     8  baseTimestamp        HLC
    34     8  maxTimestamp         HLC
    42     8  producerId
    50     4  baseSequence
    54     4  recordCount
    58     *  records
```

`baseOffset` and `leaderEpoch` sit outside the CRC, so the leader can assign them by writing
into the producer's buffer without recomputing the checksum. Assigning the timestamp does need a
new CRC, which costs a pass over the batch; it is cheaper than it sounds because the batch is hot
in cache.

Each record holds varint-encoded deltas: attributes, timestamp delta, offset delta, key, value
and headers. The leader sets every record's timestamp delta equal to its offset delta, so each
record gets its own HLC tick. Gzip compression wraps the records section only.

## Storage

A partition's directory holds segments named by their base offset:

```
00000000000000000000.log
00000000000000000000.index       offset -> file position, 8-byte entries
00000000000000000000.timeindex   timestamp -> offset, 12-byte entries
leader-epoch-checkpoint
recovery-point
log-start-offset
partition.properties
.clean-shutdown
```

The indexes are sparse (one entry every `index.interval.bytes`) and memory-mapped. Lookups are a
binary search on the mapped buffer followed by a short scan of the log. Only the active segment
is writable; when it rolls, its indexes are trimmed to size and remapped read-only.

**Reads** return a `FileSlice`, a window into the segment file. The network layer sends it with
`FileChannel.transferTo`, so fetch data goes from the page cache to the socket without passing
through the JVM heap.

**Recovery.** On clean shutdown the broker writes `.clean-shutdown`. If the marker is missing on
startup, every segment after the recovery point is rescanned: each batch's CRC and structure is
checked, the indexes are rebuilt, and the log is truncated at the first bad batch. Producer
state and the leader epoch cache are rebuilt from batch headers in the same pass.

**Retention** deletes whole segments older than `log.retention.ms` or beyond
`log.retention.bytes`, never touching the active segment or anything above the high watermark.

**Compaction** (`cleanup.policy=compact`) keeps the latest record for each key. The cleaner
builds a key-to-offset map from the dirty section, rewrites older segments into a `.cleaned`
file, renames it to `.swap` and then into place. A crash at any point leaves either the old or
the new segment, and startup finishes or discards an interrupted swap. A batch whose records are
all removed is dropped, except the last batch from each idempotent producer, which is kept empty
so the producer's sequence state survives.

## Hybrid logical clock

See [ADR 3](adr/0003-hlc-over-wall-clock.md). Timestamps are `physicalMillis << 16 | logical`.
`reserve(n)` takes `n` consecutive ticks with a CAS loop, so one append of `n` records costs one
CAS. The time index is monotonic, so `offsetForTimestamp` is a binary search plus a scan of one
batch.

## Replication

Every partition has a replica list. One replica is the leader; the others fetch from it.

```
 producer --> leader ----------------------------+
              | append (writer thread)          |
              | group commit fsync              | high watermark =
              v                                 | min over ISR of
           log end, durable end                 | fetch offsets
              ^                                 |
              | REPLICA_FETCH(offset)           |
           follower 1, follower 2 --------------+
```

**Fetching.** Each broker runs one fetcher thread per source broker. It sends a single
`REPLICA_FETCH` covering every partition it follows from that broker. The fetch offset doubles as
an acknowledgement: when a follower asks for offset `n`, it has everything below `n`. The leader
long-polls fetches in a purgatory, so an idle follower costs one parked request, and new data is
pushed as soon as it is appended.

**ISR.** A follower is in sync if it has caught up to the leader's log end within the last
`replica.lag.time.max.ms`. The leader shrinks and expands the ISR by sending `AlterIsr` to the
controller. While a change is pending, the leader uses the larger of the old and new sets for
the high watermark, so a shrink can never advance the high watermark past a replica the
controller still considers in sync.

**High watermark** is the minimum fetch offset across the ISR, with the leader counted at its
durable end (when group commit is on). Consumers only see records below it. `acks=all` produce
requests wait in a purgatory until the high watermark passes their last offset, or fail with
`NOT_ENOUGH_REPLICAS` if the ISR is smaller than `min.insync.replicas`.

**Leader change and truncation.** See [ADR 4](adr/0004-leader-epochs-for-truncation.md). A
follower that learns of a new leader asks it for the end offset of its own latest epoch and
truncates to that before fetching.

## Controller

A single process (see [ADR 5](adr/0005-single-controller.md)) that owns the cluster metadata:

- **Membership.** Brokers heartbeat every `heartbeat.interval.ms`. The first heartbeat assigns a
  broker epoch. A broker that misses heartbeats for `broker.session.timeout.ms`, or that
  restarts and registers with a new epoch, is treated as failed.
- **Election.** When a leader fails, the controller picks the first live replica in the ISR,
  bumps the leader epoch and partition epoch, and pushes the new `LeaderAndIsr` to every replica.
  If no ISR member is alive, the partition goes offline rather than electing an out-of-sync
  replica. There is no unclean election.
- **ISR changes** arrive as `AlterIsr` from leaders and are accepted only if the leader epoch,
  partition epoch and broker epoch match and every new member is alive.
- **Repair.** Each heartbeat response carries the metadata version; a broker that sees a newer
  version than it has applied asks for a full snapshot, which covers lost pushes.

State is written to `controller.state` with write-to-temp, fsync, rename on every change.

Brokers use the heartbeat as a lease ([ADR 6](adr/0006-lease-based-fencing.md)): a leader that
has not had a heartbeat acknowledged recently rejects produce requests.

## Clients

**Producer.** Records are appended to per-partition batches; a sender thread drains batches that
are full or older than `linger.ms` and groups them into one request per broker. Records without
a key use a sticky partitioner that switches partitions when a batch fills; keyed records use
murmur2. With `enable.idempotence=true` (the default) the producer gets a producer ID from the
broker, numbers every batch and keeps at most one batch in flight per partition, so a retry can
never reorder records and the broker can drop exact duplicates.

**Consumer.** Assigned partitions are fetched with one pipelined request per broker. Positions
can be set by offset, earliest, latest, or timestamp (`ListOffsets` against the time index).
Offsets for a group are committed to the internal compacted topic `__consumer_offsets`,
partition `hash(group) % n`, created on first use.

## Guarantees and limits

- A record acknowledged with `acks=all` is on the leader's disk and in the log of every ISR
  member, and it survives any failure that leaves one ISR member alive.
- Consumers never see a record above the high watermark, so they never read a record that is
  later truncated. Every chaos scenario checks this.
- Within a partition, offsets and HLC timestamps both strictly increase, across leader changes.
- With idempotence, retries do not create duplicates or reorder records within a partition.
- Not supported: transactions, consumer group rebalancing (consumers assign partitions
  themselves), authentication, a replicated controller, unclean leader election.
