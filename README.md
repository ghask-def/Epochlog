# epochlog

A replicated commit log in the style of Kafka, written from scratch in Java 21. Partitions are
replicated with an in-sync replica set, truncation after failover uses leader epochs, and every
record is stamped with a hybrid logical clock, so timestamps are unique, increase with offset
and keep increasing across leader changes even when broker clocks disagree.

Nothing here comes from Kafka, Raft, Netty or a messaging library. Networking is plain
`java.nio`, storage is `FileChannel` and `MappedByteBuffer`, and the only runtime dependencies
are SLF4J, Logback and picocli.

## Architecture

```
                         +------------------+
                         |    controller    |   brokers, topics, leaders, ISRs
                         | (controller.state)|  heartbeats = leases
                         +------------------+
                            ^      |  LeaderAndIsr
              heartbeat,    |      v
              AlterIsr   +---------------------------------------------+
                         |                  broker                     |
 producer --- produce -->|  NIO acceptor + selectors                   |
                         |     -> per-connection serial executor       |
 consumer <-- fetch -----|     -> handlers (virtual threads)           |
                         |          |                                  |
                         |   partition writer thread (one per partition)
                         |          |  HLC stamp, append to segment    |
                         |   group commit (batched, parallel fsync)    |
                         |          |                                  |
                         |   segments + mmap offset/time indexes       |
                         |   purgatories: acks=all, long-poll fetch    |
                         +---------------------------------------------+
                               ^  replica fetch (one thread per leader)
                               |
                         other brokers (followers)
```

The design doc covers each piece in more detail: [docs/design.md](docs/design.md). The threads
and who owns what are in [docs/threading-model.md](docs/threading-model.md).

## Quickstart

With Docker, a controller and three brokers:

```sh
docker compose -f deploy/docker-compose.yml up --build -d

docker compose -f deploy/docker-compose.yml exec broker1 \
  epochlog-admin create-topic -b broker1:9092 -p 6 -r 3 events

docker compose -f deploy/docker-compose.yml exec broker1 \
  epochlog-admin describe -b broker1:9092 events

docker compose -f deploy/docker-compose.yml exec -T broker1 \
  epochlog-produce -b broker1:9092 -t events --key-separator : <<'IN'
user-1:signed up
user-2:signed up
user-1:clicked
IN

docker compose -f deploy/docker-compose.yml exec broker1 \
  epochlog-consume -b broker1:9092 -t events --from-beginning --print-metadata --timeout-ms 2000
```

Without Docker:

```sh
./gradlew :deploy:installDist
export PATH=$PWD/deploy/build/install/epochlog/bin:$PATH

epochlog-controller port=9090 data.dir=data/controller &
for id in 1 2 3; do
  epochlog-broker deploy/broker.properties broker.id=$id port=909$((id + 1)) data.dir=data/broker-$id &
done

epochlog-admin create-topic -b localhost:9092 -p 3 -r 3 events
seq 1 1000 | epochlog-produce -b localhost:9092 -t events
epochlog-consume -b localhost:9092 -t events --from-beginning -n 1000 --group demo
```

Kill a broker with `kill -9` and keep producing with `--acks all`: leadership moves within the
session timeout, and nothing that was acknowledged goes missing. `--from-time 2026-01-01T00:00:00Z`
seeks by HLC timestamp.

## Guarantees

- **Durability.** With `acks=all`, a record is acknowledged once it is fsynced on the leader and
  appended on every in-sync replica, and the ISR has at least `min.insync.replicas` members. It
  survives the loss of any replicas as long as one ISR member remains.
- **No phantom reads.** Consumers only read below the high watermark, so they never see a record
  that is later truncated away.
- **Ordering.** Offsets within a partition are dense and ordered. HLC timestamps are unique and
  strictly increasing within a partition, including across leader changes.
- **Idempotence.** Producers are idempotent by default: retries never duplicate or reorder
  records within a partition.
- **Fencing.** A leader cut off from the controller stops accepting writes before a replacement
  can be elected, and a stale leader's unacknowledged tail is truncated by epoch when it rejoins.

Out of scope: transactions, consumer group rebalancing, security, a highly available controller
([ADR 5](docs/adr/0005-single-controller.md)), and unclean leader election.

## Testing

- Unit and integration tests in every module. Line coverage is enforced at 80% for
  `log-storage`, `concurrency`, `clock` and `replication` (currently 86–93%).
- [jcstress](https://github.com/openjdk/jcstress) tests for every lock-free or volatile-published
  structure: the MPSC queue, log end offset and index publication, delayed operation completion,
  HLC uniqueness and high watermark advancement. `./gradlew :stress-tests:jcstress`.
- A chaos suite (`chaos-tests`) runs real brokers in one JVM behind TCP proxies that can
  partition, delay or drop links. Scenarios: kill the leader during `acks=all`, partition a
  follower, a zombie leader cut off from the controller, ±300 ms clock skew, and retry storms.
  Every scenario records what was acknowledged and what consumers read, then checks that no
  acknowledged record is lost and no record a consumer saw ever disappears.

## Benchmarks

`./gradlew :benchmarks:loadgen -Ploadgen.args="seconds=10"` starts a controller and three brokers
in one JVM, creates a 6-partition topic with replication factor 3, and runs four idempotent
producers. Throughput is measured unthrottled; latency is measured separately at a fixed
20,000 msg/s so it reflects the configuration rather than queueing. 100-byte records, Apple M2,
8 cores, 16 GB, APFS on the internal SSD.

| configuration | msgs/sec | MB/s | p50 | p99 | p99.9 |
|---|---:|---:|---:|---:|---:|
| acks=1, group commit, virtual threads | 631,202 | 60.2 | 10.8 ms | 62.2 ms | 126.4 ms |
| acks=all, group commit, virtual threads | 414,812 | 39.6 | 11.5 ms | 35.2 ms | 56.7 ms |
| acks=1, no fsync, virtual threads | 1,454,822 | 138.7 | 3.15 ms | 24.2 ms | 39.4 ms |
| acks=all, no fsync, virtual threads | 1,796,380 | 171.3 | 3.27 ms | 9.66 ms | 17.1 ms |
| acks=1, group commit, 8 platform threads | 719,454 | 68.6 | 10.4 ms | 28.7 ms | 39.0 ms |
| acks=all, group commit, 8 platform threads | 779,134 | 74.3 | 11.6 ms | 32.4 ms | 42.2 ms |

What the numbers say:

- **Group commit** is the dominant cost. Turning fsync off roughly doubles throughput and cuts
  p50 from about 11 ms to about 3 ms; the remaining 3 ms is mostly the producer's 5 ms linger.
  Before fsyncs within a round were issued in parallel, group commit was several times slower,
  because six partitions meant six serial fsyncs per round.
- **acks=1 vs acks=all** differ less than you might expect at p50, because followers long-poll
  and receive new data within a millisecond of the append. The difference shows up in the tail
  and under saturation.
- **Virtual vs platform handlers** make no consistent difference here. Four producers means
  four connections and little handler concurrency; the run-to-run variation (around 20% on a
  laptop that is also running everything else) is larger than the gap. Virtual threads matter
  when there are many connections, each with requests parked on fsync or the purgatory.

All three brokers share one machine and one SSD, so these are comparisons between
configurations, not absolute capacity numbers. JMH microbenchmarks for the log, queue and timer
are in `benchmarks/` (`./gradlew :benchmarks:jmh`).

## Building

Requires JDK 21 (Gradle will download one through the toolchain resolver if needed).

```sh
./gradlew build                                   # compile, test, coverage checks
./gradlew :stress-tests:jcstress -Pjcstress.mode=quick
./gradlew :chaos-tests:test
```

## Docs

- [Design](docs/design.md)
- [Threading model](docs/threading-model.md)
- Architecture decision records
  - [1. One writer thread per partition](docs/adr/0001-single-writer-partitions.md)
  - [2. Group commit for leader durability](docs/adr/0002-group-commit.md)
  - [3. Hybrid logical clock timestamps](docs/adr/0003-hlc-over-wall-clock.md)
  - [4. Leader epochs for truncation](docs/adr/0004-leader-epochs-for-truncation.md)
  - [5. A single controller](docs/adr/0005-single-controller.md)
  - [6. Leaders fence themselves with a lease](docs/adr/0006-lease-based-fencing.md)
