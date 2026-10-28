# 3. Hybrid logical clock timestamps instead of wall clock

Status: accepted

## Context

Records carry a timestamp, and consumers use it for `seekToTimestamp` and for reasoning about
order across partitions. Wall clock timestamps go backwards when NTP steps the clock, differ
between brokers by however much the clocks are skewed, and are not unique. After a leader change
the new leader's clock may be behind the old one, so timestamps inside one partition stop being
monotonic and the time index stops being searchable.

## Decision

Each broker runs a hybrid logical clock (Kulkarni et al.). A timestamp is a 64-bit value: the
upper 48 bits are physical milliseconds, the lower 16 a logical counter.

- Every append reserves one tick per record, so every record in the cluster has a unique
  timestamp and timestamps within a partition strictly increase with offset.
- Producers run their own HLC, stamp each batch with it and advance it from every ack. The
  broker treats the batch timestamp and every replicated batch as a receive event and moves its
  clock past them.
- A new leader's first append is forced past the last timestamp in the log, so leader changes
  never move time backwards.
- A remote timestamp more than `hlc.max.drift.ms` ahead of local physical time is not adopted;
  it is counted and ignored, which stops one broker with a bad clock from dragging everyone
  else's clocks into the future.

## Consequences

- The time index can use plain binary search, and `offsetForTimestamp` is exact.
- Causality is preserved through the producer: once a write to one partition is acknowledged,
  anything that producer writes afterwards, to any partition on any broker, gets a larger
  timestamp, even if that broker's wall clock is behind (by less than the drift bound).
- Timestamps stay within about `max.drift` of real time, which is what users expect when they
  seek by time.
- A record's timestamp is assigned by the broker, not the producer. Producer-supplied times are
  only used as hints to advance the clock.
- 16 bits of logical counter allow 65,536 records per millisecond per partition before the clock
  runs ahead of physical time. It catches up once load drops.
