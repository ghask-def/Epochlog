# 6. Leaders fence themselves with a heartbeat lease

Status: accepted

## Context

When a leader is cut off from the controller, the controller eventually elects a new leader. The
old leader does not know this yet. If it keeps accepting `acks=1` writes, those writes are lost
when it rejoins and truncates. With `acks=all` the old leader cannot advance the high watermark
once its followers move to the new leader, but clients still see timeouts instead of a fast
`NOT_LEADER_OR_FOLLOWER`.

## Decision

The controller declares a broker dead `broker.session.timeout.ms` after the last heartbeat it
received. The broker measures its lease from the moment it *sent* the last heartbeat that was
acknowledged, and considers the lease valid for 75% of the session timeout. A produce request on
a partition it leads is rejected with `NOT_LEADER_OR_FOLLOWER` once the lease has expired.

Measuring from send time makes the broker's view conservative: the controller cannot have
received that heartbeat before it was sent, so the broker's lease always ends before the
controller's timer does, as long as clock rates are roughly equal. Durations are measured with
`System.nanoTime`, so wall-clock skew does not matter, only drift in clock rate.

## Consequences

- A partitioned leader stops accepting writes before a new leader can be elected. The zombie
  leader chaos test checks that a client pinned to the old leader gets no acknowledgements.
- A controller outage makes every partition read-only after the lease expires (ADR 5).
- A long GC pause or a suspended VM can still produce a short overlap; the lease narrows the
  window but leader epochs (ADR 4) are what guarantee the stale writes get truncated.
