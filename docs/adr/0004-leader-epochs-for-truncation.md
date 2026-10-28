# 4. Leader epochs for log truncation

Status: accepted

## Context

When a follower restarts or a new leader is elected, a replica may hold records the new leader
does not have: records that were written by an old leader but never committed. Kafka originally
truncated followers to their high watermark, which loses committed data in some double-failure
sequences and can leave replicas permanently diverged (KIP-101).

## Decision

Every batch carries the leader epoch it was written in, and each log keeps a leader epoch cache
mapping `epoch -> start offset`, persisted in a checkpoint file. Before fetching, a follower that
has just become a follower sends `OffsetForLeaderEpoch` with its latest epoch. The leader answers
with the end offset of that epoch in its own log (or of the largest epoch below it). The follower
truncates to the smaller of that offset and its own end for that epoch, then repeats with the new
latest epoch until the answer stops changing.

Leader and follower state changes are tagged with the leader epoch; responses from a fetch issued
under an older epoch are discarded.

## Consequences

- Followers never truncate committed records, and divergent tails from zombie leaders are
  removed. The chaos tests `KillLeaderTest` and `ZombieLeaderTest` check both.
- Truncation can take several round trips when a replica missed multiple epochs. In practice
  this is rare and cheap.
- The epoch cache has to be truncated alongside the log, and rebuilt from batch headers when the
  checkpoint is missing or stale after a crash.
