# 5. A single controller, and no controller high availability

Status: accepted (non-goal)

## Context

Something has to decide who leads each partition and who is in its ISR. Kafka uses a Raft
quorum (KRaft) for this; earlier versions used ZooKeeper. Building a consensus layer is a project
of its own and the spec rules out using a Raft library.

## Decision

There is exactly one controller process. It stores its state (brokers, topics, partition
leaders, ISRs, epochs) in a single file written atomically. It is not replicated. If it dies,
it is restarted and reloads that file.

Brokers heartbeat to the controller and treat the heartbeat as a lease (see ADR 6).

## Consequences

- No consensus code, and the controller logic is small enough to reason about directly.
- The controller is a single point of failure for metadata. While it is down, no leader can be
  elected, no ISR can change, and no topic can be created.
- Because of the lease, leaders also stop accepting writes after roughly 75% of the session
  timeout without a controller. Reads of committed data keep working. Making the controller
  highly available is explicitly out of scope; the obvious path would be a Raft-replicated
  metadata log, as in KRaft.
