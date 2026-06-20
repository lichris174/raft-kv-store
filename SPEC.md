# Design notes

A distributed, fault-tolerant key-value store implementing the Raft consensus
algorithm from scratch in Go. The aim was a working multi-node cluster with a
live web dashboard showing leader election, nodes going up and down, and writes
replicating across the cluster.

## Scope

In:

- Leader election, log replication, and safety under network partition.
- A 5-node cluster runnable locally via Docker Compose.
- A client API: writes route to the leader, reads are configurable (leader-only
  or any-node).
- Snapshotting and log compaction so the log does not grow without bound.
- Fault injection: kill and restart nodes and watch the cluster recover.
- A benchmark harness reporting write/read throughput and recovery time.
- A dashboard showing node roles, terms, and live writes.

Out:

- Cross-host and cloud deployment. The cluster runs locally.
- Authentication, authorization, and TLS between nodes.
- Multi-Raft and sharding. There is one replicated state machine.
- Anything fancier than an append-only log plus a snapshot file for storage.

## Stack

- Go for the Raft and KV logic.
- gRPC and Protocol Buffers for inter-node RPC (RequestVote, AppendEntries,
  InstallSnapshot) and the client API.
- Docker and Docker Compose for the multi-node cluster.
- Plain HTML and JS (no framework) for the dashboard, which polls a status
  endpoint and renders cluster state.
- The Go standard `testing` package for unit and integration tests.

## Layout

```
/proto       .proto definitions and generated Go
/server      Raft implementation, KV state machine, gRPC and HTTP servers
/client      CLI client (get/set/del/status)
/dashboard   static web UI
/benchmark   load generator
docker-compose.yml
Dockerfile
```

## Read semantics

Both modes are implemented and chosen per node with `--read-mode`:

- `leader` (default): reads are served by the leader, which first confirms it
  still has majority support (a read-index check) before answering. This is
  linearizable but funnels reads through one node.
- `follower`: any node answers from its local state. Higher throughput, but a
  read can return a value that is slightly stale, bounded by replication lag.

## RPC surface

Node-to-node:

- `RequestVote(term, candidateId, lastLogIndex, lastLogTerm) -> (term, voteGranted)`
- `AppendEntries(term, leaderId, prevLogIndex, prevLogTerm, entries[], leaderCommit) -> (term, success)`
- `InstallSnapshot(term, leaderId, lastIncludedIndex, lastIncludedTerm, data) -> (term)`

Client-facing:

- `Get(key) -> (value, found, leaderHint)`
- `Set(key, value) -> (ok, leaderHint)` (a non-leader returns NOT_LEADER plus a hint)
- `Delete(key) -> (ok, leaderHint)`
- `Status() -> (nodeId, role, term, commitIndex, lastApplied, leaderId, peers[])`

## How it was built

The cluster came together in layers, each working and tested before the next:

1. A single-node skeleton: gRPC service, in-memory KV, Get/Set/Delete/Status, no Raft.
2. Leader election: terms, randomized election timeout, heartbeats, RequestVote.
3. Log replication: the leader appends and copies entries to followers,
   commitIndex advances on majority, entries apply to the state machine in order.
4. Client routing: Set routes to the leader via the NOT_LEADER hint, reads honor
   `--read-mode`.
5. Snapshotting and compaction: snapshot the state machine, truncate the log, and
   ship a snapshot to a follower that has fallen too far behind.
6. Fault injection and recovery: scripted kill and restart, verifying no committed
   write is ever lost.
7. A benchmark harness measuring write and read throughput and recovery time.
8. The dashboard, polling Status across nodes and rendering roles, terms, and writes.

## Correctness bar

Each layer had to compile, pass `go vet`, pass its own tests, and not break the
tests that came before it.
