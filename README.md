# raft-kv-store

A key-value store that runs on several machines at once and keeps working when some of them
fail. It implements the Raft consensus algorithm from scratch in Go.

Think of it as a hash map (`set foo = bar`, `get foo`) that you can spread across 5 servers.
You can kill any server, including the one in charge, and the cluster keeps serving reads and
writes without losing any data you were told it saved.

```
5-node cluster. ~8,000 writes/sec. ~61,000 reads/sec. Sub-10ms median latency.
Zero loss of committed data when a leader is killed. (Measured locally, see Benchmarks.)
```

## What is Raft, in plain terms

Say you store your data on one server. If that server dies, your data is gone and your app is
down. So you copy the data onto several servers. Now you have a new problem: when a write comes
in, how do all the copies agree on what happened and in what order, even while servers are
crashing and restarting? If they disagree, two clients can read two different answers, or a
write you thought was saved can quietly disappear.

Raft is a set of rules that makes a group of servers agree. The short version:

1. **One server is the leader.** The others are followers. Only the leader accepts writes.
2. **The leader writes things down in order.** Every write becomes a numbered entry in a log,
   like a shared to-do list. The leader sends each new entry to the followers.
3. **A write only counts once a majority has it.** With 5 servers, that means at least 3. Once
   3 of them have written the entry down, it is "committed" and can never be undone. Only then
   does the leader tell the client "done."
4. **If the leader dies, the followers hold an election.** They wait a short random time, and
   whoever speaks up first and has an up-to-date log becomes the new leader. The random wait is
   what stops everyone from nominating themselves at the same instant.
5. **The new leader is guaranteed to have every committed write.** This falls out of the math:
   a committed write is on a majority, and you cannot win an election without a majority voting
   for you, and any two majorities share at least one server. So the winner always already has
   the committed data.

That is the whole idea. The actual key-value store is just "apply the committed log in order."
Agreeing on the log is the hard part, and that is what Raft handles.

If you want the deep version (every rule, the edge cases, the bugs I hit), read `SPEC.md`.

## What it does

- **Leader election.** Randomized timeouts, terms, and `RequestVote`. One leader at a time, and
  automatic re-election when a leader fails.
- **Log replication.** The leader appends writes and copies them to followers with a consistency
  check, commits once a majority agrees, and applies them to the store in order.
- **Two read modes**, chosen per node:
  - `--read-mode leader` (default): the leader answers reads, but only after checking it still
    has majority support. Always returns the latest value (linearizable). Clients route to the
    leader automatically.
  - `--read-mode follower`: any node answers from its own copy. Much higher read throughput, but
    a read can be slightly behind the latest write. (See Design notes.)
- **Snapshots and log compaction.** The log cannot grow forever, so nodes periodically snapshot
  the store and throw away the old log. A node that has fallen too far behind catches up by
  receiving a snapshot.
- **Durability.** State is written to disk (an append-only log plus snapshots), so a node
  recovers its data after a restart.
- **Fault tolerance.** Kill or restart any node. The cluster re-elects a leader and recovers
  with no loss of committed writes.
- **Dashboard.** A live web view of every node: its role, term, commit position, snapshot point,
  and key count, plus a write box that routes to the leader.

## How it is built

```
proto/       gRPC service definitions and generated Go (the client KV API plus internal Raft RPCs)
server/      the Raft core (no networking in it), the KV store, and the gRPC and HTTP servers
client/      a CLI that automatically follows the leader
benchmark/   a load generator that reports throughput and latency percentiles
dashboard/   a static HTML/JS page that polls each node's HTTP endpoint
scripts/     chaos.ps1 (fault injection) and bench.ps1 (benchmark runner)
docker-compose.yml   a 5-node cluster plus the dashboard
```

The Raft core in `server/raft.go` does not know about sockets. It talks to other nodes through a
`Transport` interface. Production uses a gRPC transport. The tests use an in-memory one that can
drop messages to a chosen node, which is how partitions and crashes are tested in-process without
any real network.

Each node speaks two protocols: gRPC for node-to-node Raft messages and the client API, and an
optional HTTP/JSON endpoint for the browser dashboard (the HTTP port is the gRPC port minus 1000).

## Quick start (local, no Docker)

Needs Go 1.26 or newer. `protoc` is only needed if you regenerate the protobufs.

```bash
# build
go build -o bin/raftnode ./server
go build -o bin/raftctl  ./client

# start 3 nodes
./bin/raftnode --id n1 --addr :9001 --advertise localhost:9001 --peers localhost:9002,localhost:9003 --data-dir ./data/n1
./bin/raftnode --id n2 --addr :9002 --advertise localhost:9002 --peers localhost:9001,localhost:9003 --data-dir ./data/n2
./bin/raftnode --id n3 --addr :9003 --advertise localhost:9003 --peers localhost:9001,localhost:9002 --data-dir ./data/n3

# use it (writes go to the leader automatically)
./bin/raftctl -addr :9001 set foo bar
./bin/raftctl -addr :9001 get foo        # -> bar
./bin/raftctl -addr :9001 status
```

## Cluster plus dashboard (Docker)

```bash
docker compose up --build
# open http://localhost:8080
```

Five nodes (gRPC `:9001-9005`, HTTP `:8001-8005`) plus the dashboard on `:8080`. Stop a container
(`docker compose stop node3`) and watch the dashboard re-elect and recover in real time.

## Fault injection

```powershell
pwsh scripts/chaos.ps1 -Rounds 10
```

Starts a 5-node cluster, writes some canary data, then repeatedly kills and restarts random
nodes. After every round it checks that the data is still readable and a leader has re-formed. It
exits with an error if any committed data was lost.

## Benchmarks

```powershell
pwsh scripts/bench.ps1 -Writers 24 -Readers 16 -Duration 5s   # -> benchmark/report.txt
```

Measured on a 5-node cluster with persistence on, single dev machine, follower reads.
Numbers vary by run and machine load, so these are ranges across several runs:

| Metric | Result |
|--------|--------|
| Writes | ~8,000 to 10,000 ops/sec, p50 ~2 ms, p99 ~18 ms |
| Reads  | ~60,000 to 93,000 ops/sec, p50 under 1 ms |
| Leader-kill recovery | re-elects and resumes writes within about 1 second |

Writes are limited by the round trip to a majority plus writing to disk. Reads scale with the
number of nodes in follower mode.

These numbers depend heavily on the machine, because all five nodes run on one box over loopback
and share its CPU and SSD. Measured on:

- AMD Ryzen 7 260 (8 cores / 16 threads, 3.8 GHz base)
- 16 GB DDR5-5600
- 512 GB SK Hynix NVMe SSD

The core count sets the ceiling, since the five nodes and the load generator all compete for cores,
and reads in particular are CPU-bound on gRPC and JSON work. The NVMe SSD keeps write latency low
because every committed write appends to an on-disk log. Expect lower throughput on fewer cores or a
slower disk. On separate physical machines, network round-trips would dominate write latency instead
of the local CPU and disk.

## Control dashboard (benchmark and chaos in the browser)

A small Go server (`control/main.go`) runs the benchmark and chaos harnesses from a web UI
instead of the terminal. It launches the PowerShell harnesses as child processes and streams
their output to the page over Server-Sent Events, since a browser cannot spawn a process or
speak gRPC on its own. The page shows live throughput and latency, a pass/fail strip for chaos
rounds, a node grid that updates while the cluster runs, and the raw log.

```powershell
go build -o bin/raftnode.exe    ./server
go build -o bin/raftctl.exe     ./client
go build -o bin/raftbench.exe   ./benchmark
go build -o bin/raftcontrol.exe ./control
./bin/raftcontrol.exe   # serves http://localhost:8090
```

Open `http://localhost:8090`, set the parameters (writers, readers, duration, snapshot
threshold, or chaos rounds), and run. One run executes at a time, and Stop kills it and clears
any leftover nodes. A low chaos snapshot threshold (say 25) forces aggressive log compaction,
which is the path that stresses snapshot and restore the most.

A run usually reports a small number of write/read errors, about one per worker thread
(for example 24 errors with 24 writers). That is expected and benign: each worker's first
request can land before it has routed to the current leader, so it fails once, re-routes via
the leader hint, and then succeeds for the rest of the run. It is the client correctly
finding the leader, not lost or corrupted data.

## Design notes

- **Reads: leader vs any node.** Leader reads always return the latest value but go through one
  node and need a leadership check first. Follower reads spread across all nodes and go faster,
  but can return a value that is a little stale. Both are built in and selectable per node.
- **Why committed data is safe.** A new leader commits a no-op entry at the start of its term, so
  entries from earlier terms commit along with it (Raft section 5.4.2). An entry counts as
  committed only once it is on a majority and from the current term, which prevents a committed
  entry from being lost after a leader change.
- **How durable it is.** State is an append-only log plus snapshots. Writes reach the operating
  system (so they survive a process crash, which is what the chaos test exercises) but are not
  flushed to the physical disk on every write. Surviving a sudden power loss would need a grouped
  disk flush, which is a latency tradeoff I chose not to take.
- **Why this beats a naive disk format.** The first version rewrote the entire state on every
  write, which was quadratic and ran at about 98 writes/sec. Switching to an append-only log
  brought it to about 7,900 writes/sec.

## Common questions

- **Why Raft and not Paxos?** Raft was designed to be understandable. It has an explicit leader
  and splits the problem into election, replication, and safety, which makes it much easier to
  build correctly and to reason about.
- **What about a network split (two halves that cannot talk)?** A leader needs a majority to
  commit. The smaller half cannot elect a leader or commit anything, and the old leader stranded
  there cannot commit either, so you never get two leaders both accepting writes. The leadership
  check also stops a stranded leader from answering reads with stale data.
- **What if the leader crashes mid-write?** A write that had not committed yet may be dropped, and
  the client retries. A write that had committed survives, because it is on a majority and the
  next leader is guaranteed to have it. The zero-data-loss fault test confirms this.

## Testing

```bash
go test ./...      # election, replication, snapshot and InstallSnapshot, persistence, fault
```

Consensus is tested over the in-memory transport (partitions, crashes, restarts) plus gRPC
service tests over `bufconn`. (The race detector needs cgo and a C compiler, so the tests
otherwise run without it.)
