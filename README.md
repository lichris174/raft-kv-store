# raft-kv-store

A replicated key-value store built on my own implementation of the Raft consensus algorithm,
written in Go. Five nodes keep identical copies of the data. Any node can be killed, the leader
included, and the cluster keeps serving reads and writes. No write it acknowledged is lost.

On one laptop with five nodes, it does about 12,000 writes/s and 34,000 reads/s, with a median
write latency around 1.3 ms. There is also a web dashboard where you can kill nodes and watch
the cluster recover.

## How Raft works here

One node is the leader and the rest are followers. Every write goes to the leader, which adds it
to an ordered log and sends it to the followers. A write is committed once a majority of nodes
(3 of 5) has it on disk. Only then does the client get an answer.

If the leader stops sending heartbeats, a follower waits a random timeout and asks the others to
vote for it. A node only votes for a candidate whose log is at least as complete as its own.
Every committed write is on a majority, and any two majorities overlap, so a new leader always
already has every committed write.

Applying the log to the hash map is the easy part. Getting five machines to agree on the log
while they crash and restart is what most of the code is for.

## Features

- Leader election with randomized timeouts. Before a real election, a node holds a *pre-vote*:
  it asks whether it would win, without changing any state. A node that was cut off from the
  others therefore can't push the term up and depose a working leader when it reconnects. A
  leader that loses contact with its majority steps down by itself (check-quorum).
- Log replication with Raft's consistency check. Each follower gets at most one request in
  flight, and writes that pile up meanwhile go out together in the next batch.
- Two read modes, set per node:
  - `--read-mode leader` (default): linearizable. Before answering, the leader confirms it is
    still leader (read-index).
  - `--read-mode follower`: any node answers from its own copy. Faster, but a read can lag the
    latest write slightly.
- Snapshots and log compaction. A follower that falls behind the leader's snapshot is sent the
  snapshot instead of the old log.
- Persistence: an append-only log plus snapshot files, so a restarted node comes back with its
  data. The file formats survive a crash at any point of a write; the tests cover each case.
- A dashboard for starting, killing and restarting nodes and for running the load and fault
  tests from the browser.

## Layout

```
proto/       gRPC definitions (client API and Raft RPCs) and generated code
server/      the node: Raft (raft.go, election.go, replication.go, readindex.go),
             persistence, the KV store, gRPC and HTTP servers
client/      raftctl, a CLI that follows leader redirects
benchmark/   raftbench, a load generator
control/     raftcontrol, the local server behind the dashboard
dashboard/   the web UI (plain HTML/CSS/JS, no build step)
scripts/     chaos.ps1 (fault test), bench.ps1 (benchmark), lib.ps1 (shared helpers)
docs/        HARDENING.md and RELIABILITY.md: bugs found and how they were fixed
```

The Raft code never touches a socket. It sends messages through a `Transport` interface. In
production that is gRPC. The tests use an in-memory network that can partition nodes, crash and
restart them, and drop or delay messages.

Each node serves gRPC (Raft traffic and the client API) and, optionally, a small HTTP API for the
dashboard and scripts, on the gRPC port minus 1000:

| Endpoint | Method | |
|---|---|---|
| `/status` | GET | role, term, commit index, key count (`?digest=1` adds a hash of the data) |
| `/get?key=K` | GET | read a key |
| `/set?key=K&value=V` | POST | write a key |
| `/del?key=K` | POST | delete a key |

Writes must be POSTs, and browser requests are only accepted from pages served on the same
machine.

## Running it

Requires Go 1.26+. You only need `protoc` if you change `proto/raft.proto`.

```powershell
go build -o bin/raftnode.exe    ./server
go build -o bin/raftctl.exe     ./client
go build -o bin/raftbench.exe   ./benchmark
go build -o bin/raftcontrol.exe ./control
```

### Dashboard

```powershell
./bin/raftcontrol.exe        # then open http://127.0.0.1:8090
```

Press **Start cluster** to launch five nodes. Things to try:

1. **Write 100 random keys**: every node's log bar grows, then gets folded into a snapshot.
2. **Kill** the leader. The banner turns to Degraded, and within about a second another node
   takes over. Writes keep working.
3. **Start** the killed node again. It reloads its data from disk and catches up.
4. **Read from** a specific node to check that each copy has the key.

Kill is a real process kill, the same as a crash. The two test panels at the bottom run the
fault test and the benchmark described below and show their results. A test run starts its own
cluster on the same ports, so the demo cluster is stopped while it runs.

The control server listens only on 127.0.0.1, accepts changes only as POSTs from pages on this
machine, and keeps node data in `%TEMP%\raft-kv-store\demo`. Don't keep node data in OneDrive or
Dropbox: sync clients briefly lock files while a node replaces them.

### By hand

```powershell
./bin/raftnode --id n1 --addr :9001 --advertise localhost:9001 --peers localhost:9002,localhost:9003 --data-dir data/n1 --http :8001
./bin/raftnode --id n2 --addr :9002 --advertise localhost:9002 --peers localhost:9001,localhost:9003 --data-dir data/n2 --http :8002
./bin/raftnode --id n3 --addr :9003 --advertise localhost:9003 --peers localhost:9001,localhost:9002 --data-dir data/n3 --http :8003

./bin/raftctl -addr :9001 set foo bar     # sent on to the leader if :9001 isn't it
./bin/raftctl -addr :9001 get foo
./bin/raftctl -addr :9001 status
```

### Docker

```bash
docker compose up --build    # five nodes, dashboard on http://localhost:8080
```

`docker compose stop node3` takes a node down. Under Docker the page shows the cluster and has
the read/write box, but the kill buttons and test panels need `raftcontrol`.

## Fault test

```powershell
powershell -File scripts\chaos.ps1 -Rounds 10 [-KillLeader] [-Seed N]
```

The script starts five nodes and writes 30 keys. Each round:

1. It kills one node: a random one, or the current leader with `-KillLeader`.
2. It checks that the survivors elect a leader and accept a new write.
3. It restarts the node it killed.
4. It reads back every write the cluster acknowledged.

At the end, every node must report the same data hash. Each round prints one result line
(`ROUND 3 RESULT=OK ...`). A failed round says what went wrong, and node logs are kept in
`%TEMP%\raft-kv-store\chaos\logs`. The seed is printed at the start, so a failing run can be
replayed with the same sequence of kills.

## Benchmark

```powershell
powershell -File scripts\bench.ps1 -Writers 24 -Readers 16 -Duration 5s
```

Results go to `benchmark/report.txt`. Five runs on a 5-node cluster with persistence and
follower reads:

| | Throughput | p50 | p99 | Errors |
|---|---|---|---|---|
| Writes | 12,075 – 12,819 ops/s | 1.25 ms | ~19 ms | 0 |
| Reads | 32,943 – 35,000 ops/s | 0.55 ms | ~1.6 ms | 0 |

After the leader is killed, writes resume within about a second.

Machine: AMD Ryzen 7 260 (8 cores/16 threads), 16 GB DDR5-5600, NVMe SSD. All five nodes and the
load generator share it, so the CPU sets the ceiling. On separate machines, network round trips
would dominate write latency instead.

Any failed operation is listed with its reason, and the benchmark then exits non-zero.

## Durability tradeoff

By default a write is acknowledged once it has reached the operating system. That survives a
process crash, which is what the fault test exercises. It does not survive a sudden power loss.
`--fsync` makes every node flush to disk before acknowledging, at a large cost in write
throughput (about 7x in an earlier measurement). Measure it on your machine with
`powershell -File scripts\bench.ps1 -Fsync`.

An early version rewrote the whole state file on every write and managed about 98 writes/s.
Switching to an append-only log is what made the current numbers possible.

## Tests

```powershell
go test ./...                                          # about 25 s
$env:RAFT_FUZZ_SEEDS=30; go test ./server -run Fuzz    # more randomized fault runs
```

- **Cluster tests.** Real nodes run over the in-memory network. They check:
  - elections, including one- and two-node clusters;
  - that a minority partition can't elect a leader or commit;
  - that divergent logs are repaired to match the leader's;
  - that an old-term entry isn't committed just by counting copies (figure 8 of the Raft paper);
  - pre-vote, check-quorum, read-index, snapshots, and recovery from disk.
- **Randomized fault test.** Three clients write while the network partitions and heals,
  nodes crash and restart, and 5% of messages are dropped. Two things are checked
  continuously: no term ever has two leaders, and every node applies the same entry at each
  index. At the end, every acknowledged write must be on all five nodes.
- **Storage tests.** These cover:
  - a crash between the files a compaction rewrites;
  - torn and partial log writes, and corrupt metadata;
  - a disk error at each point where Raft writes.

Each safety fix was checked by putting the bug back and confirming that a test fails.
`docs/RELIABILITY.md` lists them. The Go race detector needs a 64-bit C compiler for cgo, which this machine doesn't have,
so it isn't part of the runs above.
