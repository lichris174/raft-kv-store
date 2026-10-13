# Hardening log: Raft safety and durability fixes

This documents a code audit of the project and the first round of fixes that came
out of it: what was found, how each problem was reproduced before it was fixed,
what the fix is and why it is correct, and how it was verified. Branch:
`fix/raft-safety-and-durability`.

## Method

1. **Audit.** Read every source file, ran `go build`, `go vet`, the full test suite
   and a live chaos run to establish a baseline (all green).
2. **Triage.** The audit found six correctness problems in the Raft core and
   persistence layer, plus scalability, security and polish items. This round
   fixes the correctness ones. The rest is listed under [Not done yet](#not-done-yet).
3. **Test first.** For every bug, a test was written that fails on the old code for
   the stated reason. The failing output was observed (RED) before any fix was
   written, so each test is known to detect its bug rather than merely pass.
4. **Fix**, re-run everything (GREEN), then re-verify against the real cluster with
   chaos and the benchmark.

The baseline suite had 20 tests. It is now 41, none removed.

## Fixes

### Step 1: Raft safety

#### 1.1 A heartbeat could commit entries nobody verified

**Where:** `Raft.AppendEntries`, called by `Raft.ConfirmLeadership` (leader reads).

**Problem.** `ConfirmLeadership` sends `AppendEntries` with `PrevLogIndex=0` and
the leader's `LeaderCommit`. With prev index 0 the log-consistency check passes
trivially, and the follower then set `commitIndex = min(leaderCommit, ownLastIndex)`
and applied those entries. A follower holding uncommitted entries from a previous
leader (which may differ from the new leader's log) would apply them to its state
machine. That violates Raft's State Machine Safety property.

**Fix.** Raft figure 2 says a follower sets
`commitIndex = min(leaderCommit, index of last new entry)`, where "last new entry"
means the last entry *this message* verified: `PrevLogIndex + len(Entries)`. The
follower now uses that bound. The old bound (its own last index) is kept as a
second cap. Normal replication is unaffected because regular heartbeats carry a
matching prev entry.

**Tests:** `TestAppendEntries_UnverifiedHeartbeatDoesNotCommit` (RED: commit=2,
want 0), `TestAppendEntries_VerifiedHeartbeatCommitsUpToPrev` (RED: commit=3, want 2).

#### 1.2 A stale snapshot rolled the state machine back

**Where:** `Raft.InstallSnapshot`.

**Problem.** The handler only ignored snapshots at or below `snapshotIndex`. A
follower that had *applied* entries past the snapshot index (applied but not yet
compacted) would still call `Restore`, replacing its state with the older snapshot.
`lastApplied` stayed high, so the entries in between were never re-applied: keys
silently disappeared.

**Fix.** Ignore any snapshot with `LastIncludedIndex <= lastApplied`; the state
machine already contains everything it describes.

**Related bug found while fixing this (and caught by a new test).** Raft's
InstallSnapshot rule 6 says to keep the log tail after the snapshot only if the
local entry at `LastIncludedIndex` has the snapshot's term; otherwise the whole
log is from a different history and must be discarded. The old code always kept
the tail. While adding that check, the first version looked the entry up *after*
`snapshotIndex` had already been advanced, so the lookup always missed and the
tail was always dropped. `TestInstallSnapshot_TermMatchKeepsLogTail` (RED:
logLen=0, want 1) caught it, and the lookup now happens before the index moves.
This is a good example of why the matching-case test was worth writing even though
the mismatch-case test was already green.

**Tests:** `TestInstallSnapshot_StaleSnapshotDoesNotRollBackState`,
`TestInstallSnapshot_TermMismatchDropsLogTail`, `TestInstallSnapshot_TermMatchKeepsLogTail`.

#### 1.3 Disk write failures were logged and then acknowledged

**Where:** `appendLogLocked`, `saveMetaLocked`, `rewriteLocked` in `raft.go`.

**Problem.** Each helper logged `WARN` on error and returned nothing. A node whose
disk write failed still acknowledged the entry or granted the vote. Raft's safety
argument depends on a node never acknowledging state that is not durable, so this
could lose committed data or allow two votes in one term after a restart.

**Fix.** The helpers return their error, and every call site now picks one of
three policies depending on whether the in-memory change can be undone:

| Call site | Policy | Reason |
|---|---|---|
| `AppendEntries` clean append | Roll back the in-memory append, return the error | The leader sees an RPC error and retries; nothing false was acked |
| `Propose` (leader's own append) | Persist first, only then add to the log and replicate; return the error | A leader must never replicate an entry it has not stored |
| `becomeLeader` no-op | Decline leadership, stay follower | Leader that cannot write its log must not lead |
| `RequestVote` | Revert `votedFor`, refuse the vote, return the error | An unrecorded vote could be granted twice |
| `startElection` | Restore old term and vote, skip the campaign | Never campaign on a term we may forget |
| `AppendEntries` after a conflict truncation, `InstallSnapshot`, term change in `becomeFollower`, failed `Load` | **Crash-stop** via `fatalPersist` | The in-memory change cannot be rolled back; a crash is always safe in Raft, a false ack is not |
| Compaction in `maybeSnapshot` | Log and retry later | Safe: the old files still hold every applied entry |

`fatalPersist` calls `Config.OnPersistFatal` if set (tests use this) and
otherwise `log.Fatalf`.

**Tests:** `TestAppendEntries_PersistFailureRejectsAndRollsBack`,
`TestRequestVote_PersistFailureDoesNotGrantVote`, `TestPropose_PersistFailureRollsBack`,
`TestAppendEntries_TruncationRewriteFailureIsFatal`, all driven by a
`failingPersister` that injects disk errors.

### Step 2: Persistence crash safety

#### 2.1 Compaction wrote the three files in a lossy order

**Where:** `filePersister.Rewrite`.

**Problem.** Compaction writes `meta.json` (which records the snapshot index),
`snapshot.bin`, and `log.jsonl`. The old order wrote meta first. A crash after
meta but before the snapshot left meta claiming snapshot index N while
`snapshot.bin` still held an older state. On restart the node restored the old
snapshot, set `lastApplied = N`, and skipped entries it never applied. The chaos
harness runs with a snapshot threshold of 25 and kills nodes at random, so it hits
this window constantly in principle.

**Fix.** Two changes together:

1. **Self-describing snapshots.** `snapshot.bin` now starts with a header: magic
   `RKVSNAP1`, then the last included index and term (big endian), then the
   payload. `Load` uses the snapshot's own index when it is newer than
   `meta.json`'s.
2. **Safe write order:** snapshot, then log, then meta.

Every crash point now leaves a state `Load` can reconcile:

| Crash after | On disk | Load result |
|---|---|---|
| nothing | all old | previous state |
| snapshot | new snapshot, old log, old meta | snapshot header wins; old log is a superset, covered entries are dropped |
| snapshot + log | new snapshot, new log, old meta | same |
| all three | all new | new state |

If `snapshot.bin` is *older* than `meta.json` (which the new order cannot produce)
`Load` returns an error instead of guessing.

**Compatibility.** A `snapshot.bin` without the header (written by an older build)
is read as before, with the index taken from `meta.json`
(`TestLoad_LegacyHeaderlessSnapshot`). Existing data directories keep working and
upgrade to the new format at their next compaction.

**Test:** `TestRewriteCrashWindow_SnapshotWrittenMetaStale` (RED: meta snapshot=0,
want 8).

#### 2.2 Unreadable `meta.json` and a failed `Load` were treated as "empty"

**Problem.** `Load` ignored a JSON error in `meta.json`, and `Raft.recover` logged
a warning and carried on when `Load` failed. Either way the node started with term
0, no recorded vote and an empty log, so it could vote a second time in a term it
had already voted in.

**Fix.** `Load` returns an error for a present-but-corrupt `meta.json`.
`recover` treats any `Load` error as fatal and the node refuses to start. A
missing file is still the normal first-boot case.

**Tests:** `TestLoad_CorruptMetaIsAnError`, `TestNewRaft_LoadFailureIsFatal`.

#### 2.3 A torn log tail or gap was dropped in memory but left on disk

**Problem.** `Load` already skipped a partial last line and stopped at a gap, but
left the file untouched. The next append was then written *after* the fragment,
gluing onto it (no newline) or sitting behind the gap. On the following restart
`Load` dropped those later entries again, even though they had been acknowledged.

**Fix.** After sanitizing, if the file does not match what was accepted (a
dropped or unparsable line, or a missing final newline), `Load` rewrites
`log.jsonl` to the accepted entries before any append can happen. `Rewrite` and
this repair share one `swapLogLocked` helper.

**Tests:** `TestLoad_TornTailIsRepairedBeforeAppend` (RED: got `[1 2 3]`, want
`[1 2 3 4]`), `TestLoad_GapTruncationIsPersisted`.

### Step 3: Chaos now proves the leader-failure claim

**Problem.** `chaos.ps1` picked a random victim each round, so it only killed the
leader about one round in five. A 6-round run in the audit never killed the
leader once, yet the README claims zero data loss when the leader is killed.

**Change.**

- `scripts/chaos.ps1 -KillLeader` kills the current leader every round. The
  victim's role is printed each round.
- A **convergence check** at the end waits for every node (not just the leader)
  to report at least the number of tracked canary keys, and all nodes to agree.
  This checks the followers' replicated state, which the per-round leader reads do
  not.
- The control server accepts `killLeader=1` on `/chaos` and the control dashboard
  has an "Always kill the leader" checkbox.

### Step 4: a rejoining node must not depose the leader

**How it was found.** Not by the test suite, but by watching the redesigned dashboard
([`UI_REDESIGN.md`](UI_REDESIGN.md)): after restarting a killed node the term climbed from 4 to
13 within seconds. The node logs showed the restarted node campaigning once a second and the
healthy leader repeatedly winning and losing leadership. Chaos passed throughout because it
only checks that data survives, not that the cluster stays calm.

**Causes.**

1. The leader's gRPC channel to the restarted node sat in the default reconnect backoff
   (which grows toward two minutes), so heartbeats did not reach it.
2. With no heartbeats the node timed out, bumped its term and asked for votes. A node that
   receives a higher term must adopt it, so the healthy leader stepped down every time.
   Raft has no defense against this without pre-vote or leader stickiness.

**Fixes** (`raft.go`, `transport.go`):

- **Fast reconnect.** Peer connections retry with a 50 ms base and 500 ms cap instead of the
  gRPC default.
- **Leader stickiness** (Raft dissertation 4.2.3). A follower that heard from a leader within
  the minimum election timeout, and a leader still in contact with a majority, ignore vote
  requests for a higher term without adopting it.
- **Check-quorum.** A leader that has heard from no majority for a full election timeout steps
  down. This also fixes an isolated leader showing as leader forever.

Pre-vote (a dry-run election before bumping the term) is the complete answer and is still not
implemented; it needs a protobuf change. The two fixes above remove the observed storm.

**Tests (RED first):** `TestRequestVote_IgnoredWhileLeaderIsAlive`,
`TestRequestVote_LeaderWithQuorumIgnoresDisruptor`, `TestLeader_StepsDownWhenIsolated`, plus
`TestRequestVote_GrantedAfterLeaderGoesSilent` to prove votes still flow once a leader is gone.
The isolation test first passed for the wrong reason: the in-memory test network only blocks
traffic *to* a node, so the isolated nodes could still depose the leader. It was changed to cut
every node, which made it fail correctly before the fix.

**Live check.** Kill the leader and restart it, three times: the term rose by exactly one per
kill (terms 2, 3, 4 across three leader kills), where before it rose by
about ten per restart. The restarted node logged zero elections. Eight-round leader-kill chaos
passes with all nodes converging.

## Verification

Environment: Windows 11, Go 1.26.4, 5 nodes on loopback.

| Check | Before | After |
|---|---|---|
| `go build ./...`, `go vet ./...` | clean | clean |
| `go test ./...` | 20 pass | 41 pass (includes 2 new control-server tests) |
| Chaos, random victim | 6 rounds pass | not rerun (superseded by next row) |
| Chaos, `-KillLeader`, 8 rounds, snapshot threshold 25 | not possible | 8 leaders killed, 8 rounds pass, all 5 nodes converge on 38 keys |
| Benchmark writes (24 writers, 4s, 2 runs each) | 7,000 and 7,186 ops/s | 6,502 and 7,006 ops/s |
| Benchmark reads | 32.5k and 33.1k ops/s | 32.3k and 33.1k ops/s |
| Benchmark errors | 24 | 24 |

Correction to the audit: it reported `server/raft.go` as failing `gofmt`. That was a
false positive. The working tree uses CRLF (`core.autocrlf=true`) and `gofmt -l` flags
CRLF files; the committed content is LF and is gofmt-clean, as are all files touched
here (checked with `tr -d '' | gofmt -d`).

The write difference is within run-to-run noise on this machine (the README
already states ranges). One earlier post-fix run showed 95 write errors; the two
repeat runs returned to the usual 24, one per worker, and the same cold-start
effect described in the README. It is noted here rather than hidden.

## Reproducing

```bash
go test ./...                                              # unit + regression tests
go build -o bin/raftnode.exe ./server && go build -o bin/raftctl.exe ./client
pwsh scripts/chaos.ps1 -Rounds 8 -KillLeader               # leader-failure fault test
```

## Not done yet

Found in the audit, deliberately left for later rounds:

- **Read-index is incomplete.** No check that the leader has committed an entry in
  its own term, and no wait for `lastApplied >= readIndex`, so a read right after an
  election can be stale.
- **Snapshot transfer** uses the 120 ms RPC timeout and is re-sent every heartbeat,
  so a large snapshot can never finish. Needs chunking or a longer timeout.
- **Replication** re-sends the whole tail on every proposal with no in-flight limit
  or batch cap; compaction and snapshotting run under the Raft mutex and stall RPCs.
- **No pre-vote.** Stickiness and check-quorum (step 4) cover the observed cases; pre-vote
  would need a protobuf change.
- **The race detector does not run** here: MinGW's `gcc` is 32-bit. A 64-bit
  toolchain would let `go test -race` cover the lock-heavy code.
- **Security:** the node HTTP gateway still mutates over GET with open CORS and binds to all
  interfaces. (The control server was fixed in the UI redesign.)
- **Docs and polish:** `SPEC.md` does not contain the rules and bug history the
  README points to; CI; license. (Dashboard kill/restart buttons shipped in the UI redesign.)
