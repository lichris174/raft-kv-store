# Reliability round: consistent tests, a tested consensus core, simpler code

Follows [`HARDENING.md`](HARDENING.md). The goal of this round: find out why test runs
"sometimes showed many errors", make every test result consistent and trustworthy, put
the core Raft algorithm under real tests, and simplify the code along the way.

## Method

1. **Reproduce first.** Ran every harness repeatedly before changing anything: the Go
   suite 20 times, the chaos test 6 times, the benchmark 9 times. Node output was being
   thrown away, so the bench script was changed to keep a log per node; that is what
   exposed the first root cause.
2. **Fix root causes, not symptoms.** Each fix below names the mechanism, not just the
   symptom.
3. **Independent review in two rounds.** Three reviewers worked in parallel on different
   areas (Raft correctness, harness reliability, readability and redundancy). Their
   findings were fixed, then two fresh reviewers checked each other's areas: the Raft
   reviewer's fixes and the harness fixes were each re-reviewed by an agent that had not
   written them.
4. **Mutation check.** For every safety fix, the bug was put back and the matching test
   had to fail. Two tests passed with their bug in place. Both were strengthened, and the
   stronger one immediately found a real bug (2.6 below).
5. **Repeat until stable.** The final numbers are in [Verification](#verification).

## 1. Why test results were inconsistent

### 1.1 Nodes crashed during benchmarks ("Access is denied")

**Symptom.** About one benchmark run in three reported tens of thousands of read errors:
roughly 20% of reads failed, which is exactly one node of five.

**Cause.** The node logs (newly kept) showed
`FATAL persist installed snapshot: rename ...meta.json.tmp ...meta.json: Access is denied`.
The project folder is inside OneDrive. OneDrive (or Defender) briefly opens files that a
node is replacing, and Windows then refuses the rename. The log file's rename already
retried for this; `meta.json` and `snapshot.bin` went through a plain `os.Rename`, and
InstallSnapshot treated the failure as fatal.

**Fix.**
- Every atomic file replace retries the rename (`renameWithRetry`, up to about 1.2 s).
- InstallSnapshot, conflict truncation and compaction now build the new state, persist
  it, and only then change memory. A failed write therefore leaves the node exactly as it
  was: the RPC fails and the leader retries. No crash is needed.
- Adopting a newer term works the same way: it is persisted before it is adopted, and a
  failed write drops the message instead of crash-stopping. The only fatal disk error
  left is failing to load state at startup.
- The harnesses and the dashboard's demo cluster now keep node data in
  `%TEMP%\raft-kv-store\...`, outside synced folders.

### 1.2 The benchmark always reported errors that were not errors

**Symptom.** Every run showed `errors=24` for 24 writers (and a similar count for
readers). The README explained it as each worker's first request missing the leader.

**Cause.** That explanation was wrong. The phase context was cancelled at the deadline,
and the request each worker had in flight at that moment failed and was counted as an
error. Separately, gRPC's own deadline timer fires a hair before the context reports
done, which produced a few more phantom `context deadline exceeded` errors.

**Fix.** Workers stop *starting* requests at the deadline, and a request in flight
finishes on its own timeout. Only operations that complete inside the phase count, and
throughput is divided by the phase length, so a straggler cannot skew it. Every real
error is now listed with its reason, a redirect in leader read mode counts as an error
(it used to count as a fast successful read), and the benchmark exits non-zero on any
error. Latencies print as `ms`: the `µs` sign was garbled by the Windows console code
page on its way to the report and the dashboard.

### 1.3 The dashboard showed every passing fault test as "Failed"

**Cause.** The control server's SSE `end` event declared the exit code
`json:"code,omitempty"`, so exit code 0 was dropped. The page saw `undefined`, which is
not `0`.

**Fix.** The code is always sent.

### 1.4 The dashboard marked healthy rounds as "DATA MISSING"

**Cause.** The page decided a round's result by matching `/mismatch|lost quorum|FAIL/i`
against any output line. raftctl printed transient retry messages such as
`FAILED: not leader` during elections, which the script then retried successfully. The
script reported success; the page reported data loss.

**Fix.** The chaos script prints one machine-readable line per result
(`ROUND 3 RESULT=OK ...`, `CONVERGENCE RESULT=...`, `CHAOS RESULT=PASS|FAIL ...`), and the
page parses only those, with anchored patterns. The format is documented at the top of
`scripts/chaos.ps1`.

### 1.5 The chaos script could crash, or pass when it should not

- **Crash.** With `$ErrorActionPreference = "Stop"`, Windows PowerShell turns any stderr
  text from a native command into a terminating error. One refused read through
  `raftctl.exe` aborted the whole run and left five nodes running. The scripts now talk
  to nodes over their HTTP gateway with `Invoke-RestMethod` (`scripts/lib.ps1`), where a
  failure is an exception the script handles. Every run ends in a `finally` that stops
  its nodes.
- **Asserting unacknowledged data.** The 30 canary writes never checked whether they
  succeeded, yet all 30 were verified later. A write that failed during the first
  election showed up as false data loss. Each write now must be acknowledged, or the run
  stops with a clear error.
- **Weak checks.** Integrity sampled 8 random keys per round, so a lost key was caught
  only sometimes. Convergence compared key *counts*, so a node with a wrong value or a
  swapped key passed. Now every acknowledged key is read back every round, and at the end
  all nodes must report the same state digest: a hash of their data, newly added to
  `Status`.
- **Unchecked restarts.** A restarted node that failed to come up went unnoticed. It
  must now answer within 15 s, and every node start gets its own log file.
- **Tight timing.** Leader detection gave up after 4 s while launching up to five
  `raftctl.exe` processes per poll. It now polls over HTTP for up to 15 s.

### 1.6 The control server could hang or leak

- Nodes started by a harness inherited the server's output pipe, so `cmd.Wait` waited for
  them, not just the script. A failed bench run left the page stuck on "Running", and every
  new run was refused. Fixed with `cmd.WaitDelay`. The `end` event is sent only after the
  last output line, so the verdict line can never arrive after it.
- **Stop** killed only PowerShell, which orphaned `raftbench.exe`. It is still sending load
  to the ports the next run uses. Stop now kills the whole process tree.
- `running` is cleared before `end` is announced, so starting the next run immediately
  works.
- A page that reloads mid-run lost the run, because SSE has no replay. The server keeps
  the current run's events and replays them on every connection.
- Stopping the demo cluster now waits until its processes have actually exited.

## 2. Raft core: bugs found and fixed

None of these was a safety violation. A reviewer checked the five Raft safety properties
against the code and found them intact. These are liveness and robustness bugs.

| # | Problem | Fix |
|---|---------|-----|
| 2.1 | A node cut off from the cluster kept campaigning and inflated its term. When it reconnected, the leader saw the higher term in an AppendEntries *reply* and stepped down. Stickiness only guarded the vote path. Reproduced: one follower's term +5 deposed the leader within 100 ms. | **Pre-vote** (Raft dissertation 9.6). A node first asks whether a majority would vote for it, without changing any term. A cut-off node never wins that, so its term stays put. New proto field `RequestVoteArgs.pre_vote`. |
| 2.2 | A one-member cluster never committed anything. Commit only advanced inside follower-reply handlers, and with no followers `Propose` hung forever. (A lone `raftnode` skips Raft, so this was latent.) | One helper appends on the leader (`leaderAppendLocked`) and re-evaluates commit right away. The no-op and `Propose` both use it. |
| 2.3 | A rejected AppendEntries did `nextIndex--` once *per reply*. With 24 writers in flight, a briefly lagging follower's `nextIndex` dropped 24 at once, often below the leader's snapshot, which triggered full snapshot sends during healthy load. | The follower returns its last index (`AppendEntriesReply.last_index`). The leader sets `nextIndex = max(matchIndex+1, min(prev, last+1))`, which is idempotent under concurrent replies. |
| 2.4 | Every proposal sent each follower the whole unreplicated tail again, and snapshots were re-sent every heartbeat with a 120 ms timeout. | At most one AppendEntries or InstallSnapshot in flight per peer. When it returns, the next batch (up to 512 entries) follows at once if the peer is still behind, so concurrent proposals travel together. Snapshot timeout is 2 s. |
| 2.5 | Read-index was incomplete. A new leader served reads before committing an entry of its own term, so a read right after an election could miss a write the previous leader had committed. | `ConfirmLeadership` waits for a current-term commit, confirms with a heartbeat quorum, then waits for `lastApplied >= readIndex`. A leader that cannot confirm now fails the read with `Unavailable` instead of redirecting clients to itself. |
| 2.6 | A failed log append could leave a partial line, and the retry wrote the same entries again behind it. Found while strengthening a test: **on Windows a file opened `O_APPEND` cannot be truncated through that handle**, so even the new undo never worked. | On failure, close the handle, truncate by path to the last whole entry, reopen. If that also fails, refuse appends until a rewrite puts the file in a known state. |
| 2.7 | Compaction ran under the Raft lock and, after a failed write, retried on every single apply. Each retry rewrote the whole log and could stall heartbeats. | After a failure, compaction waits another threshold's worth of entries. |
| 2.8 | AppendEntries accepted `PrevLogTerm = 0` for an entry it did not have (a missing entry has term 0), and did not check that a batch's indexes were contiguous. Neither is reachable from a correct leader. Either would corrupt the offset arithmetic. | `matchesPrevLocked` checks that the prev entry exists and that the batch is contiguous. |
| 2.9 | An InstallSnapshot that kept the follower's log tail did not apply entries in it that were already committed. An empty snapshot payload would have left the old `snapshot.bin` on disk. | Apply after install. Treat a nil payload as empty. |

## 3. Tests

The test harness was rewritten (`server/cluster_test.go`):

- Each node now sends through its own endpoint, so the network can model **real
  partitions** (groups that reach only each other), **crashes** (a crashed node neither
  sends nor receives, and its leftover goroutines cannot speak for its restarted
  successor), and **lossy, delaying links** with a seeded random generator.
- Nodes are **crashed and restarted from their persisted state**.
- Two Raft guarantees are **checked continuously in every cluster test**: at most one
  leader per term (election safety), and every node applies the same entry at each index
  (state machine safety). Any violation fails the test at cleanup.

This replaced five overlapping helpers (`buildCluster`, `buildClusterEx`,
`findLeaderAlive`, `waitForLeader`, `proposeWithRetry`) and the 14 copies of
`go r.Run(); defer r.Stop()`.

New tests of the core algorithm:

| Test | What it proves |
|------|----------------|
| `TestPartition_MinorityLeaderCannotCommit` | A leader cut off with a minority commits nothing, its waiting proposer gets `ErrNotLeader` when check-quorum deposes it, the majority elects a new leader and commits, and after healing the old leader's entries are overwritten. |
| `TestLogMatching_DivergentLogsAreRepaired` | Three successive leaders each strand uncommitted entries. Afterwards every node's log is identical, entry by entry. |
| `TestCommit_OldTermEntryNeedsCurrentTermEntry` | Raft figure 8: an old-term entry on a majority does not commit until a current-term entry does. |
| `TestRequestVote_ElectionRestriction` | The up-to-date rule, for higher term, longer log, equal, shorter log, and lower term. |
| `TestPreVote_IsolatedNodeCannotDisruptLeaderOnRejoin` | A node isolated for 10 election timeouts keeps its term and does not disturb the leader when it returns. |
| `TestRequestVote_PreVote*` | A pre-vote changes no state, and is refused while a leader is alive. |
| `TestReadIndex_WaitsForCurrentTermCommit` | A new leader does not serve reads before committing in its own term. |
| `TestReplication_FarBehindFollowerCatchesUp` | A follower that missed 1,500 entries catches up through capped batches, without a snapshot. |
| `TestElection_OneMemberClusterLeadsItself`, `TestElection_TwoMemberClusterNeedsBoth` | Edge-sized clusters. |
| `TestAppendEntries_*`, `TestInstallSnapshot_*` | Stale shorter messages, gaps with a hint, zero prev term, non-contiguous batches, prev inside the snapshot, applying a kept committed tail, persistence failures. |
| `TestAppendLog_*` | Partial writes are undone; an append that cannot be undone blocks further appends until a rewrite. |
| `TestHTTP_*` | Writes require POST; only local pages may call the gateway. |
| `TestFuzz_RandomFaults` | Five nodes, three concurrent writers, random partitions, crashes and restarts every 100 ms for 3 s, 5% message loss and up to 3 ms delay. Seeds rotate the snapshot threshold (3, 30, none). Safety is checked throughout. Every acknowledged write must be on all five nodes at the end. `RAFT_FUZZ_SEEDS=N` runs more seeds. |

### Mutation check

Each fix was reverted in place and its test was run:

| Bug put back | Caught by |
|--------------|-----------|
| No pre-vote | `TestPreVote_IsolatedNodeCannotDisruptLeaderOnRejoin` |
| Old-term entries commit by replica count | `TestCommit_OldTermEntryNeedsCurrentTermEntry` |
| One-member cluster never commits | `TestElection_OneMemberClusterLeadsItself` |
| Zero prev term matches a missing entry | `TestAppendEntries_ZeroPrevTermDoesNotMatchMissingEntry` |
| Install does not apply the kept tail | `TestInstallSnapshot_AppliesCommittedTail` |
| Follower commits past verified entries | `TestAppendEntries_UnverifiedHeartbeatDoesNotCommit` |
| Read-index skips the current-term commit | `TestReadIndex_WaitsForCurrentTermCommit` (only after it was rebuilt on a one-member node; before that it failed for an unrelated reason and caught nothing) |
| No leader stickiness | `TestRequestVote_IgnoredWhileLeaderIsAlive` |
| No check-quorum | `TestLeader_StepsDownWhenIsolated` |
| Conflict truncation mutates the log before persisting | `TestAppendEntries_TruncationRewriteFailureLeavesLogIntact` |
| Partial append not undone | `TestAppendLog_PartialWriteIsUndone` (new, after the first version missed it) |
| Election restriction ignores log length | `TestRequestVote_ElectionRestriction` |

## 4. Simplification

- `raft.go` (about 1,000 lines) was split by protocol area:
  - `raft.go`: state, persistence helpers, log math
  - `election.go`
  - `replication.go`
  - `readindex.go`
- The repeated "drop a stale reply / step down on a newer term" check became
  `staleReplyLocked`. "Accept this leader" became `acceptLeaderLocked`. The match/commit
  update became `recordMatchLocked`. Vote collection for both pre-vote and vote is
  `collectVotesLocked`.
- `persist.go`:
  - one entry writer and one synced-file writer replace three hand-rolled copies;
  - `Load` is split into `loadMeta`, `loadSnapshot`, `readLogFile` and `contiguousTail`;
  - the test-only `memPersister` moved out of the production binary.
- The HTTP gateway's four near-identical handlers share one adapter. The client's two
  hint-following loops are one `followLeader`.
- The control server:
  - routes moved out of `main`;
  - one `unlessBusy` guard replaces three copies;
  - `strings.Join` replaces a hand-written join;
  - a dead bool return was removed.
- The two PowerShell harnesses share `scripts/lib.ps1` (cluster launch, status, leader
  wait, reads and writes) instead of duplicating it.
- Two persistence test files with an arbitrary split were merged. The disk-failure
  injector uses `atomic.Bool` instead of a mutex and a closure setter.

Production Go code grew from 2,860 to 3,096 lines despite these consolidations. The
growth is new behavior: pre-vote, full read-index, one-request-in-flight replication,
origin guards, run replay and error tallies. Tests grew from 1,450 to about 2,300 lines.

## Third review round

A final reviewer checked the round-2 patches and found four more problems, all fixed:

- **Double fault in the persister.** A failed truncate followed by a failed log swap
  could re-read the file size with the fragment included. The "repair" then cut
  nothing. `logSize` is no longer refreshed while the persister is broken.
- **Benchmark.** A failure that surfaced after the phase deadline was dropped. Now
  every failure counts; only late *successes* are left out of throughput.
- **Control server: kill.** `killNode` dropped the node from its table before the
  process exited, so a quick Start could launch a second copy. It now stays listed
  until it exits.
- **Control server: demo actions.** A demo-cluster action could interleave with a test
  run starting. The check and the action now hold the hub lock together.

## Verification

Environment: Windows 11, Go 1.26.4, 5 nodes on loopback, project inside OneDrive.

| Check | Before this round | After |
|-------|-------------------|-------|
| `go vet ./...` | clean | clean |
| Go tests | 41 | 66, all passing 20 times in a row (1,320 runs) |
| Randomized fault run (`RAFT_FUZZ_SEEDS=30`) | did not exist | 30 of 30 seeds; 23,667 acknowledged writes, none lost |
| Mutation check | did not exist | 17 of 17 reintroduced bugs caught |
| Chaos, `-KillLeader`, 8 rounds | passed from the CLI; the dashboard showed "Failed" | 5 of 5 runs passed, plus one run under heavy CPU load |
| Chaos, random victim, 8 rounds | passed | 3 of 3 runs passed |
| Benchmark errors (24 writers, 16 readers) | 24 every run; about 1 run in 3 had about 30,000 (a node crashed) | 0 in 5 of 5 runs |
| Benchmark writes | about 7,000 to 7,500 ops/s | 12,075 to 12,819 ops/s (p50 1.25 ms, p99 about 19 ms) |
| Benchmark reads | about 29,000 to 34,000 ops/s | 32,943 to 35,000 ops/s (p50 0.55 ms) |
| Control server end to end (`/cluster`, kill and restart, chaos and bench over SSE, replay, origin and host guards) | not tested | 19 of 19 checks |
| Dashboard in a browser | a passing fault test showed "Failed" | "Passed: 3 rounds, 34 acknowledged writes, none lost"; still shown after a reload |

Writes got faster because concurrent proposals now share one AppendEntries per follower,
instead of each proposal re-sending the whole tail.

## Still open

- **The race detector** cannot run on this machine. Its cgo needs a 64-bit gcc, and
  MinGW here is 32-bit.
- **Snapshots are sent whole.** They are not chunked, and they are taken under the Raft
  lock.
- **A node wiped clean but kept in the cluster** (its disk deleted, same identity) is
  outside Raft's model. The leader could keep retrying an index the node no longer has.
- **The control server and harness scripts are Windows-only.** They use PowerShell and
  `taskkill`.
- **CI and a license.**
