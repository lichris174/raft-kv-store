package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	raftpb "github.com/chrisli/raft-kv-store/proto"
)

// Regression tests for the safety and durability fixes documented in
// docs/HARDENING.md. These drive a single Raft node's RPC handlers directly (the
// run loop is never started), so each test controls exactly what the node sees.

// failingPersister wraps memPersister and fails selected operations on demand.
type failingPersister struct {
	memPersister
	mu2         sync.Mutex
	failAppend  bool
	failMeta    bool
	failRewrite bool
}

var errDisk = errors.New("injected disk failure")

func (p *failingPersister) set(fn func()) {
	p.mu2.Lock()
	defer p.mu2.Unlock()
	fn()
}

func (p *failingPersister) AppendLog(entries []*raftpb.LogEntry) error {
	p.mu2.Lock()
	fail := p.failAppend
	p.mu2.Unlock()
	if fail {
		return errDisk
	}
	return p.memPersister.AppendLog(entries)
}

func (p *failingPersister) SaveMeta(meta Meta) error {
	p.mu2.Lock()
	fail := p.failMeta
	p.mu2.Unlock()
	if fail {
		return errDisk
	}
	return p.memPersister.SaveMeta(meta)
}

func (p *failingPersister) Rewrite(meta Meta, snap []byte, log []*raftpb.LogEntry) error {
	p.mu2.Lock()
	fail := p.failRewrite
	p.mu2.Unlock()
	if fail {
		return errDisk
	}
	return p.memPersister.Rewrite(meta, snap, log)
}

// soloNode builds a 3-member-config Raft node that is never Run, so only the
// handlers the test calls ever execute.
func soloNode(p Persister, fatal func(op string, err error)) (*Raft, *KVStore) {
	store := NewKVStore()
	cfg := fastConfig("n0", "n0", []string{"n1", "n2"})
	cfg.Apply = func(e *raftpb.LogEntry) { applyOp(store, e.GetOp(), e.GetKey(), e.GetValue()) }
	cfg.Snapshot = store.Snapshot
	cfg.Restore = store.Restore
	cfg.Persister = p
	cfg.OnPersistFatal = fatal
	return NewRaft(cfg, newInmemNet()), store
}

func setEntry(term, index uint64, key, val string) *raftpb.LogEntry {
	return &raftpb.LogEntry{Term: term, Index: index, Op: "set", Key: key, Value: val}
}

func mustAppend(t *testing.T, r *Raft, args *raftpb.AppendEntriesArgs) {
	t.Helper()
	reply, err := r.AppendEntries(context.Background(), args)
	if err != nil || !reply.Success {
		t.Fatalf("AppendEntries(%+v): success=%v err=%v", args, reply.GetSuccess(), err)
	}
}

// --- Fix 1: commit index must be bounded by entries the leader verified ---

// ConfirmLeadership sends a heartbeat with PrevLogIndex=0 and the leader's
// commit index. That heartbeat proves nothing about the follower's log, so it
// must not advance the follower's commit index over entries that may diverge
// from the leader's.
func TestAppendEntries_UnverifiedHeartbeatDoesNotCommit(t *testing.T) {
	r, store := soloNode(nil, nil)
	// Entries from an old leader (term 1); they may differ from the new leader's.
	mustAppend(t, r, &raftpb.AppendEntriesArgs{
		Term: 1, LeaderId: "old",
		Entries: []*raftpb.LogEntry{setEntry(1, 1, "a", "old"), setEntry(1, 2, "b", "old")},
	})

	// New leader (term 2) read-index style heartbeat: prev=0, no entries, commit=2.
	mustAppend(t, r, &raftpb.AppendEntriesArgs{Term: 2, LeaderId: "new", LeaderCommit: 2})

	if s := r.StatusSnapshot(); s.CommitIndex != 0 || s.LastApplied != 0 {
		t.Fatalf("unverified heartbeat advanced commit=%d applied=%d, want 0/0", s.CommitIndex, s.LastApplied)
	}
	if _, ok := store.Get("a"); ok {
		t.Fatal("entry applied to the state machine without being verified against the leader")
	}
}

// A heartbeat that does carry a matching prev entry may commit up to it.
func TestAppendEntries_VerifiedHeartbeatCommitsUpToPrev(t *testing.T) {
	r, store := soloNode(nil, nil)
	mustAppend(t, r, &raftpb.AppendEntriesArgs{
		Term: 1, LeaderId: "L",
		Entries: []*raftpb.LogEntry{setEntry(1, 1, "a", "1"), setEntry(1, 2, "b", "2"), setEntry(1, 3, "c", "3")},
	})
	mustAppend(t, r, &raftpb.AppendEntriesArgs{
		Term: 1, LeaderId: "L", PrevLogIndex: 2, PrevLogTerm: 1, LeaderCommit: 3,
	})
	// Verified through index 2 only; index 3 was not covered by this message.
	if s := r.StatusSnapshot(); s.CommitIndex != 2 {
		t.Fatalf("commit=%d want 2 (bounded by prevLogIndex+len(entries))", s.CommitIndex)
	}
	if v, _ := store.Get("b"); v != "2" {
		t.Fatalf("b=%q want 2", v)
	}
	if _, ok := store.Get("c"); ok {
		t.Fatal("c applied beyond the verified prefix")
	}
}

// --- Fix 2: a stale snapshot must not roll the state machine back ---

func TestInstallSnapshot_StaleSnapshotDoesNotRollBackState(t *testing.T) {
	r, store := soloNode(nil, nil)
	var entries []*raftpb.LogEntry
	for i, k := range []string{"k1", "k2", "k3", "k4", "k5"} {
		entries = append(entries, setEntry(1, uint64(i+1), k, "v"))
	}
	mustAppend(t, r, &raftpb.AppendEntriesArgs{Term: 1, LeaderId: "L", Entries: entries, LeaderCommit: 5})
	if s := r.StatusSnapshot(); s.LastApplied != 5 {
		t.Fatalf("setup: applied=%d want 5", s.LastApplied)
	}

	// Snapshot covering only up to index 3 arrives late.
	reply, err := r.InstallSnapshot(context.Background(), &raftpb.InstallSnapshotArgs{
		Term: 1, LeaderId: "L", LastIncludedIndex: 3, LastIncludedTerm: 1,
		Data: []byte(`{"k1":"v","k2":"v","k3":"v"}`),
	})
	if err != nil || reply.Term != 1 {
		t.Fatalf("InstallSnapshot: %v %+v", err, reply)
	}
	for _, k := range []string{"k4", "k5"} {
		if _, ok := store.Get(k); !ok {
			t.Fatalf("%s lost: stale snapshot rolled the state machine back", k)
		}
	}
}

// A snapshot whose last-included entry disagrees with our log at that index
// proves our log tail is from a different history, so the tail must go.
func TestInstallSnapshot_TermMismatchDropsLogTail(t *testing.T) {
	r, _ := soloNode(nil, nil)
	mustAppend(t, r, &raftpb.AppendEntriesArgs{
		Term: 1, LeaderId: "old",
		Entries: []*raftpb.LogEntry{setEntry(1, 1, "a", "1"), setEntry(1, 2, "b", "2"), setEntry(1, 3, "c", "3")},
	})
	_, err := r.InstallSnapshot(context.Background(), &raftpb.InstallSnapshotArgs{
		Term: 2, LeaderId: "new", LastIncludedIndex: 2, LastIncludedTerm: 2,
		Data: []byte(`{"x":"y"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	s := r.StatusSnapshot()
	if s.SnapshotIndex != 2 {
		t.Fatalf("snapshotIndex=%d want 2", s.SnapshotIndex)
	}
	if s.LogLen != 0 {
		t.Fatalf("logLen=%d: entry 3 (term 1) conflicts with snapshot (index 2, term 2) and must be dropped", s.LogLen)
	}
}

// --- Fix 3: persistence failures must never be acknowledged ---

func TestAppendEntries_PersistFailureRejectsAndRollsBack(t *testing.T) {
	p := &failingPersister{}
	r, _ := soloNode(p, nil)
	p.set(func() { p.failAppend = true })

	_, err := r.AppendEntries(context.Background(), &raftpb.AppendEntriesArgs{
		Term: 1, LeaderId: "L", Entries: []*raftpb.LogEntry{setEntry(1, 1, "a", "1")},
	})
	if err == nil {
		t.Fatal("AppendEntries acked an entry whose disk write failed")
	}
	if s := r.StatusSnapshot(); s.LogLen != 0 {
		t.Fatalf("logLen=%d: unpersisted entry kept in memory", s.LogLen)
	}

	// Once the disk recovers, the same entry is accepted.
	p.set(func() { p.failAppend = false })
	mustAppend(t, r, &raftpb.AppendEntriesArgs{
		Term: 1, LeaderId: "L", Entries: []*raftpb.LogEntry{setEntry(1, 1, "a", "1")},
	})
}

func TestRequestVote_PersistFailureDoesNotGrantVote(t *testing.T) {
	p := &failingPersister{}
	// A term bump inside RequestVote also saves meta; keep that working and fail
	// only the vote record by pre-seeding the term.
	r, _ := soloNode(p, nil)
	r.mu.Lock()
	r.currentTerm = 3
	r.mu.Unlock()
	p.set(func() { p.failMeta = true })

	reply, err := r.RequestVote(context.Background(), &raftpb.RequestVoteArgs{Term: 3, CandidateId: "n1"})
	if err == nil && reply.VoteGranted {
		t.Fatal("granted a vote that was not durably recorded")
	}
	r.mu.Lock()
	voted := r.votedFor
	r.mu.Unlock()
	if voted != "" {
		t.Fatalf("votedFor=%q after failed persist, want empty", voted)
	}
}

func TestPropose_PersistFailureRollsBack(t *testing.T) {
	p := &failingPersister{}
	r, _ := soloNode(p, nil)
	r.mu.Lock()
	r.state = Leader
	r.currentTerm = 1
	r.mu.Unlock()
	p.set(func() { p.failAppend = true })

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	err := r.Propose(ctx, "set", "k", "v")
	if err == nil || errors.Is(err, ErrNotLeader) {
		t.Fatalf("Propose err=%v, want a persistence error", err)
	}
	r.mu.Lock()
	n, w := len(r.logEntries), len(r.waiters)
	r.mu.Unlock()
	if n != 0 || w != 0 {
		t.Fatalf("log=%d waiters=%d after failed propose, want 0/0", n, w)
	}
}

// A conflict truncation cannot be rolled back in memory, so if its rewrite fails
// the node must crash-stop instead of acknowledging.
func TestAppendEntries_TruncationRewriteFailureIsFatal(t *testing.T) {
	p := &failingPersister{}
	var gotOp string
	r, _ := soloNode(p, func(op string, err error) { gotOp = op })
	mustAppend(t, r, &raftpb.AppendEntriesArgs{
		Term: 1, LeaderId: "old",
		Entries: []*raftpb.LogEntry{setEntry(1, 1, "a", "1"), setEntry(1, 2, "b", "old")},
	})
	p.set(func() { p.failRewrite = true })

	// New leader overwrites index 2 with a different term: conflict -> truncate.
	r.AppendEntries(context.Background(), &raftpb.AppendEntriesArgs{
		Term: 2, LeaderId: "new", PrevLogIndex: 1, PrevLogTerm: 1,
		Entries: []*raftpb.LogEntry{setEntry(2, 2, "b", "new")},
	})
	if gotOp == "" {
		t.Fatal("failed truncation rewrite was not treated as fatal")
	}
}

// When the snapshot's last-included entry matches our log, entries after it are
// still valid and must be kept (Raft InstallSnapshot rule 6).
func TestInstallSnapshot_TermMatchKeepsLogTail(t *testing.T) {
	r, _ := soloNode(nil, nil)
	mustAppend(t, r, &raftpb.AppendEntriesArgs{
		Term: 1, LeaderId: "L",
		Entries: []*raftpb.LogEntry{setEntry(1, 1, "a", "1"), setEntry(1, 2, "b", "2"), setEntry(1, 3, "c", "3")},
	})
	if _, err := r.InstallSnapshot(context.Background(), &raftpb.InstallSnapshotArgs{
		Term: 1, LeaderId: "L", LastIncludedIndex: 2, LastIncludedTerm: 1,
		Data: []byte(`{"a":"1","b":"2"}`),
	}); err != nil {
		t.Fatal(err)
	}
	if s := r.StatusSnapshot(); s.SnapshotIndex != 2 || s.LogLen != 1 {
		t.Fatalf("snapshotIndex=%d logLen=%d, want 2/1 (entry 3 matches and must be kept)", s.SnapshotIndex, s.LogLen)
	}
}

// --- Disruption resistance: a rejoining node must not depose a healthy leader ---

// A follower that heard from a leader within the minimum election timeout must
// ignore a vote request for a higher term (Raft dissertation 4.2.3). Without
// this, one node that was cut off and kept campaigning bumps the term and forces
// the healthy leader to step down the moment it reconnects.
func TestRequestVote_IgnoredWhileLeaderIsAlive(t *testing.T) {
	r, _ := soloNode(nil, nil)
	mustAppend(t, r, &raftpb.AppendEntriesArgs{Term: 1, LeaderId: "L"})

	reply, err := r.RequestVote(context.Background(), &raftpb.RequestVoteArgs{Term: 5, CandidateId: "n1"})
	if err != nil {
		t.Fatal(err)
	}
	if reply.VoteGranted {
		t.Fatal("granted a vote while a leader was alive")
	}
	if s := r.StatusSnapshot(); s.Term != 1 {
		t.Fatalf("term=%d, want 1: a disruptive candidate must not bump our term", s.Term)
	}
}

// Once the leader has been silent for the minimum election timeout, votes flow.
func TestRequestVote_GrantedAfterLeaderGoesSilent(t *testing.T) {
	r, _ := soloNode(nil, nil)
	mustAppend(t, r, &raftpb.AppendEntriesArgs{Term: 1, LeaderId: "L"})
	time.Sleep(r.cfg.ElectionTimeoutMin + 30*time.Millisecond)

	reply, err := r.RequestVote(context.Background(), &raftpb.RequestVoteArgs{Term: 2, CandidateId: "n1"})
	if err != nil || !reply.VoteGranted {
		t.Fatalf("vote not granted after leader silence: %+v %v", reply, err)
	}
}

// A leader that is in contact with a majority also ignores higher-term requests.
func TestRequestVote_LeaderWithQuorumIgnoresDisruptor(t *testing.T) {
	net := newInmemNet()
	nodes, _ := buildCluster(net, 3)
	for _, r := range nodes {
		go r.Run()
		defer r.Stop()
	}
	leader := waitForLeader(t, nodes, nil, 2*time.Second)
	term := leader.StatusSnapshot().Term
	time.Sleep(60 * time.Millisecond) // let heartbeats establish contact

	reply, _ := leader.RequestVote(context.Background(), &raftpb.RequestVoteArgs{
		Term: term + 5, CandidateId: "x", LastLogIndex: 1 << 20, LastLogTerm: 1 << 20,
	})
	if reply.VoteGranted || !leader.IsLeader() {
		t.Fatalf("healthy leader deposed by a disruptive candidate: granted=%v leader=%v", reply.VoteGranted, leader.IsLeader())
	}
}

// Check-quorum: a leader that cannot reach a majority must stop calling itself
// leader, otherwise it shows as one forever and clients keep writing to it.
func TestLeader_StepsDownWhenIsolated(t *testing.T) {
	net := newInmemNet()
	nodes, _ := buildCluster(net, 3)
	for _, r := range nodes {
		go r.Run()
		defer r.Stop()
	}
	leader := waitForLeader(t, nodes, nil, 2*time.Second)
	// inmemNet only blocks traffic *to* a node, so cut every node, the leader
	// included, to get a real two-way partition around it.
	for _, r := range nodes {
		net.setDown(r.cfg.Addr, true)
	}
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) && leader.IsLeader() {
		time.Sleep(10 * time.Millisecond)
	}
	if leader.IsLeader() {
		t.Fatal("isolated leader never stepped down")
	}
}
