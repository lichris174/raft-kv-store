package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	raftpb "github.com/chrisli/raft-kv-store/proto"
)

// Edge cases of the follower-side AppendEntries and InstallSnapshot handlers,
// driven directly on a node that is never run.

func logIndexes(r *Raft) []uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return indicesOf(r.logEntries)
}

// A delayed, shorter AppendEntries that agrees with our log must not truncate
// the entries a later message already added.
func TestAppendEntries_StaleShorterMessageDoesNotTruncate(t *testing.T) {
	r, _ := soloNode(nil, nil)
	mustAppend(t, r, &raftpb.AppendEntriesArgs{Term: 1, LeaderId: "L", Entries: []*raftpb.LogEntry{
		setEntry(1, 1, "a", "1"), setEntry(1, 2, "b", "2"), setEntry(1, 3, "c", "3"),
	}})
	mustAppend(t, r, &raftpb.AppendEntriesArgs{Term: 1, LeaderId: "L", Entries: []*raftpb.LogEntry{setEntry(1, 1, "a", "1")}})
	if got := logIndexes(r); len(got) != 3 {
		t.Fatalf("log=%v, want [1 2 3]", got)
	}
}

// A prev index past our log is a gap: reject, and report our last index so the
// leader can jump straight back to it.
func TestAppendEntries_GapIsRejectedWithHint(t *testing.T) {
	r, _ := soloNode(nil, nil)
	mustAppend(t, r, &raftpb.AppendEntriesArgs{Term: 1, LeaderId: "L", Entries: []*raftpb.LogEntry{setEntry(1, 1, "a", "1")}})
	reply, err := r.AppendEntries(context.Background(), &raftpb.AppendEntriesArgs{
		Term: 1, LeaderId: "L", PrevLogIndex: 5, PrevLogTerm: 1, Entries: []*raftpb.LogEntry{setEntry(1, 6, "f", "6")},
	})
	if err != nil || reply.Success || reply.LastIndex != 1 {
		t.Fatalf("success=%v lastIndex=%d err=%v, want a rejection with lastIndex=1", reply.GetSuccess(), reply.GetLastIndex(), err)
	}
}

// A zero prev term with a nonzero prev index matches nothing (real terms start
// at 1); it must not pass the consistency check on an entry we do not have.
func TestAppendEntries_ZeroPrevTermDoesNotMatchMissingEntry(t *testing.T) {
	r, _ := soloNode(nil, nil)
	reply, _ := r.AppendEntries(context.Background(), &raftpb.AppendEntriesArgs{
		Term: 1, LeaderId: "L", PrevLogIndex: 3, PrevLogTerm: 0, Entries: []*raftpb.LogEntry{setEntry(1, 4, "d", "4")},
	})
	if reply.Success || len(logIndexes(r)) != 0 {
		t.Fatalf("accepted entries after a missing prev entry: log=%v", logIndexes(r))
	}
}

// A batch whose indexes do not follow prev one by one is malformed and must be
// refused whole, or the log would get a hole.
func TestAppendEntries_NonContiguousBatchIsRejected(t *testing.T) {
	r, _ := soloNode(nil, nil)
	reply, _ := r.AppendEntries(context.Background(), &raftpb.AppendEntriesArgs{
		Term: 1, LeaderId: "L", Entries: []*raftpb.LogEntry{setEntry(1, 1, "a", "1"), setEntry(1, 3, "c", "3")},
	})
	if reply.Success || len(logIndexes(r)) != 0 {
		t.Fatalf("accepted a batch with a hole: log=%v", logIndexes(r))
	}
}

// A prev index inside our snapshot is committed here, so it matches whatever
// term the leader names; overlapping entries are skipped, new ones appended.
func TestAppendEntries_PrevInsideSnapshotIsAccepted(t *testing.T) {
	r, _ := soloNode(nil, nil)
	if _, err := r.InstallSnapshot(context.Background(), &raftpb.InstallSnapshotArgs{
		Term: 1, LeaderId: "L", LastIncludedIndex: 5, LastIncludedTerm: 1, Data: []byte(`{}`),
	}); err != nil {
		t.Fatal(err)
	}
	mustAppend(t, r, &raftpb.AppendEntriesArgs{Term: 1, LeaderId: "L", PrevLogIndex: 3, PrevLogTerm: 1, Entries: []*raftpb.LogEntry{
		setEntry(1, 4, "d", "4"), setEntry(1, 5, "e", "5"), setEntry(1, 6, "f", "6"),
	}})
	if got := logIndexes(r); len(got) != 1 || got[0] != 6 {
		t.Fatalf("log=%v, want [6]: entries 4 and 5 are inside the snapshot", got)
	}
}

// A snapshot that keeps our log tail may leave entries that are already
// committed; they must be applied right away, not on some later message.
func TestInstallSnapshot_AppliesCommittedTail(t *testing.T) {
	r, store := soloNode(nil, nil)
	mustAppend(t, r, &raftpb.AppendEntriesArgs{Term: 1, LeaderId: "L", Entries: []*raftpb.LogEntry{
		setEntry(1, 1, "a", "1"), setEntry(1, 2, "b", "2"), setEntry(1, 3, "c", "3"),
	}})
	r.mu.Lock()
	r.commitIndex = 3 // committed (the leader said so) but not yet applied
	r.mu.Unlock()
	if _, err := r.InstallSnapshot(context.Background(), &raftpb.InstallSnapshotArgs{
		Term: 1, LeaderId: "L", LastIncludedIndex: 2, LastIncludedTerm: 1, Data: []byte(`{"a":"1","b":"2"}`),
	}); err != nil {
		t.Fatal(err)
	}
	if v, ok := store.Get("c"); !ok || v != "3" {
		t.Fatalf("committed entry 3 kept after the snapshot was not applied (c=%q)", v)
	}
}

// An empty snapshot payload (gRPC decodes it as nil) must replace the stored
// snapshot, not be read as "keep the old file" while meta records the new index.
func TestInstallSnapshot_EmptyPayloadReplacesSnapshot(t *testing.T) {
	p := &memPersister{snap: []byte(`{"old":"x"}`)}
	r, _ := soloNode(p, nil)
	if _, err := r.InstallSnapshot(context.Background(), &raftpb.InstallSnapshotArgs{
		Term: 1, LeaderId: "L", LastIncludedIndex: 4, LastIncludedTerm: 1, Data: nil,
	}); err != nil {
		t.Fatal(err)
	}
	if meta, snap, _, _ := p.Load(); meta.SnapshotIndex != 4 || snap == nil || len(snap) != 0 {
		t.Fatalf("stored snapshot=%q at index %d, want an empty snapshot at 4", snap, meta.SnapshotIndex)
	}
}

// A message carrying a newer term is dropped, with no state change, if the new
// term cannot be made durable.
func TestAppendEntries_UnpersistableTermIsIgnored(t *testing.T) {
	p := &failingPersister{}
	r, _ := soloNode(p, nil)
	p.failMeta.Store(true)
	reply, err := r.AppendEntries(context.Background(), &raftpb.AppendEntriesArgs{
		Term: 3, LeaderId: "L", Entries: []*raftpb.LogEntry{setEntry(3, 1, "a", "1")},
	})
	if err == nil || reply.GetSuccess() {
		t.Fatalf("accepted a term it could not persist: %+v %v", reply, err)
	}
	if s := r.StatusSnapshot(); s.Term != 0 || s.LogLen != 0 || s.LeaderAddr != "" {
		t.Fatalf("state changed: %+v", s)
	}
}

// After a failed compaction write, compaction waits another threshold's worth of
// entries instead of retrying (under the Raft lock) on every single apply.
func TestCompaction_FailedWriteBacksOff(t *testing.T) {
	p := &countingRewrites{}
	p.failRewrite.Store(true)
	store := NewKVStore()
	cfg := fastConfig("n0", "n0", []string{"n1", "n2"})
	cfg.Apply = func(e *raftpb.LogEntry) { applyOp(store, e.GetOp(), e.GetKey(), e.GetValue()) }
	cfg.Snapshot, cfg.Restore = store.Snapshot, store.Restore
	cfg.SnapshotThreshold = 5
	cfg.Persister = p
	r := NewRaft(cfg, newInmemNet().endpoint("n0"))
	for i := uint64(1); i <= 9; i++ {
		mustAppend(t, r, &raftpb.AppendEntriesArgs{Term: 1, LeaderId: "L", PrevLogIndex: i - 1, PrevLogTerm: min(i-1, 1),
			LeaderCommit: i, Entries: []*raftpb.LogEntry{setEntry(1, i, "k", "v")}})
	}
	if n := p.rewrites.Load(); n != 1 {
		t.Fatalf("%d compaction attempts over 9 applies with threshold 5, want 1 (the next is due at 10)", n)
	}
}

type countingRewrites struct {
	failingPersister
	rewrites atomic.Int32
}

func (p *countingRewrites) Rewrite(meta Meta, snap []byte, tail []*raftpb.LogEntry) error {
	p.rewrites.Add(1)
	return p.failingPersister.Rewrite(meta, snap, tail)
}

// A deposed leader must not name itself as leader in hints, or clients that
// follow the hint keep coming back to it.
func TestLeader_DeposedLeaderDoesNotHintAtItself(t *testing.T) {
	c := newCluster(t, 3, 0)
	l := c.waitLeader()
	r := c.node(l)
	r.mu.Lock()
	_ = r.becomeFollowerLocked(r.currentTerm + 1) // a newer term was seen
	r.mu.Unlock()
	if hint := r.LeaderAddr(); hint == c.addrs[l] {
		t.Fatalf("deposed leader still hints at itself (%q)", hint)
	}
}

// A long snapshot transfer must not cost the leader its quorum. Here the leader
// can stay in office only with the lagging follower's support, and that
// follower's InstallSnapshot takes longer than the check-quorum window. The
// leader keeps heartbeating it meanwhile, so both stay in contact.
func TestLeader_KeepsQuorumWhileSnapshotIsInFlight(t *testing.T) {
	c := newCluster(t, 5, 5)
	l := c.waitLeader()
	lag := (l + 1) % 5
	c.crash(lag)
	for i := 0; i < 20; i++ { // the leader compacts past everything the lagging node has
		c.mustPropose(fmt.Sprintf("k%02d", i), "v")
	}
	term := c.node(l).StatusSnapshot().Term

	c.net.mu.Lock()
	c.net.snapshotDelay = 4 * c.node(l).cfg.ElectionTimeoutMax
	c.net.mu.Unlock()
	c.restart(lag)
	c.crash((l + 2) % 5) // now the leader needs the lagging node for a majority
	c.crash((l + 3) % 5)

	time.Sleep(3 * c.node(l).cfg.ElectionTimeoutMax)
	if s := c.node(l).StatusSnapshot(); s.Role != "leader" || s.Term != term {
		t.Fatalf("leader lost office during a slow snapshot transfer: role=%s term %d -> %d", s.Role, term, s.Term)
	}
	c.waitValue(lag, "k19", "v") // and the snapshot did arrive
}
