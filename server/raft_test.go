package main

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// Cluster-level tests of the basic Raft behaviors: election, replication,
// read-index, snapshots and recovery. They run real nodes over the in-memory
// network from cluster_test.go, whose cleanup also fails a test on any safety
// violation (two leaders in a term, or nodes applying different entries).

func TestElection_OneMemberClusterLeadsItself(t *testing.T) {
	c := newCluster(t, 1, 0)
	c.waitLeader()
	c.mustPropose("k", "v") // commits with no follower to acknowledge it
	c.waitValue(0, "k", "v")
}

func TestElection_ThreeNodesElectOneLeader(t *testing.T) {
	c := newCluster(t, 3, 0)
	c.waitLeader()
}

func TestElection_LeaderFailureReelects(t *testing.T) {
	c := newCluster(t, 3, 0)
	old := c.waitLeader()
	oldTerm := c.node(old).StatusSnapshot().Term

	c.crash(old)
	l := c.waitLeader()
	if l == old {
		t.Fatal("the crashed node cannot be leader")
	}
	if term := c.node(l).StatusSnapshot().Term; term <= oldTerm {
		t.Fatalf("new leader term %d should exceed old term %d", term, oldTerm)
	}
}

func TestElection_FiveNodeStableLeader(t *testing.T) {
	c := newCluster(t, 5, 0)
	l := c.waitLeader()
	term := c.node(l).StatusSnapshot().Term

	// Heartbeats must keep the leader in place: no spurious elections.
	time.Sleep(500 * time.Millisecond)
	if s := c.node(l).StatusSnapshot(); s.Role != "leader" || s.Term != term {
		t.Fatalf("leadership changed with no failure: role=%s term %d -> %d", s.Role, term, s.Term)
	}
}

func TestReplication_CommitAndApplyAllNodes(t *testing.T) {
	c := newCluster(t, 3, 0)
	c.waitLeader()
	c.mustPropose("foo", "bar")
	for i := range c.addrs {
		c.waitValue(i, "foo", "bar")
	}
}

func TestReplication_FollowerRejectsPropose(t *testing.T) {
	c := newCluster(t, 3, 0)
	follower := (c.waitLeader() + 1) % 3
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := c.node(follower).Propose(ctx, "set", "k", "v"); err != ErrNotLeader {
		t.Fatalf("follower Propose err = %v, want ErrNotLeader", err)
	}
}

func TestReplication_MultipleWritesOrdered(t *testing.T) {
	c := newCluster(t, 3, 0)
	l := c.waitLeader()
	for i := 0; i < 5; i++ {
		c.mustPropose("counter", fmt.Sprint(i))
	}
	for i := range c.addrs {
		c.waitValue(i, "counter", "4") // last write wins everywhere
	}
	if ci := c.node(l).StatusSnapshot().CommitIndex; ci < 5 {
		t.Fatalf("leader commitIndex=%d, want >= 5", ci)
	}
}

func TestReadIndex_ConfirmLeadership(t *testing.T) {
	c := newCluster(t, 3, 0)
	l := c.waitLeader()
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if !c.node(l).ConfirmLeadership(ctx) {
		t.Fatal("healthy leader should confirm leadership")
	}

	// Cut the leader off from both followers: it can no longer reach a majority.
	// Ask straight away, well inside the check-quorum window, so the refusal has
	// to come from the failed heartbeat round, not from having stepped down.
	c.net.partition([]string{c.addrs[l]})
	ctx2, cancel2 := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel2()
	if c.node(l).ConfirmLeadership(ctx2) {
		t.Fatal("partitioned leader must not confirm leadership")
	}
	if !c.node(l).IsLeader() {
		t.Fatal("leader stepped down before the read was refused; the test proves nothing")
	}
}

func TestSnapshot_CompactsLog(t *testing.T) {
	c := newCluster(t, 3, 5)
	l := c.waitLeader()
	const writes = 20
	for i := 0; i < writes; i++ {
		c.mustPropose(fmt.Sprintf("k%02d", i), "v")
	}
	s := c.node(l).StatusSnapshot()
	if s.SnapshotIndex == 0 || s.LogLen >= writes {
		t.Fatalf("log not compacted: snapshotIndex=%d logLen=%d", s.SnapshotIndex, s.LogLen)
	}
	c.waitConverged(writes)
}

func TestSnapshot_LaggingFollowerCatchesUpViaInstallSnapshot(t *testing.T) {
	c := newCluster(t, 3, 5)
	l := c.waitLeader()
	f := (l + 1) % 3

	// The majority makes progress and compacts while one follower is cut off.
	c.net.partition([]string{c.addrs[f]})
	for i := 0; i < 20; i++ {
		c.mustPropose(fmt.Sprintf("k%02d", i), "v")
	}
	if c.node(c.leaderOf()).StatusSnapshot().SnapshotIndex == 0 {
		t.Fatal("leader should have compacted while the follower was away")
	}

	// The follower's next entry is now inside the leader's snapshot, so the
	// leader must ship the snapshot itself.
	c.net.heal()
	c.waitValue(f, "k19", "v")
	if c.node(f).StatusSnapshot().SnapshotIndex == 0 {
		t.Fatal("reconnected follower should have installed the snapshot")
	}
	c.waitConverged(20)
}

func TestPersistence_RestartRestoresSnapshotAndLog(t *testing.T) {
	c := newCluster(t, 3, 4) // compacts every 4 entries, so both a snapshot and a tail exist
	l := c.waitLeader()
	for i := 0; i < 10; i++ {
		c.mustPropose(fmt.Sprintf("k%02d", i), "val")
	}
	before := c.node(l).StatusSnapshot()
	if before.SnapshotIndex == 0 {
		t.Fatal("expected a snapshot before the restart")
	}

	c.crash(l)
	c.restart(l)
	// Straight after recovery, before any leader contact: the snapshot is
	// restored and the term and log tail came back from disk.
	after := c.node(l).StatusSnapshot()
	if after.Term < before.Term || after.SnapshotIndex != before.SnapshotIndex {
		t.Fatalf("recovered %+v, crashed with %+v", after, before)
	}
	if v, ok := c.store(l).Get("k00"); !ok || v != "val" {
		t.Fatalf("snapshot not restored: k00=%q", v)
	}
	c.waitValue(l, "k09", "val") // the rest is re-applied once a leader commits it
}

// Every acknowledged write must survive the leader crashing mid-stream.
func TestFault_NoDataLossAcrossLeaderKill(t *testing.T) {
	c := newCluster(t, 5, 0)
	c.waitLeader()
	const total = 40
	for i := 0; i < total; i++ {
		if i == 15 {
			c.crash(c.waitLeader()) // 4 of 5 remain: still a majority
		}
		c.mustPropose(fmt.Sprintf("k%02d", i), fmt.Sprintf("v%02d", i))
	}
	state := c.waitConverged(total)
	for i := 0; i < total; i++ {
		if k, want := fmt.Sprintf("k%02d", i), fmt.Sprintf("v%02d", i); state[k] != want {
			t.Fatalf("acknowledged write lost: %s=%q, want %q", k, state[k], want)
		}
	}
}
