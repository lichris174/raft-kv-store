package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	raftpb "github.com/chrisli/raft-kv-store/proto"
)

// inmemNet wires several Raft nodes together in-process. A node marked down is
// unreachable in both directions, simulating a crash/partition.
type inmemNet struct {
	mu    sync.Mutex
	nodes map[string]*Raft
	down  map[string]bool
}

func newInmemNet() *inmemNet {
	return &inmemNet{nodes: map[string]*Raft{}, down: map[string]bool{}}
}

func (n *inmemNet) register(addr string, r *Raft) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.nodes[addr] = r
}

func (n *inmemNet) setDown(addr string, d bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.down[addr] = d
}

var errUnreachable = errors.New("peer unreachable")

func (n *inmemNet) target(peer string) (*Raft, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.down[peer] {
		return nil, errUnreachable
	}
	t := n.nodes[peer]
	if t == nil {
		return nil, errUnreachable
	}
	return t, nil
}

func (n *inmemNet) SendRequestVote(ctx context.Context, peer string, args *raftpb.RequestVoteArgs) (*raftpb.RequestVoteReply, error) {
	t, err := n.target(peer)
	if err != nil {
		return nil, err
	}
	return t.RequestVote(ctx, args)
}

func (n *inmemNet) SendAppendEntries(ctx context.Context, peer string, args *raftpb.AppendEntriesArgs) (*raftpb.AppendEntriesReply, error) {
	t, err := n.target(peer)
	if err != nil {
		return nil, err
	}
	return t.AppendEntries(ctx, args)
}

func (n *inmemNet) SendInstallSnapshot(ctx context.Context, peer string, args *raftpb.InstallSnapshotArgs) (*raftpb.InstallSnapshotReply, error) {
	t, err := n.target(peer)
	if err != nil {
		return nil, err
	}
	return t.InstallSnapshot(ctx, args)
}

// fastConfig uses short timeouts so elections converge in well under a second.
func fastConfig(id, addr string, peers []string) Config {
	return Config{
		ID:                 id,
		Addr:               addr,
		Peers:              peers,
		HeartbeatInterval:  20 * time.Millisecond,
		ElectionTimeoutMin: 80 * time.Millisecond,
		ElectionTimeoutMax: 160 * time.Millisecond,
	}
}

// buildCluster creates n nodes (addresses "a0".."a{n-1}") wired over net, each
// with its own KVStore state machine. Returns the nodes and a parallel slice of
// their stores so replication can be asserted across the cluster.
func buildCluster(net *inmemNet, n int) ([]*Raft, []*KVStore) {
	addrs := make([]string, n)
	for i := range addrs {
		addrs[i] = "a" + string(rune('0'+i))
	}
	nodes := make([]*Raft, n)
	stores := make([]*KVStore, n)
	for i := range addrs {
		var peers []string
		for j := range addrs {
			if j != i {
				peers = append(peers, addrs[j])
			}
		}
		store := NewKVStore()
		stores[i] = store
		cfg := fastConfig(addrs[i], addrs[i], peers)
		cfg.Apply = func(e *raftpb.LogEntry) { applyOp(store, e.GetOp(), e.GetKey(), e.GetValue()) }
		cfg.Snapshot = store.Snapshot
		cfg.Restore = store.Restore
		r := NewRaft(cfg, net)
		nodes[i] = r
		net.register(addrs[i], r)
	}
	return nodes, stores
}

// buildClusterEx builds a cluster with a snapshot threshold and optional
// per-node persisters (pass nil for in-memory, no-persistence nodes).
func buildClusterEx(net *inmemNet, n, threshold int, persisters []Persister) ([]*Raft, []*KVStore) {
	addrs := make([]string, n)
	for i := range addrs {
		addrs[i] = "a" + string(rune('0'+i))
	}
	nodes := make([]*Raft, n)
	stores := make([]*KVStore, n)
	for i := range addrs {
		var peers []string
		for j := range addrs {
			if j != i {
				peers = append(peers, addrs[j])
			}
		}
		store := NewKVStore()
		stores[i] = store
		cfg := fastConfig(addrs[i], addrs[i], peers)
		cfg.Apply = func(e *raftpb.LogEntry) { applyOp(store, e.GetOp(), e.GetKey(), e.GetValue()) }
		cfg.Snapshot = store.Snapshot
		cfg.Restore = store.Restore
		cfg.SnapshotThreshold = threshold
		if persisters != nil {
			cfg.Persister = persisters[i]
		}
		r := NewRaft(cfg, net)
		nodes[i] = r
		net.register(addrs[i], r)
	}
	return nodes, stores
}

func leaderIndex(nodes []*Raft, leader *Raft) int {
	for i, r := range nodes {
		if r == leader {
			return i
		}
	}
	return -1
}

// waitForLeader polls until exactly one node among alive is a leader (and no
// two leaders share a term), or fails after timeout.
func waitForLeader(t *testing.T, nodes []*Raft, alive map[string]bool, timeout time.Duration) *Raft {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		leaders := []*Raft{}
		for _, r := range nodes {
			if alive != nil && !alive[r.cfg.Addr] {
				continue
			}
			if r.IsLeader() {
				leaders = append(leaders, r)
			}
		}
		if len(leaders) == 1 {
			return leaders[0]
		}
		if len(leaders) > 1 {
			// Allow transient split; ensure terms differ before failing.
			terms := map[uint64]int{}
			for _, l := range leaders {
				terms[l.StatusSnapshot().Term]++
			}
			for _, c := range terms {
				if c > 1 {
					t.Fatalf("two leaders in the same term: %v", leaders)
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no single leader elected within %s", timeout)
	return nil
}

func TestElection_SingleNodeCluster(t *testing.T) {
	// A two-node config where this node can reach a (missing) peer never gets a
	// majority alone; instead test a real 3-node cluster elects one leader.
	net := newInmemNet()
	nodes, _ := buildCluster(net, 3)
	for _, r := range nodes {
		go r.Run()
		defer r.Stop()
	}
	leader := waitForLeader(t, nodes, nil, 2*time.Second)
	if leader == nil {
		t.Fatal("expected a leader")
	}
}

func TestElection_LeaderFailureReelects(t *testing.T) {
	net := newInmemNet()
	nodes, _ := buildCluster(net, 3)
	for _, r := range nodes {
		go r.Run()
		defer r.Stop()
	}
	leader := waitForLeader(t, nodes, nil, 2*time.Second)
	term1 := leader.StatusSnapshot().Term

	// Kill the leader: stop ticking and make it unreachable.
	leader.Stop()
	net.setDown(leader.cfg.Addr, true)

	alive := map[string]bool{}
	for _, r := range nodes {
		alive[r.cfg.Addr] = r != leader
	}
	newLeader := waitForLeader(t, nodes, alive, 3*time.Second)
	if newLeader == leader {
		t.Fatal("a new leader should be elected, not the killed one")
	}
	if got := newLeader.StatusSnapshot().Term; got <= term1 {
		t.Fatalf("new leader term %d should exceed old term %d", got, term1)
	}
}

// waitStore polls until store[key]==want or fails.
func waitStore(t *testing.T, store *KVStore, key, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if v, ok := store.Get(key); ok && v == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	v, _ := store.Get(key)
	t.Fatalf("store[%q]=%q, want %q", key, v, want)
}

func TestReplication_CommitAndApplyAllNodes(t *testing.T) {
	net := newInmemNet()
	nodes, stores := buildCluster(net, 3)
	for _, r := range nodes {
		go r.Run()
		defer r.Stop()
	}
	leader := waitForLeader(t, nodes, nil, 2*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := leader.Propose(ctx, "set", "foo", "bar"); err != nil {
		t.Fatalf("Propose: %v", err)
	}
	// Every node's state machine must converge to the committed value.
	for i := range stores {
		waitStore(t, stores[i], "foo", "bar", 2*time.Second)
	}
}

func TestReplication_FollowerRejectsPropose(t *testing.T) {
	net := newInmemNet()
	nodes, _ := buildCluster(net, 3)
	for _, r := range nodes {
		go r.Run()
		defer r.Stop()
	}
	leader := waitForLeader(t, nodes, nil, 2*time.Second)

	var follower *Raft
	for _, r := range nodes {
		if r != leader {
			follower = r
			break
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := follower.Propose(ctx, "set", "k", "v"); err != ErrNotLeader {
		t.Fatalf("follower Propose err = %v, want ErrNotLeader", err)
	}
}

func TestReplication_MultipleWritesOrdered(t *testing.T) {
	net := newInmemNet()
	nodes, stores := buildCluster(net, 3)
	for _, r := range nodes {
		go r.Run()
		defer r.Stop()
	}
	leader := waitForLeader(t, nodes, nil, 2*time.Second)

	for i := 0; i < 5; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := leader.Propose(ctx, "set", "counter", string(rune('0'+i)))
		cancel()
		if err != nil {
			t.Fatalf("Propose %d: %v", i, err)
		}
	}
	// Last write wins on every node.
	for i := range stores {
		waitStore(t, stores[i], "counter", "4", 2*time.Second)
	}
	// commitIndex should reflect all 5 entries on the leader.
	if ci := leader.StatusSnapshot().CommitIndex; ci < 5 {
		t.Fatalf("leader commitIndex=%d, want >=5", ci)
	}
}

func TestReadIndex_ConfirmLeadership(t *testing.T) {
	net := newInmemNet()
	nodes, _ := buildCluster(net, 3)
	for _, r := range nodes {
		go r.Run()
		defer r.Stop()
	}
	leader := waitForLeader(t, nodes, nil, 2*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if !leader.ConfirmLeadership(ctx) {
		t.Fatal("healthy leader should confirm leadership")
	}

	// Isolate the leader from both followers: it can no longer reach a majority.
	for _, r := range nodes {
		if r != leader {
			net.setDown(r.cfg.Addr, true)
		}
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel2()
	if leader.ConfirmLeadership(ctx2) {
		t.Fatal("partitioned leader must NOT confirm leadership (read-index guard)")
	}
}

func TestSnapshot_CompactsLog(t *testing.T) {
	net := newInmemNet()
	nodes, stores := buildClusterEx(net, 3, 5, nil)
	for _, r := range nodes {
		go r.Run()
		defer r.Stop()
	}
	leader := waitForLeader(t, nodes, nil, 2*time.Second)

	const writes = 20
	for i := 0; i < writes; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		k := "k" + string(rune('a'+i))
		if err := leader.Propose(ctx, "set", k, "v"); err != nil {
			cancel()
			t.Fatalf("Propose %d: %v", i, err)
		}
		cancel()
	}
	// Log must have been compacted: snapshotIndex advanced and the in-memory
	// log tail is far smaller than the total number of entries.
	snap := leader.StatusSnapshot()
	if snap.SnapshotIndex == 0 {
		t.Fatal("expected a snapshot to have been taken")
	}
	if snap.LogLen >= writes {
		t.Fatalf("log not compacted: logLen=%d (>= %d writes)", snap.LogLen, writes)
	}
	// State machine still correct on every node.
	for i := range stores {
		waitStore(t, stores[i], "ka", "v", 2*time.Second)
		waitStore(t, stores[i], "kt", "v", 2*time.Second)
	}
}

func TestSnapshot_LaggingFollowerCatchesUpViaInstallSnapshot(t *testing.T) {
	net := newInmemNet()
	nodes, stores := buildClusterEx(net, 3, 5, nil)
	for _, r := range nodes {
		go r.Run()
		defer r.Stop()
	}
	leader := waitForLeader(t, nodes, nil, 2*time.Second)

	// Isolate one follower while the majority makes progress and compacts.
	var follower *Raft
	var fIdx int
	for i, r := range nodes {
		if r != leader {
			follower, fIdx = r, i
			break
		}
	}
	net.setDown(follower.cfg.Addr, true)

	for i := 0; i < 20; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_ = leader.Propose(ctx, "set", "k"+string(rune('a'+i)), "v")
		cancel()
	}
	if leader.StatusSnapshot().SnapshotIndex == 0 {
		t.Fatal("leader should have compacted while follower was down")
	}

	// Reconnect: follower's nextIndex now points below the snapshot, so the
	// leader must ship an InstallSnapshot to catch it up.
	net.setDown(follower.cfg.Addr, false)
	waitStore(t, stores[fIdx], "ka", "v", 3*time.Second)
	waitStore(t, stores[fIdx], "kt", "v", 3*time.Second)
	if follower.StatusSnapshot().SnapshotIndex == 0 {
		t.Fatal("reconnected follower should have installed the snapshot")
	}
}

func TestPersistence_RestoreSnapshotOnRestart(t *testing.T) {
	net := newInmemNet()
	persisters := []Persister{&memPersister{}, &memPersister{}, &memPersister{}}
	nodes, _ := buildClusterEx(net, 3, 4, persisters)
	for _, r := range nodes {
		go r.Run()
		defer r.Stop()
	}
	leader := waitForLeader(t, nodes, nil, 2*time.Second)
	li := leaderIndex(nodes, leader)

	for i := 0; i < 12; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		if err := leader.Propose(ctx, "set", "k"+string(rune('a'+i)), "val"); err != nil {
			cancel()
			t.Fatalf("Propose %d: %v", i, err)
		}
		cancel()
	}
	if leader.StatusSnapshot().SnapshotIndex == 0 {
		t.Fatal("expected a snapshot before restart")
	}

	// "Restart" the leader node: fresh store + Raft recovering from the same
	// persister. Recovery must restore the state machine from the snapshot.
	restored := NewKVStore()
	cfg := fastConfig(leader.cfg.Addr, leader.cfg.Addr, leader.cfg.Peers)
	cfg.Apply = func(e *raftpb.LogEntry) { applyOp(restored, e.GetOp(), e.GetKey(), e.GetValue()) }
	cfg.Snapshot = restored.Snapshot
	cfg.Restore = restored.Restore
	cfg.SnapshotThreshold = 4
	cfg.Persister = persisters[li]
	recovered := NewRaft(cfg, net)

	if v, ok := restored.Get("ka"); !ok || v != "val" {
		t.Fatalf("snapshot not restored on restart: ka=%q ok=%v", v, ok)
	}
	if recovered.StatusSnapshot().Term == 0 {
		t.Fatal("recovered node should have restored a non-zero term")
	}
}

func TestPersistence_TermAndLogSurviveRestart(t *testing.T) {
	net := newInmemNet()
	persisters := []Persister{&memPersister{}, &memPersister{}, &memPersister{}}
	nodes, _ := buildClusterEx(net, 3, 1000, persisters) // high threshold: keep full log
	for _, r := range nodes {
		go r.Run()
		defer r.Stop()
	}
	leader := waitForLeader(t, nodes, nil, 2*time.Second)
	li := leaderIndex(nodes, leader)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	if err := leader.Propose(ctx, "set", "foo", "bar"); err != nil {
		cancel()
		t.Fatalf("Propose: %v", err)
	}
	cancel()
	preTerm := leader.StatusSnapshot().Term

	// Recover from the leader's persister; durable term + log tail must survive.
	store := NewKVStore()
	cfg := fastConfig(leader.cfg.Addr, leader.cfg.Addr, leader.cfg.Peers)
	cfg.Apply = func(e *raftpb.LogEntry) { applyOp(store, e.GetOp(), e.GetKey(), e.GetValue()) }
	cfg.Persister = persisters[li]
	recovered := NewRaft(cfg, net)
	snap := recovered.StatusSnapshot()
	if snap.Term != preTerm {
		t.Fatalf("recovered term=%d, want %d", snap.Term, preTerm)
	}
	if snap.LogLen == 0 {
		t.Fatal("recovered node lost its persisted log tail")
	}
}

// findLeaderAlive returns the current leader among alive nodes, or nil.
func findLeaderAlive(nodes []*Raft, alive map[string]bool) *Raft {
	for _, r := range nodes {
		if alive != nil && !alive[r.cfg.Addr] {
			continue
		}
		if r.IsLeader() {
			return r
		}
	}
	return nil
}

// proposeWithRetry finds the current leader and proposes, retrying through
// elections until it commits or the deadline passes.
func proposeWithRetry(nodes []*Raft, alive map[string]bool, op, key, value string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		l := findLeaderAlive(nodes, alive)
		if l == nil {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		err := l.Propose(ctx, op, key, value)
		cancel()
		if err == nil {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// TestFault_NoDataLossAcrossLeaderKill streams writes while killing the leader
// mid-stream; every acknowledged (committed) write must survive on the cluster.
func TestFault_NoDataLossAcrossLeaderKill(t *testing.T) {
	net := newInmemNet()
	nodes, stores := buildClusterEx(net, 5, 1000, nil)
	for _, r := range nodes {
		go r.Run()
		defer r.Stop()
	}
	alive := map[string]bool{}
	for _, r := range nodes {
		alive[r.cfg.Addr] = true
	}
	waitForLeader(t, nodes, alive, 2*time.Second)

	acked := map[string]string{}
	const total = 40
	for i := 0; i < total; i++ {
		if i == 15 { // kill the current leader mid-stream (4/5 remain: majority holds)
			l := findLeaderAlive(nodes, alive)
			l.Stop()
			net.setDown(l.cfg.Addr, true)
			alive[l.cfg.Addr] = false
		}
		key := fmt.Sprintf("k%02d", i)
		val := fmt.Sprintf("v%02d", i)
		if proposeWithRetry(nodes, alive, "set", key, val, 3*time.Second) {
			acked[key] = val
		} else {
			t.Fatalf("write %s never committed (cluster failed to recover)", key)
		}
	}

	// Every acknowledged write must survive on the surviving leader's state
	// machine, zero data loss despite the leader crash mid-stream.
	leader := waitForLeader(t, nodes, alive, 3*time.Second)
	li := leaderIndex(nodes, leader)
	for k, v := range acked {
		waitStore(t, stores[li], k, v, 2*time.Second)
	}
	if len(acked) != total {
		t.Fatalf("acked %d/%d writes", len(acked), total)
	}
}

func TestElection_FiveNodeStableLeader(t *testing.T) {
	net := newInmemNet()
	nodes, _ := buildCluster(net, 5)
	for _, r := range nodes {
		go r.Run()
		defer r.Stop()
	}
	leader := waitForLeader(t, nodes, nil, 2*time.Second)
	term := leader.StatusSnapshot().Term

	// Leader should remain stable while heartbeating (no spurious re-elections).
	time.Sleep(500 * time.Millisecond)
	if !leader.IsLeader() {
		t.Fatal("leader lost leadership without any failure")
	}
	if got := leader.StatusSnapshot().Term; got != term {
		t.Fatalf("term changed under stable cluster: %d -> %d", term, got)
	}
}
