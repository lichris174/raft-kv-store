package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	raftpb "github.com/chrisli/raft-kv-store/proto"
)

// Tests of Raft's core guarantees under partitions and crashes: a minority
// cannot commit, divergent logs are repaired to the leader's, a leader never
// commits an old-term entry by counting replicas, reads wait for a current-term
// commit, and a randomized fault run that checks every invariant at once.

// A leader cut off with a minority keeps accepting proposals but can commit
// none of them. The majority elects a new leader and commits; after the
// partition heals the old leader's uncommitted entries are overwritten.
func TestPartition_MinorityLeaderCannotCommit(t *testing.T) {
	c := newCluster(t, 5, 0)
	old := c.waitLeader()
	c.mustPropose("x", "before")
	for i := range c.addrs {
		c.waitValue(i, "x", "before") // everyone has applied it before the split
	}
	minority := []int{old, (old + 1) % 5}
	majority := []int{(old + 2) % 5, (old + 3) % 5, (old + 4) % 5}
	c.net.partition([]string{c.addrs[minority[0]], c.addrs[minority[1]]})

	// The cut-off leader accepts the proposal but can never commit it. Once
	// check-quorum makes it step down, the waiting proposer is told so (rather
	// than left to time out) and nothing was acknowledged.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.node(old).Propose(ctx, "set", "x", "lost"); err != ErrNotLeader {
		t.Fatalf("Propose on a cut-off leader = %v, want ErrNotLeader after it steps down", err)
	}

	newLeader := c.waitLeader(majority...)
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	if err := c.node(newLeader).Propose(ctx2, "set", "x", "after"); err != nil {
		t.Fatalf("majority leader could not commit: %v", err)
	}
	for _, i := range minority {
		if v, _ := c.store(i).Get("x"); v != "before" {
			t.Fatalf("minority node %s applied x=%q while partitioned", c.addrs[i], v)
		}
	}

	c.net.heal()
	if state := c.waitConverged(0); state["x"] != "after" {
		t.Fatalf("after heal x=%q, want the majority's value", state["x"])
	}
	assertLogsMatch(t, c)
}

// Raft's log matching property: after any history of leader changes, once the
// cluster settles every node holds the same log. Here two successive leaders
// each strand uncommitted entries on themselves before being cut off.
func TestLogMatching_DivergentLogsAreRepaired(t *testing.T) {
	c := newCluster(t, 3, 0)
	for round := 0; round < 3; round++ {
		l := c.waitLeader()
		c.mustPropose(fmt.Sprintf("committed%d", round), "ok")
		// Isolate the leader, then hand it writes it can never commit. Each must
		// time out (accepted, never committed), not fail with ErrNotLeader: that
		// would mean nothing was stranded and the test proved nothing.
		c.net.partition([]string{c.addrs[l]})
		for i := 0; i < 3; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			err := c.node(l).Propose(ctx, "set", fmt.Sprintf("stranded%d-%d", round, i), "x")
			cancel()
			if err != context.DeadlineExceeded {
				t.Fatalf("round %d: stranded write got %v, want a timeout", round, err)
			}
		}
		others := []int{(l + 1) % 3, (l + 2) % 3}
		c.waitLeader(others...)
		c.net.heal()
	}
	state := c.waitConverged(0)
	for k := range state {
		if k[0] == 's' {
			t.Fatalf("uncommitted write %s survived on every node", k)
		}
	}
	for round := 0; round < 3; round++ {
		if state[fmt.Sprintf("committed%d", round)] != "ok" {
			t.Fatalf("committed write of round %d is missing: %v", round, state)
		}
	}
	assertLogsMatch(t, c)
}

// assertLogsMatch waits until every node's log holds the same entries (same
// index, term and command). Snapshot thresholds are high in callers, so the
// logs are complete.
func assertLogsMatch(t *testing.T, c *testCluster) {
	t.Helper()
	describe := func(i int) string {
		r := c.node(i)
		r.mu.Lock()
		defer r.mu.Unlock()
		out := ""
		for _, e := range r.logEntries {
			out += fmt.Sprintf("%d/%d:%s=%s ", e.Index, e.Term, e.Key, e.Value)
		}
		return out
	}
	var logs []string
	ok := eventually(3*time.Second, func() bool {
		logs = logs[:0]
		for i := range c.addrs {
			logs = append(logs, describe(i))
		}
		for _, l := range logs[1:] {
			if l != logs[0] {
				return false
			}
		}
		return true
	})
	if !ok {
		t.Fatalf("logs differ:\n%s", fmt.Sprint(logs))
	}
}

// Raft figure 8: a leader must not commit an entry from an earlier term just
// because a majority stores it; only entries of its own term commit by
// counting replicas (earlier ones then commit with them).
func TestCommit_OldTermEntryNeedsCurrentTermEntry(t *testing.T) {
	r, _ := soloNode(nil, nil) // members n0 (this node), n1, n2; never run
	r.mu.Lock()
	defer r.mu.Unlock()
	r.currentTerm, r.state = 3, Leader
	r.logEntries = []*raftpb.LogEntry{setEntry(2, 1, "a", "from-term-2")}
	r.matchIndex = map[string]uint64{"n1": 1, "n2": 0} // entry 1 is on a majority (n0, n1)

	r.advanceCommitLocked()
	if r.commitIndex != 0 {
		t.Fatalf("committed an old-term entry by replica count (commitIndex=%d)", r.commitIndex)
	}

	r.logEntries = append(r.logEntries, setEntry(3, 2, "b", "from-term-3"))
	r.matchIndex["n1"] = 2
	r.advanceCommitLocked()
	if r.commitIndex != 2 {
		t.Fatalf("commitIndex=%d, want 2: a current-term entry commits everything before it", r.commitIndex)
	}
}

// A freshly elected leader may lag entries its predecessor committed until it
// commits one of its own, so it must not serve a read-index read before then.
func TestReadIndex_WaitsForCurrentTermCommit(t *testing.T) {
	// One member, so the heartbeat round trivially succeeds and only the
	// current-term rule can refuse the read.
	r := NewRaft(fastConfig("n0", "n0", nil), newInmemNet().endpoint("n0"))
	r.mu.Lock()
	r.currentTerm, r.state = 2, Leader
	r.logEntries = []*raftpb.LogEntry{setEntry(1, 1, "a", "1")}
	r.commitIndex, r.lastApplied = 1, 1 // committed, but in term 1
	r.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if r.ConfirmLeadership(ctx) {
		t.Fatal("served a read before committing an entry of its own term")
	}

	// Once an entry of its own term commits, the same read goes through.
	r.mu.Lock()
	r.logEntries = append(r.logEntries, setEntry(2, 2, "b", "2"))
	r.commitIndex, r.lastApplied = 2, 2
	r.mu.Unlock()
	ctx2, cancel2 := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel2()
	if !r.ConfirmLeadership(ctx2) {
		t.Fatal("refused a read after committing in its own term")
	}
}

// A follower that missed thousands of entries catches up through capped
// batches and the rejection hint, without needing a snapshot.
func TestReplication_FarBehindFollowerCatchesUp(t *testing.T) {
	c := newCluster(t, 3, 100000) // no compaction: catch-up must use the log
	l := c.waitLeader()
	f := (l + 1) % 3
	c.crash(f)
	const writes = 1500 // about three times maxBatch
	var wg sync.WaitGroup
	for w := 0; w < 10; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := w; i < writes; i += 10 {
				if !c.propose(fmt.Sprintf("k%04d", i), "v", 3*time.Second) {
					t.Errorf("write %d was never committed", i) // Fatal is not allowed off the test goroutine
					return
				}
			}
		}(w)
	}
	wg.Wait()
	if t.Failed() {
		return
	}
	c.restart(f)
	if state := c.waitConverged(writes); len(state) != writes {
		t.Fatalf("converged on %d keys, want %d", len(state), writes)
	}
}

// TestFuzz_RandomFaults runs a 5-node cluster through random partitions,
// crashes, restarts and a lossy, delaying network while clients keep writing.
// The cluster's safety checks run throughout. At the end the network heals and
// every node must converge on a state that contains every acknowledged write.
// RAFT_FUZZ_SEEDS raises the number of seeds (default 3).
func TestFuzz_RandomFaults(t *testing.T) {
	seeds := 3
	if n, err := strconv.Atoi(os.Getenv("RAFT_FUZZ_SEEDS")); err == nil && n > 0 {
		seeds = n
	}
	for seed := int64(1); seed <= int64(seeds); seed++ {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) { fuzzOnce(t, seed) })
	}
}

func fuzzOnce(t *testing.T, seed int64) {
	// Rotate the snapshot threshold so seeds also cover constant compaction (3)
	// and none at all (100000), not just the occasional snapshot install (30).
	c := newCluster(t, 5, []int{30, 3, 100000}[seed%3])
	c.net.setUnreliable(seed, 0.05, 3*time.Millisecond)
	rng := rand.New(rand.NewSource(seed))

	var mu sync.Mutex
	acked := map[string]string{}
	stop := make(chan struct{})
	var clients sync.WaitGroup
	for w := 0; w < 3; w++ {
		clients.Add(1)
		go func(w int) {
			defer clients.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				k, v := fmt.Sprintf("w%d-%d", w, i), fmt.Sprint(i)
				if c.propose(k, v, 200*time.Millisecond) {
					mu.Lock()
					acked[k] = v
					mu.Unlock()
				}
			}
		}(w)
	}

	// Inject a random fault every 100ms for 3s, keeping at most two nodes crashed.
	for step := 0; step < 30; step++ {
		time.Sleep(100 * time.Millisecond)
		live := c.live()
		switch op := rng.Intn(4); {
		case op == 0 && len(live) > 3:
			c.crash(live[rng.Intn(len(live))])
		case op == 1 && len(live) < 5:
			for i := range c.addrs {
				if !slices.Contains(live, i) {
					c.restart(i)
					break
				}
			}
		case op == 2:
			perm := rng.Perm(5)
			cut := 1 + rng.Intn(2)
			var side []string
			for _, i := range perm[:cut] {
				side = append(side, c.addrs[i])
			}
			c.net.partition(side)
		default:
			c.net.heal()
		}
	}
	close(stop)
	clients.Wait()

	// Settle: heal the network, restart everyone, and let the cluster converge.
	c.net.setUnreliable(seed, 0, 0)
	c.net.heal()
	for i := range c.addrs {
		if !slices.Contains(c.live(), i) {
			c.restart(i)
		}
	}
	c.waitLeader()
	c.mustPropose("final", "write") // commits everything before it in the leader's term
	state := c.waitConverged(0)
	if state["final"] != "write" {
		t.Fatal("final write missing after convergence")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(acked) == 0 {
		t.Fatal("no write was acknowledged during the run; the fuzz exercised nothing")
	}
	for k, v := range acked {
		if state[k] != v {
			t.Fatalf("acknowledged write lost: %s=%q, want %q (%d acked)", k, state[k], v, len(acked))
		}
	}
	t.Logf("seed %d: %d writes acknowledged, all present on all 5 nodes", seed, len(acked))
}
