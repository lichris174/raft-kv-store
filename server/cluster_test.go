package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	raftpb "github.com/chrisli/raft-kv-store/proto"
)

// Test harness shared by the Raft tests: an in-process network that can crash
// nodes, partition them and lose or delay messages, a cluster wrapper that can
// restart nodes from their persisted state, and continuous safety checks.

// --- network ---

var errUnreachable = errors.New("peer unreachable")

// inmemNet delivers RPCs between nodes by direct method call.
type inmemNet struct {
	mu       sync.Mutex
	nodes    map[string]*Raft
	dead     map[string]bool // crashed: sends nothing, receives nothing
	group    map[string]int  // partition: nodes only reach nodes in the same group
	rng      *rand.Rand
	drop     float64       // chance that a request or a reply is lost
	maxDelay time.Duration // each delivered message waits up to this long
	// snapshotDelay holds every InstallSnapshot this long (bounded by the RPC's
	// context, as a real slow transfer would be), to model a large snapshot.
	snapshotDelay time.Duration
}

func newInmemNet() *inmemNet {
	return &inmemNet{
		nodes: map[string]*Raft{}, dead: map[string]bool{}, group: map[string]int{},
		rng: rand.New(rand.NewSource(1)),
	}
}

// endpoint returns the Transport a node at addr sends through.
func (n *inmemNet) endpoint(addr string) *endpoint { return &endpoint{net: n, from: addr} }

func (n *inmemNet) register(addr string, r *Raft) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.nodes[addr] = r
}

// setDown cuts a node off in both directions (a crash or full isolation).
func (n *inmemNet) setDown(addr string, down bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.dead[addr] = down
}

// partition splits the listed groups from each other and from everyone not
// listed (who stay together in group 0). heal undoes it.
func (n *inmemNet) partition(groups ...[]string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.group = map[string]int{}
	for i, g := range groups {
		for _, addr := range g {
			n.group[addr] = i + 1
		}
	}
}

func (n *inmemNet) heal() { n.partition() }

// setUnreliable makes every message subject to loss and random delay, drawn
// from a generator seeded with seed so a failing run can be replayed.
func (n *inmemNet) setUnreliable(seed int64, drop float64, maxDelay time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.rng, n.drop, n.maxDelay = rand.New(rand.NewSource(seed)), drop, maxDelay
}

// route decides whether one message from -> to gets through, and returns the
// receiving node. sender, if set, must still be the registered node at from: a
// crashed incarnation's leftover goroutines must not speak for its successor.
func (n *inmemNet) route(sender *Raft, from, to string) (*Raft, error) {
	n.mu.Lock()
	target := n.nodes[to]
	blocked := target == nil || n.dead[from] || n.dead[to] || n.group[from] != n.group[to] ||
		(sender != nil && n.nodes[from] != sender) || (n.drop > 0 && n.rng.Float64() < n.drop)
	var delay time.Duration
	if n.maxDelay > 0 {
		delay = time.Duration(n.rng.Int63n(int64(n.maxDelay)))
	}
	n.mu.Unlock()
	if blocked {
		return nil, errUnreachable
	}
	time.Sleep(delay)
	return target, nil
}

// deliver carries one RPC and its reply; either leg can be lost.
func deliver[R any](e *endpoint, to string, handle func(*Raft) (R, error)) (R, error) {
	var zero R
	target, err := e.net.route(e.self, e.from, to)
	if err != nil {
		return zero, err
	}
	reply, err := handle(target)
	if err != nil {
		return zero, err
	}
	if _, err := e.net.route(nil, to, e.from); err != nil {
		return zero, err
	}
	return reply, nil
}

// endpoint is one node's view of the network; it implements Transport.
type endpoint struct {
	net  *inmemNet
	from string
	self *Raft // set once the node exists; nil means "do not check"
}

func (e *endpoint) SendRequestVote(ctx context.Context, peer string, args *raftpb.RequestVoteArgs) (*raftpb.RequestVoteReply, error) {
	return deliver(e, peer, func(t *Raft) (*raftpb.RequestVoteReply, error) { return t.RequestVote(ctx, args) })
}

func (e *endpoint) SendAppendEntries(ctx context.Context, peer string, args *raftpb.AppendEntriesArgs) (*raftpb.AppendEntriesReply, error) {
	return deliver(e, peer, func(t *Raft) (*raftpb.AppendEntriesReply, error) { return t.AppendEntries(ctx, args) })
}

func (e *endpoint) SendInstallSnapshot(ctx context.Context, peer string, args *raftpb.InstallSnapshotArgs) (*raftpb.InstallSnapshotReply, error) {
	e.net.mu.Lock()
	delay := e.net.snapshotDelay
	e.net.mu.Unlock()
	select {
	case <-time.After(delay):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return deliver(e, peer, func(t *Raft) (*raftpb.InstallSnapshotReply, error) { return t.InstallSnapshot(ctx, args) })
}

// --- cluster ---

// fastConfig uses short timeouts so elections converge quickly, with enough
// margin (an election timeout of 7-15 heartbeats) that a scheduler stall on a
// loaded machine does not trigger spurious elections or check-quorum.
func fastConfig(id, addr string, peers []string) Config {
	return Config{
		ID:                 id,
		Addr:               addr,
		Peers:              peers,
		HeartbeatInterval:  20 * time.Millisecond,
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
	}
}

// fencedPersister is one node incarnation's handle on its durable state. After
// a crash it refuses writes, so goroutines the dead incarnation left running
// cannot overwrite what its restarted successor loaded.
type fencedPersister struct {
	Persister
	dead atomic.Bool
}

var errFenced = errors.New("node has crashed")

func (f *fencedPersister) AppendLog(e []*raftpb.LogEntry) error {
	if f.dead.Load() {
		return errFenced
	}
	return f.Persister.AppendLog(e)
}

func (f *fencedPersister) SaveMeta(m Meta) error {
	if f.dead.Load() {
		return errFenced
	}
	return f.Persister.SaveMeta(m)
}

func (f *fencedPersister) Rewrite(m Meta, snap []byte, tail []*raftpb.LogEntry) error {
	if f.dead.Load() {
		return errFenced
	}
	return f.Persister.Rewrite(m, snap, tail)
}

// testCluster is n running nodes ("a0".."a{n-1}"), each with its own state
// machine and an in-memory persister so it can be crashed and restarted.
type testCluster struct {
	t         *testing.T
	net       *inmemNet
	addrs     []string
	persist   []*memPersister
	threshold int
	safety    *safetyChecker
	stopWatch chan struct{}

	mu      sync.Mutex // guards the fields below; restarts swap nodes while others read
	nodes   []*Raft
	stores  []*KVStore
	fences  []*fencedPersister
	crashed map[int]bool
}

// newCluster starts n nodes. threshold is the snapshot threshold (0 = default).
// Everything is stopped, and the safety checks are enforced, at test cleanup.
func newCluster(t *testing.T, n, threshold int) *testCluster {
	t.Helper()
	c := &testCluster{
		t: t, net: newInmemNet(), threshold: threshold,
		addrs: make([]string, n), nodes: make([]*Raft, n), stores: make([]*KVStore, n), fences: make([]*fencedPersister, n),
		persist: make([]*memPersister, n), safety: newSafetyChecker(), stopWatch: make(chan struct{}),
		crashed: map[int]bool{},
	}
	for i := range c.addrs {
		c.addrs[i] = fmt.Sprintf("a%d", i)
		c.persist[i] = &memPersister{}
	}
	for i := range c.addrs {
		c.start(i)
	}
	go c.watchLeaders()
	t.Cleanup(func() {
		close(c.stopWatch)
		for i := range c.addrs {
			c.node(i).Stop()
		}
		if v := c.safety.violations(); len(v) > 0 {
			t.Errorf("safety violations:\n%v", v)
		}
	})
	return c
}

// start (re)creates node i from its persister with a fresh state machine and
// runs it. Used for the initial start and for restarts after a crash.
func (c *testCluster) start(i int) {
	addr := c.addrs[i]
	var peers []string
	for _, a := range c.addrs {
		if a != addr {
			peers = append(peers, a)
		}
	}
	store := NewKVStore()
	cfg := fastConfig(addr, addr, peers)
	cfg.Apply = func(e *raftpb.LogEntry) {
		c.safety.applied(addr, e)
		applyOp(store, e.GetOp(), e.GetKey(), e.GetValue())
	}
	cfg.Snapshot = store.Snapshot
	cfg.Restore = store.Restore
	cfg.SnapshotThreshold = c.threshold
	fence := &fencedPersister{Persister: c.persist[i]}
	cfg.Persister = fence
	ep := c.net.endpoint(addr)
	r := NewRaft(cfg, ep)
	ep.self = r
	c.mu.Lock()
	c.nodes[i], c.stores[i], c.fences[i] = r, store, fence
	c.mu.Unlock()
	c.net.register(addr, r)
	go r.Run()
}

func (c *testCluster) node(i int) *Raft {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[i]
}

func (c *testCluster) store(i int) *KVStore {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stores[i]
}

// crash stops node i and cuts it off; its persisted state survives.
func (c *testCluster) crash(i int) {
	c.net.setDown(c.addrs[i], true)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nodes[i].Stop()
	c.fences[i].dead.Store(true)
	c.crashed[i] = true
}

// restart brings a crashed node back from its persisted state.
func (c *testCluster) restart(i int) {
	c.start(i)
	c.net.setDown(c.addrs[i], false)
	c.mu.Lock()
	delete(c.crashed, i)
	c.mu.Unlock()
}

// live returns the indexes of nodes that are not crashed.
func (c *testCluster) live() []int {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []int
	for i := range c.addrs {
		if !c.crashed[i] {
			out = append(out, i)
		}
	}
	return out
}

// leaderOf returns the index of the leader with the highest term among the
// given nodes (all live nodes when none are given), or -1. A crashed node still
// reports its last role, so it is excluded by default.
func (c *testCluster) leaderOf(among ...int) int {
	if len(among) == 0 {
		among = c.live()
	}
	best, bestTerm := -1, uint64(0)
	for _, i := range among {
		if s := c.node(i).StatusSnapshot(); s.Role == "leader" && s.Term >= bestTerm {
			best, bestTerm = i, s.Term
		}
	}
	return best
}

// waitLeader waits for a leader among the given nodes (default: all).
func (c *testCluster) waitLeader(among ...int) int {
	c.t.Helper()
	var l int
	if !eventually(3*time.Second, func() bool { l = c.leaderOf(among...); return l >= 0 }) {
		c.t.Fatalf("no leader elected among %v", among)
	}
	return l
}

// propose writes key=value through whichever node leads, retrying through
// elections. It reports whether the write was acknowledged (committed).
func (c *testCluster) propose(key, value string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if l := c.leaderOf(); l >= 0 {
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			err := c.node(l).Propose(ctx, "set", key, value)
			cancel()
			if err == nil {
				return true
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// mustPropose is propose that fails the test if the write is not committed.
func (c *testCluster) mustPropose(key, value string) {
	c.t.Helper()
	if !c.propose(key, value, 3*time.Second) {
		c.t.Fatalf("write %s=%s was never committed", key, value)
	}
}

// waitValue waits until node i's state machine holds key=want.
func (c *testCluster) waitValue(i int, key, want string) {
	c.t.Helper()
	if !eventually(3*time.Second, func() bool { v, ok := c.store(i).Get(key); return ok && v == want }) {
		v, _ := c.store(i).Get(key)
		c.t.Fatalf("node %s: %s=%q, want %q", c.addrs[i], key, v, want)
	}
}

// waitConverged waits until the given nodes (default: all live) hold identical
// state machines with commitIndex at least minCommit, and returns that state.
func (c *testCluster) waitConverged(minCommit uint64, among ...int) map[string]string {
	c.t.Helper()
	if len(among) == 0 {
		among = c.live()
	}
	var snap []byte
	ok := eventually(5*time.Second, func() bool {
		snap = nil
		for _, i := range among {
			if c.node(i).StatusSnapshot().CommitIndex < minCommit {
				return false
			}
			s := c.store(i).Snapshot() // json.Marshal sorts keys, so equal maps give equal bytes
			if snap != nil && string(s) != string(snap) {
				return false
			}
			snap = s
		}
		return true
	})
	if !ok {
		for _, i := range among {
			c.t.Logf("%s: %+v keys=%d", c.addrs[i], c.node(i).StatusSnapshot(), c.store(i).Len())
		}
		c.t.Fatalf("nodes %v did not converge", among)
	}
	state := NewKVStore()
	state.Restore(snap)
	return state.data
}

// watchLeaders polls roles and records two leaders in one term (election safety).
func (c *testCluster) watchLeaders() {
	tick := time.NewTicker(2 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-c.stopWatch:
			return
		case <-tick.C:
		}
		for _, i := range c.live() {
			if s := c.node(i).StatusSnapshot(); s.Role == "leader" {
				c.safety.leader(s.Term, c.addrs[i])
			}
		}
	}
}

// eventually polls cond every 5ms until it holds or timeout passes.
func eventually(timeout time.Duration, cond func() bool) bool {
	for deadline := time.Now().Add(timeout); ; time.Sleep(5 * time.Millisecond) {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
	}
}

// --- safety invariants ---

// safetyChecker records what every node applies and who led each term, and
// flags violations of two Raft guarantees:
//   - election safety: at most one leader per term;
//   - state machine safety: every node applies the same entry at a given index.
type safetyChecker struct {
	mu       sync.Mutex
	leaders  map[uint64]string // term -> leader
	entries  map[uint64]string // index -> entry applied there
	problems []string
}

func newSafetyChecker() *safetyChecker {
	return &safetyChecker{leaders: map[uint64]string{}, entries: map[uint64]string{}}
}

func (s *safetyChecker) leader(term uint64, addr string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if prev, ok := s.leaders[term]; ok && prev != addr {
		s.problems = append(s.problems, fmt.Sprintf("term %d has two leaders: %s and %s", term, prev, addr))
	}
	s.leaders[term] = addr
}

func (s *safetyChecker) applied(addr string, e *raftpb.LogEntry) {
	got := fmt.Sprintf("term=%d %s %s=%s", e.Term, e.Op, e.Key, e.Value)
	s.mu.Lock()
	defer s.mu.Unlock()
	if prev, ok := s.entries[e.Index]; ok && prev != got {
		s.problems = append(s.problems, fmt.Sprintf("index %d: %s applied {%s}, another node applied {%s}", e.Index, addr, got, prev))
	}
	s.entries[e.Index] = got
}

func (s *safetyChecker) violations() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.problems...)
}
