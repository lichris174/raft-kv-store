package main

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	raftpb "github.com/chrisli/raft-kv-store/proto"
)

// State is a Raft node role.
type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Leader:
		return "leader"
	case Candidate:
		return "candidate"
	default:
		return "follower"
	}
}

// ErrNotLeader is returned by Propose when this node is not the leader.
var ErrNotLeader = errors.New("not leader")

// seedCounter guarantees distinct RNG streams even when many nodes are created
// within the same clock tick (Windows time resolution is ~15ms, so a plain
// time-based seed would collide and cause synchronized election timeouts).
var seedCounter atomic.Int64

func nodeSeed(addr string) int64 {
	seed := uint64(time.Now().UnixNano()) ^ (uint64(seedCounter.Add(1)) * 0x9E3779B97F4A7C15)
	for _, b := range []byte(addr) { // mix the address (FNV-1a style)
		seed = (seed ^ uint64(b)) * 1099511628211
	}
	return int64(seed)
}

// Transport abstracts sending Raft RPCs to peers so the core is testable
// without real networking (see grpcTransport for the production implementation).
type Transport interface {
	SendRequestVote(ctx context.Context, peer string, args *raftpb.RequestVoteArgs) (*raftpb.RequestVoteReply, error)
	SendAppendEntries(ctx context.Context, peer string, args *raftpb.AppendEntriesArgs) (*raftpb.AppendEntriesReply, error)
	SendInstallSnapshot(ctx context.Context, peer string, args *raftpb.InstallSnapshotArgs) (*raftpb.InstallSnapshotReply, error)
}

// Config configures a Raft node. Timeouts are exposed so tests can shrink them.
type Config struct {
	ID                 string
	Addr               string
	Peers              []string
	HeartbeatInterval  time.Duration
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration

	// Apply applies a committed log entry to the state machine, in log order,
	// exactly once per entry, while the Raft lock is held.
	Apply func(entry *raftpb.LogEntry)
	// Snapshot serializes the state machine for compaction (nil disables it).
	Snapshot func() []byte
	// Restore loads the state machine from a snapshot blob.
	Restore func(data []byte)
	// SnapshotThreshold is the number of applied entries past the last snapshot
	// that triggers a new snapshot + log truncation. 0 uses a default.
	SnapshotThreshold int
	// Persister durably stores Raft state across restarts (nil disables it).
	Persister Persister
}

func (c Config) withDefaults() Config {
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 100 * time.Millisecond
	}
	if c.ElectionTimeoutMin == 0 {
		c.ElectionTimeoutMin = 500 * time.Millisecond
	}
	if c.ElectionTimeoutMax == 0 {
		c.ElectionTimeoutMax = 1000 * time.Millisecond
	}
	if c.SnapshotThreshold == 0 {
		c.SnapshotThreshold = 100
	}
	return c
}

const tickInterval = 15 * time.Millisecond
const rpcTimeout = 120 * time.Millisecond

// Raft is a single consensus node. All mutable state is guarded by mu.
// Network IO is always performed off-lock in goroutines.
type Raft struct {
	mu    sync.Mutex
	cfg   Config
	trans Transport
	rnd   *rand.Rand

	// persistent state (durably saved via Persister)
	currentTerm uint64
	votedFor    string
	// logEntries holds entries with Index > snapshotIndex. The first entry, if
	// any, has Index == snapshotIndex+1.
	logEntries    []*raftpb.LogEntry
	snapshotIndex uint64 // last index included in the snapshot
	snapshotTerm  uint64
	snapshotData  []byte

	// volatile
	state       State
	leaderAddr  string
	commitIndex uint64
	lastApplied uint64

	// leader-only volatile
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	waiters map[uint64]chan error

	electionDeadline time.Time
	nextHeartbeat    time.Time

	stopCh  chan struct{}
	stopped bool
}

// NewRaft creates a node in the Follower state, recovering durable state from
// the configured Persister if present. Call Run to start the loop.
func NewRaft(cfg Config, trans Transport) *Raft {
	cfg = cfg.withDefaults()
	r := &Raft{
		cfg:        cfg,
		trans:      trans,
		rnd:        rand.New(rand.NewSource(nodeSeed(cfg.Addr))),
		state:      Follower,
		nextIndex:  map[string]uint64{},
		matchIndex: map[string]uint64{},
		waiters:    map[uint64]chan error{},
		stopCh:     make(chan struct{}),
	}
	r.recover()
	r.resetElectionDeadlineLocked()
	return r
}

// recover loads persisted state and restores the state machine.
func (r *Raft) recover() {
	if r.cfg.Persister == nil {
		return
	}
	meta, snap, logTail, err := r.cfg.Persister.Load()
	if err != nil {
		log.Printf("[%s] WARN load persisted state: %v", r.cfg.ID, err)
		return
	}
	r.currentTerm = meta.CurrentTerm
	r.votedFor = meta.VotedFor
	r.snapshotIndex = meta.SnapshotIndex
	r.snapshotTerm = meta.SnapshotTerm
	r.snapshotData = snap
	// Keep only log entries past the snapshot (defensive against any overlap).
	for _, e := range logTail {
		if e.Index > r.snapshotIndex {
			r.logEntries = append(r.logEntries, e)
		}
	}
	// commitIndex/lastApplied are volatile; restart from the snapshot anchor.
	// Committed-but-unsnapshotted entries are re-committed and re-applied (set/
	// del are idempotent) once a leader re-establishes them.
	r.commitIndex = r.snapshotIndex
	r.lastApplied = r.snapshotIndex
	if r.cfg.Restore != nil && snap != nil {
		r.cfg.Restore(snap)
	}
	if meta.CurrentTerm > 0 || r.snapshotIndex > 0 || len(r.logEntries) > 0 {
		log.Printf("[%s] recovered: term=%d snapshotIdx=%d logTail=%d", r.cfg.ID, r.currentTerm, r.snapshotIndex, len(r.logEntries))
	}
}

func (r *Raft) metaLocked() Meta {
	return Meta{CurrentTerm: r.currentTerm, VotedFor: r.votedFor, SnapshotIndex: r.snapshotIndex, SnapshotTerm: r.snapshotTerm}
}

// saveMetaLocked persists term/vote/snapshot metadata (small, infrequent).
func (r *Raft) saveMetaLocked() {
	if r.cfg.Persister == nil {
		return
	}
	if err := r.cfg.Persister.SaveMeta(r.metaLocked()); err != nil {
		log.Printf("[%s] WARN saveMeta: %v", r.cfg.ID, err)
	}
}

// appendLogLocked durably appends new log entries (the write hot path).
func (r *Raft) appendLogLocked(entries []*raftpb.LogEntry) {
	if r.cfg.Persister == nil || len(entries) == 0 {
		return
	}
	if err := r.cfg.Persister.AppendLog(entries); err != nil {
		log.Printf("[%s] WARN appendLog: %v", r.cfg.ID, err)
	}
}

// rewriteLocked persists a full snapshot+log rewrite (rare: compaction,
// snapshot install, or a log truncation on conflict).
func (r *Raft) rewriteLocked() {
	if r.cfg.Persister == nil {
		return
	}
	if err := r.cfg.Persister.Rewrite(r.metaLocked(), r.snapshotData, r.logEntries); err != nil {
		log.Printf("[%s] WARN rewrite: %v", r.cfg.ID, err)
	}
}

func (r *Raft) Run() {
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			r.tick()
		}
	}
}

func (r *Raft) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.stopped {
		r.stopped = true
		close(r.stopCh)
	}
}

func (r *Raft) tick() {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	if r.state == Leader {
		if now.After(r.nextHeartbeat) {
			r.nextHeartbeat = now.Add(r.cfg.HeartbeatInterval)
			r.broadcastReplicateLocked()
		}
		return
	}
	if now.After(r.electionDeadline) {
		r.startElectionLocked()
	}
}

func (r *Raft) resetElectionDeadlineLocked() {
	span := r.cfg.ElectionTimeoutMax - r.cfg.ElectionTimeoutMin
	d := r.cfg.ElectionTimeoutMin
	if span > 0 && r.rnd != nil {
		d += time.Duration(r.rnd.Int63n(int64(span)))
	}
	r.electionDeadline = time.Now().Add(d)
}

// --- log helpers (snapshot-offset aware; indices are 1-based, global) ---

func (r *Raft) lastIndexLocked() uint64 {
	if len(r.logEntries) == 0 {
		return r.snapshotIndex
	}
	return r.logEntries[len(r.logEntries)-1].Index
}

func (r *Raft) lastLogLocked() (index, term uint64) {
	idx := r.lastIndexLocked()
	return idx, r.termAtLocked(idx)
}

func (r *Raft) entryAtLocked(index uint64) *raftpb.LogEntry {
	if index <= r.snapshotIndex {
		return nil // compacted away or zero
	}
	pos := int(index - r.snapshotIndex - 1)
	if pos < 0 || pos >= len(r.logEntries) {
		return nil
	}
	return r.logEntries[pos]
}

func (r *Raft) termAtLocked(index uint64) uint64 {
	if index == r.snapshotIndex {
		return r.snapshotTerm
	}
	if e := r.entryAtLocked(index); e != nil {
		return e.Term
	}
	return 0
}

func (r *Raft) majorityLocked() int { return (len(r.cfg.Peers)+1)/2 + 1 }

// --- role transitions ---

func (r *Raft) becomeFollowerLocked(term uint64) {
	termChanged := term > r.currentTerm
	if termChanged {
		r.currentTerm = term
		r.votedFor = ""
	}
	wasLeader := r.state == Leader
	r.state = Follower
	if wasLeader {
		r.failAllWaitersLocked(ErrNotLeader)
	}
	if termChanged {
		r.saveMetaLocked()
	}
}

func (r *Raft) failAllWaitersLocked(err error) {
	for idx, ch := range r.waiters {
		ch <- err
		delete(r.waiters, idx)
	}
}

func (r *Raft) startElectionLocked() {
	r.state = Candidate
	r.currentTerm++
	r.votedFor = r.cfg.Addr
	r.leaderAddr = ""
	r.resetElectionDeadlineLocked()
	r.saveMetaLocked()

	term := r.currentTerm
	lastIdx, lastTerm := r.lastLogLocked()
	needed := r.majorityLocked()
	votes := 1
	log.Printf("[%s] start election term=%d (need %d)", r.cfg.ID, term, needed)

	for _, peer := range r.cfg.Peers {
		go func(peer string) {
			ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
			defer cancel()
			args := &raftpb.RequestVoteArgs{Term: term, CandidateId: r.cfg.Addr, LastLogIndex: lastIdx, LastLogTerm: lastTerm}
			reply, err := r.trans.SendRequestVote(ctx, peer, args)
			if err != nil {
				return
			}
			r.mu.Lock()
			defer r.mu.Unlock()
			if r.state != Candidate || r.currentTerm != term {
				return
			}
			if reply.Term > r.currentTerm {
				r.becomeFollowerLocked(reply.Term)
				return
			}
			if reply.VoteGranted {
				votes++
				if votes >= needed {
					r.becomeLeaderLocked()
				}
			}
		}(peer)
	}
}

func (r *Raft) becomeLeaderLocked() {
	if r.state == Leader {
		return
	}
	r.state = Leader
	r.leaderAddr = r.cfg.Addr
	last := r.lastIndexLocked()
	// Commit a per-term no-op so prior-term entries commit indirectly and
	// read-index reads have a committed current-term anchor. applyOp ignores it.
	noop := &raftpb.LogEntry{Term: r.currentTerm, Index: last + 1, Op: "noop"}
	r.logEntries = append(r.logEntries, noop)
	for _, p := range r.cfg.Peers {
		r.nextIndex[p] = last + 1
		r.matchIndex[p] = 0
	}
	r.nextHeartbeat = time.Now().Add(r.cfg.HeartbeatInterval)
	r.appendLogLocked([]*raftpb.LogEntry{noop})
	log.Printf("[%s] became LEADER term=%d", r.cfg.ID, r.currentTerm)
	r.broadcastReplicateLocked()
}

// --- replication (leader side) ---

func (r *Raft) broadcastReplicateLocked() {
	for _, peer := range r.cfg.Peers {
		r.replicateToLocked(peer)
	}
}

func (r *Raft) replicateToLocked(peer string) {
	next := r.nextIndex[peer]
	if next < 1 {
		next = 1
	}
	// Peer is behind our snapshot: ship the snapshot instead of log entries.
	if next <= r.snapshotIndex {
		r.sendSnapshotLocked(peer)
		return
	}

	term := r.currentTerm
	leader := r.cfg.Addr
	commit := r.commitIndex
	prevIndex := next - 1
	prevTerm := r.termAtLocked(prevIndex)

	var entries []*raftpb.LogEntry
	if e := r.entryAtLocked(next); e != nil {
		pos := int(next - r.snapshotIndex - 1)
		entries = append(entries, r.logEntries[pos:]...)
	}

	go func(peer string, entries []*raftpb.LogEntry, prevIndex, prevTerm uint64) {
		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		defer cancel()
		args := &raftpb.AppendEntriesArgs{
			Term:         term,
			LeaderId:     leader,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevTerm,
			Entries:      entries,
			LeaderCommit: commit,
		}
		reply, err := r.trans.SendAppendEntries(ctx, peer, args)
		if err != nil {
			return
		}
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.state != Leader || r.currentTerm != term {
			return
		}
		if reply.Term > r.currentTerm {
			r.becomeFollowerLocked(reply.Term)
			return
		}
		if reply.Success {
			match := prevIndex + uint64(len(entries))
			if match > r.matchIndex[peer] {
				r.matchIndex[peer] = match
				r.nextIndex[peer] = match + 1
				r.advanceCommitLocked()
			}
		} else if r.nextIndex[peer] > 1 {
			r.nextIndex[peer]--
		}
	}(peer, entries, prevIndex, prevTerm)
}

func (r *Raft) sendSnapshotLocked(peer string) {
	term := r.currentTerm
	leader := r.cfg.Addr
	sidx := r.snapshotIndex
	sterm := r.snapshotTerm
	data := append([]byte(nil), r.snapshotData...)

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		defer cancel()
		args := &raftpb.InstallSnapshotArgs{
			Term:              term,
			LeaderId:          leader,
			LastIncludedIndex: sidx,
			LastIncludedTerm:  sterm,
			Data:              data,
		}
		reply, err := r.trans.SendInstallSnapshot(ctx, peer, args)
		if err != nil {
			return
		}
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.state != Leader || r.currentTerm != term {
			return
		}
		if reply.Term > r.currentTerm {
			r.becomeFollowerLocked(reply.Term)
			return
		}
		if sidx > r.matchIndex[peer] {
			r.matchIndex[peer] = sidx
			r.nextIndex[peer] = sidx + 1
			r.advanceCommitLocked()
		}
	}()
}

func (r *Raft) advanceCommitLocked() {
	if r.state != Leader {
		return
	}
	for n := r.commitIndex + 1; n <= r.lastIndexLocked(); n++ {
		if r.termAtLocked(n) != r.currentTerm {
			continue
		}
		count := 1
		for _, p := range r.cfg.Peers {
			if r.matchIndex[p] >= n {
				count++
			}
		}
		if count >= r.majorityLocked() {
			r.commitIndex = n
		}
	}
	r.applyCommittedLocked()
}

func (r *Raft) applyCommittedLocked() {
	for r.lastApplied < r.commitIndex {
		next := r.lastApplied + 1
		e := r.entryAtLocked(next)
		if e == nil {
			break // not yet present locally (e.g. snapshot boundary)
		}
		r.lastApplied = next
		if r.cfg.Apply != nil {
			r.cfg.Apply(e)
		}
		if ch, ok := r.waiters[e.Index]; ok {
			ch <- nil
			delete(r.waiters, e.Index)
		}
	}
	r.maybeSnapshotLocked()
}

// maybeSnapshotLocked compacts the log once enough entries have been applied.
func (r *Raft) maybeSnapshotLocked() {
	if r.cfg.Snapshot == nil {
		return
	}
	if r.lastApplied <= r.snapshotIndex {
		return
	}
	if int(r.lastApplied-r.snapshotIndex) < r.cfg.SnapshotThreshold {
		return
	}
	newTerm := r.termAtLocked(r.lastApplied)
	data := r.cfg.Snapshot()
	var tail []*raftpb.LogEntry
	for _, e := range r.logEntries {
		if e.Index > r.lastApplied {
			tail = append(tail, e)
		}
	}
	old := r.snapshotIndex
	r.snapshotIndex = r.lastApplied
	r.snapshotTerm = newTerm
	r.snapshotData = data
	r.logEntries = tail
	r.rewriteLocked()
	log.Printf("[%s] snapshot @ index=%d (compacted %d entries)", r.cfg.ID, r.snapshotIndex, r.snapshotIndex-old)
}

// Propose appends an entry to the leader's log and blocks until it commits.
func (r *Raft) Propose(ctx context.Context, op, key, value string) error {
	r.mu.Lock()
	if r.state != Leader {
		r.mu.Unlock()
		return ErrNotLeader
	}
	index := r.lastIndexLocked() + 1
	entry := &raftpb.LogEntry{Term: r.currentTerm, Index: index, Op: op, Key: key, Value: value}
	r.logEntries = append(r.logEntries, entry)
	ch := make(chan error, 1)
	r.waiters[index] = ch
	r.appendLogLocked([]*raftpb.LogEntry{entry})
	r.broadcastReplicateLocked()
	r.mu.Unlock()

	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		r.mu.Lock()
		delete(r.waiters, index)
		r.mu.Unlock()
		return ctx.Err()
	}
}

// --- RPC handlers ---

func (r *Raft) RequestVote(_ context.Context, args *raftpb.RequestVoteArgs) (*raftpb.RequestVoteReply, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	reply := &raftpb.RequestVoteReply{Term: r.currentTerm}
	if args.Term < r.currentTerm {
		return reply, nil
	}
	if args.Term > r.currentTerm {
		r.becomeFollowerLocked(args.Term)
	}
	reply.Term = r.currentTerm

	lastIdx, lastTerm := r.lastLogLocked()
	upToDate := args.LastLogTerm > lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIdx)
	if (r.votedFor == "" || r.votedFor == args.CandidateId) && upToDate {
		r.votedFor = args.CandidateId
		r.resetElectionDeadlineLocked()
		reply.VoteGranted = true
		r.saveMetaLocked()
	}
	return reply, nil
}

func (r *Raft) AppendEntries(_ context.Context, args *raftpb.AppendEntriesArgs) (*raftpb.AppendEntriesReply, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	reply := &raftpb.AppendEntriesReply{Term: r.currentTerm}
	if args.Term < r.currentTerm {
		return reply, nil
	}
	if args.Term > r.currentTerm {
		r.becomeFollowerLocked(args.Term)
	} else {
		r.state = Follower
	}
	r.leaderAddr = args.LeaderId
	r.resetElectionDeadlineLocked()
	reply.Term = r.currentTerm

	// Log consistency check against prevLogIndex/prevLogTerm.
	if args.PrevLogIndex > r.snapshotIndex {
		if r.termAtLocked(args.PrevLogIndex) != args.PrevLogTerm {
			return reply, nil // gap or conflict: leader backs off
		}
	} else if args.PrevLogIndex == r.snapshotIndex && args.PrevLogIndex > 0 {
		if args.PrevLogTerm != r.snapshotTerm {
			return reply, nil
		}
	}
	// (args.PrevLogIndex < snapshotIndex: covered by our snapshot; skip below.)

	var appended []*raftpb.LogEntry
	truncated := false
	for _, e := range args.Entries {
		if e.Index <= r.snapshotIndex {
			continue // already covered by snapshot
		}
		existing := r.entryAtLocked(e.Index)
		if existing == nil {
			r.logEntries = append(r.logEntries, e)
			appended = append(appended, e)
		} else if existing.Term != e.Term {
			r.logEntries = r.logEntries[:e.Index-r.snapshotIndex-1] // truncate conflict
			r.logEntries = append(r.logEntries, e)
			truncated = true
		}
	}

	// Persist the log change BEFORE applying committed entries. applyCommittedLocked
	// can trigger a snapshot, which rewrites the whole log file (rewriteLocked) from
	// the compacted in-memory tail. If we appended AFTER that rewrite, the new
	// entries would be written twice (the rewrite already contains them), leaving a
	// duplicate at the snapshot boundary that desyncs the index->offset arithmetic
	// and lets log gaps form. Persisting first keeps disk and memory consistent.
	// A conflict truncation needs a full rewrite; a clean append is the hot path.
	if truncated {
		r.rewriteLocked()
	} else if len(appended) > 0 {
		r.appendLogLocked(appended)
	}

	if args.LeaderCommit > r.commitIndex {
		r.commitIndex = args.LeaderCommit
		if last := r.lastIndexLocked(); last < r.commitIndex {
			r.commitIndex = last
		}
		r.applyCommittedLocked()
	}

	reply.Success = true
	return reply, nil
}

func (r *Raft) InstallSnapshot(_ context.Context, args *raftpb.InstallSnapshotArgs) (*raftpb.InstallSnapshotReply, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	reply := &raftpb.InstallSnapshotReply{Term: r.currentTerm}
	if args.Term < r.currentTerm {
		return reply, nil
	}
	if args.Term > r.currentTerm {
		r.becomeFollowerLocked(args.Term)
	} else {
		r.state = Follower
	}
	r.leaderAddr = args.LeaderId
	r.resetElectionDeadlineLocked()
	reply.Term = r.currentTerm

	if args.LastIncludedIndex <= r.snapshotIndex {
		return reply, nil // stale or duplicate snapshot
	}

	if r.cfg.Restore != nil {
		r.cfg.Restore(args.Data)
	}
	r.snapshotIndex = args.LastIncludedIndex
	r.snapshotTerm = args.LastIncludedTerm
	r.snapshotData = append([]byte(nil), args.Data...)

	// Discard log entries the snapshot now covers.
	var tail []*raftpb.LogEntry
	for _, e := range r.logEntries {
		if e.Index > args.LastIncludedIndex {
			tail = append(tail, e)
		}
	}
	r.logEntries = tail
	if r.commitIndex < args.LastIncludedIndex {
		r.commitIndex = args.LastIncludedIndex
	}
	if r.lastApplied < args.LastIncludedIndex {
		r.lastApplied = args.LastIncludedIndex
	}
	r.rewriteLocked()
	log.Printf("[%s] installed snapshot @ index=%d term=%d", r.cfg.ID, r.snapshotIndex, r.snapshotTerm)
	return reply, nil
}

// --- status accessors ---

type StatusSnapshot struct {
	Role          string
	Term          uint64
	LeaderAddr    string
	CommitIndex   uint64
	LastApplied   uint64
	SnapshotIndex uint64
	LogLen        int
}

func (r *Raft) StatusSnapshot() StatusSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	return StatusSnapshot{
		Role:          r.state.String(),
		Term:          r.currentTerm,
		LeaderAddr:    r.leaderAddr,
		CommitIndex:   r.commitIndex,
		LastApplied:   r.lastApplied,
		SnapshotIndex: r.snapshotIndex,
		LogLen:        len(r.logEntries),
	}
}

func (r *Raft) IsLeader() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state == Leader
}

func (r *Raft) LeaderAddr() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.leaderAddr
}

// ConfirmLeadership implements a read-index-lite check: the leader confirms it
// still commands a majority via a heartbeat round before serving a linearizable
// read. Returns false if it cannot confirm (caller should redirect).
func (r *Raft) ConfirmLeadership(ctx context.Context) bool {
	r.mu.Lock()
	if r.state != Leader {
		r.mu.Unlock()
		return false
	}
	term := r.currentTerm
	commit := r.commitIndex
	leader := r.cfg.Addr
	peers := append([]string(nil), r.cfg.Peers...)
	needed := r.majorityLocked()
	r.mu.Unlock()

	if len(peers) == 0 {
		return true
	}
	type ack struct {
		ok     bool
		higher bool
	}
	results := make(chan ack, len(peers))
	for _, p := range peers {
		go func(p string) {
			cctx, cancel := context.WithTimeout(ctx, rpcTimeout)
			defer cancel()
			args := &raftpb.AppendEntriesArgs{Term: term, LeaderId: leader, LeaderCommit: commit}
			reply, err := r.trans.SendAppendEntries(cctx, p, args)
			if err != nil {
				results <- ack{}
				return
			}
			results <- ack{ok: reply.Success, higher: reply.Term > term}
		}(p)
	}
	votes := 1
	for i := 0; i < len(peers); i++ {
		select {
		case a := <-results:
			if a.higher {
				return false
			}
			if a.ok {
				votes++
				if votes >= needed {
					return true
				}
			}
		case <-ctx.Done():
			return false
		}
	}
	return votes >= needed
}
