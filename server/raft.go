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

// This file holds the Raft node's state, lifecycle, persistence helpers and log
// arithmetic. The protocol itself lives in election.go (pre-vote, voting,
// leadership), replication.go (log replication, commit, snapshots) and
// readindex.go (linearizable reads).

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
	// OnPersistFatal is called when durable state cannot be loaded at startup.
	// Nil crash-stops the process. (Tests set it to observe the decision.)
	OnPersistFatal func(op string, err error)
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

const (
	tickInterval = 15 * time.Millisecond
	rpcTimeout   = 120 * time.Millisecond
	// snapshotTimeout is longer than rpcTimeout: a snapshot carries the whole
	// state machine, and one that cannot finish in time would be re-sent forever.
	snapshotTimeout = 2 * time.Second
	// maxBatch caps the entries in one AppendEntries, so a follower that is far
	// behind catches up in bounded messages instead of one huge one.
	maxBatch = 512
)

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
	// compactRetryAt holds back compaction after a failed write until this index
	// is applied, so a failing disk is not retried (under the lock) on every apply.
	compactRetryAt uint64

	// leader-only volatile
	nextIndex  map[string]uint64
	matchIndex map[string]uint64
	inflight   map[string]uint64 // peer -> term of its outstanding AppendEntries/InstallSnapshot

	waiters map[uint64]chan error

	electionDeadline time.Time
	nextHeartbeat    time.Time

	// lastLeaderContact is when we last accepted AppendEntries/InstallSnapshot
	// from a current leader. lastAck is, for a leader, when each peer last
	// answered us. Together they let a node refuse to be disrupted while a
	// leader is healthy, and let a leader notice it has lost its majority.
	lastLeaderContact time.Time
	lastAck           map[string]time.Time

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
		inflight:   map[string]uint64{},
		lastAck:    map[string]time.Time{},
		waiters:    map[uint64]chan error{},
		stopCh:     make(chan struct{}),
	}
	r.loadPersisted()
	r.resetElectionDeadlineLocked()
	return r
}

// loadPersisted restores durable state and the state machine.
func (r *Raft) loadPersisted() {
	if r.cfg.Persister == nil {
		return
	}
	meta, snap, logTail, err := r.cfg.Persister.Load()
	if err != nil {
		// Starting from blank state after a failed load would forget our term and
		// vote (a double-vote risk) and our log. Refuse to start instead.
		if r.cfg.OnPersistFatal != nil {
			r.cfg.OnPersistFatal("load persisted state", err)
			return
		}
		log.Fatalf("[%s] FATAL load persisted state: %v (refusing to start with blank state)", r.cfg.ID, err)
	}
	r.currentTerm = meta.CurrentTerm
	r.votedFor = meta.VotedFor
	r.snapshotIndex = meta.SnapshotIndex
	r.snapshotTerm = meta.SnapshotTerm
	r.snapshotData = snap
	// Load returns a contiguous tail starting at snapshotIndex+1; filter anyway so
	// a persister that does not cannot break the offset arithmetic.
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

// --- persistence helpers ---
//
// Raft's safety argument assumes a node never acts on or acknowledges state (a
// term, a vote, an entry) that is not durable. Every caller therefore builds the
// state it wants, persists it with one of these, and changes memory only once
// that succeeds. A failed write leaves the node exactly as it was, and the
// request that caused it fails and is retried.

func (r *Raft) metaLocked() Meta {
	return Meta{CurrentTerm: r.currentTerm, VotedFor: r.votedFor, SnapshotIndex: r.snapshotIndex, SnapshotTerm: r.snapshotTerm}
}

// saveMetaLocked persists term/vote/snapshot metadata (small, infrequent).
func (r *Raft) saveMetaLocked(meta Meta) error {
	if r.cfg.Persister == nil {
		return nil
	}
	return r.cfg.Persister.SaveMeta(meta)
}

// appendLogLocked durably appends new log entries (the write hot path).
func (r *Raft) appendLogLocked(entries []*raftpb.LogEntry) error {
	if r.cfg.Persister == nil || len(entries) == 0 {
		return nil
	}
	return r.cfg.Persister.AppendLog(entries)
}

// rewriteLocked persists a full snapshot+log rewrite (rare: compaction,
// snapshot install, or a log truncation on conflict). A nil snapshot keeps the
// one on disk.
func (r *Raft) rewriteLocked(meta Meta, snapshot []byte, tail []*raftpb.LogEntry) error {
	if r.cfg.Persister == nil {
		return nil
	}
	return r.cfg.Persister.Rewrite(meta, snapshot, tail)
}

// --- lifecycle and timers ---

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

// Stop halts the run loop. Tests use it to simulate a crash; the node process
// itself just exits.
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
	if r.state != Leader {
		if now.After(r.electionDeadline) {
			r.startElectionLocked()
		}
		return
	}
	if !r.quorumContactLocked(now, r.cfg.ElectionTimeoutMax) {
		// Check-quorum: a leader that has not heard from a majority for a full
		// election timeout cannot commit anything and a new leader may already
		// exist. Step down so clients and the dashboard stop treating it as one.
		log.Printf("[%s] lost contact with a majority, stepping down (term=%d)", r.cfg.ID, r.currentTerm)
		_ = r.becomeFollowerLocked(r.currentTerm) // same term: nothing to persist, cannot fail
		r.leaderAddr = ""
		r.resetElectionDeadlineLocked()
		return
	}
	if now.After(r.nextHeartbeat) {
		r.nextHeartbeat = now.Add(r.cfg.HeartbeatInterval)
		for _, peer := range r.cfg.Peers {
			if r.inflight[peer] == r.currentTerm {
				// A request (maybe a long snapshot) is still outstanding. Heartbeat
				// anyway, or the peer times out and we lose its quorum contact.
				r.heartbeatLocked(peer)
			} else {
				r.replicateToLocked(peer)
			}
		}
	}
}

func (r *Raft) resetElectionDeadlineLocked() {
	d := r.cfg.ElectionTimeoutMin
	if span := r.cfg.ElectionTimeoutMax - r.cfg.ElectionTimeoutMin; span > 0 {
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

// entryAtLocked returns the entry at index, or nil if it is compacted or absent.
func (r *Raft) entryAtLocked(index uint64) *raftpb.LogEntry {
	if index <= r.snapshotIndex {
		return nil
	}
	pos := int(index - r.snapshotIndex - 1)
	if pos >= len(r.logEntries) {
		return nil
	}
	return r.logEntries[pos]
}

// termAtLocked returns the term at index: the snapshot's term at its boundary,
// 0 when the entry is unknown.
func (r *Raft) termAtLocked(index uint64) uint64 {
	if index == r.snapshotIndex {
		return r.snapshotTerm
	}
	if e := r.entryAtLocked(index); e != nil {
		return e.Term
	}
	return 0
}

// logAfterLocked returns a fresh slice of the log entries with Index > index.
func (r *Raft) logAfterLocked(index uint64) []*raftpb.LogEntry {
	var tail []*raftpb.LogEntry
	for _, e := range r.logEntries {
		if e.Index > index {
			tail = append(tail, e)
		}
	}
	return tail
}

// --- cluster membership helpers ---

func (r *Raft) majorityLocked() int { return (len(r.cfg.Peers)+1)/2 + 1 }

// quorumContactLocked reports whether, counting itself, a majority of the
// cluster has answered this leader within the window.
func (r *Raft) quorumContactLocked(now time.Time, window time.Duration) bool {
	n := 1
	for _, p := range r.cfg.Peers {
		if now.Sub(r.lastAck[p]) < window {
			n++
		}
	}
	return n >= r.majorityLocked()
}

// hasLiveLeaderLocked is true while this node should believe a leader is
// healthy: a leader still in touch with a majority, or a follower that heard
// from one within the minimum election timeout.
func (r *Raft) hasLiveLeaderLocked() bool {
	now := time.Now()
	if r.state == Leader {
		return r.quorumContactLocked(now, r.cfg.ElectionTimeoutMin)
	}
	return r.leaderAddr != "" && now.Sub(r.lastLeaderContact) < r.cfg.ElectionTimeoutMin
}

// --- role transitions shared by every RPC path ---

// becomeFollowerLocked steps down to follower, adopting term if it is newer.
// A newer term is persisted first; if that fails nothing changes and the error
// is returned, so the caller drops the message that carried the term.
func (r *Raft) becomeFollowerLocked(term uint64) error {
	if term > r.currentTerm {
		meta := r.metaLocked()
		meta.CurrentTerm, meta.VotedFor = term, ""
		if err := r.saveMetaLocked(meta); err != nil {
			log.Printf("[%s] WARN cannot persist term %d, ignoring the message: %v", r.cfg.ID, term, err)
			return err
		}
		r.currentTerm, r.votedFor = term, ""
		r.leaderAddr = "" // whoever led the old term does not lead this one
	}
	if r.state == Leader {
		r.leaderAddr = "" // never hint at ourselves once we stop leading
		r.failAllWaitersLocked(ErrNotLeader)
	}
	r.state = Follower
	return nil
}

// staleReplyLocked reports whether a reply must be dropped: this node is no
// longer in the role and term that sent the request, or the reply carries a
// newer term (this node then steps down).
func (r *Raft) staleReplyLocked(role State, term, replyTerm uint64) bool {
	if r.state != role || r.currentTerm != term {
		return true
	}
	if replyTerm > r.currentTerm {
		_ = r.becomeFollowerLocked(replyTerm)
		return true
	}
	return false
}

func (r *Raft) failAllWaitersLocked(err error) {
	for idx, ch := range r.waiters {
		ch <- err
		delete(r.waiters, idx)
	}
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
