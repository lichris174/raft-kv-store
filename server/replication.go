package main

import (
	"context"
	"log"
	"time"

	raftpb "github.com/chrisli/raft-kv-store/proto"
)

// Log replication: proposing, sending entries and snapshots, commit and apply,
// compaction, and the follower-side AppendEntries and InstallSnapshot handlers.

// Propose appends an entry to the leader's log and blocks until it commits.
func (r *Raft) Propose(ctx context.Context, op, key, value string) error {
	r.mu.Lock()
	if r.state != Leader {
		r.mu.Unlock()
		return ErrNotLeader
	}
	// Register the waiter first: in a one-member cluster the entry commits and
	// applies inside leaderAppendLocked.
	index := r.lastIndexLocked() + 1
	ch := make(chan error, 1)
	r.waiters[index] = ch
	if err := r.leaderAppendLocked(op, key, value); err != nil {
		delete(r.waiters, index)
		r.mu.Unlock()
		return err
	}
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

// leaderAppendLocked durably appends one entry to the leader's log, then
// replicates it. An entry that is not on the leader's disk is never sent or
// acknowledged. Commit is re-evaluated right away because a one-member cluster
// has no follower reply that would do it.
func (r *Raft) leaderAppendLocked(op, key, value string) error {
	e := &raftpb.LogEntry{Term: r.currentTerm, Index: r.lastIndexLocked() + 1, Op: op, Key: key, Value: value}
	if err := r.appendLogLocked([]*raftpb.LogEntry{e}); err != nil {
		return err
	}
	r.logEntries = append(r.logEntries, e)
	r.advanceCommitLocked()
	r.broadcastReplicateLocked()
	return nil
}

// --- leader side ---

func (r *Raft) broadcastReplicateLocked() {
	for _, peer := range r.cfg.Peers {
		r.replicateToLocked(peer)
	}
}

// replicateToLocked sends peer what it is missing: log entries from nextIndex
// (at most maxBatch; none makes it a heartbeat), or the snapshot when those
// entries are compacted away. Only one request per peer is in flight. When it
// returns, the next batch follows at once if the peer is still behind, so
// proposals made in the meantime travel together instead of one RPC each.
func (r *Raft) replicateToLocked(peer string) {
	if r.inflight[peer] == r.currentTerm {
		return
	}
	next := max(r.nextIndex[peer], 1)
	if next <= r.snapshotIndex {
		r.sendSnapshotLocked(peer)
		return
	}
	r.inflight[peer] = r.currentTerm
	term := r.currentTerm
	args := &raftpb.AppendEntriesArgs{
		Term: term, LeaderId: r.cfg.Addr, LeaderCommit: r.commitIndex,
		PrevLogIndex: next - 1, PrevLogTerm: r.termAtLocked(next - 1),
	}
	if pos := int(next - r.snapshotIndex - 1); pos < len(r.logEntries) {
		// Aliasing r.logEntries is safe: the log is only ever appended to in place;
		// truncation and compaction build a new slice.
		args.Entries = r.logEntries[pos:min(len(r.logEntries), pos+maxBatch)]
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		defer cancel()
		reply, err := r.trans.SendAppendEntries(ctx, peer, args)
		r.mu.Lock()
		defer r.mu.Unlock()
		r.sendDoneLocked(peer, term)
		if err != nil || r.staleReplyLocked(Leader, term, reply.Term) {
			return // a failed send is retried by the next heartbeat
		}
		r.lastAck[peer] = time.Now()
		if reply.Success {
			r.recordMatchLocked(peer, args.PrevLogIndex+uint64(len(args.Entries)))
		} else {
			// Rejected: the follower lacks the prev entry or holds another term
			// there. Jump to just past its last entry (one step back for a
			// conflict), never below what it is known to match.
			r.nextIndex[peer] = max(r.matchIndex[peer]+1, min(args.PrevLogIndex, reply.LastIndex+1))
		}
		if r.nextIndex[peer] <= r.lastIndexLocked() && (reply.Success || r.nextIndex[peer] < next) {
			r.replicateToLocked(peer) // still behind and making progress: keep going
		}
	}()
}

// heartbeatLocked sends peer an empty AppendEntries that asserts leadership
// without touching replication state: prev index 0 always matches, and the
// follower commits nothing from it. Used while a real request to the peer is
// still in flight.
func (r *Raft) heartbeatLocked(peer string) {
	term := r.currentTerm
	args := &raftpb.AppendEntriesArgs{Term: term, LeaderId: r.cfg.Addr, LeaderCommit: r.commitIndex}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		defer cancel()
		reply, err := r.trans.SendAppendEntries(ctx, peer, args)
		r.mu.Lock()
		defer r.mu.Unlock()
		if err == nil && !r.staleReplyLocked(Leader, term, reply.Term) {
			r.lastAck[peer] = time.Now()
		}
	}()
}

// sendSnapshotLocked ships the current snapshot to a peer whose next entry has
// been compacted away.
func (r *Raft) sendSnapshotLocked(peer string) {
	r.inflight[peer] = r.currentTerm
	term := r.currentTerm
	args := &raftpb.InstallSnapshotArgs{
		Term: term, LeaderId: r.cfg.Addr,
		LastIncludedIndex: r.snapshotIndex, LastIncludedTerm: r.snapshotTerm, Data: r.snapshotData,
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), snapshotTimeout)
		defer cancel()
		reply, err := r.trans.SendInstallSnapshot(ctx, peer, args)
		r.mu.Lock()
		defer r.mu.Unlock()
		r.sendDoneLocked(peer, term)
		if err != nil || r.staleReplyLocked(Leader, term, reply.Term) {
			return
		}
		r.lastAck[peer] = time.Now()
		r.recordMatchLocked(peer, args.LastIncludedIndex)
		if r.nextIndex[peer] <= r.lastIndexLocked() {
			r.replicateToLocked(peer)
		}
	}()
}

// sendDoneLocked clears peer's in-flight mark, unless a send from a newer term
// has taken it over since.
func (r *Raft) sendDoneLocked(peer string, term uint64) {
	if r.inflight[peer] == term {
		delete(r.inflight, peer)
	}
}

// recordMatchLocked notes that peer's log matches ours through index.
func (r *Raft) recordMatchLocked(peer string, index uint64) {
	if index > r.matchIndex[peer] {
		r.matchIndex[peer] = index
		r.nextIndex[peer] = index + 1
		r.advanceCommitLocked()
	}
}

// advanceCommitLocked commits the highest index stored on a majority. Only an
// entry from the current term commits by counting replicas (Raft figure 8);
// earlier entries commit along with it.
func (r *Raft) advanceCommitLocked() {
	if r.state != Leader {
		return
	}
	for n := r.lastIndexLocked(); n > r.commitIndex && r.termAtLocked(n) == r.currentTerm; n-- {
		count := 1
		for _, p := range r.cfg.Peers {
			if r.matchIndex[p] >= n {
				count++
			}
		}
		if count >= r.majorityLocked() {
			r.commitIndex = n
			break
		}
	}
	r.applyCommittedLocked()
}

// applyCommittedLocked applies committed entries in order, wakes the proposers
// waiting on them, and compacts the log when it has grown enough.
func (r *Raft) applyCommittedLocked() {
	for r.lastApplied < r.commitIndex {
		e := r.entryAtLocked(r.lastApplied + 1)
		if e == nil {
			break // not yet present locally (e.g. snapshot boundary)
		}
		r.lastApplied = e.Index
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
	threshold := uint64(r.cfg.SnapshotThreshold)
	if r.cfg.Snapshot == nil || r.lastApplied < r.snapshotIndex+threshold || r.lastApplied < r.compactRetryAt {
		return
	}
	meta := r.metaLocked()
	meta.SnapshotIndex, meta.SnapshotTerm = r.lastApplied, r.termAtLocked(r.lastApplied)
	data := r.cfg.Snapshot()
	tail := r.logAfterLocked(r.lastApplied)
	if err := r.rewriteLocked(meta, data, tail); err != nil {
		// Nothing is lost: the files on disk still hold every applied entry. Keep
		// the uncompacted log and try again after another threshold's worth of
		// entries, rather than stalling every apply on a failing disk.
		r.compactRetryAt = r.lastApplied + threshold
		log.Printf("[%s] WARN compaction not persisted, will retry: %v", r.cfg.ID, err)
		return
	}
	compacted := meta.SnapshotIndex - r.snapshotIndex
	r.snapshotIndex, r.snapshotTerm, r.snapshotData, r.logEntries = meta.SnapshotIndex, meta.SnapshotTerm, data, tail
	log.Printf("[%s] snapshot @ index=%d (compacted %d entries)", r.cfg.ID, r.snapshotIndex, compacted)
}

// --- follower side ---

// acceptLeaderLocked is the part AppendEntries and InstallSnapshot share: the
// sender leads a term at least as new as ours, so follow it.
func (r *Raft) acceptLeaderLocked(term uint64, leader string) error {
	if err := r.becomeFollowerLocked(term); err != nil {
		return err
	}
	r.leaderAddr = leader
	r.lastLeaderContact = time.Now()
	r.resetElectionDeadlineLocked()
	return nil
}

func (r *Raft) AppendEntries(_ context.Context, args *raftpb.AppendEntriesArgs) (*raftpb.AppendEntriesReply, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	reply := &raftpb.AppendEntriesReply{Term: r.currentTerm}
	if args.Term < r.currentTerm {
		return reply, nil
	}
	if err := r.acceptLeaderLocked(args.Term, args.LeaderId); err != nil {
		return reply, err
	}
	reply.Term = r.currentTerm
	if !r.matchesPrevLocked(args) {
		reply.LastIndex = r.lastIndexLocked() // tell the leader where to resume
		return reply, nil
	}

	// Build the new log, persist it, then adopt it: a failed write changes
	// nothing, the RPC errors and the leader retries. This must also happen before
	// applying, since applying can compact, which rewrites the log file from memory.
	newLog, appended, truncated := r.mergeEntriesLocked(args.Entries)
	var err error
	if truncated {
		err = r.rewriteLocked(r.metaLocked(), nil, newLog)
	} else {
		err = r.appendLogLocked(appended)
	}
	if err != nil {
		return reply, err
	}
	r.logEntries = newLog

	// Commit only entries this message verified against the leader's log. The
	// follower's own last index is not enough: with prevLogIndex=0 (e.g. a
	// read-index heartbeat) the check passes trivially while the follower may hold
	// an old leader's uncommitted entries. Raft figure 2: min(leaderCommit, index
	// of last new entry).
	if c := min(args.LeaderCommit, args.PrevLogIndex+uint64(len(args.Entries)), r.lastIndexLocked()); c > r.commitIndex {
		r.commitIndex = c
		r.applyCommittedLocked()
	}
	reply.Success = true
	return reply, nil
}

// matchesPrevLocked is the AppendEntries consistency check: the batch is
// contiguous after PrevLogIndex, and we hold the leader's entry there. A prev
// index inside our snapshot is committed here, so it always matches.
func (r *Raft) matchesPrevLocked(args *raftpb.AppendEntriesArgs) bool {
	for i, e := range args.Entries {
		if e.Index != args.PrevLogIndex+1+uint64(i) {
			return false
		}
	}
	switch {
	case args.PrevLogIndex < r.snapshotIndex:
		return true
	case args.PrevLogIndex == r.snapshotIndex:
		return args.PrevLogTerm == r.snapshotTerm
	default:
		e := r.entryAtLocked(args.PrevLogIndex)
		return e != nil && e.Term == args.PrevLogTerm
	}
}

// mergeEntriesLocked returns the log after applying entries, without touching
// r.logEntries: new entries are appended; at the first conflicting one (same
// index, other term) our entry and everything after it are replaced. appended
// lists the entries a clean append adds; truncated reports a conflict.
func (r *Raft) mergeEntriesLocked(entries []*raftpb.LogEntry) (newLog, appended []*raftpb.LogEntry, truncated bool) {
	newLog = r.logEntries
	for _, e := range entries {
		if e.Index <= r.snapshotIndex {
			continue // already covered by snapshot
		}
		switch pos := int(e.Index - r.snapshotIndex - 1); {
		case pos >= len(newLog):
			newLog = append(newLog, e)
			appended = append(appended, e)
		case newLog[pos].Term != e.Term:
			// The capped slice forces a copy, so r.logEntries stays intact.
			newLog = append(newLog[:pos:pos], e)
			truncated = true
		}
	}
	return newLog, appended, truncated
}

func (r *Raft) InstallSnapshot(_ context.Context, args *raftpb.InstallSnapshotArgs) (*raftpb.InstallSnapshotReply, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	reply := &raftpb.InstallSnapshotReply{Term: r.currentTerm}
	if args.Term < r.currentTerm {
		return reply, nil
	}
	if err := r.acceptLeaderLocked(args.Term, args.LeaderId); err != nil {
		return reply, err
	}
	reply.Term = r.currentTerm
	if args.LastIncludedIndex <= r.lastApplied {
		// Our state machine already reflects everything the snapshot covers (and
		// possibly more); restoring it would roll state back. This also covers a
		// stale or duplicate snapshot, since snapshotIndex <= lastApplied.
		return reply, nil
	}

	// Keep our log after the snapshot only if our entry at its boundary has the
	// same term (Raft InstallSnapshot rule 6). A different term means our log is
	// from another history, and the tail after it cannot be trusted either.
	var tail []*raftpb.LogEntry
	if b := r.entryAtLocked(args.LastIncludedIndex); b != nil && b.Term == args.LastIncludedTerm {
		tail = r.logAfterLocked(args.LastIncludedIndex)
	}
	meta := r.metaLocked()
	meta.SnapshotIndex, meta.SnapshotTerm = args.LastIncludedIndex, args.LastIncludedTerm
	data := args.Data
	if data == nil {
		data = []byte{} // gRPC decodes empty bytes as nil, which Rewrite reads as "keep the old file"
	}
	// Persist before changing anything, so a failed write leaves this node as it
	// was and the leader simply re-sends the snapshot.
	if err := r.rewriteLocked(meta, data, tail); err != nil {
		return reply, err
	}
	if r.cfg.Restore != nil {
		r.cfg.Restore(data)
	}
	r.snapshotIndex, r.snapshotTerm, r.snapshotData, r.logEntries = meta.SnapshotIndex, meta.SnapshotTerm, data, tail
	r.lastApplied = args.LastIncludedIndex
	r.commitIndex = max(r.commitIndex, args.LastIncludedIndex)
	r.applyCommittedLocked() // a kept tail may hold entries that are already committed
	log.Printf("[%s] installed snapshot @ index=%d term=%d", r.cfg.ID, r.snapshotIndex, r.snapshotTerm)
	return reply, nil
}
