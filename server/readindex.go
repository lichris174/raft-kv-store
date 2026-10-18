package main

import (
	"context"
	"time"

	raftpb "github.com/chrisli/raft-kv-store/proto"
)

// ConfirmLeadership runs the read-index protocol (Raft dissertation 6.4). It
// returns true once a linearizable read may be served from local state:
//
//  1. The leader must have committed an entry from its own term (its no-op).
//     Until then its commitIndex can lag entries a previous leader committed.
//  2. It records commitIndex as the read index and confirms with one heartbeat
//     round that a majority still accepts it as leader.
//  3. It waits until the state machine has applied the read index.
//
// It returns false if this node is not, or cannot confirm it is, the leader.
func (r *Raft) ConfirmLeadership(ctx context.Context) bool {
	if !r.waitUntil(ctx, func() bool { return r.state != Leader || r.termAtLocked(r.commitIndex) == r.currentTerm }) {
		return false
	}
	r.mu.Lock()
	if r.state != Leader {
		r.mu.Unlock()
		return false
	}
	term, readIndex := r.currentTerm, r.commitIndex
	args := &raftpb.AppendEntriesArgs{Term: term, LeaderId: r.cfg.Addr, LeaderCommit: readIndex}
	peers := r.cfg.Peers
	needed := r.majorityLocked()
	r.mu.Unlock()

	results := make(chan bool, len(peers))
	for _, p := range peers {
		go func() {
			cctx, cancel := context.WithTimeout(ctx, rpcTimeout)
			defer cancel()
			reply, err := r.trans.SendAppendEntries(cctx, p, args)
			results <- err == nil && reply.Success && reply.Term == term
		}()
	}
	acks := 1
	for range peers {
		if acks >= needed {
			break
		}
		select {
		case ok := <-results:
			if ok {
				acks++
			}
		case <-ctx.Done():
			return false
		}
	}
	if acks < needed {
		return false
	}
	return r.waitUntil(ctx, func() bool { return r.lastApplied >= readIndex })
}

// waitUntil polls cond under the lock until it holds or ctx ends.
func (r *Raft) waitUntil(ctx context.Context, cond func() bool) bool {
	for {
		r.mu.Lock()
		ok := cond()
		r.mu.Unlock()
		if ok {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(tickInterval):
		}
	}
}
