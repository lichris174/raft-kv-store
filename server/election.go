package main

import (
	"context"
	"log"
	"time"

	raftpb "github.com/chrisli/raft-kv-store/proto"
)

// Leader election: pre-vote, the real vote, and taking office.

// startElectionLocked runs when the election timer fires. It first holds a
// pre-vote (Raft dissertation 9.6): it asks whether a majority would vote for it
// in the next term, without changing its own term. Only then does it campaign.
// A node cut off from the cluster therefore never inflates its term, so when it
// reconnects its replies cannot depose a healthy leader.
func (r *Raft) startElectionLocked() {
	r.resetElectionDeadlineLocked()
	term := r.currentTerm
	stillWanted := func() bool {
		return r.state != Leader && r.currentTerm == term && !r.hasLiveLeaderLocked()
	}
	r.collectVotesLocked(true, term+1, stillWanted, r.campaignLocked)
}

// campaignLocked starts a real election: a new term and a vote for itself, both
// persisted before any vote is requested.
func (r *Raft) campaignLocked() {
	meta := r.metaLocked()
	meta.CurrentTerm++
	meta.VotedFor = r.cfg.Addr
	if err := r.saveMetaLocked(meta); err != nil {
		// Do not campaign on a term/self-vote we could forget after a restart.
		log.Printf("[%s] WARN cannot persist election state, skipping campaign: %v", r.cfg.ID, err)
		return
	}
	r.currentTerm, r.votedFor = meta.CurrentTerm, meta.VotedFor
	r.state, r.leaderAddr = Candidate, ""
	r.resetElectionDeadlineLocked()
	term := r.currentTerm
	log.Printf("[%s] start election term=%d (need %d)", r.cfg.ID, term, r.majorityLocked())
	stillCandidate := func() bool { return r.state == Candidate && r.currentTerm == term }
	r.collectVotesLocked(false, term, stillCandidate, r.becomeLeaderLocked)
}

// collectVotesLocked asks every peer for a vote (or pre-vote) in term and calls
// won, under the lock, once a majority counting this node grants it. Replies are
// ignored once current reports the round is over.
func (r *Raft) collectVotesLocked(preVote bool, term uint64, current func() bool, won func()) {
	votes, needed := 1, r.majorityLocked()
	if votes >= needed { // a one-member cluster wins on its own vote
		won()
		return
	}
	lastIdx, lastTerm := r.lastLogLocked()
	args := &raftpb.RequestVoteArgs{
		Term: term, CandidateId: r.cfg.Addr, LastLogIndex: lastIdx, LastLogTerm: lastTerm, PreVote: preVote,
	}
	for _, peer := range r.cfg.Peers {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
			defer cancel()
			reply, err := r.trans.SendRequestVote(ctx, peer, args)
			if err != nil {
				return
			}
			r.mu.Lock()
			defer r.mu.Unlock()
			if !current() {
				return
			}
			if reply.Term > r.currentTerm && !reply.VoteGranted {
				_ = r.becomeFollowerLocked(reply.Term) // we are behind; catch up and stop
				return
			}
			if reply.VoteGranted {
				if votes++; votes == needed {
					won()
				}
			}
		}()
	}
}

// becomeLeaderLocked takes office after winning the election.
func (r *Raft) becomeLeaderLocked() {
	now := time.Now()
	for _, p := range r.cfg.Peers {
		r.nextIndex[p] = r.lastIndexLocked() + 1
		r.matchIndex[p] = 0
		r.lastAck[p] = now // grace: the election just proved a majority was reachable
	}
	r.state = Leader
	r.leaderAddr = r.cfg.Addr
	r.nextHeartbeat = now.Add(r.cfg.HeartbeatInterval)
	// Commit a per-term no-op so prior-term entries commit indirectly and
	// read-index reads have a committed current-term anchor. applyOp ignores it.
	if err := r.leaderAppendLocked("noop", "", ""); err != nil {
		// A leader that cannot write its own log must not lead; stay a follower
		// and let another node (or a later election) take over.
		log.Printf("[%s] WARN cannot persist leader no-op, declining leadership: %v", r.cfg.ID, err)
		r.state, r.leaderAddr = Follower, ""
		return
	}
	log.Printf("[%s] became LEADER term=%d", r.cfg.ID, r.currentTerm)
}

// RequestVote handles a vote or pre-vote request.
func (r *Raft) RequestVote(_ context.Context, args *raftpb.RequestVoteArgs) (*raftpb.RequestVoteReply, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	reply := &raftpb.RequestVoteReply{Term: r.currentTerm}
	switch {
	case args.Term < r.currentTerm:
		return reply, nil
	case args.PreVote:
		// Would we vote for this candidate in args.Term? Answer without changing
		// anything: no term adoption, no recorded vote, no timer reset.
		reply.VoteGranted = args.Term > r.currentTerm && !r.hasLiveLeaderLocked() && r.upToDateLocked(args)
		return reply, nil
	case args.Term > r.currentTerm && r.hasLiveLeaderLocked():
		// Leader stickiness (dissertation 4.2.3): while a leader is healthy, ignore
		// vote requests outright, without adopting the higher term.
		return reply, nil
	case args.Term > r.currentTerm:
		if err := r.becomeFollowerLocked(args.Term); err != nil {
			return reply, err
		}
		reply.Term = r.currentTerm
	}

	if (r.votedFor == "" || r.votedFor == args.CandidateId) && r.upToDateLocked(args) {
		meta := r.metaLocked()
		meta.VotedFor = args.CandidateId
		if err := r.saveMetaLocked(meta); err != nil {
			// A vote we might forget after a restart could be granted twice in one
			// term, so refuse rather than grant an unrecorded vote.
			return reply, err
		}
		r.votedFor = args.CandidateId
		r.resetElectionDeadlineLocked()
		reply.VoteGranted = true
	}
	return reply, nil
}

// upToDateLocked is the election restriction: the candidate's log must be at
// least as up to date as ours (higher last term, or same term and no shorter).
func (r *Raft) upToDateLocked(args *raftpb.RequestVoteArgs) bool {
	lastIdx, lastTerm := r.lastLogLocked()
	return args.LastLogTerm > lastTerm || (args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIdx)
}
