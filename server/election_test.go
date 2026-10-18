package main

import (
	"context"
	"testing"
	"time"

	raftpb "github.com/chrisli/raft-kv-store/proto"
)

// Tests of election.go: the election restriction, pre-vote, and the cluster
// behaviors they produce.

// The election restriction (Raft 5.4.1): a vote goes only to a candidate whose
// log is at least as up to date as the voter's. Our log ends at index 3, term 2.
func TestRequestVote_ElectionRestriction(t *testing.T) {
	cases := []struct {
		name                string
		lastIndex, lastTerm uint64
		want                bool
	}{
		{"higher last term, shorter log", 1, 3, true},
		{"same term, longer log", 4, 2, true},
		{"same term, same length", 3, 2, true},
		{"same term, shorter log", 2, 2, false},
		{"lower last term, longer log", 9, 1, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, _ := soloNode(nil, nil)
			mustAppend(t, r, &raftpb.AppendEntriesArgs{Term: 2, LeaderId: "L", Entries: []*raftpb.LogEntry{
				setEntry(1, 1, "a", "1"), setEntry(2, 2, "b", "2"), setEntry(2, 3, "c", "3"),
			}})
			r.mu.Lock()
			r.leaderAddr = "" // no live leader, so stickiness does not apply
			r.mu.Unlock()
			reply, err := r.RequestVote(context.Background(), &raftpb.RequestVoteArgs{
				Term: 3, CandidateId: "n1", LastLogIndex: tc.lastIndex, LastLogTerm: tc.lastTerm,
			})
			if err != nil || reply.VoteGranted != tc.want {
				t.Fatalf("granted=%v err=%v, want granted=%v", reply.GetVoteGranted(), err, tc.want)
			}
		})
	}
}

// A pre-vote is a question, not a vote: the voter's term, vote and timer stay
// as they were whatever it answers.
func TestRequestVote_PreVoteChangesNothing(t *testing.T) {
	r, _ := soloNode(nil, nil)
	before := r.StatusSnapshot()
	r.mu.Lock()
	deadline := r.electionDeadline
	r.mu.Unlock()

	reply, err := r.RequestVote(context.Background(), &raftpb.RequestVoteArgs{Term: 7, CandidateId: "n1", PreVote: true})
	if err != nil || !reply.VoteGranted {
		t.Fatalf("pre-vote not granted to an up-to-date candidate: %+v %v", reply, err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.currentTerm != before.Term || r.votedFor != "" || !r.electionDeadline.Equal(deadline) {
		t.Fatalf("pre-vote changed state: term %d->%d votedFor=%q", before.Term, r.currentTerm, r.votedFor)
	}
}

// While a leader is alive a pre-vote is refused, so a node that merely lost
// contact cannot even start an election.
func TestRequestVote_PreVoteRefusedWhileLeaderIsAlive(t *testing.T) {
	r, _ := soloNode(nil, nil)
	mustAppend(t, r, &raftpb.AppendEntriesArgs{Term: 1, LeaderId: "L"})
	reply, _ := r.RequestVote(context.Background(), &raftpb.RequestVoteArgs{Term: 2, CandidateId: "n1", PreVote: true})
	if reply.VoteGranted {
		t.Fatal("pre-vote granted while a leader was alive")
	}
}

// The case pre-vote exists for. A node cut off from the cluster keeps timing out
// but cannot win a pre-vote, so its term stays put; when it reconnects, its
// replies carry no higher term and the healthy leader stays in office.
func TestPreVote_IsolatedNodeCannotDisruptLeaderOnRejoin(t *testing.T) {
	c := newCluster(t, 3, 0)
	l := c.waitLeader()
	c.mustPropose("before", "ok") // proves heartbeats are flowing before the cut
	leaderTerm := c.node(l).StatusSnapshot().Term
	f, other := (l+1)%3, (l+2)%3
	followerTerm := c.node(f).StatusSnapshot().Term

	c.net.partition([]string{c.addrs[f]})
	time.Sleep(10 * c.node(f).cfg.ElectionTimeoutMax) // many election timeouts
	if term := c.node(f).StatusSnapshot().Term; term != followerTerm {
		t.Fatalf("isolated node's term rose %d -> %d: it campaigned without a pre-vote majority", followerTerm, term)
	}
	if term := c.node(other).StatusSnapshot().Term; term != leaderTerm {
		t.Fatalf("the connected follower's term changed %d -> %d", leaderTerm, term)
	}

	c.net.heal()
	c.mustPropose("after-heal", "ok") // replicates to the rejoined node too
	c.waitValue(f, "after-heal", "ok")
	if s := c.node(l).StatusSnapshot(); s.Role != "leader" || s.Term != leaderTerm {
		t.Fatalf("leader disrupted by a rejoining node: role=%s term %d -> %d", s.Role, leaderTerm, s.Term)
	}
}

// In a two-member cluster the majority is both members: one alone cannot elect
// a leader or commit anything.
func TestElection_TwoMemberClusterNeedsBoth(t *testing.T) {
	c := newCluster(t, 2, 0)
	l := c.waitLeader()
	c.mustPropose("k", "both-up")
	c.crash((l + 1) % 2)

	commit := c.node(l).StatusSnapshot().CommitIndex
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := c.node(l).Propose(ctx, "set", "k", "alone"); err == nil {
		t.Fatal("a lone member of a two-member cluster committed a write")
	}
	if s := c.node(l).StatusSnapshot(); s.CommitIndex != commit {
		t.Fatalf("commitIndex moved %d -> %d without a majority", commit, s.CommitIndex)
	}
	if v, _ := c.store(l).Get("k"); v != "both-up" {
		t.Fatalf("k=%q applied without a majority", v)
	}
}

// A follower that moves to a newer term forgets the old term's leader, rather
// than keep sending clients to a node that no longer leads.
func TestRequestVote_NewTermForgetsOldLeader(t *testing.T) {
	r, _ := soloNode(nil, nil)
	mustAppend(t, r, &raftpb.AppendEntriesArgs{Term: 1, LeaderId: "L"})
	time.Sleep(r.cfg.ElectionTimeoutMin + 30*time.Millisecond) // the old leader goes silent
	if _, err := r.RequestVote(context.Background(), &raftpb.RequestVoteArgs{Term: 2, CandidateId: "n1"}); err != nil {
		t.Fatal(err)
	}
	if hint := r.LeaderAddr(); hint != "" {
		t.Fatalf("leader hint %q survived the move to a new term", hint)
	}
}
