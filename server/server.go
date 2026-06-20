package main

import (
	"context"

	raftpb "github.com/chrisli/raft-kv-store/proto"
)

// kvServer implements the gRPC KV service. Feature 1: a single node that acts
// as its own leader, serving reads and writes directly from the in-memory
// store. Later features add Raft so writes are replicated before applying.
type kvServer struct {
	raftpb.UnimplementedKVServer
	nodeID   string
	readMode string // "leader" | "follower"
	peers    []string
	store    *KVStore
	raft     *Raft // nil in the single-node Feature 1 tests
}

func newKVServer(nodeID, readMode string, peers []string) *kvServer {
	return &kvServer{
		nodeID:   nodeID,
		readMode: readMode,
		peers:    peers,
		store:    NewKVStore(),
	}
}

func (s *kvServer) Get(ctx context.Context, req *raftpb.GetRequest) (*raftpb.GetReply, error) {
	// Single-node or follower read-mode: serve local committed state directly.
	// follower mode may return slightly stale data (documented tradeoff).
	if s.raft == nil || s.readMode == "follower" {
		v, found := s.store.Get(req.GetKey())
		return &raftpb.GetReply{Value: v, Found: found}, nil
	}
	// leader read-mode: linearizable. Only the leader serves, and only after a
	// read-index leadership confirmation. Non-leaders redirect via LeaderHint.
	if !s.raft.IsLeader() {
		return &raftpb.GetReply{LeaderHint: s.raft.LeaderAddr()}, nil
	}
	if !s.raft.ConfirmLeadership(ctx) {
		return &raftpb.GetReply{LeaderHint: s.raft.LeaderAddr()}, nil
	}
	v, found := s.store.Get(req.GetKey())
	return &raftpb.GetReply{Value: v, Found: found}, nil
}

func (s *kvServer) Set(ctx context.Context, req *raftpb.SetRequest) (*raftpb.WriteReply, error) {
	return s.propose(ctx, "set", req.GetKey(), req.GetValue())
}

func (s *kvServer) Delete(ctx context.Context, req *raftpb.DeleteRequest) (*raftpb.WriteReply, error) {
	return s.propose(ctx, "del", req.GetKey(), "")
}

// propose routes a write through Raft. With no Raft (single-node Feature 1
// path) it applies directly. On a follower it returns NOT_LEADER + a hint.
func (s *kvServer) propose(ctx context.Context, op, key, value string) (*raftpb.WriteReply, error) {
	if s.raft == nil {
		applyOp(s.store, op, key, value)
		return &raftpb.WriteReply{Ok: true}, nil
	}
	err := s.raft.Propose(ctx, op, key, value)
	switch {
	case err == nil:
		return &raftpb.WriteReply{Ok: true}, nil
	case err == ErrNotLeader:
		return &raftpb.WriteReply{Ok: false, Error: "not leader", LeaderHint: s.raft.LeaderAddr()}, nil
	default:
		return &raftpb.WriteReply{Ok: false, Error: err.Error()}, nil
	}
}

// applyOp applies a committed operation to the state machine.
func applyOp(store *KVStore, op, key, value string) {
	switch op {
	case "set":
		store.Set(key, value)
	case "del":
		store.Delete(key)
	}
}

func (s *kvServer) Status(_ context.Context, _ *raftpb.StatusRequest) (*raftpb.StatusReply, error) {
	reply := &raftpb.StatusReply{
		NodeId:  s.nodeID,
		Peers:   s.peers,
		NumKeys: int32(s.store.Len()),
	}
	if s.raft != nil {
		snap := s.raft.StatusSnapshot()
		reply.Role = snap.Role
		reply.Term = snap.Term
		reply.LeaderId = snap.LeaderAddr
		reply.CommitIndex = snap.CommitIndex
		reply.LastApplied = snap.LastApplied
		reply.SnapshotIndex = snap.SnapshotIndex
		reply.LogLen = int32(snap.LogLen)
	} else {
		// Single-node Feature 1 path: no Raft, act as own leader.
		reply.Role = "leader"
		reply.LeaderId = s.nodeID
	}
	return reply, nil
}
