package main

import (
	"context"
	"errors"

	raftpb "github.com/chrisli/raft-kv-store/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// kvServer implements the gRPC KV service. With Raft enabled, writes are
// replicated through the leader before they apply; without it (a node started
// with no peers) the node is its own leader and applies writes directly.
type kvServer struct {
	raftpb.UnimplementedKVServer
	nodeID   string
	readMode string // "leader" | "follower"
	peers    []string
	store    *KVStore
	raft     *Raft // nil when the node runs without peers
}

func newKVServer(nodeID, readMode string, peers []string) *kvServer {
	return &kvServer{
		nodeID:   nodeID,
		readMode: readMode,
		peers:    peers,
		store:    NewKVStore(),
	}
}

// Get serves a read. In follower read mode (or without Raft) any node answers
// from local state, which may be slightly stale. In leader mode the read is
// linearizable: only the leader serves, after the read-index check. A node that
// is not leader redirects; a leader that cannot confirm itself fails the read
// rather than answer with possibly stale data.
func (s *kvServer) Get(ctx context.Context, req *raftpb.GetRequest) (*raftpb.GetReply, error) {
	if s.raft != nil && s.readMode != "follower" && !s.raft.ConfirmLeadership(ctx) {
		if hint := s.raft.LeaderAddr(); hint != "" && !s.raft.IsLeader() {
			return &raftpb.GetReply{LeaderHint: hint}, nil
		}
		return nil, status.Error(codes.Unavailable, "no leader can confirm this read; try again")
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

// propose routes a write through Raft. Without Raft it applies directly. On a
// follower it fails with "not leader" plus a hint.
func (s *kvServer) propose(ctx context.Context, op, key, value string) (*raftpb.WriteReply, error) {
	if s.raft == nil {
		applyOp(s.store, op, key, value)
		return &raftpb.WriteReply{Ok: true}, nil
	}
	err := s.raft.Propose(ctx, op, key, value)
	if err == nil {
		return &raftpb.WriteReply{Ok: true}, nil
	}
	reply := &raftpb.WriteReply{Error: err.Error()}
	if errors.Is(err, ErrNotLeader) {
		reply.LeaderHint = s.raft.LeaderAddr()
	}
	return reply, nil
}

func (s *kvServer) Status(context.Context, *raftpb.StatusRequest) (*raftpb.StatusReply, error) {
	return s.status(true), nil
}

// status reports this node's state. The digest hashes the whole store, so the
// dashboard's frequent polls leave it out (withDigest=false).
func (s *kvServer) status(withDigest bool) *raftpb.StatusReply {
	reply := &raftpb.StatusReply{
		NodeId:   s.nodeID,
		Peers:    s.peers,
		NumKeys:  int32(s.store.Len()),
		Role:     "leader", // without Raft this node is its own leader
		LeaderId: s.nodeID,
	}
	if withDigest {
		reply.StateDigest = s.store.Digest()
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
	}
	return reply
}
