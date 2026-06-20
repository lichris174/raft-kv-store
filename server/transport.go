package main

import (
	"context"
	"sync"

	raftpb "github.com/chrisli/raft-kv-store/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// grpcTransport sends Raft RPCs to peers over gRPC, caching one client per peer.
type grpcTransport struct {
	mu    sync.Mutex
	conns map[string]raftpb.RaftClient
}

func newGRPCTransport() *grpcTransport {
	return &grpcTransport{conns: make(map[string]raftpb.RaftClient)}
}

func (t *grpcTransport) client(peer string) (raftpb.RaftClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if c, ok := t.conns[peer]; ok {
		return c, nil
	}
	conn, err := grpc.NewClient(peer, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	c := raftpb.NewRaftClient(conn)
	t.conns[peer] = c
	return c, nil
}

func (t *grpcTransport) SendRequestVote(ctx context.Context, peer string, args *raftpb.RequestVoteArgs) (*raftpb.RequestVoteReply, error) {
	c, err := t.client(peer)
	if err != nil {
		return nil, err
	}
	return c.RequestVote(ctx, args)
}

func (t *grpcTransport) SendAppendEntries(ctx context.Context, peer string, args *raftpb.AppendEntriesArgs) (*raftpb.AppendEntriesReply, error) {
	c, err := t.client(peer)
	if err != nil {
		return nil, err
	}
	return c.AppendEntries(ctx, args)
}

func (t *grpcTransport) SendInstallSnapshot(ctx context.Context, peer string, args *raftpb.InstallSnapshotArgs) (*raftpb.InstallSnapshotReply, error) {
	c, err := t.client(peer)
	if err != nil {
		return nil, err
	}
	return c.InstallSnapshot(ctx, args)
}

// raftService adapts *Raft to the generated RaftServer interface.
type raftService struct {
	raftpb.UnimplementedRaftServer
	r *Raft
}

func (s *raftService) RequestVote(ctx context.Context, args *raftpb.RequestVoteArgs) (*raftpb.RequestVoteReply, error) {
	return s.r.RequestVote(ctx, args)
}

func (s *raftService) AppendEntries(ctx context.Context, args *raftpb.AppendEntriesArgs) (*raftpb.AppendEntriesReply, error) {
	return s.r.AppendEntries(ctx, args)
}

func (s *raftService) InstallSnapshot(ctx context.Context, args *raftpb.InstallSnapshotArgs) (*raftpb.InstallSnapshotReply, error) {
	return s.r.InstallSnapshot(ctx, args)
}
