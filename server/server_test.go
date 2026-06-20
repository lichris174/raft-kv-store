package main

import (
	"context"
	"net"
	"testing"

	raftpb "github.com/chrisli/raft-kv-store/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// newTestClient spins up the gRPC KV service over an in-memory bufconn so the
// service is exercised end-to-end (marshalling + handlers) without a real port.
func newTestClient(t *testing.T) raftpb.KVClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	raftpb.RegisterKVServer(srv, newKVServer("n1", "leader", nil))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return raftpb.NewKVClient(conn)
}

func TestService_SetGetDelete(t *testing.T) {
	cli := newTestClient(t)
	ctx := context.Background()

	if _, err := cli.Set(ctx, &raftpb.SetRequest{Key: "foo", Value: "bar"}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	g, err := cli.Get(ctx, &raftpb.GetRequest{Key: "foo"})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !g.GetFound() || g.GetValue() != "bar" {
		t.Fatalf("Get got (%q,%v), want (bar,true)", g.GetValue(), g.GetFound())
	}

	if _, err := cli.Delete(ctx, &raftpb.DeleteRequest{Key: "foo"}); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	g, err = cli.Get(ctx, &raftpb.GetRequest{Key: "foo"})
	if err != nil {
		t.Fatalf("Get after delete: %v", err)
	}
	if g.GetFound() {
		t.Fatal("key should be absent after delete")
	}
}

func TestService_Status(t *testing.T) {
	cli := newTestClient(t)
	ctx := context.Background()
	_, _ = cli.Set(ctx, &raftpb.SetRequest{Key: "a", Value: "1"})

	st, err := cli.Status(ctx, &raftpb.StatusRequest{})
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.GetNodeId() != "n1" || st.GetRole() != "leader" {
		t.Fatalf("Status node/role = %q/%q, want n1/leader", st.GetNodeId(), st.GetRole())
	}
	if st.GetNumKeys() != 1 {
		t.Fatalf("NumKeys = %d, want 1", st.GetNumKeys())
	}
}
