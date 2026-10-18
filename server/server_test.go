package main

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
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

// httpDo sends one request to a node's HTTP gateway and decodes the JSON reply.
func httpDo(t *testing.T, h http.Handler, method, target, origin string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(method, target, nil)
	if origin != "" {
		req.Header.Set("Origin", origin)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var body map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	return rec.Code, body
}

func TestHTTP_SetGetDeleteRoundTrip(t *testing.T) {
	h := newKVServer("n1", "leader", nil).httpHandler()
	if code, body := httpDo(t, h, "POST", "/set?key=a&value=1", ""); code != 200 || body["ok"] != true {
		t.Fatalf("set: %d %v", code, body)
	}
	if code, body := httpDo(t, h, "GET", "/get?key=a", ""); code != 200 || body["value"] != "1" {
		t.Fatalf("get: %d %v", code, body)
	}
	if code, body := httpDo(t, h, "POST", "/del?key=a", ""); code != 200 || body["ok"] != true {
		t.Fatalf("del: %d %v", code, body)
	}
	if _, body := httpDo(t, h, "GET", "/status?digest=1", ""); body["keys"] != 0.0 || body["digest"] == "" {
		t.Fatalf("status after delete: %v", body)
	}
}

// Writes over GET would let any link or <img> tag change data.
func TestHTTP_WritesRequirePOST(t *testing.T) {
	h := newKVServer("n1", "leader", nil).httpHandler()
	for _, path := range []string{"/set?key=a&value=1", "/del?key=a"} {
		if code, _ := httpDo(t, h, "GET", path, ""); code != http.StatusMethodNotAllowed {
			t.Errorf("GET %s = %d, want 405", path, code)
		}
	}
}

// A page on another site must not be able to drive the cluster from the
// user's browser; the dashboard (any local origin) must.
func TestHTTP_OnlyLocalPagesMayCall(t *testing.T) {
	h := newKVServer("n1", "leader", nil).httpHandler()
	if code, _ := httpDo(t, h, "POST", "/set?key=a&value=evil", "https://attacker.example"); code != http.StatusForbidden {
		t.Fatalf("foreign origin got %d, want 403", code)
	}
	for _, origin := range []string{"http://localhost:8090", "http://127.0.0.1:8080", "http://[::1]:8090"} {
		if code, _ := httpDo(t, h, "POST", "/set?key=a&value=1", origin); code != 200 {
			t.Errorf("local origin %s got %d, want 200", origin, code)
		}
	}
}
