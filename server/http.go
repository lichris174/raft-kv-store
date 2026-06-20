package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	raftpb "github.com/chrisli/raft-kv-store/proto"
)

// serveHTTP exposes a small JSON/REST gateway used by the dashboard, since
// browsers can't speak gRPC directly. Convention: the HTTP port is the gRPC
// port minus 1000 (e.g. gRPC :9001 -> HTTP :8001). CORS is open so a dashboard
// served from anywhere can poll every node.
func (s *kvServer) serveHTTP(addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/status", s.httpStatus)
	mux.HandleFunc("/get", s.httpGet)
	mux.HandleFunc("/set", s.httpSet)
	mux.HandleFunc("/del", s.httpDel)

	log.Printf("node %s HTTP gateway on %s", s.nodeID, addr)
	if err := http.ListenAndServe(addr, cors(mux)); err != nil {
		log.Printf("http server (%s): %v", addr, err)
	}
}

func cors(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		h.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func ctx2s() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 2*time.Second)
}

func (s *kvServer) httpStatus(w http.ResponseWriter, _ *http.Request) {
	ctx, cancel := ctx2s()
	defer cancel()
	r, _ := s.Status(ctx, &raftpb.StatusRequest{})
	writeJSON(w, http.StatusOK, map[string]any{
		"nodeId":        r.GetNodeId(),
		"role":          r.GetRole(),
		"term":          r.GetTerm(),
		"leader":        r.GetLeaderId(),
		"commitIndex":   r.GetCommitIndex(),
		"lastApplied":   r.GetLastApplied(),
		"snapshotIndex": r.GetSnapshotIndex(),
		"logLen":        r.GetLogLen(),
		"keys":          r.GetNumKeys(),
		"peers":         r.GetPeers(),
		"readMode":      s.readMode,
	})
}

func (s *kvServer) httpGet(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := ctx2s()
	defer cancel()
	rep, err := s.Get(ctx, &raftpb.GetRequest{Key: r.URL.Query().Get("key")})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"value": rep.GetValue(), "found": rep.GetFound(), "leaderHint": rep.GetLeaderHint(),
	})
}

func (s *kvServer) httpSet(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	ctx, cancel := ctx2s()
	defer cancel()
	rep, err := s.Set(ctx, &raftpb.SetRequest{Key: q.Get("key"), Value: q.Get("value")})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok": rep.GetOk(), "leaderHint": rep.GetLeaderHint(), "error": rep.GetError(),
	})
}

func (s *kvServer) httpDel(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := ctx2s()
	defer cancel()
	rep, err := s.Delete(ctx, &raftpb.DeleteRequest{Key: r.URL.Query().Get("key")})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok": rep.GetOk(), "leaderHint": rep.GetLeaderHint(), "error": rep.GetError(),
	})
}
