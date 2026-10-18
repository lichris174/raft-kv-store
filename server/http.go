package main

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"net/url"
	"time"

	raftpb "github.com/chrisli/raft-kv-store/proto"
)

// serveHTTP exposes a small JSON gateway used by the dashboard and the harness
// scripts, since browsers can't speak gRPC directly. Convention: the HTTP port is
// the gRPC port minus 1000 (e.g. gRPC :9001 -> HTTP :8001).
//
//	GET  /status[?digest=1]  node status (with a hash of the data on request)
//	GET  /get?key=K          read
//	POST /set?key=K&value=V  write
//	POST /del?key=K          delete
//
// Writes require POST so a plain link or <img> tag cannot trigger them, and
// browser requests are only accepted from pages served by this machine (see
// localOrigin), so another website the user visits cannot drive the cluster.
func (s *kvServer) serveHTTP(addr string) {
	log.Printf("node %s HTTP gateway on %s", s.nodeID, addr)
	if err := http.ListenAndServe(addr, s.httpHandler()); err != nil {
		log.Printf("http server (%s): %v", addr, err)
	}
}

func (s *kvServer) httpHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, statusJSON(s.status(r.URL.Query().Get("digest") == "1"), s.readMode))
	})
	mux.HandleFunc("GET /get", s.withTimeout(func(ctx context.Context, q url.Values) (any, error) {
		rep, err := s.Get(ctx, &raftpb.GetRequest{Key: q.Get("key")})
		if err != nil {
			return nil, err
		}
		return map[string]any{"value": rep.GetValue(), "found": rep.GetFound(), "leaderHint": rep.GetLeaderHint()}, nil
	}))
	mux.HandleFunc("POST /set", s.withTimeout(func(ctx context.Context, q url.Values) (any, error) {
		return writeJSONReply(s.Set(ctx, &raftpb.SetRequest{Key: q.Get("key"), Value: q.Get("value")}))
	}))
	mux.HandleFunc("POST /del", s.withTimeout(func(ctx context.Context, q url.Values) (any, error) {
		return writeJSONReply(s.Delete(ctx, &raftpb.DeleteRequest{Key: q.Get("key")}))
	}))
	return cors(mux)
}

// withTimeout adapts a handler to HTTP with a 2s deadline: its result becomes
// a 200 JSON body, its error a 500 JSON body.
func (s *kvServer) withTimeout(h func(context.Context, url.Values) (any, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		v, err := h(ctx, r.URL.Query())
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, v)
	}
}

func writeJSONReply(rep *raftpb.WriteReply, err error) (any, error) {
	if err != nil {
		return nil, err
	}
	return map[string]any{"ok": rep.GetOk(), "leaderHint": rep.GetLeaderHint(), "error": rep.GetError()}, nil
}

func statusJSON(r *raftpb.StatusReply, readMode string) map[string]any {
	return map[string]any{
		"nodeId":        r.GetNodeId(),
		"role":          r.GetRole(),
		"term":          r.GetTerm(),
		"leader":        r.GetLeaderId(),
		"commitIndex":   r.GetCommitIndex(),
		"lastApplied":   r.GetLastApplied(),
		"snapshotIndex": r.GetSnapshotIndex(),
		"logLen":        r.GetLogLen(),
		"keys":          r.GetNumKeys(),
		"digest":        r.GetStateDigest(),
		"peers":         r.GetPeers(),
		"readMode":      readMode,
	}
}

// cors lets pages served from this machine (the dashboard on any local port)
// call every node, and refuses browser requests from anywhere else. Requests
// without an Origin header (curl, PowerShell, the harness) pass through.
func cors(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if origin := r.Header.Get("Origin"); origin != "" {
			if !localOrigin(origin) {
				writeJSON(w, http.StatusForbidden, map[string]string{"error": "cross-origin request from a non-local page"})
				return
			}
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Vary", "Origin")
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		h.ServeHTTP(w, r)
	})
}

// localOrigin reports whether a browser Origin is a page on this machine.
func localOrigin(origin string) bool {
	u, err := url.Parse(origin)
	if err != nil {
		return false
	}
	switch host := u.Hostname(); host {
	case "localhost":
		return true
	default:
		ip := net.ParseIP(host)
		return ip != nil && ip.IsLoopback()
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
