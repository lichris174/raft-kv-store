// Command control is the local control-plane server behind the dashboard.
// Browsers cannot spawn processes, so this server:
//
//   - owns a 5-node demo cluster (start/stop all, kill/start one node), and
//   - runs the benchmark and chaos harnesses as child processes, streaming
//     their output to the page over Server-Sent Events.
//
//	go run ./control            # serves http://localhost:8090
//
// Every endpoint that changes anything requires POST, and the server listens on
// loopback only, so a web page the user happens to visit cannot drive it.
// One test run executes at a time and pauses the demo cluster (they share ports).
// The dashboard in dashboard/ is served at /.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

// event is one SSE message. Kinds: start | line | end.
type event struct {
	T    string `json:"t"`
	Kind string `json:"kind,omitempty"` // "bench" | "chaos" (on start)
	Cmd  string `json:"cmd,omitempty"`  // resolved command (on start)
	S    string `json:"s,omitempty"`    // a line of output (on line)
	Code int    `json:"code,omitempty"` // exit code (on end)
}

// hub fans out job output to all connected SSE clients and enforces a single
// concurrent job.
type hub struct {
	mu      sync.Mutex
	subs    map[chan event]struct{}
	running bool
	kind    string
	proc    *os.Process
}

func newHub() *hub { return &hub{subs: map[chan event]struct{}{}} }

func (h *hub) sub() chan event {
	ch := make(chan event, 512)
	h.mu.Lock()
	h.subs[ch] = struct{}{}
	h.mu.Unlock()
	return ch
}

func (h *hub) unsub(ch chan event) {
	h.mu.Lock()
	if _, ok := h.subs[ch]; ok {
		delete(h.subs, ch)
		close(ch)
	}
	h.mu.Unlock()
}

func (h *hub) emit(e event) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.subs {
		select {
		case ch <- e:
		default: // slow client: drop rather than block the job
		}
	}
}

// start launches a PowerShell harness. args are appended after the script path.
func (h *hub) start(kind, script string, args []string) error {
	h.mu.Lock()
	if h.running {
		h.mu.Unlock()
		return fmt.Errorf("a %s run is already in progress", h.kind)
	}
	h.running = true
	h.kind = kind
	h.mu.Unlock()

	psArgs := append([]string{
		"-NoProfile", "-ExecutionPolicy", "Bypass", "-File", script,
	}, args...)

	go func() {
		defer func() {
			h.mu.Lock()
			h.running = false
			h.proc = nil
			h.mu.Unlock()
		}()

		pr, pw := io.Pipe()
		cmd := exec.Command("powershell", psArgs...)
		cmd.Stdout = pw
		cmd.Stderr = pw

		h.emit(event{T: "start", Kind: kind, Cmd: "powershell " + script + " " + joinArgs(args)})

		if err := cmd.Start(); err != nil {
			h.emit(event{T: "line", S: "failed to start: " + err.Error()})
			h.emit(event{T: "end", Code: -1})
			_ = pw.Close()
			return
		}
		h.mu.Lock()
		h.proc = cmd.Process
		h.mu.Unlock()

		done := make(chan error, 1)
		go func() {
			done <- cmd.Wait()
			_ = pw.Close()
		}()

		sc := bufio.NewScanner(pr)
		sc.Buffer(make([]byte, 1<<20), 1<<20)
		for sc.Scan() {
			h.emit(event{T: "line", S: sc.Text()})
		}

		code := 0
		if err := <-done; err != nil {
			if ee, ok := err.(*exec.ExitError); ok {
				code = ee.ExitCode()
			} else {
				code = -1
			}
		}
		h.emit(event{T: "end", Code: code})
	}()
	return nil
}

// stop kills the running harness process. The harnesses kill stale raftnode
// processes on their next launch, so any orphaned nodes are reclaimed then;
// stopNodes() also clears them immediately.
func (h *hub) stop() {
	h.mu.Lock()
	p := h.proc
	h.mu.Unlock()
	if p != nil {
		_ = p.Kill()
	}
	stopNodes()
}

// stopNodes force-kills any lingering raftnode processes.
func stopNodes() {
	_ = exec.Command("powershell", "-NoProfile", "-Command",
		"Get-Process raftnode -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue").Run()
}

func (h *hub) stream(w http.ResponseWriter, r *http.Request) {
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ch := h.sub()
	defer h.unsub(ch)

	// Greet so the client knows the stream is live.
	writeSSE(w, fl, event{T: "line", S: "[control] connected, ready"})

	ping := time.NewTicker(15 * time.Second)
	defer ping.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case e := <-ch:
			writeSSE(w, fl, e)
		case <-ping.C:
			fmt.Fprint(w, ": ping\n\n") // SSE comment keeps the connection warm
			fl.Flush()
		}
	}
}

func writeSSE(w http.ResponseWriter, fl http.Flusher, e event) {
	b, _ := json.Marshal(e)
	fmt.Fprintf(w, "data: %s\n\n", b)
	fl.Flush()
}

func joinArgs(a []string) string {
	out := ""
	for i, s := range a {
		if i > 0 {
			out += " "
		}
		out += s
	}
	return out
}

func main() {
	addr := flag.String("addr", "127.0.0.1:8090", "control server listen address (keep it on loopback)")
	root := flag.String("root", ".", "repo root (where scripts/ and dashboard/ live)")
	flag.Parse()

	abs, err := filepath.Abs(*root)
	if err != nil {
		log.Fatalf("root: %v", err)
	}
	if _, err := os.Stat(filepath.Join(abs, "scripts", "bench.ps1")); err != nil {
		log.Fatalf("scripts/bench.ps1 not found under %s; run from the repo root or pass -root", abs)
	}
	if err := os.Chdir(abs); err != nil {
		log.Fatalf("chdir %s: %v", abs, err)
	}

	h := newHub()
	cl := newCluster(abs)
	mux := http.NewServeMux()

	// Leave no orphaned nodes behind when the control server exits.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	go func() {
		<-sig
		cl.stopAll()
		os.Exit(0)
	}()

	mux.HandleFunc("/stream", h.stream)

	mux.HandleFunc("/bench", post(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		args := []string{
			"-Writers", clampInt(q.Get("writers"), 1, 256, 24),
			"-Readers", clampInt(q.Get("readers"), 1, 256, 16),
			"-Duration", durArg(q.Get("duration"), "5s"),
			"-Threshold", clampInt(q.Get("threshold"), 25, 100000, 2000),
		}
		if !h.begin(w, cl, "bench", filepath.Join("scripts", "bench.ps1"), args) {
			return
		}
	}))

	mux.HandleFunc("/chaos", post(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		args := []string{
			"-Rounds", clampInt(q.Get("rounds"), 1, 100, 8),
			"-Threshold", clampInt(q.Get("threshold"), 5, 100000, 25),
		}
		if q.Get("killLeader") == "1" {
			args = append(args, "-KillLeader")
		}
		if !h.begin(w, cl, "chaos", filepath.Join("scripts", "chaos.ps1"), args) {
			return
		}
	}))

	mux.HandleFunc("/stop", post(func(w http.ResponseWriter, r *http.Request) {
		h.stop()
		cl.stopAll()
		writeJSON(w, http.StatusOK, map[string]bool{"stopped": true})
	}))

	// GET /cluster: the demo cluster's processes plus whether a test run is active.
	mux.HandleFunc("/cluster", func(w http.ResponseWriter, r *http.Request) {
		h.mu.Lock()
		busy, kind := h.running, h.kind
		h.mu.Unlock()
		writeJSON(w, http.StatusOK, map[string]any{"nodes": cl.state(), "busy": busy, "kind": kind})
	})

	mux.HandleFunc("/cluster/start", post(func(w http.ResponseWriter, r *http.Request) {
		if h.isRunning() {
			writeJSON(w, http.StatusConflict, map[string]string{"error": "a test run is using the cluster ports; stop it first"})
			return
		}
		if err := cl.startAll(r.URL.Query().Get("fresh") == "1"); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
	}))

	mux.HandleFunc("/cluster/stop", post(func(w http.ResponseWriter, r *http.Request) {
		cl.stopAll()
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
	}))

	mux.HandleFunc("/node/kill", post(nodeAction(h, cl.killNode)))
	mux.HandleFunc("/node/start", post(nodeAction(h, cl.startNode)))

	mux.Handle("/", http.FileServer(http.Dir(filepath.Join(abs, "dashboard"))))

	log.Printf("control plane on %s  (root=%s)", *addr, abs)
	log.Printf("open http://%s", *addr)
	if err := http.ListenAndServe(*addr, mux); err != nil {
		log.Fatal(err)
	}
}

// post restricts a handler to POST. Mutating endpoints must not be reachable by
// a plain GET (an <img> tag or link on any web page can issue one).
func post(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "use POST"})
			return
		}
		next(w, r)
	}
}

func nodeAction(h *hub, act func(id string) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if h.isRunning() {
			writeJSON(w, http.StatusConflict, map[string]string{"error": "a test run is in progress"})
			return
		}
		if err := act(r.URL.Query().Get("id")); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
	}
}

func (h *hub) isRunning() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.running
}

// begin pauses the demo cluster (a test run starts its own on the same ports)
// and launches the harness, replying to the HTTP request either way.
func (h *hub) begin(w http.ResponseWriter, cl *cluster, kind, script string, args []string) bool {
	if h.isRunning() {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "a test run is already in progress"})
		return false
	}
	cl.stopAll()
	if err := h.start(kind, script, args); err != nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return false
	}
	writeJSON(w, http.StatusOK, map[string]bool{"started": true})
	return true
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func clampInt(s string, lo, hi, def int) string {
	n, err := strconv.Atoi(s)
	if err != nil {
		return strconv.Itoa(def)
	}
	if n < lo {
		n = lo
	}
	if n > hi {
		n = hi
	}
	return strconv.Itoa(n)
}

// durArg sanitizes a Go duration string (e.g. "5s", "10s"); falls back to def.
func durArg(s, def string) string {
	if _, err := time.ParseDuration(s); err != nil {
		return def
	}
	return s
}
