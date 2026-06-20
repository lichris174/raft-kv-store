// Command control is a local control-plane server for the benchmark and chaos
// harnesses. Browsers can't spawn raftbench.exe / chaos.ps1 directly, so this
// server runs the existing PowerShell harnesses as child processes and streams
// their output to the control dashboard over Server-Sent Events.
//
//	go run ./control            # serves http://localhost:8090
//
// One job runs at a time. Start a run via POST /bench or /chaos; watch it on
// the /stream SSE endpoint; stop it via POST /stop. The dashboard is served at /.
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
	w.Header().Set("Access-Control-Allow-Origin", "*")

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
	addr := flag.String("addr", ":8090", "control server listen address")
	root := flag.String("root", ".", "repo root (where scripts/ and control-dashboard/ live)")
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
	mux := http.NewServeMux()

	mux.HandleFunc("/stream", h.stream)

	mux.HandleFunc("/bench", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		args := []string{
			"-Writers", clampInt(q.Get("writers"), 1, 256, 24),
			"-Readers", clampInt(q.Get("readers"), 1, 256, 16),
			"-Duration", durArg(q.Get("duration"), "5s"),
			"-Threshold", clampInt(q.Get("threshold"), 25, 100000, 2000),
		}
		if err := h.start("bench", filepath.Join("scripts", "bench.ps1"), args); err != nil {
			writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]bool{"started": true})
	})

	mux.HandleFunc("/chaos", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		args := []string{
			"-Rounds", clampInt(q.Get("rounds"), 1, 100, 8),
			"-Threshold", clampInt(q.Get("threshold"), 5, 100000, 25),
		}
		if err := h.start("chaos", filepath.Join("scripts", "chaos.ps1"), args); err != nil {
			writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]bool{"started": true})
	})

	mux.HandleFunc("/stop", func(w http.ResponseWriter, r *http.Request) {
		h.stop()
		writeJSON(w, http.StatusOK, map[string]bool{"stopped": true})
	})

	mux.HandleFunc("/state", func(w http.ResponseWriter, r *http.Request) {
		h.mu.Lock()
		running, kind := h.running, h.kind
		h.mu.Unlock()
		writeJSON(w, http.StatusOK, map[string]any{"running": running, "kind": kind})
	})

	mux.Handle("/", http.FileServer(http.Dir(filepath.Join(abs, "control-dashboard"))))

	log.Printf("control plane on %s  (root=%s)", *addr, abs)
	log.Printf("open http://localhost%s", *addr)
	if err := http.ListenAndServe(*addr, mux); err != nil {
		log.Fatal(err)
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
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
