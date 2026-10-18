// Command control is the local control-plane server behind the dashboard.
// Browsers cannot spawn processes, so this server:
//
//   - owns a 5-node demo cluster (start/stop all, kill/start one node), and
//
//   - runs the benchmark and chaos harnesses as child processes, streaming
//     their output to the page over Server-Sent Events.
//
//     go run ./control            # serves http://localhost:8090
//
// The server listens on loopback only, every endpoint that changes anything
// requires POST, and requests must come from a local page with a local Host
// header, so a web page the user happens to visit cannot drive it (by a
// cross-site form post or by DNS rebinding).
// One test run executes at a time and pauses the demo cluster (they share ports).
// The dashboard in dashboard/ is served at /.
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// event is one SSE message. T is "start", "line" or "end".
type event struct {
	T    string `json:"t"`
	Kind string `json:"kind,omitempty"` // "bench" | "chaos" (on start)
	Cmd  string `json:"cmd,omitempty"`  // resolved command (on start)
	S    string `json:"s,omitempty"`    // a line of output (on line)
	Code int    `json:"code"`           // exit code (on end); never omitted, 0 means success
}

// maxHistory bounds the replay buffer (a chaos run prints a few hundred lines).
const maxHistory = 5000

// hub runs at most one harness at a time and fans its output out to every
// connected SSE client. It keeps the current (or last) run's events so a page
// that connects or reloads mid-run gets the whole run, verdict included.
type hub struct {
	mu       sync.Mutex
	subs     map[chan event]struct{}
	history  []event
	running  bool
	kind     string
	proc     *os.Process
	stopWant bool // Stop arrived before the process existed
}

func newHub() *hub { return &hub{subs: map[chan event]struct{}{}} }

// subscribe registers a client and returns the events it has missed so far.
func (h *hub) subscribe() (chan event, []event) {
	ch := make(chan event, 1024)
	h.mu.Lock()
	defer h.mu.Unlock()
	h.subs[ch] = struct{}{}
	return ch, append([]event(nil), h.history...)
}

func (h *hub) unsubscribe(ch chan event) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.subs[ch]; ok {
		delete(h.subs, ch)
		close(ch)
	}
}

func (h *hub) emit(e event) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.emitLocked(e)
}

// emitLocked records e for replay and sends it to every client. A full history
// still records start and end, so a late page always learns how the run ended.
// A client too slow to keep up is disconnected rather than silently skipped;
// its browser reconnects and the replay brings it back in sync.
func (h *hub) emitLocked(e event) {
	if len(h.history) < maxHistory || e.T != "line" {
		h.history = append(h.history, e)
	}
	for ch := range h.subs {
		select {
		case ch <- e:
		default:
			delete(h.subs, ch)
			close(ch)
		}
	}
}

func (h *hub) status() (running bool, kind string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.running, h.kind
}

// start launches a PowerShell harness. args are appended after the script path.
func (h *hub) start(kind, script string, args []string) error {
	h.mu.Lock()
	if h.running {
		h.mu.Unlock()
		return fmt.Errorf("a %s run is already in progress", h.kind)
	}
	h.running, h.kind, h.history, h.stopWant = true, kind, nil, false
	h.mu.Unlock()

	pr, pw := io.Pipe()
	cmd := exec.Command("powershell", append([]string{"-NoProfile", "-ExecutionPolicy", "Bypass", "-File", script}, args...)...)
	cmd.Stdout, cmd.Stderr = pw, pw
	// Nodes the script starts can inherit the output pipe and keep it open after
	// the script exits. WaitDelay makes Wait give up on the pipe a second after
	// the script itself is gone, so the run always ends.
	cmd.WaitDelay = time.Second

	h.emit(event{T: "start", Kind: kind, Cmd: "powershell " + script + " " + strings.Join(args, " ")})
	if err := cmd.Start(); err != nil {
		h.finish(-1, "failed to start: "+err.Error())
		return nil
	}
	h.mu.Lock()
	h.proc = cmd.Process
	stopNow := h.stopWant
	h.mu.Unlock()
	if stopNow {
		h.stop()
	}

	waited := make(chan error, 1)
	go func() {
		err := cmd.Wait()
		pw.Close() // ends the scan below once every line has been read
		waited <- err
	}()
	go func() {
		sc := bufio.NewScanner(pr)
		sc.Buffer(make([]byte, 1<<20), 1<<20)
		for sc.Scan() {
			h.emit(event{T: "line", S: sc.Text()})
		}
		io.Copy(io.Discard, pr) // drain an over-long line so the writer never blocks
		err := <-waited
		code := 0
		var exitErr *exec.ExitError
		switch {
		case errors.As(err, &exitErr):
			code = exitErr.ExitCode()
		case err != nil && !errors.Is(err, exec.ErrWaitDelay):
			code = -1
		}
		h.mu.Lock()
		h.proc = nil
		h.mu.Unlock()
		h.finish(code, "") // only after the last line, so the verdict line is never late
	}()
	return nil
}

// finish marks the run over before announcing it, so a client that reacts to
// the end event can start the next run straight away.
func (h *hub) finish(code int, line string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if line != "" {
		h.emitLocked(event{T: "line", S: line})
	}
	// One critical section: a new run cannot start between these two, so this
	// end event can never land in the next run's history.
	h.running = false
	h.emitLocked(event{T: "end", Code: code})
}

// stop kills the running harness together with everything it started (the
// benchmark or raftctl), then any raftnode left behind.
func (h *hub) stop() {
	h.mu.Lock()
	p := h.proc
	h.stopWant = h.running && p == nil
	h.mu.Unlock()
	if p != nil {
		_ = exec.Command("taskkill", "/T", "/F", "/PID", strconv.Itoa(p.Pid)).Run()
	}
	killStrayNodes()
}

// killStrayNodes force-kills raftnode processes started by the harnesses or the
// demo cluster (their data lives under %TEMP%aft-kv-store), and no others.
func killStrayNodes() {
	_ = exec.Command("powershell", "-NoProfile", "-Command",
		`Get-CimInstance Win32_Process -Filter "Name='raftnode.exe'" | `+
			`Where-Object { $_.CommandLine -like '*raft-kv-store*' } | `+
			`ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }`).Run()
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

	ch, backlog := h.subscribe()
	defer h.unsubscribe(ch)
	writeSSE(w, event{T: "reset"}) // the client clears its view, then the backlog rebuilds it
	for _, e := range backlog {
		writeSSE(w, e)
	}
	fl.Flush()

	ping := time.NewTicker(15 * time.Second)
	defer ping.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case e, ok := <-ch:
			if !ok {
				return // dropped for being slow; the browser reconnects and replays
			}
			writeSSE(w, e)
			fl.Flush()
		case <-ping.C:
			fmt.Fprint(w, ": ping\n\n") // SSE comment keeps the connection warm
			fl.Flush()
		}
	}
}

func writeSSE(w io.Writer, e event) {
	b, _ := json.Marshal(e)
	fmt.Fprintf(w, "data: %s\n\n", b)
}

func main() {
	addr := flag.String("addr", "127.0.0.1:8090", "control server listen address (keep it on loopback)")
	root := flag.String("root", ".", "repo root (where scripts/ and dashboard/ live)")
	data := flag.String("data", filepath.Join(os.TempDir(), "raft-kv-store", "demo"),
		"demo cluster data directory (keep it out of OneDrive/Dropbox: sync clients lock files mid-write)")
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
	cl := newCluster(abs, *data)

	// Leave no orphaned nodes behind when the control server exits.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	go func() {
		<-sig
		cl.stopAll()
		os.Exit(0)
	}()

	log.Printf("control plane on %s  (root=%s, demo data=%s)", *addr, abs, *data)
	log.Printf("open http://%s", *addr)
	if err := http.ListenAndServe(*addr, localOnly(routes(h, cl, abs))); err != nil {
		log.Fatal(err)
	}
}

func routes(h *hub, cl *cluster, root string) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /stream", h.stream)

	mux.HandleFunc("POST /bench", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		h.begin(w, cl, "bench", filepath.Join("scripts", "bench.ps1"), []string{
			"-Writers", clampArg(q.Get("writers"), 1, 256, 24),
			"-Readers", clampArg(q.Get("readers"), 1, 256, 16),
			"-Duration", durArg(q.Get("duration"), "5s"),
			"-Threshold", clampArg(q.Get("threshold"), 25, 100000, 2000),
		})
	})

	mux.HandleFunc("POST /chaos", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		args := []string{
			"-Rounds", clampArg(q.Get("rounds"), 1, 100, 8),
			"-Threshold", clampArg(q.Get("threshold"), 5, 100000, 25),
		}
		if q.Get("killLeader") == "1" {
			args = append(args, "-KillLeader")
		}
		h.begin(w, cl, "chaos", filepath.Join("scripts", "chaos.ps1"), args)
	})

	mux.HandleFunc("POST /stop", func(w http.ResponseWriter, r *http.Request) {
		h.stop()
		cl.stopAll()
		writeJSON(w, http.StatusOK, map[string]bool{"stopped": true})
	})

	// The demo cluster's processes plus whether a test run is active.
	mux.HandleFunc("GET /cluster", func(w http.ResponseWriter, r *http.Request) {
		busy, kind := h.status()
		writeJSON(w, http.StatusOK, map[string]any{"nodes": cl.state(), "busy": busy, "kind": kind})
	})

	mux.HandleFunc("POST /cluster/start", h.unlessBusy(func(r *http.Request) error {
		return cl.startAll(r.URL.Query().Get("fresh") == "1")
	}))
	mux.HandleFunc("POST /cluster/stop", func(w http.ResponseWriter, r *http.Request) {
		cl.stopAll()
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
	})
	mux.HandleFunc("POST /node/kill", h.unlessBusy(func(r *http.Request) error { return cl.killNode(r.URL.Query().Get("id")) }))
	mux.HandleFunc("POST /node/start", h.unlessBusy(func(r *http.Request) error { return cl.startNode(r.URL.Query().Get("id")) }))

	mux.Handle("/", http.FileServer(http.Dir(filepath.Join(root, "dashboard"))))
	return mux
}

// localOnly refuses requests that did not come from a page on this machine: a
// non-local Origin (a cross-site form post or fetch) or a non-local Host (DNS
// rebinding, where an attacker's hostname resolves to 127.0.0.1).
func localOnly(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if host, _, err := net.SplitHostPort(r.Host); err != nil || !isLocalHost(host) {
			writeErr(w, http.StatusForbidden, "requests must address localhost")
			return
		}
		if o := r.Header.Get("Origin"); o != "" {
			if u, err := url.Parse(o); err != nil || !isLocalHost(u.Hostname()) {
				writeErr(w, http.StatusForbidden, "cross-origin request from a non-local page")
				return
			}
		}
		h.ServeHTTP(w, r)
	})
}

func isLocalHost(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	return ip != nil && ip.IsLoopback()
}

// unlessBusy runs a demo-cluster action, refusing while a test run owns the
// cluster ports. The hub stays locked during the action, so a run cannot start
// halfway through it (lock order is always hub, then cluster).
func (h *hub) unlessBusy(act func(*http.Request) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		h.mu.Lock()
		err := errBusy
		if !h.running {
			err = act(r)
		}
		h.mu.Unlock()
		if err == errBusy {
			writeErr(w, http.StatusConflict, "a test run is using the cluster ports; stop it first")
			return
		}
		if err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
	}
}

var errBusy = errors.New("busy")

// begin pauses the demo cluster (a test run starts its own on the same ports)
// and launches the harness, replying to the HTTP request either way.
func (h *hub) begin(w http.ResponseWriter, cl *cluster, kind, script string, args []string) {
	if busy, _ := h.status(); busy {
		writeErr(w, http.StatusConflict, "a test run is already in progress")
		return
	}
	cl.stopAll()
	if err := h.start(kind, script, args); err != nil {
		writeErr(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"started": true})
}

// writeJSON is the same small helper as in server/http.go; the two binaries
// share no package on purpose.
func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

// clampArg parses s as an int, clamps it to [lo, hi] (def if unparsable), and
// returns it as a command-line argument.
func clampArg(s string, lo, hi, def int) string {
	n, err := strconv.Atoi(s)
	if err != nil {
		return strconv.Itoa(def)
	}
	return strconv.Itoa(min(max(n, lo), hi))
}

// durArg sanitizes a Go duration string (e.g. "5s", "10s"); falls back to def.
func durArg(s, def string) string {
	if _, err := time.ParseDuration(s); err != nil {
		return def
	}
	return s
}
