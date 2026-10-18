package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

// The demo cluster is five local raftnode processes owned by the control server,
// so the dashboard can kill and restart individual nodes. Ports follow the
// project convention: gRPC on 9001+, HTTP gateway on gRPC port minus 1000.
const (
	nodeCount      = 5
	grpcBase       = 9001
	httpPortOffset = 1000
	demoThreshold  = 50 // low enough that a short burst of writes visibly compacts the log
)

type nodeInfo struct {
	ID      string `json:"id"`
	GRPC    int    `json:"grpc"`
	HTTP    int    `json:"http"`
	Running bool   `json:"running"`
}

func nodeSpecs() []nodeInfo {
	out := make([]nodeInfo, nodeCount)
	for i := range out {
		out[i] = nodeInfo{ID: "n" + strconv.Itoa(i+1), GRPC: grpcBase + i, HTTP: grpcBase + i - httpPortOffset}
	}
	return out
}

func (n nodeInfo) advertise() string { return fmt.Sprintf("localhost:%d", n.GRPC) }

// nodeArgs builds the raftnode command line for one member of the demo cluster.
func nodeArgs(spec nodeInfo, all []nodeInfo, dataDir string) []string {
	var peers []string
	for _, o := range all {
		if o.ID != spec.ID {
			peers = append(peers, o.advertise())
		}
	}
	return []string{
		"--id", spec.ID,
		"--addr", fmt.Sprintf(":%d", spec.GRPC),
		"--advertise", spec.advertise(),
		"--peers", strings.Join(peers, ","),
		"--read-mode", "follower",
		"--data-dir", filepath.Join(dataDir, spec.ID),
		"--snapshot-threshold", strconv.Itoa(demoThreshold),
		"--http", fmt.Sprintf(":%d", spec.HTTP),
	}
}

type cluster struct {
	mu      sync.Mutex
	root    string // repo root, where bin/ lives
	dataDir string // node data and logs; kept out of synced folders on purpose
	specs   []nodeInfo
	procs   map[string]*proc // only nodes currently running
}

// proc is one running node; done closes once the process has exited.
type proc struct {
	cmd  *exec.Cmd
	done chan struct{}
}

func newCluster(root, dataDir string) *cluster {
	return &cluster{root: root, dataDir: dataDir, specs: nodeSpecs(), procs: map[string]*proc{}}
}

func (c *cluster) binary() string {
	name := "raftnode"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	return filepath.Join(c.root, "bin", name)
}

func (c *cluster) spec(id string) (nodeInfo, bool) {
	for _, s := range c.specs {
		if s.ID == id {
			return s, true
		}
	}
	return nodeInfo{}, false
}

// state reports every node and whether its process is currently alive.
func (c *cluster) state() []nodeInfo {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]nodeInfo, len(c.specs))
	for i, s := range c.specs {
		_, s.Running = c.procs[s.ID]
		out[i] = s
	}
	return out
}

// startNode launches one node, resuming from its data directory. No-op if it
// is already running.
func (c *cluster) startNode(id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.startLocked(id)
}

func (c *cluster) startLocked(id string) error {
	spec, ok := c.spec(id)
	if !ok {
		return fmt.Errorf("unknown node %q", id)
	}
	if _, running := c.procs[id]; running {
		return nil
	}
	bin := c.binary()
	if _, err := os.Stat(bin); err != nil {
		return fmt.Errorf("%s not found: build it with  go build -o bin/%s ./server", bin, filepath.Base(bin))
	}
	if err := os.MkdirAll(c.dataDir, 0o755); err != nil {
		return err
	}
	logFile, err := os.OpenFile(filepath.Join(c.dataDir, id+".log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	cmd := exec.Command(bin, nodeArgs(spec, c.specs, c.dataDir)...)
	cmd.Dir = c.root
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		logFile.Close()
		return err
	}
	p := &proc{cmd: cmd, done: make(chan struct{})}
	c.procs[id] = p
	go func() {
		_ = cmd.Wait()
		logFile.Close()
		close(p.done)
		c.mu.Lock()
		if c.procs[id] == p { // exited on its own (crash), not via killNode
			delete(c.procs, id)
		}
		c.mu.Unlock()
	}()
	return nil
}

// killNode force-kills one node, the same as a crash: no clean shutdown. The
// node stays listed until its process has actually exited (killing is
// asynchronous on Windows), so a Start in the meantime is a no-op instead of a
// second process racing the dying one for its port and files.
func (c *cluster) killNode(id string) error {
	if _, ok := c.spec(id); !ok {
		return fmt.Errorf("unknown node %q", id)
	}
	c.mu.Lock()
	p, running := c.procs[id]
	c.mu.Unlock()
	if !running {
		return fmt.Errorf("%s is not running", id)
	}
	if err := p.cmd.Process.Kill(); err != nil {
		return err
	}
	select { // the exit watcher in startLocked removes the entry
	case <-p.done:
	case <-time.After(5 * time.Second):
	}
	return nil
}

// startAll launches every node that is not already running. With fresh set and
// no node running, the previous demo data is wiped first.
func (c *cluster) startAll(fresh bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if fresh && len(c.procs) == 0 {
		if err := removeAllRetry(c.dataDir); err != nil {
			return err
		}
	}
	for _, s := range c.specs {
		if err := c.startLocked(s.ID); err != nil {
			return err
		}
	}
	return nil
}

// stopAll kills every node and waits for the processes to exit, so their ports
// and files are free when it returns.
func (c *cluster) stopAll() {
	c.mu.Lock()
	procs := c.procs
	c.procs = map[string]*proc{}
	c.mu.Unlock()
	for _, p := range procs {
		_ = p.cmd.Process.Kill()
	}
	for _, p := range procs {
		select {
		case <-p.done:
		case <-time.After(5 * time.Second):
		}
	}
}

// removeAllRetry deletes dir. A just-killed process can hold files for a moment
// on Windows, so retry briefly.
func removeAllRetry(dir string) error {
	var err error
	for i := 0; i < 20; i++ {
		if err = os.RemoveAll(dir); err == nil {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return err
}
