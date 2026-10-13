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
	nodeCount     = 5
	grpcBase      = 9001
	demoDataDir   = "datademo"
	demoThreshold = 50 // low enough that a short burst of writes visibly compacts the log
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
		out[i] = nodeInfo{ID: "n" + strconv.Itoa(i+1), GRPC: grpcBase + i, HTTP: grpcBase + i - 1000}
	}
	return out
}

// nodeArgs builds the raftnode command line for one member of the demo cluster.
func nodeArgs(spec nodeInfo, all []nodeInfo) []string {
	var peers []string
	for _, o := range all {
		if o.ID != spec.ID {
			peers = append(peers, fmt.Sprintf("localhost:%d", o.GRPC))
		}
	}
	return []string{
		"--id", spec.ID,
		"--addr", fmt.Sprintf(":%d", spec.GRPC),
		"--advertise", fmt.Sprintf("localhost:%d", spec.GRPC),
		"--peers", strings.Join(peers, ","),
		"--read-mode", "follower",
		"--data-dir", filepath.Join(demoDataDir, spec.ID),
		"--snapshot-threshold", strconv.Itoa(demoThreshold),
		"--http", fmt.Sprintf(":%d", spec.HTTP),
	}
}

type cluster struct {
	mu    sync.Mutex
	root  string
	specs []nodeInfo
	procs map[string]*exec.Cmd // only nodes currently running
}

func newCluster(root string) *cluster {
	return &cluster{root: root, specs: nodeSpecs(), procs: map[string]*exec.Cmd{}}
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

func (c *cluster) anyRunning() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.procs) > 0
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
	if err := os.MkdirAll(filepath.Join(c.root, demoDataDir), 0o755); err != nil {
		return err
	}
	logFile, err := os.OpenFile(filepath.Join(c.root, demoDataDir, id+".log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	cmd := exec.Command(bin, nodeArgs(spec, c.specs)...)
	cmd.Dir = c.root
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		logFile.Close()
		return err
	}
	c.procs[id] = cmd
	go func() {
		_ = cmd.Wait()
		logFile.Close()
		c.mu.Lock()
		if c.procs[id] == cmd { // exited on its own (crash), not via killNode
			delete(c.procs, id)
		}
		c.mu.Unlock()
	}()
	return nil
}

// killNode force-kills one node, the same as a crash: no clean shutdown.
func (c *cluster) killNode(id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.spec(id); !ok {
		return fmt.Errorf("unknown node %q", id)
	}
	cmd, running := c.procs[id]
	if !running {
		return fmt.Errorf("%s is not running", id)
	}
	delete(c.procs, id)
	return cmd.Process.Kill()
}

// startAll launches every node that is not already running. With fresh set and
// no node running, the previous demo data is wiped first.
func (c *cluster) startAll(fresh bool) error {
	if fresh && !c.anyRunning() {
		if err := c.wipe(); err != nil {
			return err
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, s := range c.specs {
		if err := c.startLocked(s.ID); err != nil {
			return err
		}
	}
	return nil
}

func (c *cluster) stopAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, cmd := range c.procs {
		_ = cmd.Process.Kill()
		delete(c.procs, id)
	}
}

// wipe removes the demo data directory. A just-killed process can hold files for
// a moment on Windows, so retry briefly.
func (c *cluster) wipe() error {
	dir := filepath.Join(c.root, demoDataDir)
	var err error
	for i := 0; i < 10; i++ {
		if err = os.RemoveAll(dir); err == nil {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return err
}
