package main

import (
	"strings"
	"testing"
)

func TestNodeArgs(t *testing.T) {
	specs := nodeSpecs()
	if len(specs) != 5 {
		t.Fatalf("want 5 nodes, got %d", len(specs))
	}
	args := strings.Join(nodeArgs(specs[2], specs, "d"), " ")
	for _, want := range []string{
		"--id n3", "--addr :9003", "--advertise localhost:9003", "--http :8003",
		"--peers localhost:9001,localhost:9002,localhost:9004,localhost:9005",
	} {
		if !strings.Contains(args, want) {
			t.Errorf("args missing %q:\n%s", want, args)
		}
	}
	if strings.Contains(args, "localhost:9003,") || strings.HasSuffix(strings.Split(args, "--peers ")[1], "localhost:9003") {
		t.Error("a node must not list itself as a peer")
	}
}

func TestClusterRejectsUnknownAndStoppedNodes(t *testing.T) {
	c := newCluster(t.TempDir(), t.TempDir())
	if err := c.killNode("n9"); err == nil {
		t.Error("kill of unknown node should fail")
	}
	if err := c.killNode("n1"); err == nil {
		t.Error("kill of a node that is not running should fail")
	}
	if err := c.startNode("n1"); err == nil {
		t.Error("start without a built binary should explain how to build it")
	}
	for _, n := range c.state() {
		if n.Running {
			t.Errorf("%s reported running", n.ID)
		}
	}
}
