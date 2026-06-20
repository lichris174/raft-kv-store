// Command client is a CLI for testing the raft-kv-store. It auto-follows leader
// hints: a write (or leader-mode read) sent to a follower is transparently
// retried against the leader the node points to.
//
//	client -addr :8001 set foo bar
//	client -addr :8001 get foo
//	client -addr :8001 del foo
//	client -addr :8001 status
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	raftpb "github.com/chrisli/raft-kv-store/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const maxHops = 4

func main() {
	addr := flag.String("addr", ":8001", "node address")
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		usage()
	}
	switch args[0] {
	case "get":
		need(args, 2)
		runGet(*addr, args[1])
	case "set":
		need(args, 3)
		runWrite(*addr, "set", args[1], args[2])
	case "del", "delete":
		need(args, 2)
		runWrite(*addr, "del", args[1], "")
	case "status":
		runStatus(*addr)
	default:
		usage()
	}
}

func dial(addr string) (*grpc.ClientConn, raftpb.KVClient) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fail("connect %s: %v", addr, err)
	}
	return conn, raftpb.NewKVClient(conn)
}

func ctx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 5*time.Second)
}

// runWrite follows WriteReply.LeaderHint until the op lands or hops run out.
func runWrite(addr, op, key, value string) {
	for hop := 0; hop < maxHops; hop++ {
		conn, cli := dial(addr)
		c, cancel := ctx()
		var r *raftpb.WriteReply
		var err error
		if op == "set" {
			r, err = cli.Set(c, &raftpb.SetRequest{Key: key, Value: value})
		} else {
			r, err = cli.Delete(c, &raftpb.DeleteRequest{Key: key})
		}
		cancel()
		conn.Close()
		if err != nil {
			fail("rpc error: %v", err)
		}
		if r.GetOk() {
			fmt.Println("OK")
			return
		}
		if hint := r.GetLeaderHint(); hint != "" && hint != addr {
			addr = hint
			continue
		}
		fail("FAILED: %s", r.GetError())
	}
	fail("giving up after %d hops (no stable leader)", maxHops)
}

// runGet follows GetReply.LeaderHint (set when a non-leader can't serve a
// linearizable read) until a node answers.
func runGet(addr, key string) {
	for hop := 0; hop < maxHops; hop++ {
		conn, cli := dial(addr)
		c, cancel := ctx()
		r, err := cli.Get(c, &raftpb.GetRequest{Key: key})
		cancel()
		conn.Close()
		if err != nil {
			fail("rpc error: %v", err)
		}
		if hint := r.GetLeaderHint(); hint != "" && hint != addr {
			addr = hint
			continue
		}
		if r.GetFound() {
			fmt.Println(r.GetValue())
		} else {
			fmt.Println("(not found)")
			os.Exit(1)
		}
		return
	}
	fail("giving up after %d hops (no stable leader)", maxHops)
}

func runStatus(addr string) {
	conn, cli := dial(addr)
	defer conn.Close()
	c, cancel := ctx()
	defer cancel()
	r, err := cli.Status(c, &raftpb.StatusRequest{})
	if err != nil {
		fail("rpc error: %v", err)
	}
	fmt.Printf("node=%s role=%s term=%d leader=%s commit=%d applied=%d snapshot=%d logLen=%d keys=%d peers=%v\n",
		r.GetNodeId(), r.GetRole(), r.GetTerm(), r.GetLeaderId(),
		r.GetCommitIndex(), r.GetLastApplied(), r.GetSnapshotIndex(), r.GetLogLen(),
		r.GetNumKeys(), r.GetPeers())
}

func need(args []string, n int) {
	if len(args) < n {
		usage()
	}
}

func fail(format string, a ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", a...)
	os.Exit(1)
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: client -addr ADDR <get KEY | set KEY VAL | del KEY | status>")
	os.Exit(2)
}
