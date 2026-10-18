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
		runWrite(*addr, func(c context.Context, cli raftpb.KVClient) (*raftpb.WriteReply, error) {
			return cli.Set(c, &raftpb.SetRequest{Key: args[1], Value: args[2]})
		})
	case "del", "delete":
		need(args, 2)
		runWrite(*addr, func(c context.Context, cli raftpb.KVClient) (*raftpb.WriteReply, error) {
			return cli.Delete(c, &raftpb.DeleteRequest{Key: args[1]})
		})
	case "status":
		runStatus(*addr)
	default:
		usage()
	}
}

// followLeader calls the node at addr and, while the reply carries a leader
// hint pointing elsewhere, calls that node instead. call returns the hint from
// its reply ("" when the node served the request). It exits on an RPC error.
func followLeader(addr string, call func(context.Context, raftpb.KVClient) (hint string, err error)) {
	for hop := 0; hop < maxHops; hop++ {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			fail("connect %s: %v", addr, err)
		}
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		hint, err := call(c, raftpb.NewKVClient(conn))
		cancel()
		conn.Close()
		if err != nil {
			fail("rpc error: %v", err)
		}
		if hint == "" {
			return
		}
		if hint == addr {
			fail("FAILED: %s names itself as leader but did not serve the request", addr)
		}
		addr = hint
	}
	fail("giving up after %d hops (no stable leader)", maxHops)
}

func runWrite(addr string, write func(context.Context, raftpb.KVClient) (*raftpb.WriteReply, error)) {
	followLeader(addr, func(c context.Context, cli raftpb.KVClient) (string, error) {
		r, err := write(c, cli)
		if err != nil {
			return "", err
		}
		if r.GetOk() {
			fmt.Println("OK")
			return "", nil
		}
		if r.GetLeaderHint() == "" {
			fail("FAILED: %s", r.GetError())
		}
		return r.GetLeaderHint(), nil
	})
}

func runGet(addr, key string) {
	followLeader(addr, func(c context.Context, cli raftpb.KVClient) (string, error) {
		r, err := cli.Get(c, &raftpb.GetRequest{Key: key})
		if err != nil || r.GetLeaderHint() != "" {
			return r.GetLeaderHint(), err
		}
		if !r.GetFound() {
			fmt.Println("(not found)")
			os.Exit(1)
		}
		fmt.Println(r.GetValue())
		return "", nil
	})
}

func runStatus(addr string) {
	followLeader(addr, func(c context.Context, cli raftpb.KVClient) (string, error) {
		r, err := cli.Status(c, &raftpb.StatusRequest{})
		if err != nil {
			return "", err
		}
		fmt.Printf("node=%s role=%s term=%d leader=%s commit=%d applied=%d snapshot=%d logLen=%d keys=%d digest=%s peers=%v\n",
			r.GetNodeId(), r.GetRole(), r.GetTerm(), r.GetLeaderId(), r.GetCommitIndex(), r.GetLastApplied(),
			r.GetSnapshotIndex(), r.GetLogLen(), r.GetNumKeys(), r.GetStateDigest(), r.GetPeers())
		return "", nil
	})
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
