package main

import (
	"flag"
	"log"
	"net"
	"strings"

	raftpb "github.com/chrisli/raft-kv-store/proto"
	"google.golang.org/grpc"
)

func main() {
	id := flag.String("id", "n1", "node id")
	addr := flag.String("addr", ":8001", "gRPC listen address")
	advertise := flag.String("advertise", "", "advertised dialable address (defaults to addr); used as raft identity")
	peersCSV := flag.String("peers", "", "comma-separated peer advertised addresses (empty for single node)")
	readMode := flag.String("read-mode", "leader", "read mode: leader | follower")
	dataDir := flag.String("data-dir", "", "directory for durable Raft state (empty = in-memory, no persistence)")
	snapThreshold := flag.Int("snapshot-threshold", 100, "applied entries past last snapshot before compaction")
	httpAddr := flag.String("http", "", "HTTP/JSON gateway address for the dashboard (e.g. :8001)")
	fsync := flag.Bool("fsync", false, "fsync every durable write before acknowledging (survives power loss, costs write latency)")
	flag.Parse()

	var peers []string
	if strings.TrimSpace(*peersCSV) != "" {
		for _, p := range strings.Split(*peersCSV, ",") {
			if p = strings.TrimSpace(p); p != "" {
				peers = append(peers, p)
			}
		}
	}

	self := *advertise
	if self == "" {
		self = *addr
	}

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen %s: %v", *addr, err)
	}

	kv := newKVServer(*id, *readMode, peers)
	grpcServer := grpc.NewServer()
	raftpb.RegisterKVServer(grpcServer, kv)

	// Only run Raft when peers are configured; a lone node stays the Feature 1
	// single-node path (always its own leader) without election churn.
	if len(peers) > 0 {
		var persister Persister
		if *dataDir != "" {
			fp, err := newFilePersister(*dataDir, *fsync)
			if err != nil {
				log.Fatalf("data-dir %s: %v", *dataDir, err)
			}
			persister = fp
		}
		cfg := Config{
			ID:                *id,
			Addr:              self,
			Peers:             peers,
			Apply:             func(e *raftpb.LogEntry) { applyOp(kv.store, e.GetOp(), e.GetKey(), e.GetValue()) },
			Snapshot:          kv.store.Snapshot,
			Restore:           kv.store.Restore,
			SnapshotThreshold: *snapThreshold,
			Persister:         persister,
		}
		rf := NewRaft(cfg, newGRPCTransport())
		kv.raft = rf
		raftpb.RegisterRaftServer(grpcServer, &raftService{r: rf})
		go rf.Run()
		log.Printf("node %s raft enabled (advertise=%s, peers=%v, dataDir=%q, fsync=%v)", *id, self, peers, *dataDir, *fsync)
	}

	if *httpAddr != "" {
		go kv.serveHTTP(*httpAddr)
	}

	log.Printf("node %s listening on %s (read-mode=%s)", *id, *addr, *readMode)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
