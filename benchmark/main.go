// Command benchmark loads a raft-kv-store cluster and reports write/read
// throughput and latency percentiles.
//
//	benchmark -addrs localhost:9001,localhost:9002,... -writers 16 -readers 16 -duration 5s
//
// Writers route to the leader (following NOT_LEADER hints). Readers spread
// round-robin across all nodes; run the cluster with -read-mode follower for
// scalable local reads, or leave it leader-only to measure linearizable reads.
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	raftpb "github.com/chrisli/raft-kv-store/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	addrsCSV := flag.String("addrs", "localhost:9001", "comma-separated node addresses")
	writers := flag.Int("writers", 16, "concurrent writer goroutines")
	readers := flag.Int("readers", 16, "concurrent reader goroutines")
	duration := flag.Duration("duration", 5*time.Second, "duration of each phase")
	keyspace := flag.Int("keyspace", 10000, "number of distinct keys")
	flag.Parse()

	addrs := splitCSV(*addrsCSV)
	if len(addrs) == 0 {
		fmt.Println("need -addrs")
		return
	}

	cs := dialAll(addrs)
	defer cs.close()

	fmt.Printf("cluster: %v\nwriters=%d readers=%d duration=%s keyspace=%d\n\n",
		addrs, *writers, *readers, *duration, *keyspace)

	leader := resolveLeader(cs.clients)
	if leader == "" {
		fmt.Println("no leader found, is the cluster up?")
		return
	}
	fmt.Printf("leader: %s\n\n", leader)

	wr := runPhase("WRITE", *writers, *duration, addrs, leader, func(ctx context.Context, rng *rand.Rand, conns map[string]raftpb.KVClient) (time.Duration, bool) {
		key := fmt.Sprintf("k%d", rng.Intn(*keyspace))
		return writeOnce(ctx, conns, addrs, key, "value")
	})

	rd := runPhase("READ", *readers, *duration, addrs, leader, func(ctx context.Context, rng *rand.Rand, conns map[string]raftpb.KVClient) (time.Duration, bool) {
		key := fmt.Sprintf("k%d", rng.Intn(*keyspace))
		addr := addrs[rng.Intn(len(addrs))]
		return readOnce(ctx, conns, addr, key)
	})

	fmt.Println(strings.Repeat("=", 56))
	wr.print()
	rd.print()
}

// leaderAddr is shared across writers; updated when a NOT_LEADER hint arrives.
var leaderAddr atomic.Value // string

func writeOnce(ctx context.Context, conns map[string]raftpb.KVClient, addrs []string, key, val string) (time.Duration, bool) {
	addr, _ := leaderAddr.Load().(string)
	if addr == "" {
		addr = addrs[0]
	}
	start := time.Now()
	// A few attempts: follow NOT_LEADER hints and retry transient RPC errors
	// (e.g. a brief UNAVAILABLE during a leader change) before counting a failure.
	for attempt := 0; attempt < 5; attempt++ {
		cli := conns[addr]
		if cli == nil {
			addr = addrs[0]
			continue
		}
		cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		r, err := cli.Set(cctx, &raftpb.SetRequest{Key: key, Value: val})
		cancel()
		if err != nil {
			continue // transient: retry
		}
		if r.GetOk() {
			return time.Since(start), true
		}
		if h := r.GetLeaderHint(); h != "" && conns[h] != nil {
			leaderAddr.Store(h)
			addr = h
			continue
		}
		return 0, false
	}
	return 0, false
}

func readOnce(ctx context.Context, conns map[string]raftpb.KVClient, addr, key string) (time.Duration, bool) {
	cli := conns[addr]
	if cli == nil {
		return 0, false
	}
	start := time.Now()
	// Retry a transient RPC error a couple of times before counting a failure; a
	// not-found result or a leader-mode redirect is still a successful round-trip.
	for try := 0; try < 3; try++ {
		cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		_, err := cli.Get(cctx, &raftpb.GetRequest{Key: key})
		cancel()
		if err == nil {
			return time.Since(start), true
		}
	}
	return 0, false
}

type result struct {
	name    string
	ops     int64
	dur     time.Duration
	lats    []time.Duration
	errored int64
}

func runPhase(name string, workers int, dur time.Duration, addrs []string, leader string,
	op func(context.Context, *rand.Rand, map[string]raftpb.KVClient) (time.Duration, bool)) result {

	leaderAddr.Store(leader)
	ctx, cancel := context.WithTimeout(context.Background(), dur)
	defer cancel()

	var wg sync.WaitGroup
	latCh := make(chan []time.Duration, workers)
	var ops, errs int64

	start := time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			cs := dialAll(addrs) // each worker owns its connections (no shared-socket contention)
			defer cs.close()
			rng := rand.New(rand.NewSource(seed))
			local := make([]time.Duration, 0, 4096)
			for {
				select {
				case <-ctx.Done():
					latCh <- local
					return
				default:
				}
				lat, ok := op(ctx, rng, cs.clients)
				if ok {
					atomic.AddInt64(&ops, 1)
					local = append(local, lat)
				} else {
					atomic.AddInt64(&errs, 1)
				}
			}
		}(int64(w)*7919 + time.Now().UnixNano())
	}
	wg.Wait()
	elapsed := time.Since(start)
	close(latCh)

	all := make([]time.Duration, 0, ops)
	for l := range latCh {
		all = append(all, l...)
	}
	return result{name: name, ops: ops, dur: elapsed, lats: all, errored: errs}
}

func (r result) print() {
	tput := float64(r.ops) / r.dur.Seconds()
	fmt.Printf("%-6s  ops=%-8d  %.0f ops/s  errors=%d\n", r.name, r.ops, tput, r.errored)
	if len(r.lats) == 0 {
		return
	}
	sort.Slice(r.lats, func(i, j int) bool { return r.lats[i] < r.lats[j] })
	p := func(q float64) time.Duration { return r.lats[int(float64(len(r.lats)-1)*q)] }
	fmt.Printf("        latency  p50=%s  p95=%s  p99=%s  max=%s\n\n",
		round(p(0.50)), round(p(0.95)), round(p(0.99)), round(r.lats[len(r.lats)-1]))
}

func round(d time.Duration) time.Duration { return d.Round(10 * time.Microsecond) }

// --- helpers ---

func splitCSV(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// connSet is one set of gRPC connections to every node. Each load-generating
// worker owns its own connSet: sharing a single connection across many concurrent
// workers serializes streams on one socket and produces transient RPC errors that
// look like cluster failures but are pure client-side contention.
type connSet struct {
	clients map[string]raftpb.KVClient
	conns   []*grpc.ClientConn
}

func dialAll(addrs []string) *connSet {
	cs := &connSet{clients: map[string]raftpb.KVClient{}}
	for _, a := range addrs {
		conn, err := grpc.NewClient(a, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			continue
		}
		cs.clients[a] = raftpb.NewKVClient(conn)
		cs.conns = append(cs.conns, conn)
	}
	return cs
}

func (cs *connSet) close() {
	for _, c := range cs.conns {
		_ = c.Close()
	}
}

func resolveLeader(conns map[string]raftpb.KVClient) string {
	for addr, cli := range conns {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		r, err := cli.Status(ctx, &raftpb.StatusRequest{})
		cancel()
		if err == nil && r.GetRole() == "leader" {
			if r.GetLeaderId() != "" {
				return r.GetLeaderId()
			}
			return addr
		}
		if err == nil && r.GetLeaderId() != "" {
			return r.GetLeaderId()
		}
	}
	return ""
}
