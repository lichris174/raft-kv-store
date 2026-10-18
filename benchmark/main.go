// Command benchmark loads a raft-kv-store cluster and reports write/read
// throughput and latency percentiles.
//
//	benchmark -addrs localhost:9001,localhost:9002,... -writers 16 -readers 16 -duration 5s
//
// Writers route to the leader (following NOT_LEADER hints). Readers spread
// randomly across all nodes; run the cluster with -read-mode follower for
// scalable local reads, or leave it leader-only to measure linearizable reads.
// Every failed operation is tallied by reason, so a non-zero error count always
// says what went wrong. The exit code is 1 if nothing could be measured or any
// operation failed, so scripts can tell a clean run from a broken one.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	raftpb "github.com/chrisli/raft-kv-store/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const rpcTimeout = 2 * time.Second

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
		os.Exit(1)
	}
	fmt.Printf("cluster: %v\nwriters=%d readers=%d duration=%s keyspace=%d\n\n",
		addrs, *writers, *readers, *duration, *keyspace)

	cs := dialAll(addrs)
	leader := resolveLeader(cs.clients)
	cs.close()
	if leader == "" {
		fmt.Println("no leader found, is the cluster up?")
		os.Exit(1)
	}
	fmt.Printf("leader: %s\n\n", leader)
	leaderAddr.Store(leader)

	randomKey := func(rng *rand.Rand) string { return fmt.Sprintf("k%d", rng.Intn(*keyspace)) }
	wr := runPhase("WRITE", *writers, *duration, addrs, func(rng *rand.Rand, conns map[string]raftpb.KVClient) error {
		return writeOnce(conns, addrs, randomKey(rng), "value")
	})
	rd := runPhase("READ", *readers, *duration, addrs, func(rng *rand.Rand, conns map[string]raftpb.KVClient) error {
		return readOnce(conns[addrs[rng.Intn(len(addrs))]], randomKey(rng))
	})

	fmt.Println(strings.Repeat("=", 56))
	wr.print()
	rd.print()
	if wr.errors()+rd.errors() > 0 || len(wr.lats) == 0 || len(rd.lats) == 0 {
		os.Exit(1) // any failure, or a phase that completed nothing
	}
}

// leaderAddr is shared across writers; updated when a NOT_LEADER hint arrives.
var leaderAddr atomic.Value // string

// writeOnce sets key, following NOT_LEADER hints and retrying transient RPC
// errors (e.g. a brief UNAVAILABLE during a leader change) a few times.
func writeOnce(conns map[string]raftpb.KVClient, addrs []string, key, val string) error {
	addr, _ := leaderAddr.Load().(string)
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		cli := conns[addr]
		if cli == nil {
			addr = addrs[0]
			if cli = conns[addr]; cli == nil {
				return fmt.Errorf("no connection to %s", addr)
			}
		}
		cctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		r, rpcErr := cli.Set(cctx, &raftpb.SetRequest{Key: key, Value: val})
		cancel()
		switch {
		case rpcErr != nil:
			err = rpcErr
		case r.GetOk():
			return nil
		case r.GetLeaderHint() != "" && conns[r.GetLeaderHint()] != nil:
			addr = r.GetLeaderHint()
			leaderAddr.Store(addr)
			err = errors.New("not leader")
		default:
			return errors.New(r.GetError())
		}
	}
	return err
}

// readOnce gets key from one node, retrying a transient RPC error. A not-found
// result is a successful read. A redirect is not: it means the cluster runs in
// leader read mode, where this benchmark's spread-out reads do not apply.
func readOnce(cli raftpb.KVClient, key string) error {
	if cli == nil {
		return errors.New("no connection")
	}
	var err error
	for try := 0; try < 3; try++ {
		cctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		var r *raftpb.GetReply
		r, err = cli.Get(cctx, &raftpb.GetRequest{Key: key})
		cancel()
		if err == nil && r.GetLeaderHint() != "" {
			return errors.New("redirected to the leader (cluster is in leader read mode; use -read-mode follower)")
		}
		if err == nil {
			return nil
		}
	}
	return err
}

type result struct {
	name    string
	dur     time.Duration
	lats    []time.Duration // one per successful operation
	reasons map[string]int  // failure reason -> count
}

// worker is one load generator's private tally, merged after the phase ends.
type worker struct {
	lats    []time.Duration
	reasons map[string]int
}

func runPhase(name string, workers int, dur time.Duration, addrs []string,
	op func(*rand.Rand, map[string]raftpb.KVClient) error) result {

	// Workers stop starting new requests at the deadline, but a request already in
	// flight runs to completion on its own timeout. Cancelling it instead would make
	// the benchmark's own shutdown show up as errors (it used to add about one per
	// worker to every run). Only successes inside the phase count toward
	// throughput, which is over the phase length, so a straggler cannot skew it;
	// every failure counts, whenever it surfaces.
	deadline := time.Now().Add(dur)
	tallies := make([]worker, workers)
	var wg sync.WaitGroup
	for i := range tallies {
		wg.Add(1)
		go func(w *worker, seed int64) {
			defer wg.Done()
			cs := dialAll(addrs) // each worker owns its connections (no shared-socket contention)
			defer cs.close()
			rng := rand.New(rand.NewSource(seed))
			w.reasons = map[string]int{}
			for time.Now().Before(deadline) {
				t0 := time.Now()
				err := op(rng, cs.clients)
				switch {
				case err != nil: // a real failure even if it surfaced after the deadline
					w.reasons[reason(err)]++
				case time.Now().Before(deadline):
					w.lats = append(w.lats, time.Since(t0))
				}
			}
		}(&tallies[i], int64(i)*7919+time.Now().UnixNano())
	}
	wg.Wait()

	res := result{name: name, dur: dur, reasons: map[string]int{}}
	for _, w := range tallies {
		res.lats = append(res.lats, w.lats...)
		for k, v := range w.reasons {
			res.reasons[k] += v
		}
	}
	return res
}

// reason condenses an error to a short, groupable label.
func reason(err error) string {
	s := err.Error()
	if i := strings.Index(s, "desc = "); i >= 0 { // gRPC status: keep the description
		s = s[i+len("desc = "):]
	}
	if len(s) > 60 {
		s = s[:60] + "..."
	}
	return s
}

func (r result) errors() int {
	n := 0
	for _, c := range r.reasons {
		n += c
	}
	return n
}

// print writes the phase summary. dashboard/tests.js parses these lines.
func (r result) print() {
	ops := len(r.lats)
	fmt.Printf("%-6s  ops=%-8d  %.0f ops/s  errors=%d\n", r.name, ops, float64(ops)/r.dur.Seconds(), r.errors())
	for msg, n := range r.reasons {
		fmt.Printf("        error x%d: %s\n", n, msg)
	}
	if ops == 0 {
		fmt.Println()
		return
	}
	sort.Slice(r.lats, func(i, j int) bool { return r.lats[i] < r.lats[j] })
	// Plain "ms" rather than Duration's "µs": the micro sign is mangled by the
	// Windows console code page on its way to the report and the dashboard.
	p := func(q float64) string { return fmt.Sprintf("%.2fms", float64(r.lats[int(float64(ops-1)*q)])/1e6) }
	fmt.Printf("        latency  p50=%s  p95=%s  p99=%s  max=%s\n\n", p(0.50), p(0.95), p(0.99), p(1))
}

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

// resolveLeader asks each node for status and returns the leader's address
// (every node, the leader included, reports it as LeaderId).
func resolveLeader(conns map[string]raftpb.KVClient) string {
	for _, cli := range conns {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		r, err := cli.Status(ctx, &raftpb.StatusRequest{})
		cancel()
		if err != nil {
			continue
		}
		if id := r.GetLeaderId(); id != "" {
			return id
		}
	}
	return ""
}
