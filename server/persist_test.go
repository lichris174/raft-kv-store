package main

import (
	"os"
	"path/filepath"
	"testing"

	raftpb "github.com/chrisli/raft-kv-store/proto"
)

// TestLoadSanitizesDuplicateBoundaryAndGap reproduces the snapshot-boundary log
// corruption that the chaos test surfaced: a duplicate entry at snapshotIndex+1
// (from a snapshot rewrite racing a log append) plus a downstream gap. Load must
// return a clean, contiguous tail starting at snapshotIndex+1 so the in-memory
// offset arithmetic stays valid; unreliable post-gap entries are dropped and
// refetched from the leader on rejoin.
func TestLoadSanitizesDuplicateBoundaryAndGap(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "meta.json"),
		[]byte(`{"current_term":4,"voted_for":"","snapshot_index":25,"snapshot_term":1}`), 0o644); err != nil {
		t.Fatal(err)
	}
	corrupt := "" +
		`{"term":1,"index":26,"op":"set","key":"a","value":"1"}` + "\n" +
		`{"term":1,"index":26,"op":"set","key":"a","value":"1"}` + "\n" + // duplicate boundary
		`{"term":1,"index":27,"op":"set","key":"b","value":"2"}` + "\n" +
		`{"term":4,"index":29,"op":"set","key":"d","value":"4"}` + "\n" + // gap: index 28 missing
		`{"term":4,"index":30,"op":"set","key":"e","value":"5"}` + "\n"
	if err := os.WriteFile(filepath.Join(dir, "log.jsonl"), []byte(corrupt), 0o644); err != nil {
		t.Fatal(err)
	}

	p, err := newFilePersister(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	meta, _, log, err := p.Load()
	if err != nil {
		t.Fatal(err)
	}
	if meta.SnapshotIndex != 25 {
		t.Fatalf("snapshotIndex=%d want 25", meta.SnapshotIndex)
	}
	// Expect exactly 26, 27 (dup removed, gap truncates 29/30).
	want := []uint64{26, 27}
	if len(log) != len(want) {
		t.Fatalf("got %d entries %v, want %v", len(log), indicesOf(log), want)
	}
	for i, e := range log {
		if e.Index != want[i] {
			t.Fatalf("entry %d index=%d want %d", i, e.Index, want[i])
		}
		// contiguity: each index is snapshotIndex + 1 + position
		if e.Index != meta.SnapshotIndex+1+uint64(i) {
			t.Fatalf("entry %d breaks contiguity: index=%d", i, e.Index)
		}
	}
}

func indicesOf(log []*raftpb.LogEntry) []uint64 {
	out := make([]uint64, 0, len(log))
	for _, e := range log {
		out = append(out, e.Index)
	}
	return out
}
