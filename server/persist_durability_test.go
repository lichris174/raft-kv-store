package main

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	raftpb "github.com/chrisli/raft-kv-store/proto"
)

// Regression tests for the persistence fixes documented in docs/HARDENING.md.

func entries(from, to uint64) []*raftpb.LogEntry {
	var out []*raftpb.LogEntry
	for i := from; i <= to; i++ {
		out = append(out, &raftpb.LogEntry{Term: 1, Index: i, Op: "set", Key: "k", Value: "v"})
	}
	return out
}

func mustPersister(t *testing.T, dir string) *filePersister {
	t.Helper()
	p, err := newFilePersister(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}

// Compaction rewrites three files (snapshot, log, meta) and a crash can land
// between any two of them. Whatever subset made it to disk, the next Load must
// yield a consistent (snapshot, snapshotIndex, log) triple with no lost state.
func TestRewriteCrashWindow_SnapshotWrittenMetaStale(t *testing.T) {
	dir := t.TempDir()
	p := mustPersister(t, dir)
	oldMeta := Meta{CurrentTerm: 1, SnapshotIndex: 0}
	if err := p.SaveMeta(oldMeta); err != nil {
		t.Fatal(err)
	}
	if err := p.AppendLog(entries(1, 10)); err != nil {
		t.Fatal(err)
	}

	// Crash right after the new snapshot (state at index 8) hit the disk, before
	// the log swap and before meta.json learned about it.
	newMeta := Meta{CurrentTerm: 1, SnapshotIndex: 8, SnapshotTerm: 1}
	if err := p.writeSnapshotFileLocked(newMeta, []byte(`{"state":"at-8"}`)); err != nil {
		t.Fatal(err)
	}
	p.Close()

	meta, snap, log, err := mustPersister(t, dir).Load()
	if err != nil {
		t.Fatal(err)
	}
	if string(snap) != `{"state":"at-8"}` {
		t.Fatalf("snapshot payload=%q (header must be stripped on load)", snap)
	}
	if meta.SnapshotIndex != 8 || meta.SnapshotTerm != 1 {
		t.Fatalf("meta snapshot=%d/%d, want 8/1: the snapshot on disk is newer than meta.json, "+
			"restoring it under the old index would re-apply stale entries over newer state",
			meta.SnapshotIndex, meta.SnapshotTerm)
	}
	if len(log) != 2 || log[0].Index != 9 || log[1].Index != 10 {
		t.Fatalf("log tail=%v, want [9 10]", indicesOf(log))
	}
}

// Snapshots written by older builds have no header; they must still load.
func TestLoad_LegacyHeaderlessSnapshot(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "meta.json"),
		[]byte(`{"current_term":2,"voted_for":"","snapshot_index":5,"snapshot_term":1}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "snapshot.bin"), []byte(`{"a":"b"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	meta, snap, _, err := mustPersister(t, dir).Load()
	if err != nil {
		t.Fatal(err)
	}
	if string(snap) != `{"a":"b"}` || meta.SnapshotIndex != 5 {
		t.Fatalf("legacy load: snap=%q meta=%+v", snap, meta)
	}
}

// A meta.json that exists but is unreadable must stop startup. Treating it as
// empty resets the term and the recorded vote, which allows a double vote.
func TestLoad_CorruptMetaIsAnError(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "meta.json"), []byte(`{"current_term":7,"vot`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := mustPersister(t, dir).Load(); err == nil {
		t.Fatal("Load accepted a corrupt meta.json")
	}
}

type loadFailPersister struct{ memPersister }

func (*loadFailPersister) Load() (Meta, []byte, []*raftpb.LogEntry, error) {
	return Meta{}, nil, nil, errors.New("unreadable state")
}

// If durable state cannot be loaded, Raft must not start from blank state.
func TestNewRaft_LoadFailureIsFatal(t *testing.T) {
	called := false
	soloNode(&loadFailPersister{}, func(op string, err error) { called = true })
	if !called {
		t.Fatal("NewRaft continued with empty state after a failed Load")
	}
}

// A crash mid-append leaves a partial last line. Load must discard it AND repair
// the file, otherwise the next append is glued onto the fragment and every entry
// after it is lost on the following restart.
func TestLoad_TornTailIsRepairedBeforeAppend(t *testing.T) {
	dir := t.TempDir()
	good := `{"term":1,"index":1,"op":"set","key":"a","value":"1"}` + "\n" +
		`{"term":1,"index":2,"op":"set","key":"b","value":"2"}` + "\n" +
		`{"term":1,"index":3,"op":"set","key":"c","value":"3"}` + "\n"
	if err := os.WriteFile(filepath.Join(dir, "log.jsonl"), []byte(good+`{"term":1,"ind`), 0o644); err != nil {
		t.Fatal(err)
	}

	p := mustPersister(t, dir)
	_, _, log, err := p.Load()
	if err != nil || len(log) != 3 {
		t.Fatalf("first load: %v entries=%v", err, indicesOf(log))
	}
	if err := p.AppendLog(entries(4, 4)); err != nil {
		t.Fatal(err)
	}
	p.Close()

	_, _, log, err = mustPersister(t, dir).Load()
	if err != nil {
		t.Fatal(err)
	}
	if got := indicesOf(log); len(got) != 4 || got[3] != 4 {
		t.Fatalf("after reload entries=%v, want [1 2 3 4]: append was glued to the torn fragment", got)
	}
}

// Dropping a gap tail in memory is not enough: it must be dropped on disk too,
// or entries appended afterwards sit behind the gap and vanish on next load.
func TestLoad_GapTruncationIsPersisted(t *testing.T) {
	dir := t.TempDir()
	content := `{"term":1,"index":1,"op":"set","key":"a","value":"1"}` + "\n" +
		`{"term":1,"index":2,"op":"set","key":"b","value":"2"}` + "\n" +
		`{"term":1,"index":4,"op":"set","key":"d","value":"4"}` + "\n" // gap at 3
	if err := os.WriteFile(filepath.Join(dir, "log.jsonl"), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	p := mustPersister(t, dir)
	if _, _, log, err := p.Load(); err != nil || len(log) != 2 {
		t.Fatalf("first load: %v %v", err, indicesOf(log))
	}
	if err := p.AppendLog(entries(3, 3)); err != nil {
		t.Fatal(err)
	}
	p.Close()
	_, _, log, err := mustPersister(t, dir).Load()
	if err != nil {
		t.Fatal(err)
	}
	if got := indicesOf(log); len(got) != 3 || got[2] != 3 {
		t.Fatalf("after reload entries=%v, want [1 2 3]", got)
	}
}
