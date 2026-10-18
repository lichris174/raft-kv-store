package main

import (
	"bufio"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	raftpb "github.com/chrisli/raft-kv-store/proto"
)

// Tests of the file persister: round trips, crash windows between the files a
// rewrite touches, and repair of damaged files. Several are regression tests for
// fixes documented in docs/HARDENING.md.

// openPersister opens a file persister on dir, closed at test cleanup.
func openPersister(t *testing.T, dir string, fsync bool) *filePersister {
	t.Helper()
	p, err := newFilePersister(dir, fsync)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}

func writeFile(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

// entries returns set entries for indexes from..to in term 1.
func entries(from, to uint64) []*raftpb.LogEntry {
	var out []*raftpb.LogEntry
	for i := from; i <= to; i++ {
		out = append(out, &raftpb.LogEntry{Term: 1, Index: i, Op: "set", Key: "k", Value: "v"})
	}
	return out
}

func indicesOf(log []*raftpb.LogEntry) []uint64 {
	out := make([]uint64, 0, len(log))
	for _, e := range log {
		out = append(out, e.Index)
	}
	return out
}

func mustLoad(t *testing.T, p *filePersister) (Meta, []byte, []*raftpb.LogEntry) {
	t.Helper()
	meta, snap, log, err := p.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return meta, snap, log
}

// Every durable write (append, meta, snapshot + log rewrite) goes through an
// explicit disk flush, and the state still reloads intact.
func TestFsyncPersisterRoundTrip(t *testing.T) {
	p := openPersister(t, t.TempDir(), true)
	if err := p.AppendLog(entries(1, 2)); err != nil {
		t.Fatalf("AppendLog: %v", err)
	}
	if err := p.SaveMeta(Meta{CurrentTerm: 3, VotedFor: "n1"}); err != nil {
		t.Fatalf("SaveMeta: %v", err)
	}
	meta, _, log := mustLoad(t, p)
	if meta.CurrentTerm != 3 || meta.VotedFor != "n1" || len(log) != 2 {
		t.Fatalf("round trip: meta=%+v log=%v", meta, indicesOf(log))
	}

	// Compaction: snapshot at index 2, log truncated to the tail.
	tail := []*raftpb.LogEntry{{Term: 2, Index: 3, Op: "set", Key: "c", Value: "3"}}
	if err := p.Rewrite(Meta{CurrentTerm: 3, SnapshotIndex: 2, SnapshotTerm: 1}, []byte(`{"a":"1","b":"2"}`), tail); err != nil {
		t.Fatalf("Rewrite: %v", err)
	}
	meta, snap, log := mustLoad(t, p)
	if meta.SnapshotIndex != 2 || string(snap) != `{"a":"1","b":"2"}` || len(log) != 1 || log[0].Index != 3 {
		t.Fatalf("after rewrite: meta=%+v snap=%q log=%v", meta, snap, indicesOf(log))
	}
}

// Compaction rewrites three files (snapshot, log, meta) and a crash can land
// between any two of them. Whatever subset made it to disk, the next Load must
// yield a consistent (snapshot, snapshotIndex, log) triple with no lost state.
func TestRewriteCrashWindow_SnapshotWrittenMetaStale(t *testing.T) {
	dir := t.TempDir()
	p := openPersister(t, dir, false)
	if err := p.SaveMeta(Meta{CurrentTerm: 1}); err != nil {
		t.Fatal(err)
	}
	if err := p.AppendLog(entries(1, 10)); err != nil {
		t.Fatal(err)
	}
	// Crash right after the new snapshot (state at index 8) hit the disk, before
	// the log swap and before meta.json learned about it. This is the first file
	// Rewrite writes.
	if err := atomicWrite(p.snapF, encodeSnapshot(8, 1, []byte(`{"state":"at-8"}`)), false); err != nil {
		t.Fatal(err)
	}
	p.Close()

	meta, snap, log := mustLoad(t, openPersister(t, dir, false))
	if string(snap) != `{"state":"at-8"}` {
		t.Fatalf("snapshot payload=%q (header must be stripped on load)", snap)
	}
	if meta.SnapshotIndex != 8 || meta.SnapshotTerm != 1 {
		t.Fatalf("meta snapshot=%d/%d, want 8/1: the snapshot on disk is newer than meta.json, "+
			"restoring it under the old index would re-apply stale entries over newer state",
			meta.SnapshotIndex, meta.SnapshotTerm)
	}
	if got := indicesOf(log); len(got) != 2 || got[0] != 9 || got[1] != 10 {
		t.Fatalf("log tail=%v, want [9 10]", got)
	}
}

// Snapshots written by older builds have no header; they must still load.
func TestLoad_LegacyHeaderlessSnapshot(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "meta.json", `{"current_term":2,"voted_for":"","snapshot_index":5,"snapshot_term":1}`)
	writeFile(t, dir, "snapshot.bin", `{"a":"b"}`)
	meta, snap, _ := mustLoad(t, openPersister(t, dir, false))
	if string(snap) != `{"a":"b"}` || meta.SnapshotIndex != 5 {
		t.Fatalf("legacy load: snap=%q meta=%+v", snap, meta)
	}
}

// A meta.json that exists but is unreadable must stop startup. Treating it as
// empty resets the term and the recorded vote, which allows a double vote.
func TestLoad_CorruptMetaIsAnError(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "meta.json", `{"current_term":7,"vot`)
	if _, _, _, err := openPersister(t, dir, false).Load(); err == nil {
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

// A duplicate entry at the snapshot boundary plus a later gap (the corruption
// an old chaos run produced): Load returns a contiguous tail from
// snapshotIndex+1 so the in-memory offset arithmetic stays valid.
func TestLoad_SanitizesDuplicateBoundaryAndGap(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "meta.json", `{"current_term":4,"voted_for":"","snapshot_index":25,"snapshot_term":1}`)
	writeFile(t, dir, "log.jsonl", ""+
		`{"term":1,"index":26,"op":"set","key":"a","value":"1"}`+"\n"+
		`{"term":1,"index":26,"op":"set","key":"a","value":"1"}`+"\n"+ // duplicate boundary
		`{"term":1,"index":27,"op":"set","key":"b","value":"2"}`+"\n"+
		`{"term":4,"index":29,"op":"set","key":"d","value":"4"}`+"\n"+ // gap: index 28 missing
		`{"term":4,"index":30,"op":"set","key":"e","value":"5"}`+"\n")
	meta, _, log := mustLoad(t, openPersister(t, dir, false))
	if got := indicesOf(log); meta.SnapshotIndex != 25 || len(got) != 2 || got[0] != 26 || got[1] != 27 {
		t.Fatalf("snapshotIndex=%d tail=%v, want 25 and [26 27]", meta.SnapshotIndex, got)
	}
}

// A crash mid-append leaves a partial last line. Load must discard it AND repair
// the file, otherwise the next append is glued onto the fragment and every entry
// after it is lost on the following restart.
func TestLoad_TornTailIsRepairedBeforeAppend(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "log.jsonl", ""+
		`{"term":1,"index":1,"op":"set","key":"a","value":"1"}`+"\n"+
		`{"term":1,"index":2,"op":"set","key":"b","value":"2"}`+"\n"+
		`{"term":1,"index":3,"op":"set","key":"c","value":"3"}`+"\n"+
		`{"term":1,"ind`)
	p := openPersister(t, dir, false)
	if _, _, log := mustLoad(t, p); len(log) != 3 {
		t.Fatalf("first load: %v", indicesOf(log))
	}
	if err := p.AppendLog(entries(4, 4)); err != nil {
		t.Fatal(err)
	}
	p.Close()
	if _, _, log := mustLoad(t, openPersister(t, dir, false)); len(log) != 4 || log[3].Index != 4 {
		t.Fatalf("after reload entries=%v, want [1 2 3 4]: append was glued to the torn fragment", indicesOf(log))
	}
}

// Dropping a gap tail in memory is not enough: it must be dropped on disk too,
// or entries appended afterwards sit behind the gap and vanish on next load.
func TestLoad_GapTruncationIsPersisted(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "log.jsonl", ""+
		`{"term":1,"index":1,"op":"set","key":"a","value":"1"}`+"\n"+
		`{"term":1,"index":2,"op":"set","key":"b","value":"2"}`+"\n"+
		`{"term":1,"index":4,"op":"set","key":"d","value":"4"}`+"\n") // gap at 3
	p := openPersister(t, dir, false)
	if _, _, log := mustLoad(t, p); len(log) != 2 {
		t.Fatalf("first load: %v", indicesOf(log))
	}
	if err := p.AppendLog(entries(3, 3)); err != nil {
		t.Fatal(err)
	}
	p.Close()
	if _, _, log := mustLoad(t, openPersister(t, dir, false)); len(log) != 3 || log[2].Index != 3 {
		t.Fatalf("after reload entries=%v, want [1 2 3]", indicesOf(log))
	}
}

// If a failed append cannot be undone (here the file turned read-only, so the
// truncate fails too), the persister must refuse further appends rather than
// write behind the fragment. Once the file can be truncated again, the next
// append repairs it first and goes through.
func TestAppendLog_UnrecoverableFailureRefusesAppendsUntilRepaired(t *testing.T) {
	dir := t.TempDir()
	p := openPersister(t, dir, false)
	if err := p.AppendLog(entries(1, 2)); err != nil {
		t.Fatal(err)
	}
	p.logW = bufio.NewWriterSize(&failAfter{w: p.logFile, n: 20}, 16)
	if err := os.Chmod(p.logF, 0o444); err != nil {
		t.Fatal(err)
	}
	if err := p.AppendLog(entries(3, 3)); err == nil {
		t.Fatal("partial write reported success")
	}
	if err := p.AppendLog(entries(3, 3)); err == nil {
		t.Fatal("append behind an un-truncated fragment was accepted")
	}
	if err := os.Chmod(p.logF, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := p.AppendLog(entries(3, 3)); err != nil {
		t.Fatalf("append once the file could be repaired: %v", err)
	}
	p.Close()
	if _, _, log := mustLoad(t, openPersister(t, dir, false)); len(log) != 3 {
		t.Fatalf("entries=%v, want [1 2 3]: the fragment was not cut off", indicesOf(log))
	}
}

// failAfter passes the first n bytes to w, then fails: a disk that fills up
// partway through a write.
type failAfter struct {
	w io.Writer
	n int
}

func (f *failAfter) Write(b []byte) (int, error) {
	if len(b) <= f.n {
		f.n -= len(b)
		return f.w.Write(b)
	}
	k, _ := f.w.Write(b[:f.n])
	f.n = 0
	return k, errDisk
}

// An append that fails partway leaves a fragment in the file. It must be cut
// off, so that the retry starts on a clean line and nothing is lost on reload.
func TestAppendLog_PartialWriteIsUndone(t *testing.T) {
	dir := t.TempDir()
	p := openPersister(t, dir, false)
	if err := p.AppendLog(entries(1, 2)); err != nil {
		t.Fatal(err)
	}
	p.logW = bufio.NewWriterSize(&failAfter{w: p.logFile, n: 20}, 16) // 20 bytes reach the file, then the disk is "full"
	if err := p.AppendLog(entries(3, 4)); err == nil {
		t.Fatal("partial write reported success")
	}
	if err := p.AppendLog(entries(3, 4)); err != nil {
		t.Fatalf("retry: %v", err)
	}
	p.Close()
	if _, _, log := mustLoad(t, openPersister(t, dir, false)); len(log) != 4 {
		t.Fatalf("after reload entries=%v, want [1 2 3 4]", indicesOf(log))
	}
}
