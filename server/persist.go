package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	raftpb "github.com/chrisli/raft-kv-store/proto"
)

// Meta is the small, durable non-log Raft state.
type Meta struct {
	CurrentTerm   uint64 `json:"current_term"`
	VotedFor      string `json:"voted_for"`
	SnapshotIndex uint64 `json:"snapshot_index"`
	SnapshotTerm  uint64 `json:"snapshot_term"`
}

// Persister stores durable Raft state. The hot path is AppendLog (O(new
// entries)); SaveMeta is small and infrequent (elections); Rewrite is rare
// (compaction, snapshot install, log truncation).
type Persister interface {
	AppendLog(entries []*raftpb.LogEntry) error
	SaveMeta(meta Meta) error
	Rewrite(meta Meta, snapshot []byte, log []*raftpb.LogEntry) error
	Load() (Meta, []byte, []*raftpb.LogEntry, error)
}

// snapshot.bin is self-describing: a magic string, the snapshot's last included
// index and term, then the state-machine payload. Compaction rewrites three
// files (snapshot, log, meta) and a crash can land between any two of them. If
// the snapshot carried no index of its own, a new snapshot paired with a stale
// meta.json would be restored under the old index and entries already folded
// into it would be re-applied over newer state. Files from older builds have no
// header and are read as before, with the index taken from meta.json.
const snapMagic = "RKVSNAP1"
const snapHeaderLen = len(snapMagic) + 16

func encodeSnapshot(index, term uint64, data []byte) []byte {
	b := make([]byte, 0, snapHeaderLen+len(data))
	b = append(b, snapMagic...)
	b = binary.BigEndian.AppendUint64(b, index)
	b = binary.BigEndian.AppendUint64(b, term)
	return append(b, data...)
}

// decodeSnapshot reports ok=false for a headerless (legacy) file.
func decodeSnapshot(b []byte) (index, term uint64, data []byte, ok bool) {
	if len(b) < snapHeaderLen || string(b[:len(snapMagic)]) != snapMagic {
		return 0, 0, b, false
	}
	off := len(snapMagic)
	return binary.BigEndian.Uint64(b[off:]), binary.BigEndian.Uint64(b[off+8:]), b[snapHeaderLen:], true
}

// filePersister keeps three files in a data dir:
//   - meta.json     : Meta (atomic rewrite)
//   - snapshot.bin  : latest state-machine snapshot
//   - log.jsonl     : one JSON LogEntry per line (append-only; rewritten on
//     compaction/truncation)
//
// By default appends are not fsync'd per entry: state survives a process crash
// (the bytes are handed to the OS) but not power loss. Set fsync to trade write
// latency for power-loss durability.
type filePersister struct {
	mu      sync.Mutex
	dir     string
	metaF   string
	snapF   string
	logF    string
	logFile *os.File
	logW    *bufio.Writer
	// fsync forces each durable write through to the physical disk before
	// returning. Off by default: writes then reach the OS page cache only, which
	// survives a process crash but not sudden power loss. See the --fsync flag.
	fsync bool
}

func newFilePersister(dataDir string, fsync bool) (*filePersister, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, err
	}
	p := &filePersister{
		dir:   dataDir,
		metaF: filepath.Join(dataDir, "meta.json"),
		snapF: filepath.Join(dataDir, "snapshot.bin"),
		logF:  filepath.Join(dataDir, "log.jsonl"),
		fsync: fsync,
	}
	f, err := os.OpenFile(p.logF, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	p.logFile = f
	p.logW = bufio.NewWriter(f)
	return p, nil
}

// Close flushes and releases the append handle. Safe to call once at shutdown.
func (p *filePersister) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.logW != nil {
		_ = p.logW.Flush()
	}
	if p.logFile != nil {
		err := p.logFile.Close()
		p.logFile = nil
		return err
	}
	return nil
}

// atomicWrite replaces path via a temp file + rename. When fsync is set the
// temp file is flushed to disk before the rename, so a power loss cannot leave
// the renamed file pointing at unwritten data.
func atomicWrite(path string, data []byte, fsync bool) error {
	tmp := path + ".tmp"
	if fsync {
		f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
		if err != nil {
			return err
		}
		if _, err := f.Write(data); err != nil {
			f.Close()
			return err
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
	} else if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (p *filePersister) AppendLog(entries []*raftpb.LogEntry) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	// If a prior Rewrite failed to swap the log file it should have reopened an
	// append handle already; recover one here so a nil writer never reaches Write.
	if p.logW == nil {
		if err := p.reopenAppendLocked(); err != nil {
			return err
		}
	}
	for _, e := range entries {
		b, err := json.Marshal(e)
		if err != nil {
			return err
		}
		if _, err := p.logW.Write(b); err != nil {
			return err
		}
		if err := p.logW.WriteByte('\n'); err != nil {
			return err
		}
	}
	if err := p.logW.Flush(); err != nil {
		return err
	}
	// The commit path: without this the entry is only in the OS page cache.
	if p.fsync {
		return p.logFile.Sync()
	}
	return nil
}

func (p *filePersister) SaveMeta(meta Meta) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	b, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	return atomicWrite(p.metaF, b, p.fsync)
}

// writeSnapshotFileLocked atomically replaces snapshot.bin with a self-describing
// snapshot for meta.SnapshotIndex/SnapshotTerm.
func (p *filePersister) writeSnapshotFileLocked(meta Meta, snap []byte) error {
	return atomicWrite(p.snapF, encodeSnapshot(meta.SnapshotIndex, meta.SnapshotTerm, snap), p.fsync)
}

// Rewrite persists compaction, snapshot install or a log truncation.
//
// Order matters for crash safety: snapshot, then log, then meta. Each step
// leaves a state Load can reconcile:
//   - snapshot new, log old: the old log is a superset of the new tail; Load
//     adopts the snapshot's own index and drops the entries it covers.
//   - snapshot new, log new, meta old: same, the snapshot header wins.
//
// Writing meta first (the old order) could record a snapshot index that the
// snapshot file on disk did not yet reflect, which silently loses state.
func (p *filePersister) Rewrite(meta Meta, snapshot []byte, log []*raftpb.LogEntry) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	metaJSON, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	if snapshot != nil {
		if err := p.writeSnapshotFileLocked(meta, snapshot); err != nil {
			return err
		}
	}
	if err := p.swapLogLocked(log); err != nil {
		return err
	}
	return atomicWrite(p.metaF, metaJSON, p.fsync)
}

// swapLogLocked replaces log.jsonl with exactly the given entries and reopens
// the append handle. Caller holds p.mu.
func (p *filePersister) swapLogLocked(log []*raftpb.LogEntry) error {
	tmp := p.logF + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	for _, e := range log {
		b, err := json.Marshal(e)
		if err != nil {
			f.Close()
			return err
		}
		w.Write(b)
		w.WriteByte('\n')
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	if p.fsync {
		if err := f.Sync(); err != nil {
			f.Close()
			return err
		}
	}
	f.Close()
	// Close the current append handle so the rename can replace the file;
	// Windows refuses to replace a file that still has an open handle. Every
	// failure path below has to reopen a handle before returning, otherwise the
	// next AppendLog hits a nil writer.
	if p.logW != nil {
		_ = p.logW.Flush()
	}
	if p.logFile != nil {
		_ = p.logFile.Close()
		p.logFile = nil
	}
	p.logW = nil
	// Swap in the new log. Retry the rename: on Windows a sync client (OneDrive)
	// or AV scanner can briefly hold the target so MoveFileEx fails with "Access
	// is denied"; the lock clears within a few millis.
	if err := renameWithRetry(tmp, p.logF); err != nil {
		// Could not swap. The original log.jsonl is intact and is a superset of
		// the new tail (Load reconciles the extra head against the snapshot's
		// index), so reopen it and keep running rather than wedging the
		// persister. The next compaction retries the swap.
		_ = os.Remove(tmp)
		if rErr := p.reopenAppendLocked(); rErr != nil {
			return rErr
		}
		return err
	}
	return p.reopenAppendLocked()
}

// reopenAppendLocked (re)opens log.jsonl in append mode and installs a fresh
// buffered writer. Caller holds p.mu.
func (p *filePersister) reopenAppendLocked() error {
	nf, err := os.OpenFile(p.logF, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	p.logFile = nf
	p.logW = bufio.NewWriter(nf)
	return nil
}

// renameWithRetry retries os.Rename to absorb transient file locks (on Windows,
// OneDrive/Defender can briefly hold a file being replaced).
func renameWithRetry(oldPath, newPath string) error {
	var err error
	for attempt := 0; attempt < 10; attempt++ {
		if err = os.Rename(oldPath, newPath); err == nil {
			return nil
		}
		time.Sleep(time.Duration(attempt+1) * 5 * time.Millisecond)
	}
	return err
}

func (p *filePersister) Load() (Meta, []byte, []*raftpb.LogEntry, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var meta Meta
	if b, err := os.ReadFile(p.metaF); err == nil {
		// A meta.json that exists but cannot be parsed is an error, not an empty
		// state: silently resetting the term and votedFor would let this node vote
		// twice in a term it has already voted in.
		if err := json.Unmarshal(b, &meta); err != nil {
			return Meta{}, nil, nil, fmt.Errorf("corrupt %s: %w", p.metaF, err)
		}
	} else if !os.IsNotExist(err) {
		return meta, nil, nil, err
	}
	var snap []byte
	if b, err := os.ReadFile(p.snapF); err == nil {
		hIndex, hTerm, payload, ok := decodeSnapshot(b)
		snap = payload
		if ok {
			switch {
			case hIndex > meta.SnapshotIndex:
				// Crash after the snapshot was written but before meta.json was:
				// the snapshot is the newer truth.
				meta.SnapshotIndex, meta.SnapshotTerm = hIndex, hTerm
			case hIndex < meta.SnapshotIndex:
				return Meta{}, nil, nil, fmt.Errorf("%s (index %d) is older than meta.json (index %d)", p.snapF, hIndex, meta.SnapshotIndex)
			}
		}
	} else if !os.IsNotExist(err) {
		return meta, nil, nil, err
	}
	var raw []*raftpb.LogEntry
	nonEmpty := 0
	endsClean := true
	if b, err := os.ReadFile(p.logF); err == nil {
		endsClean = len(b) == 0 || b[len(b)-1] == '\n'
		for _, line := range splitLines(b) {
			if len(bytes.TrimSpace(line)) == 0 {
				continue
			}
			nonEmpty++
			var e raftpb.LogEntry
			if json.Unmarshal(line, &e) == nil {
				raw = append(raw, &e)
			}
		}
	} else if !os.IsNotExist(err) {
		return meta, nil, nil, err
	}
	// Sanitize: the in-memory offset math (pos = index - snapshotIndex - 1) requires
	// a contiguous log starting at snapshotIndex+1, strictly increasing by one. Drop
	// duplicates and stop at the first gap so a partially-written or previously
	// corrupted file cannot desync the indexing; any dropped tail entries are
	// refetched from the leader on rejoin.
	var log []*raftpb.LogEntry
	expect := meta.SnapshotIndex + 1
	for _, e := range raw {
		if e.Index < expect {
			continue // duplicate / already covered by snapshot
		}
		if e.Index > expect {
			break // gap: rest of the file is unreliable
		}
		log = append(log, e)
		expect++
	}
	// Repair the file so it matches what we just accepted. Otherwise a torn last
	// line (crash mid-append) or a dropped post-gap tail stays on disk, the next
	// append lands behind it, and those later entries are discarded again on the
	// following restart even though they were acknowledged.
	if len(log) != nonEmpty || !endsClean {
		if err := p.swapLogLocked(log); err != nil {
			return Meta{}, nil, nil, fmt.Errorf("repair %s: %w", p.logF, err)
		}
	}
	return meta, snap, log, nil
}

func splitLines(b []byte) [][]byte {
	var out [][]byte
	start := 0
	for i, c := range b {
		if c == '\n' {
			out = append(out, b[start:i])
			start = i + 1
		}
	}
	if start < len(b) {
		out = append(out, b[start:])
	}
	return out
}

// memPersister is an in-memory Persister for tests and restart simulation.
type memPersister struct {
	mu   sync.Mutex
	meta Meta
	snap []byte
	log  []*raftpb.LogEntry
}

func (p *memPersister) AppendLog(entries []*raftpb.LogEntry) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.log = append(p.log, entries...)
	return nil
}

func (p *memPersister) SaveMeta(meta Meta) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.meta = meta
	return nil
}

func (p *memPersister) Rewrite(meta Meta, snapshot []byte, log []*raftpb.LogEntry) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.meta = meta
	if snapshot != nil {
		p.snap = append([]byte(nil), snapshot...)
	}
	p.log = append([]*raftpb.LogEntry(nil), log...)
	return nil
}

func (p *memPersister) Load() (Meta, []byte, []*raftpb.LogEntry, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.meta, p.snap, append([]*raftpb.LogEntry(nil), p.log...), nil
}
