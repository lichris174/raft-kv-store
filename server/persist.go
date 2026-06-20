package main

import (
	"bufio"
	"encoding/json"
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

// filePersister keeps three files in a data dir:
//   - meta.json     : Meta (atomic rewrite)
//   - snapshot.bin  : latest state-machine snapshot
//   - log.jsonl     : one JSON LogEntry per line (append-only; rewritten on
//     compaction/truncation)
//
// Appends are not fsync'd per entry: state survives a process crash (the bytes
// are handed to the OS) but not power loss. Group-fsync would add power-safety
// at a latency cost; documented as a deliberate tradeoff.
type filePersister struct {
	mu      sync.Mutex
	dir     string
	metaF   string
	snapF   string
	logF    string
	logFile *os.File
	logW    *bufio.Writer
}

func newFilePersister(dataDir string) (*filePersister, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, err
	}
	p := &filePersister{
		dir:   dataDir,
		metaF: filepath.Join(dataDir, "meta.json"),
		snapF: filepath.Join(dataDir, "snapshot.bin"),
		logF:  filepath.Join(dataDir, "log.jsonl"),
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

func atomicWrite(path string, data []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
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
	return p.logW.Flush()
}

func (p *filePersister) SaveMeta(meta Meta) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	b, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	return atomicWrite(p.metaF, b)
}

func (p *filePersister) Rewrite(meta Meta, snapshot []byte, log []*raftpb.LogEntry) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if b, err := json.Marshal(meta); err != nil {
		return err
	} else if err := atomicWrite(p.metaF, b); err != nil {
		return err
	}
	if snapshot != nil {
		if err := atomicWrite(p.snapF, snapshot); err != nil {
			return err
		}
	}
	// Rewrite the log file from scratch with the current tail.
	tmp := p.logF + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	for _, e := range log {
		b, _ := json.Marshal(e)
		w.Write(b)
		w.WriteByte('\n')
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
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
		// the compacted tail (Load sanitizes the extra head against the new
		// snapshot index), so reopen it and keep running rather than wedging the
		// persister. Next compaction retries the swap.
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
		_ = json.Unmarshal(b, &meta)
	} else if !os.IsNotExist(err) {
		return meta, nil, nil, err
	}
	var snap []byte
	if b, err := os.ReadFile(p.snapF); err == nil {
		snap = b
	} else if !os.IsNotExist(err) {
		return meta, nil, nil, err
	}
	var raw []*raftpb.LogEntry
	if b, err := os.ReadFile(p.logF); err == nil {
		for _, line := range splitLines(b) {
			if len(line) == 0 {
				continue
			}
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
