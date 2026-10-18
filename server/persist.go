package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
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
// (compaction, snapshot install, log truncation). A nil snapshot passed to
// Rewrite keeps the snapshot already stored.
type Persister interface {
	AppendLog(entries []*raftpb.LogEntry) error
	SaveMeta(meta Meta) error
	Rewrite(meta Meta, snapshot []byte, tail []*raftpb.LogEntry) error
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
type filePersister struct {
	mu      sync.Mutex
	metaF   string
	snapF   string
	logF    string
	logFile *os.File      // append handle on log.jsonl; nil after a failed swap
	logW    *bufio.Writer // buffers one AppendLog call, flushed before it returns
	logSize int64         // bytes of log.jsonl known to be whole entries
	broken  error         // set when a failed append could not be undone
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
		metaF: filepath.Join(dataDir, "meta.json"),
		snapF: filepath.Join(dataDir, "snapshot.bin"),
		logF:  filepath.Join(dataDir, "log.jsonl"),
		fsync: fsync,
	}
	if err := p.reopenAppendLocked(); err != nil {
		return nil, err
	}
	return p, nil
}

// Close flushes and releases the append handle. Tests use it to free the files
// (Windows will not delete open files); the node process just exits.
func (p *filePersister) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closeAppendLocked()
}

func (p *filePersister) AppendLog(entries []*raftpb.LogEntry) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.broken != nil {
		// A fragment from a failed append is still in the file. Try to cut it off
		// again (the cause, such as a sync client's lock, may have passed).
		if err := os.Truncate(p.logF, p.logSize); err != nil {
			return fmt.Errorf("%w (retry: %v)", p.broken, err)
		}
		p.broken = nil
	}
	if p.logFile == nil { // a previous swap failed and could not reopen
		if err := p.reopenAppendLocked(); err != nil {
			return err
		}
	}
	n, err := writeEntries(p.logW, entries)
	if err == nil && p.fsync {
		err = p.logFile.Sync() // without this the entry is only in the OS page cache
	}
	if err != nil {
		// Part of the batch may be on disk. Cut the file back to the last whole
		// entry, or a retry would add the same indexes again behind the fragment.
		// Truncate by path: Windows refuses to truncate through an append-only handle.
		p.logW.Reset(p.logFile) // drop what is still buffered
		_ = p.closeAppendLocked()
		if tErr := os.Truncate(p.logF, p.logSize); tErr != nil {
			p.broken = fmt.Errorf("log append failed and could not be undone: %w", errors.Join(err, tErr))
			return p.broken
		}
		_ = p.reopenAppendLocked() // if this fails, the next AppendLog retries it
		return err
	}
	p.logSize += n // counted, not re-read: a failed Stat must never leave a stale size to truncate to
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

// Rewrite persists compaction, snapshot install or a log truncation.
//
// Order matters for crash safety: snapshot, then log, then meta. Each step
// leaves a state Load can reconcile:
//   - snapshot new, log old: the old log is a superset of the new tail; Load
//     adopts the snapshot's own index and drops the entries it covers.
//   - snapshot new, log new, meta old: same, the snapshot header wins.
//
// Writing meta first could record a snapshot index that the snapshot file on
// disk did not yet reflect, which silently loses state.
func (p *filePersister) Rewrite(meta Meta, snapshot []byte, tail []*raftpb.LogEntry) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	metaJSON, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	if snapshot != nil {
		if err := atomicWrite(p.snapF, encodeSnapshot(meta.SnapshotIndex, meta.SnapshotTerm, snapshot), p.fsync); err != nil {
			return err
		}
	}
	if err := p.swapLogLocked(tail); err != nil {
		return err
	}
	return atomicWrite(p.metaF, metaJSON, p.fsync)
}

// swapLogLocked replaces log.jsonl with exactly the given entries and reopens
// the append handle.
func (p *filePersister) swapLogLocked(entries []*raftpb.LogEntry) error {
	tmp := p.logF + ".tmp"
	if err := writeFileSynced(tmp, p.fsync, func(w *bufio.Writer) error {
		_, err := writeEntries(w, entries)
		return err
	}); err != nil {
		return err
	}
	// Windows refuses to replace a file that still has an open handle.
	_ = p.closeAppendLocked()
	if err := renameWithRetry(tmp, p.logF); err != nil {
		// The original log.jsonl is intact and is a superset of the new tail (Load
		// reconciles the extra head against the snapshot's index). Keep using it;
		// the next compaction retries the swap.
		_ = os.Remove(tmp)
		if rErr := p.reopenAppendLocked(); rErr != nil {
			return errors.Join(err, rErr)
		}
		return err
	}
	p.broken = nil // the file now holds exactly the entries we wrote
	return p.reopenAppendLocked()
}

// reopenAppendLocked opens log.jsonl for appending and records its size.
func (p *filePersister) reopenAppendLocked() error {
	f, err := os.OpenFile(p.logF, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	p.logFile, p.logW = f, bufio.NewWriter(f)
	if p.broken == nil {
		// While broken, the file still ends in a fragment; logSize must keep
		// pointing at the last whole entry so the repair cuts the fragment off.
		p.logSize = st.Size()
	}
	return nil
}

func (p *filePersister) closeAppendLocked() error {
	if p.logFile == nil {
		return nil
	}
	_ = p.logW.Flush()
	err := p.logFile.Close()
	p.logFile, p.logW = nil, nil
	return err
}

// writeEntries writes one JSON entry per line, flushes, and returns the bytes written.
func writeEntries(w *bufio.Writer, entries []*raftpb.LogEntry) (int64, error) {
	var n int64
	for _, e := range entries {
		b, err := json.Marshal(e)
		if err != nil {
			return 0, err
		}
		w.Write(b)
		w.WriteByte('\n')
		n += int64(len(b)) + 1
	}
	return n, w.Flush() // reports any earlier write error: bufio errors are sticky
}

// writeFileSynced creates path, fills it via fill, and (with fsync) forces it to
// disk before closing, so a later rename cannot expose unwritten data.
func writeFileSynced(path string, fsync bool, fill func(*bufio.Writer) error) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	err = fill(bufio.NewWriter(f))
	if err == nil && fsync {
		err = f.Sync()
	}
	return errors.Join(err, f.Close())
}

// atomicWrite replaces path via a temp file + rename.
func atomicWrite(path string, data []byte, fsync bool) error {
	tmp := path + ".tmp"
	if err := writeFileSynced(tmp, fsync, func(w *bufio.Writer) error {
		w.Write(data)
		return w.Flush()
	}); err != nil {
		return err
	}
	return renameWithRetry(tmp, path)
}

// renameWithRetry retries os.Rename to absorb transient file locks. On Windows a
// sync client (OneDrive) or virus scanner can hold the file being replaced, and
// MoveFileEx then fails with "Access is denied". Benchmark runs inside a OneDrive
// folder hit this on meta.json often enough to crash a node, so the window is
// generous (about 1.2s in total) while the common case still costs one call.
func renameWithRetry(oldPath, newPath string) error {
	var err error
	for attempt := 1; attempt <= 15; attempt++ {
		if err = os.Rename(oldPath, newPath); err == nil {
			return nil
		}
		time.Sleep(time.Duration(attempt) * 10 * time.Millisecond)
	}
	return err
}

// Load reads the persisted state back and repairs the log file if needed.
func (p *filePersister) Load() (Meta, []byte, []*raftpb.LogEntry, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	meta, err := p.loadMeta()
	if err != nil {
		return Meta{}, nil, nil, err
	}
	snap, err := p.loadSnapshot(&meta)
	if err != nil {
		return Meta{}, nil, nil, err
	}
	raw, lines, endsClean, err := p.readLogFile()
	if err != nil {
		return Meta{}, nil, nil, err
	}
	tail := contiguousTail(raw, meta.SnapshotIndex)
	// Repair the file so it matches what we just accepted. Otherwise a torn last
	// line (crash mid-append) or a dropped post-gap tail stays on disk, the next
	// append lands behind it, and those later entries are discarded again on the
	// following restart even though they were acknowledged.
	if len(tail) != lines || !endsClean {
		if err := p.swapLogLocked(tail); err != nil {
			return Meta{}, nil, nil, fmt.Errorf("repair %s: %w", p.logF, err)
		}
	}
	return meta, snap, tail, nil
}

// loadMeta reads meta.json. A file that exists but cannot be parsed is an
// error, not an empty state: silently resetting the term and vote would let this
// node vote twice in a term it has already voted in.
func (p *filePersister) loadMeta() (Meta, error) {
	var meta Meta
	b, err := os.ReadFile(p.metaF)
	if os.IsNotExist(err) {
		return meta, nil
	}
	if err != nil {
		return meta, err
	}
	if err := json.Unmarshal(b, &meta); err != nil {
		return meta, fmt.Errorf("corrupt %s: %w", p.metaF, err)
	}
	return meta, nil
}

// loadSnapshot reads snapshot.bin. Its header index wins over meta.json when
// newer (a crash after the snapshot was written but before meta.json was).
func (p *filePersister) loadSnapshot(meta *Meta) ([]byte, error) {
	b, err := os.ReadFile(p.snapF)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	index, term, payload, ok := decodeSnapshot(b)
	switch {
	case !ok: // legacy headerless file: trust meta.json
	case index > meta.SnapshotIndex:
		meta.SnapshotIndex, meta.SnapshotTerm = index, term
	case index < meta.SnapshotIndex:
		return nil, fmt.Errorf("%s (index %d) is older than meta.json (index %d)", p.snapF, index, meta.SnapshotIndex)
	}
	return payload, nil
}

// readLogFile parses log.jsonl, skipping unreadable lines. It also reports how
// many non-blank lines there were and whether the file ends with a newline, so
// Load can tell whether the file needs repair.
func (p *filePersister) readLogFile() (entries []*raftpb.LogEntry, lines int, endsClean bool, err error) {
	b, err := os.ReadFile(p.logF)
	if os.IsNotExist(err) {
		return nil, 0, true, nil
	}
	if err != nil {
		return nil, 0, false, err
	}
	for _, line := range bytes.Split(b, []byte{'\n'}) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		lines++
		var e raftpb.LogEntry
		if json.Unmarshal(line, &e) == nil {
			entries = append(entries, &e)
		}
	}
	return entries, lines, len(b) == 0 || b[len(b)-1] == '\n', nil
}

// contiguousTail keeps the entries that continue the snapshot: starting at
// snapshotIndex+1 and increasing by one. The in-memory offset arithmetic depends
// on that. Duplicates are dropped and the first gap ends the tail; anything lost
// that way is refetched from the leader.
func contiguousTail(raw []*raftpb.LogEntry, snapshotIndex uint64) []*raftpb.LogEntry {
	var tail []*raftpb.LogEntry
	expect := snapshotIndex + 1
	for _, e := range raw {
		if e.Index < expect {
			continue // duplicate, or already covered by the snapshot
		}
		if e.Index > expect {
			break // gap: the rest of the file is unreliable
		}
		tail = append(tail, e)
		expect++
	}
	return tail
}
