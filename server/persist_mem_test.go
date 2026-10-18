package main

import (
	"bytes"
	"sync"

	raftpb "github.com/chrisli/raft-kv-store/proto"
)

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

func (p *memPersister) Rewrite(meta Meta, snapshot []byte, tail []*raftpb.LogEntry) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.meta = meta
	if snapshot != nil {
		p.snap = bytes.Clone(snapshot) // keeps an empty snapshot non-nil, unlike append
	}
	p.log = append([]*raftpb.LogEntry(nil), tail...)
	return nil
}

func (p *memPersister) Load() (Meta, []byte, []*raftpb.LogEntry, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.meta, p.snap, append([]*raftpb.LogEntry(nil), p.log...), nil
}
