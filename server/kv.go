package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sync"
)

// KVStore is a thread-safe in-memory key-value store. It is the Raft state
// machine that committed log entries are applied to.
type KVStore struct {
	mu   sync.RWMutex
	data map[string]string
}

func NewKVStore() *KVStore {
	return &KVStore{data: make(map[string]string)}
}

func (s *KVStore) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

func (s *KVStore) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

// Delete removes key and reports whether it existed.
func (s *KVStore) Delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, existed := s.data[key]
	delete(s.data, key)
	return existed
}

func (s *KVStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

// Snapshot serializes the full store for log compaction. json.Marshal sorts map
// keys, so equal contents always give equal bytes.
func (s *KVStore) Snapshot() []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, _ := json.Marshal(s.data)
	return b
}

// Digest is a short hash of the store's contents, equal on two nodes exactly
// when they hold the same data.
func (s *KVStore) Digest() string {
	sum := sha256.Sum256(s.Snapshot())
	return hex.EncodeToString(sum[:8])
}

// Restore replaces the store contents from a snapshot blob.
func (s *KVStore) Restore(data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := map[string]string{}
	if len(data) > 0 {
		_ = json.Unmarshal(data, &m)
	}
	s.data = m
}

// applyOp applies a committed operation to the state machine. Other ops (the
// leader's per-term "noop") change nothing.
func applyOp(store *KVStore, op, key, value string) {
	switch op {
	case "set":
		store.Set(key, value)
	case "del":
		store.Delete(key)
	}
}
