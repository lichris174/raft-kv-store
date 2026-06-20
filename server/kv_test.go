package main

import (
	"sync"
	"testing"
)

func TestKVStore_SetGet(t *testing.T) {
	s := NewKVStore()
	if _, ok := s.Get("missing"); ok {
		t.Fatal("expected missing key to be absent")
	}
	s.Set("foo", "bar")
	v, ok := s.Get("foo")
	if !ok || v != "bar" {
		t.Fatalf("got (%q,%v), want (bar,true)", v, ok)
	}
	s.Set("foo", "baz") // overwrite
	if v, _ := s.Get("foo"); v != "baz" {
		t.Fatalf("overwrite: got %q, want baz", v)
	}
}

func TestKVStore_Delete(t *testing.T) {
	s := NewKVStore()
	s.Set("k", "v")
	if !s.Delete("k") {
		t.Fatal("Delete should report existed=true")
	}
	if s.Delete("k") {
		t.Fatal("Delete of absent key should report existed=false")
	}
	if _, ok := s.Get("k"); ok {
		t.Fatal("key should be gone after delete")
	}
}

func TestKVStore_Len(t *testing.T) {
	s := NewKVStore()
	if s.Len() != 0 {
		t.Fatalf("empty Len=%d", s.Len())
	}
	s.Set("a", "1")
	s.Set("b", "2")
	if s.Len() != 2 {
		t.Fatalf("Len=%d, want 2", s.Len())
	}
}

// TestKVStore_Concurrent ensures the RWMutex protects against races
// (run with -race). 100 goroutines writing + reading must not panic/corrupt.
func TestKVStore_Concurrent(t *testing.T) {
	s := NewKVStore()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			key := "k"
			s.Set(key, "v")
			_, _ = s.Get(key)
			_ = s.Len()
		}(i)
	}
	wg.Wait()
}
