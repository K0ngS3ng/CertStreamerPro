package store

import (
	"sync"
	"time"
)

type MemoryStore struct {
	mu       sync.RWMutex
	domains  map[string]time.Time
	progress map[string]uint64
	pending  map[string]PendingEvent
}

func OpenMemory() *MemoryStore {
	return &MemoryStore{
		domains:  make(map[string]time.Time),
		progress: make(map[string]uint64),
		pending:  make(map[string]PendingEvent),
	}
}

func (m *MemoryStore) Close() error { return nil }

func (m *MemoryStore) GetDomain(key string) (bool, error) {
	m.mu.RLock()
	_, ok := m.domains[key]
	m.mu.RUnlock()
	return ok, nil
}

func (m *MemoryStore) PutDomain(key string, ts time.Time) error {
	m.mu.Lock()
	m.domains[key] = ts
	m.mu.Unlock()
	return nil
}

func (m *MemoryStore) PutDomainsBatch(entries []DomainEntry) error {
	m.mu.Lock()
	for _, e := range entries {
		m.domains[e.Key] = e.TS
	}
	m.mu.Unlock()
	return nil
}

func (m *MemoryStore) GetProgress(logID string) (uint64, bool, error) {
	m.mu.RLock()
	v, ok := m.progress[logID]
	m.mu.RUnlock()
	return v, ok, nil
}

func (m *MemoryStore) PutProgress(logID string, idx uint64) error {
	m.mu.Lock()
	m.progress[logID] = idx
	m.mu.Unlock()
	return nil
}

func (m *MemoryStore) PutPending(key string, ev PendingEvent) error {
	m.mu.Lock()
	if _, ok := m.pending[key]; !ok {
		m.pending[key] = ev
	}
	m.mu.Unlock()
	return nil
}

func (m *MemoryStore) DeletePending(key string) error {
	m.mu.Lock()
	delete(m.pending, key)
	m.mu.Unlock()
	return nil
}

func (m *MemoryStore) IteratePending(limit int, fn func(key string, ev PendingEvent) bool) error {
	if limit <= 0 {
		limit = 1000
	}
	m.mu.RLock()
	keys := make([]string, 0, len(m.pending))
	for k := range m.pending {
		keys = append(keys, k)
	}
	m.mu.RUnlock()

	count := 0
	for _, k := range keys {
		m.mu.RLock()
		ev, ok := m.pending[k]
		m.mu.RUnlock()
		if !ok {
			continue
		}
		if !fn(k, ev) {
			break
		}
		count++
		if count >= limit {
			break
		}
	}
	return nil
}

func (m *MemoryStore) RunGC(interval time.Duration, stop <-chan struct{}) {
	<-stop
}
