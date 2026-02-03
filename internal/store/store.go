package store

import "time"

type Store interface {
	GetDomain(key string) (bool, error)
	PutDomain(key string, ts time.Time) error
	PutDomainsBatch(entries []DomainEntry) error
	GetProgress(logID string) (uint64, bool, error)
	PutProgress(logID string, idx uint64) error
	PutPending(key string, ev PendingEvent) error
	DeletePending(key string) error
	IteratePending(limit int, fn func(key string, ev PendingEvent) bool) error
	RunGC(interval time.Duration, stop <-chan struct{})
	Close() error
}
