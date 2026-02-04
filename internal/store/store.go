package store

import "time"

type DomainEntry struct {
	Key string
	TS  time.Time
}

type PendingEvent struct {
	Domain   string    `json:"domain"`
	LogURL   string    `json:"log_url"`
	Observed time.Time `json:"observed"`
	SCTime   time.Time `json:"sct_time"`
}

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
