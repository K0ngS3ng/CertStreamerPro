package store

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"
)

type Store struct {
	db *badger.DB
}

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

func Open(path string, valueLogFileSize int64) (*Store, error) {
	if err := os.MkdirAll(path, 0o755); err != nil {
		return nil, err
	}
	opts := badger.DefaultOptions(path)
	opts.Logger = nil
	opts.ValueLogFileSize = valueLogFileSize
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) GetDomain(key string) (bool, error) {
	k := []byte("d/" + key)
	var found bool
	err := s.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(k)
		if err == nil {
			found = true
			return nil
		}
		if errors.Is(err, badger.ErrKeyNotFound) {
			found = false
			return nil
		}
		return err
	})
	return found, err
}

func (s *Store) PutDomain(key string, ts time.Time) error {
	k := []byte("d/" + key)
	v := make([]byte, 8)
	binary.BigEndian.PutUint64(v, uint64(ts.Unix()))
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(k, v)
	})
}

func (s *Store) PutDomainsBatch(entries []DomainEntry) error {
	if len(entries) == 0 {
		return nil
	}
	wb := s.db.NewWriteBatch()
	defer wb.Cancel()
	for _, e := range entries {
		k := []byte("d/" + e.Key)
		v := make([]byte, 8)
		binary.BigEndian.PutUint64(v, uint64(e.TS.Unix()))
		if err := wb.Set(k, v); err != nil {
			return err
		}
	}
	return wb.Flush()
}

func (s *Store) PutPending(key string, ev PendingEvent) error {
	k := []byte("e/" + key)
	return s.db.Update(func(txn *badger.Txn) error {
		_, err := txn.Get(k)
		if err == nil {
			return nil
		}
		if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		val, err := json.Marshal(ev)
		if err != nil {
			return err
		}
		return txn.Set(k, val)
	})
}

func (s *Store) DeletePending(key string) error {
	k := []byte("e/" + key)
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(k)
	})
}

func (s *Store) IteratePending(limit int, fn func(key string, ev PendingEvent) bool) error {
	if limit <= 0 {
		limit = 1000
	}
	return s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte("e/")
		count := 0
		stop := false
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			if stop {
				break
			}
			item := it.Item()
			key := string(item.KeyCopy(nil))
			err := item.Value(func(val []byte) error {
				var ev PendingEvent
				if err := json.Unmarshal(val, &ev); err != nil {
					return err
				}
				if !fn(strings.TrimPrefix(key, "e/"), ev) {
					stop = true
				}
				return nil
			})
			if err != nil {
				return err
			}
			count++
			if count >= limit {
				break
			}
		}
		return nil
	})
}

func (s *Store) GetProgress(logID string) (uint64, bool, error) {
	k := []byte("p/" + logID)
	var out uint64
	var found bool
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(k)
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				found = false
				return nil
			}
			return err
		}
		found = true
		return item.Value(func(val []byte) error {
			if len(val) != 8 {
				return errors.New("invalid progress value")
			}
			out = binary.BigEndian.Uint64(val)
			return nil
		})
	})
	return out, found, err
}

func (s *Store) PutProgress(logID string, idx uint64) error {
	k := []byte("p/" + logID)
	v := make([]byte, 8)
	binary.BigEndian.PutUint64(v, idx)
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(k, v)
	})
}

func (s *Store) RunGC(interval time.Duration, stop <-chan struct{}) {
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			_ = s.db.RunValueLogGC(0.5)
		}
	}
}

func DataPaths(base string) (string, string) {
	return filepath.Join(base, "badger"), filepath.Join(base, "state")
}
