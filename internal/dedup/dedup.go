package dedup

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"time"

	"github.com/K0ngS3ng/CertStreamerPro/internal/store"
)

type Event struct {
	Domain   string
	LogURL   string
	Observed time.Time
	SCTime   time.Time
	RawName  string
}

type Deduper struct {
	store *store.Store
	bloom *Bloom
}

func NewDeduper(store *store.Store, bloom *Bloom) *Deduper {
	return &Deduper{store: store, bloom: bloom}
}

func (d *Deduper) RunSharded(ctx context.Context, in <-chan Event, out chan<- Event, workers int, batchSize int, flushInterval time.Duration, logf func(string, ...any)) {
	if workers < 1 {
		workers = 1
	}
	if batchSize < 1 {
		batchSize = 200
	}
	if flushInterval <= 0 {
		flushInterval = time.Second
	}

	shards := make([]chan workItem, workers)
	for i := 0; i < workers; i++ {
		shards[i] = make(chan workItem, 2048)
		go d.worker(ctx, shards[i], out, batchSize, flushInterval, logf)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-in:
			if !ok {
				return
			}
			sum := sha256.Sum256([]byte(ev.Domain))
			hashHex := hex.EncodeToString(sum[:])
			shard := int(binary.BigEndian.Uint64(sum[:8]) % uint64(workers))
			item := workItem{ev: ev, hash: hashHex}
			select {
			case shards[shard] <- item:
			default:
				if logf != nil {
					logf("dedup shard full, dropping domain: %s", ev.Domain)
				}
			}
		}
	}
}

type workItem struct {
	ev   Event
	hash string
}

func (d *Deduper) worker(ctx context.Context, in <-chan workItem, out chan<- Event, batchSize int, flushInterval time.Duration, logf func(string, ...any)) {
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	var (
		batch []workItem
		seen  = make(map[string]struct{}, batchSize)
	)
	for {
		select {
		case <-ctx.Done():
			d.flush(ctx, batch, seen, out, logf)
			return
		case item, ok := <-in:
			if !ok {
				d.flush(ctx, batch, seen, out, logf)
				return
			}
			if _, ok := seen[item.hash]; ok {
				continue
			}
			exists, err := d.store.GetDomain(item.hash)
			if err != nil {
				if logf != nil {
					logf("dedup read error: %v", err)
				}
				continue
			}
			if exists {
				if d.bloom != nil {
					d.bloom.Add(item.hash)
				}
				continue
			}
			seen[item.hash] = struct{}{}
			batch = append(batch, item)
			if len(batch) >= batchSize {
				batch = d.flush(ctx, batch, seen, out, logf)
			}
		case <-ticker.C:
			batch = d.flush(ctx, batch, seen, out, logf)
		}
	}
}

func (d *Deduper) flush(ctx context.Context, batch []workItem, seen map[string]struct{}, out chan<- Event, logf func(string, ...any)) []workItem {
	if len(batch) == 0 {
		return batch[:0]
	}
	entries := make([]store.DomainEntry, 0, len(batch))
	for _, item := range batch {
		entries = append(entries, store.DomainEntry{Key: item.hash, TS: item.ev.Observed})
	}
	if err := d.putBatchWithRetry(ctx, entries, logf); err != nil {
		if logf != nil {
			logf("dedup batch write error, falling back: %v", err)
		}
		for _, item := range batch {
			if err := d.store.PutDomain(item.hash, item.ev.Observed); err != nil {
				if logf != nil {
					logf("dedup single write error: %v", err)
				}
				continue
			}
			d.emit(out, item, logf)
		}
	} else {
		for _, item := range batch {
			d.emit(out, item, logf)
		}
	}
	for k := range seen {
		delete(seen, k)
	}
	return batch[:0]
}

func (d *Deduper) putBatchWithRetry(ctx context.Context, entries []store.DomainEntry, logf func(string, ...any)) error {
	var err error
	backoff := 50 * time.Millisecond
	for i := 0; i < 3; i++ {
		err = d.store.PutDomainsBatch(entries)
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
	}
	return err
}

func (d *Deduper) emit(out chan<- Event, item workItem, logf func(string, ...any)) {
	if d.bloom != nil {
		d.bloom.Add(item.hash)
	}
	select {
	case out <- item.ev:
	default:
		if logf != nil {
			logf("output channel full, dropping domain: %s", item.ev.Domain)
		}
		if d.store != nil {
			_ = d.store.PutPending(item.hash, store.PendingEvent{
				Domain:   item.ev.Domain,
				LogURL:   item.ev.LogURL,
				Observed: item.ev.Observed,
				SCTime:   item.ev.SCTime,
			})
		}
	}
}
