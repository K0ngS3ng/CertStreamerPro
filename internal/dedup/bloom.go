package dedup

import (
	"crypto/sha256"
	"encoding/binary"
	"sync"
	"time"
)

type Bloom struct {
	bits      uint64
	hashes    uint32
	windows   int
	windowSec int64

	mu        sync.RWMutex
	filters   [][]uint64
	lastIndex int64
}

func NewBloom(bits uint64, hashes uint32, windows int, window time.Duration) *Bloom {
	if windows < 1 {
		windows = 1
	}
	if hashes < 1 {
		hashes = 1
	}
	if bits < 1024 {
		bits = 1024
	}
	words := (bits + 63) / 64
	filters := make([][]uint64, windows)
	for i := 0; i < windows; i++ {
		filters[i] = make([]uint64, words)
	}
	return &Bloom{
		bits:      bits,
		hashes:    hashes,
		windows:   windows,
		windowSec: int64(window.Seconds()),
		filters:   filters,
		lastIndex: -1,
	}
}

func (b *Bloom) Seen(key string) bool {
	b.rotateIfNeeded(time.Now().Unix())
	b.mu.RLock()
	defer b.mu.RUnlock()
	idx := b.currentIndex(time.Now().Unix())
	for _, bit := range b.hashesFor(key) {
		word := bit / 64
		mask := uint64(1) << (bit % 64)
		found := false
		for i := 0; i < b.windows; i++ {
			pos := (idx - int64(i) + int64(b.windows)) % int64(b.windows)
			if b.filters[pos][word]&mask != 0 {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func (b *Bloom) Add(key string) {
	b.rotateIfNeeded(time.Now().Unix())
	b.mu.Lock()
	defer b.mu.Unlock()
	idx := b.currentIndex(time.Now().Unix())
	for _, bit := range b.hashesFor(key) {
		word := bit / 64
		mask := uint64(1) << (bit % 64)
		b.filters[idx][word] |= mask
	}
}

func (b *Bloom) currentIndex(now int64) int64 {
	if b.windowSec <= 0 {
		return 0
	}
	return (now / b.windowSec) % int64(b.windows)
}

func (b *Bloom) rotateIfNeeded(now int64) {
	if b.windowSec <= 0 {
		return
	}
	idx := now / b.windowSec
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.lastIndex == -1 {
		b.lastIndex = idx
		return
	}
	if idx == b.lastIndex {
		return
	}
	for i := b.lastIndex + 1; i <= idx; i++ {
		pos := i % int64(b.windows)
		for j := range b.filters[pos] {
			b.filters[pos][j] = 0
		}
	}
	b.lastIndex = idx
}

func (b *Bloom) hashesFor(key string) []uint64 {
	sum := sha256.Sum256([]byte(key))
	h1 := binary.BigEndian.Uint64(sum[0:8])
	h2 := binary.BigEndian.Uint64(sum[8:16])
	bits := b.bits
	out := make([]uint64, b.hashes)
	for i := uint32(0); i < b.hashes; i++ {
		combined := h1 + uint64(i)*h2
		out[i] = combined % bits
	}
	return out
}
