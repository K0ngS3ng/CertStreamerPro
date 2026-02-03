package util

import (
	"math"
	"math/rand"
	"time"
)

type Backoff struct {
	Initial time.Duration
	Max     time.Duration
	Factor  float64
	Jitter  float64
	current time.Duration
}

func (b *Backoff) Reset() {
	b.current = 0
}

func (b *Backoff) Next() time.Duration {
	if b.current == 0 {
		b.current = b.Initial
	} else {
		next := time.Duration(float64(b.current) * b.Factor)
		if next > b.Max {
			next = b.Max
		}
		b.current = next
	}
	j := 1.0
	if b.Jitter > 0 {
		j = 1 + (rand.Float64()*2-1)*b.Jitter
	}
	return time.Duration(math.Max(float64(time.Millisecond), float64(b.current)*j))
}
