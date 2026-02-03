package util

import (
	"sync"

	"golang.org/x/net/publicsuffix"
)

type pslCache struct {
	mu    sync.RWMutex
	max   int
	items map[string]string
}

var globalPSLCache = &pslCache{max: 100000, items: make(map[string]string)}

func SetPSLCacheSize(n int) {
	if n <= 0 {
		return
	}
	globalPSLCache.mu.Lock()
	defer globalPSLCache.mu.Unlock()
	globalPSLCache.max = n
}

func EffectiveTLDPlusOneCached(domain string) (string, error) {
	c := globalPSLCache
	c.mu.RLock()
	if v, ok := c.items[domain]; ok {
		c.mu.RUnlock()
		return v, nil
	}
	c.mu.RUnlock()

	root, err := publicsuffix.EffectiveTLDPlusOne(domain)
	if err != nil {
		return "", err
	}

	c.mu.Lock()
	if len(c.items) >= c.max {
		// Simple eviction: remove a small arbitrary subset.
		removed := 0
		for k := range c.items {
			delete(c.items, k)
			removed++
			if removed >= c.max/10+1 {
				break
			}
		}
	}
	c.items[domain] = root
	c.mu.Unlock()
	return root, nil
}
