package overflow

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/K0ngS3ng/CertStreamerPro/internal/dedup"
)

type SpillQueue struct {
	path string
	mu   sync.Mutex
}

func NewSpillQueue(dir string) (*SpillQueue, error) {
	if dir == "" {
		return nil, fmt.Errorf("spill dir is required")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "spill.log")
	return &SpillQueue{path: path}, nil
}

func (s *SpillQueue) Append(ev dedup.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	f, err := os.OpenFile(s.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	return enc.Encode(ev)
}

func (s *SpillQueue) Drain(fn func(ev dedup.Event) bool) error {
	s.mu.Lock()
	if _, err := os.Stat(s.path); err != nil {
		s.mu.Unlock()
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	tmp := fmt.Sprintf("%s.%d", s.path, time.Now().UnixNano())
	if err := os.Rename(s.path, tmp); err != nil {
		s.mu.Unlock()
		return err
	}
	s.mu.Unlock()

	f, err := os.Open(tmp)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var ev dedup.Event
		if err := json.Unmarshal(scanner.Bytes(), &ev); err != nil {
			continue
		}
		if !fn(ev) {
			break
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return os.Remove(tmp)
}
