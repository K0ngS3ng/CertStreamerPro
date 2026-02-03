package tailer

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/K0ngS3ng/CertStreamerPro/internal/ctlog"
	"github.com/K0ngS3ng/CertStreamerPro/internal/parser"
	"github.com/K0ngS3ng/CertStreamerPro/internal/store"
	"github.com/K0ngS3ng/CertStreamerPro/internal/util"
)

type Tailer struct {
	Log        ctlog.Log
	Client     *http.Client
	Store      *store.Store
	BatchSize  int
	Poll       time.Duration
	Retry      util.Backoff
	RawOut     chan<- parser.RawEntry
	Logf       func(string, ...any)
	DropOnFull bool
}

type sthResponse struct {
	TreeSize uint64 `json:"tree_size"`
}

type entriesResponse struct {
	Entries []struct {
		LeafInput string `json:"leaf_input"`
		ExtraData string `json:"extra_data"`
	} `json:"entries"`
}

func (t *Tailer) Run(ctx context.Context) {
	if t.Client == nil || t.Store == nil {
		return
	}
	if t.BatchSize < 1 {
		t.BatchSize = 256
	}
	if t.Poll <= 0 {
		t.Poll = 5 * time.Second
	}

	backoff := t.Retry
	if backoff.Initial == 0 {
		backoff.Initial = 500 * time.Millisecond
		backoff.Max = 30 * time.Second
		backoff.Factor = 2.0
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		sth, err := t.getSTH(ctx)
		if err != nil {
			t.sleep(ctx, backoff.Next(), "get-sth error: %v", err)
			continue
		}
		backoff.Reset()

		start, ok := t.startIndex(sth.TreeSize)
		if !ok {
			t.sleep(ctx, backoff.Next(), "progress read error, retrying")
			continue
		}
		for start < sth.TreeSize {
			end := start + uint64(t.BatchSize) - 1
			if end >= sth.TreeSize {
				end = sth.TreeSize - 1
			}
			entries, err := t.getEntries(ctx, start, end)
			if err != nil {
				t.sleep(ctx, backoff.Next(), "get-entries error: %v", err)
				break
			}
			backoff.Reset()
			if !t.emitEntries(ctx, entries) {
				break
			}
			_ = t.Store.PutProgress(t.Log.LogID, end)
			start = end + 1
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(t.Poll):
		}
	}
}

func (t *Tailer) startIndex(treeSize uint64) (uint64, bool) {
	last, found, err := t.Store.GetProgress(t.Log.LogID)
	if err != nil {
		return 0, false
	}
	if !found {
		if treeSize == 0 {
			return 0, true
		}
		last = treeSize - 1
		_ = t.Store.PutProgress(t.Log.LogID, last)
		return treeSize, true
	}
	if last+1 > treeSize {
		return treeSize, true
	}
	return last + 1, true
}

func (t *Tailer) emitEntries(ctx context.Context, entries []parser.RawEntry) bool {
	for _, e := range entries {
		select {
		case <-ctx.Done():
			return false
		case t.RawOut <- e:
			continue
		default:
			if t.DropOnFull {
				if t.Logf != nil {
					t.Logf("raw channel full, dropping entry from %s", t.Log.URL)
				}
				continue
			}
			select {
			case <-ctx.Done():
				return false
			case t.RawOut <- e:
				continue
			}
		}
	}
	return true
}

func (t *Tailer) getSTH(ctx context.Context) (*sthResponse, error) {
	url := fmt.Sprintf("%s/ct/v1/get-sth", t.Log.URL)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := t.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("get-sth status: %s", resp.Status)
	}
	var out sthResponse
	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(&out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (t *Tailer) getEntries(ctx context.Context, start, end uint64) ([]parser.RawEntry, error) {
	url := fmt.Sprintf("%s/ct/v1/get-entries?start=%d&end=%d", t.Log.URL, start, end)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := t.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("get-entries status: %s", resp.Status)
	}
	var out entriesResponse
	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(&out); err != nil {
		return nil, err
	}
	entries := make([]parser.RawEntry, 0, len(out.Entries))
	for _, e := range out.Entries {
		entries = append(entries, parser.RawEntry{
			LogURL:    t.Log.URL,
			LeafInput: e.LeafInput,
			ExtraData: e.ExtraData,
			Observed:  time.Now(),
		})
	}
	return entries, nil
}

func (t *Tailer) sleep(ctx context.Context, d time.Duration, format string, args ...any) {
	if t.Logf != nil {
		t.Logf(format, args...)
	}
	select {
	case <-time.After(d):
	case <-ctx.Done():
	}
}
