package ctlog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type logList struct {
	Operators []struct {
		Name string `json:"name"`
		Logs []struct {
			LogID string                 `json:"log_id"`
			URL   string                 `json:"url"`
			State map[string]interface{} `json:"state"`
		} `json:"logs"`
	} `json:"operators"`
}

func Discover(ctx context.Context, client *http.Client, url string, includeAll bool, excludeSubs []string, allowedOps []string) ([]Log, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("log list status: %s", resp.Status)
	}

	var list logList
	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(&list); err != nil {
		return nil, err
	}

	var logs []Log
	for _, op := range list.Operators {
		if !operatorAllowed(op.Name, allowedOps) {
			continue
		}
		for _, l := range op.Logs {
			if !stateAllowed(l.State, includeAll) {
				continue
			}
			if l.URL == "" || l.LogID == "" {
				continue
			}
			if !validLogURL(l.URL, excludeSubs) {
				continue
			}
			logs = append(logs, Log{
				LogID:    l.LogID,
				URL:      strings.TrimRight(l.URL, "/"),
				Operator: op.Name,
			})
		}
	}
	if len(logs) == 0 {
		return nil, errors.New("no usable logs found")
	}
	return logs, nil
}

func isUsable(state map[string]interface{}) bool {
	if state == nil {
		return false
	}
	_, ok := state["usable"]
	return ok
}

func stateAllowed(state map[string]interface{}, includeAll bool) bool {
	if state == nil {
		return false
	}
	if includeAll {
		if _, ok := state["test"]; ok {
			return false
		}
		if _, ok := state["pending"]; ok {
			return false
		}
		if _, ok := state["rejected"]; ok {
			return false
		}
		return true
	}
	return isUsable(state)
}

func validLogURL(raw string, excludeSubs []string) bool {
	u, err := url.Parse(raw)
	if err != nil {
		return false
	}
	if u.Scheme != "https" || u.Host == "" {
		return false
	}
	lower := strings.ToLower(raw)
	for _, sub := range excludeSubs {
		if sub == "" {
			continue
		}
		if strings.Contains(lower, strings.ToLower(sub)) {
			return false
		}
	}
	return true
}

func operatorAllowed(name string, allowed []string) bool {
	if len(allowed) == 0 {
		return true
	}
	n := strings.ToLower(strings.TrimSpace(name))
	for _, a := range allowed {
		if strings.ToLower(strings.TrimSpace(a)) == n {
			return true
		}
	}
	return false
}

func DefaultHTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{Timeout: timeout}
}
